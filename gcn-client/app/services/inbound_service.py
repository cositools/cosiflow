from __future__ import annotations

import logging
import random
import time
from datetime import datetime, timezone
from typing import Callable

from app.config import Settings
from app.db.store import NoticeStore
from app.parsers.json_notice import parse_notice_payload
from app.resilience import BackoffPolicy
from app.schemas.validator import CosiNoticeValidator

logger = logging.getLogger(__name__)


class InboundService:
    def __init__(
        self,
        settings: Settings,
        store: NoticeStore,
        validator: CosiNoticeValidator,
        *,
        consumer_factory=None,
        sleep: Callable[[float], None] = time.sleep,
        random_fn: Callable[[], float] = random.random,
    ):
        self.settings = settings
        self.store = store
        self.validator = validator
        self._consumer_factory = consumer_factory
        self._sleep = sleep
        self._random_fn = random_fn
        self._backoff = BackoffPolicy(
            settings.worker_backoff_initial_seconds,
            settings.worker_backoff_max_seconds,
            settings.worker_backoff_jitter_ratio,
        )

    def run_forever(self) -> None:
        if not self.settings.consumer_enabled:
            raise RuntimeError("Inbound consumer is disabled; use run-outbox for an outbox-only process")
        if not self.settings.consumer_topics:
            raise RuntimeError("No GCN_CONSUMER_TOPICS configured")
        if not self.settings.client_id or not self.settings.client_secret:
            raise RuntimeError("GCN credentials are missing")

        consecutive_failures = 0
        while True:
            consumer = None
            retry_delay = None
            try:
                consumer = self._build_consumer()
                consumer.subscribe(self.settings.consumer_topics)
                logger.info("Subscribed to GCN topics: %s", ", ".join(self.settings.consumer_topics))
                while True:
                    self.store.heartbeat(
                        "inbound", "running", {"topics": self.settings.consumer_topics}
                    )
                    messages = consumer.consume(timeout=self.settings.consumer_poll_timeout)
                    for message in messages:
                        if message.error():
                            raise RuntimeError(f"Kafka consumer error: {message.error()}")
                        self._handle_message(message)
                        if self.settings.consumer_commit:
                            consumer.commit(message=message, asynchronous=False)
                    consecutive_failures = 0
            except Exception as exc:
                consecutive_failures += 1
                if consecutive_failures >= self.settings.worker_failure_budget:
                    self._safe_heartbeat(
                        "failed",
                        {
                            "error_class": type(exc).__name__,
                            "consecutive_failures": consecutive_failures,
                        },
                    )
                    raise
                delay = self._backoff.delay(consecutive_failures, self._random_fn)
                logger.exception(
                    "Inbound worker failure %s/%s; retrying in %.3fs",
                    consecutive_failures,
                    self.settings.worker_failure_budget,
                    delay,
                )
                self._safe_heartbeat(
                    "degraded",
                    {
                        "error_class": type(exc).__name__,
                        "consecutive_failures": consecutive_failures,
                        "retry_in_seconds": delay,
                    },
                )
                retry_delay = delay
            finally:
                self._close_consumer(consumer)
            if retry_delay is not None:
                self._sleep(retry_delay)

    def consume_for(self, seconds: float, max_messages: int = 1) -> int:
        if not self.settings.consumer_enabled:
            logger.info("Inbound consumer disabled")
            return 0
        if not self.settings.consumer_topics:
            logger.info("No GCN_CONSUMER_TOPICS configured; inbound consumer idle")
            return 0
        if not self.settings.client_id or not self.settings.client_secret:
            logger.warning("GCN credentials are missing; inbound consumer idle")
            self.store.heartbeat(
                "inbound",
                "idle",
                {"reason": "missing GCN_CLIENT_ID or GCN_CLIENT_SECRET"},
            )
            return 0

        consumer = self._build_consumer()
        try:
            consumer.subscribe(self.settings.consumer_topics)
            deadline = time.monotonic() + seconds
            stored = 0
            self.store.heartbeat(
                "inbound",
                "running",
                {"topics": self.settings.consumer_topics, "mode": "consume_for"},
            )
            while time.monotonic() < deadline and stored < max_messages:
                for message in consumer.consume(timeout=self.settings.consumer_poll_timeout):
                    if message.error():
                        logger.error("Kafka consumer error: %s", message.error())
                        continue
                    self._handle_message(message)
                    if self.settings.consumer_commit:
                        consumer.commit(message=message, asynchronous=False)
                    stored += 1
                    if stored >= max_messages:
                        break
            self.store.heartbeat("inbound", "idle", {"last_consume_for_stored": stored})
            return stored
        finally:
            self._close_consumer(consumer)

    def inject(self, raw_payload: str, topic: str, source: str = "injection") -> int:
        notice = parse_notice_payload(
            raw_payload,
            topic=topic,
            source=source,
            validator=self.validator,
        )
        return self.store.insert_inbound_notice(notice)

    def _build_consumer(self):
        factory = self._consumer_factory
        if factory is None:
            from gcn_kafka import Consumer

            factory = Consumer
        return factory(
            config={
                "group.id": self.settings.consumer_group_id,
                "enable.auto.commit": False,
            },
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            domain=self.settings.gcn_domain,
        )

    def _safe_heartbeat(self, status: str, details: dict) -> None:
        try:
            self.store.heartbeat("inbound", status, details)
        except Exception:
            logger.exception("Could not persist inbound %s heartbeat", status)

    @staticmethod
    def _close_consumer(consumer) -> None:
        if consumer is None or not hasattr(consumer, "close"):
            return
        try:
            consumer.close()
        except Exception:
            logger.exception("Could not close inbound consumer")

    def _handle_message(self, message) -> None:
        raw_value = message.value()
        if isinstance(raw_value, bytes):
            raw_payload = raw_value.decode("utf-8", errors="replace")
        else:
            raw_payload = str(raw_value)

        timestamp = None
        try:
            timestamp_type, timestamp_ms = message.timestamp()
            if timestamp_ms:
                timestamp = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).replace(tzinfo=None).isoformat(" ")
        except Exception:
            timestamp = None

        notice = parse_notice_payload(
            raw_payload,
            topic=message.topic(),
            source="kafka",
            validator=self.validator,
            kafka_partition=message.partition(),
            kafka_offset=message.offset(),
            kafka_key=message.key(),
            kafka_timestamp=timestamp,
        )
        notice_id = self.store.insert_inbound_notice(notice)
        logger.info("Stored inbound notice id=%s topic=%s offset=%s", notice_id, message.topic(), message.offset())
