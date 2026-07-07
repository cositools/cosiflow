from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from app.config import Settings
from app.db.store import NoticeStore
from app.parsers.json_notice import parse_notice_payload
from app.schemas.validator import CosiNoticeValidator

logger = logging.getLogger(__name__)


class InboundService:
    def __init__(self, settings: Settings, store: NoticeStore, validator: CosiNoticeValidator):
        self.settings = settings
        self.store = store
        self.validator = validator

    def run_forever(self) -> None:
        if not self.settings.consumer_enabled:
            logger.info("Inbound consumer disabled")
            return
        if not self.settings.consumer_topics:
            logger.info("No GCN_CONSUMER_TOPICS configured; inbound consumer idle")
            return
        if not self.settings.client_id or not self.settings.client_secret:
            logger.warning("GCN credentials are missing; inbound consumer idle")
            self.store.heartbeat(
                "inbound",
                "idle",
                {"reason": "missing GCN_CLIENT_ID or GCN_CLIENT_SECRET"},
            )
            return

        from gcn_kafka import Consumer

        consumer = Consumer(
            config={"group.id": self.settings.consumer_group_id},
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            domain=self.settings.gcn_domain,
        )
        consumer.subscribe(self.settings.consumer_topics)
        logger.info("Subscribed to GCN topics: %s", ", ".join(self.settings.consumer_topics))

        while True:
            self.store.heartbeat("inbound", "running", {"topics": self.settings.consumer_topics})
            for message in consumer.consume(timeout=self.settings.consumer_poll_timeout):
                if message.error():
                    logger.error("Kafka consumer error: %s", message.error())
                    continue
                self._handle_message(message)

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

        from gcn_kafka import Consumer

        consumer = Consumer(
            config={"group.id": self.settings.consumer_group_id},
            client_id=self.settings.client_id,
            client_secret=self.settings.client_secret,
            domain=self.settings.gcn_domain,
        )
        consumer.subscribe(self.settings.consumer_topics)
        deadline = time.monotonic() + seconds
        stored = 0
        self.store.heartbeat("inbound", "running", {"topics": self.settings.consumer_topics, "mode": "consume_for"})
        while time.monotonic() < deadline and stored < max_messages:
            for message in consumer.consume(timeout=self.settings.consumer_poll_timeout):
                if message.error():
                    logger.error("Kafka consumer error: %s", message.error())
                    continue
                self._handle_message(message)
                stored += 1
                if stored >= max_messages:
                    break
        self.store.heartbeat("inbound", "idle", {"last_consume_for_stored": stored})
        return stored

    def inject(self, raw_payload: str, topic: str, source: str = "injection") -> int:
        notice = parse_notice_payload(
            raw_payload,
            topic=topic,
            source=source,
            validator=self.validator,
        )
        return self.store.insert_inbound_notice(notice)

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
