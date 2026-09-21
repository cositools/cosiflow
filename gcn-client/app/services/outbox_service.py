from __future__ import annotations

import json
import logging
import random
import socket
import time
from typing import Any, Callable

from app.config import Settings
from app.db.store import NoticeStore
from app.kafka.producer import GcnProducer
from app.parsers.normalization import canonical_json, normalize_json_notice, payload_sha256
from app.resilience import BackoffPolicy
from app.schemas.validator import CosiNoticeValidator, enforce_prototype_safety

logger = logging.getLogger(__name__)


class OutboxService:
    def __init__(
        self,
        settings: Settings,
        store: NoticeStore,
        validator: CosiNoticeValidator,
        *,
        sleep: Callable[[float], None] = time.sleep,
        random_fn: Callable[[], float] = random.random,
    ):
        self.settings = settings
        self.store = store
        self.validator = validator
        self.worker_id = f"{settings.producer_client_label}:{socket.gethostname()}"
        self._producer: GcnProducer | None = None
        self._sleep = sleep
        self._random_fn = random_fn
        self._worker_backoff = BackoffPolicy(
            settings.worker_backoff_initial_seconds,
            settings.worker_backoff_max_seconds,
            settings.worker_backoff_jitter_ratio,
        )
        self._delivery_backoff = BackoffPolicy(
            settings.outbox_retry_initial_seconds,
            settings.outbox_retry_max_seconds,
            settings.worker_backoff_jitter_ratio,
        )

    def run_forever(self) -> None:
        consecutive_failures = 0
        while True:
            try:
                self.process_once()
                consecutive_failures = 0
                self._sleep(self.settings.publish_poll_seconds)
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
                delay = self._worker_backoff.delay(consecutive_failures, self._random_fn)
                logger.exception(
                    "Outbox worker failure %s/%s; retrying in %.3fs",
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
                self._sleep(delay)

    def process_once(self) -> int:
        recovered_locks = self.store.recover_stale_outbound_locks(
            self.settings.outbox_lock_timeout_seconds
        )
        self.store.heartbeat(
            "outbox",
            "running",
            {
                "dry_run": self.settings.dry_run,
                "producer_enabled": self.settings.producer_enabled,
                "allowlist": self.settings.topic_allowlist,
                "recovered_locks": recovered_locks,
            },
        )
        rows = self.store.claim_outbound_notices(self.settings.publish_batch_size, self.worker_id)
        for row in rows:
            self._publish_row(row)
        return len(rows)

    def queue_payload(
        self,
        payload: dict[str, Any],
        *,
        topic: str | None = None,
        idempotency_key: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> int:
        topic = topic or self.settings.outbound_topic_default
        validation_errors = self.validator.validate(payload)
        safety_errors = enforce_prototype_safety(
            payload,
            topic,
            self.settings.topic_allowlist,
            self.settings.require_test_topics,
        )
        all_errors = validation_errors + [{"message": error, "guard": "prototype_safety"} for error in safety_errors]
        normalized = normalize_json_notice(payload)
        canonical = canonical_json(payload)
        notice = {
            **normalized,
            **(metadata or {}),
            "status": "invalid" if all_errors else "queued",
            "topic": topic,
            "topic_kind": "test",
            "payload_json": payload,
            "payload_sha256": payload_sha256(canonical),
            "validation_status": "invalid" if all_errors else "valid",
            "validation_errors": all_errors or None,
            "idempotency_key": idempotency_key or payload_sha256(canonical),
        }
        return self.store.queue_outbound_notice(notice)

    def _publish_row(self, row: dict[str, Any]) -> None:
        try:
            payload = _ensure_dict(row["payload_json"])
            validation_errors = self.validator.validate(payload)
            safety_errors = enforce_prototype_safety(
                payload,
                row["topic"],
                self.settings.topic_allowlist,
                self.settings.require_test_topics,
            )
        except Exception as exc:
            logger.warning(
                "Rejecting unreadable outbound id=%s error_class=%s",
                row.get("id"),
                type(exc).__name__,
            )
            self.store.record_claim_failure(row, exc, permanent=True)
            return
        if validation_errors or safety_errors:
            exc = RuntimeError(
                "Outbound notice failed validation: "
                + json.dumps(validation_errors + safety_errors, default=str)
            )
            self.store.record_claim_failure(row, exc, permanent=True)
            return

        attempt_id = self.store.start_attempt(row["id"], int(row["attempts_count"]) + 1, row, self.settings.dry_run)
        try:
            if self.settings.dry_run or not self.settings.producer_enabled:
                metadata = {"mode": "dry-run", "producer_enabled": self.settings.producer_enabled}
                logger.info("Dry-run publish outbound id=%s topic=%s", row["id"], row["topic"])
            else:
                metadata = self._get_producer().publish(row["topic"], payload)
                logger.info("Published outbound id=%s topic=%s", row["id"], row["topic"])
            self.store.finish_attempt_success(
                attempt_id,
                row["id"],
                self.settings.dry_run or not self.settings.producer_enabled,
                metadata,
            )
        except Exception as exc:
            logger.exception("Failed to publish outbound id=%s", row["id"])
            self._producer = None
            attempt_no = int(row["attempts_count"]) + 1
            retry_delay = self._delivery_backoff.delay(attempt_no, self._random_fn)
            self.store.finish_attempt_failure(
                attempt_id,
                row["id"],
                row,
                exc,
                retry_delay_seconds=retry_delay,
            )

    def _get_producer(self) -> GcnProducer:
        if self._producer is None:
            self._producer = GcnProducer(self.settings)
        return self._producer

    def _safe_heartbeat(self, status: str, details: dict[str, Any]) -> None:
        try:
            self.store.heartbeat("outbox", status, details)
        except Exception:
            logger.exception("Could not persist outbox %s heartbeat", status)


def _ensure_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise TypeError(f"Expected JSON object, got {type(value).__name__}")
