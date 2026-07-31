from __future__ import annotations

import json
import logging
import socket
import time
from typing import Any

from app.config import Settings
from app.db.store import NoticeStore
from app.kafka.producer import GcnProducer
from app.parsers.normalization import canonical_json, normalize_json_notice, payload_sha256
from app.schemas.validator import CosiNoticeValidator, enforce_prototype_safety

logger = logging.getLogger(__name__)


class OutboxService:
    def __init__(self, settings: Settings, store: NoticeStore, validator: CosiNoticeValidator):
        self.settings = settings
        self.store = store
        self.validator = validator
        self.worker_id = f"{settings.producer_client_label}:{socket.gethostname()}"
        self._producer: GcnProducer | None = None

    def run_forever(self) -> None:
        while True:
            self.process_once()
            time.sleep(self.settings.publish_poll_seconds)

    def process_once(self) -> int:
        self.store.heartbeat(
            "outbox",
            "running",
            {
                "dry_run": self.settings.dry_run,
                "producer_enabled": self.settings.producer_enabled,
                "allowlist": self.settings.topic_allowlist,
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
        payload = _ensure_dict(row["payload_json"])
        validation_errors = self.validator.validate(payload)
        safety_errors = enforce_prototype_safety(
            payload,
            row["topic"],
            self.settings.topic_allowlist,
            self.settings.require_test_topics,
        )
        if validation_errors or safety_errors:
            exc = RuntimeError(
                "Outbound notice failed validation: "
                + json.dumps(validation_errors + safety_errors, default=str)
            )
            attempt_id = self.store.start_attempt(row["id"], int(row["attempts_count"]) + 1, row, self.settings.dry_run)
            self.store.finish_attempt_failure(attempt_id, row["id"], row, exc)
            return

        attempt_id = self.store.start_attempt(row["id"], int(row["attempts_count"]) + 1, row, self.settings.dry_run)
        try:
            if self.settings.dry_run or not self.settings.producer_enabled:
                metadata = {"mode": "dry-run", "producer_enabled": self.settings.producer_enabled}
                logger.info("Dry-run publish outbound id=%s topic=%s", row["id"], row["topic"])
            else:
                metadata = self._get_producer().publish(row["topic"], payload)
                logger.info("Published outbound id=%s topic=%s", row["id"], row["topic"])
            self.store.finish_attempt_success(attempt_id, row["id"], self.settings.dry_run or not self.settings.producer_enabled, metadata)
        except Exception as exc:
            logger.exception("Failed to publish outbound id=%s", row["id"])
            self.store.finish_attempt_failure(attempt_id, row["id"], row, exc)

    def _get_producer(self) -> GcnProducer:
        if self._producer is None:
            self._producer = GcnProducer(self.settings)
        return self._producer


def _ensure_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        return json.loads(value)
    raise TypeError(f"Expected JSON object, got {type(value).__name__}")
