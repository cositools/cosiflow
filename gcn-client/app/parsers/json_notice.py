from __future__ import annotations

import json
from typing import Any

from app.parsers.normalization import normalize_json_notice, payload_sha256
from app.parsers.voevent import extract_voevent_summary
from app.schemas.validator import CosiNoticeValidator


def parse_notice_payload(
    raw_payload: str,
    *,
    topic: str,
    source: str,
    validator: CosiNoticeValidator,
    kafka_partition: int | None = None,
    kafka_offset: int | None = None,
    kafka_key: bytes | None = None,
    kafka_timestamp: str | None = None,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "source": source,
        "topic": topic,
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "kafka_key": kafka_key,
        "kafka_timestamp": kafka_timestamp,
        "payload_sha256": payload_sha256(raw_payload),
        "raw_payload": raw_payload,
    }

    try:
        payload = json.loads(raw_payload)
    except json.JSONDecodeError:
        return _parse_as_voevent(raw_payload, base)

    if not isinstance(payload, dict):
        return {
            **base,
            "content_type": "json",
            "parse_status": "failed",
            "validation_status": "invalid",
            "validation_errors": [{"message": "JSON notice payload must be an object"}],
        }

    is_cosi_schema = str(payload.get("$schema", "")).endswith("/gcn/notices/cosi/alert.schema.json")
    validation_status = "not_applicable"
    validation_errors: list[dict[str, Any]] | None = None
    if is_cosi_schema:
        errors = validator.validate(payload)
        validation_status = "valid" if not errors else "invalid"
        validation_errors = errors or None

    return {
        **base,
        **normalize_json_notice(payload),
        "content_type": "json",
        "payload_json": payload,
        "parse_status": "parsed",
        "validation_status": validation_status,
        "validation_errors": validation_errors,
    }


def _parse_as_voevent(raw_payload: str, base: dict[str, Any]) -> dict[str, Any]:
    try:
        summary = extract_voevent_summary(raw_payload)
    except Exception as exc:
        content_type = "text" if str(base.get("topic", "")).startswith("gcn.classic.text.") else "unknown"
        return {
            **base,
            "content_type": content_type,
            "parse_status": "raw_only",
            "validation_status": "not_applicable",
            "validation_errors": [{"message": str(exc), "parser": "voevent"}],
        }
    return {
        **base,
        **summary,
        "content_type": "voevent_xml",
        "parse_status": "parsed",
        "validation_status": "not_applicable",
    }
