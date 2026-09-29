from __future__ import annotations

import json
from typing import Any

from .normalization import canonical_json, normalize_json_notice, payload_bytes, payload_sha256
from .validation import enforce_prototype_safety, topic_kind
from .xml_notice import extract_voevent_summary


def prepare_inbound_notice(
    raw_payload: str | bytes | bytearray | memoryview,
    *,
    topic: str,
    source: str,
    validator,
    kafka_partition: int | None = None,
    kafka_offset: int | None = None,
    kafka_key: bytes | None = None,
    kafka_timestamp: str | None = None,
    idempotency_key: str | None = None,
) -> dict[str, Any]:
    raw_bytes = payload_bytes(raw_payload)
    payload_hash = payload_sha256(raw_bytes)
    base: dict[str, Any] = {
        "source": source,
        "topic": str(topic).strip(),
        "kafka_partition": kafka_partition,
        "kafka_offset": kafka_offset,
        "kafka_key": kafka_key,
        "kafka_timestamp": kafka_timestamp,
        "payload_sha256": payload_hash,
        "raw_payload": raw_bytes,
        "idempotency_key": None,
    }
    if kafka_partition is None and kafka_offset is None:
        base["idempotency_key"] = (
            str(idempotency_key or "").strip()
            or f"inbound:{base['topic']}:{payload_hash}"
        )

    try:
        text = raw_bytes.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        return {
            **base,
            "content_type": "binary",
            "parse_status": "raw_only",
            "validation_status": "invalid",
            "validation_errors": [_diagnostic(exc, "utf8")],
        }

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        return _parse_non_json(text, base)

    if not isinstance(payload, dict):
        return {
            **base,
            "content_type": "json",
            "parse_status": "failed",
            "validation_status": "invalid",
            "validation_errors": [{"message": "JSON notice payload must be an object"}],
        }

    is_cosi_schema = str(payload.get("$schema", "")).endswith(
        "/gcn/notices/cosi/alert.schema.json"
    )
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


def prepare_outbound_notice(
    payload: dict[str, Any],
    *,
    topic: str,
    validator,
    allowlist: list[str],
    require_test_topics: bool,
    idempotency_key: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise TypeError("Outbound payload must be a JSON object")
    topic = str(topic or "").strip()
    if not topic:
        raise ValueError("Outbox topic is required")
    validation_errors = validator.validate(payload)
    safety_errors = enforce_prototype_safety(
        payload,
        topic,
        allowlist,
        require_test_topics,
    )
    all_errors = validation_errors + [
        {"message": error, "guard": "prototype_safety"}
        for error in safety_errors
    ]
    canonical = canonical_json(payload)
    payload_hash = payload_sha256(canonical)
    key = str(idempotency_key or "").strip() or f"outbound:{topic}:{payload_hash}"
    return {
        **normalize_json_notice(payload),
        **(metadata or {}),
        "status": "invalid" if all_errors else "queued",
        "topic": topic,
        "topic_kind": topic_kind(topic),
        "payload_json": payload,
        "payload_sha256": payload_hash,
        "validation_status": "invalid" if all_errors else "valid",
        "validation_errors": all_errors or None,
        "idempotency_key": key,
    }


def _parse_non_json(text: str, base: dict[str, Any]) -> dict[str, Any]:
    if not text.lstrip().startswith("<"):
        content_type = (
            "text" if str(base.get("topic", "")).startswith("gcn.classic.text.") else "unknown"
        )
        return {
            **base,
            "content_type": content_type,
            "parse_status": "raw_only",
            "validation_status": "not_applicable",
        }
    try:
        summary = extract_voevent_summary(text)
    except Exception as exc:
        return {
            **base,
            "content_type": "unknown",
            "parse_status": "raw_only",
            "validation_status": "invalid",
            "validation_errors": [_diagnostic(exc, "voevent")],
        }
    return {
        **base,
        **summary,
        "content_type": "voevent_xml",
        "parse_status": "parsed",
        "validation_status": "not_applicable",
    }


def _diagnostic(exc: Exception, parser: str) -> dict[str, str]:
    message = str(exc).replace("\n", " ").strip()
    if len(message) > 300:
        message = f"{message[:297]}..."
    return {"message": message or type(exc).__name__, "parser": parser}
