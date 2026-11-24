from __future__ import annotations

import json
from typing import Any
from uuid import uuid4


class IdempotencyConflictError(ValueError):
    """Raised when an idempotency identity is reused for different content."""


INBOUND_COLUMNS = [
    "notice_uuid",
    "source",
    "topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_key",
    "kafka_timestamp",
    "idempotency_key",
    "content_type",
    "payload_sha256",
    "raw_payload",
    "payload_json",
    "parse_status",
    "validation_status",
    "validation_errors",
    "schema_url",
    "schema_version",
    "mission",
    "instrument",
    "alert_type",
    "alert_tense",
    "event_name",
    "event_ids",
    "trigger_time",
    "alert_datetime",
    "ra_deg",
    "dec_deg",
    "ra_dec_error_json",
    "healpix_url",
    "classification_json",
    "packet_type",
    "packet_type_name",
    "trig_id",
    "sequence_num",
    "isotime",
]

OUTBOUND_COLUMNS = [
    "outbox_uuid",
    "status",
    "topic",
    "topic_kind",
    "payload_json",
    "payload_sha256",
    "schema_url",
    "schema_version",
    "validation_status",
    "validation_errors",
    "mission",
    "instrument",
    "alert_type",
    "alert_tense",
    "record_number",
    "event_name",
    "event_ids",
    "trigger_time",
    "alert_datetime",
    "created_by_dag_id",
    "dag_run_id",
    "task_id",
    "source_pipeline",
    "source_product_dir",
    "source_config_path",
    "priority",
    "max_attempts",
    "idempotency_key",
]


def insert_inbound_notice(conn, notice: dict[str, Any]) -> int:
    values = {column: notice.get(column) for column in INBOUND_COLUMNS}
    values["notice_uuid"] = values["notice_uuid"] or str(uuid4())
    for column in (
        "payload_json",
        "validation_errors",
        "event_ids",
        "ra_dec_error_json",
        "classification_json",
    ):
        values[column] = json_or_none(values[column])

    placeholders = ", ".join(["%s"] * len(INBOUND_COLUMNS))
    sql = f"""
        INSERT INTO gcn_inbound_notices ({", ".join(INBOUND_COLUMNS)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
    """
    with conn.cursor() as cur:
        cur.execute(sql, [values[column] for column in INBOUND_COLUMNS])
        notice_id = int(cur.lastrowid)
        cur.execute(
            """
            SELECT id, topic, kafka_partition, kafka_offset, idempotency_key,
                   payload_sha256, raw_payload
            FROM gcn_inbound_notices
            WHERE id = %s
            FOR UPDATE
            """,
            (notice_id,),
        )
        existing = cur.fetchone()
    if not existing or not _same_inbound_identity(existing, values):
        raise IdempotencyConflictError(
            "Inbound idempotency identity already belongs to a different payload"
        )
    return notice_id


def queue_outbound_notice(conn, notice: dict[str, Any], *, default_max_attempts: int) -> int:
    values = {column: notice.get(column) for column in OUTBOUND_COLUMNS}
    values["outbox_uuid"] = values["outbox_uuid"] or str(uuid4())
    values["status"] = values["status"] or "queued"
    values["topic_kind"] = values["topic_kind"] or "non_test"
    values["priority"] = values["priority"] or 0
    values["max_attempts"] = values["max_attempts"] or default_max_attempts
    for column in ("payload_json", "validation_errors", "event_ids"):
        values[column] = json_or_none(values[column])
    if not values["idempotency_key"]:
        raise ValueError("Outbound idempotency_key is required")

    placeholders = ", ".join(["%s"] * len(OUTBOUND_COLUMNS))
    sql = f"""
        INSERT INTO gcn_outbound_notices ({", ".join(OUTBOUND_COLUMNS)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)
    """
    with conn.cursor() as cur:
        cur.execute(sql, [values[column] for column in OUTBOUND_COLUMNS])
        notice_id = int(cur.lastrowid)
        cur.execute(
            """
            SELECT id, topic, payload_json, payload_sha256, idempotency_key
            FROM gcn_outbound_notices
            WHERE id = %s
            FOR UPDATE
            """,
            (notice_id,),
        )
        existing = cur.fetchone()
    if not existing or not _same_outbound_identity(existing, values):
        raise IdempotencyConflictError(
            "Outbound idempotency key already belongs to a different topic or payload"
        )
    return notice_id


def json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def _same_inbound_identity(existing: dict[str, Any], proposed: dict[str, Any]) -> bool:
    return (
        existing.get("topic") == proposed.get("topic")
        and existing.get("kafka_partition") == proposed.get("kafka_partition")
        and existing.get("kafka_offset") == proposed.get("kafka_offset")
        and existing.get("idempotency_key") == proposed.get("idempotency_key")
        and existing.get("payload_sha256") == proposed.get("payload_sha256")
        and _as_bytes(existing.get("raw_payload")) == _as_bytes(proposed.get("raw_payload"))
    )


def _same_outbound_identity(existing: dict[str, Any], proposed: dict[str, Any]) -> bool:
    return (
        existing.get("idempotency_key") == proposed.get("idempotency_key")
        and existing.get("topic") == proposed.get("topic")
        and existing.get("payload_sha256") == proposed.get("payload_sha256")
        and _canonical_json_value(existing.get("payload_json"))
        == _canonical_json_value(proposed.get("payload_json"))
    )


def _as_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, str):
        return value.encode("utf-8")
    return bytes(value)


def _canonical_json_value(value: Any) -> str:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
