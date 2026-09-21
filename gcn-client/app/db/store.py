from __future__ import annotations

import json
import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

import pymysql
from pymysql.connections import Connection

from app.config import Settings

logger = logging.getLogger(__name__)


class NoticeStore:
    def __init__(self, settings: Settings):
        self.settings = settings

    @contextmanager
    def connection(self):
        conn = pymysql.connect(
            host=self.settings.db_host,
            port=self.settings.db_port,
            database=self.settings.db_name,
            user=self.settings.db_user,
            password=self.settings.db_password,
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=False,
            connect_timeout=self.settings.db_connect_timeout,
        )
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        schema_path = Path(__file__).with_name("schema.sql")
        sql = schema_path.read_text(encoding="utf-8")
        statements = [part.strip() for part in sql.split(";") if part.strip()]
        with self.connection() as conn:
            with conn.cursor() as cur:
                for statement in statements:
                    cur.execute(statement)
        logger.info("GCN MySQL schema is ready")

    def insert_inbound_notice(self, notice: dict[str, Any]) -> int:
        columns = [
            "notice_uuid",
            "source",
            "topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_key",
            "kafka_timestamp",
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
        values = {column: notice.get(column) for column in columns}
        values["notice_uuid"] = values["notice_uuid"] or str(uuid4())
        for json_column in [
            "payload_json",
            "validation_errors",
            "event_ids",
            "ra_dec_error_json",
            "classification_json",
        ]:
            values[json_column] = _json_or_none(values[json_column])

        placeholders = ", ".join(["%s"] * len(columns))
        sql = f"""
            INSERT INTO gcn_inbound_notices ({", ".join(columns)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE
              received_at = received_at,
              id = LAST_INSERT_ID(id)
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [values[column] for column in columns])
                return int(cur.lastrowid)

    def queue_outbound_notice(self, notice: dict[str, Any]) -> int:
        columns = [
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
        values = {column: notice.get(column) for column in columns}
        values["outbox_uuid"] = values["outbox_uuid"] or str(uuid4())
        values["status"] = values["status"] or "queued"
        values["topic_kind"] = values["topic_kind"] or "test"
        values["priority"] = values["priority"] or 0
        values["max_attempts"] = values["max_attempts"] or self.settings.max_attempts
        for json_column in ["payload_json", "validation_errors", "event_ids"]:
            values[json_column] = _json_or_none(values[json_column])
        if not values["idempotency_key"]:
            values["idempotency_key"] = values["payload_sha256"]

        placeholders = ", ".join(["%s"] * len(columns))
        update_columns = [
            "updated_at = CURRENT_TIMESTAMP(6)",
            "id = LAST_INSERT_ID(id)",
            "payload_json = VALUES(payload_json)",
            "payload_sha256 = VALUES(payload_sha256)",
            "validation_status = VALUES(validation_status)",
            "validation_errors = VALUES(validation_errors)",
            "last_error = NULL",
        ]
        sql = f"""
            INSERT INTO gcn_outbound_notices ({", ".join(columns)})
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {", ".join(update_columns)}
        """
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, [values[column] for column in columns])
                return int(cur.lastrowid)

    def claim_outbound_notices(self, batch_size: int, worker_id: str) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT *
                    FROM gcn_outbound_notices
                    WHERE status = 'queued'
                      AND available_at <= CURRENT_TIMESTAMP(6)
                      AND attempts_count < max_attempts
                    ORDER BY priority DESC, created_at ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                    """,
                    (batch_size,),
                )
                rows = list(cur.fetchall())
                if not rows:
                    return []
                ids = [row["id"] for row in rows]
                cur.execute(
                    f"""
                    UPDATE gcn_outbound_notices
                    SET status = 'locked',
                        locked_by = %s,
                        locked_at = CURRENT_TIMESTAMP(6)
                    WHERE id IN ({", ".join(["%s"] * len(ids))})
                    """,
                    [worker_id, *ids],
                )
                return rows

    def recover_stale_outbound_locks(self, lock_timeout_seconds: int) -> int:
        if lock_timeout_seconds <= 0:
            raise ValueError("lock_timeout_seconds must be positive")
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE gcn_delivery_attempts AS attempt
                    JOIN gcn_outbound_notices AS notice
                      ON notice.id = attempt.outbound_notice_id
                    SET attempt.status = 'failed',
                        attempt.finished_at = CURRENT_TIMESTAMP(6),
                        attempt.error_class = 'WorkerLockExpired',
                        attempt.error_message = 'Worker lock expired before attempt completion'
                    WHERE attempt.status = 'started'
                      AND notice.status = 'locked'
                      AND notice.locked_at < TIMESTAMPADD(
                        SECOND, -%s, CURRENT_TIMESTAMP(6)
                      )
                    """,
                    (lock_timeout_seconds,),
                )
                cur.execute(
                    """
                    UPDATE gcn_outbound_notices
                    SET status = CASE
                          WHEN attempts_count + 1 >= max_attempts THEN 'failed'
                          ELSE 'queued'
                        END,
                        attempts_count = attempts_count + 1,
                        available_at = CURRENT_TIMESTAMP(6),
                        locked_by = NULL,
                        locked_at = NULL,
                        last_error = 'Recovered expired worker lock'
                    WHERE status = 'locked'
                      AND locked_at < TIMESTAMPADD(
                        SECOND, -%s, CURRENT_TIMESTAMP(6)
                      )
                    """,
                    (lock_timeout_seconds,),
                )
                recovered = int(cur.rowcount)
        if recovered:
            logger.warning("Recovered %s expired outbound lock(s)", recovered)
        return recovered

    def start_attempt(self, outbound_notice_id: int, attempt_no: int, row: dict[str, Any], dry_run: bool) -> int:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gcn_delivery_attempts (
                      outbound_notice_id, attempt_no, status, dry_run, topic,
                      producer_client_label, payload_sha256
                    )
                    VALUES (%s, %s, 'started', %s, %s, %s, %s)
                    """,
                    (
                        outbound_notice_id,
                        attempt_no,
                        dry_run,
                        row["topic"],
                        self.settings.producer_client_label,
                        row["payload_sha256"],
                    ),
                )
                return int(cur.lastrowid)

    def finish_attempt_success(
        self,
        attempt_id: int,
        outbound_notice_id: int,
        dry_run: bool,
        kafka_metadata: dict[str, Any] | None = None,
    ) -> None:
        status = "dry_run_published" if dry_run else "published"
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE gcn_delivery_attempts
                    SET status = %s,
                        finished_at = CURRENT_TIMESTAMP(6),
                        kafka_partition = %s,
                        kafka_offset = %s,
                        kafka_metadata_json = %s
                    WHERE id = %s
                    """,
                    (
                        status,
                        (kafka_metadata or {}).get("partition"),
                        (kafka_metadata or {}).get("offset"),
                        _json_or_none(kafka_metadata),
                        attempt_id,
                    ),
                )
                cur.execute(
                    """
                    UPDATE gcn_outbound_notices
                    SET status = %s,
                        attempts_count = attempts_count + 1,
                        published_at = CURRENT_TIMESTAMP(6),
                        locked_by = NULL,
                        locked_at = NULL,
                        last_error = NULL
                    WHERE id = %s
                    """,
                    (status, outbound_notice_id),
                )

    def finish_attempt_failure(
        self,
        attempt_id: int,
        outbound_notice_id: int,
        row: dict[str, Any],
        exc: Exception,
        *,
        retry_delay_seconds: float = 0.0,
    ) -> None:
        error_message = str(exc)
        next_status = "failed" if int(row["attempts_count"]) + 1 >= int(row["max_attempts"]) else "queued"
        retry_delay_microseconds = max(0, int(retry_delay_seconds * 1_000_000))
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE gcn_delivery_attempts
                    SET status = 'failed',
                        finished_at = CURRENT_TIMESTAMP(6),
                        error_class = %s,
                        error_message = %s
                    WHERE id = %s
                    """,
                    (exc.__class__.__name__, error_message, attempt_id),
                )
                cur.execute(
                    """
                    UPDATE gcn_outbound_notices
                    SET status = %s,
                        attempts_count = attempts_count + 1,
                        available_at = CASE
                          WHEN %s = 'queued' THEN TIMESTAMPADD(
                            MICROSECOND, %s, CURRENT_TIMESTAMP(6)
                          )
                          ELSE available_at
                        END,
                        locked_by = NULL,
                        locked_at = NULL,
                        last_error = %s
                    WHERE id = %s
                    """,
                    (
                        next_status,
                        next_status,
                        retry_delay_microseconds,
                        error_message,
                        outbound_notice_id,
                    ),
                )

    def record_claim_failure(
        self,
        row: dict[str, Any],
        exc: Exception,
        *,
        permanent: bool,
        retry_delay_seconds: float = 0.0,
    ) -> None:
        attempt_no = int(row["attempts_count"]) + 1
        next_status = (
            "failed"
            if permanent or attempt_no >= int(row["max_attempts"])
            else "queued"
        )
        retry_delay_microseconds = max(0, int(retry_delay_seconds * 1_000_000))
        error_message = str(exc)
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gcn_delivery_attempts (
                      outbound_notice_id, attempt_no, status, dry_run, topic,
                      producer_client_label, payload_sha256, finished_at,
                      error_class, error_message
                    )
                    VALUES (%s, %s, 'failed', %s, %s, %s, %s,
                            CURRENT_TIMESTAMP(6), %s, %s)
                    """,
                    (
                        row["id"],
                        attempt_no,
                        self.settings.dry_run,
                        row["topic"],
                        self.settings.producer_client_label,
                        row["payload_sha256"],
                        exc.__class__.__name__,
                        error_message,
                    ),
                )
                cur.execute(
                    """
                    UPDATE gcn_outbound_notices
                    SET status = %s,
                        attempts_count = attempts_count + 1,
                        available_at = CASE
                          WHEN %s = 'queued' THEN TIMESTAMPADD(
                            MICROSECOND, %s, CURRENT_TIMESTAMP(6)
                          )
                          ELSE available_at
                        END,
                        locked_by = NULL,
                        locked_at = NULL,
                        last_error = %s
                    WHERE id = %s
                    """,
                    (
                        next_status,
                        next_status,
                        retry_delay_microseconds,
                        error_message,
                        row["id"],
                    ),
                )

    def heartbeat(self, component: str, status: str, details: dict[str, Any] | None = None) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gcn_client_heartbeats (component, status, details_json)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                      updated_at = CURRENT_TIMESTAMP(6),
                      status = VALUES(status),
                      details_json = VALUES(details_json)
                    """,
                    (component, status, _json_or_none(details)),
                )

    def fetch_heartbeats(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT component, updated_at, status, details_json
                    FROM gcn_client_heartbeats
                    ORDER BY component
                    """
                )
                return list(cur.fetchall())

    def assert_healthy(self, degraded_after_seconds: float, offline_after_seconds: float) -> None:
        if degraded_after_seconds <= 0 or offline_after_seconds <= degraded_after_seconds:
            raise ValueError("heartbeat thresholds must be positive and ordered")
        rows = {row["component"]: row for row in self.fetch_heartbeats()}
        now = datetime.now(timezone.utc)
        failures: list[str] = []
        for component in ("inbound", "outbox"):
            row = rows.get(component)
            if not row:
                failures.append(f"{component}: no heartbeat")
                continue
            status = str(row.get("status") or "").lower()
            if status in {"failed", "error", "dead", "offline", "degraded", "warning", "locked"}:
                failures.append(f"{component}: status={status}")
                continue
            updated_at = row.get("updated_at")
            if updated_at is None:
                failures.append(f"{component}: missing timestamp")
                continue
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            age = (now - updated_at).total_seconds()
            if age > offline_after_seconds:
                failures.append(f"{component}: heartbeat age exceeds offline threshold")
            elif age > degraded_after_seconds:
                failures.append(f"{component}: heartbeat is degraded")
        if failures:
            raise RuntimeError("; ".join(failures))

    def assert_workers_not_stale(self, offline_after_seconds: float) -> None:
        if offline_after_seconds <= 0:
            raise ValueError("offline_after_seconds must be positive")
        rows = {row["component"]: row for row in self.fetch_heartbeats()}
        now = datetime.now(timezone.utc)
        failures: list[str] = []
        for component in ("inbound", "outbox"):
            row = rows.get(component)
            if not row:
                failures.append(f"{component}: no heartbeat")
                continue
            status = str(row.get("status") or "").lower()
            if status in {"failed", "error", "dead", "offline"}:
                failures.append(f"{component}: status={status}")
                continue
            updated_at = row.get("updated_at")
            if updated_at is None:
                failures.append(f"{component}: missing timestamp")
                continue
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)
            if (now - updated_at).total_seconds() > offline_after_seconds:
                failures.append(f"{component}: heartbeat age exceeds offline threshold")
        if failures:
            raise RuntimeError("; ".join(failures))

    def lifecycle_event(self, event: str, details: dict[str, Any] | None = None) -> None:
        with self.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO gcn_client_lifecycle_events (event, details_json)
                    VALUES (%s, %s)
                    """,
                    (event, _json_or_none(details)),
                )


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), sort_keys=True)
