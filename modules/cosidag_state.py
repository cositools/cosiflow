"""Transactional processed-input state for COSIDAG.

The schema is installed explicitly during ``airflow-init``.  Runtime functions
use the Airflow metadata database transaction supplied by ``provide_session``;
they never read or rewrite a shared JSON document.
"""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path
from typing import Iterable, Optional

from airflow import settings
from airflow.models import Variable
from airflow.utils.session import provide_session
from sqlalchemy import text


LOGGER = logging.getLogger(__name__)
LEGACY_VARIABLE_PREFIX = "COSIDAG_PROCESSED::"
STATE_TABLE = "cosiflow_cosidag_state"
MIGRATION_TABLE = "cosiflow_cosidag_state_migration"


def _row_exists(result) -> bool:
    return result.first() is not None


def apply_schema(sql_path: str) -> None:
    """Apply the versioned PostgreSQL schema in one transaction."""
    sql = Path(sql_path).read_text(encoding="utf-8")
    with settings.engine.begin() as connection:
        for statement in (part.strip() for part in sql.split(";")):
            if statement:
                connection.execute(text(statement))


@provide_session
def list_unavailable_paths(
    dag_id: str,
    owner_run_id: Optional[str] = None,
    *,
    session=None,
) -> set[str]:
    """Return paths that this run must not select.

    Successful paths are always unavailable. Claimed paths are unavailable to
    other runs, while the owning run may reacquire its own claim after a task
    retry that happened before XCom publication.
    """
    statement = f"""
        SELECT path
        FROM {STATE_TABLE}
        WHERE dag_id = :dag_id
          AND (
            status = 'succeeded'
            OR (
              status = 'claimed'
              AND (:owner_run_id IS NULL OR owner_run_id <> :owner_run_id)
            )
          )
    """
    rows = session.execute(
        text(statement),
        {"dag_id": dag_id, "owner_run_id": owner_run_id},
    )
    return {str(row[0]) for row in rows}


@provide_session
def claim_path(
    dag_id: str,
    path: str,
    owner_run_id: str,
    monitoring_policy: str,
    *,
    session=None,
) -> bool:
    """Atomically acquire a path, returning whether this run owns the claim."""
    statement = f"""
        INSERT INTO {STATE_TABLE} (
            dag_id, path, status, owner_run_id, monitoring_policy,
            claimed_at, completed_at, updated_at, attempt_count, last_error
        ) VALUES (
            :dag_id, :path, 'claimed', :owner_run_id, :monitoring_policy,
            CURRENT_TIMESTAMP, NULL, CURRENT_TIMESTAMP, 1, NULL
        )
        ON CONFLICT (dag_id, path) DO UPDATE
        SET status = 'claimed',
            owner_run_id = EXCLUDED.owner_run_id,
            monitoring_policy = EXCLUDED.monitoring_policy,
            claimed_at = CURRENT_TIMESTAMP,
            completed_at = NULL,
            updated_at = CURRENT_TIMESTAMP,
            attempt_count = {STATE_TABLE}.attempt_count + 1,
            last_error = NULL
        WHERE {STATE_TABLE}.status = 'failed'
           OR (
                {STATE_TABLE}.status = 'claimed'
                AND {STATE_TABLE}.owner_run_id = EXCLUDED.owner_run_id
           )
        RETURNING path
    """
    result = session.execute(
        text(statement),
        {
            "dag_id": dag_id,
            "path": path,
            "owner_run_id": owner_run_id,
            "monitoring_policy": monitoring_policy,
        },
    )
    return _row_exists(result)


@provide_session
def release_orphaned_claims(
    dag_id: str,
    stale_after_seconds: int,
    *,
    session=None,
) -> int:
    """Release stale claims only when their owning DagRun is no longer active."""
    if stale_after_seconds <= 0:
        raise ValueError("stale_after_seconds must be positive")
    statement = f"""
        UPDATE {STATE_TABLE} AS state
        SET status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP,
            last_error = 'orphaned claim recovered'
        WHERE state.dag_id = :dag_id
          AND state.status = 'claimed'
          AND state.claimed_at < (
              CURRENT_TIMESTAMP - make_interval(secs => :stale_after_seconds)
          )
          AND NOT EXISTS (
              SELECT 1
              FROM dag_run
              WHERE dag_run.dag_id = state.dag_id
                AND dag_run.run_id = state.owner_run_id
                AND dag_run.state IN ('queued', 'running')
          )
    """
    result = session.execute(
        text(statement),
        {"dag_id": dag_id, "stale_after_seconds": stale_after_seconds},
    )
    return int(result.rowcount or 0)


@provide_session
def mark_path_succeeded(
    dag_id: str,
    path: str,
    owner_run_id: str,
    *,
    session=None,
) -> bool:
    """Finalize the owning claim exactly once; repeated calls are harmless."""
    statement = f"""
        UPDATE {STATE_TABLE}
        SET status = 'succeeded',
            completed_at = COALESCE(completed_at, CURRENT_TIMESTAMP),
            updated_at = CURRENT_TIMESTAMP,
            last_error = NULL
        WHERE dag_id = :dag_id
          AND path = :path
          AND owner_run_id = :owner_run_id
          AND status IN ('claimed', 'succeeded')
        RETURNING path
    """
    result = session.execute(
        text(statement),
        {"dag_id": dag_id, "path": path, "owner_run_id": owner_run_id},
    )
    return _row_exists(result)


@provide_session
def mark_path_failed(
    dag_id: str,
    path: str,
    owner_run_id: str,
    reason: str,
    *,
    session=None,
) -> bool:
    """Make an unsuccessful input eligible for a later claim."""
    statement = f"""
        UPDATE {STATE_TABLE}
        SET status = 'failed',
            completed_at = CURRENT_TIMESTAMP,
            updated_at = CURRENT_TIMESTAMP,
            last_error = :reason
        WHERE dag_id = :dag_id
          AND path = :path
          AND owner_run_id = :owner_run_id
          AND status IN ('claimed', 'failed')
        RETURNING path
    """
    result = session.execute(
        text(statement),
        {
            "dag_id": dag_id,
            "path": path,
            "owner_run_id": owner_run_id,
            "reason": reason[:1000],
        },
    )
    return _row_exists(result)


@provide_session
def list_processed_paths(dag_id: str, *, session=None) -> list[str]:
    """Return only scientifically successful paths for the reset UI."""
    statement = f"""
        SELECT path
        FROM {STATE_TABLE}
        WHERE dag_id = :dag_id AND status = 'succeeded'
        ORDER BY path
    """
    rows = session.execute(text(statement), {"dag_id": dag_id})
    return [str(row[0]) for row in rows]


@provide_session
def reset_processed_paths(dag_id: str, *, session=None) -> int:
    """Delete successful history without stealing claims from active runs."""
    statement = f"""
        DELETE FROM {STATE_TABLE}
        WHERE dag_id = :dag_id AND status = 'succeeded'
    """
    result = session.execute(text(statement), {"dag_id": dag_id})
    return int(result.rowcount or 0)


@provide_session
def delete_processed_paths(
    dag_id: str,
    paths: Iterable[str],
    *,
    session=None,
) -> int:
    """Delete selected successful paths in the caller's transaction."""
    removed = 0
    statement = text(
        f"""
        DELETE FROM {STATE_TABLE}
        WHERE dag_id = :dag_id
          AND path = :path
          AND status = 'succeeded'
        """
    )
    for path in sorted({str(item) for item in paths}):
        result = session.execute(statement, {"dag_id": dag_id, "path": path})
        removed += int(result.rowcount or 0)
    return removed


@provide_session
def migrate_legacy_variables(*, session=None) -> dict[str, int]:
    """Import each legacy JSON list once, preserving the Variables for rollback."""
    key_rows = session.execute(
        text("SELECT key FROM variable WHERE key LIKE :prefix ORDER BY key"),
        {"prefix": f"{LEGACY_VARIABLE_PREFIX}%"},
    )
    migrated: dict[str, int] = {}
    for row in key_rows:
        key = str(row[0])
        dag_id = key[len(LEGACY_VARIABLE_PREFIX) :]
        already_done = session.execute(
            text(
                f"SELECT 1 FROM {MIGRATION_TABLE} WHERE dag_id = :dag_id"
            ),
            {"dag_id": dag_id},
        ).first()
        if already_done is not None:
            continue

        raw = Variable.get(key)
        try:
            paths = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError(f"Legacy COSIDAG state for {dag_id!r} is not valid JSON") from exc
        if not isinstance(paths, list) or not all(isinstance(item, str) for item in paths):
            raise ValueError(f"Legacy COSIDAG state for {dag_id!r} must be a list of paths")

        unique_paths = sorted(set(paths))
        for path in unique_paths:
            session.execute(
                text(
                    f"""
                    INSERT INTO {STATE_TABLE} (
                        dag_id, path, status, owner_run_id, monitoring_policy,
                        claimed_at, completed_at, updated_at, attempt_count, last_error
                    ) VALUES (
                        :dag_id, :path, 'succeeded', 'legacy-variable', NULL,
                        NULL, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1, NULL
                    )
                    ON CONFLICT (dag_id, path) DO NOTHING
                    """
                ),
                {"dag_id": dag_id, "path": path},
            )

        session.execute(
            text(
                f"""
                INSERT INTO {MIGRATION_TABLE} (
                    dag_id, source_key, item_count, migrated_at
                ) VALUES (
                    :dag_id, :source_key, :item_count, CURRENT_TIMESTAMP
                )
                ON CONFLICT (dag_id) DO NOTHING
                """
            ),
            {"dag_id": dag_id, "source_key": key, "item_count": len(unique_paths)},
        )
        migrated[dag_id] = len(unique_paths)
    return migrated


def main() -> int:
    parser = argparse.ArgumentParser(description="Manage the COSIDAG state schema")
    parser.add_argument("command", choices=["migrate"])
    parser.add_argument("--sql", required=True, help="Path to the versioned SQL migration")
    args = parser.parse_args()

    apply_schema(args.sql)
    migrated = migrate_legacy_variables()
    LOGGER.info("COSIDAG state schema ready; migrated legacy DAGs: %s", migrated)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
