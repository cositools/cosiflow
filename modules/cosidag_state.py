"""Transactional processed-input state for COSIDAG.

The schema is installed explicitly during ``airflow-init``.  Runtime functions
use the Airflow metadata database transaction supplied by ``provide_session``;
they never read or rewrite a shared JSON document.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Iterable, Mapping, Optional

from airflow import settings
from airflow.models import Variable
from airflow.utils.session import provide_session
from sqlalchemy import text


LOGGER = logging.getLogger(__name__)
LEGACY_VARIABLE_PREFIX = "COSIDAG_PROCESSED::"
STATE_TABLE = "cosiflow_cosidag_state"
MIGRATION_TABLE = "cosiflow_cosidag_state_migration"
STABILITY_TABLE = "cosiflow_cosidag_stability"


def _row_exists(result) -> bool:
    return result.first() is not None


def _row_mapping(row) -> dict[str, Any] | None:
    if row is None:
        return None
    mapping = getattr(row, "_mapping", row)
    return {str(key): value for key, value in mapping.items()}


def _stability_identity_hash(identity: str) -> str:
    return hashlib.sha256(identity.encode("utf-8", errors="surrogateescape")).hexdigest()


@provide_session
def observe_stability(
    dag_id: str,
    scope: str,
    identity: str,
    snapshot: Any,
    idle_seconds: int,
    *,
    skip_if_tracked: bool = False,
    session=None,
) -> bool:
    """Atomically persist a stability window outside task-scoped XCom."""
    if idle_seconds < 0:
        raise ValueError("idle_seconds must be non-negative")
    if not scope or len(scope) > 64:
        raise ValueError("stability scope must contain between 1 and 64 characters")
    serialized_snapshot = json.dumps(snapshot, separators=(",", ":"), sort_keys=True)
    row = session.execute(
        text(
            f"""
            INSERT INTO {STABILITY_TABLE} (
                dag_id, scope, identity_hash, identity, snapshot,
                stable_since, updated_at
            ) SELECT
                :dag_id, :scope, :identity_hash, :identity, :snapshot,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            WHERE NOT :skip_if_tracked
               OR NOT EXISTS (
                    SELECT 1 FROM {STATE_TABLE}
                    WHERE dag_id=:dag_id AND path=:identity
               )
            ON CONFLICT (dag_id, scope, identity_hash) DO UPDATE
            SET identity = EXCLUDED.identity,
                snapshot = EXCLUDED.snapshot,
                stable_since = CASE
                    WHEN {STABILITY_TABLE}.identity = EXCLUDED.identity
                     AND {STABILITY_TABLE}.snapshot = EXCLUDED.snapshot
                    THEN {STABILITY_TABLE}.stable_since
                    ELSE CURRENT_TIMESTAMP
                END,
                updated_at = CURRENT_TIMESTAMP
            RETURNING (
                identity = :identity
                AND snapshot = :snapshot
                AND stable_since < CURRENT_TIMESTAMP
                AND CURRENT_TIMESTAMP - stable_since >= make_interval(secs => :idle_seconds)
            ) AS ready
            """
        ),
        {
            "dag_id": dag_id,
            "scope": scope,
            "identity_hash": _stability_identity_hash(identity),
            "identity": identity,
            "snapshot": serialized_snapshot,
            "idle_seconds": idle_seconds,
            "skip_if_tracked": skip_if_tracked,
        },
    ).first()
    return bool(row and row[0])


@provide_session
def forget_stability(
    dag_id: str,
    scope: str,
    identity: str,
    *,
    session=None,
) -> bool:
    """Remove a consumed stability observation."""
    result = session.execute(
        text(
            f"""
            DELETE FROM {STABILITY_TABLE}
            WHERE dag_id=:dag_id AND scope=:scope
              AND identity_hash=:identity_hash AND identity=:identity
            """
        ),
        {
            "dag_id": dag_id,
            "scope": scope,
            "identity_hash": _stability_identity_hash(identity),
            "identity": identity,
        },
    )
    return bool(result.rowcount)


@provide_session
def prune_stability_observations(
    stale_after_seconds: int = 86_400,
    *,
    session=None,
) -> int:
    """Bound abandoned observations; active candidates refresh updated_at."""
    if stale_after_seconds <= 0:
        raise ValueError("stability retention must be positive")
    result = session.execute(
        text(
            f"""
            DELETE FROM {STABILITY_TABLE}
            WHERE updated_at < CURRENT_TIMESTAMP
                - make_interval(secs => :stale_after_seconds)
            """
        ),
        {"stale_after_seconds": stale_after_seconds},
    )
    return int(result.rowcount or 0)


@provide_session
def queued_count(dag_id: str, *, session=None) -> int:
    """Return all queued rows, including rows waiting for their backoff."""
    row = session.execute(
        text(f"SELECT COUNT(*) FROM {STATE_TABLE} WHERE dag_id=:dag_id AND status='queued'"),
        {"dag_id": dag_id},
    ).first()
    return int(row[0] if row else 0)


@provide_session
def enqueue_candidates(
    dag_id: str,
    candidates: Iterable[Mapping[str, Any]],
    limit: int,
    *,
    session=None,
) -> int:
    """Insert up to ``limit`` canonical candidates without reviving old rows."""
    if limit <= 0:
        raise ValueError("discovery batch limit must be positive")
    statement = text(
        f"""
        INSERT INTO {STATE_TABLE} (
            dag_id, path, observed_path, status, owner_run_id,
            monitoring_policy, monitoring_root, candidate_snapshot,
            snapshot_observed_at, queued_at, next_attempt_at, queue_priority,
            claimed_at, completed_at, updated_at, attempt_count, last_error
        ) VALUES (
            :dag_id, :path, :observed_path, 'queued', NULL,
            :monitoring_policy, :monitoring_root, :candidate_snapshot,
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, :queue_priority,
            NULL, NULL, CURRENT_TIMESTAMP, 0, NULL
        )
        ON CONFLICT (dag_id, path) DO NOTHING
        RETURNING path
        """
    )
    inserted = 0
    for priority, candidate in enumerate(candidates):
        if inserted >= limit:
            break
        result = session.execute(
            statement,
            {
                "dag_id": dag_id,
                "path": str(candidate["path"]),
                "observed_path": str(candidate.get("observed_path", candidate["path"])),
                "monitoring_policy": str(candidate["monitoring_policy"]),
                "monitoring_root": str(candidate["monitoring_root"]),
                "candidate_snapshot": json.dumps(
                    candidate.get("snapshot"), separators=(",", ":")
                ),
                "queue_priority": priority,
            },
        )
        if _row_exists(result):
            inserted += 1
    return inserted


@provide_session
def claim_next_path(dag_id: str, owner_run_id: str, *, session=None) -> dict[str, Any] | None:
    """Atomically claim one eligible queue item using PostgreSQL SKIP LOCKED."""
    statement = text(
        f"""
        WITH next_item AS (
            SELECT dag_id, path
            FROM {STATE_TABLE}
            WHERE dag_id = :dag_id
              AND status = 'queued'
              AND (next_attempt_at IS NULL OR next_attempt_at <= CURRENT_TIMESTAMP)
            ORDER BY queued_at NULLS FIRST, queue_priority, path
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        )
        UPDATE {STATE_TABLE} AS state
        SET status = 'claimed',
            owner_run_id = :owner_run_id,
            claimed_at = CURRENT_TIMESTAMP,
            completed_at = NULL,
            updated_at = CURRENT_TIMESTAMP,
            attempt_count = state.attempt_count + 1,
            last_error = NULL
        FROM next_item
        WHERE state.dag_id = next_item.dag_id AND state.path = next_item.path
        RETURNING state.path, state.observed_path, state.monitoring_policy,
                  state.monitoring_root, state.candidate_snapshot,
                  state.snapshot_observed_at, state.attempt_count,
                  state.runtime_overrides
        """
    )
    return _row_mapping(
        session.execute(
            statement,
            {"dag_id": dag_id, "owner_run_id": owner_run_id},
        ).first()
    )


@provide_session
def requeue_claim(
    dag_id: str,
    path: str,
    owner_run_id: str,
    reason: str,
    *,
    snapshot: Any = None,
    retry_after_seconds: int = 0,
    restore_attempt: bool = True,
    session=None,
) -> bool:
    """Return a claimed row to the queue after temporary revalidation failure."""
    if retry_after_seconds < 0:
        raise ValueError("retry_after_seconds must be non-negative")
    result = session.execute(
        text(
            f"""
            UPDATE {STATE_TABLE}
            SET status='queued', owner_run_id=NULL, claimed_at=NULL,
                completed_at=NULL, updated_at=CURRENT_TIMESTAMP,
                next_attempt_at=CURRENT_TIMESTAMP + make_interval(secs => :delay),
                candidate_snapshot=COALESCE(:snapshot, candidate_snapshot),
                snapshot_observed_at=CASE WHEN :snapshot IS NULL
                    THEN snapshot_observed_at ELSE CURRENT_TIMESTAMP END,
                attempt_count=CASE WHEN :restore_attempt
                    THEN GREATEST(attempt_count - 1, 0) ELSE attempt_count END,
                last_error=:reason
            WHERE dag_id=:dag_id AND path=:path AND owner_run_id=:owner_run_id
              AND status='claimed'
            RETURNING path
            """
        ),
        {
            "dag_id": dag_id,
            "path": path,
            "owner_run_id": owner_run_id,
            "reason": reason[:1000],
            "delay": retry_after_seconds,
            "snapshot": None if snapshot is None else json.dumps(snapshot, separators=(",", ":")),
            "restore_attempt": restore_attempt,
        },
    )
    return _row_exists(result)


@provide_session
def discard_claim(
    dag_id: str,
    path: str,
    owner_run_id: str,
    reason: str,
    *,
    session=None,
) -> bool:
    result = session.execute(
        text(
            f"""
            UPDATE {STATE_TABLE}
            SET status='discarded', completed_at=CURRENT_TIMESTAMP,
                updated_at=CURRENT_TIMESTAMP, last_error=:reason
            WHERE dag_id=:dag_id AND path=:path AND owner_run_id=:owner_run_id
              AND status='claimed'
            RETURNING path
            """
        ),
        {"dag_id": dag_id, "path": path, "owner_run_id": owner_run_id, "reason": reason[:1000]},
    )
    return _row_exists(result)


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
    """Backward-compatible direct claim used by migrations and older callers."""
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
        WHERE {STATE_TABLE}.status IN ('failed', 'queued')
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
        SET status = CASE WHEN attempt_count < 2 THEN 'queued' ELSE 'failed' END,
            owner_run_id = CASE WHEN attempt_count < 2 THEN NULL ELSE owner_run_id END,
            completed_at = CASE WHEN attempt_count < 2 THEN NULL ELSE CURRENT_TIMESTAMP END,
            next_attempt_at = CASE WHEN attempt_count < 2
                THEN CURRENT_TIMESTAMP ELSE next_attempt_at END,
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
    retry_backoff_seconds: int = 300,
    *,
    session=None,
) -> bool:
    """Queue one automatic retry, then keep the second failure terminal."""
    if retry_backoff_seconds < 0:
        raise ValueError("retry_backoff_seconds must be non-negative")
    statement = f"""
        UPDATE {STATE_TABLE}
        SET status = CASE WHEN attempt_count < 2 THEN 'queued' ELSE 'failed' END,
            owner_run_id = CASE WHEN attempt_count < 2 THEN NULL ELSE owner_run_id END,
            claimed_at = CASE WHEN attempt_count < 2 THEN NULL ELSE claimed_at END,
            completed_at = CASE WHEN attempt_count < 2 THEN NULL ELSE CURRENT_TIMESTAMP END,
            next_attempt_at = CASE WHEN attempt_count < 2
                THEN CURRENT_TIMESTAMP + make_interval(secs => :retry_backoff_seconds)
                ELSE next_attempt_at END,
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
            "retry_backoff_seconds": retry_backoff_seconds,
        },
    )
    return _row_exists(result)


@provide_session
def list_failed_paths(dag_id: str, *, session=None) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            f"""
            SELECT path, observed_path, attempt_count, last_error, completed_at,
                   manual_retry_by, manual_retry_reason, manual_retry_at
            FROM {STATE_TABLE}
            WHERE dag_id=:dag_id AND status='failed'
            ORDER BY completed_at DESC NULLS LAST, path
            """
        ),
        {"dag_id": dag_id},
    )
    return [_row_mapping(row) for row in rows]


@provide_session
def manual_retry_failed_path(
    dag_id: str,
    path: str,
    requested_by: str,
    reason: str,
    runtime_overrides: Mapping[str, Any] | None = None,
    *,
    session=None,
) -> bool:
    """Requeue a terminal failure with visible operator audit fields."""
    if not requested_by.strip():
        raise ValueError("requested_by is required")
    if not reason.strip():
        raise ValueError("manual retry reason is required")
    result = session.execute(
        text(
            f"""
            UPDATE {STATE_TABLE}
            SET status='queued', owner_run_id=NULL, claimed_at=NULL,
                completed_at=NULL, next_attempt_at=CURRENT_TIMESTAMP,
                queued_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP,
                manual_retry_by=:requested_by,
                manual_retry_reason=:reason,
                manual_retry_at=CURRENT_TIMESTAMP,
                runtime_overrides=:runtime_overrides,
                last_error='manual retry requested'
            WHERE dag_id=:dag_id AND path=:path AND status='failed'
            RETURNING path
            """
        ),
        {
            "dag_id": dag_id,
            "path": path,
            "requested_by": requested_by[:250],
            "reason": reason[:1000],
            "runtime_overrides": json.dumps(runtime_overrides or {}, separators=(",", ":")),
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
