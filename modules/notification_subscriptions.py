"""Database-backed recipient subscriptions for COSIflow notifications."""

from __future__ import annotations

import argparse
import hashlib
import logging
from email.utils import parseaddr
from fnmatch import fnmatchcase
from pathlib import Path
from typing import Any, Iterable, Mapping

from airflow import settings
from airflow.utils.session import provide_session
from sqlalchemy import text


LOGGER = logging.getLogger(__name__)
SUBSCRIPTION_TABLE = "cosiflow_notification_subscription"
MIGRATION_TABLE = "cosiflow_notification_migration"
SUPPORTED_EVENTS = (
    "task_failure",
    "task_retry",
    "task_success",
    "dag_failure",
    "dag_success",
)
DEFAULT_ADMIN_EVENTS = ("task_failure", "dag_failure")
MAX_PATTERN_LENGTH = 250


def valid_email_address(value: Any) -> bool:
    """Return whether a value is a single, header-safe email address."""
    if not isinstance(value, str) or not value or "\n" in value or "\r" in value:
        return False
    display_name, address = parseaddr(value)
    return (
        not display_name
        and address == value.strip()
        and "@" in address
        and not address.startswith("@")
        and not address.endswith("@")
        and not address.rsplit("@", 1)[0].endswith(".")
    )


def normalize_event(event_type: Any) -> str:
    event = str(event_type or "").strip()
    if event not in SUPPORTED_EVENTS:
        raise ValueError(f"Unsupported notification event: {event or '<empty>'}")
    return event


def normalize_pattern(value: Any, field_name: str) -> str:
    pattern = str(value if value is not None else "*").strip() or "*"
    if "\x00" in pattern or len(pattern) > MAX_PATTERN_LENGTH:
        raise ValueError(
            f"{field_name} must contain 1 to {MAX_PATTERN_LENGTH} characters without NUL"
        )
    return pattern


def normalize_subscription(payload: Mapping[str, Any]) -> dict[str, Any]:
    try:
        user_id = int(payload.get("user_id"))
    except (TypeError, ValueError) as exc:
        raise ValueError("A valid Airflow user is required") from exc
    if user_id <= 0:
        raise ValueError("A valid Airflow user is required")
    enabled_value = payload.get("enabled", True)
    if isinstance(enabled_value, bool):
        enabled = enabled_value
    elif isinstance(enabled_value, str) and enabled_value.strip().lower() in {
        "true",
        "false",
    }:
        enabled = enabled_value.strip().lower() == "true"
    else:
        raise ValueError("Enabled must be true or false")
    return {
        "user_id": user_id,
        "event_type": normalize_event(payload.get("event_type")),
        "dag_pattern": normalize_pattern(payload.get("dag_pattern"), "DAG pattern"),
        "task_pattern": normalize_pattern(payload.get("task_pattern"), "Task pattern"),
        "operator_pattern": normalize_pattern(
            payload.get("operator_pattern"), "Operator pattern"
        ),
        "enabled": enabled,
    }


def _row_mapping(row) -> dict[str, Any]:
    mapping = getattr(row, "_mapping", row)
    return {str(key): value for key, value in mapping.items()}


def _matches(subscription: Mapping[str, Any], context: Mapping[str, Any]) -> bool:
    return all(
        fnmatchcase(str(context.get(field, "") or ""), str(subscription[pattern_field]))
        for field, pattern_field in (
            ("dag_id", "dag_pattern"),
            ("task_id", "task_pattern"),
            ("operator", "operator_pattern"),
        )
    )


@provide_session
def resolve_notification_recipients(
    event_type: str,
    context: Mapping[str, Any],
    *,
    session=None,
) -> list[str]:
    """Resolve active, valid, deduplicated recipients for one event."""
    event = normalize_event(event_type)
    rows = session.execute(
        text(
            f"""
            SELECT
                subscription.dag_pattern,
                subscription.task_pattern,
                subscription.operator_pattern,
                users.email
            FROM {SUBSCRIPTION_TABLE} AS subscription
            JOIN ab_user AS users ON users.id = subscription.user_id
            WHERE subscription.event_type = :event_type
              AND subscription.enabled = TRUE
              AND users.active = TRUE
            ORDER BY users.id, subscription.id
            """
        ),
        {"event_type": event},
    ).all()
    recipients = {
        str(row["email"]).strip()
        for row in (_row_mapping(item) for item in rows)
        if valid_email_address(row.get("email")) and _matches(row, context)
    }
    return sorted(recipients, key=str.casefold)


@provide_session
def list_notification_users(*, session=None) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            """
            SELECT id, username, first_name, last_name, email, active
            FROM ab_user
            ORDER BY lower(username), id
            """
        )
    ).all()
    return [_row_mapping(row) for row in rows]


@provide_session
def list_notification_subscriptions(*, session=None) -> list[dict[str, Any]]:
    rows = session.execute(
        text(
            f"""
            SELECT
                subscription.id,
                subscription.user_id,
                subscription.event_type,
                subscription.dag_pattern,
                subscription.task_pattern,
                subscription.operator_pattern,
                subscription.enabled,
                subscription.updated_at,
                subscription.updated_by,
                users.username,
                users.email,
                users.active
            FROM {SUBSCRIPTION_TABLE} AS subscription
            JOIN ab_user AS users ON users.id = subscription.user_id
            ORDER BY lower(users.username), subscription.event_type,
                     subscription.dag_pattern, subscription.task_pattern,
                     subscription.operator_pattern
            """
        )
    ).all()
    return [_row_mapping(row) for row in rows]


@provide_session
def save_notification_subscription(
    payload: Mapping[str, Any],
    updated_by: str,
    *,
    session=None,
) -> int:
    subscription = normalize_subscription(payload)
    user = session.execute(
        text("SELECT id, email, active FROM ab_user WHERE id = :user_id"),
        {"user_id": subscription["user_id"]},
    ).first()
    if user is None:
        raise ValueError("Unknown Airflow user")
    user_data = _row_mapping(user)
    if not user_data.get("active"):
        raise ValueError("The selected Airflow user is inactive")
    if not valid_email_address(user_data.get("email")):
        raise ValueError("The selected Airflow user has no valid email address")
    result = session.execute(
        text(
            f"""
            INSERT INTO {SUBSCRIPTION_TABLE} (
                user_id, event_type, dag_pattern, task_pattern,
                operator_pattern, enabled, updated_by
            ) VALUES (
                :user_id, :event_type, :dag_pattern, :task_pattern,
                :operator_pattern, :enabled, :updated_by
            )
            ON CONFLICT (
                user_id, event_type, dag_pattern, task_pattern, operator_pattern
            ) DO UPDATE SET
                enabled = EXCLUDED.enabled,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = EXCLUDED.updated_by
            RETURNING id
            """
        ),
        {**subscription, "updated_by": str(updated_by or "unknown")[:250]},
    ).first()
    session.commit()
    return int(result[0])


@provide_session
def set_notification_subscription_enabled(
    subscription_id: int,
    enabled: bool,
    updated_by: str,
    *,
    session=None,
) -> bool:
    result = session.execute(
        text(
            f"""
            UPDATE {SUBSCRIPTION_TABLE}
            SET enabled = :enabled,
                updated_at = CURRENT_TIMESTAMP,
                updated_by = :updated_by
            WHERE id = :subscription_id
            RETURNING id
            """
        ),
        {
            "subscription_id": int(subscription_id),
            "enabled": bool(enabled),
            "updated_by": str(updated_by or "unknown")[:250],
        },
    ).first()
    session.commit()
    return result is not None


@provide_session
def delete_notification_subscription(subscription_id: int, *, session=None) -> bool:
    result = session.execute(
        text(
            f"DELETE FROM {SUBSCRIPTION_TABLE} WHERE id = :subscription_id RETURNING id"
        ),
        {"subscription_id": int(subscription_id)},
    ).first()
    session.commit()
    return result is not None


def apply_schema(sql_path: str) -> None:
    """Apply a checksummed notification schema exactly once."""
    sql = Path(sql_path).read_text(encoding="utf-8")
    checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
    migration_id = Path(sql_path).name
    with settings.engine.begin() as connection:
        connection.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {MIGRATION_TABLE} (
                    migration_id VARCHAR(250) PRIMARY KEY,
                    checksum VARCHAR(64) NOT NULL,
                    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
        )
        existing = connection.execute(
            text(
                f"SELECT checksum FROM {MIGRATION_TABLE} WHERE migration_id = :migration_id"
            ),
            {"migration_id": migration_id},
        ).first()
        if existing:
            if existing[0] != checksum:
                raise RuntimeError(f"Applied notification migration changed: {migration_id}")
            return
        for statement in (part.strip() for part in sql.split(";")):
            if statement:
                connection.execute(text(statement))
        connection.execute(
            text(
                f"""
                INSERT INTO {MIGRATION_TABLE} (migration_id, checksum)
                VALUES (:migration_id, :checksum)
                """
            ),
            {"migration_id": migration_id, "checksum": checksum},
        )


@provide_session
def seed_admin_subscriptions(*, session=None) -> int:
    """Subscribe active Admin users to task and DAG failures idempotently."""
    result = session.execute(
        text(
            f"""
            INSERT INTO {SUBSCRIPTION_TABLE} (
                user_id, event_type, dag_pattern, task_pattern,
                operator_pattern, enabled, updated_by
            )
            SELECT users.id, events.event_type, '*', '*', '*', TRUE, 'airflow-init'
            FROM ab_user AS users
            JOIN ab_user_role AS user_role ON user_role.user_id = users.id
            JOIN ab_role AS roles ON roles.id = user_role.role_id
            CROSS JOIN (
                VALUES ('task_failure'), ('dag_failure')
            ) AS events(event_type)
            WHERE roles.name = 'Admin' AND users.active = TRUE
            ON CONFLICT (
                user_id, event_type, dag_pattern, task_pattern, operator_pattern
            ) DO NOTHING
            RETURNING id
            """
        )
    ).all()
    session.commit()
    return len(result)


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    migrate = subparsers.add_parser("migrate")
    migrate.add_argument("--sql", required=True)
    subparsers.add_parser("seed-admin")
    args = parser.parse_args()
    if args.command == "migrate":
        apply_schema(args.sql)
        LOGGER.info("Notification subscription schema is current")
    elif args.command == "seed-admin":
        count = seed_admin_subscriptions()
        LOGGER.info("Seeded %s Admin notification subscription(s)", count)


if __name__ == "__main__":
    main()
