from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


APPLICATION_TABLES = (
    "gcn_client_heartbeats",
    "gcn_client_lifecycle_events",
    "gcn_delivery_attempts",
    "gcn_inbound_notices",
    "gcn_outbound_notices",
)
BASE_REQUIRED_COLUMNS = (
    ("gcn_inbound_notices", "id"),
    ("gcn_inbound_notices", "notice_uuid"),
    ("gcn_inbound_notices", "raw_payload"),
    ("gcn_outbound_notices", "id"),
    ("gcn_outbound_notices", "idempotency_key"),
    ("gcn_delivery_attempts", "outbound_notice_id"),
    ("gcn_client_heartbeats", "component"),
    ("gcn_client_lifecycle_events", "event"),
)
MIGRATION_LOCK = "cosiflow_gcn_schema_migrations"
MIGRATION_LOCK_TIMEOUT_SECONDS = 30
MIGRATION_PATTERN = re.compile(r"^(?P<version>\d{3})_(?P<name>[a-z0-9_]+)\.sql$")


class SchemaMigrationError(RuntimeError):
    """Raised when the database cannot be migrated without guessing."""


@dataclass(frozen=True)
class Migration:
    version: str
    description: str
    checksum: str
    script: str


def load_migrations(directory: Path | None = None) -> list[Migration]:
    migration_dir = directory or Path(__file__).with_name("migrations")
    migrations: list[Migration] = []
    for path in sorted(migration_dir.glob("*.sql")):
        match = MIGRATION_PATTERN.fullmatch(path.name)
        if not match:
            raise SchemaMigrationError(f"Invalid migration filename: {path.name}")
        script_bytes = path.read_bytes()
        migrations.append(
            Migration(
                version=match.group("version"),
                description=match.group("name").replace("_", " "),
                checksum=hashlib.sha256(script_bytes).hexdigest(),
                script=script_bytes.decode("utf-8"),
            )
        )
    versions = [migration.version for migration in migrations]
    if not migrations or len(versions) != len(set(versions)) or versions != sorted(versions):
        raise SchemaMigrationError("Migrations must have unique ordered versions")
    return migrations


def migrate_schema(conn, database_name: str, directory: Path | None = None) -> None:
    migrations = load_migrations(directory)
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS gcn_schema_migrations (
              version VARCHAR(64) NOT NULL,
              description VARCHAR(255) NOT NULL,
              checksum CHAR(64) NOT NULL,
              applied_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
              PRIMARY KEY (version)
            )
            """
        )
        cur.execute(
            "SELECT GET_LOCK(%s, %s) AS acquired",
            (MIGRATION_LOCK, MIGRATION_LOCK_TIMEOUT_SECONDS),
        )
        if int(_row_value(cur.fetchone(), "acquired") or 0) != 1:
            raise SchemaMigrationError("Could not acquire the GCN schema migration lock")

    try:
        _run_locked_migrations(conn, database_name, migrations)
    finally:
        with conn.cursor() as cur:
            cur.execute("SELECT RELEASE_LOCK(%s)", (MIGRATION_LOCK,))


def split_sql_script(script: str) -> list[str]:
    """Split MySQL scripts while respecting quotes, comments, and DELIMITER."""
    statements: list[str] = []
    buffer: list[str] = []
    delimiter = ";"
    quote: str | None = None
    block_comment = False

    for line in script.splitlines(keepends=True):
        if quote is None and not block_comment and not "".join(buffer).strip():
            directive = re.fullmatch(r"\s*DELIMITER\s+(\S+)\s*(?:\r?\n)?", line, re.I)
            if directive:
                delimiter = directive.group(1)
                continue

        index = 0
        line_comment = False
        while index < len(line):
            if line_comment:
                break
            if block_comment:
                end = line.find("*/", index)
                if end < 0:
                    break
                index = end + 2
                block_comment = False
                continue
            if quote is not None:
                char = line[index]
                buffer.append(char)
                if char == "\\" and index + 1 < len(line):
                    buffer.append(line[index + 1])
                    index += 2
                    continue
                if char == quote:
                    if index + 1 < len(line) and line[index + 1] == quote:
                        buffer.append(line[index + 1])
                        index += 2
                        continue
                    quote = None
                index += 1
                continue

            if line.startswith(delimiter, index):
                statement = "".join(buffer).strip()
                if statement:
                    statements.append(statement)
                buffer = []
                index += len(delimiter)
                continue
            if line.startswith("/*", index):
                buffer.append(" ")
                block_comment = True
                index += 2
                continue
            if line[index] == "#" or (
                line.startswith("--", index)
                and (index + 2 == len(line) or line[index + 2].isspace())
            ):
                line_comment = True
                continue
            if line[index] in {"'", '"', "`"}:
                quote = line[index]
            buffer.append(line[index])
            index += 1

    if quote is not None or block_comment:
        raise SchemaMigrationError("Unterminated quote or comment in migration script")
    trailing = "".join(buffer).strip()
    if trailing:
        statements.append(trailing)
    return statements


def _run_locked_migrations(
    conn,
    database_name: str,
    migrations: list[Migration],
) -> None:
    applied = _applied_migrations(conn)
    _verify_applied_checksums(applied, migrations)
    schema_state = _schema_state(conn, database_name)

    if not applied:
        if schema_state == "legacy":
            _record_migration(conn, migrations[0])
            applied[migrations[0].version] = migrations[0].checksum
        elif schema_state == "review25":
            for migration in migrations[:2]:
                _record_migration(conn, migration)
                applied[migration.version] = migration.checksum
        elif schema_state != "empty":
            raise SchemaMigrationError(
                f"Unrecognized existing GCN schema state: {schema_state}"
            )
    elif "002" in applied and schema_state != "review25":
        raise SchemaMigrationError(
            "Migration 002 is recorded but the Review 25 schema contract is absent"
        )

    for migration in migrations:
        if migration.version in applied:
            continue
        if migration.version == "002" and _schema_state(conn, database_name) == "review25":
            _record_migration(conn, migration)
            applied[migration.version] = migration.checksum
            continue
        with conn.cursor() as cur:
            for statement in split_sql_script(migration.script):
                cur.execute(statement)
        _record_migration(conn, migration)
        applied[migration.version] = migration.checksum

    final_state = _schema_state(conn, database_name)
    if final_state != "review25":
        raise SchemaMigrationError(
            f"GCN schema verification failed after migrations: {final_state}"
        )


def _applied_migrations(conn) -> dict[str, str]:
    with conn.cursor() as cur:
        cur.execute("SELECT version, checksum FROM gcn_schema_migrations ORDER BY version")
        return {
            str(row["version"]): str(row["checksum"])
            for row in cur.fetchall()
        }


def _verify_applied_checksums(
    applied: dict[str, str],
    migrations: list[Migration],
) -> None:
    available = {migration.version: migration for migration in migrations}
    unknown = sorted(set(applied) - set(available))
    if unknown:
        raise SchemaMigrationError(
            f"Database contains unknown migration version(s): {', '.join(unknown)}"
        )
    for version, checksum in applied.items():
        if checksum != available[version].checksum:
            raise SchemaMigrationError(
                f"Checksum mismatch for applied migration {version}"
            )


def _record_migration(conn, migration: Migration) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO gcn_schema_migrations (version, description, checksum)
            VALUES (%s, %s, %s)
            """,
            (migration.version, migration.description, migration.checksum),
        )


def _schema_state(conn, database_name: str) -> str:
    placeholders = ", ".join(["%s"] * len(APPLICATION_TABLES))
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT COUNT(*) AS table_count
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME IN ({placeholders})
            """,
            (database_name, *APPLICATION_TABLES),
        )
        table_count = int(_row_value(cur.fetchone(), "table_count") or 0)
        if table_count == 0:
            return "empty"
        if table_count != len(APPLICATION_TABLES):
            return "partial_tables"

        required_clauses = " OR ".join(
            ["(TABLE_NAME = %s AND COLUMN_NAME = %s)"]
            * len(BASE_REQUIRED_COLUMNS)
        )
        required_params: list[str] = [database_name]
        for table_name, column_name in BASE_REQUIRED_COLUMNS:
            required_params.extend([table_name, column_name])
        cur.execute(
            f"""
            SELECT TABLE_NAME, COLUMN_NAME
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND ({required_clauses})
            """,
            required_params,
        )
        found_columns = {
            (str(row["TABLE_NAME"]), str(row["COLUMN_NAME"]))
            for row in cur.fetchall()
        }
        if not set(BASE_REQUIRED_COLUMNS).issubset(found_columns):
            return "missing_core_columns"

        cur.execute(
            """
            SELECT DATA_TYPE
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = 'gcn_inbound_notices'
              AND COLUMN_NAME = 'raw_payload'
            """,
            (database_name,),
        )
        raw_type = str(_row_value(cur.fetchone(), "DATA_TYPE") or "").lower()
        cur.execute(
            """
            SELECT COUNT(*) AS count
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = 'gcn_inbound_notices'
              AND COLUMN_NAME = 'idempotency_key'
            """,
            (database_name,),
        )
        idempotency_column = int(_row_value(cur.fetchone(), "count") or 0)
        cur.execute(
            """
            SELECT COUNT(*) AS count
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = 'gcn_inbound_notices'
              AND INDEX_NAME = 'uq_inbound_idempotency_key'
              AND NON_UNIQUE = 0
            """,
            (database_name,),
        )
        idempotency_index = int(_row_value(cur.fetchone(), "count") or 0)

    if raw_type in {"tinytext", "text", "mediumtext", "longtext"}:
        if idempotency_column == 0 and idempotency_index == 0:
            return "legacy"
        return "partial_review25"
    if raw_type in {"tinyblob", "blob", "mediumblob", "longblob"}:
        if idempotency_column == 1 and idempotency_index >= 1:
            return "review25"
        return "partial_review25"
    return "unknown_raw_payload"


def _row_value(row: Any, key: str) -> Any:
    if isinstance(row, dict):
        return row.get(key)
    if isinstance(row, (tuple, list)) and row:
        return row[0]
    return None
