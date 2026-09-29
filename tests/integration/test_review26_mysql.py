from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = Path(__file__).with_name("docker-compose.review12.yml")
PROJECT = "cosiflow-review26-test"


@unittest.skipUnless(
    os.environ.get("COSIFLOW_RUN_REVIEW26_MYSQL_TESTS") == "1",
    "set COSIFLOW_RUN_REVIEW26_MYSQL_TESTS=1 to run disposable Review 26 MySQL tests",
)
class Review26MySQLIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.addClassCleanup(cls.cleanup_compose)
        cls.compose("up", "-d", "--wait", "mysql")
        cls.compose("build", "client")
        cls.compose("run", "--rm", "client", "python", "-m", "app.main", "init-db")

    @classmethod
    def cleanup_compose(cls):
        cls.compose("down", "-v", "--remove-orphans", check=False)

    @classmethod
    def compose(cls, *args, check=True):
        return subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                PROJECT,
                "-f",
                str(COMPOSE_FILE),
                *args,
            ],
            check=check,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )

    @classmethod
    def mysql(cls, sql):
        return cls.compose(
            "exec",
            "-T",
            "mysql",
            "mysql",
            "-ureview12",
            "-preview12-test-only",
            "-N",
            "-B",
            "review12",
            "-e",
            sql,
        ).stdout.strip()

    @classmethod
    def init_db(cls, check=True):
        return cls.compose(
            "run",
            "--rm",
            "client",
            "python",
            "-m",
            "app.main",
            "init-db",
            check=check,
        )

    def test_empty_database_records_ordered_migrations_and_replay_is_noop(self):
        before = self.mysql(
            "SELECT CONCAT(version,':',checksum) "
            "FROM gcn_schema_migrations ORDER BY version;"
        )
        self.init_db()
        after = self.mysql(
            "SELECT CONCAT(version,':',checksum) "
            "FROM gcn_schema_migrations ORDER BY version;"
        )
        self.assertEqual(before, after)
        rows = before.splitlines()
        self.assertEqual([row.split(":", 1)[0] for row in rows], ["001", "002"])
        self.assertTrue(all(len(row.split(":", 1)[1]) == 64 for row in rows))

    def test_review25_schema_without_ledger_is_verified_and_bootstrapped(self):
        self.mysql("DROP TABLE gcn_schema_migrations;")
        self.init_db()
        self.assertEqual(
            self.mysql("SELECT GROUP_CONCAT(version ORDER BY version) FROM gcn_schema_migrations;"),
            "001,002",
        )

    def test_legacy_schema_is_upgraded_without_data_loss(self):
        self.mysql(
            "INSERT INTO gcn_inbound_notices "
            "(notice_uuid,topic,payload_sha256,raw_payload) VALUES "
            "('00000000-0000-0000-0000-000000000026','legacy.topic',"
            "REPEAT('a',64),'legacy payload'); "
            "ALTER TABLE gcn_inbound_notices DROP INDEX uq_inbound_idempotency_key; "
            "ALTER TABLE gcn_inbound_notices DROP COLUMN idempotency_key; "
            "ALTER TABLE gcn_inbound_notices MODIFY COLUMN raw_payload LONGTEXT NOT NULL; "
            "DELETE FROM gcn_schema_migrations;"
        )
        self.init_db()
        state = self.mysql(
            "SELECT CONCAT("
            "(SELECT DATA_TYPE FROM information_schema.COLUMNS "
            " WHERE TABLE_SCHEMA='review12' AND TABLE_NAME='gcn_inbound_notices' "
            " AND COLUMN_NAME='raw_payload'),':',"
            "(SELECT COUNT(*) FROM information_schema.COLUMNS "
            " WHERE TABLE_SCHEMA='review12' AND TABLE_NAME='gcn_inbound_notices' "
            " AND COLUMN_NAME='idempotency_key'),':',"
            "(SELECT COUNT(*) FROM gcn_inbound_notices "
            " WHERE notice_uuid='00000000-0000-0000-0000-000000000026'));"
        )
        self.assertEqual(state, "longblob:1:1")

    def test_checksum_drift_fails_closed(self):
        checksum = self.mysql(
            "SELECT checksum FROM gcn_schema_migrations WHERE version='002';"
        )
        self.assertRegex(checksum, r"^[0-9a-f]{64}$")
        try:
            self.mysql(
                "UPDATE gcn_schema_migrations SET checksum=REPEAT('0',64) "
                "WHERE version='002';"
            )
            result = self.init_db(check=False)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Checksum mismatch", result.stderr)
        finally:
            self.mysql(
                "UPDATE gcn_schema_migrations "
                f"SET checksum='{checksum}' WHERE version='002';"
            )

    def test_concurrent_initializers_are_serialized(self):
        source = """
import threading
from app.config import load_settings
from app.db.store import NoticeStore
barrier = threading.Barrier(2)
errors = []
def run():
    try:
        barrier.wait()
        NoticeStore(load_settings()).init_schema()
    except Exception as exc:
        errors.append(type(exc).__name__)
threads = [threading.Thread(target=run) for _ in range(2)]
for thread in threads: thread.start()
for thread in threads: thread.join()
print(errors)
"""
        result = self.compose(
            "run", "--rm", "client", "python", "-c", source
        )
        self.assertEqual(result.stdout.strip(), "[]")

    def test_unrecognized_partial_schema_fails_visibly(self):
        self.mysql(
            "DELETE FROM gcn_schema_migrations; "
            "DROP TABLE gcn_client_lifecycle_events;"
        )
        result = self.init_db(check=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("partial_tables", result.stderr)


if __name__ == "__main__":
    unittest.main()
