from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = Path(__file__).with_name("docker-compose.review12.yml")
PROJECT = "cosiflow-review25-test"


@unittest.skipUnless(
    os.environ.get("COSIFLOW_RUN_REVIEW25_MYSQL_TESTS") == "1",
    "set COSIFLOW_RUN_REVIEW25_MYSQL_TESTS=1 to run disposable Review 25 MySQL tests",
)
class Review25MySQLIntegrationTests(unittest.TestCase):
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
    def client_python(cls, source, check=True):
        return cls.compose(
            "run",
            "--rm",
            "client",
            "python",
            "-c",
            source,
            check=check,
        )

    def setUp(self):
        self.mysql(
            "TRUNCATE gcn_delivery_attempts; "
            "SET FOREIGN_KEY_CHECKS=0; "
            "TRUNCATE gcn_outbound_notices; "
            "TRUNCATE gcn_inbound_notices; "
            "SET FOREIGN_KEY_CHECKS=1;"
        )

    def test_outbox_key_is_immutable_for_identical_and_conflicting_retries(self):
        source = """
from app.config import load_settings
from app.db.store import NoticeStore
store = NoticeStore(load_settings())
notice = {
    "status": "queued",
    "topic": "gcn.notices.cosi.test.alert",
    "topic_kind": "test",
    "payload_json": {"value": 1},
    "payload_sha256": "a" * 64,
    "validation_status": "valid",
    "idempotency_key": "immutable-key",
}
print(store.queue_outbound_notice(notice))
print(store.queue_outbound_notice(notice))
"""
        result = self.client_python(source)
        self.assertEqual(result.stdout.strip().splitlines(), ["1", "1"])
        self.mysql(
            "UPDATE gcn_outbound_notices SET status='published', "
            "published_at=CURRENT_TIMESTAMP(6) WHERE id=1;"
        )
        before = self.mysql(
            "SELECT status,payload_sha256,JSON_EXTRACT(payload_json,'$.value'),"
            "DATE_FORMAT(updated_at,'%Y-%m-%d %H:%i:%s.%f') "
            "FROM gcn_outbound_notices WHERE id=1;"
        )
        conflict = self.client_python(
            source.replace('"a" * 64', '"b" * 64').replace('{"value": 1}', '{"value": 2}'),
            check=False,
        )
        self.assertNotEqual(conflict.returncode, 0)
        self.assertIn("IdempotencyConflictError", conflict.stderr)
        after = self.mysql(
            "SELECT status,payload_sha256,JSON_EXTRACT(payload_json,'$.value'),"
            "DATE_FORMAT(updated_at,'%Y-%m-%d %H:%i:%s.%f') "
            "FROM gcn_outbound_notices WHERE id=1;"
        )
        self.assertEqual(after, before)

    def test_binary_inbound_storage_and_manual_deduplication(self):
        source = r'''
from app.config import load_settings
from app.db.store import NoticeStore
from gcn_shared.payloads import prepare_inbound_notice
class Validator:
    def validate(self, payload): return []
store = NoticeStore(load_settings())
def add(value):
    return store.insert_inbound_notice(prepare_inbound_notice(
        value, topic="gcn.classic.text.FERMI_GBM_POS_TEST",
        source="manual", validator=Validator()))
print(add(b"\xff"))
print(add(b"\xff"))
print(add(b"\xfe"))
'''
        result = self.client_python(source)
        ids = result.stdout.strip().splitlines()
        self.assertEqual(ids[0], ids[1])
        self.assertNotEqual(ids[0], ids[2])
        rows = self.mysql(
            "SELECT id,HEX(raw_payload),payload_sha256,idempotency_key "
            "FROM gcn_inbound_notices ORDER BY id;"
        ).splitlines()
        self.assertEqual(len(rows), 2)
        self.assertIn("\tFF\t", rows[0])
        self.assertIn("\tFE\t", rows[1])

    def test_concurrent_identical_outbox_inserts_converge_on_one_row(self):
        source = """
import threading
from app.config import load_settings
from app.db.store import NoticeStore
notice = {
    "status": "queued", "topic": "gcn.notices.cosi.test.alert",
    "topic_kind": "test", "payload_json": {"value": 1},
    "payload_sha256": "c" * 64, "validation_status": "valid",
    "idempotency_key": "concurrent-key",
}
barrier = threading.Barrier(2)
results = []
def run():
    barrier.wait()
    results.append(NoticeStore(load_settings()).queue_outbound_notice(dict(notice)))
threads = [threading.Thread(target=run) for _ in range(2)]
for thread in threads: thread.start()
for thread in threads: thread.join()
print(sorted(results))
"""
        result = self.client_python(source)
        ids = result.stdout.strip().strip("[]").split(", ")
        self.assertEqual(len(ids), 2)
        self.assertEqual(ids[0], ids[1])
        self.assertEqual(
            self.mysql(
                "SELECT COUNT(*) FROM gcn_outbound_notices "
                "WHERE idempotency_key='concurrent-key';"
            ),
            "1",
        )

    def test_legacy_inbound_schema_is_migrated_idempotently(self):
        self.mysql(
            "ALTER TABLE gcn_inbound_notices DROP INDEX uq_inbound_idempotency_key; "
            "ALTER TABLE gcn_inbound_notices DROP COLUMN idempotency_key; "
            "ALTER TABLE gcn_inbound_notices MODIFY COLUMN raw_payload LONGTEXT NOT NULL; "
            "DELETE FROM gcn_schema_migrations;"
        )
        self.compose("run", "--rm", "client", "python", "-m", "app.main", "init-db")
        self.compose("run", "--rm", "client", "python", "-m", "app.main", "init-db")
        state = self.mysql(
            "SELECT CONCAT("
            "(SELECT DATA_TYPE FROM information_schema.COLUMNS "
            " WHERE TABLE_SCHEMA='review12' AND TABLE_NAME='gcn_inbound_notices' "
            " AND COLUMN_NAME='raw_payload'),':',"
            "(SELECT COUNT(*) FROM information_schema.COLUMNS "
            " WHERE TABLE_SCHEMA='review12' AND TABLE_NAME='gcn_inbound_notices' "
            " AND COLUMN_NAME='idempotency_key'),':',"
            "(SELECT COUNT(*) FROM information_schema.STATISTICS "
            " WHERE TABLE_SCHEMA='review12' AND TABLE_NAME='gcn_inbound_notices' "
            " AND INDEX_NAME='uq_inbound_idempotency_key'));"
        )
        self.assertEqual(state, "longblob:1:1")


if __name__ == "__main__":
    unittest.main()
