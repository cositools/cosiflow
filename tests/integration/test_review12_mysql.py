from __future__ import annotations

import json
import os
import subprocess
import time
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = Path(__file__).with_name("docker-compose.review12.yml")
PROJECT = "cosiflow-review12-test"


@unittest.skipUnless(
    os.environ.get("COSIFLOW_RUN_REVIEW12_MYSQL_TESTS") == "1",
    "set COSIFLOW_RUN_REVIEW12_MYSQL_TESTS=1 to run disposable MySQL/Compose tests",
)
class Review12MySQLIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.addClassCleanup(cls.cleanup_compose)
        cls.compose("up", "-d", "--wait", "mysql")
        cls.compose("run", "--rm", "client", "python", "-m", "app.main", "init-db")

    @classmethod
    def cleanup_compose(cls):
        cls.compose(
            "--profile",
            "restart",
            "down",
            "-v",
            "--remove-orphans",
            check=False,
        )

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
            "SET FOREIGN_KEY_CHECKS=1; "
            "TRUNCATE gcn_client_heartbeats;"
        )

    def test_stale_lock_is_recovered(self):
        self.mysql(
            "INSERT INTO gcn_outbound_notices "
            "(outbox_uuid,status,topic,payload_json,payload_sha256,idempotency_key,locked_by,locked_at) "
            "VALUES (UUID(),'locked','gcn.test',JSON_OBJECT(),REPEAT('a',64),'stale-lock','old-worker',"
            "CURRENT_TIMESTAMP(6) - INTERVAL 10 MINUTE);"
        )
        result = self.client_python(
            "from app.config import load_settings; "
            "from app.db.store import NoticeStore; "
            "print(NoticeStore(load_settings()).recover_stale_outbound_locks(60))"
        )
        self.assertEqual(result.stdout.strip(), "1")
        self.assertEqual(
            self.mysql(
                "SELECT CONCAT(status,':',attempts_count,':',IFNULL(locked_by,'NULL'),':',"
                "IFNULL(locked_at,'NULL')) "
                "FROM gcn_outbound_notices WHERE idempotency_key='stale-lock';"
            ),
            "queued:1:NULL:NULL",
        )

    def test_failed_publish_sets_future_availability_and_releases_lock(self):
        self.mysql(
            "INSERT INTO gcn_outbound_notices "
            "(outbox_uuid,status,topic,payload_json,payload_sha256,idempotency_key,locked_by,locked_at) "
            "VALUES (UUID(),'locked','gcn.test',JSON_OBJECT(),REPEAT('b',64),'retry-row',"
            "'worker',CURRENT_TIMESTAMP(6));"
        )
        source = """
from app.config import load_settings
from app.db.store import NoticeStore
store = NoticeStore(load_settings())
row = {
    "id": 1,
    "attempts_count": 0,
    "max_attempts": 3,
    "topic": "gcn.test",
    "payload_sha256": "b" * 64,
}
attempt_id = store.start_attempt(1, 1, row, True)
store.finish_attempt_failure(
    attempt_id, 1, row, OSError("injected transient failure"),
    retry_delay_seconds=30,
)
"""
        self.client_python(source)
        status, unlocked, delay = self.mysql(
            "SELECT status, locked_by IS NULL AND locked_at IS NULL, "
            "TIMESTAMPDIFF(SECOND,CURRENT_TIMESTAMP(6),available_at) "
            "FROM gcn_outbound_notices WHERE id=1;"
        ).split("\t")
        self.assertEqual(status, "queued")
        self.assertEqual(unlocked, "1")
        self.assertGreaterEqual(int(delay), 25)
        self.assertLessEqual(int(delay), 30)

    def test_healthcheck_rejects_a_stale_required_heartbeat(self):
        self.mysql(
            "INSERT INTO gcn_client_heartbeats(component,status,updated_at) VALUES "
            "('inbound','running',CURRENT_TIMESTAMP(6)),"
            "('outbox','running',CURRENT_TIMESTAMP(6));"
        )
        healthy = self.client_python(
            "from app.config import load_settings; "
            "from app.db.store import NoticeStore; "
            "NoticeStore(load_settings()).assert_healthy(30,90)"
        )
        self.assertEqual(healthy.returncode, 0)

        self.mysql(
            "UPDATE gcn_client_heartbeats SET updated_at=CURRENT_TIMESTAMP(6)-INTERVAL 5 MINUTE "
            "WHERE component='outbox';"
        )
        stale = self.client_python(
            "from app.config import load_settings; "
            "from app.db.store import NoticeStore; "
            "NoticeStore(load_settings()).assert_healthy(30,90)",
            check=False,
        )
        self.assertNotEqual(stale.returncode, 0)
        self.assertIn("outbox: heartbeat age exceeds offline threshold", stale.stderr)

    def test_unless_stopped_policy_restarts_a_failed_container(self):
        self.compose("--profile", "restart", "up", "-d", "restart-probe")
        try:
            container_id = self.compose(
                "--profile", "restart", "ps", "-q", "restart-probe"
            ).stdout.strip()
            self.assertTrue(container_id)
            restart_count = 0
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                inspected = subprocess.run(
                    ["docker", "inspect", container_id],
                    check=True,
                    text=True,
                    capture_output=True,
                )
                restart_count = json.loads(inspected.stdout)[0]["RestartCount"]
                if restart_count >= 1:
                    break
                time.sleep(0.25)
            self.assertGreaterEqual(restart_count, 1)
        finally:
            self.compose(
                "--profile", "restart", "stop", "restart-probe", check=False
            )


if __name__ == "__main__":
    unittest.main()
