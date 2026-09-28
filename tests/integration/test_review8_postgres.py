import os
import subprocess
import threading
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = Path(__file__).with_name("docker-compose.review8.yml")
PROJECT = "cosiflow-review8-test"


@unittest.skipUnless(
    os.environ.get("COSIFLOW_RUN_REVIEW8_POSTGRES_TESTS") == "1",
    "set COSIFLOW_RUN_REVIEW8_POSTGRES_TESTS=1 to run disposable PostgreSQL tests",
)
class Review8PostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.compose("up", "-d", "--wait")
        cls.compose(
            "run", "--rm", "--entrypoint", "airflow", "airflow", "db", "migrate"
        )
        cls.run_state_migration()
        cls.run_state_migration()

    @classmethod
    def tearDownClass(cls):
        cls.compose("down", "-v")

    @classmethod
    def compose(cls, *args):
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
            check=True,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        )

    @classmethod
    def psql(cls, sql):
        return subprocess.run(
            [
                "docker",
                "compose",
                "-p",
                PROJECT,
                "-f",
                str(COMPOSE_FILE),
                "exec",
                "-T",
                "postgres",
                "psql",
                "-X",
                "-v",
                "ON_ERROR_STOP=1",
                "-U",
                "review8",
                "-d",
                "review8",
                "-At",
            ],
            input=sql,
            check=True,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        ).stdout

    @classmethod
    def run_state_migration(cls):
        cls.compose(
            "run",
            "--rm",
            "--entrypoint",
            "python",
            "airflow",
            "/home/gamma/airflow/modules/cosidag_state.py",
            "migrate",
            "--sql",
            "/migrations/001_cosidag_state.sql",
        )

    def setUp(self):
        self.psql("TRUNCATE cosiflow_cosidag_state, cosiflow_cosidag_state_migration;")

    def test_two_concurrent_runs_cannot_claim_the_same_path(self):
        claim_sql = """
        BEGIN;
        INSERT INTO cosiflow_cosidag_state (
            dag_id, path, status, owner_run_id, monitoring_policy,
            claimed_at, updated_at, attempt_count
        ) VALUES (
            'science', '/data/a', 'claimed', '{owner}', 'folder-driven',
            CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1
        )
        ON CONFLICT (dag_id, path) DO UPDATE
        SET status = 'claimed', owner_run_id = EXCLUDED.owner_run_id,
            claimed_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP,
            attempt_count = cosiflow_cosidag_state.attempt_count + 1
        WHERE cosiflow_cosidag_state.status = 'failed'
           OR (cosiflow_cosidag_state.status = 'claimed'
               AND cosiflow_cosidag_state.owner_run_id = EXCLUDED.owner_run_id)
        RETURNING owner_run_id;
        SELECT pg_sleep(0.2);
        COMMIT;
        """
        outputs = []

        def claim(owner):
            outputs.append(self.psql(claim_sql.format(owner=owner)))

        threads = [
            threading.Thread(target=claim, args=("run-one",)),
            threading.Thread(target=claim, args=("run-two",)),
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        owners = self.psql(
            "SELECT owner_run_id FROM cosiflow_cosidag_state "
            "WHERE dag_id='science' AND path='/data/a';"
        ).strip().splitlines()
        self.assertEqual(len(owners), 1)
        self.assertIn(owners[0], {"run-one", "run-two"})
        self.assertEqual(sum("run-one" in value for value in outputs) + sum("run-two" in value for value in outputs), 1)

    def test_legacy_variable_migration_is_repeatable(self):
        self.compose(
            "run",
            "--rm",
            "--entrypoint",
            "airflow",
            "airflow",
            "variables",
            "set",
            "COSIDAG_PROCESSED::legacy_science",
            '["/data/a", "/data/b", "/data/a"]',
        )
        self.run_state_migration()
        self.run_state_migration()
        rows = self.psql(
            "SELECT path || ':' || status FROM cosiflow_cosidag_state "
            "WHERE dag_id='legacy_science' ORDER BY path;"
        ).strip().splitlines()
        marker = self.psql(
            "SELECT dag_id || ':' || item_count FROM cosiflow_cosidag_state_migration "
            "WHERE dag_id='legacy_science';"
        ).strip()
        self.assertEqual(rows, ["/data/a:succeeded", "/data/b:succeeded"])
        self.assertEqual(marker, "legacy_science:2")

    def test_failure_is_retryable_and_success_finalization_is_idempotent(self):
        self.psql("""
        INSERT INTO cosiflow_cosidag_state
            (dag_id, path, status, owner_run_id, claimed_at, updated_at, attempt_count)
        VALUES ('science', '/data/a', 'claimed', 'run-one', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1);
        UPDATE cosiflow_cosidag_state
        SET status='failed', completed_at=CURRENT_TIMESTAMP
        WHERE dag_id='science' AND path='/data/a' AND owner_run_id='run-one';
        INSERT INTO cosiflow_cosidag_state
            (dag_id, path, status, owner_run_id, claimed_at, updated_at, attempt_count)
        VALUES ('science', '/data/a', 'claimed', 'run-two', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1)
        ON CONFLICT (dag_id, path) DO UPDATE
        SET status='claimed', owner_run_id=EXCLUDED.owner_run_id,
            claimed_at=CURRENT_TIMESTAMP, updated_at=CURRENT_TIMESTAMP,
            attempt_count=cosiflow_cosidag_state.attempt_count + 1
        WHERE cosiflow_cosidag_state.status='failed';
        UPDATE cosiflow_cosidag_state
        SET status='succeeded', completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP)
        WHERE dag_id='science' AND path='/data/a' AND owner_run_id='run-two'
          AND status IN ('claimed', 'succeeded');
        UPDATE cosiflow_cosidag_state
        SET status='succeeded', completed_at=COALESCE(completed_at, CURRENT_TIMESTAMP)
        WHERE dag_id='science' AND path='/data/a' AND owner_run_id='run-two'
          AND status IN ('claimed', 'succeeded');
        """)
        row = self.psql(
            "SELECT status || ':' || owner_run_id || ':' || attempt_count "
            "FROM cosiflow_cosidag_state WHERE dag_id='science' AND path='/data/a';"
        ).strip()
        self.assertEqual(row, "succeeded:run-two:2")

    def test_state_api_runs_through_airflow_sqlalchemy_sessions(self):
        script = """
import sys
sys.path.insert(0, "/home/gamma/airflow/modules")
from airflow import settings
from sqlalchemy import text
from cosidag_state import (
    claim_next_path,
    claim_path,
    delete_processed_paths,
    enqueue_candidates,
    forget_stability,
    list_failed_paths,
    list_processed_paths,
    list_unavailable_paths,
    manual_retry_failed_path,
    mark_path_failed,
    mark_path_succeeded,
    observe_stability,
    queued_count,
    release_orphaned_claims,
)

candidates = [
    {"path": f"/data/queue-{index}", "observed_path": f"/data/queue-{index}",
     "monitoring_policy": "folder-driven", "monitoring_root": "/data",
     "snapshot": [1, index, index, str(index)]}
    for index in range(3)
]
assert enqueue_candidates("queue-api", candidates, 2) == 2
assert queued_count("queue-api") == 2
first = claim_next_path("queue-api", "queue-run-one")
second = claim_next_path("queue-api", "queue-run-two")
assert first and second and first["path"] != second["path"]
assert claim_next_path("queue-api", "queue-run-three") is None
assert mark_path_failed("queue-api", first["path"], "queue-run-one", "first failure", 0)
retry = claim_next_path("queue-api", "queue-run-three")
assert retry and retry["path"] == first["path"] and retry["attempt_count"] == 2
assert mark_path_failed("queue-api", first["path"], "queue-run-three", "second failure", 0)
assert list_failed_paths("queue-api")[0]["path"] == first["path"]
assert manual_retry_failed_path("queue-api", first["path"], "operator", "diagnosed", {})
manual = claim_next_path("queue-api", "queue-run-four")
assert manual and manual["path"] == first["path"] and manual["attempt_count"] == 3

assert claim_path("api", "/data/a", "run-one", "folder-driven")
assert not claim_path("api", "/data/a", "run-two", "folder-driven")
assert "/data/a" in list_unavailable_paths("api", "run-two")
assert mark_path_failed("api", "/data/a", "run-one", "test failure", 0)
assert claim_path("api", "/data/a", "run-two", "folder-driven")
assert mark_path_succeeded("api", "/data/a", "run-two")
assert mark_path_succeeded("api", "/data/a", "run-two")
assert list_processed_paths("api") == ["/data/a"]
assert delete_processed_paths("api", ["/data/a"]) == 1

assert claim_path("api", "/data/orphan", "missing-run", "folder-driven")
session = settings.Session()
try:
    session.execute(text(
        "UPDATE cosiflow_cosidag_state "
        "SET claimed_at = CURRENT_TIMESTAMP - INTERVAL '2 hours' "
        "WHERE dag_id = 'api' AND path = '/data/orphan'"
    ))
    session.commit()
finally:
    session.close()
assert release_orphaned_claims("api", 3600) == 1
assert claim_path("api", "/data/orphan", "recovery-run", "folder-driven")

identity = "/data/stable-candidate"
snapshot = [2, 100, 200, "digest"]
assert not observe_stability("stability-api", "candidate-folder", identity, snapshot, 5)
session = settings.Session()
try:
    session.execute(text(
        "UPDATE cosiflow_cosidag_stability "
        "SET stable_since = CURRENT_TIMESTAMP - INTERVAL '10 seconds' "
        "WHERE dag_id = 'stability-api' AND scope = 'candidate-folder'"
    ))
    session.commit()
finally:
    session.close()
assert observe_stability("stability-api", "candidate-folder", identity, snapshot, 5)
assert not observe_stability(
    "stability-api", "candidate-folder", identity, [2, 101, 200, "changed"], 5
)
assert forget_stability("stability-api", "candidate-folder", identity)
print("state-api-ok")
"""
        result = self.compose(
            "run",
            "--rm",
            "--entrypoint",
            "python",
            "airflow",
            "-c",
            script,
        )
        self.assertIn("state-api-ok", result.stdout)

    def test_reset_deletes_successes_but_preserves_active_claims(self):
        self.psql("""
        INSERT INTO cosiflow_cosidag_state
            (dag_id, path, status, owner_run_id, claimed_at, updated_at, attempt_count)
        VALUES
            ('science', '/data/done', 'succeeded', 'run-one', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1),
            ('science', '/data/active', 'claimed', 'run-two', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, 1);
        DELETE FROM cosiflow_cosidag_state
        WHERE dag_id='science' AND status='succeeded';
        """)
        rows = self.psql(
            "SELECT path || ':' || status FROM cosiflow_cosidag_state ORDER BY path;"
        ).strip().splitlines()
        self.assertEqual(rows, ["/data/active:claimed"])


if __name__ == "__main__":
    unittest.main()
