from __future__ import annotations

import os
import subprocess
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = Path(__file__).with_name("docker-compose.review8.yml")
PROJECT = "cosiflow-review27-test"


@unittest.skipUnless(
    os.environ.get("COSIFLOW_RUN_REVIEW27_POSTGRES_TESTS") == "1",
    "set COSIFLOW_RUN_REVIEW27_POSTGRES_TESTS=1 to run disposable PostgreSQL tests",
)
class Review27PostgresIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.addClassCleanup(cls.cleanup_compose)
        cls.compose("up", "-d", "--wait", "postgres")
        cls.compose(
            "run", "--rm", "--entrypoint", "airflow", "airflow", "db", "migrate"
        )
        cls.compose(
            "run",
            "--rm",
            "--entrypoint",
            "airflow",
            "airflow",
            "users",
            "create",
            "--username",
            "admin",
            "--firstname",
            "COSI",
            "--lastname",
            "Admin",
            "--role",
            "Admin",
            "--email",
            "admin@example.org",
            "--password",
            "review27-test-only",
        )
        cls.migrate()
        cls.migrate()
        cls.seed_admin()
        cls.seed_admin()

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
    def sql(cls, statement):
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
            input=statement,
            check=True,
            cwd=REPO_ROOT,
            text=True,
            capture_output=True,
        ).stdout.strip()

    @classmethod
    def migrate(cls):
        cls.compose(
            "run",
            "--rm",
            "--entrypoint",
            "python",
            "airflow",
            "/home/gamma/airflow/modules/notification_subscriptions.py",
            "migrate",
            "--sql",
            "/migrations/002_notification_subscriptions.sql",
        )

    @classmethod
    def seed_admin(cls):
        cls.compose(
            "run",
            "--rm",
            "--entrypoint",
            "python",
            "airflow",
            "/home/gamma/airflow/modules/notification_subscriptions.py",
            "seed-admin",
        )

    def setUp(self):
        self.sql("UPDATE ab_user SET active=TRUE WHERE username='admin';")
        self.sql(
            "DELETE FROM cosiflow_notification_subscription "
            "WHERE event_type IN ('task_retry','task_success','dag_success');"
        )

    def test_migration_and_admin_seed_are_idempotent(self):
        events = self.sql(
            "SELECT event_type FROM cosiflow_notification_subscription "
            "ORDER BY event_type;"
        ).splitlines()
        self.assertEqual(events, ["dag_failure", "task_failure"])
        ledger = self.sql(
            "SELECT migration_id || ':' || length(checksum) "
            "FROM cosiflow_notification_migration;"
        )
        self.assertEqual(ledger, "002_notification_subscriptions.sql:64")

    def test_success_is_opt_in_and_inactive_users_are_excluded(self):
        admin_id = self.sql("SELECT id FROM ab_user WHERE username='admin';")
        self.sql(
            "INSERT INTO cosiflow_notification_subscription "
            "(user_id,event_type,dag_pattern,task_pattern,operator_pattern,enabled,updated_by) "
            f"VALUES ({admin_id},'task_success','science_*','reduce_*','Python*',TRUE,'test');"
        )
        source = """
import sys
sys.path.insert(0, '/home/gamma/airflow/modules')
from notification_subscriptions import resolve_notification_recipients
print(resolve_notification_recipients('task_success', {
    'dag_id': 'science_daily', 'task_id': 'reduce_events',
    'operator': 'PythonOperator'}))
"""
        result = self.compose(
            "run", "--rm", "--entrypoint", "python", "airflow", "-c", source
        )
        self.assertIn("admin@example.org", result.stdout)
        self.sql("UPDATE ab_user SET active=FALSE WHERE username='admin';")
        result = self.compose(
            "run", "--rm", "--entrypoint", "python", "airflow", "-c", source
        )
        self.assertIn("[]", result.stdout)

    def test_deleting_user_cascades_subscriptions(self):
        self.compose(
            "run",
            "--rm",
            "--entrypoint",
            "airflow",
            "airflow",
            "users",
            "create",
            "--username",
            "cascade-user",
            "--firstname",
            "Cascade",
            "--lastname",
            "Test",
            "--role",
            "Admin",
            "--email",
            "cascade@example.org",
            "--password",
            "review27-test-only",
        )
        user_id = self.sql("SELECT id FROM ab_user WHERE username='cascade-user';")
        self.sql(
            "INSERT INTO cosiflow_notification_subscription "
            "(user_id,event_type,updated_by) "
            f"VALUES ({user_id},'task_success','test');"
        )
        self.sql(f"DELETE FROM ab_user_role WHERE user_id={user_id};")
        self.sql("DELETE FROM ab_user WHERE username='cascade-user';")
        self.assertEqual(
            self.sql(
                "SELECT COUNT(*) FROM cosiflow_notification_subscription "
                f"WHERE user_id={user_id};"
            ),
            "0",
        )


if __name__ == "__main__":
    unittest.main()
