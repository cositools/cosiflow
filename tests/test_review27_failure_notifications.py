from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from types import ModuleType, SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]


def load_module(path, name):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def identity_session(function):
    return function


airflow = ModuleType("airflow")
airflow.settings = SimpleNamespace(engine=None)
airflow_utils = ModuleType("airflow.utils")
airflow_session = ModuleType("airflow.utils.session")
airflow_session.provide_session = identity_session
sqlalchemy = ModuleType("sqlalchemy")
sqlalchemy.text = lambda statement: statement

with patch.dict(
    sys.modules,
    {
        "airflow": airflow,
        "airflow.utils": airflow_utils,
        "airflow.utils.session": airflow_session,
        "sqlalchemy": sqlalchemy,
    },
):
    SUBSCRIPTIONS = load_module(
        "modules/notification_subscriptions.py", "review27_subscriptions_under_test"
    )


class FakeConf:
    def __init__(self):
        self.base = None

    def get(self, section, key):
        if key == "base_log_folder":
            return str(self.base)
        if key == "log_filename_template":
            return "ignored"
        raise KeyError((section, key))


FAKE_CONF = FakeConf()


class FakeFileTaskHandler:
    last_try_number = None

    def __init__(self, base, template):
        self.base = base
        self.template = template

    def _render_filename(self, task_instance, try_number):
        self.__class__.last_try_number = try_number
        map_segment = (
            f"/map_index={task_instance.map_index}"
            if task_instance.map_index >= 0
            else ""
        )
        return (
            f"dag_id={task_instance.dag_id}/run_id={task_instance.run_id}/"
            f"task_id={task_instance.task_id}{map_segment}/attempt={try_number}.log"
        )


airflow_configuration = ModuleType("airflow.configuration")
airflow_configuration.conf = FAKE_CONF
airflow_email = ModuleType("airflow.utils.email")
airflow_email.send_email = lambda **kwargs: None
airflow_log = ModuleType("airflow.utils.log")
airflow_file_handler = ModuleType("airflow.utils.log.file_task_handler")
airflow_file_handler.FileTaskHandler = FakeFileTaskHandler

with patch.dict(
    sys.modules,
    {
        "airflow": airflow,
        "airflow.configuration": airflow_configuration,
        "airflow.utils": airflow_utils,
        "airflow.utils.email": airflow_email,
        "airflow.utils.log": airflow_log,
        "airflow.utils.log.file_task_handler": airflow_file_handler,
        "notification_subscriptions": SUBSCRIPTIONS,
    },
):
    CALLBACK = load_module(
        "callbacks/on_failure_callback.py", "review27_callback_under_test"
    )


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return list(self.rows)

    def first(self):
        return self.rows[0] if self.rows else None


class FakeSession:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    def execute(self, statement, params=None):
        self.statements.append((str(statement), params or {}))
        return FakeResult(self.rows)


class Review27SubscriptionTests(unittest.TestCase):
    def test_email_validation_rejects_headers_and_malformed_values(self):
        self.assertTrue(SUBSCRIPTIONS.valid_email_address("admin@example.org"))
        for value in (None, "", "Admin <admin@example.org>", "a@", "@b", "a@b\nBcc:x@y"):
            with self.subTest(value=value):
                self.assertFalse(SUBSCRIPTIONS.valid_email_address(value))

    def test_subscription_validation_defaults_patterns_and_rejects_unknown_events(self):
        normalized = SUBSCRIPTIONS.normalize_subscription(
            {"user_id": "4", "event_type": "task_success", "enabled": False}
        )
        self.assertEqual(normalized["dag_pattern"], "*")
        self.assertEqual(normalized["task_pattern"], "*")
        self.assertFalse(normalized["enabled"])
        self.assertFalse(
            SUBSCRIPTIONS.normalize_subscription(
                {"user_id": 4, "event_type": "task_success", "enabled": "false"}
            )["enabled"]
        )
        with self.assertRaisesRegex(ValueError, "Enabled"):
            SUBSCRIPTIONS.normalize_subscription(
                {"user_id": 4, "event_type": "task_success", "enabled": "yes"}
            )
        with self.assertRaisesRegex(ValueError, "Unsupported"):
            SUBSCRIPTIONS.normalize_event("task_scheduled")

    def test_cosidag_owns_one_callback_path_for_each_supported_event(self):
        source = (REPO_ROOT / "modules/cosidag.py").read_text()
        self.assertIn('"email_on_failure": False', source)
        self.assertIn('"email_on_retry": False', source)
        self.assertIn('"on_failure_callback": notify_email', source)
        self.assertIn('"on_retry_callback": notify_retry', source)
        self.assertIn('"on_success_callback": notify_success', source)
        self.assertIn('kwargs.setdefault("on_failure_callback", notify_dag_failure)', source)
        self.assertIn('kwargs.setdefault("on_success_callback", notify_dag_success)', source)

    def test_recipient_resolution_filters_patterns_validates_and_deduplicates(self):
        rows = [
            {"dag_pattern": "science_*", "task_pattern": "reduce_*", "operator_pattern": "Python*", "email": "ops@example.org"},
            {"dag_pattern": "science_*", "task_pattern": "*", "operator_pattern": "*", "email": "ops@example.org"},
            {"dag_pattern": "other", "task_pattern": "*", "operator_pattern": "*", "email": "other@example.org"},
            {"dag_pattern": "*", "task_pattern": "*", "operator_pattern": "*", "email": "bad\n@example.org"},
        ]
        recipients = SUBSCRIPTIONS.resolve_notification_recipients(
            "task_failure",
            {"dag_id": "science_daily", "task_id": "reduce_events", "operator": "PythonOperator"},
            session=FakeSession(rows),
        )
        self.assertEqual(recipients, ["ops@example.org"])

    def test_migration_defines_supported_events_foreign_key_and_unique_rule(self):
        source = (REPO_ROOT / "env/migrations/002_notification_subscriptions.sql").read_text()
        self.assertIn("REFERENCES ab_user(id) ON DELETE CASCADE", source)
        self.assertIn("'task_success'", source)
        self.assertIn("'dag_success'", source)
        self.assertIn("CONSTRAINT cosiflow_notification_subscription_unique UNIQUE", source)


class Review27CallbackTests(unittest.TestCase):
    def task_instance(self, **overrides):
        values = {
            "dag_id": "science_<dag>",
            "task_id": "reduce_&_publish",
            "run_id": "manual__<run>",
            "try_number": 3,
            "map_index": 7,
            "operator": "PythonOperator",
            "execution_date": "2026-09-29T12:00:00+00:00",
            "log_url": "https://airflow.example.org/dags/science/log?x=1&y=2",
            "task": None,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_log_path_uses_current_attempt_and_map_index(self):
        ti = self.task_instance()
        with tempfile.TemporaryDirectory() as directory:
            FAKE_CONF.base = Path(directory)
            path = CALLBACK._task_log_path(ti)
        self.assertEqual(FakeFileTaskHandler.last_try_number, 3)
        self.assertIn("map_index=7", str(path))
        self.assertTrue(str(path).endswith("attempt=3.log"))

    def test_log_tail_is_bounded_and_decodes_invalid_utf8(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "task.log"
            path.write_bytes(b"old line\n" + b"x" * 200 + b"\nlast\xffline\n")
            result = CALLBACK._tail_text(path, max_lines=2, max_bytes=64)
        self.assertIn("earlier log content omitted", result)
        self.assertIn("last�line", result)
        self.assertNotIn("old line", result)

    def test_rendering_escapes_every_dynamic_value_and_preserves_valid_url(self):
        snapshot = CALLBACK._context_snapshot(
            {"task_instance": self.task_instance(), "exception": "<script>&\"boom\""}
        )
        with patch.object(CALLBACK, "_log_preview", return_value="<img src=x>&lt;"):
            subject, body = CALLBACK._render_email(snapshot, "task_failure")
        self.assertNotIn("\n", subject)
        self.assertNotIn("<script>", body)
        self.assertIn("&lt;script&gt;&amp;&quot;boom&quot;", body)
        self.assertIn("&lt;img src=x&gt;&amp;lt;", body)
        self.assertIn("x=1&amp;y=2", body)
        self.assertIn("Attempt:</strong></td><td>3", body)
        self.assertIn("Map index:</strong></td><td>7", body)

    def test_unsafe_url_is_omitted(self):
        ti = self.task_instance(log_url="javascript:alert(1)")
        snapshot = CALLBACK._context_snapshot({"task_instance": ti})
        with (
            patch.object(CALLBACK, "_log_preview", return_value="preview"),
            patch.object(CALLBACK.LOGGER, "exception"),
        ):
            _, body = CALLBACK._render_email(snapshot, "task_failure")
        self.assertNotIn("href=", body)

    def test_callback_never_raises_when_primary_task_and_notifier_both_fail(self):
        context = {
            "task_instance": self.task_instance(),
            "exception": RuntimeError("primary task failed"),
        }
        with (
            patch.object(
                CALLBACK,
                "resolve_notification_recipients",
                side_effect=RuntimeError("database unavailable"),
            ),
            patch.dict(
                CALLBACK.os.environ,
                {"COSIFLOW_ALERT_FALLBACK_RECIPIENTS": "admin@example.org"},
            ),
            patch.object(CALLBACK, "_render_email", return_value=("subject", "body")),
            patch.object(CALLBACK, "send_email", side_effect=RuntimeError("SMTP down")),
            patch.object(CALLBACK.LOGGER, "exception"),
        ):
            self.assertIsNone(CALLBACK.notify_email(context))
        self.assertIsInstance(context["exception"], RuntimeError)
        self.assertEqual(str(context["exception"]), "primary task failed")

    def test_success_is_supported_but_not_seeded_by_default(self):
        migration_module = (REPO_ROOT / "modules/notification_subscriptions.py").read_text()
        self.assertIn('"task_success"', migration_module)
        self.assertIn('DEFAULT_ADMIN_EVENTS = ("task_failure", "dag_failure")', migration_module)
        self.assertNotIn("task_scheduled", migration_module)

    def test_personal_recipient_file_is_removed(self):
        self.assertFalse((REPO_ROOT / "env/alert_users.yaml").exists())
        dockerfile = (REPO_ROOT / "env/Dockerfile.airflow").read_text()
        self.assertNotIn("COPY alert_users.yaml", dockerfile)


if __name__ == "__main__":
    unittest.main()
