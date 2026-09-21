from __future__ import annotations

import sys
import threading
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[1]
GCN_CLIENT_ROOT = REPO_ROOT / "gcn-client"
if str(GCN_CLIENT_ROOT) not in sys.path:
    sys.path.insert(0, str(GCN_CLIENT_ROOT))

try:
    import pymysql  # noqa: F401
except ModuleNotFoundError:
    pymysql_stub = types.ModuleType("pymysql")
    pymysql_stub.connect = lambda **_kwargs: None
    pymysql_connections_stub = types.ModuleType("pymysql.connections")
    pymysql_connections_stub.Connection = object
    pymysql_stub.connections = pymysql_connections_stub
    sys.modules["pymysql"] = pymysql_stub
    sys.modules["pymysql.connections"] = pymysql_connections_stub

try:
    import dotenv  # noqa: F401
except ModuleNotFoundError:
    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = lambda: None
    sys.modules["dotenv"] = dotenv_stub

try:
    import jsonschema  # noqa: F401
except ModuleNotFoundError:
    validator_stub = types.ModuleType("app.schemas.validator")

    class StubCosiNoticeValidator:
        pass

    validator_stub.CosiNoticeValidator = StubCosiNoticeValidator
    validator_stub.enforce_prototype_safety = lambda *_args, **_kwargs: []
    sys.modules["app.schemas.validator"] = validator_stub

from app.db.store import NoticeStore  # noqa: E402
from app.main import _run_supervised  # noqa: E402
from app.resilience import BackoffPolicy  # noqa: E402
from app.services.inbound_service import InboundService  # noqa: E402
from app.services.outbox_service import OutboxService  # noqa: E402


class StopWorker(BaseException):
    pass


def settings(**overrides):
    values = {
        "consumer_enabled": True,
        "consumer_topics": ["gcn.test"],
        "client_id": "client",
        "client_secret": "secret",
        "consumer_group_id": "group",
        "consumer_poll_timeout": 0.1,
        "consumer_commit": True,
        "gcn_domain": None,
        "worker_backoff_initial_seconds": 1.0,
        "worker_backoff_max_seconds": 4.0,
        "worker_backoff_jitter_ratio": 0.0,
        "worker_failure_budget": 2,
        "producer_client_label": "test-worker",
        "outbox_retry_initial_seconds": 5.0,
        "outbox_retry_max_seconds": 60.0,
        "outbox_lock_timeout_seconds": 120,
        "publish_poll_seconds": 1.0,
        "publish_batch_size": 10,
        "dry_run": True,
        "producer_enabled": False,
        "topic_allowlist": ["gcn.test"],
        "require_test_topics": False,
        "outbound_topic_default": "gcn.test",
        "max_attempts": 3,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class Validator:
    def validate(self, _payload):
        return []


class Message:
    def error(self):
        return None

    def value(self):
        return b"{}"

    def timestamp(self):
        return (0, None)

    def topic(self):
        return "gcn.test"

    def partition(self):
        return 0

    def offset(self):
        return 42

    def key(self):
        return b"key"


class RecordingStore:
    def __init__(self):
        self.heartbeats = []
        self.inserted = []
        self.permanent_failures = []
        self.failure_updates = []
        self.successes = []
        self.rows = []

    def heartbeat(self, component, status, details=None):
        self.heartbeats.append((component, status, details))

    def insert_inbound_notice(self, notice):
        self.inserted.append(notice)
        return 1

    def recover_stale_outbound_locks(self, _timeout):
        return 0

    def claim_outbound_notices(self, _batch_size, _worker_id):
        rows, self.rows = self.rows, []
        return rows

    def record_claim_failure(self, row, exc, *, permanent, retry_delay_seconds=0.0):
        self.permanent_failures.append(
            (row["id"], type(exc).__name__, permanent, retry_delay_seconds)
        )

    def start_attempt(self, outbound_notice_id, attempt_no, _row, _dry_run):
        return outbound_notice_id * 100 + attempt_no

    def finish_attempt_success(self, attempt_id, outbound_notice_id, dry_run, metadata):
        self.successes.append((attempt_id, outbound_notice_id, dry_run, metadata))

    def finish_attempt_failure(
        self,
        attempt_id,
        outbound_notice_id,
        row,
        exc,
        *,
        retry_delay_seconds=0.0,
    ):
        self.failure_updates.append(
            (
                attempt_id,
                outbound_notice_id,
                row["attempts_count"],
                type(exc).__name__,
                retry_delay_seconds,
            )
        )


class BackoffTests(unittest.TestCase):
    def test_exponential_backoff_is_bounded_and_jitter_is_injectable(self):
        policy = BackoffPolicy(1.0, 4.0, 0.25)
        self.assertEqual(policy.delay(1, lambda: 0.5), 1.0)
        self.assertEqual(policy.delay(2, lambda: 0.5), 2.0)
        self.assertEqual(policy.delay(3, lambda: 0.5), 4.0)
        self.assertEqual(policy.delay(9, lambda: 0.5), 4.0)
        self.assertEqual(policy.delay(1, lambda: 0.0), 0.75)


class InboundResilienceTests(unittest.TestCase):
    def test_transient_failure_recreates_consumer_then_commits_after_store(self):
        sleeps = []
        store = RecordingStore()
        consumers = []

        class Consumer:
            def __init__(self, first):
                self.first = first
                self.calls = 0
                self.commits = []

            def subscribe(self, _topics):
                pass

            def consume(self, **_kwargs):
                self.calls += 1
                if self.first:
                    raise OSError("temporary broker failure")
                if self.calls == 1:
                    return [Message()]
                raise StopWorker()

            def commit(self, **kwargs):
                self.commits.append(kwargs)

        def factory(**_kwargs):
            consumer = Consumer(first=not consumers)
            consumers.append(consumer)
            return consumer

        service = InboundService(
            settings(),
            store,
            Validator(),
            consumer_factory=factory,
            sleep=sleeps.append,
            random_fn=lambda: 0.5,
        )
        with self.assertLogs("app.services.inbound_service", level="ERROR"):
            with self.assertRaises(StopWorker):
                service.run_forever()

        self.assertEqual(sleeps, [1.0])
        self.assertEqual(len(consumers), 2)
        self.assertEqual(len(store.inserted), 1)
        self.assertEqual(len(consumers[1].commits), 1)
        self.assertIsInstance(consumers[1].commits[0]["message"], Message)
        self.assertFalse(consumers[1].commits[0]["asynchronous"])

    def test_failure_budget_is_propagated_to_supervisor(self):
        sleeps = []
        store = RecordingStore()

        class Consumer:
            def subscribe(self, _topics):
                pass

            def consume(self, **_kwargs):
                raise ConnectionError("database unavailable")

        service = InboundService(
            settings(worker_failure_budget=2),
            store,
            Validator(),
            consumer_factory=lambda **_kwargs: Consumer(),
            sleep=sleeps.append,
            random_fn=lambda: 0.5,
        )
        with self.assertLogs("app.services.inbound_service", level="ERROR"):
            with self.assertRaises(ConnectionError):
                service.run_forever()
        self.assertEqual(sleeps, [1.0])
        self.assertEqual(store.heartbeats[-1][1], "failed")


class OutboxResilienceTests(unittest.TestCase):
    def test_malformed_row_is_terminal_and_does_not_stop_batch(self):
        store = RecordingStore()
        store.rows = [
            {
                "id": 1,
                "payload_json": [],
                "topic": "gcn.test",
                "payload_sha256": "a" * 64,
                "attempts_count": 0,
                "max_attempts": 3,
            },
            {
                "id": 2,
                "payload_json": {},
                "topic": "gcn.test",
                "payload_sha256": "b" * 64,
                "attempts_count": 0,
                "max_attempts": 3,
            },
        ]
        service = OutboxService(settings(), store, Validator())

        with self.assertLogs("app.services.outbox_service", level="WARNING"):
            self.assertEqual(service.process_once(), 2)
        self.assertEqual(store.permanent_failures, [(1, "TypeError", True, 0.0)])
        self.assertEqual(len(store.successes), 1)
        self.assertEqual(store.successes[0][1], 2)

    def test_publish_failure_schedules_persisted_backoff(self):
        store = RecordingStore()
        row = {
            "id": 7,
            "payload_json": {},
            "topic": "gcn.test",
            "payload_sha256": "c" * 64,
            "attempts_count": 0,
            "max_attempts": 3,
        }

        class Producer:
            def publish(self, _topic, _payload):
                raise OSError("temporary publish failure")

        service = OutboxService(
            settings(dry_run=False, producer_enabled=True),
            store,
            Validator(),
            random_fn=lambda: 0.5,
        )
        service._producer = Producer()
        with self.assertLogs("app.services.outbox_service", level="ERROR"):
            service._publish_row(row)

        self.assertEqual(store.failure_updates[0][-1], 5.0)
        self.assertEqual(store.failure_updates[0][3], "OSError")

    def test_worker_failure_budget_stops_retrying(self):
        sleeps = []

        class FailingStore(RecordingStore):
            def recover_stale_outbound_locks(self, _timeout):
                raise ConnectionError("mysql unavailable")

        store = FailingStore()
        service = OutboxService(
            settings(worker_failure_budget=2),
            store,
            Validator(),
            sleep=sleeps.append,
            random_fn=lambda: 0.5,
        )
        with self.assertLogs("app.services.outbox_service", level="ERROR"):
            with self.assertRaises(ConnectionError):
                service.run_forever()
        self.assertEqual(sleeps, [1.0])
        self.assertEqual(store.heartbeats[-1][1], "failed")


class SupervisorTests(unittest.TestCase):
    def test_reporting_failure_cannot_hide_worker_exit(self):
        class Store:
            def heartbeat(self, *_args, **_kwargs):
                raise ConnectionError("mysql unavailable")

            def lifecycle_event(self, *_args, **_kwargs):
                raise ConnectionError("mysql unavailable")

        def fail():
            raise OSError("worker failed")

        with self.assertLogs("app.main", level="ERROR"):
            with self.assertRaisesRegex(RuntimeError, "inbound failed"):
                _run_supervised(
                    Store(),
                    (("inbound", fail),),
                    watchdog_interval_seconds=0.01,
                )

    def test_stale_watchdog_exhaustion_exits_nonzero_path(self):
        release = threading.Event()

        class Store:
            def assert_workers_not_stale(self, _seconds):
                raise RuntimeError("stale heartbeat")

        try:
            with self.assertLogs("app.main", level="WARNING"):
                with self.assertRaisesRegex(RuntimeError, "watchdog failure budget exhausted"):
                    _run_supervised(
                        Store(),
                        (("inbound", lambda: release.wait(1)),),
                        watchdog_interval_seconds=0.005,
                        watchdog_start_grace_seconds=0.001,
                        watchdog_failure_budget=2,
                    )
        finally:
            release.set()


class HealthcheckAndComposeTests(unittest.TestCase):
    def test_healthcheck_rejects_degraded_and_stale_workers(self):
        store = object.__new__(NoticeStore)
        now = datetime.now(timezone.utc)
        store.fetch_heartbeats = lambda: [
            {"component": "inbound", "status": "degraded", "updated_at": now},
            {
                "component": "outbox",
                "status": "running",
                "updated_at": now - timedelta(seconds=100),
            },
        ]
        with self.assertRaises(RuntimeError) as raised:
            store.assert_healthy(30, 90)
        self.assertIn("inbound: status=degraded", str(raised.exception))
        self.assertIn("outbox: heartbeat age exceeds offline threshold", str(raised.exception))

    def test_compose_uses_unbounded_restart_policy_and_application_healthcheck(self):
        compose = (REPO_ROOT / "env" / "docker-compose.yaml").read_text(encoding="utf-8")
        gcn_service = compose.split("  gcn-client:", 1)[1].split("\n  postgres:", 1)[0]
        self.assertIn('command: ["python", "-m", "app.main", "run"]', gcn_service)
        self.assertIn('test: ["CMD", "python", "-m", "app.main", "healthcheck"]', gcn_service)
        self.assertIn("restart: unless-stopped", gcn_service)
        self.assertNotIn('restart: "on-failure:5"', gcn_service)


if __name__ == "__main__":
    unittest.main()
