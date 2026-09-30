from __future__ import annotations

import os
import re
import sys
import tempfile
import types
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGINS_ROOT = REPO_ROOT / "plugins"
GCN_CLIENT_ROOT = REPO_ROOT / "gcn-client"
for path in (PLUGINS_ROOT, GCN_CLIENT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

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

from app.config import ConfigurationError, load_settings  # noqa: E402
from app.db.migration_runner import (  # noqa: E402
    SchemaMigrationError,
    load_migrations,
    split_sql_script,
)
from app.db.store import NoticeStore  # noqa: E402
from explore_notices.query_limits import (  # noqa: E402
    MAX_PAGE_SIZE,
    MAX_RESULT_WINDOW,
    MAX_TOPIC_FILTERS,
    normalize_topics,
    query_int,
    query_text,
    validate_result_window,
)
from gcn_shared.payloads import (  # noqa: E402
    PayloadTooLargeError,
    prepare_inbound_notice,
    prepare_outbound_notice,
)
from gcn_shared.normalization import canonical_json  # noqa: E402


class Validator:
    def __init__(self):
        self.calls = 0

    def validate(self, _payload):
        self.calls += 1
        return []


class PayloadBoundaryTests(unittest.TestCase):
    def test_inbound_accepts_boundary_and_rejects_before_validation(self):
        validator = Validator()
        accepted = prepare_inbound_notice(
            b"12345678",
            topic="gcn.classic.text.TEST",
            source="manual",
            validator=validator,
            max_payload_bytes=8,
        )
        self.assertEqual(accepted["raw_payload"], b"12345678")
        with self.assertRaises(PayloadTooLargeError):
            prepare_inbound_notice(
                b"123456789",
                topic="gcn.classic.text.TEST",
                source="manual",
                validator=validator,
                max_payload_bytes=8,
            )
        self.assertEqual(validator.calls, 0)

    def test_outbound_uses_canonical_utf8_byte_boundary_before_validation(self):
        payload = {"a": "é"}
        canonical_size = len(canonical_json(payload).encode("utf-8"))
        validator = Validator()
        prepare_outbound_notice(
            payload,
            topic="gcn.notices.cosi.test.alert",
            validator=validator,
            allowlist=["gcn.notices.cosi.test.alert"],
            require_test_topics=False,
            max_payload_bytes=canonical_size,
        )
        self.assertEqual(validator.calls, 1)
        validator = Validator()
        with self.assertRaises(PayloadTooLargeError):
            prepare_outbound_notice(
                payload,
                topic="gcn.notices.cosi.test.alert",
                validator=validator,
                allowlist=["gcn.notices.cosi.test.alert"],
                require_test_topics=False,
                max_payload_bytes=canonical_size - 1,
            )
        self.assertEqual(validator.calls, 0)


class RecordingCursor:
    def __init__(self, statements):
        self.statements = statements

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.statements.append((" ".join(sql.split()), params))


class RecordingConnection:
    def __init__(self, statements):
        self.statements = statements

    def cursor(self):
        return RecordingCursor(self.statements)


class RecordingHeartbeatStore(NoticeStore):
    def __init__(self, clock):
        super().__init__(
            SimpleNamespace(heartbeat_interval_seconds=10.0),
            monotonic=lambda: clock[0],
        )
        self.statements = []

    @contextmanager
    def connection(self):
        yield RecordingConnection(self.statements)


class HeartbeatLoadTests(unittest.TestCase):
    def test_identical_heartbeats_are_coalesced_but_expiry_writes(self):
        clock = [100.0]
        store = RecordingHeartbeatStore(clock)
        self.assertTrue(store.heartbeat("inbound", "running", {"topics": ["a"]}))
        for _ in range(99):
            clock[0] += 0.09
            self.assertFalse(
                store.heartbeat("inbound", "running", {"topics": ["a"]})
            )
        clock[0] = 110.0
        self.assertTrue(store.heartbeat("inbound", "running", {"topics": ["a"]}))
        self.assertEqual(len(store.statements), 2)

    def test_status_transition_bypasses_interval(self):
        clock = [100.0]
        store = RecordingHeartbeatStore(clock)
        self.assertTrue(store.heartbeat("outbox", "running", {"dry_run": True}))
        clock[0] = 101.0
        self.assertTrue(store.heartbeat("outbox", "degraded", {"error": "db"}))
        self.assertEqual(len(store.statements), 2)


class MigrationTests(unittest.TestCase):
    def test_sql_parser_handles_semicolons_comments_and_delimiters(self):
        script = """SELECT 'a;b'; -- keep ; in comment
DELIMITER $$
CREATE PROCEDURE p() BEGIN SELECT \"x;y\"; END$$
DELIMITER ;
SELECT 3;
"""
        statements = split_sql_script(script)
        self.assertEqual(len(statements), 3)
        self.assertIn("'a;b'", statements[0])
        self.assertIn('SELECT "x;y";', statements[1])
        self.assertEqual(statements[2], "SELECT 3")

    def test_migrations_are_ordered_and_checksummed(self):
        migrations = load_migrations()
        self.assertEqual([migration.version for migration in migrations], ["001", "002"])
        self.assertTrue(all(len(migration.checksum) == 64 for migration in migrations))
        self.assertIn("CREATE TABLE IF NOT EXISTS gcn_inbound_notices", migrations[0].script)
        self.assertIn("MODIFY COLUMN raw_payload LONGBLOB", migrations[1].script)

    def test_duplicate_or_invalid_migration_names_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "not-versioned.sql"
            path.write_text("SELECT 1;", encoding="utf-8")
            with self.assertRaises(SchemaMigrationError):
                load_migrations(Path(tmp))


class QueryBoundaryTests(unittest.TestCase):
    def test_numeric_and_window_boundaries_are_rejected_not_clamped(self):
        self.assertEqual(query_int(str(MAX_PAGE_SIZE), 15, 1, MAX_PAGE_SIZE, "limit"), 100)
        with self.assertRaises(ValueError):
            query_int(str(MAX_PAGE_SIZE + 1), 15, 1, MAX_PAGE_SIZE, "limit")
        self.assertEqual(validate_result_window(100, 100), MAX_RESULT_WINDOW)
        with self.assertRaises(ValueError):
            validate_result_window(101, 100)

    def test_topic_count_and_filter_lengths_are_bounded(self):
        self.assertEqual(
            len(normalize_topics([f"topic.{index}" for index in range(MAX_TOPIC_FILTERS)])),
            MAX_TOPIC_FILTERS,
        )
        with self.assertRaises(ValueError):
            normalize_topics([f"topic.{index}" for index in range(MAX_TOPIC_FILTERS + 1)])
        self.assertEqual(query_text("x" * 255, 255, "topic"), "x" * 255)
        with self.assertRaises(ValueError):
            query_text("x" * 256, 255, "topic")

    def test_collection_query_uses_only_a_bounded_payload_preview(self):
        source = (
            REPO_ROOT / "plugins/explore_notices/explore_notices_plugin.py"
        ).read_text(encoding="utf-8")
        list_query = source.split("def _fetch_notices", 1)[1].split(
            "def _fetch_outbound_notice_count", 1
        )[0]
        self.assertIn("LEFT(raw_payload, {NOTICE_PREVIEW_BYTES})", list_query)
        self.assertNotIn("dec_deg, raw_payload", list_query)


class ConfigurationTests(unittest.TestCase):
    def test_heartbeat_interval_must_precede_degraded_threshold(self):
        env = {
            "GCN_DB_PASSWORD": "test-only",
            "GCN_CONSUMER_ENABLED": "false",
            "GCN_HEARTBEAT_INTERVAL_SECONDS": "30",
            "GCN_HEARTBEAT_DEGRADED_SECONDS": "30",
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ConfigurationError):
                load_settings()

    def test_payload_limits_must_be_positive(self):
        env = {
            "GCN_DB_PASSWORD": "test-only",
            "GCN_CONSUMER_ENABLED": "false",
            "GCN_MAX_INBOUND_PAYLOAD_BYTES": "0",
        }
        with patch.dict(os.environ, env, clear=True):
            with self.assertRaises(ConfigurationError):
                load_settings()


class PluginDiscoveryTests(unittest.TestCase):
    def test_shared_package_is_importable_but_ignored_as_an_airflow_plugin(self):
        ignore_file = PLUGINS_ROOT / ".airflowignore"
        patterns = [
            line.strip()
            for line in ignore_file.read_text(encoding="utf-8").splitlines()
            if line.strip() and not line.lstrip().startswith("#")
        ]

        self.assertTrue(
            any(re.search(pattern, "gcn_shared") for pattern in patterns),
            "gcn_shared must not be scanned as a standalone Airflow plugin",
        )
        self.assertIn("gcn_shared", sys.modules)


if __name__ == "__main__":
    unittest.main()
