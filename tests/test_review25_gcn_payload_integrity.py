from __future__ import annotations

import hashlib
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGINS_ROOT = REPO_ROOT / "plugins"
GCN_CLIENT_ROOT = REPO_ROOT / "gcn-client"
for path in (PLUGINS_ROOT, GCN_CLIENT_ROOT):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from gcn_shared.payloads import (  # noqa: E402
    prepare_inbound_notice,
    prepare_outbound_notice,
)
from gcn_shared.storage import (  # noqa: E402
    IdempotencyConflictError,
    insert_inbound_notice,
    queue_outbound_notice,
)
from gcn_shared.validation import enforce_prototype_safety, topic_is_test  # noqa: E402


class Validator:
    def validate(self, _payload):
        return []


class PayloadTests(unittest.TestCase):
    def test_invalid_utf8_bytes_are_preserved_and_do_not_collapse(self):
        first = prepare_inbound_notice(
            b"\xff",
            topic="gcn.classic.text.TEST",
            source="kafka",
            validator=Validator(),
            kafka_partition=1,
            kafka_offset=1,
        )
        second = prepare_inbound_notice(
            b"\xfe",
            topic="gcn.classic.text.TEST",
            source="kafka",
            validator=Validator(),
            kafka_partition=1,
            kafka_offset=2,
        )
        self.assertEqual(first["raw_payload"], b"\xff")
        self.assertEqual(second["raw_payload"], b"\xfe")
        self.assertEqual(first["payload_sha256"], hashlib.sha256(b"\xff").hexdigest())
        self.assertEqual(second["payload_sha256"], hashlib.sha256(b"\xfe").hexdigest())
        self.assertNotEqual(first["payload_sha256"], second["payload_sha256"])
        self.assertEqual(first["content_type"], "binary")
        self.assertEqual(first["validation_status"], "invalid")

    def test_manual_inbound_default_key_is_deterministic(self):
        kwargs = {
            "topic": "gcn.classic.text.FERMI_GBM_POS_TEST",
            "source": "manual",
            "validator": Validator(),
        }
        first = prepare_inbound_notice("NOTICE_TYPE: test", **kwargs)
        second = prepare_inbound_notice(b"NOTICE_TYPE: test", **kwargs)
        self.assertEqual(first["idempotency_key"], second["idempotency_key"])
        self.assertTrue(first["idempotency_key"].startswith("inbound:"))

    def test_voevent_is_parsed_without_dtd(self):
        xml = """<VOEvent><What>
        <Param name="Packet_Type" value="119"/>
        <Param name="Sequence_Num" value="2"/>
        <Param name="TrigID" value="42"/>
        </What><WhereWhen><ISOTime>2026-01-02T03:04:05Z</ISOTime>
        <Position2D><Value2><C1>12.5</C1><C2>-8.0</C2></Value2>
        <Error2Radius>1.2</Error2Radius></Position2D></WhereWhen></VOEvent>"""
        notice = prepare_inbound_notice(
            xml,
            topic="gcn.voevent.test",
            source="manual",
            validator=Validator(),
        )
        self.assertEqual(notice["content_type"], "voevent_xml")
        self.assertEqual(notice["packet_type"], 119)
        self.assertEqual(notice["ra_deg"], 12.5)

    def test_voevent_dtd_and_entities_fail_closed(self):
        hostile = """<!DOCTYPE x [<!ENTITY e SYSTEM "file:///etc/passwd">]>
        <VOEvent><What><Param name="TrigID" value="&e;"/></What></VOEvent>"""
        notice = prepare_inbound_notice(
            hostile,
            topic="gcn.voevent.test",
            source="manual",
            validator=Validator(),
        )
        self.assertEqual(notice["parse_status"], "raw_only")
        self.assertEqual(notice["validation_status"], "invalid")
        self.assertIn("forbidden", notice["validation_errors"][0]["message"])
        self.assertEqual(notice["raw_payload"], hostile.encode("utf-8"))

    def test_outbound_default_identity_includes_topic(self):
        payload = {"mission": "COSI", "alert_tense": "test"}
        first = prepare_outbound_notice(
            payload,
            topic="gcn.notices.cosi.test.alert",
            validator=Validator(),
            allowlist=["gcn.notices.cosi.test.alert", "gcn.notices.cosi.test.other"],
            require_test_topics=True,
        )
        second = prepare_outbound_notice(
            payload,
            topic="gcn.notices.cosi.test.other",
            validator=Validator(),
            allowlist=["gcn.notices.cosi.test.alert", "gcn.notices.cosi.test.other"],
            require_test_topics=True,
        )
        self.assertNotEqual(first["idempotency_key"], second["idempotency_key"])
        self.assertEqual(first["topic_kind"], "test")


class TopicPolicyTests(unittest.TestCase):
    def test_test_topic_requires_an_exact_dot_segment(self):
        self.assertTrue(topic_is_test("gcn.notices.cosi.test.alert"))
        self.assertFalse(topic_is_test("gcn.notices.cosi.latest.alert"))
        self.assertFalse(topic_is_test("gcn.notices.cosi.contest.alert"))

    def test_empty_allowlist_and_substring_topics_fail_closed(self):
        payload = {"mission": "COSI", "alert_tense": "test"}
        empty_errors = enforce_prototype_safety(payload, "gcn.test", [], True)
        substring_errors = enforce_prototype_safety(
            payload,
            "gcn.notices.cosi.latest.alert",
            ["gcn.notices.cosi.latest.alert"],
            True,
        )
        self.assertIn("GCN_TOPIC_ALLOWLIST is empty", empty_errors)
        self.assertTrue(any("exact dot-delimited" in error for error in substring_errors))


class FakeCursor:
    def __init__(self, existing, schema_answers=None):
        self.existing = existing
        self.schema_answers = list(schema_answers or [])
        self.executed = []
        self.lastrowid = int(existing.get("id", 1)) if isinstance(existing, dict) else 1
        self._next = existing

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.executed.append((" ".join(sql.split()), params))
        if "information_schema" in sql:
            self._next = self.schema_answers.pop(0)
        elif sql.lstrip().startswith("SELECT id"):
            self._next = self.existing

    def fetchone(self):
        return self._next


class FakeConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class StorageTests(unittest.TestCase):
    def test_identical_outbound_retry_is_noop(self):
        notice = {
            "topic": "gcn.notices.cosi.test.alert",
            "payload_json": {"mission": "COSI"},
            "payload_sha256": "a" * 64,
            "idempotency_key": "same-key",
        }
        existing = {
            "id": 7,
            "topic": notice["topic"],
            "payload_json": notice["payload_json"],
            "payload_sha256": notice["payload_sha256"],
            "idempotency_key": notice["idempotency_key"],
        }
        cursor = FakeCursor(existing)
        result = queue_outbound_notice(
            FakeConnection(cursor), notice, default_max_attempts=3
        )
        self.assertEqual(result, 7)
        insert_sql = cursor.executed[0][0]
        self.assertIn("ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id)", insert_sql)
        self.assertNotIn("payload_json = VALUES", insert_sql)
        self.assertNotIn("updated_at", insert_sql)

    def test_conflicting_outbound_retry_is_rejected(self):
        notice = {
            "topic": "gcn.notices.cosi.test.alert",
            "payload_json": {"mission": "COSI"},
            "payload_sha256": "b" * 64,
            "idempotency_key": "reused-key",
        }
        cursor = FakeCursor(
            {
                "id": 3,
                "topic": notice["topic"],
                "payload_json": {"mission": "COSI", "other": True},
                "payload_sha256": "a" * 64,
                "idempotency_key": notice["idempotency_key"],
            }
        )
        with self.assertRaises(IdempotencyConflictError):
            queue_outbound_notice(FakeConnection(cursor), notice, default_max_attempts=3)

    def test_identical_inbound_retry_returns_existing_row(self):
        notice = prepare_inbound_notice(
            b"payload",
            topic="gcn.classic.text.TEST",
            source="manual",
            validator=Validator(),
        )
        existing = {
            "id": 9,
            "topic": notice["topic"],
            "kafka_partition": None,
            "kafka_offset": None,
            "idempotency_key": notice["idempotency_key"],
            "payload_sha256": notice["payload_sha256"],
            "raw_payload": notice["raw_payload"],
        }
        self.assertEqual(insert_inbound_notice(FakeConnection(FakeCursor(existing)), notice), 9)

class SharedPathSourceTests(unittest.TestCase):
    def test_client_and_plugin_use_shared_payload_and_storage_paths(self):
        plugin = (REPO_ROOT / "plugins/explore_notices/explore_notices_plugin.py").read_text()
        inbound = (REPO_ROOT / "gcn-client/app/services/inbound_service.py").read_text()
        outbox = (REPO_ROOT / "gcn-client/app/services/outbox_service.py").read_text()
        self.assertIn("prepare_inbound_notice", plugin)
        self.assertIn("prepare_outbound_notice", plugin)
        self.assertIn("insert_inbound_notice(conn, notice)", plugin)
        self.assertIn("queue_outbound_notice(", plugin)
        self.assertNotIn("INSERT INTO gcn_inbound_notices", plugin)
        self.assertNotIn("INSERT INTO gcn_outbound_notices", plugin)
        self.assertIn("parse_notice_payload", inbound)
        self.assertIn("prepare_outbound_notice", outbox)

    def test_schema_and_cli_are_binary_safe(self):
        schema = (
            REPO_ROOT
            / "gcn-client/app/db/migrations/002_review25_inbound_integrity.sql"
        ).read_text()
        main = (REPO_ROOT / "gcn-client/app/main.py").read_text()
        service = (REPO_ROOT / "gcn-client/app/services/inbound_service.py").read_text()
        self.assertIn("raw_payload LONGBLOB NOT NULL", schema)
        self.assertIn("uq_inbound_idempotency_key", schema)
        self.assertIn("_read_bytes(args.file, settings.max_inbound_payload_bytes)", main)
        self.assertNotIn('decode("utf-8", errors="replace")', service)

    def test_defaults_cannot_publish_real_gcn_messages(self):
        compose = (REPO_ROOT / "env/docker-compose.yaml").read_text()
        self.assertIn("GCN_PRODUCER_ENABLED: ${GCN_PRODUCER_ENABLED:-false}", compose)
        self.assertIn("GCN_DRY_RUN: ${GCN_DRY_RUN:-true}", compose)


if __name__ == "__main__":
    unittest.main()
