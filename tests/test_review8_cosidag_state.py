import importlib.util
import json
import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULES = REPO_ROOT / "modules"


def load_runtime_module():
    spec = importlib.util.spec_from_file_location(
        "review8_cosidag_runtime",
        MODULES / "cosidag_runtime.py",
    )
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


class RetriggerRuntimeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.runtime = load_runtime_module()

    def test_run_id_is_stable_for_retry_of_same_source_run(self):
        first = self.runtime.automatic_retrigger_run_id("science", "manual__one")
        second = self.runtime.automatic_retrigger_run_id("science", "manual__one")
        self.assertEqual(first, second)

    def test_two_source_runs_receive_distinct_successor_ids(self):
        first = self.runtime.automatic_retrigger_run_id("science", "manual__one")
        second = self.runtime.automatic_retrigger_run_id("science", "manual__two")
        self.assertNotEqual(first, second)
        self.assertLessEqual(len(first), 250)

    def test_successor_conf_is_a_detached_mapping_with_incremented_count(self):
        source = {"auto_retrig": True, "retrig_run_count": "4"}
        successor = self.runtime.build_successor_conf(source)
        self.assertEqual(successor["retrig_run_count"], 5)
        self.assertEqual(source["retrig_run_count"], "4")

    def test_legacy_json_string_conf_is_normalized_for_compatibility(self):
        successor = self.runtime.build_successor_conf(
            json.dumps({"retrig_run_count": 1, "policy": "file-driven"})
        )
        self.assertEqual(successor["retrig_run_count"], 2)
        self.assertEqual(successor["policy"], "file-driven")

    def test_non_mapping_conf_and_invalid_counter_are_rejected(self):
        with self.assertRaises(ValueError):
            self.runtime.build_successor_conf(["not", "a", "mapping"])
        with self.assertRaises(ValueError):
            self.runtime.build_successor_conf({"retrig_run_count": "many"})


class TransactionalStateContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cosidag_source = (MODULES / "cosidag.py").read_text(encoding="utf-8")
        cls.state_source = (MODULES / "cosidag_state.py").read_text(encoding="utf-8")
        cls.plugin_source = (
            REPO_ROOT / "plugins/reset_cosidag_link/reset_cosidag_plugin.py"
        ).read_text(encoding="utf-8")
        cls.entrypoint_source = (
            REPO_ROOT / "env/entrypoint-airflow.sh"
        ).read_text(encoding="utf-8")
        cls.dockerfile_source = (
            REPO_ROOT / "env/Dockerfile.airflow"
        ).read_text(encoding="utf-8")
        cls.schema = (
            REPO_ROOT / "env/migrations/001_cosidag_state.sql"
        ).read_text(encoding="utf-8")

    def test_schema_has_per_path_uniqueness_and_state_constraints(self):
        self.assertIn("PRIMARY KEY (dag_id, path)", self.schema)
        self.assertIn("'claimed', 'succeeded', 'failed'", self.schema)
        self.assertIn("status <> 'claimed' OR owner_run_id IS NOT NULL", self.schema)

    def test_claim_is_one_atomic_insert_with_conflict_arbitration(self):
        self.assertIn("ON CONFLICT (dag_id, path) DO UPDATE", self.state_source)
        self.assertIn("RETURNING path", self.state_source)
        self.assertIn("owner_run_id = EXCLUDED.owner_run_id", self.state_source)

    def test_success_requires_the_owning_claim_and_is_idempotent(self):
        self.assertIn("status IN ('claimed', 'succeeded')", self.state_source)
        self.assertIn("AND owner_run_id = :owner_run_id", self.state_source)
        self.assertIn("completed_at = COALESCE", self.state_source)

    def test_sensor_claims_before_xcom_and_never_writes_legacy_variable(self):
        claim_position = self.cosidag_source.index("if not claim_path(")
        xcom_position = self.cosidag_source.index(
            'ti.xcom_push(key="detected_path", value=new_path)'
        )
        self.assertLess(claim_position, xcom_position)
        self.assertNotIn("def _save_processed_set", self.cosidag_source)
        self.assertNotIn("def _load_processed_set", self.cosidag_source)

    def test_finalizer_is_all_done_and_preserves_failure_semantics(self):
        self.assertIn('task_id="finalize_cosidag_state"', self.cosidag_source)
        self.assertIn("trigger_rule=TriggerRule.ALL_DONE", self.cosidag_source)
        self.assertIn("mark_path_succeeded", self.cosidag_source)
        self.assertIn("mark_path_failed", self.cosidag_source)
        self.assertIn("Required COSIDAG work did not succeed", self.cosidag_source)

    def test_retrigger_id_and_conf_are_assigned_inside_execute(self):
        execute = self.cosidag_source[
            self.cosidag_source.index("class ConditionalTriggerDagRunOperator") :
            self.cosidag_source.index("# ---- COSIDAG")
        ]
        self.assertIn("self.trigger_run_id = automatic_retrigger_run_id", execute)
        self.assertIn("self.conf = build_successor_conf(conf)", execute)
        self.assertIn("except DagRunAlreadyExists", execute)
        self.assertNotIn("dag_run.conf or {}", self.cosidag_source)

    def test_reset_plugin_uses_transactional_store_and_preserves_authorization(self):
        self.assertNotIn("Variable.get", self.plugin_source)
        self.assertNotIn("Variable.set", self.plugin_source)
        self.assertIn("reset_processed_paths", self.plugin_source)
        self.assertIn("delete_state_paths", self.plugin_source)
        self.assertIn("@require_cosiflow_permission(ACTION_EDIT, COSIDAG_STATE)", self.plugin_source)

    def test_init_applies_schema_before_runtime_starts(self):
        migrate_position = self.entrypoint_source.index("airflow db migrate")
        state_position = self.entrypoint_source.index("cosidag_state.py migrate")
        rbac_position = self.entrypoint_source.index("configure_rbac.py")
        self.assertLess(migrate_position, state_position)
        self.assertLess(state_position, rbac_position)
        self.assertIn("COPY migrations /home/gamma/migrations/", self.dockerfile_source)
        self.assertIn(
            "/home/gamma/migrations/001_cosidag_state.sql",
            self.entrypoint_source,
        )

    def test_legacy_migration_is_versioned_and_idempotent(self):
        self.assertIn("cosiflow_cosidag_state_migration", self.schema)
        self.assertIn("ON CONFLICT (dag_id, path) DO NOTHING", self.state_source)
        self.assertIn("ON CONFLICT (dag_id) DO NOTHING", self.state_source)
        self.assertIn("must be a list of paths", self.state_source)


if __name__ == "__main__":
    unittest.main()
