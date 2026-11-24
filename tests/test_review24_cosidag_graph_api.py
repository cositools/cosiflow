import inspect
import sys
import tempfile
import unittest
import warnings
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules"
sys.path.insert(0, str(MODULE_ROOT))
sys.path.insert(0, str(REPO_ROOT / "callbacks"))

from cosidag_filesystem import resolve_patterns, scan_file_inventory  # noqa: E402
from cosidag_runtime import normalize_file_patterns  # noqa: E402


class SourceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (MODULE_ROOT / "cosidag.py").read_text(encoding="utf-8")

    def test_dead_pathinfo_fallback_is_removed(self):
        self.assertNotIn("PathInfo", self.source)
        self.assertNotIn("build_url_fragment", self.source)
        self.assertNotIn("cosiflow.modules.path", self.source)

    def test_legacy_regex_search_implementation_is_removed(self):
        self.assertNotIn("rx.search", self.source)
        self.assertIn('normalize_file_patterns({"match": f"regex:{pattern}"})', self.source)
        self.assertIn("scan_file_inventory(detected_folder)", self.source)

    def test_custom_graph_bypass_statement_is_guarded(self):
        guarded_wiring = (
            "if roots:\n"
            "            last_custom >> show_results\n"
            "        elif anchor is not None:\n"
            "            anchor >> last_custom >> show_results"
        )
        self.assertIn(guarded_wiring, self.source)


class ResolverContractTests(unittest.TestCase):
    def test_documented_regex_semantics_are_anchored_to_basename_start(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "file.fits").write_bytes(b"root")
            (root / "prefix-file.fits").write_bytes(b"prefix")
            selected, missing = resolve_patterns(
                scan_file_inventory(tmp),
                {"match": r"regex:file\.fits$"},
                "first",
            )
            self.assertFalse(missing)
            self.assertEqual(selected["match"].basename, "file.fits")

    def test_first_regex_match_is_deterministic_across_nested_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "z").mkdir()
            (root / "a").mkdir()
            (root / "z" / "file.fits").write_bytes(b"z")
            (root / "a" / "file.fits").write_bytes(b"a")
            selected, missing = resolve_patterns(
                scan_file_inventory(tmp),
                {"match": r"regex:file\.fits$"},
                "first",
            )
            self.assertFalse(missing)
            self.assertEqual(selected["match"].path, str(root / "a" / "file.fits"))

    def test_invalid_regex_fails_during_shared_validation(self):
        with self.assertRaisesRegex(ValueError, "Invalid regex"):
            normalize_file_patterns({"match": "regex:["})


try:
    from datetime import datetime

    import cosidag
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    cosidag = None


@unittest.skipIf(cosidag is None, "Airflow is not installed")
class AirflowGraphAndApiTests(unittest.TestCase):
    def dag_kwargs(self, dag_id):
        return {
            "dag_id": dag_id,
            "start_date": datetime(2026, 1, 1),
            "schedule_interval": None,
            "catchup": False,
            "auto_retrig": False,
        }

    def test_custom_barrier_has_only_custom_leaves_as_upstream(self):
        def build_custom(dag):
            root = EmptyOperator(task_id="science_root", dag=dag)
            left = EmptyOperator(task_id="science_left", dag=dag)
            right = EmptyOperator(task_id="science_right", dag=dag)
            root >> [left, right]

        dag = cosidag.COSIDAG(
            monitoring_folders=["/tmp"],
            build_custom=build_custom,
            **self.dag_kwargs("review24_graph"),
        )
        barrier = dag.get_task("custom_anchor")
        discovery = dag.get_task("check_new_file")
        science_root = dag.get_task("science_root")

        self.assertEqual(barrier.upstream_task_ids, {"science_left", "science_right"})
        self.assertNotIn("custom_anchor", discovery.downstream_task_ids)
        self.assertIn("check_new_file", science_root.upstream_task_ids)
        self.assertEqual(barrier.downstream_task_ids, {"show_results"})
        self.assertEqual(dag.get_task("show_results").downstream_task_ids, {"finalize_cosidag_state"})

    def test_placeholder_keeps_anchor_chain_when_no_custom_tasks_exist(self):
        dag = cosidag.COSIDAG(
            monitoring_folders=["/tmp"],
            **self.dag_kwargs("review24_placeholder"),
        )
        placeholder = dag.get_task("custom_placeholder")
        self.assertEqual(placeholder.upstream_task_ids, {"check_new_file"})
        self.assertEqual(placeholder.downstream_task_ids, {"show_results"})

    def test_airflow_dag_id_can_be_positional(self):
        dag = cosidag.COSIDAG(
            "review24_positional_dag_id",
            monitoring_folders=[],
            start_date=datetime(2026, 1, 1),
            schedule_interval=None,
            catchup=False,
            auto_retrig=False,
        )
        self.assertEqual(dag.dag_id, "review24_positional_dag_id")

    def test_legacy_monitoring_folders_remain_temporarily_compatible(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            dag = cosidag.COSIDAG(
                [],
                **self.dag_kwargs("review24_legacy_monitoring"),
            )
        self.assertEqual(dag.dag_id, "review24_legacy_monitoring")
        self.assertTrue(
            any(
                item.category is DeprecationWarning
                and "monitoring_folders positionally is deprecated" in str(item.message)
                for item in caught
            )
        )

    def test_missing_and_duplicate_monitoring_configuration_fail_clearly(self):
        with self.assertRaisesRegex(TypeError, "required keyword-only"):
            cosidag.COSIDAG("review24_missing")
        with self.assertRaisesRegex(TypeError, "both positionally and by keyword"):
            cosidag.COSIDAG(
                [],
                monitoring_folders=[],
                **self.dag_kwargs("review24_duplicate"),
            )

    def test_constructor_signature_is_keyword_first_for_cosidag_options(self):
        signature = inspect.signature(cosidag.COSIDAG.__init__)
        self.assertEqual(signature.parameters["dag_args"].kind, inspect.Parameter.VAR_POSITIONAL)
        self.assertEqual(
            signature.parameters["monitoring_folders"].kind,
            inspect.Parameter.KEYWORD_ONLY,
        )

    def test_public_regex_helper_matches_declarative_resolver(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "prefix-file.fits").write_bytes(b"prefix")
            (root / "file.fits").write_bytes(b"exact")
            expected, _ = resolve_patterns(
                scan_file_inventory(tmp),
                {"match": r"regex:file\.fits$"},
                "first",
            )
            actual = cosidag.COSIDAG.find_file_by_pattern(
                object(),
                r"file\.fits$",
                tmp,
            )
            self.assertEqual(actual, expected["match"].path)

    def test_public_regex_helper_rejects_invalid_patterns(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "Invalid regex"):
                cosidag.COSIDAG.find_file_by_pattern(object(), "[", tmp)


if __name__ == "__main__":
    unittest.main()
