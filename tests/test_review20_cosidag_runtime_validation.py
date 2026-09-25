import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules"
sys.path.insert(0, str(MODULE_ROOT))
sys.path.insert(0, str(REPO_ROOT / "callbacks"))

from cosidag_filesystem import (  # noqa: E402
    resolve_confined_relative_path,
    resolve_patterns,
    scan_file_inventory,
)
from cosidag_runtime import (  # noqa: E402
    normalize_monitoring_policy,
    normalize_relative_runtime_path,
    normalize_select_policy,
    parse_runtime_bool,
    validate_sensor_runtime_config,
)


DEFAULTS = {
    "monitoring_folders": ["/data/incoming"],
    "level": 1,
    "date": None,
    "date_queries": None,
    "idle_seconds": 20,
    "min_files": 1,
    "ready_marker": None,
    "only_basename": None,
    "prefer_deepest": True,
    "policy": "folder-driven",
    "claim_stale_seconds": 86400,
}


class RuntimeBooleanTests(unittest.TestCase):
    def test_supported_false_values_are_false(self):
        for value in (False, 0, "0", "false", "False", " NO ", "off"):
            with self.subTest(value=value):
                self.assertFalse(parse_runtime_bool(value, "flag"))

    def test_supported_true_values_are_true(self):
        for value in (True, 1, "1", "true", "TRUE", " yes ", "on"):
            with self.subTest(value=value):
                self.assertTrue(parse_runtime_bool(value, "flag"))

    def test_ambiguous_boolean_values_fail_clearly(self):
        for value in ("", "disabled", 2, -1, 0.0, None, [], {}):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "flag must be one of"):
                    parse_runtime_bool(value, "flag")

    def test_sensor_overrides_are_normalized_together(self):
        runtime = validate_sensor_runtime_config(
            {
                "prefer_deepest": "false",
                "policy": "FILE-DRIVEN",
                "idle_seconds": "0",
                "claim_stale_seconds": "30",
            },
            DEFAULTS,
        )
        self.assertFalse(runtime.prefer_deepest)
        self.assertEqual(runtime.policy, "file-driven")
        self.assertEqual(runtime.idle_seconds, 0)
        self.assertEqual(runtime.claim_stale_seconds, 30)

    def test_invalid_runtime_override_fails_during_validation(self):
        with self.assertRaisesRegex(ValueError, "prefer_deepest"):
            validate_sensor_runtime_config({"prefer_deepest": "sometimes"}, DEFAULTS)


class PolicyTests(unittest.TestCase):
    def test_supported_policies_are_normalized(self):
        self.assertEqual(normalize_monitoring_policy(" FILE-DRIVEN "), "file-driven")
        self.assertEqual(normalize_select_policy(" LATEST_MTIME "), "latest_mtime")

    def test_unsupported_policies_fail_closed(self):
        with self.assertRaisesRegex(ValueError, "monitoring policy"):
            normalize_monitoring_policy("random")
        with self.assertRaisesRegex(ValueError, "select_policy"):
            normalize_select_policy("random")


class MarkerConfinementTests(unittest.TestCase):
    def test_ready_marker_is_optional(self):
        self.assertIsNone(normalize_relative_runtime_path(None, "ready_marker"))
        self.assertIsNone(normalize_relative_runtime_path("", "ready_marker"))

    def test_absolute_and_parent_traversal_markers_are_rejected(self):
        for marker in ("/tmp/READY", "../READY", "nested/../../READY", r"C:\\READY"):
            with self.subTest(marker=marker):
                with self.assertRaisesRegex(ValueError, "ready_marker"):
                    normalize_relative_runtime_path(marker, "ready_marker")

    def test_valid_relative_marker_resolves_inside_candidate(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            marker = root / "control" / "READY"
            marker.parent.mkdir()
            marker.touch()
            normalized = normalize_relative_runtime_path("control/READY", "ready_marker")
            self.assertEqual(
                resolve_confined_relative_path(tmp, normalized),
                os.path.realpath(marker),
            )

    def test_marker_symlink_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside:
            root = Path(tmp)
            external_marker = Path(outside) / "READY"
            external_marker.touch()
            (root / "READY").symlink_to(external_marker)
            with self.assertRaisesRegex(ValueError, "escapes"):
                resolve_confined_relative_path(tmp, "READY")

    def test_parent_symlink_escape_is_rejected_even_if_marker_is_missing(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside:
            root = Path(tmp)
            (root / "external").symlink_to(outside, target_is_directory=True)
            with self.assertRaisesRegex(ValueError, "escapes"):
                resolve_confined_relative_path(tmp, "external/READY")

    def test_absolute_file_pattern_cannot_select_outside_inventory(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside:
            root = Path(tmp)
            external = Path(outside) / "outside.fits"
            external.write_bytes(b"external")
            (root / "inside.fits").write_bytes(b"inside")
            selected, missing = resolve_patterns(
                scan_file_inventory(tmp),
                {"input": str(external)},
                "first",
            )
            self.assertEqual(selected, {})
            self.assertEqual(missing, ["input"])


class SourceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (MODULE_ROOT / "cosidag.py").read_text(encoding="utf-8")

    def test_runtime_boolean_overrides_do_not_use_generic_bool(self):
        self.assertNotIn('bool(conf.get("prefer_deepest"', self.source)

    def test_marker_is_not_joined_without_confinement(self):
        self.assertNotIn("os.path.join(path, marker)", self.source)
        self.assertIn("resolve_confined_relative_path(path, marker)", self.source)


try:
    from datetime import datetime

    import cosidag
except ModuleNotFoundError:
    cosidag = None


@unittest.skipIf(cosidag is None, "Airflow is not installed")
class AirflowRuntimeValidationTests(unittest.TestCase):
    def build_dag(self, **overrides):
        kwargs = {
            "monitoring_folders": ["/tmp"],
            "auto_retrig": False,
            "dag_id": "review20_runtime_validation",
            "start_date": datetime(2026, 1, 1),
            "schedule_interval": None,
            "catchup": False,
        }
        kwargs.update(overrides)
        return cosidag.COSIDAG(**kwargs)

    def test_constructor_boolean_strings_control_defaults_and_task_creation(self):
        dag = self.build_dag(auto_retrig="false", prefer_deepest="false")
        self.assertFalse(dag.cosidag_defaults["auto_retrig"])
        self.assertFalse(dag.cosidag_defaults["prefer_deepest"])
        self.assertIsNone(dag.automatic_retrig)

    def test_constructor_rejects_invalid_select_policy_before_task_execution(self):
        with self.assertRaisesRegex(ValueError, "select_policy"):
            self.build_dag(select_policy="random")

    def test_constructor_rejects_absolute_marker(self):
        with self.assertRaisesRegex(ValueError, "ready_marker"):
            self.build_dag(ready_marker="/tmp/READY")

    def test_dag_run_false_override_reaches_folder_selection_as_false(self):
        dag = self.build_dag()
        sensor = dag.get_task("check_new_file")
        context = {
            "ti": SimpleNamespace(),
            "dag_run": SimpleNamespace(
                conf={"prefer_deepest": "false"},
                run_id="manual__review20",
            ),
        }
        with (
            patch.object(cosidag, "release_orphaned_claims"),
            patch.object(cosidag, "_find_new_folder", return_value=None) as find_folder,
        ):
            self.assertFalse(sensor.poke(context))
        self.assertFalse(find_folder.call_args.kwargs["prefer_deepest"])

    def test_invalid_runtime_policy_fails_before_scan_release_or_claim(self):
        dag = self.build_dag()
        sensor = dag.get_task("check_new_file")
        context = {
            "ti": SimpleNamespace(),
            "dag_run": SimpleNamespace(
                conf={"policy": "random"},
                run_id="manual__review20",
            ),
        }
        with (
            patch.object(cosidag, "release_orphaned_claims") as release,
            patch.object(cosidag, "_find_new_folder") as find_folder,
            patch.object(cosidag, "claim_path") as claim,
        ):
            with self.assertRaisesRegex(ValueError, "monitoring policy"):
                sensor.poke(context)
        release.assert_not_called()
        find_folder.assert_not_called()
        claim.assert_not_called()

    def test_invalid_runtime_marker_fails_before_scan_release_or_claim(self):
        dag = self.build_dag()
        sensor = dag.get_task("check_new_file")
        context = {
            "ti": SimpleNamespace(),
            "dag_run": SimpleNamespace(
                conf={"ready_marker": "../READY"},
                run_id="manual__review20",
            ),
        }
        with (
            patch.object(cosidag, "release_orphaned_claims") as release,
            patch.object(cosidag, "_find_new_folder") as find_folder,
            patch.object(cosidag, "claim_path") as claim,
        ):
            with self.assertRaisesRegex(ValueError, "ready_marker"):
                sensor.poke(context)
        release.assert_not_called()
        find_folder.assert_not_called()
        claim.assert_not_called()


if __name__ == "__main__":
    unittest.main()
