import os
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_ROOT = REPO_ROOT / "modules"
sys.path.insert(0, str(MODULE_ROOT))

from cosidag_filesystem import (  # noqa: E402
    canonicalize_confined_path,
    resolve_patterns,
    scan_file_inventory,
)
from cosidag_runtime import (  # noqa: E402
    normalize_file_patterns,
    validate_resolve_runtime_config,
    validate_retry_runtime_overrides,
)
from date_helper import normalize_date_filters  # noqa: E402


class StructuredDateContractTests(unittest.TestCase):
    def test_structured_filters_accept_only_real_iso_dates_and_known_operators(self):
        parsed = normalize_date_filters(
            [
                {"operator": ">=", "date": "2026-09-01"},
                {"operator": "<", "date": "2026-10-01"},
            ]
        )
        self.assertEqual(parsed, ((">=", date(2026, 9, 1)), ("<", date(2026, 10, 1))))
        for invalid in (
            [{"operator": "~", "date": "2026-09-01"}],
            [{"operator": ">=", "date": "2026-13-01"}],
            [{"operator": ">=", "date": "20260901"}],
            [{"operator": ">=", "date": "2026-09-01", "extra": True}],
            ["2026-09-01"],
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    normalize_date_filters(invalid)

    def test_invalid_legacy_dates_fail_closed(self):
        for invalid in (">=2026-13-01", ">=2026-09-01T12:00:00", "", 12):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    normalize_date_filters(legacy_queries=[invalid])


class PatternContractTests(unittest.TestCase):
    def test_matching_directories_are_ignored_but_root_and_nested_files_match(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root / "directory.fits").mkdir()
            (root / "root.fits").write_bytes(b"root")
            nested = root / "nested"
            nested.mkdir()
            (nested / "nested.fits").write_bytes(b"nested")
            inventory = scan_file_inventory(tmp)
            selected, missing = resolve_patterns(
                inventory,
                {"first_fits": "*.fits", "nested_fits": "nested/*.fits"},
                "first",
            )
            self.assertFalse(missing)
            self.assertEqual(selected["first_fits"].basename, "nested.fits")
            self.assertEqual(selected["nested_fits"].relative_path, os.path.join("nested", "nested.fits"))
            self.assertNotIn("directory.fits", {item.relative_path for item in inventory})

    def test_pattern_contract_is_validated_before_inventory(self):
        for invalid in (
            {},
            {"": "*.fits"},
            {"input": ""},
            {"input": "../*.fits"},
            {"input": "/tmp/*.fits"},
            {"input": "regex:["},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    normalize_file_patterns(invalid)

        runtime = validate_resolve_runtime_config(
            {"file_patterns": {"input": "*.fits"}, "select_policy": "latest_mtime"},
            {"file_patterns": {"default": "*.h5"}, "select_policy": "first", "idle_seconds": 20},
        )
        self.assertEqual(runtime.file_patterns, {"input": "*.fits"})
        self.assertEqual(runtime.select_policy, "latest_mtime")


class CanonicalPathTests(unittest.TestCase):
    def test_aliases_and_symlinks_share_one_confined_identity(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = root / "target"
            target.mkdir()
            alias = root / "alias"
            alias.symlink_to(target, target_is_directory=True)
            canonical_target, canonical_root = canonicalize_confined_path(str(target), [tmp])
            canonical_alias, _ = canonicalize_confined_path(str(alias), [tmp])
            self.assertEqual(canonical_alias, canonical_target)
            self.assertEqual(canonical_root, os.path.realpath(tmp))

    def test_escape_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside:
            with self.assertRaises(ValueError):
                canonicalize_confined_path(outside, [tmp])


class RetryOverrideContractTests(unittest.TestCase):
    def test_only_consumed_validated_overrides_are_accepted(self):
        normalized = validate_retry_runtime_overrides(
            {
                "date_filters": [{"operator": ">=", "date": "2026-09-01"}],
                "file_patterns": {"input": "*.fits"},
                "select_policy": "first",
            }
        )
        self.assertIn("date_filters", normalized)
        with self.assertRaisesRegex(ValueError, "Unsupported retry override"):
            validate_retry_runtime_overrides({"monitoring_folders": ["/tmp"]})


class QueueSourceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.state = (MODULE_ROOT / "cosidag_state.py").read_text(encoding="utf-8")
        cls.cosidag = (MODULE_ROOT / "cosidag.py").read_text(encoding="utf-8")
        cls.schema = (REPO_ROOT / "env/migrations/001_cosidag_state.sql").read_text(encoding="utf-8")
        cls.plugin = (
            REPO_ROOT / "plugins/reset_cosidag_link/reset_cosidag_plugin.py"
        ).read_text(encoding="utf-8")

    def test_queue_claim_is_skip_locked_and_owner_scoped(self):
        self.assertIn("FOR UPDATE SKIP LOCKED", self.state)
        self.assertIn("attempt_count = state.attempt_count + 1", self.state)
        self.assertIn("owner_run_id=:owner_run_id", self.state)

    def test_refill_is_threshold_and_batch_bounded(self):
        self.assertIn("current_queued < defaults[\"refill_threshold\"]", self.cosidag)
        self.assertIn("defaults[\"discovery_batch_size\"]", self.cosidag)
        self.assertIn("ON CONFLICT (dag_id, path) DO NOTHING", self.state)

    def test_stability_survives_sensor_reschedules_outside_xcom(self):
        self.assertIn("CREATE TABLE IF NOT EXISTS cosiflow_cosidag_stability", self.schema)
        self.assertIn("def observe_stability(", self.state)
        self.assertIn('scope="candidate-folder"', self.cosidag)
        self.assertIn('scope="resolved-inputs"', self.cosidag)
        self.assertNotIn("def _stability_ready(", self.cosidag)
        self.assertNotIn("candidate_stability_", self.cosidag)

    def test_abandoned_stability_observations_have_bounded_retention(self):
        self.assertIn("def prune_stability_observations(", self.state)
        self.assertIn("prune_stability_observations()", self.cosidag)
        self.assertIn("cosiflow_cosidag_stability_updated_idx", self.schema)

    def test_success_is_retained_and_second_failure_is_terminal(self):
        self.assertIn("status = CASE WHEN attempt_count < 2 THEN 'queued' ELSE 'failed' END", self.state)
        self.assertNotIn("state_retention", self.cosidag)
        self.assertIn("status IN ('queued', 'claimed', 'succeeded', 'failed', 'discarded')", self.schema)

    def test_manual_retry_is_authorized_and_audited(self):
        self.assertIn("@require_cosiflow_permission(ACTION_EDIT, COSIDAG_STATE)", self.plugin)
        self.assertIn("manual_retry_by", self.state)
        self.assertIn("manual_retry_reason", self.state)

    def test_parse_time_controls_are_not_airflow_params(self):
        params_block = self.cosidag[
            self.cosidag.index("# Base params") : self.cosidag.index("self.auto_retrig =")
        ]
        for key in (
            '"home_env_var": _param',
            '"input_poke_seconds": _param',
            '"input_timeout_seconds": _param',
            '"max_active_runs": _param',
            '"max_active_tasks": _param',
            '"concurrency": _param',
        ):
            self.assertNotIn(key, params_block)


if __name__ == "__main__":
    unittest.main()
