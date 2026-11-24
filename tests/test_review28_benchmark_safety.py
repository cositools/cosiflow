import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[1]
PERFORMANCE_TEST_PATH = REPO_ROOT / "test" / "performance_test.py"
SPEC = importlib.util.spec_from_file_location("review28_performance_test", PERFORMANCE_TEST_PATH)
performance_test = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
sys.modules[SPEC.name] = performance_test
SPEC.loader.exec_module(performance_test)


def runtime_dag(state: str):
    return performance_test.RuntimeDag(
        dag_id="review28",
        run_id="perf__review28__test",
        trigger_time="2026-01-01T00:00:00+00:00",
        conf={},
        task_ids=["work"],
        state=state,
    )


class CleanupSafetyTests(unittest.TestCase):
    def config(self, **cleanup_overrides):
        cleanup = {
            "enabled": True,
            "dry_run": False,
            "allowed_root": "data/benchmark",
            "roots": [{"path": "data/benchmark/run"}],
        }
        cleanup.update(cleanup_overrides)
        return {"cleanup": cleanup}

    def make_tree(self, root: Path):
        target = root / "data" / "benchmark" / "run"
        target.mkdir(parents=True)
        (target / "delete.txt").write_text("delete", encoding="utf-8")
        return target

    def test_committed_default_disables_cleanup_and_uses_dry_run(self):
        config = performance_test.load_config(REPO_ROOT / "test" / "performance_test.yaml")
        self.assertFalse(config["cleanup"]["enabled"])
        self.assertTrue(config["cleanup"]["dry_run"])
        self.assertFalse(config["finalization"]["stop_other_active_runs"])

    def test_real_cleanup_requires_cli_opt_in(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = self.make_tree(root)
            with self.assertRaisesRegex(ValueError, "--allow-destructive-cleanup"):
                performance_test.cleanup_data(self.config(), root)
            self.assertTrue((target / "delete.txt").exists())

    def test_repository_root_and_allowed_root_are_rejected_as_targets(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.make_tree(root)
            for target in (".", "data/benchmark"):
                with self.subTest(target=target):
                    config = self.config(roots=[{"path": target}], dry_run=True)
                    with self.assertRaisesRegex(ValueError, "strict descendant"):
                        performance_test.cleanup_data(config, root)

    def test_symlink_escape_fails_preflight_before_any_deletion(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as outside:
            root = Path(tmp)
            target = self.make_tree(root)
            escape = root / "data" / "benchmark" / "escape"
            escape.symlink_to(Path(outside), target_is_directory=True)
            config = self.config(
                roots=[
                    {"path": "data/benchmark/run"},
                    {"path": "data/benchmark/escape"},
                ]
            )
            with self.assertRaisesRegex(ValueError, "strict descendant"):
                performance_test.cleanup_data(config, root, allow_destructive=True)
            self.assertTrue((target / "delete.txt").exists())

    def test_dry_run_lists_work_without_deleting(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = self.make_tree(root)
            performance_test.cleanup_data(self.config(dry_run=True), root)
            self.assertTrue((target / "delete.txt").exists())

    def test_valid_cleanup_is_confined_and_preserves_keep_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            target = self.make_tree(root)
            keep = target / "keep.txt"
            keep.write_text("keep", encoding="utf-8")
            config = self.config(
                roots=[{"path": "data/benchmark/run", "keep_files": ["keep.txt"]}]
            )
            performance_test.cleanup_data(config, root, allow_destructive=True)
            self.assertFalse((target / "delete.txt").exists())
            self.assertEqual(keep.read_text(encoding="utf-8"), "keep")


class OutcomeTests(unittest.TestCase):
    def test_terminal_and_success_are_separate_contracts(self):
        failed = [runtime_dag("failed")]
        self.assertTrue(performance_test.terminal(failed))
        self.assertFalse(performance_test.successful(failed))
        self.assertEqual(performance_test.benchmark_exit_code(failed), 2)

    def test_success_requires_all_runs_and_complete_finalization(self):
        success = [runtime_dag("success"), runtime_dag("success")]
        self.assertEqual(performance_test.benchmark_exit_code(success), 0)
        self.assertEqual(performance_test.benchmark_exit_code(success, timed_out=True), 2)
        self.assertEqual(performance_test.benchmark_exit_code(success, finalization_ok=False), 2)
        self.assertEqual(performance_test.benchmark_exit_code(success, artifacts_ok=False), 2)

    def test_mixed_or_unfinished_runs_fail(self):
        self.assertEqual(performance_test.benchmark_exit_code([]), 2)
        self.assertEqual(
            performance_test.benchmark_exit_code([runtime_dag("success"), runtime_dag("failed")]),
            2,
        )
        self.assertEqual(performance_test.benchmark_exit_code([runtime_dag("running")]), 2)


class StopVerificationTests(unittest.TestCase):
    def client(self):
        return object.__new__(performance_test.AirflowClient)

    def test_stop_waits_until_job_and_process_are_gone(self):
        client = self.client()
        client.python_json = lambda _code: {
            "found": True,
            "requested_tasks": ["work"],
            "job_ids": [42],
        }
        activities = iter(
            [
                {"jobs": {"42": "running"}, "processes": [{"pid": 100}]},
                {"jobs": {"42": "success"}, "processes": []},
            ]
        )
        client.run_activity = lambda *_args: next(activities)
        with patch.object(performance_test.time, "sleep", return_value=None):
            result = client.stop_active_run("review28", "perf__review28", timeout_seconds=5, poll_seconds=0)
        self.assertTrue(result["verified"])
        self.assertEqual(result["processes"], [])

    def test_stop_timeout_reports_residual_jobs_and_processes(self):
        client = self.client()
        client.python_json = lambda _code: {
            "found": True,
            "requested_tasks": ["work"],
            "job_ids": [42],
        }
        client.run_activity = lambda *_args: {
            "jobs": {"42": "running"},
            "processes": [{"pid": 100, "args": ["airflow", "tasks", "run"]}],
        }
        with patch.object(performance_test.time, "monotonic", side_effect=[0.0, 2.0]):
            result = client.stop_active_run("review28", "perf__review28", timeout_seconds=1, poll_seconds=0)
        self.assertFalse(result["verified"])
        self.assertEqual(result["job_ids"], ["42"])
        self.assertEqual(result["processes"][0]["pid"], 100)

    def test_missing_run_fails_closed(self):
        client = self.client()
        client.python_json = lambda _code: {"found": False, "job_ids": []}
        result = client.stop_active_run("review28", "missing", timeout_seconds=0, poll_seconds=0)
        self.assertFalse(result["verified"])
        self.assertIn("not found", result["reason"])

    def test_other_run_discovery_is_prefix_scoped(self):
        source = PERFORMANCE_TEST_PATH.read_text(encoding="utf-8")
        self.assertIn("str(dr.run_id).startswith(run_id_prefix)", source)
        self.assertIn('stop_other_active_runs: false', (REPO_ROOT / "test" / "performance_test.yaml").read_text())


if __name__ == "__main__":
    unittest.main()
