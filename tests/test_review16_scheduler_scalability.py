import json
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
    directory_snapshot,
    file_snapshot,
    resolve_patterns,
    scan_file_inventory,
    stability_observation,
)


class StabilityTests(unittest.TestCase):
    def test_file_snapshot_uses_size_and_nanosecond_mtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.dat"
            path.write_bytes(b"abc")
            snapshot = file_snapshot(str(path))
            self.assertEqual(snapshot[0], 3)
            self.assertEqual(snapshot[1], path.stat().st_mtime_ns)

    def test_removed_file_has_no_snapshot(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "input.dat"
            path.write_bytes(b"abc")
            path.unlink()
            self.assertIsNone(file_snapshot(str(path)))

    def test_unchanged_snapshot_requires_the_full_idle_window(self):
        first_ready, first = stability_observation(None, "file", (3, 10), 100.0, 20)
        early_ready, early = stability_observation(first, "file", (3, 10), 119.9, 20)
        ready, current = stability_observation(early, "file", (3, 10), 120.0, 20)
        self.assertFalse(first_ready)
        self.assertFalse(early_ready)
        self.assertTrue(ready)
        self.assertEqual(current["stable_since"], 100.0)

    def test_size_or_mtime_change_restarts_the_idle_window(self):
        _, first = stability_observation(None, "file", (3, 10), 100.0, 20)
        size_ready, size_changed = stability_observation(first, "file", (4, 10), 121.0, 20)
        mtime_ready, mtime_changed = stability_observation(first, "file", (3, 11), 121.0, 20)
        self.assertFalse(size_ready)
        self.assertFalse(mtime_ready)
        self.assertEqual(size_changed["stable_since"], 121.0)
        self.assertEqual(mtime_changed["stable_since"], 121.0)

    def test_identity_change_restarts_the_idle_window(self):
        _, first = stability_observation(None, "a", (3, 10), 100.0, 20)
        ready, current = stability_observation(first, "b", (3, 10), 130.0, 20)
        self.assertFalse(ready)
        self.assertEqual(current["stable_since"], 130.0)

    def test_directory_snapshot_detects_add_remove_and_metadata_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            first = root / "first.dat"
            second = root / "second.dat"
            first.write_bytes(b"aaa")
            baseline = directory_snapshot(str(root))
            second.write_bytes(b"bbb")
            added = directory_snapshot(str(root))
            second.unlink()
            removed = directory_snapshot(str(root))
            original_mtime = first.stat().st_mtime_ns
            os.utime(first, ns=(original_mtime + 1_000_000, original_mtime + 1_000_000))
            metadata_changed = directory_snapshot(str(root))
            self.assertNotEqual(baseline, added)
            self.assertNotEqual(added, removed)
            self.assertEqual(baseline[0], removed[0])
            self.assertEqual(baseline[1], removed[1])
            self.assertNotEqual(baseline, metadata_changed)


class InventoryTests(unittest.TestCase):
    def create_tree(self, root: Path) -> None:
        (root / "nested").mkdir()
        (root / "a.fits").write_bytes(b"a")
        (root / "nested" / "b.fits").write_bytes(b"bb")
        (root / "nested" / "response.h5").write_bytes(b"h5")

    def test_all_patterns_reuse_one_walk_and_collected_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.create_tree(root)
            import cosidag_filesystem

            original_walk = cosidag_filesystem.os.walk
            walk_calls = []

            def counted_walk(*args, **kwargs):
                walk_calls.append(args[0])
                return original_walk(*args, **kwargs)

            with patch.object(cosidag_filesystem.os, "walk", side_effect=counted_walk):
                inventory = scan_file_inventory(str(root))
                selected, missing = resolve_patterns(
                    inventory,
                    {
                        "fits": "*.fits",
                        "response": "*.h5",
                        "regex": r"regex:^b\.fits$",
                    },
                    "first",
                )
            self.assertEqual(len(walk_calls), 1)
            self.assertFalse(missing)
            self.assertEqual(selected["fits"].basename, "a.fits")
            self.assertEqual(selected["response"].basename, "response.h5")
            self.assertEqual(selected["regex"].basename, "b.fits")

    def test_latest_mtime_uses_inventory_metadata(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            self.create_tree(root)
            a = root / "a.fits"
            b = root / "nested" / "b.fits"
            os.utime(a, ns=(10_000_000_000, 10_000_000_000))
            os.utime(b, ns=(20_000_000_000, 20_000_000_000))
            inventory = scan_file_inventory(str(root))
            selected, missing = resolve_patterns(inventory, {"fits": "*.fits"}, "latest_mtime")
            self.assertFalse(missing)
            self.assertEqual(selected["fits"].path, str(b))

    def test_missing_pattern_is_reported_without_rescanning(self):
        with tempfile.TemporaryDirectory() as tmp:
            inventory = scan_file_inventory(tmp)
            selected, missing = resolve_patterns(inventory, {"missing": "*.fits"}, "first")
            self.assertEqual(selected, {})
            self.assertEqual(missing, ["missing"])

    def test_stability_state_is_json_serializable_for_airflow_xcom(self):
        _, state = stability_observation(None, "file", (3, 10), 100.0, 20)
        self.assertEqual(json.loads(json.dumps(state)), state)

    def test_invalid_selection_policy_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "Unsupported select_policy"):
            resolve_patterns([], {"input": "*.fits"}, "random")


class SourceContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source = (MODULE_ROOT / "cosidag.py").read_text(encoding="utf-8")

    def test_both_filesystem_sensors_use_reschedule_mode(self):
        self.assertGreaterEqual(self.source.count('mode="reschedule"'), 2)
        self.assertNotIn('mode="poke"', self.source)

    def test_no_sleep_remains_in_cosidag(self):
        self.assertNotIn("time.sleep(", self.source)

    def test_unavailable_paths_are_converted_to_sets(self):
        self.assertEqual(
            self.source.count("set(list_unavailable_paths("),
            2,
        )


try:
    from datetime import datetime

    import cosidag
    from airflow.exceptions import AirflowRescheduleException
    from airflow.sensors.python import PythonSensor
except ModuleNotFoundError:
    cosidag = None


@unittest.skipIf(cosidag is None, "Airflow is not installed")
class AirflowSensorTests(unittest.TestCase):
    def test_discovery_and_input_readiness_release_worker_slots(self):
        dag = cosidag.COSIDAG(
            monitoring_folders=["/tmp"],
            file_patterns={"input": "*.fits"},
            dag_id="review16_runtime_test",
            start_date=datetime(2026, 1, 1),
            schedule_interval=None,
            catchup=False,
        )
        self.assertEqual(dag.get_task("check_new_file").mode, "reschedule")
        self.assertEqual(dag.get_task("resolve_inputs").mode, "reschedule")

    def test_waiting_sensor_emits_airflow_reschedule_signal(self):
        sensor = PythonSensor(
            task_id="waiting_sensor",
            python_callable=lambda **_: False,
            mode="reschedule",
            poke_interval=30,
            timeout=300,
        )
        ti = SimpleNamespace(
            max_tries=0,
            dag_id="review16_runtime_test",
            task_id="waiting_sensor",
            run_id="manual__review16",
            map_index=-1,
        )
        with patch("airflow.sensors.base._orig_start_date", return_value=None):
            with self.assertRaises(AirflowRescheduleException):
                sensor.execute({"ti": ti})

    def test_input_sensor_requires_consecutive_unchanged_snapshots(self):
        class FakeTaskInstance:
            task_id = "resolve_inputs"

            def __init__(self):
                self.values = {}

            def xcom_pull(self, task_ids, key):
                self.assert_task_id = task_ids
                return self.values.get(key)

            def xcom_push(self, key, value):
                self.values[key] = value

        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "input.fits"
            source.write_bytes(b"stable")
            dag = cosidag.COSIDAG(
                monitoring_folders=[],
                file_patterns={"input": "*.fits"},
                idle_seconds=0,
                dag_id="review16_readiness_test",
                start_date=datetime(2026, 1, 1),
                schedule_interval=None,
                catchup=False,
            )
            sensor = dag.get_task("resolve_inputs")
            ti = FakeTaskInstance()
            context = {
                "ti": ti,
                "dag_run": SimpleNamespace(conf={"detected_folder": tmp}),
            }
            self.assertFalse(sensor.poke(context))
            self.assertTrue(sensor.poke(context))
            self.assertEqual(ti.values["input"], str(source))
            self.assertEqual(ti.values["run_dir"], tmp)


if __name__ == "__main__":
    unittest.main()
