import sys
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "modules"))
sys.path.insert(0, str(REPO_ROOT / "callbacks"))

try:
    from airflow.exceptions import AirflowFailException
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    import cosidag
except ModuleNotFoundError:
    cosidag = None


@unittest.skipIf(cosidag is None, "Airflow is not installed")
class Review8AirflowRuntimeTests(unittest.TestCase):
    def build_dag(self):
        return cosidag.COSIDAG(
            monitoring_folders=["/tmp"],
            dag_id="review8_runtime_test",
            start_date=datetime(2026, 1, 1),
            schedule_interval=None,
            catchup=False,
        )

    def test_dag_contains_finalizer_after_show_results(self):
        dag = self.build_dag()
        finalizer = dag.get_task("finalize_cosidag_state")
        self.assertEqual(
            {task.task_id for task in finalizer.upstream_list},
            {"show_results"},
        )
        self.assertEqual(finalizer.trigger_rule, "all_done")

    def test_retrigger_assigns_runtime_id_and_mapping_conf(self):
        operator = cosidag.ConditionalTriggerDagRunOperator(
            task_id="automatic_retrig",
            trigger_dag_id="science",
            conf={},
        )
        dag_run = SimpleNamespace(
            conf={"retrig_run_count": 2, "policy": "folder-driven"},
            run_id="manual__source",
        )
        with patch.object(
            TriggerDagRunOperator,
            "execute",
            return_value="triggered",
        ) as parent_execute:
            result = operator.execute({"dag_run": dag_run})

        self.assertEqual(result, "triggered")
        self.assertEqual(operator.conf["retrig_run_count"], 3)
        self.assertIsInstance(operator.conf, dict)
        self.assertEqual(
            operator.trigger_run_id,
            cosidag.automatic_retrigger_run_id("science", "manual__source"),
        )
        parent_execute.assert_called_once()

    def test_finalizer_marks_success_only_after_show_results_success(self):
        dag = self.build_dag()
        finalizer = dag.get_task("finalize_cosidag_state").python_callable
        ti = Mock()
        ti.xcom_pull.return_value = "/data/input"
        dag_run = SimpleNamespace(
            run_id="manual__source",
            get_task_instance=lambda task_id: SimpleNamespace(state="success"),
        )
        with patch.object(cosidag, "mark_path_succeeded", return_value=True) as mark:
            result = finalizer(ti=ti, dag_run=dag_run)
        self.assertEqual(result["status"], "succeeded")
        mark.assert_called_once_with(
            "review8_runtime_test",
            "/data/input",
            "manual__source",
        )

    def test_finalizer_releases_failed_input_and_keeps_dag_failed(self):
        dag = self.build_dag()
        finalizer = dag.get_task("finalize_cosidag_state").python_callable
        ti = Mock()
        ti.xcom_pull.return_value = "/data/input"
        dag_run = SimpleNamespace(
            run_id="manual__source",
            get_task_instance=lambda task_id: SimpleNamespace(state="upstream_failed"),
        )
        with patch.object(cosidag, "mark_path_failed", return_value=True) as mark:
            with self.assertRaises(AirflowFailException):
                finalizer(ti=ti, dag_run=dag_run)
        mark.assert_called_once()


if __name__ == "__main__":
    unittest.main()

