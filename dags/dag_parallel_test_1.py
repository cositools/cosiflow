from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_parallel_test_1",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=2,
    concurrency=3,
    tags=["test", "parallel"]
) as dag:

    BashOperator(task_id="sleep_a", bash_command="sleep 60")
    BashOperator(task_id="sleep_b", bash_command="sleep 60")
