from __future__ import annotations

import time
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context


def write_heartbeat() -> None:
    run_id = str(get_current_context()["dag_run"].run_id)
    path = Path(f"/tmp/review28-{run_id}.heartbeat")
    while True:
        with path.open("a", encoding="utf-8") as handle:
            handle.write(f"{time.time_ns()}\n")
        time.sleep(0.2)


with DAG(
    dag_id="review28_long_running",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=2,
) as dag:
    PythonOperator(
        task_id="write_heartbeat",
        python_callable=write_heartbeat,
    )
