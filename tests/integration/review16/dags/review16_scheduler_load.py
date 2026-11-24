from __future__ import annotations

from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor


SENSOR_COUNT = 6
SENTINEL = Path("/tmp/review16-ready-work.success")


def wait_forever() -> bool:
    return False


def create_sentinel() -> None:
    SENTINEL.write_text("ready task completed\n", encoding="utf-8")


with DAG(
    dag_id="review16_waiting_sensors",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_tasks=16,
) as waiting_sensors:
    for index in range(SENSOR_COUNT):
        PythonSensor(
            task_id=f"waiting_sensor_{index + 1}",
            python_callable=wait_forever,
            mode="reschedule",
            poke_interval=60,
            timeout=240,
        )


with DAG(
    dag_id="review16_ready_work",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=False,
) as ready_work:
    PythonOperator(
        task_id="create_ready_sentinel",
        python_callable=create_sentinel,
    )

