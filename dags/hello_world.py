# hello_world_dag.py
# Airflow 2.x
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

BASE_DIR = Path("/home/gamma/workspace/data/tutorials")
RESULT_FILE = BASE_DIR / "result.txt"

def write_hello():
    """Append 'Hello Wolrd!' into result.txt.
    Note: the folder/file is guaranteed to exist from the Bash task."""
    with open(RESULT_FILE, "a", encoding="utf-8") as f:
        f.write("Hello Wolrd!\n")  # intentionally keeping the requested typo

with DAG(
    dag_id="hello_world_dag",
    description="Minimal example: Bash touch + Python writes text",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,   # run on-demand
    catchup=False,
    tags=["handson", "tutorials"],
) as dag:

    make_file = BashOperator(
        task_id="make_folder_and_file",
        bash_command=(
            f"mkdir -p {BASE_DIR} && "
            f"touch {RESULT_FILE}"
        ),
        # Good practice: fail if any piece fails
        env={},
    )

    write_text = PythonOperator(
        task_id="write_text",
        python_callable=write_hello,
    )

    make_file >> write_text
