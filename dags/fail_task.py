from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import sys
import os
airflow_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow")
sys.path.append(os.path.join(airflow_home, "callbacks"))
from on_failure_callback import notify_email

def failing_task():
    raise ValueError("This task fails.")

with DAG(
    'dag_with_email_alert',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': True,
        'email_on_retry': False,
        'on_failure_callback': notify_email,
        'retries': 0,
    },
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    fail = PythonOperator(
        task_id='failing_task',
        python_callable=failing_task
    )
