from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/shared_dir/pipeline/')
from alert_manager import run_monitor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'cosiflow_alert_monitor',
    default_args=default_args,
    description='Monitoraggio log per alert',
    schedule_interval=timedelta(minutes=2),
    catchup=False
)

parse_and_alert_task = PythonOperator(
    task_id='scan_logs_and_notify',
    python_callable=run_monitor,
    dag=dag
)

parse_and_alert_task
