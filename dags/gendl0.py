from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import time
import csv
import random

class DataPipeline:
    def __init__(self):
        self.base_dir = '/home/gamma/workspace/data'

    def generate_dl0_file(self):
        os.makedirs(f'{self.base_dir}/input', exist_ok=True)
        filename = f"{self.base_dir}/input/dl0_{int(time.time())}.csv"
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(["parameter1", "parameter2", "parameter3"])
            for _ in range(100):
                writer.writerow([random.random() for _ in range(3)])

pipeline = DataPipeline()

# DAG for generating DL0 files periodically
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'generate_dl0_data',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),  # Cambiato da '@secondly' a timedelta
    catchup=False,
) as generate_dl0_dag:
    generate_dl0_task = PythonOperator(
        task_id='generate_dl0_file',
        python_callable=pipeline.generate_dl0_file
    )
