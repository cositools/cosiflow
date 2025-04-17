from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os
import time
import csv
import random

# Define a class to encapsulate data generation logic
class DataPipeline:
    def __init__(self):
        # Base directory for storing input data
        self.base_dir = '/home/gamma/workspace/data'

    def generate_dl0_file(self):
        # Create the input directory if it doesn't exist
        input_directory = os.path.join(self.base_dir, 'input')
        if not os.path.exists(input_directory):
            os.makedirs(input_directory)
        # Generate a filename using the current timestamp
        filename = os.path.join(input_directory, f"dl0_{int(time.time())}.csv")
        # Write random data to the CSV file
        with open(filename, 'w', newline='') as file:
            writer = csv.writer(file)
            # Write header row
            writer.writerow(["parameter1", "parameter2", "parameter3"])
            # Write 100 rows of random float values
            for _ in range(100):
                writer.writerow([random.random() for _ in range(3)])

# Instantiate the data pipeline
pipeline = DataPipeline()

# Define default arguments for the Airflow DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Define a DAG that periodically generates DL0 data
with DAG(
    'generate_dl0_data',
    default_args=default_args,
    schedule_interval=timedelta(seconds=10),  # Runs every 10 seconds
    catchup=False,
) as generate_dl0_dag:
    # Create a task that calls the generate_dl0_file function
    generate_dl0_task = PythonOperator(
        task_id='generate_dl0_file',
        python_callable=pipeline.generate_dl0_file
    )
