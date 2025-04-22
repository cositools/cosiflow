from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago
import os
import time
import csv
import random
import logging
from datetime import datetime, timedelta
from airflow.exceptions import AirflowSkipException

class DataPipeline:
    def __init__(self):
        self.base_dir = '/home/gamma/workspace/data'
        self.heasarc_dir = '/home/gamma/workspace/heasarc'
        self.logger = logging.getLogger(__name__)

    def ingest_and_store_dl0(self, **kwargs):
        try:
            ti = kwargs['ti']
            input_files = os.listdir(f'{self.base_dir}/input')
            if input_files:
                oldest_file = min([f"{self.base_dir}/input/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(oldest_file):
                    raise FileNotFoundError(f"Input file {oldest_file} does not exist.")
                self.logger.info(f"Oldest DL0 file: {oldest_file}")
                os.makedirs(f'{self.heasarc_dir}/dl0', exist_ok=True)
                new_file_path = f"{self.heasarc_dir}/dl0/{os.path.basename(oldest_file)}"
                os.rename(oldest_file, new_file_path)
                self.logger.info(f"Stored DL0 file: {new_file_path}")
                ti.xcom_push(key='stored_dl0_file', value=new_file_path)
            else:
                self.logger.warning("No input files found in the directory. Exiting task gracefully.")
                raise AirflowSkipException("No input files found, skipping task.")
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise AirflowSkipException(f"File not found: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise
        
    def generate_placeholder_file(self, input_file, output_dir, stage):
        try:
            if not os.path.exists(input_file):
                raise FileNotFoundError(f"Input file {input_file} does not exist.")
            os.makedirs(output_dir, exist_ok=True)
            current_time = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
            filename = f"{output_dir}/{stage}_{os.path.basename(input_file)}_{current_time}"
            with open(filename, 'w', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(["parameter1", "parameter2", "parameter3"])
                for _ in range(100):
                    writer.writerow([random.random() for _ in range(3)])
            self.logger.info(f"Generated placeholder file: {filename}")
            return filename
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Exiting task gracefully.")
            raise AirflowSkipException(f"File not found: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise

    def generate_dl1a(self, **kwargs):
        dl0_file = kwargs['ti'].xcom_pull(key='stored_dl0_file', task_ids='ingest_and_store_dl0')
        if dl0_file:
            self.generate_placeholder_file(dl0_file, f'{self.heasarc_dir}/dl1a', 'dl1a')

    def generate_dl1b(self):
        try:
            input_files = os.listdir(f'{self.heasarc_dir}/dl1a')
            if input_files:
                latest_file = max([f"{self.heasarc_dir}/dl1a/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(latest_file):
                    raise FileNotFoundError(f"Input file {latest_file} does not exist.")
                self.generate_placeholder_file(latest_file, f'{self.heasarc_dir}/dl1b', 'dl1b')
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise

    def generate_dl1c(self):
        try:
            input_files = os.listdir(f'{self.heasarc_dir}/dl1b')
            if input_files:
                latest_file = max([f"{self.heasarc_dir}/dl1b/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(latest_file):
                    raise FileNotFoundError(f"Input file {latest_file} does not exist.")
                self.generate_placeholder_file(latest_file, f'{self.heasarc_dir}/dl1c', 'dl1c')
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise

    def generate_dl2(self):
        try:
            input_files = os.listdir(f'{self.heasarc_dir}/dl1c')
            if input_files:
                latest_file = max([f"{self.heasarc_dir}/dl1c/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(latest_file):
                    raise FileNotFoundError(f"Input file {latest_file} does not exist.")
                self.generate_placeholder_file(latest_file, f'{self.heasarc_dir}/dl2', 'dl2')
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise

    def fast_transient_stage_one(self):
        try:
            input_files = os.listdir(f'{self.heasarc_dir}/dl2')
            if input_files:
                latest_file = max([f"{self.heasarc_dir}/dl2/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(latest_file):
                    raise FileNotFoundError(f"Input file {latest_file} does not exist.")
                self.generate_placeholder_file(latest_file, f'{self.heasarc_dir}/fast_transient_stage_1', 'stage1')
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise

    def fast_transient_stage_two(self):
        try:
            input_files = os.listdir(f'{self.heasarc_dir}/fast_transient_stage_1')
            if input_files:
                latest_file = max([f"{self.heasarc_dir}/fast_transient_stage_1/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(latest_file):
                    raise FileNotFoundError(f"Input file {latest_file} does not exist.")
                self.generate_placeholder_file(latest_file, f'{self.heasarc_dir}/fast_transient_stage_2', 'stage2')
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise

    def fast_transient_stage_three(self):
        try:
            input_files = os.listdir(f'{self.heasarc_dir}/fast_transient_stage_2')
            if input_files:
                latest_file = max([f"{self.heasarc_dir}/fast_transient_stage_2/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(latest_file):
                    raise FileNotFoundError(f"Input file {latest_file} does not exist.")
                self.generate_placeholder_file(latest_file, f'{self.heasarc_dir}/fast_transient_stage_3', 'stage3')
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise

    def notify_completion(self):
        self.logger.info("Pipeline has completed successfully.")

    def log_performance_metric(self, task_id, start_time):
        end_time = time.time()
        duration = end_time - start_time
        self.logger.info(f"Task {task_id} took {duration} seconds to start after receiving its input.")

    def check_new_file(self):
        input_files = os.listdir(f'{self.base_dir}/input')
        self.logger.info(f"Checking for new files. Current files: {input_files}")
        return bool(input_files)

pipeline = DataPipeline()

# DAG for processing DL0 and subsequent steps
with DAG('cosi_data_analysis_pipeline_v1', default_args={'owner': 'airflow'}, schedule_interval=None, 
        start_date=datetime.now(),
        concurrency=5,  # Numero massimo di task eseguibili contemporaneamente per DAG
        max_active_runs=4  # Numero massimo di istanze del DAG che possono essere eseguite contemporaneamente
        ) as dag:
    
    wait_for_new_file = PythonSensor(
        task_id='wait_for_new_file',
        python_callable=pipeline.check_new_file,
        poke_interval=1,
        timeout=600
    )

    ingest_and_store_dl0_task = PythonOperator(
        task_id='ingest_and_store_dl0',
        python_callable=pipeline.ingest_and_store_dl0,
        provide_context=True
    )

    generate_dl1a_task = PythonOperator(
        task_id='generate_dl1a',
        python_callable=pipeline.generate_dl1a,
        provide_context=True
    )

    generate_dl1b_task = PythonOperator(
        task_id='generate_dl1b',
        python_callable=pipeline.generate_dl1b
    )

    generate_dl1c_task = PythonOperator(
        task_id='generate_dl1c',
        python_callable=pipeline.generate_dl1c
    )

    generate_dl2_task = PythonOperator(
        task_id='generate_dl2',
        python_callable=pipeline.generate_dl2
    )

    fast_transient_stage_one_task = PythonOperator(
        task_id='fast_transient_stage_one',
        python_callable=pipeline.fast_transient_stage_one
    )

    fast_transient_stage_two_task = PythonOperator(
        task_id='fast_transient_stage_two',
        python_callable=pipeline.fast_transient_stage_two
    )

    fast_transient_stage_three_task = PythonOperator(
        task_id='fast_transient_stage_three',
        python_callable=pipeline.fast_transient_stage_three
    )

    notify_completion_task = PythonOperator(
        task_id='notify_completion',
        python_callable=pipeline.notify_completion
    )

    wait_for_new_file >> ingest_and_store_dl0_task >> generate_dl1a_task >> generate_dl1b_task >> generate_dl1c_task >> generate_dl2_task >> fast_transient_stage_one_task >> fast_transient_stage_two_task >> fast_transient_stage_three_task >> notify_completion_task
