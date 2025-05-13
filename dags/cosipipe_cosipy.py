from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import time
import datetime
import logging
import sys
sys.path.append('/shared_dir/pipeline')
from logging_config import get_data_pipeline_logger
from inotify_simple import INotify, flags
from airflow.exceptions import AirflowSkipException, AirflowFailException
from airflow.operators.dagrun_operator import TriggerDagRunOperator
import shutil
from functools import wraps

import random

# Decorator to handle exceptions in Airflow PythonOperator tasks.
# It logs meaningful messages for Airflow-specific exceptions (Skip/Fail)
# and also logs unexpected exceptions with full traceback for debugging.
def airflow_task_handler(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        # Attempt to use the instance logger if available, fallback to module logger
        logger = args[0].logger

        try:
            # Execute the actual task function
            return func(*args, **kwargs)
        except FileNotFoundError as e:
            logger.error(f"File not found: {e}", exc_info=True)
            raise AirflowSkipException(f"File not found: {e}")
        except AirflowSkipException as e:
            # Expected: task should be skipped (e.g., no input file)
            logger.warning(f"Task skipped: {e}", exc_info=True)
            raise
        except AirflowFailException as e:
            # Expected: task should fail intentionally without retries
            logger.error(f"Task failed intentionally: {e}", exc_info=True)
            raise
        except Exception as e:
            # Unexpected error: log with full traceback for debugging
            logger.error(f"Unexpected error in task '{func.__name__}': {e}", exc_info=True)
            raise
    return wrapper

# Import necessary Airflow classes and standard libraries

# Define a data pipeline class for monitoring, ingesting, and storing DL0 files
class DataPipeline:
    def __init__(self):
        # Define directory paths for input, processed data (heasarc), and logs
        self.base_dir = '/home/gamma/workspace/data'
        self.heasarc_dir = '/home/gamma/workspace/heasarc'
        self.logger_dir = '/home/gamma/workspace/log'

        # Set up inotify to watch the input directory for file-close-write events
        self.inotify = INotify()
        self.watch_flags = flags.CLOSE_WRITE
        self.inotify.add_watch(f'{self.base_dir}/input', self.watch_flags)

        # Configure logger with both file rotation and console output
        self.logger = get_data_pipeline_logger(self.logger_dir)
        
    
    # Return the path to the oldest file in the input directory, or None if the directory is empty
    @airflow_task_handler
    def get_oldest_file_in_input_dir(self) -> str:
        input_directory = os.path.join(self.base_dir, 'input')
        input_files = os.listdir(input_directory)
        if input_files:
            # Build full paths and get the file with the oldest creation time
            oldest_file = min(
                [os.path.join(input_directory, f) for f in input_files],
                key=os.path.getctime
            )
            if os.path.exists(oldest_file):
                return oldest_file
        return None

    # Move the given input file to a timestamped folder inside the heasarc directory
    # Return the full path to the stored file
    @airflow_task_handler
    def store_input_file(self, input_file: str) -> str:
        # Check if the file exists
        if not os.path.exists(input_file):
            raise FileNotFoundError(f"Input file {input_file} does not exist.")
        
        # ⚠️ Simulazione di errore
        if random.random() < 1/3:
            raise ConnectionError("Simulated connection error during file storage.")

        
        self.logger.info(f"Processing DL0 file: {input_file}")
        
        # Create base directory for storage
        os.makedirs(f'{self.heasarc_dir}/dl0', exist_ok=True)
        
        # Generate timestamp-based folder name
        timestamp_utc = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d_%H-%M-%S')
        new_dir = f'{self.heasarc_dir}/dl0/{timestamp_utc}'
        
        # Move the file or directory into the new destination
        if not os.path.isdir(input_file):
            os.makedirs(os.path.dirname(new_dir), exist_ok=True)
        shutil.copytree(input_file, new_dir)
        shutil.rmtree(input_file)
        
        # Extract and return the full path of the stored file
        stored_file_path = os.path.join(new_dir, os.listdir(new_dir)[0])
        self.logger.info(f"Stored DL0 file: {stored_file_path}")
        return stored_file_path

    @airflow_task_handler
    def check_new_file_sensor(self, **kwargs):
        ti = kwargs['ti']
        self.logger.info("Daemon process started for continuous file monitoring...")
        while True:
            oldest_file = self.get_oldest_file_in_input_dir()
            if oldest_file:
                self.logger.info(f"New file detected: {oldest_file}")
                ti.xcom_push(key='new_file_path', value=oldest_file)
                return True
            time.sleep(5)

    @airflow_task_handler
    def ingest_and_store_dl0_sensor(self, **kwargs):
        ti = kwargs['ti']
        input_file = ti.xcom_pull(key='new_file_path', task_ids='wait_for_new_file_sensor_task')
        if not input_file:
            self.logger.warning("No input files found in the directory. Exiting task gracefully.")
            raise FileNotFoundError("No input files found, skipping task.")
        stored_path = self.store_input_file(input_file)
        ti.xcom_push(key='stored_dl0_file', value=stored_path)

pipeline = DataPipeline()

# Define the Airflow DAG to orchestrate DL0 file monitoring, ingestion, and plotting
with DAG('cosipy_test_v0', default_args={'owner': 'airflow'}, schedule=None, 
        max_active_tasks=5,  # Maximum number of tasks that can be executed simultaneously per DAG
        max_active_runs=4  # Maximum number of DAG instances that can be executed simultaneously
        ) as dag:

    # Task to detect the arrival of new files in the input directory
    wait_for_new_file_sensor_task = PythonOperator(
        task_id='wait_for_new_file_sensor_task',
        python_callable=pipeline.check_new_file_sensor,
        dag=dag
    )
    
    # Task to move and organize the newly detected file
    ingest_and_store_dl0_task_sensor = PythonOperator(
        task_id='ingest_and_store_dl0_sensor',
        python_callable=pipeline.ingest_and_store_dl0_sensor,
    )
 
    # Task to generate plots using an external script in the cosipy environment
    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
		trigger_dag_id="cosipy_test_v0",  # Stesso DAG
		dag=dag,
	)
    
    # Task to trigger the same DAG again for continuous processing
    generate_plots = BashOperator(
        task_id='generate_plots',
        bash_command="""
            source activate cosipy && 
            python /shared_dir/pipeline/generate_plot.py  "{{ task_instance.xcom_pull(task_ids='ingest_and_store_dl0_sensor', key='stored_dl0_file') }}"
        """,
        dag=dag,
    )

    wait_for_new_file_sensor_task  >> ingest_and_store_dl0_task_sensor >> generate_plots  >> trigger_next_run

# Separate DAG that runs the initialization script every two hours and then triggers the main DAG
with DAG('cosipy_contactsimulator',
         default_args={
             'owner': 'airflow',
             'start_date': datetime.datetime(2025, 5, 6, 0, 0, 0),  # Set the start date for the DAG to a specific time.
             # NOTE: you cannot use datetime.datetime.now() since it is re-evaluated every time the file is imported (i.e. 
             #       every DAG parse by Airflow). This makes the start_date unstable, and can cause weird behavior like 
             #       no scheduled runs, or inconsistent triggers.
         },
         schedule_interval=datetime.timedelta(minutes=2),  # Execute every 2 hours
         catchup=False,  # Do not run past scheduled runs
         max_active_runs=1,  # Only one instance of this DAG can run at a time
         ) as init_dag:

    # Task to run the pipeline initialization script in the cosipy environment
    initialize_pipeline_task = BashOperator(
        task_id='initialize_pipeline_task',
        bash_command="""
            cd /shared_dir/pipeline &&
            source activate cosipy && 
            python /shared_dir/pipeline/initialize_pipeline.py
        """,
        dag=init_dag
    )
    
    # Task to move a specific initial file to a timestamped folder in the input directory
    copy_initfile_task = BashOperator(
        task_id='copy_initfile_task',
        bash_command="""
            FILE_NAME=GalacticScan.inc1.id1.crab2hr.extracted.tra.gz &&
            TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S") &&
            DEST_DIR="/home/gamma/workspace/data/input/$TIMESTAMP" &&
            mkdir -p "$DEST_DIR" &&
            mv "/home/gamma/workspace/data/$FILE_NAME" "$DEST_DIR/" 
        """,
        dag=init_dag
    )

    # Task to trigger the main DAG that handles monitoring and processing
    trigger_main_dag = TriggerDagRunOperator(
        task_id="trigger_cosipy_test_v0",
        trigger_dag_id="cosipy_test_v0",
        dag=init_dag
    )

    # Define task execution order
    initialize_pipeline_task >> copy_initfile_task >> trigger_main_dag
