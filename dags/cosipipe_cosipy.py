from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import time
import datetime
import logging
from logging.handlers import RotatingFileHandler
from inotify_simple import INotify, flags
from airflow.exceptions import AirflowSkipException
from airflow.operators.dagrun_operator import TriggerDagRunOperator

#airflow dags trigger cosi_data_analysis_pipeline_v3
#airflow dags list-runs -d cosi_data_analysis_pipeline_v3 --state running


#AIRFLOW
class DataPipeline:
    def __init__(self):
        self.base_dir = '/home/gamma/workspace/data'
        self.heasarc_dir = '/home/gamma/workspace/heasarc'
        self.logger_dir = '/home/gamma/workspace/log'

        self.inotify = INotify()
        self.watch_flags = flags.CLOSE_WRITE
        self.inotify.add_watch(f'{self.base_dir}/input', self.watch_flags)

        # Logger setup for both Celery and the pipeline
        self.logger = logging.getLogger('data_pipeline_logger')
        self.logger.setLevel(logging.DEBUG)

        # File handler for logging to a file
        file_handler = RotatingFileHandler('/home/gamma/workspace/data_pipeline.log', maxBytes=5*1024*1024, backupCount=3)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        file_handler.setFormatter(file_formatter)

        # Console handler for logging to the console
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console_handler.setFormatter(console_formatter)

        # Adding handlers to the logger
        if not self.logger.hasHandlers():
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
        
        self.logger.propagate = False

    # This method checks if new file are written inside the input directory.
    def check_new_file_sensor(self, **kwargs):
        ti = kwargs['ti']
        self.logger.info("Daemon process started for continuous file monitoring...")

        while True:
            input_files = os.listdir(f'{self.base_dir}/input')
            
            # Check if there are any files
            if input_files:
                # Get the oldest file
                oldest_file = min([f"{pipeline.base_dir}/input/{f}" for f in input_files], key=os.path.getctime)
                if os.path.exists(oldest_file):
                    # Log and push to XCom
                    self.logger.info(f"New file detected: {oldest_file}")
                    ti.xcom_push(key='new_file_path', value=oldest_file)
                    # Allow subsequent tasks to run
                    return True

            # Sleep before next check to avoid high CPU usage
            time.sleep(5)
   
    # This method receives the input path from the file sensor and move the file in the workspace
    def ingest_and_store_dl0_sensor(self, **kwargs):
        try:
            ti = kwargs['ti']
            input_files = ti.xcom_pull(key='new_file_path', task_ids='wait_for_new_file_sensor_task')
            if input_files:
                #oldest_file = min([f"{self.base_dir}/input/{f}" for f in input_files], key=os.path.getctime)
                if not os.path.exists(input_files):
                    raise FileNotFoundError(f"Input file {input_files} does not exist.")
                self.logger.info(f"Processing DL0 file: {input_files}")
                os.makedirs(f'{self.heasarc_dir}/dl0', exist_ok=True)
                timestamp_utc = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d_%H-%M-%S')
                new_dir = f'{self.heasarc_dir}/dl0/{timestamp_utc}'
                os.makedirs(new_dir, exist_ok=True)
                stored_file_path = f"{new_dir}/{os.path.basename(input_files)}"
                os.rename(input_files, stored_file_path)
                self.logger.info(f"Stored DL0 file: {stored_file_path}")
                ti.xcom_push(key='stored_dl0_file', value=stored_file_path)
            else:
                self.logger.warning("No input files found in the directory. Exiting task gracefully.")
                raise AirflowSkipException("No input files found, skipping task.")
        except FileNotFoundError as e:
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise AirflowSkipException(f"File not found: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise


pipeline = DataPipeline()

# DAG for processing DL0 and subsequent steps
with DAG('cosipy_test_v0', default_args={'owner': 'airflow'}, schedule=None, 
        #start_date=datetime.now(),
        max_active_tasks=5,  # Maximum number of tasks that can be executed simultaneously per DAG
        max_active_runs=4  # Maximum number of DAG instances that can be executed simultaneously
        ) as dag:

    wait_for_new_file_sensor_task = PythonOperator(
        task_id='wait_for_new_file_sensor_task',
        python_callable=pipeline.check_new_file_sensor,
        dag=dag
    )
    
    ingest_and_store_dl0_task_sensor = PythonOperator(
        task_id='ingest_and_store_dl0_sensor',
        python_callable=pipeline.ingest_and_store_dl0_sensor,
    )
 
    # Definisci il task per triggerare il DAG stesso
    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
		trigger_dag_id="cosipy_test_v0",  # Stesso DAG
		dag=dag,
	)
    
    # Task 2 (BashOperator) che esegue il comando nel nuovo ambiente
    generate_plots = BashOperator(
        task_id='generate_plots',
        bash_command="""
            source activate cosipy && 
            python /shared_dir/pipeline/generate_plot.py  "{{ task_instance.xcom_pull(task_ids='ingest_and_store_dl0_sensor', key='stored_dl0_file') }}"
        """,
        dag=dag,
    )

    wait_for_new_file_sensor_task  >> ingest_and_store_dl0_task_sensor >> generate_plots  >> trigger_next_run
    