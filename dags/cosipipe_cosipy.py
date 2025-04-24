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
import shutil
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
        # Avoid duplicate logger handlers
        if not self.logger.hasHandlers():
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
        
        self.logger.propagate = False

    # Monitor input directory for new files and return the oldest file when available
    def check_new_file_sensor(self, **kwargs):
        ti = kwargs['ti']
        self.logger.info("Daemon process started for continuous file monitoring...")

        # Start infinite polling loop to check for input files
        while True:
            input_directory = os.path.join(self.base_dir, 'input')
            input_files = os.listdir(input_directory)
            
            # Check if there are any files
            if input_files:
                # Find and return the path to the oldest file in the input directory
                oldest_file = min([f"{pipeline.base_dir}/input/{f}" for f in input_files], key=os.path.getctime)
                if os.path.exists(oldest_file):
                    # Log and push to XCom
                    self.logger.info(f"New file detected: {oldest_file}")
                    # Push file path to XCom for downstream tasks
                    ti.xcom_push(key='new_file_path', value=oldest_file)
                    # Allow subsequent tasks to run
                    return True

            # Sleep between checks to reduce CPU usage
            time.sleep(5)
   
    # Move detected input file into a timestamped subdirectory inside heasarc
    # Store and push the new path for downstream tasks
    def ingest_and_store_dl0_sensor(self, **kwargs):
        try:
            ti = kwargs['ti']
            # Retrieve the input file path from XCom
            input_files = ti.xcom_pull(key='new_file_path', task_ids='wait_for_new_file_sensor_task')
            if input_files:
                # Check that the file exists and move it into a new timestamped subfolder
                if not os.path.exists(input_files):
                    raise FileNotFoundError(f"Input file {input_files} does not exist.")
                self.logger.info(f"Processing DL0 file: {input_files}")
                os.makedirs(f'{self.heasarc_dir}/dl0', exist_ok=True)
                timestamp_utc = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d_%H-%M-%S')
                new_dir = f'{self.heasarc_dir}/dl0/{timestamp_utc}'
                if os.path.isdir(input_files):
                    shutil.move(input_files, new_dir)
                else:
                    os.makedirs(os.path.dirname(new_dir), exist_ok=True)
                    shutil.move(input_files, new_dir)
                # List the files in the new directory and get the tar.gz file
                stored_file_path = os.path.join(new_dir, os.listdir(new_dir)[0])
                self.logger.info(f"Stored DL0 file: {stored_file_path}")
                # Push the new file path to XCom for further use
                ti.xcom_push(key='stored_dl0_file', value=stored_file_path)
            else:
                self.logger.warning("No input files found in the directory. Exiting task gracefully.")
                raise AirflowSkipException("No input files found, skipping task.")
        except FileNotFoundError as e:
            # Handle missing file or other unexpected exceptions gracefully
            self.logger.error(f"Error: {e}. Stopping pipeline.")
            raise AirflowSkipException(f"File not found: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
            raise


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
             'start_date': datetime.datetime(2025, 4, 23, 14, 0, 0),  # Set the start date for the DAG to a specific time.
             # NOTE: you cannot use datetime.datetime.now() since it is re-evaluated every time the file is imported (i.e. 
             #       every DAG parse by Airflow). This makes the start_date unstable, and can cause weird behavior like 
             #       no scheduled runs, or inconsistent triggers.
         },
         schedule_interval=datetime.timedelta(hours=2),  # Execute every 2 hours
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
