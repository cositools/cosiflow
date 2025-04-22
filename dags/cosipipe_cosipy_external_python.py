from airflow import DAG
from airflow.operators.python import PythonOperator, ExternalPythonOperator
from airflow.operators.bash_operator import BashOperator
import os
import time
import datetime
import logging
from logging.handlers import RotatingFileHandler
from airflow.exceptions import AirflowSkipException
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.decorators import task, dag

# Import required modules and operators from Airflow and standard Python libraries

# Define a class to encapsulate the data ingestion and logging logic
class DataPipeline:
    def __init__(self):
        # Set base directories for input, output, and logs
        self.base_dir = '/home/gamma/workspace/data'
        self.heasarc_dir = '/home/gamma/workspace/heasarc'
        self.logger_dir = '/home/gamma/workspace/log'

        # Configure logger for both file and console output
        self.logger = logging.getLogger('data_pipeline_logger')
        self.logger.setLevel(logging.DEBUG)

        # Add rotating file handler to limit log file size
        file_handler = RotatingFileHandler('/home/gamma/workspace/data_pipeline.log', maxBytes=5*1024*1024, backupCount=3)
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        file_handler.setFormatter(file_formatter)

        # Add console stream handler for real-time feedback
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        console_handler.setFormatter(console_formatter)

        # Avoid adding handlers multiple times
        if not self.logger.hasHandlers():
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
        
        self.logger.propagate = False

    # Continuously monitor the input directory for new files
    def check_new_file_sensor(self, **kwargs):
        ti = kwargs['ti']
        self.logger.info("Daemon process started for continuous file monitoring...")

        while True:
            # List files in the input directory
            input_directory = os.path.join(self.base_dir, 'input')
            input_files = os.listdir(input_directory)
            
            # Select the oldest file available
            if input_files:
                oldest_file = min([f"{pipeline.base_dir}/input/{f}" for f in input_files], key=os.path.getctime)
                if os.path.exists(oldest_file):
                    # Push file path to XCom for downstream tasks
                    self.logger.info(f"New file detected: {oldest_file}")
                    ti.xcom_push(key='new_file_path', value=oldest_file)
                    # Wait a short time before the next polling iteration
                    return True

            # Sleep before next check to avoid high CPU usage
            time.sleep(5)
   
    # Move detected input file to a timestamped directory and store the path
    def ingest_and_store_dl0_sensor(self, **kwargs):
        try:
            ti = kwargs['ti']
            # Retrieve file path from XCom
            input_files = ti.xcom_pull(key='new_file_path', task_ids='wait_for_new_file_sensor_task')
            if input_files:
                # Check if the file exists before proceeding
                if not os.path.exists(input_files):
                    raise FileNotFoundError(f"Input file {input_files} does not exist.")
                self.logger.info(f"Processing DL0 file: {input_files}")
                # Create directory structure for storing the file
                os.makedirs(f'{self.heasarc_dir}/dl0', exist_ok=True)
                timestamp_utc = datetime.datetime.now(datetime.UTC).strftime('%Y-%m-%d_%H-%M-%S')
                new_dir = f'{self.heasarc_dir}/dl0/{timestamp_utc}'
                os.makedirs(new_dir, exist_ok=True)
                # Rename (move) the file to the new directory
                stored_file_path = f"{new_dir}/{os.path.basename(input_files)}"
                os.rename(input_files, stored_file_path)
                # Push the new file path to XCom
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

# Generate plots and summary data from the DL0 file using the cosipy library
def generate_plots_task(file_path):
    
    import sys, os
    from cosipy.util import fetch_wasabi_file
    from cosipy import BinnedData
    from pathlib import Path

    # Define the directory and create the input YAML configuration file
    print("test")
    print(file_path)
    dir_name = os.path.dirname(file_path)

    content_to_write = f"""#----------#
    # Data I/O:

    # data files available on the COSI Sharepoint: https://drive.google.com/drive/folders/1UdLfuLp9Fyk4dNussn1wt7WEOsTWrlQ6
    data_file: {file_path} # full path
    ori_file: "NA" # full path
    unbinned_output: 'hdf5' # 'fits' or 'hdf5'
    time_bins: 60 # time bin size in seconds. Takes int, float, or list of bin edges.
    energy_bins: [100.,  200.,  500., 1000., 2000., 5000.] # Takes list. Needs to match response.
    phi_pix_size: 6 # binning of Compton scattering anlge [deg]
    nside: 8 # healpix binning of psi chi local
    scheme: 'ring' # healpix binning of psi chi local
    tmin: 1835478000.0 # Min time cut in seconds.
    tmax: 1835485200.0 # Max time cut in seconds.
    #----------#
    """

    dir_name_path = Path(dir_name)

    # Open the file in write mode and write the content
    with open(dir_name_path / "inputs.yaml", "w") as file:
        file.write(content_to_write)

    # Run analysis steps: read .tra file, bin data, create spectrum and light curve
    analysis = BinnedData(dir_name_path / "inputs.yaml")
    analysis.read_tra(output_name=dir_name_path / "unbinned_data")
    analysis.get_binned_data()
    analysis.get_raw_spectrum(output_name=file_path.replace(".crab2hr.extracted.tra.gz", ""))
    analysis.get_raw_lightcurve(output_name=file_path.replace(".crab2hr.extracted.tra.gz", ""))


# Define the DAG and the task pipeline for DL0 processing and plotting
with DAG('cosipy_external_python_v2', default_args={'owner': 'airflow'}, schedule=None, 
        #start_date=datetime.now(),
        max_active_tasks=5,  # Maximum number of tasks that can be executed simultaneously per DAG
        max_active_runs=4  # Maximum number of DAG instances that can be executed simultaneously
        ) as dag:

    # Wait for new file to appear in input directory
    wait_for_new_file_sensor_task = PythonOperator(
        task_id='wait_for_new_file_sensor_task',
        python_callable=pipeline.check_new_file_sensor,
        dag=dag
    )
    
    # Move the file and store it in the appropriate location
    ingest_and_store_dl0_task_sensor = PythonOperator(
        task_id='ingest_and_store_dl0_sensor',
        python_callable=pipeline.ingest_and_store_dl0_sensor,
    )
 
    # Trigger the same DAG again to run continuously
    trigger_next_run = TriggerDagRunOperator(
        task_id="trigger_next_run",
        trigger_dag_id="cosipy_external_python_v2", 
        dag=dag,
    )
    
    # Run the plot generation script in an external Python environment
    generate_plots = ExternalPythonOperator(
        task_id='generate_plots',
        python_callable=generate_plots_task,
        python="/home/gamma/.conda/envs/cosipy/bin/python",
        op_args=["{{ task_instance.xcom_pull(task_ids='ingest_and_store_dl0_sensor', key='stored_dl0_file') }}"],
        dag=dag,
    )

    wait_for_new_file_sensor_task >> ingest_and_store_dl0_task_sensor >> generate_plots >> trigger_next_run