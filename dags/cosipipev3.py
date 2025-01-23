from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago, timezone
import os
import time
import csv
import random
import logging
from logging.handlers import RotatingFileHandler
from inotify_simple import INotify, flags
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
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

		self.logger = logging.getLogger(__name__)

	def ingest_and_store_dl0(self, **kwargs):
		try:
			ti = kwargs['ti']
			new_file_path = ti.xcom_pull(key='new_file_path', task_ids='wait_for_new_file')
			if new_file_path:
				if not os.path.exists(new_file_path):
					raise FileNotFoundError(f"Input file {new_file_path} does not exist.")
				self.logger.info(f"Oldest DL0 file: {new_file_path}")
				os.makedirs(f'{self.heasarc_dir}/dl0', exist_ok=True)
				stored_file_path = f"{self.heasarc_dir}/dl0/{os.path.basename(new_file_path)}"
				os.rename(new_file_path, stored_file_path)
				self.logger.info(f"Stored DL0 file: {stored_file_path}")
				ti.xcom_push(key='stored_dl0_file', value=stored_file_path)
			else:
				self.logger.warning("No input files found in the directory. Exiting task gracefully.")
				raise AirflowSkipException("No input files found, skipping task.")
		except FileNotFoundError as e:
			self.logger.error(f"Error: {e}. Exiting task gracefully.")
			raise AirflowSkipException(f"File not found: {e}")
		except Exception as e:
			self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
			raise

	def ingest_and_store_dl0_sensor(self, **kwargs):
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
			filename = f"{output_dir}/{stage}_{os.path.basename(input_file)}"
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
		try:
			ti = kwargs['ti']
			dl0_file = ti.xcom_pull(key='stored_dl0_file')
			if dl0_file:
				if not os.path.exists(dl0_file):
					raise FileNotFoundError(f"DL0 file {dl0_file} does not exist. It may have been processed by another instance.")
				filename = self.generate_placeholder_file(dl0_file, f'{self.heasarc_dir}/dl1a', 'dl1a')
				ti.xcom_push(key='stored_dl1a_file', value=filename)
			else:
				self.logger.warning("No DL0 file found in XCom. Exiting task gracefully.")
				raise AirflowSkipException("No DL0 file found in XCom, skipping task.")
		except FileNotFoundError as e:
			self.logger.error(f"Error: {e}. Exiting task gracefully.")
			raise AirflowSkipException(f"File not found: {e}")
		except Exception as e:
			self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
			raise

	def generate_dl1b(self, **kwargs):
		try:
			ti = kwargs['ti']
			dl1a_file = ti.xcom_pull(key='stored_dl1a_file', task_ids='generate_dl1a')
			if dl1a_file:
				if not os.path.exists(dl1a_file):
					raise FileNotFoundError(f"DL1a file {dl1a_file} does not exist.")
				filename = self.generate_placeholder_file(dl1a_file, f'{self.heasarc_dir}/dl1b', 'dl1b')
				ti.xcom_push(key='stored_dl1b_file', value=filename)
			else:
				self.logger.warning("No DL1a file found in XCom. Exiting task gracefully.")
				raise AirflowSkipException("No DL1a file found in XCom, skipping task.")
		except FileNotFoundError as e:
			self.logger.error(f"Error: {e}. Stopping pipeline.")
			raise
		except Exception as e:
			self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
			raise

	def generate_dl1c(self, **kwargs):
		try:
			ti = kwargs['ti']
			dl1b_file = ti.xcom_pull(key='stored_dl1b_file', task_ids='generate_dl1b')
			if dl1b_file:
				if not os.path.exists(dl1b_file):
					raise FileNotFoundError(f"DL1b file {dl1b_file} does not exist.")
				filename = self.generate_placeholder_file(dl1b_file, f'{self.heasarc_dir}/dl1c', 'dl1c')
				ti.xcom_push(key='stored_dl1c_file', value=filename)
			else:
				self.logger.warning("No DL1b file found in XCom. Exiting task gracefully.")
				raise AirflowSkipException("No DL1b file found in XCom, skipping task.")
		except FileNotFoundError as e:
			self.logger.error(f"Error: {e}. Stopping pipeline.")
			raise
		except Exception as e:
			self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
			raise

	def generate_dl2(self, **kwargs):
		try:
			ti = kwargs['ti']
			dl1c_file = ti.xcom_pull(key='stored_dl1c_file', task_ids='generate_dl1c')
			if dl1c_file:
				if not os.path.exists(dl1c_file):
					raise FileNotFoundError(f"DL1c file {dl1c_file} does not exist.")
				filename = self.generate_placeholder_file(dl1c_file, f'{self.heasarc_dir}/dl2', 'dl2')
				ti.xcom_push(key='stored_dl2_file', value=filename)
			else:
				self.logger.warning("No DL1c file found in XCom. Exiting task gracefully.")
				raise AirflowSkipException("No DL1c file found in XCom, skipping task.")
		except FileNotFoundError as e:
			self.logger.error(f"Error: {e}. Stopping pipeline.")
			raise
		except Exception as e:
			self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
			raise

	def fast_transient_stage_one(self, **kwargs):
		try:
			ti = kwargs['ti']
			dl2_file = ti.xcom_pull(key='stored_dl2_file', task_ids='generate_dl2')
			if dl2_file:
				if not os.path.exists(dl2_file):
					raise FileNotFoundError(f"DL2 file {dl2_file} does not exist.")
				filename = self.generate_placeholder_file(dl2_file, f'{self.heasarc_dir}/fast_transient_stage_1', 'stage1')
				ti.xcom_push(key='stored_stage1_file', value=filename)
			else:
				self.logger.warning("No DL2 file found in XCom. Exiting task gracefully.")
				raise AirflowSkipException("No DL2 file found in XCom, skipping task.")
		except FileNotFoundError as e:
			self.logger.error(f"Error: {e}. Stopping pipeline.")
			raise
		except Exception as e:
			self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
			raise

	def fast_transient_stage_two(self, **kwargs):
		try:
			ti = kwargs['ti']
			dl2_file = ti.xcom_pull(key='stored_dl2_file', task_ids='generate_dl2')
			if dl2_file:
				if not os.path.exists(dl2_file):
					raise FileNotFoundError(f"DL2 file {dl2_file} does not exist.")
				filename = self.generate_placeholder_file(dl2_file, f'{self.heasarc_dir}/fast_transient_stage_2', 'stage2')
				ti.xcom_push(key='stored_stage2_file', value=filename)
			else:
				self.logger.warning("No DL2 file found in XCom. Exiting task gracefully.")
				raise AirflowSkipException("No DL2 file found in XCom, skipping task.")
		except FileNotFoundError as e:
			self.logger.error(f"Error: {e}. Stopping pipeline.")
			raise
		except Exception as e:
			self.logger.error(f"Unexpected error: {e}. Stopping pipeline.")
			raise


	def fast_transient_stage_three(self, **kwargs):
		try:
			ti = kwargs['ti']
			input_files = ti.xcom_pull(key='stored_stage2_file', task_ids='fast_transient_stage_two')
			if not os.path.exists(input_files):
				raise FileNotFoundError(f"stage 2 file {dl2_file} does not exist.")
			filename = self.generate_placeholder_file(input_files, f'{self.heasarc_dir}/fast_transient_stage_3', 'stage3')
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

	def check_new_file(self, **kwargs):
		try:
			ti = kwargs['ti']
			for event in self.inotify.read(timeout=100000):  # Wait for 1 second for an event
				if flags.CLOSE_WRITE in flags.from_mask(event.mask):
					file_path = f"{self.base_dir}/input/{event.name}"
					self.logger.info(f"File {event.name} has been written and closed in the input directory.")
					ti.xcom_push(key='new_file_path', value=file_path)
					return True
		except Exception as e:
			self.logger.error(f"Unexpected error while monitoring directory: {e}")
			raise
		self.logger.info("No new file events detected. Continuing to monitor...")
		return False

	def check_new_file(self, **kwargs):
		try:
			ti = kwargs['ti']
			for event in self.inotify.read(timeout=100000):  
				if flags.CLOSE_WRITE in flags.from_mask(event.mask):
					self.logger.info(f"File {event.name} has been written and closed in the input directory.")
					ti.xcom_push(key='new_file_path', value=event.name)
					return True
		except Exception as e:
			self.logger.error(f"Unexpected error while monitoring directory: {e}")
			raise
		self.logger.info("No new file events detected. Continuing to monitor...")
		return False

	def check_new_file_sensor(self, **kwargs):
		ti = kwargs['ti']
		pipeline.logger.info("Daemon process started for continuous file monitoring...")

		while True:
			input_files = os.listdir(f'{pipeline.base_dir}/input')
			
			# Check if there are any files
			if input_files:
				# Get the oldest file
				oldest_file = min([f"{pipeline.base_dir}/input/{f}" for f in input_files], key=os.path.getctime)
				
				if os.path.exists(oldest_file):
					# Log and push to XCom
					pipeline.logger.info(f"New file detected: {oldest_file}")
					ti.xcom_push(key='new_file_path', value=oldest_file)
					
					# Allow subsequent tasks to run
					return True

			# Sleep before next check to avoid high CPU usage
			time.sleep(5)

pipeline = DataPipeline()

# DAG for processing DL0 and subsequent steps
with DAG('cosi_data_analysis_pipeline_v3', default_args={'owner': 'airflow'}, schedule=None, 
		start_date=datetime.now(),
		max_active_tasks=5,  # Maximum number of tasks that can be executed simultaneously per DAG
		max_active_runs=4  # Maximum number of DAG instances that can be executed simultaneously
		) as dag:
	
	#wait_for_new_file = PythonOperator(
	#	task_id='wait_for_new_file',
	#	python_callable=pipeline.check_new_file,
	#	provide_context=True
	#)

	# ingest_and_store_dl0_task = PythonOperator(
	# 	task_id='ingest_and_store_dl0',
	# 	python_callable=pipeline.ingest_and_store_dl0,
	# 	provide_context=True
	# )

	wait_for_new_file_sensor_task = PythonOperator(
		task_id='wait_for_new_file_sensor_task',
		python_callable=pipeline.check_new_file_sensor,
		provide_context=True,
		dag=dag
	)



	ingest_and_store_dl0_task_sensor = PythonOperator(
		task_id='ingest_and_store_dl0_sensor',
		python_callable=pipeline.ingest_and_store_dl0_sensor,
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

		# Definisci il task per triggerare il DAG stesso
	trigger_next_run = TriggerDagRunOperator(
		task_id="trigger_next_run",
		trigger_dag_id="cosi_data_analysis_pipeline_v3",  # Stesso DAG
		dag=dag,
	)

	wait_for_new_file_sensor_task >> ingest_and_store_dl0_task_sensor >> generate_dl1a_task >> generate_dl1b_task >> generate_dl1c_task >> generate_dl2_task >> [fast_transient_stage_one_task, fast_transient_stage_two_task]
	fast_transient_stage_two_task >> fast_transient_stage_three_task >> trigger_next_run
