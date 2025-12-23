from datetime import datetime
from pydoc import describe
import sys

# Add cosiflow modules path
sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def build_custom(dag):
    """
    Minimal COSIDAG example.
    Shows how to attach a single custom task to the COSIDAG lifecycle.
    """

    def _set_detected_folder(ti):
        ti.xcom_push(
            key="detected_folder",
            value="/home/gamma/workspace/data/tutorials"
        )
        return "/home/gamma/workspace/data/tutorials"

    set_detected_folder = PythonOperator(
        task_id="set_detected_folder",
        # Set the detected folder for the DAG, it is used by the COSIDAG to the folder for the show_results task.
        # This task is not part of the COSIDAG workflow, it is used to set the detected folder for the DAG.
        python_callable=_set_detected_folder,
        dag=dag,
    )
    
    hello_world = BashOperator( 
        task_id="hello_world",
        bash_command="echo 'Hello from COSIDAG' > /home/gamma/workspace/data/tutorials/hello_world.txt",
        dag=dag,
    )

    set_detected_folder >> hello_world  

with COSIDAG(
    dag_id="cosidag_helloworld",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=None,  # dummy folder for demo
    auto_retrig=False,
    level=0,
    build_custom=build_custom,
    tags=["cosidag", "example", "helloworld", "tutorial"],
) as dag:
    pass
