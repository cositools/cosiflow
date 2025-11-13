from datetime import datetime
# add the path to the cosiflow module
import sys
sys.path.append("/home/gamma/airflow/modules")
from cosidag import COSIDAG
from airflow.operators.python import PythonOperator
#
def build_custom(dag):
    # Example custom task consuming the detected folder via XCom
    def _process_folder(folder_path: str):
        # Do your science here
        print(f"Processing folder: {folder_path}")
        # search for the file by pattern
        file_path = dag.find_file_by_pattern(r".*\.fits.*", folder_path)
        print(f"Found file: {file_path}")
#
    PythonOperator(
        task_id="custom_process",
        python_callable=lambda ti, **_: _process_folder(
            ti.xcom_pull(task_ids="check_new_file", key="detected_folder")
        ),
        dag=dag,
    )
#
with COSIDAG(
    dag_id="cosidag_example",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=["/home/gamma/workspace/data/tsmap"],
    level=3,
    date=datetime.now().strftime("%Y%m%d"),
    build_custom=build_custom,
    idle_seconds=5,
    min_files=1,
    ready_marker=None,
    only_basename="products",
    tags=["cosidag", "example"],
    #ready_marker="_SUCCESS",
) as dag:
    pass
