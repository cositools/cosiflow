from datetime import datetime
import sys
sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from cosidag import cfg
from airflow.operators.python import ExternalPythonOperator
from airflow.models import Variable


def build_custom(dag):

    EXTERNAL_PYTHON = cfg("EXTERNAL_PYTHON", "/home/gamma/.conda/envs/cosipy/bin/python")
    LIB_DIR = cfg("TSMAP_LIB_DIR", "/home/gamma/airflow/pipeline/ts_map")

    # ----- Python callables executed in the external interpreter -----

    def _bin_grb(run_dir: str, lib_dir: str, grb_file: str) -> str:
        import sys
        sys.path.insert(0, lib_dir)
        from cosipipe_tsmap_ops_cosidag import bin_grb_data
        return bin_grb_data(grb_file, run_dir)

    def _bin_bkg(run_dir: str, lib_dir: str, background_file: str) -> str:
        import sys
        sys.path.insert(0, lib_dir)
        from cosipipe_tsmap_ops_cosidag import bin_background_data
        return bin_background_data(background_file, run_dir)

    def _ts_map(run_dir: str, lib_dir: str, grb_file: str, background_file: str, 
                orientation_file: str, response_file: str) -> str:
        import sys
        sys.path.insert(0, lib_dir)
        from cosipipe_tsmap_ops_cosidag import compute_ts_map
        return compute_ts_map(grb_file, background_file, orientation_file, response_file, run_dir)

    def _ts_map_mulres(run_dir: str, lib_dir: str, grb_file: str, background_file: str, 
                       orientation_file: str, response_file: str) -> str:
        import sys
        sys.path.insert(0, lib_dir)
        from cosipipe_tsmap_ops_cosidag import compute_ts_map_mulres
        return compute_ts_map_mulres(grb_file, background_file, orientation_file, response_file, run_dir)

    # ----- Operators (IMPORTANT: pass dag=dag) -----

    # Pull the run_dir produced by COSIDAG's sensor:
    #   key='detected_folder' from task_id='check_new_file'
    RUN_DIR_JINJA = "{{ ti.xcom_pull(task_ids='check_new_file', key='detected_folder') }}"
    RUN_DIR = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='run_dir') }}"
    GRB_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='grb_file') }}"
    BKG_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='background_file') }}"
    ORI_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='orientation_file') }}"
    RSP_FILE = "{{ ti.xcom_pull(task_ids='resolve_inputs', key='response_file') }}"

    bin_grb = ExternalPythonOperator(
        task_id="bin_grb_source",
        python=EXTERNAL_PYTHON,
        python_callable=_bin_grb,
        op_kwargs={"run_dir": RUN_DIR, "lib_dir": LIB_DIR, "grb_file": GRB_FILE},
        dag=dag,  # <<< IMPORTANT
    )

    bin_bkg = ExternalPythonOperator(
        task_id="bin_background",
        python=EXTERNAL_PYTHON,
        python_callable=_bin_bkg,
        op_kwargs={"run_dir": RUN_DIR, "lib_dir": LIB_DIR, "background_file": BKG_FILE},
        dag=dag,  # <<< IMPORTANT
    )

    GRB_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_grb_source', key='return_value') }}"
    BKG_BINNED_FILE = "{{ ti.xcom_pull(task_ids='bin_background', key='return_value') }}"

    ts_map = ExternalPythonOperator(
        task_id="ts_map_computation",
        python=EXTERNAL_PYTHON,
        python_callable=_ts_map,
        op_kwargs={"run_dir": RUN_DIR, 
                   "lib_dir": LIB_DIR, 
                   "grb_file": GRB_BINNED_FILE, 
                   "background_file": BKG_BINNED_FILE, 
                   "orientation_file": ORI_FILE, 
                   "response_file": RSP_FILE},
        dag=dag,  # <<< IMPORTANT
    )

    ts_map_mulres = ExternalPythonOperator(
        task_id="ts_map_mulres_computation",
        python=EXTERNAL_PYTHON,
        python_callable=_ts_map_mulres,
        op_kwargs={"run_dir": RUN_DIR, 
                   "lib_dir": LIB_DIR, 
                   "grb_file": GRB_BINNED_FILE, 
                   "background_file": BKG_BINNED_FILE, 
                   "orientation_file": ORI_FILE, 
                   "response_file": RSP_FILE},
        dag=dag,  # <<< IMPORTANT
    )

    # [bin_grb, bin_bkg] >> aggregate >> [ts_map, ts_map_mulres]
    bin_grb >> [ts_map, ts_map_mulres]
    bin_bkg >> [ts_map, ts_map_mulres]


with COSIDAG(
    dag_id="cosidag_tsmap",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=["/home/gamma/workspace/data/tsmap"],
    level=3,
    # Let the sensor accept only the deepest-level leaf (products)
    only_basename="products",
    # Robustness against partial writes
    idle_seconds=5,
    min_files=1,
    # ready_marker="_SUCCESS",   # enable if you create a sentinel at end-of-write
    # controlled parallelism:
    max_active_runs=2,         # up to 2 DAG runs in parallel
    max_active_tasks=8,        # up to 8 tasks in parallel in the DAG
    concurrency=8,             # local alternative limit (Airflow <2.7)
    date_queries=f"=={datetime.now().strftime("%Y%m%d")}",
    file_patterns={
        "grb_file": "GRB*_unbinned_*.fits*",
        "background_file": "Total_BG*_unbinned_*_window.fits*",
        "orientation_file": "*.ori",
        "response_file": "Response*.h5",
    },
    select_policy="latest_mtime",   # oppure "first"
    build_custom=build_custom,
    tags=["cosidag", "tsmap"]
) as dag:
    pass
