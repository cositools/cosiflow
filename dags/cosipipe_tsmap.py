# cosipipe_tsmap.py — COSIfest refactor
from __future__ import annotations

from pathlib import Path
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import ExternalPythonOperator
from airflow.models import Variable

# Config via Airflow Variables (fallback to env/defaults happens in ops module as well)
def _v(key, default=None):
    try:
        return Variable.get(key)
    except Exception:
        import os
        return os.environ.get(key, default)

BASE_DIR = Path(_v("TSMAP_INCOMING_DIR", "/home/gamma/workspace/data/tsmap"))
RUNS_DIR = Path(_v("TSMAP_RUNS_DIR", str(BASE_DIR)))
RUNS_DIR.mkdir(parents=True, exist_ok=True)

EXTERNAL_PYTHON = _v("EXTERNAL_PYTHON", "/home/gamma/.conda/envs/cosipy/bin/python")
FILE_STABILITY_SECONDS = int(_v("TSMAP_FILE_STABILITY_S", "10"))

LIB_DIR = _v("TSMAP_LIB_DIR", "/home/gamma/airflow/pipeline/ts_map")

ARCHIVE_EXT = tuple(_v("TSMAP_ARCHIVE_EXT", ".zip,.tar.gz,.tgz,.tar").split(","))

default_args = {
    "owner": "gamma",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id="cosipipe_tsmap",
    start_date=datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Rome")),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["cosipy", "tsmap", "cosifest", "handson"],
    description="TS Map pipeline that waits for a zipped archive and processes it with COSIpy.",
)

# =========[ TASK CALLABLES ]=========
def _find_new_archive(**kwargs) -> bool:
    import time
    from pathlib import Path
    from datetime import datetime
    from zoneinfo import ZoneInfo

    ti = kwargs.get("ti")
    if ti is None:
        raise RuntimeError("Task instance (ti) not found; cannot push XCom.")

    now = datetime.now(tz=ZoneInfo("Europe/Rome"))
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    candidates = []
    for p in Path(BASE_DIR).glob("*"):
        if p.is_file() and any(p.name.endswith(ext) for ext in ARCHIVE_EXT):
            st = p.stat()
            if (now.timestamp() - st.st_mtime) < FILE_STABILITY_SECONDS:
                continue
            if datetime.fromtimestamp(st.st_mtime, tz=ZoneInfo("Europe/Rome")) < today_start:
                continue
            candidates.append(p)

    if not candidates:
        print("[find_new_archive] No stable archives from today yet.")
        return False

    picked = max(candidates, key=lambda p: p.stat().st_mtime)
    ti.xcom_push(key="archive_path", value=str(picked))
    print(f"[find_new_archive] Selected: {picked}")
    return True

def _decompress_archive(archive_path: str, runs_dir: str, lib_dir: str) -> str:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_tsmap_ops import decompress_archive
    return decompress_archive(archive_path, runs_dir)

def _validate_inputs(run_dir: str, lib_dir: str) -> None:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_tsmap_ops import validate_inputs_tsmap
    return validate_inputs_tsmap(run_dir)

def _bin_grb(run_dir: str, lib_dir: str) -> str:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_tsmap_ops import bin_grb_data
    return bin_grb_data(run_dir)

def _bin_bkg(run_dir: str, lib_dir: str) -> str:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_tsmap_ops import bin_background_data
    return bin_background_data(run_dir)

def _aggregate(run_dir: str, lib_dir: str):
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_tsmap_ops import aggregate_data
    return aggregate_data(run_dir)

def _ts_map(run_dir: str, lib_dir: str) -> str:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_tsmap_ops import compute_ts_map
    return compute_ts_map(run_dir)

def _ts_map_mulres(run_dir: str, lib_dir: str) -> str:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_tsmap_ops import compute_ts_map_mulres
    return compute_ts_map_mulres(run_dir)

with dag:
    wait_for_archive = PythonSensor(
        task_id="wait_for_archive",
        python_callable=_find_new_archive,
        poke_interval=10,
        timeout=60 * 60 * 24,
        mode="poke",
    )

    decompress_archive = ExternalPythonOperator(
        task_id="decompress_archive",
        python=EXTERNAL_PYTHON,
        python_callable=_decompress_archive,
        op_kwargs={
            "archive_path": "{{ ti.xcom_pull(task_ids='wait_for_archive', key='archive_path') }}",
            "runs_dir": str(RUNS_DIR),
            "lib_dir": LIB_DIR,
        },
    )

    validate_inputs = ExternalPythonOperator(
        task_id="validate_inputs",
        python=EXTERNAL_PYTHON,
        python_callable=_validate_inputs,
        op_kwargs={
            "run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
            "lib_dir": LIB_DIR,
        },
    )

    bin_grb = ExternalPythonOperator(
        task_id="bin_grb_source",
        python=EXTERNAL_PYTHON,
        python_callable=_bin_grb,
        op_kwargs={
            "run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
            "lib_dir": LIB_DIR,
        },
    )

    bin_bkg = ExternalPythonOperator(
        task_id="bin_background",
        python=EXTERNAL_PYTHON,
        python_callable=_bin_bkg,
        op_kwargs={
            "run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
            "lib_dir": LIB_DIR,
        },
    )

    aggregate = ExternalPythonOperator(
        task_id="data_aggregation",
        python=EXTERNAL_PYTHON,
        python_callable=_aggregate,
        op_kwargs={
            "run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
            "lib_dir": LIB_DIR,
        },
    )

    ts_map = ExternalPythonOperator(
        task_id="ts_map_computation",
        python=EXTERNAL_PYTHON,
        python_callable=_ts_map,
        op_kwargs={
            "run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
            "lib_dir": LIB_DIR,
        },
    )

    ts_map_mulres = ExternalPythonOperator(
        task_id="ts_map_mulres_computation",
        python=EXTERNAL_PYTHON,
        python_callable=_ts_map_mulres,
        op_kwargs={
            "run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
            "lib_dir": LIB_DIR,
        },
    )

    # Orchestration
    wait_for_archive >> decompress_archive
    decompress_archive >> validate_inputs >> [bin_grb, bin_bkg]
    [bin_grb, bin_bkg] >> aggregate
    aggregate >> [ts_map, ts_map_mulres]