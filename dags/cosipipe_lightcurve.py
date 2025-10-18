# cosipipe_lightcurve.py
#
# DAG to generate a GRB light curve from a newly arrived compressed folder.
# Sequence:
#   1) Wait for a new compressed folder under /home/gamma/workspace/data/lcurve
#   2) Decompress it into a run directory
#   3) Bin the GRB source if not already present
#   4) Bin the background if not already present
#   5) Produce the light curve plot following the notebook cells
#
# Parallelism:
#   1 -> 2
#   2 -> [3, 4]
#   [3, 4] -> 5
#
# Uses ExternalPythonOperator with the cosipy environment interpreter.

from __future__ import annotations

import os
import time
import tarfile
import zipfile
from pathlib import Path
from datetime import datetime

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import ExternalPythonOperator, get_current_context

# =========[ CONFIG ]=========
BASE_DIR = Path("/home/gamma/workspace/data/lcurve")  # incoming archives directory
RUNS_DIR = BASE_DIR / "runs"                          # where each run is extracted
RUNS_DIR.mkdir(parents=True, exist_ok=True)
# External Python interpreter (your cosipy conda env)
EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"
# File stability before pickup (seconds since last mtime update)
FILE_STABILITY_SECONDS = 10
# Supported archive extensions
ARCHIVE_EXT = (".zip", ".tar.gz", ".tgz", ".tar")
# Web base URL for the Airflow webserver + plugin mount
PLUGIN_BASE_URL = "http://agilehost3.iasfbo.inaf.it:8080/heasarcbrowser"
PLUGIN_ROOT_DIR = str("/home/gamma/workspace/data")  # the explorer browses lcurve/, so make it the root

# =========[ TASK CALLABLES ]=========
LIB_DIR = "/home/gamma/airflow/pipeline/lcurve"
RUNS_DIR_STR = str(RUNS_DIR)

def _find_new_archive(**kwargs) -> bool:
    """
    Scan BASE_DIR for *stable* compressed archives whose mtime is on/after today's date
    (Europe/Rome, local midnight). Do not move files; just select one and push its path
    to XCom as 'archive_path'. If none match, return False so the sensor keeps poking.

    Stable = now - mtime >= FILE_STABILITY_SECONDS
    Date filter = mtime >= today_start (00:00 local day in Europe/Rome)

    Selection policy: pick the MOST RECENT stable archive from today.
    """
    import time
    from pathlib import Path
    from zoneinfo import ZoneInfo
    from datetime import datetime

    ti = kwargs.get("ti")
    if ti is None:
        raise RuntimeError("Task instance (ti) not found; cannot push XCom.")

    base = Path(BASE_DIR)
    if not base.exists():
        # Directory not yet present; keep waiting
        return False

    # Compute "today" start at local midnight Europe/Rome
    # tz = ZoneInfo("Europe/Rome")
    now_local = datetime.now()
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    today_start_ts = today_start.timestamp()

    # Collect candidate archives in BASE_DIR (flat, non-recursive)
    candidates = []
    for ext in ARCHIVE_EXT:  # e.g. (".zip", ".tar.gz", ".tgz", ".tar")
        candidates.extend(p for p in base.glob(f"*{ext}") if p.is_file())

    if not candidates:
        return False

    now = time.time()
    # Keep only stable files
    stable = [p for p in candidates if (now - p.stat().st_mtime) >= FILE_STABILITY_SECONDS]
    if not stable:
        return False

    # Keep only files from "today" (mtime >= local midnight)
    todays = [p for p in stable if p.stat().st_mtime >= today_start_ts]
    if not todays:
        # Only older files exist -> keep waiting for new arrivals
        return False

    # Pick the MOST RECENT among today's stable files
    picked = max(todays, key=lambda p: p.stat().st_mtime)

    # Push to XCom and unblock the DAG
    ti.xcom_push(key="archive_path", value=str(picked))
    print(f"[find_new_archive] Selected archive (today & stable): {picked}")
    return True



def _decompress_archive(archive_path: str, runs_dir: str, lib_dir: str) -> str:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_lc_ops import decompress_archive
    return decompress_archive(archive_path, runs_dir)

def _validate_inputs(run_dir: str, lib_dir: str) -> None:
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_lc_ops import validate_inputs
    return validate_inputs(run_dir)

def _bin_grb_source(run_dir: str, lib_dir: str, **_):
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_lc_ops import bin_grb_source
    return bin_grb_source(run_dir)

def _bin_background(run_dir: str, lib_dir: str, **_):
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_lc_ops import bin_background
    return bin_background(run_dir)

def _plot_lightcurve_from_cells(run_dir: str, lib_dir: str,
                                plugin_base_url: str | None = None,
                                plugin_root_dir: str | None = None,
                                **_):
    import sys
    sys.path.insert(0, lib_dir)
    from cosipipe_lc_ops import plot_lightcurve_from_cells
    return plot_lightcurve_from_cells(
        run_dir,
        plugin_base_url=plugin_base_url,
        plugin_root_dir=plugin_root_dir,
    )




# =========[ DAG DEFINITION ]=========

default_args = {
    "owner": "gamma",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="cosipipe_lightcurve",
    description="Light curve pipeline (wait -> decompress -> [bin GRB, bin BG] -> plot) following lc_generation notebook cells",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cosipy", "handson", "lightcurve", "extpythonenv", "tutorial"],
) as dag:
    
    wait_for_archive = PythonSensor(
        task_id="wait_for_archive",
        python_callable=_find_new_archive,
        poke_interval=10,
        timeout=60 * 60 * 24,
        mode="poke",
    )


    # Helper string evaluated in the main env (safe to use RUNS_DIR here)
    RUNS_DIR_STR = str(RUNS_DIR)

    # usa direttamente BASE_DIR come destinazione:
    decompress_archive = ExternalPythonOperator(
        task_id="decompress_archive",
        python=EXTERNAL_PYTHON,
        python_callable=_decompress_archive,
        op_kwargs={
            "archive_path": "{{ ti.xcom_pull(task_ids='wait_for_archive', key='archive_path') }}",
            "runs_dir": str(BASE_DIR),   # <<--- così l’estrazione va in lcurve/<stem>
            "lib_dir": LIB_DIR,
        },
    )


    # pull run_dir from previous step via Jinja + XCom return_value
    run_dir_xcom = "{{ ti.xcom_pull(task_ids='decompress_archive') or ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}"

    bin_source = ExternalPythonOperator(
        task_id="bin_grb_source",
        python=EXTERNAL_PYTHON,
        python_callable=_bin_grb_source,
        op_kwargs={"run_dir": run_dir_xcom,
                   "lib_dir": LIB_DIR,},
        doc_md="""
        Bin the GRB source if `GRB_bn081207680_binned_O3.hdf5` does not exist.
        Exactly follows the notebook cell (BinnedData with `inputs_GRB__galactic.yaml`).
        """,
    )

    bin_background = ExternalPythonOperator(
        task_id="bin_background",
        python=EXTERNAL_PYTHON,
        python_callable=_bin_background,
        op_kwargs={"run_dir": run_dir_xcom,
                  "lib_dir": LIB_DIR,},
        doc_md="""
        Bin the background if `Total_BG_continuum_O3_binned.hdf5` does not exist.
        Exactly follows the notebook cell (BinnedData with `inputs_bkg__galactic.yaml`).
        """,
    )

    plot_lightcurve = ExternalPythonOperator(
        task_id="plot_lightcurve",
        python=EXTERNAL_PYTHON,
        python_callable=_plot_lightcurve_from_cells,
        op_kwargs={
            "run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive') or ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
            "lib_dir": LIB_DIR,
            "plugin_base_url": PLUGIN_BASE_URL,
            "plugin_root_dir": PLUGIN_ROOT_DIR,
        },
        doc_md="Save lightcurve.png and print Data Explorer links in logs.",
    )

    validate_inputs = ExternalPythonOperator(
        task_id="validate_inputs",
        python=EXTERNAL_PYTHON,
        python_callable=_validate_inputs,
        op_kwargs={"run_dir": "{{ ti.xcom_pull(task_ids='decompress_archive') or ti.xcom_pull(task_ids='decompress_archive', key='return_value') }}",
                   "lib_dir": LIB_DIR,},
        doc_md="Validate presence of YAML/FITS/ORI/Response before binning.",
    )


    # Dependencies
    wait_for_archive >> decompress_archive
    decompress_archive >> validate_inputs >> [bin_source, bin_background]
    [bin_source, bin_background] >> plot_lightcurve
