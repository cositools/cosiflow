# b_dag.py
# Airflow 2.x — Bob: wait for factors.pkl, reconstruct L@R, plot float and binary images
from datetime import datetime

from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import ExternalPythonOperator

EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"

BASE_DIR = "/home/gamma/workspace/data/tutorials/a_b_factor"
PKL_PATH = f"{BASE_DIR}/factors.pkl"
BIN_THR = 0.5  # threshold to binarize reconstruction

def _file_exists(pkl_path: str) -> bool:
    """Sensor callable: returns True when the pickle file exists."""
    import os
    return os.path.exists(pkl_path)

def _b_reconstruct_and_plot(base_dir: str, pkl_path: str, bin_thr: float):
    """Run in external interpreter. Load L,R -> M=L@R; save float & binarized reconstructions."""
    from pathlib import Path
    import pickle
    import numpy as np
    import matplotlib.pyplot as plt

    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    img_rec_float = base / "reconstruction_float.png"
    img_rec_bin = base / "reconstruction_binary.png"

    with open(pkl_path, "rb") as f:
        payload = pickle.load(f)

    L = np.asarray(payload["L"], dtype=float)  # (32×k)
    R = np.asarray(payload["R"], dtype=float)  # (k×32)

    # 1) Reconstruct
    M = L @ R

    # 2) Save float heatmap
    plt.figure(figsize=(4, 4), dpi=120)
    plt.imshow(M, cmap="gray_r", interpolation="nearest")
    plt.title("Reconstruction (float)")
    plt.axis("off")
    plt.tight_layout(pad=0.2)
    plt.savefig(img_rec_float)
    plt.close()

    # 3) Save binarized heatmap (to match Alice's binary look)
    M_bin = (M >= bin_thr).astype(int)
    plt.figure(figsize=(4, 4), dpi=120)
    plt.imshow(M_bin, cmap="gray_r", interpolation="nearest")
    plt.title(f"Reconstruction (binary, thr={bin_thr})")
    plt.axis("off")
    plt.tight_layout(pad=0.2)
    plt.savefig(img_rec_bin)
    plt.close()

default_args = {
    "owner": "gamma",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="b_dag",
    default_args=default_args,
    description="B: wait for L,R factors, reconstruct L@R and re-plot the original matrix",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cosifest", "handson", "tutorial", "consumer", "linalg"],
) as dag:

    wait_for_factors = PythonSensor(
        task_id="wait_for_factors_pickle",
        python_callable=_file_exists,
        op_kwargs={"pkl_path": PKL_PATH},
        poke_interval=10,    # seconds
        timeout=60 * 60,     # 1 hour
        mode="poke",
    )

    b_reconstruct = ExternalPythonOperator(
        task_id="b_reconstruct_and_plot",
        python=EXTERNAL_PYTHON,
        python_callable=_b_reconstruct_and_plot,
        op_kwargs={
            "base_dir": BASE_DIR,
            "pkl_path": PKL_PATH,
            "bin_thr": BIN_THR,
        },
    )

    wait_for_factors >> b_reconstruct