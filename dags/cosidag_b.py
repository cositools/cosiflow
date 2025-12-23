# cosidag_tutorial_b.py
from datetime import datetime
import sys

sys.path.append("/home/gamma/airflow/modules")

from cosidag import COSIDAG
from airflow.operators.python import PythonOperator, ExternalPythonOperator

EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"

BASE_DIR = "/home/gamma/workspace/data/tutorials/a_b_factor"
PKL_PATH = f"{BASE_DIR}/factors.pkl"
BIN_THR = 0.5


def build_custom(dag):
    """
    Tutorial B:
    reconstruct matrix from A/B factors and produce plots.
    """

    # -------------------------------------------------
    # 0) Declare the result folder (same as A)
    # -------------------------------------------------
    def _set_detected_folder(ti):
        ti.xcom_push(key="detected_folder", value=BASE_DIR)
        return BASE_DIR

    set_detected_folder = PythonOperator(
        task_id="set_detected_folder",
        python_callable=_set_detected_folder,
        dag=dag,
    )

    # -------------------------------------------------
    # 1) Reconstruction + plots (external env)
    # -------------------------------------------------
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
        
    b_reconstruct = ExternalPythonOperator(
        task_id="b_reconstruct_and_plot",
        python=EXTERNAL_PYTHON,
        python_callable=_b_reconstruct_and_plot,  # IDENTICA al tutorial originale
        op_kwargs={
            "base_dir": BASE_DIR,
            "pkl_path": PKL_PATH,
            "bin_thr": BIN_THR,
        },
        dag=dag,
    )

    set_detected_folder >> b_reconstruct


with COSIDAG(
    dag_id="cosidag_tutorial_b_reconstruct",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    monitoring_folders=None,
    auto_retrig=False,
    build_custom=build_custom,
    tags=["cosidag", "tutorial", "reconstruction", "external-python"],
) as dag:
    pass
