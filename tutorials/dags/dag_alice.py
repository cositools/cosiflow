# alice_dag.py
# Airflow 2.x — Alice: build 32x32 binary text matrix, factorize via SVD, save A,B and plots
from datetime import datetime

from airflow import DAG
from airflow.operators.python import ExternalPythonOperator

EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"

# Defaults for the demo
TEXT = "DAGs\n  ARE\nCOOL!"
SIZE = [48, 48]      # pass lists in op_kwargs (safer JSON-serializable)
FONT_SIZE = 6
RANK = 12
BASE_DIR = "/home/gamma/workspace/data/tutorials/alice_bob_factor"

def _alice_make_factors(base_dir: str, text: str, size: list, font_size: int, rank: int):
    """Run entirely in the external 'cosipy' interpreter.
    Robustly measure multiline text size across Pillow versions (no draw.textsize).
    """
    from pathlib import Path
    import pickle
    import numpy as np
    import matplotlib
    matplotlib.use("Agg")  # safe non-interactive backend
    import matplotlib.pyplot as plt
    from PIL import Image, ImageDraw, ImageFont

    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    pkl_path = base / "factors.pkl"
    img_A = base / "factor_A.png"
    img_B = base / "factor_B.png"

    W, H = int(size[0]), int(size[1])

    # -- Load a mono font if available, otherwise default fallback
    try:
        font = ImageFont.truetype("DejaVuSansMono.ttf", font_size)
    except Exception:
        font = ImageFont.load_default()

    # -- Helper: robust multiline text bounding box across Pillow versions
    def measure_multiline(draw: ImageDraw.ImageDraw, txt: str, font: ImageFont.ImageFont):
        """Return (w, h) for multiline text. Tries modern APIs first, falls back gracefully."""
        if hasattr(draw, "multiline_textbbox"):
            left, top, right, bottom = draw.multiline_textbbox((0, 0), txt, font=font, align="center")
            return (right - left, bottom - top)
        if hasattr(draw, "textbbox"):
            lines = txt.splitlines() or [txt]
            widths, heights = [], []
            for line in lines:
                if line == "":
                    try:
                        ascent, descent = font.getmetrics()
                        lh = ascent + descent
                    except Exception:
                        lh = font.size
                    widths.append(0)
                    heights.append(lh)
                else:
                    l, t, r, b = draw.textbbox((0, 0), line, font=font)
                    widths.append(r - l)
                    heights.append(b - t)
            return (max(widths) if widths else 0, sum(heights) if heights else 0)
        # Fallback
        lines = txt.splitlines() or [txt]
        widths, heights = [], []
        for line in lines:
            try:
                w_line = draw.textlength(line, font=font)
            except Exception:
                w_line = max(1, int(len(line) * font.size * 0.6))
            widths.append(int(w_line))
            try:
                ascent, descent = font.getmetrics()
                lh = ascent + descent
            except Exception:
                lh = font.size
            heights.append(lh)
        return (max(widths) if widths else 0, sum(heights) if heights else 0)

    # -- 1) Render text -> binary matrix (0 white, 1 black)
    img = Image.new("L", (W, H), color=255)
    draw = ImageDraw.Draw(img)

    w, h = measure_multiline(draw, text, font)
    x = (W - w) // 2
    y = (H - h) // 2

    if hasattr(draw, "multiline_text"):
        draw.multiline_text((x, y), text, fill=0, font=font, align="center")
    else:
        lines = text.splitlines() or [text]
        cur_y = y
        for line in lines:
            try:
                ascent, descent = font.getmetrics()
                lh = ascent + descent
            except Exception:
                lh = font.size
            draw.text((x, cur_y), line, fill=0, font=font)
            cur_y += lh

    arr = np.array(img)
    X = (arr < 128).astype(float)  # binary 0/1 as float

    # -- 2) SVD factorization: X ≈ (U_k sqrt(S)) (sqrt(S) V_k^T)
    U, s, Vt = np.linalg.svd(X, full_matrices=False)
    k = max(1, min(int(rank), len(s)))
    Uk = U[:, :k]
    Sk = np.diag(s[:k])
    Vk = Vt[:k, :]
    Ssqrt = np.sqrt(Sk)
    A = Uk @ Ssqrt
    B = Ssqrt @ Vk

    # -- 3) Persist factors
    with open(pkl_path, "wb") as f:
        pickle.dump(
            {
                "A": A.astype("float32"),
                "B": B.astype("float32"),
                "meta": {"rank": int(k), "size": [W, H], "text": text},
            },
            f,
        )

    # -- 4) Visualize A and B (not binary)
    def _plot_matrix(M, out_path, title):
        plt.figure(figsize=(4, 4), dpi=120)
        plt.imshow(M, cmap="gray_r", interpolation="nearest")
        plt.title(title)
        plt.axis("off")
        plt.tight_layout(pad=0.2)
        plt.savefig(out_path)
        plt.close()

    _plot_matrix(A, img_A, f"A factor ({W}×{k})")
    _plot_matrix(B, img_B, f"B factor ({k}×{H})")




default_args = {
    "owner": "gamma",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


with DAG(
    dag_id="alice_dag",
    default_args=default_args,
    description="Alice: make 32×32 text matrix, factorize via SVD into A,B and save them",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["cosifest", "handson", "tutorial", "cosipy", "producer", "linalg"],
) as dag:

    alice_factorize = ExternalPythonOperator(
        task_id="alice_factorize_text_matrix",
        python=EXTERNAL_PYTHON,               # interpreter in cosipy env
        python_callable=_alice_make_factors,  # callable executed in external PY
        op_kwargs={
            "base_dir": BASE_DIR,
            "text": TEXT,
            "size": SIZE,
            "font_size": FONT_SIZE,
            "rank": RANK,
        },
    )
