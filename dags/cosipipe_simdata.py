# dags/init_pipelines.py
# Airflow 2.x — Initialize and stage COSI pipeline data
# - staged raw subfolders (source/background/orientation/response)
# - staging & background cut executed in Conda 'cosipy' via ExternalPythonOperator (no Airflow context inside)
# - run folder under DEST_MAP/YYYY_MM/YYMMDDXXX/products with symlinks and cut result

from __future__ import annotations
import json, os, re, shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, ExternalPythonOperator

# === External env/interpreter ===
EXTERNAL_PYTHON = "/home/gamma/.conda/envs/cosipy/bin/python"
BKG_CUT_SCRIPT = "/home/gamma/airflow/pipeline/bkg_cut.py"

# === Paths ===
RAW_ROOT = Path("/home/gamma/workspace/data/raw")
RAW_SUBDIRS = {
    "source": RAW_ROOT / "source",
    "background": RAW_ROOT / "background",
    "orientation": RAW_ROOT / "orientation",
    "response": RAW_ROOT / "response",
}

DEST_MAP = {
    "lcurve": Path("/home/gamma/workspace/data/lcurve"),
    "tsmap": Path("/home/gamma/workspace/data/tsmap"),
}

# === Default Wasabi keys ===
WASABI_DEFAULTS = {
    "response":    "Responses/ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5.zip",
    "orientation": "Orientation/DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
    "source":      "Sources/GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
    "background":  "Backgrounds/Ge/Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
}

# === Helpers ===
def ensure_dir(p: Path) -> Path:
    p.mkdir(parents=True, exist_ok=True)
    return p

def next_run_products_dir(base_dest: Path) -> Path:
    now = datetime.now()
    yyyy_mm = now.strftime("%Y_%m")
    yymmdd = now.strftime("%y%m%d")
    base_month_dir = ensure_dir(base_dest / yyyy_mm)
    pat = re.compile(rf"^{yymmdd}(\d{{3}})$")
    max_idx = -1
    for d in base_month_dir.iterdir():
        if d.is_dir() and pat.match(d.name):
            max_idx = max(max_idx, int(pat.match(d.name).group(1)))
    run_dir = ensure_dir(base_month_dir / f"{yymmdd}{max_idx+1:03d}")
    return ensure_dir(run_dir / "products")

def make_symlinks(target_dir: Path, files_by_kind: Dict[str, List[Path]]) -> Dict[str, List[str]]:
    result = {}
    for kind, paths in files_by_kind.items():
        result[kind] = []
        for p in paths:
            link = target_dir / p.name
            if not link.exists():
                link.symlink_to(p)
                print(f"[symlink] {link} -> {p}")
            # append comunque, anche se già esisteva
            result[kind].append(str(link))
    return result

# === ExternalPythonOperator callables ===
def stage_all_files_external(
    *,
    inputs_json: str,
    response_dir: str,
    orientation_dir: str,
    source_dir: str,
    background_dir: str,
) -> Dict[str, List[str]]:
    """
    Eseguito in cosipy env. Evita riscaricare/ri-estrarre file già pronti.
    Rileva .gz/.zip corrotti (riscaria) e valida il FITS di background se già presente.
    """
    import json, os, shutil, gzip, zipfile
    from zipfile import BadZipFile
    from pathlib import Path

    inputs = json.loads(inputs_json)

    def ensure_dir(p: Path) -> Path:
        p.mkdir(parents=True, exist_ok=True)
        return p

    def ready_files(target_dir: Path) -> list[Path]:
        return [p for p in target_dir.iterdir() if p.is_file() and not p.name.endswith((".zip", ".gz"))]

    def ready_files_exist(target_dir: Path) -> bool:
        return len(ready_files(target_dir)) > 0

    def validate_background_fits(f: Path) -> bool:
        """Prova ad aprire l'HDU[1] per intercettare file troncati."""
        try:
            from astropy.io import fits
            with fits.open(f, memmap=True) as hdul:
                # accesso minimo per forzare la lettura del blocco tabellare
                _ = hdul[1].data.shape  # noqa
            return True
        except Exception as e:
            print(f"[stage] Background FITS validation failed: {f} ({e})")
            return False

    def gunzip_to_same_dir(gz_path: Path) -> Path:
        ready = gz_path.with_suffix("")
        if ready.exists():
            return ready
        with gzip.open(gz_path, "rb") as gz_f, open(ready, "wb") as out_f:
            shutil.copyfileobj(gz_f, out_f)
        gz_path.unlink(missing_ok=True)
        return ready

    def fetch_wasabi(remote_key: str, out_path: Path) -> None:
        from cosipy.util import fetch_wasabi_file
        ensure_dir(out_path.parent)
        fetch_wasabi_file(f"COSI-SMEX/DC3/Data/{remote_key}", output=out_path)

    def redownload(remote_key: str, out_path: Path) -> None:
        try:
            out_path.unlink(missing_ok=True)
        except Exception:
            pass
        fetch_wasabi(remote_key, out_path)

    def download_or_use_one(remote_or_local: str, target_dir: Path, *, kind: str) -> list[Path]:
        ensure_dir(target_dir)

        # Se ready presenti, per il BACKGROUND prima valida il FITS
        if ready_files_exist(target_dir):
            files = ready_files(target_dir)
            if kind == "background":
                # valida il primo FITS
                bg_fits = next((p for p in files if p.suffix.lower() == ".fits" or p.name.endswith(".fits")), None)
                if bg_fits and not validate_background_fits(bg_fits):
                    print(f"[stage] Detected corrupted ready background in {target_dir}, cleaning and re-fetching…")
                    for p in target_dir.iterdir():
                        if p.is_file():
                            p.unlink(missing_ok=True)
                else:
                    print(f"[stage] Ready files already in {target_dir}, skipping.")
                    return files

            else:
                print(f"[stage] Ready files already in {target_dir}, skipping.")
                return files

        src = Path(remote_or_local)

        # --- Locale assoluto ---
        if src.is_absolute() and src.exists():
            dst = target_dir / src.name
            if not dst.exists():
                try:
                    os.link(src, dst)
                except OSError:
                    shutil.copy2(src, dst)
            if dst.suffix == ".gz":
                ready = gunzip_to_same_dir(dst)
                return [ready]
            if dst.suffix == ".zip":
                try:
                    with zipfile.ZipFile(dst, "r") as zf:
                        zf.testzip()
                        zf.extractall(target_dir)
                    return ready_files(target_dir)
                except BadZipFile as e:
                    raise RuntimeError(f"[stage] Local .zip seems corrupted: {dst} ({e})")
            # per background locale, valida
            if kind == "background" and dst.suffix.lower() == ".fits" and not validate_background_fits(dst):
                raise RuntimeError(f"[stage] Local background FITS seems corrupted: {dst}")
            return [dst]

        # --- Wasabi ---
        out_path = target_dir / src.name

        # download se manca
        if not out_path.exists():
            fetch_wasabi(remote_or_local, out_path)

        if out_path.suffix == ".gz":
            try:
                ready = gunzip_to_same_dir(out_path)
            except Exception:
                print(f"[stage] Corrupted .gz at {out_path}, re-downloading…")
                redownload(remote_or_local, out_path)
                ready = gunzip_to_same_dir(out_path)
            if kind == "background" and not validate_background_fits(ready):
                print(f"[stage] Re-downloading background FITS after failed validation…")
                redownload(remote_or_local, out_path)
                ready = gunzip_to_same_dir(out_path)
                if not validate_background_fits(ready):
                    raise RuntimeError(f"[stage] Background FITS still invalid after re-download: {ready}")
            return [ready]

        if out_path.suffix == ".zip":
            try:
                with zipfile.ZipFile(out_path, "r") as zf:
                    zf.testzip()
                    zf.extractall(target_dir)
            except BadZipFile:
                print(f"[stage] Corrupted .zip at {out_path}, re-downloading…")
                redownload(remote_or_local, out_path)
                with zipfile.ZipFile(out_path, "r") as zf:
                    zf.testzip()
                    zf.extractall(target_dir)
            files = ready_files(target_dir)
            return files

        # file semplice
        if kind == "background" and out_path.suffix.lower() == ".fits" and not validate_background_fits(out_path):
            print(f"[stage] Background FITS invalid at {out_path}, re-downloading…")
            redownload(remote_or_local, out_path)
            if not validate_background_fits(out_path):
                raise RuntimeError(f"[stage] Background FITS still invalid after re-download: {out_path}")
        return [out_path]

    staged = {
        "response":   [str(p) for p in download_or_use_one(inputs["response"],   Path(response_dir),   kind="response")],
        "orientation":[str(p) for p in download_or_use_one(inputs["orientation"],Path(orientation_dir),kind="orientation")],
        "source":     [str(p) for p in download_or_use_one(inputs["source"],     Path(source_dir),     kind="source")],
        "background": [str(p) for p in download_or_use_one(inputs["background"], Path(background_dir), kind="background")],
    }
    print(json.dumps(staged, indent=2))
    return staged

def run_bkg_cut_and_move(*, script: str, products_dir: str, source_link_path: str, background_link_path: str, eps_time: float):
    """
    Esegue bkg_cut nella cartella products usando i SYMLINK come input.
    Non sposta nulla: lo script deve generare l'output direttamente in products/.
    Ritorna la lista dei nuovi file creati in products.
    """
    import sys, subprocess, time
    from pathlib import Path

    products = Path(products_dir); products.mkdir(parents=True, exist_ok=True)
    before = {p.name for p in products.glob("*.fits*")}
    cmd = [sys.executable, script, source_link_path, background_link_path, "--eps_time", str(eps_time)]
    print(f"[bkg_cut] CWD={products} CMD: {' '.join(cmd)}")
    subprocess.run(cmd, check=True, cwd=str(products))

    # nuovi file creati in products
    after = list(products.glob("*.fits*"))
    new_files = [str(p) for p in after if p.name not in before]
    print(f"[bkg_cut] new files in products/: {new_files}")
    return {"created": new_files}

# === DAG ===
with DAG(
    dag_id="init_pipelines",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["cosiflow", "init"],
    description="Initialize and stage COSI pipeline data (optimized with skip logic)",
    params={
        "response_path": Param(default=WASABI_DEFAULTS["response"], type="string"),
        "orientation_path": Param(default=WASABI_DEFAULTS["orientation"], type="string"),
        "source_path": Param(default=WASABI_DEFAULTS["source"], type="string"),
        "background_path": Param(default=WASABI_DEFAULTS["background"], type="string"),
        "destination": Param(default="tsmap", enum=["lcurve", "tsmap"]),
        "eps_time": Param(default=1e-9, type="number"),
    },
) as dag:

    def prepare_raw_dirs():
        for d in RAW_SUBDIRS.values():
            ensure_dir(d)
        return {k: str(v) for k, v in RAW_SUBDIRS.items()}

    t_prepare = PythonOperator(task_id="prepare_raw_dirs", python_callable=prepare_raw_dirs)

    def resolve_config(**context):
        p = context["params"]
        return {
            "destination_root": str(DEST_MAP[p["destination"]]),
            "eps_time": float(p["eps_time"]),
            "inputs": {
                "response": p["response_path"],
                "orientation": p["orientation_path"],
                "source": p["source_path"],
                "background": p["background_path"],
            },
        }

    t_resolve = PythonOperator(task_id="resolve_config", python_callable=resolve_config)

    t_stage = ExternalPythonOperator(
        task_id="stage_all_files",
        python=EXTERNAL_PYTHON,
        python_callable=stage_all_files_external,
        op_kwargs={
            "inputs_json": "{{ ti.xcom_pull(task_ids='resolve_config')['inputs'] | tojson }}",
            "response_dir": str(RAW_SUBDIRS["response"]),
            "orientation_dir": str(RAW_SUBDIRS["orientation"]),
            "source_dir": str(RAW_SUBDIRS["source"]),
            "background_dir": str(RAW_SUBDIRS["background"]),
        },
    )

    def create_products_dir(ti):
        cfg = ti.xcom_pull(task_ids="resolve_config")
        pdir = next_run_products_dir(Path(cfg["destination_root"]))
        return {"products_dir": str(pdir)}

    t_products = PythonOperator(task_id="create_products_dir", python_callable=create_products_dir)

    t_bkgcut = ExternalPythonOperator(
        task_id="background_cut",
        python=EXTERNAL_PYTHON,
        python_callable=run_bkg_cut_and_move,
        op_kwargs={
            "script": BKG_CUT_SCRIPT,
            "products_dir": "{{ ti.xcom_pull(task_ids='create_products_dir')['products_dir'] }}",
            "source_link_path": "{{ ti.xcom_pull(task_ids='create_symlinks')['source'][0] }}",
            "background_link_path": "{{ ti.xcom_pull(task_ids='create_symlinks')['background'][0] }}",
            "eps_time": "{{ ti.xcom_pull(task_ids='resolve_config')['eps_time'] }}",
        },
    )


    def create_symlinks(ti):
        """
        Crea symlink in products/ per:
        - source file
        - response file(s) (possono essere più di uno dopo unzip)
        - orientation file
        - (opzionale) background originale
        Il/i file di background 'cut' sono già stati spostati in products/ dal task precedente,
        quindi non servono symlink per quelli.
        """
        staged = ti.xcom_pull(task_ids="stage_all_files")
        products_dir = Path(ti.xcom_pull(task_ids="create_products_dir")["products_dir"])

        # Se esistono più file (es. response dopo unzip), linkali tutti
        files_by_kind = {
            "source":      [Path(p) for p in staged.get("source", [])],
            "response":    [Path(p) for p in staged.get("response", [])],
            "orientation": [Path(p) for p in staged.get("orientation", [])],
            # Puoi commentare la riga seguente se non vuoi il link al background originale
            "background":  [Path(p) for p in staged.get("background", [])],
        }

        return make_symlinks(products_dir, files_by_kind)

    t_link = PythonOperator(task_id="create_symlinks", python_callable=create_symlinks)

    t_prepare >> t_resolve >> t_stage >> t_products >> t_link >> t_bkgcut
