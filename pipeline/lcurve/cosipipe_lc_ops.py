# /home/gamma/airflow/dags/_lib/cosipipe_lc_ops.py
# Utility functions used by the cosipipe_lightcurve DAG with ExternalPythonOperator.
# All functions are self-contained and do not rely on DAG-level globals.

from __future__ import annotations

from pathlib import Path
import tarfile
import zipfile
import glob
from urllib.parse import quote


# -----------------------------
# Archive decompression
# -----------------------------
def decompress_archive(archive_path: str, runs_dir: str) -> str:
    """
    Decompress the selected archive directly into:
      <runs_dir>/<archive_stem>/
    No timestamp subfolder is created.
    Returns the extraction directory path as string.
    """
    ap = Path(archive_path)
    if not ap.exists():
        raise FileNotFoundError(f"Archive not found: {ap}")

    # Strip common compressed extensions to derive the stem
    stem = ap.name
    for ext in (".tar.gz", ".tgz", ".zip", ".tar"):
        if stem.endswith(ext):
            stem = stem[: -len(ext)]
            break

    target = Path(runs_dir) / stem
    target.mkdir(parents=True, exist_ok=True)

    # Extract directly into <target>
    if ap.suffix == ".zip":
        with zipfile.ZipFile(ap, "r") as zf:
            zf.extractall(target)
    elif ap.name.endswith(".tar.gz") or ap.suffix == ".tgz":
        with tarfile.open(ap, "r:gz") as tf:
            tf.extractall(target)
    elif ap.suffix == ".tar":
        with tarfile.open(ap, "r:") as tf:
            tf.extractall(target)
    else:
        raise ValueError(f"Unsupported archive format: {ap.name}")

    return str(target)


# -----------------------------
# Helpers for file discovery
# -----------------------------
def _resolve_data_root(run_dir: str, must_have: list[str] | None = None) -> str:
    """
    Return the folder that actually contains the expected files.
    Strategy:
      1) Try run_dir itself.
      2) If not found and run_dir has exactly one subdirectory, try that subdir.
      3) If still not found, search recursively (rglob) for any 'must_have' item,
         and return the parent directory of the first hit.
    If must_have is None or empty, simply return run_dir.
    """
    rd = Path(run_dir)

    def has_any(p: Path, items: list[str]) -> bool:
        return any((p / it).exists() for it in items)

    if not must_have:
        return str(rd)

    if has_any(rd, must_have):
        return str(rd)

    subdirs = [p for p in rd.iterdir() if p.is_dir()]
    if len(subdirs) == 1 and has_any(subdirs[0], must_have):
        return str(subdirs[0])

    for it in must_have:
        hits = list(rd.rglob(it))
        if hits:
            return str(hits[0].parent)

    # Fallback to run_dir (callers should raise clearer errors if needed)
    return str(rd)


def _first_existing_or_glob(base: str, exact: str, patterns: list[str]) -> str:
    """
    Return an existing path inside 'base' for 'exact' or the first match among 'patterns'.
    Raise FileNotFoundError with a helpful message if nothing is found.
    """
    b = Path(base)
    if (b / exact).exists():
        return str(b / exact)

    for pat in patterns:
        found = sorted(glob.glob(str(b / pat)))
        if found:
            return found[0]

    raise FileNotFoundError(
        f"Cannot find '{exact}' or any of patterns {patterns} under {base}"
    )


# -----------------------------
# Validation
# -----------------------------
def validate_inputs(run_dir: str) -> None:
    """
    Fail early with a clear message if key inputs are missing after extraction.
    IMPORTANT: pass must_have to _resolve_data_root so we correctly descend into an
    inner folder (if the archive contains a root directory).
    """
    # Use "must_have" to allow resolving an inner root directory
    must_have = [
        "inputs_GRB__galactic.yaml",
        "inputs_bkg__galactic.yaml",
        "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        "Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
        "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
    ]
    root = _resolve_data_root(run_dir, must_have=must_have)

    # Optional: small debug print to help future diagnostics
    try:
        from pathlib import Path
        listing = "\n".join(sorted(p.name for p in Path(root).iterdir()))
        print(f"[validate_inputs] Using data root: {root}\nContents:\n{listing}")
    except Exception:
        pass

    required = [
        # YAMLs
        ("inputs_GRB__galactic.yaml", ["inputs_GRB__*.yaml", "*inputs*GRB*galactic*.yaml"]),
        ("inputs_bkg__galactic.yaml", ["inputs_bkg__*.yaml", "*inputs*bkg*galactic*.yaml"]),
        # Unbinned FITS
        ("GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
         ["GRB*_unbinned*fits*", "*GRB*unbinned*fits*"]),
        ("Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
         ["Total_BG*unbinned*fits*", "*BG*unbinned*fits*"]),
        # ORI + Response
        ("DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
         ["*.ori", "**/*.ori"]),
        ("ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
         ["ResponseContinuum*.h5", "**/ResponseContinuum*.h5"]),
    ]

    missing = []
    for exact, pats in required:
        try:
            _first_existing_or_glob(root, exact, pats)
        except FileNotFoundError as e:
            missing.append(str(e))

    if missing:
        msg = "Input validation failed. Missing files:\n- " + "\n- ".join(missing) + f"\nSearch root: {root}"
        raise FileNotFoundError(msg)

# -----------------------------
# Binning (source & background)
# -----------------------------
def bin_grb_source(run_dir: str) -> str:
    """
    Mirror of the notebook's GRB binning cell (idempotent and robust to extra root folders / minor name variations).
    """
    from cosipy import BinnedData

    data_dir = _resolve_data_root(
        run_dir,
        must_have=[
            "inputs_GRB__galactic.yaml",
            "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        ],
    )

    out_h5 = Path(data_dir) / "GRB_bn081207680_binned_O3.hdf5"
    if out_h5.exists():
        print(f"[bin_grb_source] already exists: {out_h5}")
        return str(out_h5)

    yaml_path = _first_existing_or_glob(
        data_dir, "inputs_GRB__galactic.yaml",
        patterns=["inputs_GRB__*.yaml", "*inputs*GRB*galactic*.yaml"]
    )
    fits_path = _first_existing_or_glob(
        data_dir, "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        patterns=["GRB*_unbinned*fits*", "*GRB*unbinned*fits*"]
    )

    base = str(Path(data_dir) / "GRB_bn081207680_binned_O3")
    analysis = BinnedData(Path(yaml_path))
    analysis.get_binned_data(
        unbinned_data=Path(fits_path),
        output_name=base,
        psichi_binning="galactic",
    )
    return str(out_h5)


def bin_background(run_dir: str) -> str:
    """
    Mirror of the notebook's background binning cell (idempotent and robust).
    """
    from cosipy import BinnedData

    data_dir = _resolve_data_root(
        run_dir,
        must_have=[
            "inputs_bkg__galactic.yaml",
            "Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        ],
    )

    out_h5 = Path(data_dir) / "Total_BG_continuum_O3_binned.hdf5"
    if out_h5.exists():
        print(f"[bin_background] already exists: {out_h5}")
        return str(out_h5)

    yaml_path = _first_existing_or_glob(
        data_dir, "inputs_bkg__galactic.yaml",
        patterns=["inputs_bkg__*.yaml", "*inputs*bkg*galactic*.yaml"]
    )
    fits_path = _first_existing_or_glob(
        data_dir, "Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        patterns=["Total_BG*unbinned*fits*", "*BG*unbinned*fits*"]
    )

    base = str(Path(data_dir) / "Total_BG_continuum_O3_binned")
    analysis = BinnedData(Path(yaml_path))
    analysis.get_binned_data(
        unbinned_data=Path(fits_path),
        output_name=base,
        psichi_binning="galactic",
    )
    return str(out_h5)


# -----------------------------
# Light curve plotting (full cell logic)
# -----------------------------
def plot_lightcurve_from_cells(
    run_dir: str,
    plugin_base_url: str | None = None,   # es. "http://agilehost3.iasfbo.inaf.it:8080/heasarcbrowser"
    plugin_root_dir: str | None = None,   # es. "/home/gamma/workspace/data"
) -> str:
    """
    Reproduce the notebook logic and save 'lightcurve.png'.
    If plugin_base_url and plugin_root_dir are provided, print preview/download links in logs.
    Returns the absolute path to the PNG.
    """
    # Resolve data root using key assets needed by plotting
    data_dir = _resolve_data_root(
        run_dir,
        must_have=[
            "GRB_bn081207680_binned_O3.hdf5",
            "Total_BG_continuum_O3_binned.hdf5",
            "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
            "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
        ],
    )

    # Imports used by the notebook cells
    import numpy as np
    import matplotlib.pyplot as plt
    import astropy.units as u
    from astropy.coordinates import SkyCoord
    from histpy import Histogram
    from cosipy.background_estimation import ContinuumEstimation
    from cosipy.spacecraftfile import SpacecraftFile
    from cosipy.response import FullDetectorResponse

    # ---- helpers (as in notebook) ----
    def load_projected_psr(psr_file):
        instance = ContinuumEstimation()
        psr_hist = instance.load_psr_from_file(psr_file)
        projected = psr_hist.project(['Em', 'Phi', 'PsiChi'])
        data = projected.contents.value
        return projected, data

    def mask_from_cumdist_vectorized(psr_map, containment=0.4):
        psr_norm = psr_map / np.sum(psr_map, axis=-1, keepdims=True)
        sort_idx = np.argsort(psr_norm, axis=-1)[..., ::-1]
        sorted_vals = np.take_along_axis(psr_norm, sort_idx, axis=-1)
        cumsum_vals = np.cumsum(sorted_vals, axis=-1)
        mask_sorted = (cumsum_vals < containment).astype(float)
        mask = np.empty_like(mask_sorted)
        np.put_along_axis(mask, sort_idx, mask_sorted, axis=-1)
        # NOTE: like in your cell, we do NOT invert the mask.
        return mask

    def create_psr(l, b, ori_file: str, response_file: str):
        ori = SpacecraftFile.parse_from_file(ori_file)
        coord = SkyCoord(l=l * u.deg, b=b * u.deg, frame="galactic")
        scatt_map = ori.get_scatt_map(coord, nside=16, coordsys='galactic')
        with FullDetectorResponse.open(response_file) as response:
            psr = response.get_point_source_response(coord=coord, scatt_map=scatt_map)
            print("Works!")
            return psr

    def get_signal_window(signal):
        grb_tmin = signal.axes["Time"].edges.min()
        grb_tmax = signal.axes["Time"].edges.max()
        print(f"The GRB duration is {grb_tmax - grb_tmin} from {grb_tmin} to {grb_tmax}")
        return grb_tmin, grb_tmax

    def load_data(signal_full, bkg_full, tstart, tstop, window_start, window_stop):
        # Slice & project Signal only if within GRB window
        if tstart >= window_start.value and tstop <= window_stop.value:
            signal_tmin_idx = np.where(signal_full.axes['Time'].edges.value == tstart)[0][0]
            signal_tmax_idx = np.where(signal_full.axes["Time"].edges.value == tstop)[0][0]
            signal = signal_full.slice[signal_tmin_idx:signal_tmax_idx, :]
            signal = signal.project(['Em', 'Phi', 'PsiChi'])
        # Background always sliced on the same interval
        bkg_tmin_idx = np.where(bkg_full.axes['Time'].edges.value == tstart)[0][0]
        bkg_tmax_idx = np.where(bkg_full.axes["Time"].edges.value == tstop)[0][0]
        bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx, :]
        bkg = bkg.project(['Em', 'Phi', 'PsiChi'])
        # Add signal only inside window
        if tstart >= window_start.value and tstop <= window_stop.value:
            data = signal + bkg
        else:
            data = bkg
        return data, data.contents.todense()

    # ---- resolve filenames (allowing minor variations) ----
    ori_file = _first_existing_or_glob(
        data_dir,
        "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
        patterns=["*.ori", "*GalacticEarth*/*.ori", "**/*.ori"],
    )
    response_file = _first_existing_or_glob(
        data_dir,
        "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
        patterns=["ResponseContinuum*.h5", "**/ResponseContinuum*.h5"],
    )
    background_path = _first_existing_or_glob(
        data_dir, "Total_BG_continuum_O3_binned.hdf5",
        patterns=["*BG*O3*binned*.hdf5", "**/*BG*O3*binned*.hdf5"]
    )
    GRB_signal_path = _first_existing_or_glob(
        data_dir, "GRB_bn081207680_binned_O3.hdf5",
        patterns=["*GRB*O3*binned*.hdf5", "**/*GRB*O3*binned*.hdf5"]
    )

    # ---- build PSR + mask ----
    psr_map = create_psr(171.56, -4.780, ori_file=ori_file, response_file=response_file)
    input_psr = psr_map.project(['Em', 'Phi', 'PsiChi']).contents.value
    mask_map = mask_from_cumdist_vectorized(input_psr, containment=0.5)

    # ---- open histograms ----
    signal_full = Histogram.open(GRB_signal_path)
    bkg_full = Histogram.open(background_path)

    # ---- time scan around the GRB window ----
    window_start, window_stop = get_signal_window(signal_full)

    counts = []
    bin_size = 5  # seconds
    tstart = window_start.value - 20
    while tstart < window_stop.value + 20:
        tstop = tstart + bin_size
        _, data_map = load_data(signal_full, bkg_full, tstart, tstop, window_start, window_stop)
        masked_data = mask_map * data_map
        counts.append(masked_data.sum())
        tstart = tstop

    # Build bin edges like in the notebook
    N = int(((window_stop.value + 20) - (window_start.value - 20)) / bin_size)
    bins = np.linspace(window_start.value - 20, window_stop.value + 20, N + 1)

    # ---- plot ----
    plt.figure(figsize=(10, 4))
    plt.step(bins, counts)
    plt.xlabel("Time (s)")
    plt.ylabel("Counts")
    plt.title(f"GRB light curve (bin = {bin_size}s)")
    plt.axvline(x=window_start.value, linestyle='--', linewidth=1.5)
    out_png = Path(data_dir) / "lightcurve.png"
    plt.tight_layout()
    plt.savefig(out_png, dpi=150)
    plt.close()

    print(f"Light curve saved to: {out_png}")

    # ----- STAMPA LINK PLUGIN -----
    if plugin_base_url and plugin_root_dir:
        try:
            rel_file = Path(out_png).resolve().relative_to(Path(plugin_root_dir).resolve())
            rel_dir = rel_file.parent  # es. lcurve/20251007_100700/20250930_150000

            base = plugin_base_url.rstrip("/")
            #rel_file_q = quote(str(rel_file))
            #rel_dir_q  = quote(str(rel_dir))

            # preview_url  = f"{base}/preview/{rel_file}"
            # download_url = f"{base}/download/{rel_file}"
            folder_url   = f"{base}/folder/{rel_dir}"

            print("\n[Data Explorer]")
            print(f"  Folder  : {folder_url}")
            # print(f"  Preview : {preview_url}")
            # print(f"  Download: {download_url}")
        except Exception as e:
            print(f"[Data Explorer] Could not build plugin URLs: {e}")

    return str(out_png)


