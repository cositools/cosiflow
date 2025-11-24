# /home/gamma/airflow/dags/_lib/cosipipe_lc_ops.py
# Utility functions used by the cosipipe_lightcurve DAG with ExternalPythonOperator.
# All functions are self-contained and do not rely on DAG-level globals.

from __future__ import annotations

from pathlib import Path
import os
import yaml

# -----------------------------
# Binning (source & background)
# -----------------------------
def bin_grb_source(unbinned_file_path: str, data_folder: str) -> str:
    """
    Bin GRB data source based on the bin_grb.py script
    """
    print(f"[bin_grb_source] Found GRB unbinned fits file: {unbinned_file_path}")

    # Define output file path
    extension = unbinned_file_path.split(".")[-1][:-1]
    binned_file_name = unbinned_file_path.replace("_unbinned_", "_binned_").replace(extension, "")
    binned_file_path = os.path.join(data_folder, f"{binned_file_name}.hdf5")
    
    print(f"[bin_grb_source] Expected output file: {binned_file_path}")

    # Check if binned file already exists
    if os.path.exists(binned_file_path):
        print(f"[bin_grb_source] Binned GRB file already exists: {binned_file_path}")
        print("[bin_grb_source] Skipping GRB binning step.")
        return binned_file_path
    
    print("[bin_grb_source] Binned GRB file not found. Proceeding with binning process...")
    
    # Construct paths
    # Create inputs.yaml configuration for binning
    print("[bin_grb_source] Creating GRB binning configuration...")
    config = {
        "data_file": unbinned_file_path,
        "ori_file": "NA",
        "unbinned_output": "fits",
        "time_bins": 1,
        "energy_bins": [100., 158.489, 251.189, 398.107, 630.957, 1000., 1584.89, 2511.89, 3981.07, 6309.57, 10000.],
        "phi_pix_size": 6,
        "nside": 8,
        "scheme": "ring",
        "tmin": 1836496300.00,
        "tmax": 1836496389.00
    }
    
    # Write inputs.yaml to the data folder
    inputs_path = os.path.join(data_folder, "inputs_grb.yaml")
    with open(inputs_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"[bin_grb_source] ✓ Created GRB binning configuration: {inputs_path}")
    print(f"[bin_grb_source] Configuration details:")
    print(f"[bin_grb_source]   - Time bins: {config['time_bins']} seconds")
    print(f"[bin_grb_source]   - Energy bins: {len(config['energy_bins'])-1} bins from {config['energy_bins'][0]} to {config['energy_bins'][-1]} keV")
    print(f"[bin_grb_source]   - Phi pixel size: {config['phi_pix_size']} degrees")
    print(f"[bin_grb_source]   - Nside: {config['nside']}")
    print(f"[bin_grb_source]   - Scheme: {config['scheme']}")
    print(f"[bin_grb_source]   - Time range: {config['tmin']} to {config['tmax']}")
    print(f"[bin_grb_source]   - GRB duration: {config['tmax'] - config['tmin']} seconds")
    
    # Execute the binning process
    print("[bin_grb_source] Initializing COSIpy BinnedData analysis...")
    from cosipy import BinnedData
    
    analysis = BinnedData(inputs_path)
    print("[bin_grb_source] ✓ BinnedData analysis object created successfully")
    
    print(f"[bin_grb_source] Starting binning process for output: {binned_file_name}")
    print("[bin_grb_source] This may take several minutes depending on data size...")
    
    analysis.get_binned_data(
        unbinned_data=unbinned_file_path,
        output_name=binned_file_name,
        psichi_binning="galactic"
    )
    
    print(f"[bin_grb_source] GRB data binning completed successfully!")
    print(f"[bin_grb_source] Output file created: {binned_file_path}")
    
    # Verify the output file was created
    if os.path.exists(binned_file_path):
        file_size = os.path.getsize(binned_file_path) / (1024 * 1024)  # Size in MB
        print(f"[bin_grb_source] Output file verified: {file_size:.2f} MB")
    else:
        print(f"[bin_grb_source] Warning: Expected output file not found: {binned_file_path}")
    
    return binned_file_path


def bin_background_data(unbinned_file_path: str, data_folder: str) -> str:
    """
    Bin background data based on the bin_bg.py script
    """
    print(f"[bin_background_data] Found background unbinned fits file: {unbinned_file_path}")

    # Define output file path
    extension = unbinned_file_path.split(".")[-1][:-1]
    print(f"[bin_background_data] Extension: {extension}")
    binned_file_name = unbinned_file_path.replace("_unbinned_", "_binned_").replace(extension, "")
    print(f"[bin_background_data] Binned file name: {binned_file_name}")
    binned_file_path = os.path.join(data_folder, f"{binned_file_name}.hdf5")
    
    print(f"[bin_background_data] Expected output file: {binned_file_path}")

    # Check if binned file already exists
    if os.path.exists(binned_file_path):
        print(f"[bin_background_data] Binned background file already exists: {binned_file_path}")
        print("[bin_background_data] Skipping background binning step.")
        return binned_file_path
    
    print("[bin_background_data] Binned background file not found. Proceeding with binning process...")
    
    # Construct paths
    # Create inputs.yaml configuration for binning
    print("[bin_background_data] Creating background binning configuration...")
    config = {
        "data_file": unbinned_file_path,
        "ori_file": "NA",
        "unbinned_output": "fits",
        "time_bins": 1,
        "energy_bins": [100., 158.489, 251.189, 398.107, 630.957, 1000., 1584.89, 2511.89, 3981.07, 6309.57, 10000.],
        "phi_pix_size": 6,
        "nside": 8,
        "scheme": "ring",
        "tmin": 1835487300.0,
        "tmax": 1843467255.0
    }
    
    # Write inputs.yaml to the data folder
    inputs_path = os.path.join(data_folder, "inputs_bg.yaml")
    with open(inputs_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    
    print(f"[bin_background_data] ✓ Created background binning configuration: {inputs_path}")
    print(f"[bin_background_data] Configuration details:")
    print(f"[bin_background_data]   - Time bins: {config['time_bins']} seconds")
    print(f"[bin_background_data]   - Energy bins: {len(config['energy_bins'])-1} bins from {config['energy_bins'][0]} to {config['energy_bins'][-1]} keV")
    print(f"[bin_background_data]   - Phi pixel size: {config['phi_pix_size']} degrees")
    print(f"[bin_background_data]   - Nside: {config['nside']}")
    print(f"[bin_background_data]   - Scheme: {config['scheme']}")
    print(f"[bin_background_data]   - Time range: {config['tmin']} to {config['tmax']}")
    
    # Execute the binning process
    print("[bin_background_data] Initializing COSIpy BinnedData analysis...")
    from cosipy import BinnedData
    
    analysis = BinnedData(inputs_path)
    print("[bin_background_data] ✓ BinnedData analysis object created successfully")
    
    print(f"[bin_background_data] Starting binning process for output: {binned_file_name}")
    print("[bin_background_data] This may take several minutes depending on data size...")
    
    analysis.get_binned_data(
        unbinned_data=unbinned_file_path,
        # Use output_file without the extension
        output_name=binned_file_name,
        psichi_binning="galactic"
    )
    
    print(f"[bin_background_data] Background data binning completed successfully!")
    print(f"[bin_background_data] Output file created: {binned_file_path}")
    
    # Verify the output file was created
    if os.path.exists(binned_file_path):
        file_size = os.path.getsize(binned_file_path) / (1024 * 1024)  # Size in MB
        print(f"[bin_background_data] ✓ Output file verified: {file_size:.2f} MB")
    else:
        print(f"[bin_background_data] Warning: Expected output file not found: {binned_file_path}")
    
    return binned_file_path


# -----------------------------
# Light curve plotting (full cell logic)
# -----------------------------
def plot_lightcurve_from_cells(grb_signal_path: str, 
                               background_path: str, 
                               orientation_path: str, 
                               response_path: str, 
                               data_dir: str) -> str:
    """
    Plot the light curve based on the grb_signal_path, background_path, orientation_path, and response_path
    """

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

    # ---- build PSR + mask ----
    psr_map = create_psr(171.56, -4.780, ori_file=orientation_path, response_file=response_path)
    input_psr = psr_map.project(['Em', 'Phi', 'PsiChi']).contents.value
    mask_map = mask_from_cumdist_vectorized(input_psr, containment=0.5)

    # ---- open histograms ----
    signal_full = Histogram.open(grb_signal_path)
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

    return str(out_png)


