# /home/gamma/airflow/dags/_lib/cosipipe_tsmap_ops.py
# Integrated ops for TSMap pipeline (COSIfest version).
# - Includes logic inlined from the original step scripts:
#   1_contactsimulator.py, 2_binGRBdatasource.py, 3_binBackground.py,
#   4_dataAggregation.py, 5_tsmapcomputation.py, 5_tsmapmulres_computation.py
#
# Added:
# - decompress_archive() and validate_inputs_tsmap()
# - Data Explorer URL logging (like light-curve) via plugin_base_url / plugin_root_dir
# - Config via Airflow Variables OR environment variables (fallback)
from __future__ import annotations

import os
from pathlib import Path
import tarfile, zipfile
from urllib.parse import quote

# ------------------ Config getter ------------------
def cfg(key: str, default: str | int | float | None = None):
    """Get config from Airflow Variable or ENV (str), else default."""
    val = None
    try:
        from airflow.models import Variable
        val = Variable.get(key)
    except Exception:
        pass
    if val is None:
        val = os.environ.get(key, default)
    return val

# convenience casters
def cfg_int(key: str, default: int) -> int:
    v = cfg(key, default)
    try:
        return int(v)
    except Exception:
        return default

# ------------------ Archive ------------------
def decompress_archive(archive_path: str, runs_dir: str) -> str:
    ap = Path(archive_path)
    if not ap.exists():
        raise FileNotFoundError(f"Archive not found: {ap}")
    stem = ap.name
    for ext in (".tar.gz", ".tgz", ".zip", ".tar"):
        if stem.endswith(ext):
            stem = stem[: -len(ext)]
            break
    target = Path(runs_dir) / stem
    target.mkdir(parents=True, exist_ok=True)

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

    print(f"[decompress_archive] Extracted to: {target}")
    return str(target)

# ------------------ Validation ------------------
def validate_inputs_tsmap(run_dir: str) -> None:
    rd = Path(run_dir)
    patterns = (cfg("TSMAP_REQUIRED_GLOB", None) or "GRB*unbinned*fits*;Total_BG*unbinned*fits*;*.ori;*.h5").split(";")
    missing = []
    for pat in patterns:
        pat = pat.strip()
        if not pat:
            continue
        if not list(rd.rglob(pat)):
            missing.append(pat)
    if missing:
        raise FileNotFoundError(f"Missing required inputs under {run_dir}: {missing}")
    print(f"[validate_inputs_tsmap] OK under: {run_dir}")

# ------------------ Data Explorer helper ------------------
def _print_explorer_links(out_path: str | Path) -> None:
    base_url = cfg("PLUGIN_BASE_URL", None)  # e.g., http://agilehost3.iasfbo.inaf.it:8080/heasarcbrowser
    root_dir = cfg("PLUGIN_ROOT_DIR", None)  # e.g., /home/gamma/workspace/data
    if not base_url or not root_dir:
        return
    try:
        out_path = Path(out_path).resolve()
        root = Path(root_dir).resolve()
        rel_dir = out_path if out_path.is_dir() else out_path.parent
        rel = rel_dir.relative_to(root)
        rel_q = quote(str(rel))
        folder_url = f"{base_url}/folder/{rel_q}"
        print("\n[Data Explorer]")
        print(f"  Folder  : {folder_url}")
    except Exception as e:
        print(f"[Data Explorer] Could not build plugin URLs: {e}")

# =====================================================================
# =============== Inlined pipeline step implementations ===============
# =====================================================================


# ---[ from 1_contactsimulator.py ]---
import os
import shutil
from datetime import datetime

def create_new_data_folder():
    """
    Create a new folder with current date and time for new satellite data simulation
    """
    # Create timestamp for folder name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source_folder = f"/home/gamma/workspace/tsmap_test/data"
    new_folder = f"/home/gamma/workspace/data/tsmap/{timestamp}"
    
    # Create the new folder
    os.makedirs(new_folder, exist_ok=True)
    print(f"Created new data folder: {new_folder}")
    
    # Source files to copy
    source_files = {
        "background_file": f"{source_folder}/Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        "grb_data_source": f"{source_folder}/GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz", 
        "response_file": f"{source_folder}/ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
        "orientation_file": f"{source_folder}/DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
        "binned_background": f"{source_folder}/Total_BG_continuum_O3_binned.hdf5"
    }
    
    # Copy files to new folder
    for file_type, source_path in source_files.items():
        if os.path.exists(source_path):
            filename = os.path.basename(source_path)
            dest_path = os.path.join(new_folder, filename)
            shutil.copy2(source_path, dest_path)
            print(f"Copied {file_type}: {filename}")
        else:
            print(f"Warning: Source file not found: {source_path}")
    
    # Return the new folder path for use by other scripts
    return new_folder


# ---[ from 2_binGRBdatasource.py ]---
import os
import sys
import yaml
from pathlib import Path

def bin_grb_data(data_folder):
    """
    Bin GRB data source based on the bin_grb.py script
    """
    print(f"Starting GRB binning process for folder: {data_folder}")
    
    # Define output file path
    output_name = "GRB_bn081207680_binned_O3"
    output_file = os.path.join(data_folder, f"{output_name}.hdf5")
    
    print(f"Expected output file: {output_file}")
    
    # Check if binned file already exists
    if os.path.exists(output_file):
        print(f"Binned GRB file already exists: {output_file}")
        print("Skipping GRB binning step.")
        return output_file
    
    print("✗ Binned GRB file not found. Proceeding with binning process...")
    
    # Construct paths
    grb_file = os.path.join(data_folder, "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits")
    print(f"Looking for GRB data file: {grb_file}")
    
    if not os.path.exists(grb_file):
        raise FileNotFoundError(f"GRB data file not found: {grb_file}")
    
    print(f"✓ Found GRB data file: {grb_file}")
    
    # Create inputs.yaml configuration for binning
    print("Creating GRB binning configuration...")
    config = {
        "data_file": grb_file,
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
    
    print(f"✓ Created GRB binning configuration: {inputs_path}")
    print("Configuration details:")
    print(f"  - Time bins: {config['time_bins']} seconds")
    print(f"  - Energy bins: {len(config['energy_bins'])-1} bins from {config['energy_bins'][0]} to {config['energy_bins'][-1]} keV")
    print(f"  - Phi pixel size: {config['phi_pix_size']} degrees")
    print(f"  - Nside: {config['nside']}")
    print(f"  - Scheme: {config['scheme']}")
    print(f"  - Time range: {config['tmin']} to {config['tmax']}")
    print(f"  - GRB duration: {config['tmax'] - config['tmin']} seconds")
    
    # Execute the binning process
    print("Initializing COSIpy BinnedData analysis...")
    from cosipy import BinnedData
    
    analysis = BinnedData(inputs_path)
    print("✓ BinnedData analysis object created successfully")
    
    print(f"Starting binning process for output: {output_name}")
    print("This may take several minutes depending on data size...")
    
    analysis.get_binned_data(
        unbinned_data=grb_file,
        output_name=os.path.splitext(output_file)[0],
        psichi_binning="local"
    )
    
    print(f"GRB data binning completed successfully!")
    print(f"Output file created: {output_file}")
    
    # Verify the output file was created
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
        print(f"Output file verified: {file_size:.2f} MB")
    else:
        print(f"Warning: Expected output file not found: {output_file}")
    
    return output_file


# ---[ from 3_binBackground.py ]---
import os
import sys
import yaml
from pathlib import Path

def bin_background_data(data_folder):
    """
    Bin background data based on the bin_bg.py script
    """
    print(f"Starting background binning process for folder: {data_folder}")
    
    # Define output file path
    output_name = "Total_BG_continuum_O3_binned"
    output_file = os.path.join(data_folder, f"{output_name}.hdf5")
    
    print(f"Expected output file: {output_file}")
    
    # Check if binned file already exists
    if os.path.exists(output_file):
        print(f"✓ Binned background file already exists: {output_file}")
        print("✓ Skipping background binning step.")
        return output_file
    
    print("✗ Binned background file not found. Proceeding with binning process...")
    
    # Construct paths
    bg_file = os.path.join(data_folder, "Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits")
    print(f"Looking for background data file: {bg_file}")
    
    if not os.path.exists(bg_file):
        raise FileNotFoundError(f"Background data file not found: {bg_file}")
    
    print(f"✓ Found background data file: {bg_file}")
    
    # Create inputs.yaml configuration for binning
    print("Creating binning configuration...")
    config = {
        "data_file": bg_file,
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
    
    print(f"✓ Created background binning configuration: {inputs_path}")
    print("Configuration details:")
    print(f"  - Time bins: {config['time_bins']} seconds")
    print(f"  - Energy bins: {len(config['energy_bins'])-1} bins from {config['energy_bins'][0]} to {config['energy_bins'][-1]} keV")
    print(f"  - Phi pixel size: {config['phi_pix_size']} degrees")
    print(f"  - Nside: {config['nside']}")
    print(f"  - Scheme: {config['scheme']}")
    print(f"  - Time range: {config['tmin']} to {config['tmax']}")
    
    # Execute the binning process
    print("Initializing COSIpy BinnedData analysis...")
    from cosipy import BinnedData
    
    analysis = BinnedData(inputs_path)
    print("✓ BinnedData analysis object created successfully")
    
    print(f"Starting binning process for output: {output_name}")
    print("This may take several minutes depending on data size...")
    
    analysis.get_binned_data(
        unbinned_data=bg_file,
        # Use output_file without the extension
        output_name=os.path.splitext(output_file)[0],
        psichi_binning="local"
    )
    
    print(f"Background data binning completed successfully!")
    print(f"Output file created: {output_file}")
    
    # Verify the output file was created
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
        print(f"✓ Output file verified: {file_size:.2f} MB")
    else:
        print(f"Warning: Expected output file not found: {output_file}")
    
    return output_file


# ---[ from 4_dataAggregation.py ]---
import os
import sys
import gc
import numpy as np
from astropy.time import Time
from astropy.coordinates import SkyCoord
import astropy.units as u
from cosipy import SpacecraftFile
from histpy import Histogram
from cosipy import FastTSMap

def _orig_aggregate_data(data_folder):
    """
    Aggregate and prepare data for TS map computation based on notebook code
    """
    # Define file paths
    GRB_signal_path = os.path.join(data_folder, "GRB_bn081207680_binned_O3.hdf5")
    background_path = os.path.join(data_folder, "Total_BG_continuum_O3_binned.hdf5")
    orientation_path = os.path.join(data_folder, "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori")
    response_path = os.path.join(data_folder, "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5")
    
    # Check if files exist
    for path, name in [(GRB_signal_path, "GRB signal"), (background_path, "background"), 
                      (orientation_path, "orientation"), (response_path, "response")]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"{name} file not found: {path}")
    
    print("Reading GRB signal...")
    # Read the GRB signal
    signal = Histogram.open(GRB_signal_path)
    
    # get the starting and ending time tag of the GRB
    grb_tmin = signal.axes["Time"].edges.min()
    grb_tmax = signal.axes["Time"].edges.max()
    
    # project to three axes: measure energy(Em), scattering direction(PsiChi) and Compton scattering angle (Phi)
    signal = signal.project(['Em', 'PsiChi', 'Phi'])
    
    print("Reading background data...")
    # load the background file
    bkg_full = Histogram.open(background_path)
    
    # Extract 40s background from the 3-month one
    bkg_tmin_idx = np.where(bkg_full.axes['Time'].edges.value == grb_tmin.value)[0][0]  # the time idx corresponding to the tima tag
    bkg_tmax_idx = np.where(bkg_full.axes["Time"].edges.value == grb_tmax.value)[0][0]
    bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx,:]  # It slices the Time axis
    
    # project to three axes: measure energy(Em), scattering direction(PsiChi) and Compton scattering angle (Phi)
    bkg = bkg.project(['Em', 'PsiChi', 'Phi'])
    
    print("Assembling data...")
    # assemble the data
    data = bkg + signal
    
    print("Creating background model...")
    # calculate the duration of the background
    bkg_full_duration = (bkg_full.axes['Time'].edges.max() - bkg_full.axes['Time'].edges.min())
    
    # average the background model down to 40s
    bkg_model = bkg_full/(bkg_full_duration/40)
    
    # project to three axes: measure energy(Em), scattering direction(PsiChi) and Compton scattering angle (Phi)
    bkg_model = bkg_model.project(['Em', 'PsiChi', 'Phi'])
    
    print("Processing orientation data...")
    # read the full oritation but only get the interval for the GRB
    ori_full = SpacecraftFile.parse_from_file(orientation_path)
    grb_ori = ori_full.source_interval(Time(grb_tmin, format = "unix"), Time(grb_tmax, format = "unix"))
    
    # clear redundant data from RAM
    del bkg_full
    del ori_full
    _ = gc.collect()
    
    print("Creating FastTSMap object...")
    # here let's create a FastTSMap object for fitting the ts map in the following cells
    ts = FastTSMap(data = data, bkg_model = bkg_model, orientation = grb_ori, 
                   response_path = response_path, cds_frame = "local", scheme = "RING")
    
    # get a list of hypothesis coordinates to fit. The models will be put on these locations for get the expected counts from the source spectrum.
    # note that this nside is also the nside of the final TS map
    hypothesis_coords = FastTSMap.get_hypothesis_coords(nside = 16)
    
    # This the true location of the GRB
    coord = SkyCoord(l = 93, b = -53, unit = (u.deg, u.deg), frame = "galactic")
    
    # Save the aggregated data and objects for the next step
    output_data = {
        'data_folder': data_folder,
        'ts': ts,
        'coord': coord,
        'hypothesis_coords': hypothesis_coords,
        'data': data,
        'bkg_model': bkg_model,
        'grb_ori': grb_ori,
        'response_path': response_path
    }
    
    print("Data aggregation completed successfully!")
    return output_data


# ---[ from 5_tsmapcomputation.py ]---
import os
import sys
import pickle
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environment
import matplotlib.pyplot as plt

def _orig_compute_ts_map(data_folder):
    """
    Compute the final TS map and generate plots
    """
    # Load the aggregated data from the previous step
    # In a real implementation, you might want to use a more robust data passing mechanism
    # For now, we'll recreate the FastTSMap object
    
    print("Recreating FastTSMap object for TS computation...")
    
    # Import required modules
    import gc
    import numpy as np
    from astropy.time import Time
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    from cosipy import SpacecraftFile, FastTSMap
    from histpy import Histogram
    from threeML import Powerlaw
    
    # Define file paths
    GRB_signal_path = os.path.join(data_folder, "GRB_bn081207680_binned_O3.hdf5")
    background_path = os.path.join(data_folder, "Total_BG_continuum_O3_binned.hdf5")
    orientation_path = os.path.join(data_folder, "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori")
    response_path = os.path.join(data_folder, "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5")
    
    # Read the GRB signal
    signal = Histogram.open(GRB_signal_path)
    grb_tmin = signal.axes["Time"].edges.min()
    grb_tmax = signal.axes["Time"].edges.max()
    signal = signal.project(['Em', 'PsiChi', 'Phi'])
    
    # Load background data
    bkg_full = Histogram.open(background_path)
    bkg_tmin_idx = np.where(bkg_full.axes['Time'].edges.value == grb_tmin.value)[0][0]
    bkg_tmax_idx = np.where(bkg_full.axes["Time"].edges.value == grb_tmax.value)[0][0]
    bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx,:]
    bkg = bkg.project(['Em', 'PsiChi', 'Phi'])
    
    # Assemble data
    data = bkg + signal
    
    # Create background model
    bkg_full_duration = (bkg_full.axes['Time'].edges.max() - bkg_full.axes['Time'].edges.min())
    bkg_model = bkg_full/(bkg_full_duration/40)
    bkg_model = bkg_model.project(['Em', 'PsiChi', 'Phi'])
    
    # Process orientation
    ori_full = SpacecraftFile.parse_from_file(orientation_path)
    grb_ori = ori_full.source_interval(Time(grb_tmin, format = "unix"), Time(grb_tmax, format = "unix"))
    
    # Clear memory
    del bkg_full
    del ori_full
    _ = gc.collect()
    
    # Create FastTSMap object
    ts = FastTSMap(data = data, bkg_model = bkg_model, orientation = grb_ori, 
                   response_path = response_path, cds_frame = "local", scheme = "RING")
    
    # Define the true location of the GRB
    coord = SkyCoord(l = 93, b = -53, unit = (u.deg, u.deg), frame = "galactic")
    
    # get a list of hypothesis coordinates to fit. The models will be put on these locations for get the expected counts from the source spectrum.
    # note that this nside is also the nside of the final TS map
    hypothesis_coords = FastTSMap.get_hypothesis_coords(nside = 16)
    print("Computing TS map...")

    # Define spectrum
    index = -2.2
    K = 10 / u.cm / u.cm / u.s / u.keV
    piv = 100 * u.keV
    spectrum = Powerlaw()
    spectrum.index.value = index
    spectrum.K.value = K.value
    spectrum.piv.value = piv.value 
    spectrum.K.unit = K.unit
    spectrum.piv.unit = piv.unit
    
    # Generate TS map plots
    try:
        ts_results = ts.parallel_ts_fit(hypothesis_coords=hypothesis_coords, 
                                        energy_channel = [2,3], 
                                        spectrum=spectrum, 
                                        ts_scheme="RING", 
                                        cpu_cores=56)
        # plots the raw TS values, which is also an image of the GRB. However, 
        # for the purpose of localization, we are more interested in the confidence 
        # level of the imaged GRB. Thus, you can plot the 90% containment level of 
        # the GRB location by setting `containment` parameter to the percetage you 
        # want to plot. However, because the strength of the GRB signal is very 
        # very strong, the ts map looks the same under different containment levels.
        ts.plot_ts(save_plot = True, save_dir = data_folder, save_name = "ts_map.png")

        ts.plot_ts(containment = 0.9, save_plot = True, save_dir = data_folder, save_name = "ts_map_90containment.png")
        print(f"TS map data saved")
        
        print("TS map computation completed successfully!")
        return data_folder
        
    except Exception as e:
        print(f"Error during TS map computation: {str(e)}")
        raise e


# ---[ from 5_tsmapmulres_computation.py ]---
import os
import sys
import pickle
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environment
import matplotlib.pyplot as plt

def _orig_compute_ts_map_mulres(data_folder):
    """
    Compute the final TS map and generate plots
    """
    # Load the aggregated data from the previous step
    # In a real implementation, you might want to use a more robust data passing mechanism
    # For now, we'll recreate the FastTSMap object
    
    print("Recreating FastTSMap object for TS computation...")
    
    # Import required modules
    import gc
    import numpy as np
    from astropy.time import Time
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    from cosipy import SpacecraftFile, MOCTSMap
    from histpy import Histogram
    from threeML import Powerlaw
    
    # Define file paths
    GRB_signal_path = os.path.join(data_folder, "GRB_bn081207680_binned_O3.hdf5")
    background_path = os.path.join(data_folder, "Total_BG_continuum_O3_binned.hdf5")
    orientation_path = os.path.join(data_folder, "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori")
    response_path = os.path.join(data_folder, "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5")
    
    # Read the GRB signal
    signal = Histogram.open(GRB_signal_path)
    grb_tmin = signal.axes["Time"].edges.min()
    grb_tmax = signal.axes["Time"].edges.max()
    signal = signal.project(['Em', 'PsiChi', 'Phi'])
    
    # Load background data
    bkg_full = Histogram.open(background_path)
    bkg_tmin_idx = np.where(bkg_full.axes['Time'].edges.value == grb_tmin.value)[0][0]
    bkg_tmax_idx = np.where(bkg_full.axes["Time"].edges.value == grb_tmax.value)[0][0]
    bkg = bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx,:]
    bkg = bkg.project(['Em', 'PsiChi', 'Phi'])
    
    # Assemble data
    data = bkg + signal
    
    # Create background model
    bkg_full_duration = (bkg_full.axes['Time'].edges.max() - bkg_full.axes['Time'].edges.min())
    bkg_model = bkg_full/(bkg_full_duration/40)
    bkg_model = bkg_model.project(['Em', 'PsiChi', 'Phi'])
    
    # Process orientation
    ori_full = SpacecraftFile.parse_from_file(orientation_path)
    grb_ori = ori_full.source_interval(Time(grb_tmin, format = "unix"), Time(grb_tmax, format = "unix"))
    
    # Clear memory
    del bkg_full
    del ori_full
    _ = gc.collect()
    
    # Here we will us MOCTSMap instead of FastTSMap, the parameters are same
    moc_fit = MOCTSMap(data = data, 
                    bkg_model = bkg_model, 
                    response_path = response_path, 
                    orientation = grb_ori, # we don't need orientation since we are using the precomputed galactic reaponse
                    cds_frame = "local")
    
    # Define the true location of the GRB
    coord = SkyCoord(l = 93, b = -53, unit = (u.deg, u.deg), frame = "galactic")

    print("Computing TS map...")

    # Define spectrum
    index = -2.2
    K = 10 / u.cm / u.cm / u.s / u.keV
    piv = 100 * u.keV
    spectrum = Powerlaw()
    spectrum.index.value = index
    spectrum.K.value = K.value
    spectrum.piv.value = piv.value 
    spectrum.K.unit = K.unit
    spectrum.piv.unit = piv.unit

    # get a list of hypothesis coordinates to fit. The models will be put on these locations for get the expected counts from the source spectrum.
    # note that this nside is also the nside of the final TS map
    # here we need to give the order of map to stop fitting and the top 8 likelihood to find the pixels to upscale the resolution
    moc_map = moc_fit.moc_ts_fit(max_moc_order = 4, # this is the maximum order of the final map
                                top_number = 8, # In each iterations, only the pixels with top 8 likelihood values will be split in the next iteration
                                energy_channel = [2,3],  # The energy channel used to perform the fit.
                                spectrum = spectrum)
    
    # Generate TS map plots
    try:
        # here we need to give the order of map to stop fitting and the top 8 likelihood to find the pixels to upscale the resolution
        moc_map = moc_fit.moc_ts_fit(max_moc_order = 4, # this is the maximum order of the final map
                                    top_number = 8, # In each iterations, only the pixels with top 8 likelihood values will be split in the next iteration
                                    energy_channel = [2,3],  # The energy channel used to perform the fit.
                                    spectrum = spectrum)

        # plot the raw ts values
        moc_fit.plot_ts(dpi = 300, save_plot = True, save_dir = data_folder, save_name = "ts_map_multires.png")

        # plot the 90% confidence region
        # You can see from the plot below, we recover the same 90% containment region as we did in Example 3
        moc_fit.plot_ts(dpi = 300, containment = 0.9, save_plot = True, save_dir = data_folder, save_name = "ts_map_multires_90containment.png")

        print(f"TS map data saved")
        
        print("TS map computation completed successfully!")
        return data_folder
        
    except Exception as e:
        print(f"Error during TS map computation: {str(e)}")
        raise e



# ------------------ Public façades with Data Explorer logging ------------------
def aggregate_data(run_dir: str):
    _orig_aggregate_data(run_dir)
    _print_explorer_links(run_dir)
    return str(run_dir)

def compute_ts_map(run_dir: str) -> str:
    _orig_compute_ts_map(run_dir)
    _print_explorer_links(run_dir)
    return str(run_dir)

def compute_ts_map_mulres(run_dir: str) -> str:
    _orig_compute_ts_map_mulres(run_dir)
    _print_explorer_links(run_dir)
    return str(run_dir)
