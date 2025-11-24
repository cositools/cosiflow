"""
Utility functions used by the cosipipe_tsmap DAG with ExternalPythonOperator.
All functions are self-contained and do not rely on DAG-level globals.

- bin_grb_data: bin the GRB data source based on the bin_grb.py script
- bin_background_data: bin the background data based on the bin_bg.py script
- compute_ts_map: compute the TS map based on the ts_map.py script
- compute_ts_map_mulres: compute the TS map based on the ts_map_mulres.py script
"""
from __future__ import annotations

import os
from pathlib import Path

# =====================================================================
# =============== Inlined pipeline step implementations ===============
# =====================================================================


# ---[ from 2_binGRBdatasource.py ]---
import os
import sys
import yaml
from pathlib import Path

def bin_grb_data(unbinned_file_path: str, data_folder: str) -> str:
    """
    Bin GRB data source based on the bin_grb.py script
    """

    print(f"[bin_grb_data] Found GRB unbinned fits file: {unbinned_file_path}")
    
    # Define output file path
    extension = unbinned_file_path.split(".")[-1][:-1]
    binned_file_name = unbinned_file_path.replace("_unbinned_", "_binned_").replace(extension, "")
    binned_file_path = os.path.join(data_folder, f"{binned_file_name}.hdf5")
    
    print(f"[bin_grb_data] Expected output file: {binned_file_path}")
    
    # Check if binned file already exists
    if os.path.exists(binned_file_path):
        print(f"[bin_grb_data] Binned GRB file already exists: {binned_file_path}")
        print("[bin_grb_data] Skipping GRB binning step.")
        return binned_file_path
    
    print("[bin_grb_data] Binned GRB file not found. Proceeding with binning process...")
    
    # Construct paths
    # Create inputs.yaml configuration for binning
    print("[bin_grb_data] Creating GRB binning configuration...")
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
    
    print(f"[bin_grb_data] ✓ Created GRB binning configuration: {inputs_path}")
    print(f"[bin_grb_data] Configuration details:")
    print(f"[bin_grb_data]   - Time bins: {config['time_bins']} seconds")
    print(f"[bin_grb_data]   - Energy bins: {len(config['energy_bins'])-1} bins from {config['energy_bins'][0]} to {config['energy_bins'][-1]} keV")
    print(f"[bin_grb_data]   - Phi pixel size: {config['phi_pix_size']} degrees")
    print(f"[bin_grb_data]   - Nside: {config['nside']}")
    print(f"[bin_grb_data]   - Scheme: {config['scheme']}")
    print(f"[bin_grb_data]   - Time range: {config['tmin']} to {config['tmax']}")
    print(f"[bin_grb_data]   - GRB duration: {config['tmax'] - config['tmin']} seconds")
    
    # Execute the binning process
    print("[bin_grb_data] Initializing COSIpy BinnedData analysis...")
    from cosipy import BinnedData
    
    analysis = BinnedData(inputs_path)
    print("[bin_grb_data] ✓ BinnedData analysis object created successfully")
    
    print(f"[bin_grb_data] Starting binning process for output: {binned_file_name}")
    print("[bin_grb_data] This may take several minutes depending on data size...")
    
    analysis.get_binned_data(
        unbinned_data=unbinned_file_path,
        output_name=binned_file_name,
        psichi_binning="local"
    )
    
    print(f"[bin_grb_data] GRB data binning completed successfully!")
    print(f"[bin_grb_data] Output file created: {binned_file_path}")
    
    # Verify the output file was created
    if os.path.exists(binned_file_path):
        file_size = os.path.getsize(binned_file_path) / (1024 * 1024)  # Size in MB
        print(f"[bin_grb_data] Output file verified: {file_size:.2f} MB")
    else:
        print(f"[bin_grb_data] Warning: Expected output file not found: {binned_file_path}")
    
    return binned_file_path


# ---[ from 3_binBackground.py ]---
import os
import sys
import yaml
from pathlib import Path

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
        psichi_binning="local"
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


# ---[ from 4_tsmapcomputation.py ]---
import os
import sys
import pickle
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environment
import matplotlib.pyplot as plt

def compute_ts_map(grb_signal_path: str, 
                   background_path: str, 
                   orientation_path: str, 
                   response_path: str, 
                   data_folder: str) -> dict:
    """
    Compute the final TS map and generate plots
    """
    # Load the aggregated data from the previous step
    # In a real implementation, you might want to use a more robust data passing mechanism
    # For now, we'll recreate the FastTSMap object

    # Check if files exist
    for path, name in [(grb_signal_path, "GRB signal"), 
                       (background_path, "background"), 
                       (orientation_path, "orientation"), 
                       (response_path, "response")]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"{name} file not found: {path}")
    
    print(f"[compute_ts_map] Recreating FastTSMap object for TS computation...")
    
    # Import required modules
    import gc
    import numpy as np
    from astropy.time import Time
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    from cosipy import SpacecraftFile, FastTSMap
    from histpy import Histogram
    from threeML import Powerlaw
    
    # Read the GRB signal
    signal = Histogram.open(grb_signal_path)
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
    print(f"[compute_ts_map] Computing TS map...")

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
        print(f"[compute_ts_map] TS map data saved")
        
        print(f"[compute_ts_map] TS map computation completed successfully!")
        return data_folder
        
    except Exception as e:
        print(f"[compute_ts_map] Error during TS map computation: {str(e)}")
        raise e


# ---[ from 4_tsmapmulres_computation.py ]---
import os
import sys
import pickle
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environment
import matplotlib.pyplot as plt

def compute_ts_map_mulres(grb_signal_path: str, 
                         background_path: str, 
                         orientation_path: str, 
                         response_path: str, 
                         data_folder: str) -> dict:
    """
    Compute the final TS map and generate plots
    """
    # Load the aggregated data from the previous step
    # In a real implementation, you might want to use a more robust data passing mechanism
    # For now, we'll recreate the FastTSMap object
    
    print(f"[compute_ts_map_mulres] Recreating FastTSMap object for TS computation...")
    
    # Import required modules
    import gc
    import numpy as np
    from astropy.time import Time
    from astropy.coordinates import SkyCoord
    import astropy.units as u
    from cosipy import SpacecraftFile, MOCTSMap
    from histpy import Histogram
    from threeML import Powerlaw
    
    # Read the GRB signal
    signal = Histogram.open(grb_signal_path)
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

    print(f"[compute_ts_map_mulres] Computing TS map...")

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

        print(f"[compute_ts_map_mulres] TS map data saved")
        
        print(f"[compute_ts_map_mulres] TS map computation completed successfully!")
        return data_folder
        
    except Exception as e:
        print(f"[compute_ts_map_mulres] Error during TS map computation: {str(e)}")
        raise e