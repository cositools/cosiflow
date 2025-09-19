import os
import sys
import pickle
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environment
import matplotlib.pyplot as plt

def compute_ts_map(data_folder):
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
        ts.plot_ts(skycoord = coord, save_plot = True)

        ts.plot_ts(skycoord = coord, containment = 0.9, save_plot = True)
        print(f"TS map data saved")
        
        print("TS map computation completed successfully!")
        return data_folder
        
    except Exception as e:
        print(f"Error during TS map computation: {str(e)}")
        raise e

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python 5_tsmapcomputation.py <data_folder>")
        sys.exit(1)
    
    data_folder = sys.argv[1]
    result = compute_ts_map(data_folder)
    print(f"TS map computation completed for folder: {data_folder}")
    print(f"Results: {result}")
