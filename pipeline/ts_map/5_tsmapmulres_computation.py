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
        moc_fit.plot_ts(dpi = 300, skycoord=coord, save_plot = True)

        # plot the 90% confidence region
        # You can see from the plot below, we recover the same 90% containment region as we did in Example 3
        moc_fit.plot_ts(dpi = 300, skycoord=coord, containment = 0.9, save_plot = True)

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
