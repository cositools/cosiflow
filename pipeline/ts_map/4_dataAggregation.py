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

def aggregate_data(data_folder):
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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python 4_dataAggregation.py <data_folder>")
        sys.exit(1)
    
    data_folder = sys.argv[1]
    result = aggregate_data(data_folder)
    print(f"Data aggregation completed for folder: {data_folder}")
