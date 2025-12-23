from astropy.io import fits
from astropy.table import Table
import os
import numpy as np
import argparse

"""
This script is used to cut the background window from the background file.
"""

def check_file(path):
    """Check if the file exists"""
    print(f"Checking file: {path}")
    # Check if the file exists
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")
    print(f"File found: {path}")
    return True

def open_background(background_path):
    """Open the background file"""
    check_file(background_path)
    # Open the background file
    print(f"Opening background file: {background_path}")
    hdul = fits.open(background_path)
    bkg_full = hdul[1].data
    return bkg_full

def get_times_source(source_path):
    """Get the starting and ending time tag of the GRB"""
    check_file(source_path)
    # Read the GRB signal
    signal = fits.open(source_path)
    # get the starting and ending time tag of the GRB
    times = signal[1].data["TimeTags"]
    grb_tmin = float(np.min(times))
    grb_tmax = float(np.max(times))
    grb_duration = grb_tmax - grb_tmin
    return grb_tmin, grb_tmax, grb_duration
    
def extract_bkg_window(background_path, source_path, eps_time = 0.000000001):
    """Extract the background window from the background file. 
       The eps_time is the adding time in seconds to add before and after the source time 
       to extract the background. It is a safety margin to avoid edge effects."""
    bkg_full = open_background(background_path)
    grb_tmin, grb_tmax, _ = get_times_source(source_path)
    mask = (bkg_full['TimeTags'] >= grb_tmin - eps_time) & (bkg_full['TimeTags'] <= grb_tmax + eps_time)
    bkg_cut = bkg_full[mask]
    return bkg_cut

def save_bkg_window(background_path, source_path, eps_time):
    """Save the background window to a file"""
    # Reuse the same name of the background file but with the word "cut" added
    # output_path = background_path.replace(".fits.gz", "_cut.fits")
    base, ext = os.path.splitext(background_path)
    output_path = base + "_window.fits"
    bkg_cut = extract_bkg_window(background_path, source_path, eps_time)
    # Save the background window to a file
    print(f"Saving background window to: {output_path}")
    t = Table(bkg_cut)
    t.write(output_path, format="fits", overwrite=True)
    print(f"Background window saved to: {output_path}")
    return True

def main():
    """Main function to cut the background window from the background file based on GRB time range
    Args:
        grb_source_path: Path to the GRB source FITS file
        total_background_path: Path to the total background FITS file (can be compressed .fits.gz)
        --eps_time <eps_time>: Time margin in seconds to add before and after source time (default: 1e-9)
    Returns:
        None
    """
    parser = argparse.ArgumentParser(description="Cut background window from background file based on GRB time range")
    parser.add_argument("grb_source_path", help="Path to the GRB source FITS file (can be compressed .fits.gz)")
    parser.add_argument("total_background_path", help="Path to the total background FITS file (can be compressed .fits.gz)")
    parser.add_argument("--eps_time", type=float, default=0.000000001, 
                       help="Time margin in seconds to add before and after source time (default: 1e-9)")
    
    args = parser.parse_args()
    
    save_bkg_window(args.total_background_path, args.grb_source_path, args.eps_time)

if __name__ == "__main__":
    main()