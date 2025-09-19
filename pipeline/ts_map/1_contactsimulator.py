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

if __name__ == "__main__":
    new_folder = create_new_data_folder()
    print(f"Data simulation complete. New folder: {new_folder}")
