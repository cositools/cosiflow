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
    bg_file = os.path.join(data_folder, "Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz")
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
        output_name=output_name,
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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python 3_binBackground.py <data_folder>")
        sys.exit(1)
    
    data_folder = sys.argv[1]
    result = bin_background_data(data_folder)
    print(f"Binned background data saved to: {result}")
