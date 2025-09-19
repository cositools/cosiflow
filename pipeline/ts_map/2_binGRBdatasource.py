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
    grb_file = os.path.join(data_folder, "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz")
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
        output_name=output_name,
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

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python 2_binGRBdatasource.py <data_folder>")
        sys.exit(1)
    
    data_folder = sys.argv[1]
    result = bin_grb_data(data_folder)
    print(f"Binned GRB data saved to: {result}")
