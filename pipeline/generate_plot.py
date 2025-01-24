import sys,os
from cosipy.util import fetch_wasabi_file
from cosipy import BinnedData
from pathlib import Path

#/home/gamma/workspace/heasarc/dl0/2025-01-24_14-16-50/GalacticScan.inc1.id1.crab2hr.extracted.tra.gz

# create the inputs.yaml file to process the data.
print("test")
print(sys.argv[1])
file_path = sys.argv[1]
dir_name = os.path.dirname(file_path)

content_to_write = f"""#----------#
# Data I/O:

# data files available on the COSI Sharepoint: https://drive.google.com/drive/folders/1UdLfuLp9Fyk4dNussn1wt7WEOsTWrlQ6
data_file: {file_path} # full path
ori_file: "NA" # full path
unbinned_output: 'hdf5' # 'fits' or 'hdf5'
time_bins: 60 # time bin size in seconds. Takes int, float, or list of bin edges.
energy_bins: [100.,  200.,  500., 1000., 2000., 5000.] # Takes list. Needs to match response.
phi_pix_size: 6 # binning of Compton scattering anlge [deg]
nside: 8 # healpix binning of psi chi local
scheme: 'ring' # healpix binning of psi chi local
tmin: 1835478000.0 # Min time cut in seconds.
tmax: 1835485200.0 # Max time cut in seconds.
#----------#
"""

dir_name_path = Path(dir_name)

# Open the file in write mode and write the content
with open(dir_name_path / "inputs.yaml", "w") as file:
    file.write(content_to_write)


analysis = BinnedData(dir_name_path / "inputs.yaml")
analysis.read_tra(output_name = dir_name_path / "unbinned_data")
analysis.get_binned_data()
analysis.get_raw_spectrum(output_name = file_path.replace(".crab2hr.extracted.tra.gz",""))
analysis.get_raw_lightcurve(output_name = file_path.replace(".crab2hr.extracted.tra.gz",""))
