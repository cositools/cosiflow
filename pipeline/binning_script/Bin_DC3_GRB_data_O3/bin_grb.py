from cosipy import BinnedData

analysis = BinnedData("inputs.yaml")

analysis.get_binned_data(unbinned_data = "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz", 
                         output_name = "GRB_bn081207680_binned_O3", 
                         psichi_binning = "local")
