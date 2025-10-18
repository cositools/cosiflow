from cosipy import BinnedData

analysis = BinnedData("inputs.yaml")

analysis.get_binned_data(unbinned_data = "/project/majello/astrohe/yong/COSI/Data_Challenges/DC3/Unbinned_raw_data/Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz", 
                         output_name = "Total_BG_continuum_O3_binned", 
                         psichi_binning = "local")
