import os
import sys
import gc
import pickle
import yaml
import numpy as np

# Set matplotlib cache directory for headless environments
os.environ.setdefault("MPLCONFIGDIR", "/tmp/mpl")
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend for server environment
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
from astropy.time import Time
from astropy.coordinates import SkyCoord
import astropy.units as u
from cosipy import SpacecraftFile, FastTSMap, MOCTSMap, BinnedData
from histpy import Histogram
from threeML import Powerlaw


class TSMapPipeline:
    """
    Pipeline class for TS Map computation with COSIpy.
    Supports both standard and multi-resolution TS map computation.
    """
    
    def __init__(self, data_folder, multi_resolution=False, 
                 background_file=None, source_file=None, 
                 response_file=None, orientation_file=None):
        """
        Initialize the TS Map Pipeline.
        
        Parameters:
        -----------
        data_folder : str
            Path to the data folder containing all input files
        multi_resolution : bool, optional
            If True, use multi-resolution TS map computation (MOCTSMap)
            If False, use standard TS map computation (FastTSMap)
        background_file : str, optional
            Name of the background file (default: Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz)
        source_file : str, optional
            Name of the GRB source file (default: GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz)
        response_file : str, optional
            Name of the response file (default: ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5)
        orientation_file : str, optional
            Name of the orientation file (default: DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori)
        """
        self.data_folder = data_folder
        self.multi_resolution = multi_resolution
        
        # Set default file names if not provided
        self.background_file = background_file or "Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz"
        self.source_file = source_file or "GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz"
        self.response_file = response_file or "ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5"
        self.orientation_file = orientation_file or "DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori"
        
        # Construct full paths
        self.background_path = os.path.join(data_folder, self.background_file)
        self.source_path = os.path.join(data_folder, self.source_file)
        self.response_path = os.path.join(data_folder, self.response_file)
        self.orientation_path = os.path.join(data_folder, self.orientation_file)
        
        # Binned file paths
        self.binned_background_path = os.path.join(data_folder, "Total_BG_continuum_O3_binned.hdf5")
        self.binned_grb_path = os.path.join(data_folder, "GRB_bn081207680_binned_O3.hdf5")
        
        # Output paths
        self.plots_dir = os.path.join(data_folder, "plots")
        self.ts_map_data_path = os.path.join(data_folder, "ts_map_data.pkl")
        
        # Initialize data objects (will be set during processing)
        self.signal = None
        self.bkg_full = None
        self.bkg = None
        self.data = None
        self.bkg_model = None
        self.grb_ori = None
        self.ts = None
        self.coord = None
        self.hypothesis_coords = None
        self.spectrum = None
        
        print(f"TSMapPipeline initialized:")
        print(f"  - Data folder: {data_folder}")
        print(f"  - Multi-resolution: {multi_resolution}")
        print(f"  - Background file: {self.background_file}")
        print(f"  - Source file: {self.source_file}")
        print(f"  - Response file: {self.response_file}")
        print(f"  - Orientation file: {self.orientation_file}")
    
    def check_files_exist(self):
        """Check if all required input files exist."""
        required_files = [
            (self.background_path, "background"),
            (self.source_path, "GRB source"),
            (self.response_path, "response"),
            (self.orientation_path, "orientation")
        ]
        
        missing_files = []
        for file_path, file_type in required_files:
            if not os.path.exists(file_path):
                missing_files.append(f"{file_type}: {file_path}")
        
        if missing_files:
            raise FileNotFoundError(f"Missing required files:\n" + "\n".join(missing_files))
        
        print("✓ All required input files found")
        return True
    
    def bin_grb_data(self):
        """Bin GRB data source using COSIpy."""
        print(f"Starting GRB binning process...")
        
        # Check if binned file already exists
        if os.path.exists(self.binned_grb_path):
            print(f"✓ Binned GRB file already exists: {self.binned_grb_path}")
            print("Skipping GRB binning step.")
            return self.binned_grb_path
        
        print("✗ Binned GRB file not found. Proceeding with binning process...")
        
        if not os.path.exists(self.source_path):
            raise FileNotFoundError(f"GRB data file not found: {self.source_path}")
        
        print(f"✓ Found GRB data file: {self.source_path}")
        
        # Create inputs.yaml configuration for binning
        print("Creating GRB binning configuration...")
        config = {
            "data_file": self.source_path,
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
        inputs_path = os.path.join(self.data_folder, "inputs_grb.yaml")
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
        
        # FIX: Force working directory and use absolute paths
        from pathlib import Path
        os.makedirs(self.data_folder, exist_ok=True)
        old_cwd = os.getcwd()
        
        try:
            # Change to data folder to ensure files are saved in the right place
            os.chdir(self.data_folder)
            print(f"Changed working directory to: {os.getcwd()}")
            
            # Execute the binning process
            print("Initializing COSIpy BinnedData analysis...")
            analysis = BinnedData(inputs_path)
            print("✓ BinnedData analysis object created successfully")
            
            output_name = "GRB_bn081207680_binned_O3"
            print(f"Starting binning process for output: {output_name}")
            print("This may take several minutes depending on data size...")
            
            analysis.get_binned_data(
                unbinned_data=self.source_path,
                output_name=output_name,
                psichi_binning="local"
            )
            
            print(f"GRB data binning completed successfully!")
            print(f"Output file created: {self.binned_grb_path}")
            
        finally:
            # Always restore original working directory
            os.chdir(old_cwd)
            print(f"Restored working directory to: {os.getcwd()}")
        
        # Verify the output file was created with robust diagnostics
        if not os.path.exists(self.binned_grb_path):
            # Diagnostic information
            ls_here = "\n".join(sorted(os.listdir(self.data_folder)))
            raise FileNotFoundError(
                f"GRB binning expected at:\n  {self.binned_grb_path}\n"
                f"but not found.\nCWD during save: {self.data_folder}\n"
                f"Listing data_folder:\n{ls_here}"
            )
        
        # Verify file size
        file_size = os.path.getsize(self.binned_grb_path) / (1024 * 1024)  # Size in MB
        print(f"✓ Output file verified: {file_size:.2f} MB")
        
        return self.binned_grb_path
    
    def bin_background_data(self):
        """Bin background data using COSIpy."""
        print(f"Starting background binning process...")
        
        # Check if binned file already exists
        if os.path.exists(self.binned_background_path):
            print(f"✓ Binned background file already exists: {self.binned_background_path}")
            print("Skipping background binning step.")
            return self.binned_background_path
        
        print("✗ Binned background file not found. Proceeding with binning process...")
        
        if not os.path.exists(self.background_path):
            raise FileNotFoundError(f"Background data file not found: {self.background_path}")
        
        print(f"✓ Found background data file: {self.background_path}")
        
        # Create inputs.yaml configuration for binning
        print("Creating background binning configuration...")
        config = {
            "data_file": self.background_path,
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
        inputs_path = os.path.join(self.data_folder, "inputs_bg.yaml")
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
        analysis = BinnedData(inputs_path)
        print("✓ BinnedData analysis object created successfully")
        
        output_name = "Total_BG_continuum_O3_binned"
        print(f"Starting binning process for output: {output_name}")
        print("This may take several minutes depending on data size...")
        
        analysis.get_binned_data(
            unbinned_data=self.background_path,
            output_name=output_name,
            psichi_binning="local"
        )
        
        print(f"Background data binning completed successfully!")
        print(f"Output file created: {self.binned_background_path}")
        
        # Verify the output file was created
        if os.path.exists(self.binned_background_path):
            file_size = os.path.getsize(self.binned_background_path) / (1024 * 1024)  # Size in MB
            print(f"✓ Output file verified: {file_size:.2f} MB")
        else:
            print(f"Warning: Expected output file not found: {self.binned_background_path}")
        
        return self.binned_background_path
    
    def aggregate_data(self):
        """Aggregate and prepare data for TS map computation."""
        print("Starting data aggregation process...")
        
        # Check if binned files exist
        required_files = [
            (self.binned_grb_path, "GRB signal"),
            (self.binned_background_path, "background"),
            (self.orientation_path, "orientation"),
            (self.response_path, "response")
        ]
        
        for path, name in required_files:
            if not os.path.exists(path):
                raise FileNotFoundError(f"{name} file not found: {path}")
        
        print("Reading GRB signal...")
        # Read the GRB signal
        self.signal = Histogram.open(self.binned_grb_path)
        
        # get the starting and ending time tag of the GRB
        grb_tmin = self.signal.axes["Time"].edges.min()
        grb_tmax = self.signal.axes["Time"].edges.max()
        
        # project to three axes: measure energy(Em), scattering direction(PsiChi) and Compton scattering angle (Phi)
        self.signal = self.signal.project(['Em', 'PsiChi', 'Phi'])
        
        print("Reading background data...")
        # load the background file
        self.bkg_full = Histogram.open(self.binned_background_path)
        
        # Extract 40s background from the 3-month one
        bkg_tmin_idx = np.where(self.bkg_full.axes['Time'].edges.value == grb_tmin.value)[0][0]
        bkg_tmax_idx = np.where(self.bkg_full.axes["Time"].edges.value == grb_tmax.value)[0][0]
        self.bkg = self.bkg_full.slice[bkg_tmin_idx:bkg_tmax_idx,:]
        
        # project to three axes: measure energy(Em), scattering direction(PsiChi) and Compton scattering angle (Phi)
        self.bkg = self.bkg.project(['Em', 'PsiChi', 'Phi'])
        
        print("Assembling data...")
        # assemble the data
        self.data = self.bkg + self.signal
        
        print("Creating background model...")
        # calculate the duration of the background
        bkg_full_duration = (self.bkg_full.axes['Time'].edges.max() - self.bkg_full.axes['Time'].edges.min())
        
        # average the background model down to 40s
        self.bkg_model = self.bkg_full/(bkg_full_duration/40)
        
        # project to three axes: measure energy(Em), scattering direction(PsiChi) and Compton scattering angle (Phi)
        self.bkg_model = self.bkg_model.project(['Em', 'PsiChi', 'Phi'])
        
        print("Processing orientation data...")
        # read the full orientation but only get the interval for the GRB
        ori_full = SpacecraftFile.parse_from_file(self.orientation_path)
        self.grb_ori = ori_full.source_interval(Time(grb_tmin, format = "unix"), Time(grb_tmax, format = "unix"))
        
        # clear redundant data from RAM
        del ori_full
        _ = gc.collect()
        
        print("Creating FastTSMap object...")
        # Create FastTSMap object
        self.ts = FastTSMap(data = self.data, bkg_model = self.bkg_model, orientation = self.grb_ori, 
                           response_path = self.response_path, cds_frame = "local", scheme = "RING")
        
        # get a list of hypothesis coordinates to fit
        self.hypothesis_coords = FastTSMap.get_hypothesis_coords(nside = 16)
        
        # This the true location of the GRB
        self.coord = SkyCoord(l = 93, b = -53, unit = (u.deg, u.deg), frame = "galactic")
        
        print("Data aggregation completed successfully!")
        return self
    
    def compute_ts_map(self):
        """Compute the TS map using either standard or multi-resolution approach."""
        print("Starting TS map computation...")
        
        # Create output directory for plots
        os.makedirs(self.plots_dir, exist_ok=True)
        
        # Define spectrum
        index = -2.2
        K = 10 / u.cm / u.cm / u.s / u.keV
        piv = 100 * u.keV
        self.spectrum = Powerlaw()
        self.spectrum.index.value = index
        self.spectrum.K.value = K.value
        self.spectrum.piv.value = piv.value 
        self.spectrum.K.unit = K.unit
        self.spectrum.piv.unit = piv.unit
        
        try:
            if self.multi_resolution:
                print("Using multi-resolution TS map computation (MOCTSMap)...")
                return self._compute_multi_resolution_ts_map()
            else:
                print("Using standard TS map computation (FastTSMap)...")
                return self._compute_standard_ts_map()
                
        except Exception as e:
            print(f"Error during TS map computation: {str(e)}")
            raise e
    
    def _plot_ts_get_fig_ax(self, **kwargs):
        """Robust wrapper for plot_ts that handles different return types."""
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        
        ret = self.ts.plot_ts(**kwargs)  # può tornare (fig, ax) oppure None
        if isinstance(ret, tuple) and len(ret) == 2:
            return ret  # fig, ax
        
        # Fallback: recupera la figura/assi correnti (molte API disegnano ma non restituiscono)
        fig = plt.gcf()
        ax = plt.gca() if fig is not None else None
        if fig is None or ax is None:
            raise RuntimeError("plot_ts() non ha restituito (fig, ax) e non c'è una figura corrente.")
        return fig, ax

    def _plot_ts_get_fig_ax_moc(self, moc_fit, **kwargs):
        """Robust wrapper for MOCTSMap plot_ts that handles different return types."""
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        
        ret = moc_fit.plot_ts(**kwargs)  # può tornare (fig, ax) oppure None
        if isinstance(ret, tuple) and len(ret) == 2:
            return ret  # fig, ax
        
        # Fallback: recupera la figura/assi correnti (molte API disegnano ma non restituiscono)
        fig = plt.gcf()
        ax = plt.gca() if fig is not None else None
        if fig is None or ax is None:
            raise RuntimeError("moc_fit.plot_ts() non ha restituito (fig, ax) e non c'è una figura corrente.")
        return fig, ax

    def _compute_standard_ts_map(self):
        """Compute standard TS map using FastTSMap."""
        print("Computing standard TS map...")
        
        import matplotlib.pyplot as plt
        
        # Use the existing FastTSMap object
        ts_results = self.ts.parallel_ts_fit(hypothesis_coords=self.hypothesis_coords,
                                           energy_channel = [2,3],
                                           spectrum=self.spectrum,
                                           ts_scheme="RING",
                                           cpu_cores=56)
        
        # Generate TS map plot
        print("Generating TS map plot...")
        fig, ax = self._plot_ts_get_fig_ax(skycoord=self.coord, save_plot=False)
        plot_path = os.path.join(self.plots_dir, "ts_map.png")
        fig.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"TS map plot saved to: {plot_path}")
        
        # Generate TS map plot with containment
        print("Generating TS map plot with 90% containment...")
        fig, ax = self._plot_ts_get_fig_ax(skycoord=self.coord, containment=0.9, save_plot=False)
        plot_path_containment = os.path.join(self.plots_dir, "ts_map_90containment.png")
        fig.savefig(plot_path_containment, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"TS map plot with containment saved to: {plot_path_containment}")
        
        # Save the TS map data
        with open(self.ts_map_data_path, 'wb') as f:
            pickle.dump(self.ts, f)
        print(f"TS map data saved to: {self.ts_map_data_path}")
        
        print("Standard TS map computation completed successfully!")
        return {
            'ts_map_plot': plot_path,
            'ts_map_containment_plot': plot_path_containment,
            'ts_map_data': self.ts_map_data_path,
            'data_folder': self.data_folder,
            'method': 'standard'
        }
    
    def _compute_multi_resolution_ts_map(self):
        """Compute multi-resolution TS map using MOCTSMap."""
        print("Computing multi-resolution TS map...")
        
        import matplotlib.pyplot as plt
        
        # Create MOCTSMap object
        moc_fit = MOCTSMap(data = self.data,
                          bkg_model = self.bkg_model,
                          response_path = self.response_path,
                          orientation = self.grb_ori,
                          cds_frame = "local")
        
        # Compute multi-resolution TS map
        moc_map = moc_fit.moc_ts_fit(max_moc_order = 4,  # maximum order of the final map
                                    top_number = 8,      # top 8 likelihood values will be split
                                    energy_channel = [2,3],  # energy channel used for fit
                                    spectrum = self.spectrum)
        
        # Generate TS map plot
        print("Generating multi-resolution TS map plot...")
        fig, ax = self._plot_ts_get_fig_ax_moc(moc_fit, skycoord=self.coord, save_plot=False)
        plot_path = os.path.join(self.plots_dir, "ts_map_multi_res.png")
        fig.savefig(plot_path, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"Multi-resolution TS map plot saved to: {plot_path}")
        
        # Generate TS map plot with containment
        print("Generating multi-resolution TS map plot with 90% containment...")
        fig, ax = self._plot_ts_get_fig_ax_moc(moc_fit, skycoord=self.coord, containment=0.9, save_plot=False)
        plot_path_containment = os.path.join(self.plots_dir, "ts_map_multi_res_90containment.png")
        fig.savefig(plot_path_containment, dpi=300, bbox_inches='tight')
        plt.close(fig)
        print(f"Multi-resolution TS map plot with containment saved to: {plot_path_containment}")
        
        # Save the TS map data
        with open(self.ts_map_data_path, 'wb') as f:
            pickle.dump(moc_fit, f)
        print(f"Multi-resolution TS map data saved to: {self.ts_map_data_path}")
        
        print("Multi-resolution TS map computation completed successfully!")
        return {
            'ts_map_plot': plot_path,
            'ts_map_containment_plot': plot_path_containment,
            'ts_map_data': self.ts_map_data_path,
            'data_folder': self.data_folder,
            'method': 'multi_resolution'
        }
    
    def run_full_pipeline(self):
        """Run the complete TS map pipeline."""
        print("=" * 60)
        print("STARTING TS MAP PIPELINE")
        print("=" * 60)
        
        try:
            # Step 1: Check files exist
            self.check_files_exist()
            
            # Step 2: Bin GRB data
            print("\n" + "=" * 40)
            print("STEP 2: BINNING GRB DATA")
            print("=" * 40)
            self.bin_grb_data()
            
            # Step 3: Bin background data
            print("\n" + "=" * 40)
            print("STEP 3: BINNING BACKGROUND DATA")
            print("=" * 40)
            self.bin_background_data()
            
            # Step 4: Aggregate data
            print("\n" + "=" * 40)
            print("STEP 4: DATA AGGREGATION")
            print("=" * 40)
            self.aggregate_data()
            
            # Step 5: Compute TS map
            print("\n" + "=" * 40)
            print("STEP 5: TS MAP COMPUTATION")
            print("=" * 40)
            results = self.compute_ts_map()
            
            print("\n" + "=" * 60)
            print("PIPELINE COMPLETED SUCCESSFULLY!")
            print("=" * 60)
            print(f"Results: {results}")
            
            return results
            
        except Exception as e:
            print(f"\n❌ PIPELINE FAILED: {str(e)}")
            raise e
    
    def get_summary(self):
        """Get a summary of the pipeline configuration and status."""
        summary = {
            'data_folder': self.data_folder,
            'multi_resolution': self.multi_resolution,
            'files': {
                'background': self.background_file,
                'source': self.source_file,
                'response': self.response_file,
                'orientation': self.orientation_file
            },
            'paths': {
                'background_path': self.background_path,
                'source_path': self.source_path,
                'response_path': self.response_path,
                'orientation_path': self.orientation_path,
                'binned_background_path': self.binned_background_path,
                'binned_grb_path': self.binned_grb_path,
                'plots_dir': self.plots_dir,
                'ts_map_data_path': self.ts_map_data_path
            },
            'status': {
                'background_exists': os.path.exists(self.background_path),
                'source_exists': os.path.exists(self.source_path),
                'response_exists': os.path.exists(self.response_path),
                'orientation_exists': os.path.exists(self.orientation_path),
                'binned_background_exists': os.path.exists(self.binned_background_path),
                'binned_grb_exists': os.path.exists(self.binned_grb_path)
            }
        }
        return summary


# Example usage and testing
if __name__ == "__main__":
    # Example usage
    data_folder = "/home/gamma/workspace/data/tsmap/20250118_120000"
    
    # Create pipeline instance
    pipeline = TSMapPipeline(
        data_folder=data_folder,
        multi_resolution=False,  # Set to True for multi-resolution
        background_file="Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        source_file="GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        response_file="ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
        orientation_file="DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori"
    )
    
    # Print summary
    print("Pipeline Summary:")
    import json
    print(json.dumps(pipeline.get_summary(), indent=2))
    
    # Run full pipeline
    # results = pipeline.run_full_pipeline()
    # print(f"Pipeline results: {results}")
