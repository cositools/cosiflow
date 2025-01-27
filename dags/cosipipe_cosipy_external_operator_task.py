from airflow.decorators import task, dag
from airflow.sensors.base import PokeReturnValue
from datetime import datetime
import os
import time


base_dir = '/home/gamma/workspace/data'

@dag(dag_id='cosipy_test_extoperator_v0',catchup=False)
def my_dag():
    
    @task.sensor(poke_interval=5, timeout=600, mode='poke')
    def check_new_file_sensor(ti):
        input_dir = base_dir+'/input'
        input_files = os.listdir(input_dir)
        
        if input_files:
            oldest_file = min([os.path.join(input_dir, f) for f in input_files], key=os.path.getctime)
            if os.path.exists(oldest_file):
                ti.xcom_push(key='new_file_path', value=oldest_file)
                return PokeReturnValue(is_done=True, xcom_value=oldest_file)
        
        return PokeReturnValue(is_done=False)

    @task.external_python(python='/home/gamma/.conda/envs/cosipy/bin/python')
    def process_file(file_path):
        
        
        print(file_path)
        import os
        from cosipy.util import fetch_wasabi_file
        from cosipy import BinnedData
        from pathlib import Path
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


    file_path = check_new_file_sensor()
    process_file(file_path)

my_dag()
