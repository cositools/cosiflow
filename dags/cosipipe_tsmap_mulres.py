from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    'owner': 'cosipy_team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
"""
The TS map we fit above loops over all the pixels of the entire sky, which has already taken a long time. 
If you want to increase the resolution/order of the TS map, the number of pixels will grow exponentially:

$$
npix=12\times4^{order}
$$


For a map of order 3, you will fit the entire sky with 768 pixels. For a map of order 4, you will end up 
with 3072 pixels to fit! To speed up the fitting, we can fit a multi-resolution map (also called 
multi-order coverage map, MOC map) instead of the single-resolution map we did before.

The multi-resolution map fitting will reduce the number of pixels to fit by fitting the background region 
with low resolution while keeping the source region with the details we want. We will use Crab as an example 
to show you how to fit a multi-resolution map to save your time and computational resources.
"""
dag = DAG(
    'cosipipe_tsmap_mulres',
    default_args=default_args,
    description='COSI Multi-Resolution TS Map computation pipeline - fits background region with low resolution ' 
                'while keeping source region with high resolution to save computational resources',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['cosipy', 'tsmap', 'grb', 'multi-resolution', 'moc'],
)

# Define the directory where our scripts are located
SCRIPT_DIR = "/home/gamma/airflow/pipeline/ts_map"

def contact_simulator_task(**context):
    """
    Task to simulate satellite contact and prepare data folder
    """
    import os
    import shutil
    from datetime import datetime
    
    # Create timestamp for folder name
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    #source_folder = f"/home/gamma/workspace/data/tsmap/{timestamp}"
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
    
    # Store the data folder path in XCom for other tasks to use
    context['task_instance'].xcom_push(key='data_folder', value=new_folder)
    return new_folder

def get_data_folder(**context):
    """
    Helper function to retrieve data folder from XCom
    """
    return context['task_instance'].xcom_pull(key='data_folder')

# Task 1: Contact Simulator
contact_simulator = PythonOperator(
    task_id='1_contact_simulator',
    python_callable=contact_simulator_task,
    dag=dag,
)

# Task 2: Bin GRB Data Source
bin_grb_data = BashOperator(
    task_id='2_bin_grb_data',
    bash_command="""
    source activate cosipy
    cd {{ ti.xcom_pull(key='data_folder') }}
    python {{ params.script_dir }}/2_binGRBdatasource.py {{ ti.xcom_pull(key='data_folder') }}
    """,
    params={'script_dir': SCRIPT_DIR},
    dag=dag,
)

# Task 3: Bin Background Data
bin_background = BashOperator(
    task_id='3_bin_background',
    bash_command="""
    source activate cosipy
    cd {{ ti.xcom_pull(key='data_folder') }}
    python {{ params.script_dir }}/3_binBackground.py {{ ti.xcom_pull(key='data_folder') }}
    """,
    params={'script_dir': SCRIPT_DIR},
    dag=dag,
)

# Task 4: Data Aggregation
data_aggregation = BashOperator(
    task_id='4_data_aggregation',
    bash_command="""
    source activate cosipy
    cd {{ ti.xcom_pull(key='data_folder') }}
    python {{ params.script_dir }}/4_dataAggregation.py {{ ti.xcom_pull(key='data_folder') }}
    """,
    params={'script_dir': SCRIPT_DIR},
    dag=dag,
)

# Task 5: TS Map Computation
ts_map_mulres_computation = BashOperator(
    task_id='5_ts_map_mulres_computation',
    bash_command="""
    source activate cosipy
    cd {{ ti.xcom_pull(key='data_folder') }}
    python {{ params.script_dir }}/5_tsmapmulres_computation.py {{ ti.xcom_pull(key='data_folder') }}
    """,
    params={'script_dir': SCRIPT_DIR},
    dag=dag,
)

# Task 6: Cleanup (Optional)
# TODO: Add cleanup task
cleanup = BashOperator(
    task_id='6_cleanup',
    bash_command="""
    echo "Pipeline completed successfully!"
    echo "Data folder: {{ ti.xcom_pull(key='data_folder') }}"
    echo "Check the plots directory for TS map visualizations"
    """,
    dag=dag,
)

# Define task dependencies
contact_simulator >> [bin_grb_data, bin_background]
[bin_grb_data, bin_background] >> data_aggregation
data_aggregation >> ts_map_mulres_computation
ts_map_mulres_computation >> cleanup
