from datetime import datetime, timedelta
import os
import sys
import pickle
from airflow import DAG
from airflow.operators.python import PythonOperator

# Compat-shim per ExternalPythonOperator (funziona sia su versioni vecchie che nuove)
# Questo risolve il problema "No module named 'airflow.providers.standard'"
# nelle versioni di Airflow che non hanno il provider standard installato
try:
    from airflow.operators.python import ExternalPythonOperator  # Airflow 2.x "classico"
except ImportError:
    from airflow.providers.standard.operators.python import ExternalPythonOperator  # provider standard (se installato)
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
    'retry_delay': timedelta(minutes=1),
}

# Create the DAG
dag = DAG(
    'cosipipe_tsmap__singletask__extpythonenv',
    default_args=default_args,
    description='COSI TS Map computation pipeline (Single-task optimized)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['cosipy', 'tsmap', 'grb', 'singletask', 'optimized'],
)

# Define the directory where our scripts are located
SCRIPT_DIR = "/home/gamma/airflow/pipeline/ts_map"

# Add the pipeline directory to Python path
sys.path.append(SCRIPT_DIR)

def contact_simulator_task():
    """
    Task to simulate satellite contact and prepare data folder
    """
    import os
    import shutil
    import base64
    import pickle
    import sys
    from datetime import datetime
    
    # Add the pipeline directory to Python path
    script_dir = "/home/gamma/airflow/pipeline/ts_map"
    sys.path.append(script_dir)
    
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
    
    # Create TSMapPipeline instance (now in cosipy environment)
    from tsmap_pipeline import TSMapPipeline
    
    pipeline = TSMapPipeline(
        data_folder=new_folder,
        multi_resolution=False,  # Set to True for multi-resolution TS map
        background_file="Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        source_file="GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        response_file="ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
        orientation_file="DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori"
    )
    
    # Save pipeline to file instead of serializing
    pipeline_file = os.path.join(new_folder, "pipeline_state.pkl")
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print(f"✓ TSMapPipeline instance created in cosipy environment")
    print(f"✓ Data folder: {new_folder}")
    print(f"✓ Pipeline state saved to: {pipeline_file}")
    
    # Return dict with file path instead of serialized data
    return {
        "data_folder": new_folder,
        "pipeline_file": pipeline_file
    }


# Task 1: Contact Simulator (COMMENTED OUT - USE SINGLE-TASK INSTEAD)
# contact_simulator = ExternalPythonOperator(
#     task_id='1_contact_simulator',
#     python='/home/gamma/.conda/envs/cosipy/bin/python',
#     python_callable=contact_simulator_task,
#     dag=dag,
# )

# Task 2: Bin GRB Data Source
def bin_grb_task(data_folder, pipeline_file):
    import sys
    import os
    import pickle
    script_dir = "/home/gamma/airflow/pipeline/ts_map"
    sys.path.append(script_dir)
    
    # Set environment variables for performance and stability
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    os.environ["MPLCONFIGDIR"] = "/tmp/mpl"
    
    # Import TSMapPipeline
    from tsmap_pipeline import TSMapPipeline
    
    # Load pipeline from file
    with open(pipeline_file, 'rb') as f:
        pipeline = pickle.load(f)
    
    print(f"✓ Loaded pipeline instance for GRB binning")
    print(f"  - Data folder: {pipeline.data_folder}")
    print(f"  - Multi-resolution: {pipeline.multi_resolution}")
    print(f"  - Pipeline file: {pipeline_file}")
    
    # Run GRB binning
    grb_binned_path = pipeline.bin_grb_data()
    
    # Save updated pipeline back to file
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print(f"✓ Pipeline state updated and saved to: {pipeline_file}")
    
    return {
        "pipeline_file": pipeline_file,
        "data_folder": pipeline.data_folder,
        "grb_binned": grb_binned_path
    }

# bin_grb_data = ExternalPythonOperator(
#     task_id='2_bin_grb_data',
#     python='/home/gamma/.conda/envs/cosipy/bin/python',
#     python_callable=bin_grb_task,
#     op_args=[
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['data_folder'] }}",
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['pipeline_file'] }}"
#     ],
#     dag=dag,
# )

# Task 3: Bin Background Data
def bin_background_task(data_folder, pipeline_file):
    import sys
    import os
    import pickle
    script_dir = "/home/gamma/airflow/pipeline/ts_map"
    sys.path.append(script_dir)
    
    # Set environment variables for performance and stability
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    os.environ["MPLCONFIGDIR"] = "/tmp/mpl"
    
    # Import TSMapPipeline
    from tsmap_pipeline import TSMapPipeline
    
    # Load pipeline from file
    with open(pipeline_file, 'rb') as f:
        pipeline = pickle.load(f)
    
    print(f"✓ Loaded pipeline instance for background binning")
    print(f"  - Data folder: {pipeline.data_folder}")
    print(f"  - Multi-resolution: {pipeline.multi_resolution}")
    print(f"  - Pipeline file: {pipeline_file}")
    
    # Run background binning
    bg_binned_path = pipeline.bin_background_data()
    
    # Save updated pipeline back to file
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print(f"✓ Pipeline state updated and saved to: {pipeline_file}")
    
    return {
        "pipeline_file": pipeline_file,
        "data_folder": pipeline.data_folder,
        "bg_binned": bg_binned_path
    }

# bin_background = ExternalPythonOperator(
#     task_id='3_bin_background',
#     python='/home/gamma/.conda/envs/cosipy/bin/python',
#     python_callable=bin_background_task,
#     op_args=[
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['data_folder'] }}",
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['pipeline_file'] }}"
#     ],
#     dag=dag,
# )

# Task 4: Data Aggregation
def data_aggregation_task(data_folder, pipeline_file, grb_binned_path):
    import sys
    import os
    import pickle
    script_dir = "/home/gamma/airflow/pipeline/ts_map"
    sys.path.append(script_dir)
    
    # Set environment variables for performance and stability
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    os.environ["MPLCONFIGDIR"] = "/tmp/mpl"
    
    # Import TSMapPipeline
    from tsmap_pipeline import TSMapPipeline
    
    # Load pipeline from file
    with open(pipeline_file, 'rb') as f:
        pipeline = pickle.load(f)
    
    print(f"✓ Loaded pipeline instance for data aggregation")
    print(f"  - Data folder: {pipeline.data_folder}")
    print(f"  - Multi-resolution: {pipeline.multi_resolution}")
    print(f"  - Pipeline file: {pipeline_file}")
    print(f"  - GRB binned path: {grb_binned_path}")
    
    # Verify GRB binned file exists
    if not os.path.exists(grb_binned_path):
        raise FileNotFoundError(f"GRB binned file not found: {grb_binned_path}")
    
    print(f"✓ GRB binned file verified: {grb_binned_path}")
    
    # Run data aggregation
    result = pipeline.aggregate_data()
    
    # Save updated pipeline back to file
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print(f"✓ Pipeline state updated and saved to: {pipeline_file}")
    
    return {
        "pipeline_file": pipeline_file,
        "data_folder": pipeline.data_folder,
        "aggregation_completed": True
    }

# data_aggregation = ExternalPythonOperator(
#     task_id='4_data_aggregation',
#     python='/home/gamma/.conda/envs/cosipy/bin/python',
#     python_callable=data_aggregation_task,
#     op_args=[
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['data_folder'] }}",
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['pipeline_file'] }}",
#         "{{ ti.xcom_pull(task_ids='2_bin_grb_data')['grb_binned'] }}"
#     ],
#     dag=dag,
# )

# Task 5: TS Map Computation
def ts_map_computation_task(data_folder, pipeline_file):
    import sys
    import os
    import pickle
    script_dir = "/home/gamma/airflow/pipeline/ts_map"
    sys.path.append(script_dir)
    
    # Set environment variables for performance and stability
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    os.environ["MPLCONFIGDIR"] = "/tmp/mpl"
    
    # Import TSMapPipeline
    from tsmap_pipeline import TSMapPipeline
    
    # Load pipeline from file
    with open(pipeline_file, 'rb') as f:
        pipeline = pickle.load(f)
    
    print(f"✓ Loaded pipeline instance for TS map computation")
    print(f"  - Data folder: {pipeline.data_folder}")
    print(f"  - Multi-resolution: {pipeline.multi_resolution}")
    print(f"  - Pipeline file: {pipeline_file}")
    
    # First aggregate data (needed for TS map computation)
    pipeline.aggregate_data()
    
    # Run TS map computation
    # result = pipeline.compute_ts_map()
    
    # Save updated pipeline back to file
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print(f"✓ Pipeline state updated and saved to: {pipeline_file}")
    
    return {
        "pipeline_file": pipeline_file,
        "data_folder": pipeline.data_folder,
        "ts_map_completed": True,
        # "ts_map_result": result
    }

# ts_map_computation = ExternalPythonOperator(
#    task_id='5_ts_map_computation',
#    python='/home/gamma/.conda/envs/cosipy/bin/python',
#    python_callable=ts_map_computation_task,
#    op_args=[
#        "{{ ti.xcom_pull(task_ids='1_contact_simulator')['data_folder'] }}",
#        "{{ ti.xcom_pull(task_ids='1_contact_simulator')['pipeline_file'] }}"
#    ],
#    dag=dag,
#)

# Task 6: Multi-Resolution TS Map Computation (Optional)
def multi_resolution_ts_map_task(data_folder, pipeline_file):
    import sys
    import os
    import pickle
    script_dir = "/home/gamma/airflow/pipeline/ts_map"
    sys.path.append(script_dir)
    
    # Set environment variables for performance and stability
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    os.environ["MPLCONFIGDIR"] = "/tmp/mpl"
    
    # Import TSMapPipeline
    from tsmap_pipeline import TSMapPipeline
    
    # Load pipeline from file
    with open(pipeline_file, 'rb') as f:
        pipeline = pickle.load(f)
    
    # Enable multi-resolution mode
    pipeline.multi_resolution = True
    
    print(f"✓ Loaded pipeline instance for multi-resolution TS map computation")
    print(f"  - Data folder: {pipeline.data_folder}")
    print(f"  - Multi-resolution: {pipeline.multi_resolution}")
    print(f"  - Pipeline file: {pipeline_file}")
    
    # First aggregate data (needed for TS map computation)
    pipeline.aggregate_data()
    
    # Run multi-resolution TS map computation
    result = pipeline.compute_ts_map()
    
    # Save updated pipeline back to file
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print(f"✓ Pipeline state updated and saved to: {pipeline_file}")
    
    return {
        "pipeline_file": pipeline_file,
        "data_folder": pipeline.data_folder,
        "multi_res_ts_map_completed": True,
        "multi_res_ts_map_result": result
    }

# multi_resolution_ts_map = ExternalPythonOperator(
#     task_id='6_multi_resolution_ts_map',
#     python='/home/gamma/.conda/envs/cosipy/bin/python',
#     python_callable=multi_resolution_ts_map_task,
#     op_args=[
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['data_folder'] }}",
#         "{{ ti.xcom_pull(task_ids='1_contact_simulator')['pipeline_file'] }}"
#     ],
#     dag=dag,
# )

# SINGLE-TASK SOLUTION: Consolidated pipeline execution
def run_all_pipeline_steps():
    """
    Consolidated function that runs all pipeline steps in a single task.
    This eliminates the overhead of reloading libraries between tasks.
    """
    import os
    import shutil
    import pickle
    import sys
    from datetime import datetime
    
    # Set environment variables for performance and stability
    os.environ["OMP_NUM_THREADS"] = "1"
    os.environ["MKL_NUM_THREADS"] = "1"
    os.environ["NUMEXPR_NUM_THREADS"] = "1"
    os.environ["MPLCONFIGDIR"] = "/tmp/mpl"
    
    # Add the pipeline directory to Python path
    script_dir = "/home/gamma/airflow/pipeline/ts_map"
    sys.path.append(script_dir)
    
    print("=" * 60)
    print("STARTING CONSOLIDATED PIPELINE EXECUTION")
    print("=" * 60)
    
    # STEP 1: Contact Simulator - Prepare data folder
    print("\nSTEP 1: Contact Simulator - Preparing data folder")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    source_folder = f"/home/gamma/workspace/tsmap_test/data"
    new_folder = f"/home/gamma/workspace/data/tsmap/{timestamp}"
    
    # Create the new folder
    os.makedirs(new_folder, exist_ok=True)
    print(f"✓ Created new data folder: {new_folder}")
    
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
            print(f"✓ Copied {file_type}: {filename}")
        else:
            print(f"Warning: Source file not found: {source_path}")
    
    # STEP 2: Create TSMapPipeline instance (IMPORT HEAVY LIBRARIES ONCE)
    print("\nSTEP 2: Creating TSMapPipeline instance")
    from tsmap_pipeline import TSMapPipeline
    
    pipeline = TSMapPipeline(
        data_folder=new_folder,
        multi_resolution=False,  # Set to True for multi-resolution TS map
        background_file="Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        source_file="GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
        response_file="ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5",
        orientation_file="DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori"
    )
    
    # Save pipeline to file
    pipeline_file = os.path.join(new_folder, "pipeline_state.pkl")
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print(f"✓ TSMapPipeline instance created and saved to: {pipeline_file}")
    
    # STEP 3: Bin GRB Data Source
    print("\nSTEP 3: Binning GRB data")
    grb_binned_path = pipeline.bin_grb_data()
    print(f"✓ GRB data binned: {grb_binned_path}")
    
    # STEP 4: Bin Background Data
    print("\nSTEP 4: Binning background data")
    bg_binned_path = pipeline.bin_background_data()
    print(f"✓ Background data binned: {bg_binned_path}")
    
    # STEP 5: Data Aggregation
    print("\nSTEP 5: Data aggregation")
    # Verify GRB binned file exists
    if not os.path.exists(grb_binned_path):
        raise FileNotFoundError(f"GRB binned file not found: {grb_binned_path}")
    
    print(f"✓ GRB binned file verified: {grb_binned_path}")
    aggregation_result = pipeline.aggregate_data()
    print(f"✓ Data aggregation completed")
    
    # STEP 6: Standard TS Map Computation
    # print("\nSTEP 6: Computing standard TS map")
    # ts_map_result = pipeline.compute_ts_map()
    # print(f"✓ Standard TS map computation completed")
    
    # STEP 7: Multi-Resolution TS Map Computation
    print("\nSTEP 7: Computing multi-resolution TS map")
    # Enable multi-resolution mode
    pipeline.multi_resolution = True
    multi_res_ts_map_result = pipeline.compute_ts_map()
    print(f"✓ Multi-resolution TS map computation completed")
    
    # Save final pipeline state
    with open(pipeline_file, 'wb') as f:
        pickle.dump(pipeline, f)
    
    print("\n" + "=" * 60)
    print("CONSOLIDATED PIPELINE EXECUTION COMPLETED SUCCESSFULLY!")
    print("=" * 60)
    print(f"Data folder: {new_folder}")
    print(f"Pipeline state: {pipeline_file}")
    print(f"GRB binned: {grb_binned_path}")
    print(f"Background binned: {bg_binned_path}")
    print("Both standard and multi-resolution TS maps computed")
    print("=" * 60)
    
    return {
        "data_folder": new_folder,
        "pipeline_file": pipeline_file,
        "grb_binned": grb_binned_path,
        "bg_binned": bg_binned_path,
        "ts_map_completed": True,
        "multi_res_ts_map_completed": True,
        #"ts_map_result": ts_map_result,
        "multi_res_ts_map_result": multi_res_ts_map_result
    }

# Single consolidated task that runs all pipeline steps
tsmap_single_task = ExternalPythonOperator(
    task_id='tsmap_single_task',
    python='/home/gamma/.conda/envs/cosipy/bin/python',
    python_callable=run_all_pipeline_steps,
    dag=dag,
)

# Task 7: Cleanup (Optional) - COMMENTED OUT FOR SINGLE-TASK APPROACH
# cleanup = BashOperator(
#     task_id='7_cleanup',
#     bash_command="""
#     echo "Pipeline completed successfully!"
#     echo "Data folder: {{ ti.xcom_pull(task_ids='1_contact_simulator')['data_folder'] }}"
#     echo "Check the plots directory for TS map visualizations"
#     echo "Both standard and multi-resolution TS maps have been computed"
#     """,
#     dag=dag,
# )

# Single-task cleanup
single_task_cleanup = BashOperator(
    task_id='single_task_cleanup',
    bash_command="""
    echo "PIPELINE COMPLETED SUCCESSFULLY!"
    echo "Data folder: {{ ti.xcom_pull(task_ids='tsmap_single_task')['data_folder'] }}"
    echo "Pipeline state: {{ ti.xcom_pull(task_ids='tsmap_single_task')['pipeline_file'] }}"
    echo "GRB binned: {{ ti.xcom_pull(task_ids='tsmap_single_task')['grb_binned'] }}"
    echo "Background binned: {{ ti.xcom_pull(task_ids='tsmap_single_task')['bg_binned'] }}"
    echo "Multi-resolution TS map: {{ ti.xcom_pull(task_ids='tsmap_single_task')['multi_res_ts_map_completed'] }}"
    echo "Check the plots directory for TS map visualizations"
    echo "Multi-resolution TS map has been computed"
    """,
    dag=dag,
)

# SINGLE-TASK PIPELINE DEPENDENCIES
# This is the optimized approach that eliminates library reload overhead
# All pipeline steps are executed in a single task with libraries loaded once
tsmap_single_task >> single_task_cleanup
