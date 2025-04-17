from cosipy.util import fetch_wasabi_file
import os
import shutil
from pathlib import Path

# This script must be executed the first time we install this airflow app to obtain a file used to test the DAG

home_dir = Path(os.environ['HOME'])
new_path = os.path.join(home_dir, "workspace", "data", "GalacticScan.inc1.id1.crab2hr.extracted.tra.gz")

# Check if the file already exists
if os.path.exists(new_path):
    print(f"File {new_path} already exists. Removing it to fetch a new one.")
    # If the file exists, remove it
    os.remove(new_path)

fetch_wasabi_file(file='ComptonSphere/mini-DC2/GalacticScan.inc1.id1.crab2hr.extracted.tra.gz',
                  output=new_path)
