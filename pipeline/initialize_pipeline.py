from cosipy.util import fetch_wasabi_file
import os
import shutil
from pathlib import Path

# this script must be executed the first time we install this airflow app to obtain a file used to test the DAG

home_dir = Path(os.environ['HOME'])

fetch_wasabi_file('ComptonSphere/mini-DC2/GalacticScan.inc1.id1.crab2hr.extracted.tra.gz')

new_path = home_dir / "workspace" / "data" / "GalacticScan.inc1.id1.crab2hr.extracted.tra.gz"

shutil.move("GalacticScan.inc1.id1.crab2hr.extracted.tra.gz", new_path)