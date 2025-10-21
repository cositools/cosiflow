
from cosipy.util import fetch_wasabi_file
from pathlib import Path
import subprocess
import zipfile
import tarfile
from datetime import datetime

# === Setup download paths ===
data_folder = Path("/home/gamma/workspace/data/raw")
data_folder.mkdir(exist_ok=True)

wasabi = {
    # NOTE: The original background file is extremely large, and the complete data directory can reach about 52 GB.
    # To avoid heavy storage usage and slowdowns for experiments, we trimmed the background around the source window,
    # producing a much smaller background file (~1MB) for use here.
    #
    # "background":  "Backgrounds/Ge/Total_BG_with_SAAcomponent_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
    "response":    "Responses/ResponseContinuum.o3.e100_10000.b10log.s10396905069491.m2284.filtered.nonsparse.binnedimaging.imagingresponse_nside8.area.good_chunks.h5.zip",
    "orientation": "Orientation/DC3_final_530km_3_month_with_slew_1sbins_GalacticEarth_SAA.ori",
    "source":      "Sources/GRB_bn081207680_3months_unbinned_data_filtered_with_SAAcut.fits.gz",
}

paths = {key: data_folder / Path(val).name for key, val in wasabi.items()}

# === Download and prepare files ===
for key, remote in wasabi.items():
    out = paths[key]
    ready = out.with_suffix('') if out.suffix == ".gz" else out
    if not ready.exists():
        print(f"Downloading {key} from Wasabi...")
        fetch_wasabi_file(f"COSI-SMEX/DC3/Data/{remote}", output=out)
        if out.suffix == ".gz":
            subprocess.run(["gunzip", "-f", str(out)], check=True)
        elif out.suffix == ".zip":
            print(f"Unzipping {out.name}...")
            with zipfile.ZipFile(out, 'r') as zip_ref:
                zip_ref.extractall(data_folder)
            print("Unzip complete")
    else:
        print(f"[Ready] {key}")

# === Compress all files into a tar.gz archive ===
print("\nCreating compressed archive...")
archive_name = f"raw.tar.gz"
archive_path = data_folder / archive_name

with tarfile.open(archive_path, "w:gz") as tar:
    for file_path in data_folder.iterdir():
        if file_path.is_file() and not file_path.name.endswith(".zip") and not file_path.name.endswith(".gz"):
            tar.add(file_path, arcname=file_path.name)
            print(f"Added {file_path.name} to archive")

print(f"Archive created: {archive_path}")
print(f"Archive size: {archive_path.stat().st_size / (1024*1024):.2f} MB")

