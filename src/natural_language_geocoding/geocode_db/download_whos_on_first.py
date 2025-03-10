import tarfile
from pathlib import Path

import requests

url = "https://data.geocode.earth/wof/dist/legacy/whosonfirst-data-country-latest.tar.bz2"

temp_dir = Path("temp")
temp_dir.mkdir(exist_ok=True)

filename = url.split("/")[-1]
dirname = filename.split(".")[0]

tar_file = temp_dir / filename
extract_dir = temp_dir / dirname

# Download file
response = requests.get(url, stream=True, timeout=10)
with tar_file.open("wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

# Extract archive
extract_dir.mkdir(parents=True)
with tarfile.open(tar_file, "r:bz2") as tar:
    tar.extractall(path=extract_dir, filter="data")

# Clean up archive file
tar_file.unlink()
