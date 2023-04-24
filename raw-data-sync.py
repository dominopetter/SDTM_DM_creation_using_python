from art import *
from domino.data_sources import DataSourceClient
import os

# create a client and fetch the data source instance
client = DataSourceClient()
data_source = client.get_datasource("clinical_data_repository")

# specify the local directory to download files to
local_dir = "/mnt/data/test"

# Create header showing we are downloading
tprint("RAW DATA DL")

# download each file to the local directory
for obj in data_source.list_objects():
    key = obj.key
    print(key)
    local_path = os.path.join(local_dir, key)
    full_local_path = os.path.dirname(local_path)
    if not os.path.isdir(full_local_path):
        print(f"{full_local_path} directory doesn't exist... Creating it...")
        os.makedirs(full_local_path)
    print(f"Attempting to download {key} to {local_path}")
    data_source.download_file(key, local_path)