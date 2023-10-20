from google.cloud import storage
from configparser import ConfigParser
from google.oauth2 import service_account
from googleapiclient.discovery import build
from utils.demo_io import (
    get_initial_slide_df,
    get_fovs_df,
    get_top_level_dirs,
    populate_slide_rows,
    get_histogram_df,
)
import polars as pl
from gcsfs import GCSFileSystem

# Parse in key and bucket name from config file
cfp = ConfigParser()
cfp.read("config.ini")

service_account_key_json = cfp["GCS"]["gcs_storage_key"]
gs_url = cfp["GCS"]["bucket_url"]

bucket_name = gs_url.replace("gs://", "")


# Define GCS file system so files can be read
gcs = GCSFileSystem(token=service_account_key_json)

# Authenticate using the service account key file
credentials = service_account.Credentials.from_service_account_file(
    service_account_key_json, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Create a storage client
storage_service = build("storage", "v1", credentials=credentials)

# Get an initial, mostly-unpopulated slide dataframe
slide_df = get_initial_slide_df(storage_service, bucket_name, gcs)

print(slide_df)

# select a couple of slides
slides_of_interest = get_top_level_dirs(storage_service, bucket_name)

# repopulate rows on some slides with spot counts missing, and set threshold
new_slide_df = populate_slide_rows(
    storage_service,
    bucket_name,
    gcs,
    slide_df,
    slides_of_interest[:4],
    set_threshold=0.8,
)

print(new_slide_df)

# get DF for these slides' FOVs
fov_df = get_fovs_df(storage_service, bucket_name, slides_of_interest)
print(fov_df)
