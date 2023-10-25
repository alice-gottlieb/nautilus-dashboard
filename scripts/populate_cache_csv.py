from google.cloud import storage
from configparser import ConfigParser
from google.oauth2 import service_account
from googleapiclient.discovery import build
from utils.demo_io import (
    get_initial_slide_df_with_predictions_only,
    get_fovs_df,
    get_top_level_dirs,
    populate_slide_rows,
    get_histogram_df,
    list_blobs_with_prefix,
    get_spots_csv,
)
from utils.polars_helpers import get_results_from_threshold
import polars as pl
from gcsfs import GCSFileSystem


default_threshold = 0.876  # threshold given in rinni's code


csv_write_path = "slide_df_cache/slides.csv"

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

client = storage.Client.from_service_account_json(service_account_key_json)

# Create a storage client
storage_service = build("storage", "v1", credentials=credentials)

# Get an initial, mostly-unpopulated slide dataframe
slide_df = get_initial_slide_df_with_predictions_only(client, bucket_name, gcs)

print(slide_df)

slide_list = slide_df["slide_name"].to_list()

slide_df = populate_slide_rows(
    client, bucket_name, gcs, slide_df, slide_list, set_threshold=default_threshold
)

print(slide_df)

slide_df.write_csv(csv_write_path)
