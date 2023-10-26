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
)
import polars as pl
from gcsfs import GCSFileSystem
from PIL import Image
import asyncio

from utils.zarr_utils import parse_slide, get_image_from_zarr

# Parse in key and bucket name from config file
cfp = ConfigParser()
cfp.read("config.ini")

service_account_key_json = cfp["GCS"]["gcs_storage_key"]
gs_url = cfp["GCS"]["bucket_url"]

bucket_name = gs_url.replace("gs://", "")

bucket2_name = "octopi-malaria-data-processing"

zipzarr_url = "octopi-malaria-data-processing/072622-D1-3_2022-07-26_17-50-42.852998/version1/spot_images.zip"


# Define GCS file system so files can be read
gcs = GCSFileSystem(token=service_account_key_json)


# Authenticate using the service account key file
credentials = service_account.Credentials.from_service_account_file(
    service_account_key_json, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

client = storage.Client.from_service_account_json(service_account_key_json)

# Create a storage client
storage_service = build("storage", "v1", credentials=credentials)

spot_img_zarr = parse_slide(gcs, zipzarr_url)

for i in range(25):
    spot_img = Image.fromarray(get_image_from_zarr(spot_img_zarr, 240 + i)["compose"])
    spot_img.show()
