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
    get_combined_spots_df,
    crop_spots_from_slide,
)
import polars as pl
from gcsfs import GCSFileSystem

cutoff = 10  # how many slides to view cropped spot images from

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


slide_files_raw = list_blobs_with_prefix(
    client, bucket_name, prefix="patient_slides_analysis", cutoff=cutoff * 2
)["blobs"]


slides_of_interest = [
    slidefile.split("/")[-1].strip(".npy")
    for slidefile in slide_files_raw
    if slidefile.endswith(".npy")
]


for sl in slides_of_interest:
    spot_df = get_combined_spots_df(bucket_name, gcs, sl)

    print(spot_df)

    spot_df_top = spot_df.sort(pl.col("parasite output"), descending=True).head(20)

    spot_df_top = spot_df_top.with_columns(spot_df_top["r"].cast(pl.Int64) * 2)
    spot_coords = []

    for spot in spot_df_top.rows(named=True):
        spot_coords.append(
            (
                spot["FOV_row"],
                spot["FOV_col"],
                spot["FOV_z"],
                spot["x"],
                spot["y"],
                spot["r"],
            )
        )

    print(spot_df_top)

    spot_imgs = crop_spots_from_slide(storage_service, bucket_name, sl, spot_coords)

    for img in spot_imgs:
        img.show()
