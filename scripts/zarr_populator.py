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
import xarray as xr
import numpy as np
import zarr
import os

# Parse in key and bucket name from config file
cfp = ConfigParser()
cfp.read("config.ini")

service_account_key_json = cfp["GCS"]["gcs_storage_key"]


bucket_name = "YOUR BUCKET NAME HERE"

rel_zipzarr_path = "version1/spot_images.zip"


# Define GCS file system so files can be read
gcs = GCSFileSystem(token=service_account_key_json)


client = storage.Client.from_service_account_json(service_account_key_json)



slide_files_raw = list_blobs_with_prefix(
    client, bucket_name, prefix="patient_slides_analysis")["blobs"]

# select a couple of slide

slide_npys_of_interest = [
    slidefile
    for slidefile in slide_files_raw
    if slidefile.endswith(".npy")
]

for npypath in slide_npys_of_interest:
    slide_name =  npypath.split("/")[-1].strip(".npy")

    slide_dir_path = '/home/prakashlab/Desktop/slide_zarrs/'+slide_name+'/'
    os.mkdir(slide_dir_path)
    print(slide_name)
    gcs.get(bucket_name.strip("/")+"/"+npypath, slide_dir_path+"test.npy")
    data = np.load(slide_dir_path+"test.npy")
    data = xr.DataArray(data, dims=['t','c','y','x'])
    data = data.expand_dims('z')
    data = data.transpose('t','c','z','y','x')

    print(data)

    y_dim = data.shape[data.dims.index('y')]
    x_dim = data.shape[data.dims.index('x')]

    ds = xr.Dataset({'spot_images':data})
    ds.spot_images.encoding = {'chunks': (1,1,1,y_dim,x_dim)}


    with zarr.ZipStore(slide_dir_path+'spot_images.zip', mode='w') as store:
        ds.to_zarr(store, mode='w')
    print("uploading zarr")
    
    zarr_upload_path = bucket_name+"/"+slide_name+"/"+rel_zipzarr_path
    try:
        gcs.upload(slide_dir_path+'spot_images.zip',zarr_upload_path)
    except Exception as e:
        print(e)
