# Mostly adapted from octopi-ml repo

from threading import Lock, RLock
import zipfile
import zarr

import random

import numpy as np
from PIL import Image
from io import BytesIO
import base64
import asyncio

class RemoteZipStore(zarr.ZipStore):
    def __init__(
        self,
        path,
        compression=zipfile.ZIP_STORED,
        allowZip64=True,
        mode="a",
        dimension_separator=None,
    ):
        # store properties
        self.path = None  # TODO: This need to be handled properly for os.PathLike or file-like object
        self.compression = compression
        self.allowZip64 = allowZip64
        self.mode = mode
        self._dimension_separator = dimension_separator

        # Current understanding is that zipfile module in stdlib is not thread-safe,
        # and so locking is required for both read and write. However, this has not
        # been investigated in detail, perhaps no lock is needed if mode='r'.
        self.mutex = RLock()
        # open zip file
        self.zf = zipfile.ZipFile(
            path, mode=mode, compression=compression, allowZip64=allowZip64
        )


def generate_signed_url_v4(
    bucket, blob_name, method="GET", expiration=900, context=None
):
    """Generates a v4 signed URL for downloading a blob."""
    # decode url
    if "%" in blob_name:
        blob_name = urllib.parse.unquote(blob_name)
    blob = bucket.blob(blob_name)
    assert method in ["GET", "POST", "PUT"], "Only GET, PUT or POST methods are allowed"

    url = blob.generate_signed_url(
        version="v4",
        expiration=datetime.timedelta(seconds=expiration),
        method=method,
    )
    return url


### Replacing HTTPFile method with gcs.open from a bucket url
def parse_slide(gcs, slide_img_url):
    file = gcs.open(slide_img_url, mode="rb")
    store = RemoteZipStore(file, mode="r")
    source = zarr.open(store=store, mode="r")
    spot_images = source["/spot_images"]
    return spot_images

async def _get_image_from_zarr(spot_images, sample_id, img_list, img_index):
    img_list[img_index]= get_image_from_zarr(spot_images,sample_id)

async def get_images_from_zarr_async(spot_images, sample_id_list):
    ret_list = [None for i in range(len(sample_id_list))]
    tasklist = []
    for img_index, sample_id in zip(range(len(sample_id_list)),sample_id_list):
        tasklist.append(asyncio.create_task(_get_image_from_zarr(spot_images,sample_id,ret_list,img_index)))
    for task in tasklist:
        await task
    return ret_list

def get_images_from_zarr_async_wrapper(spot_images,sample_id_list):
    return asyncio.run(get_images_from_zarr_async(spot_images,sample_id_list))

def get_images_from_zarr_built_in(spot_images, sample_id_list):
    sel_indices = [sample_id_list]
    for i in range(len(spot_images.shape)-1):
        sel_indices.append(slice(None))
    sel_indices = tuple(sel_indices)
    spot_samples = spot_images.get_orthogonal_selection(sel_indices)
    ret_images = []
    for sample_id,i in zip(sample_id_list,range(spot_samples.shape[0])):
        array = spot_samples[i,:,0,:,:]
        dapi = np.stack([array[2, :, :], array[1, :, :], array[0, :, :]], axis=2)
        bf = array[3, :, :]
        compose = (0.4* np.stack([bf]* 3,axis=2)+ 0.6 * dapi).astype("uint8")
        ret_images.append({"spot_id": sample_id, "bf": encode_image(bf), "dapi": encode_image(dapi), "compose": encode_image(compose)})
    return ret_images

def get_image_from_zarr(spot_images, sample_id):
    """
    :brief: This is highly dependent on our current way of stacking
    spot image fields. May need to change the way this dict is returned
    in the future.
    """
    array = spot_images[sample_id, :, 0, :, :]
    dapi = np.stack([array[2, :, :], array[1, :, :], array[0, :, :]], axis=2)
    bf = array[3, :, :]
    compose = (
        0.4
        * np.stack(
            [
                bf,
            ]
            * 3,
            axis=2,
        )
        + 0.6 * dapi
    ).astype("uint8")
    return {"spot_id": sample_id, "bf": bf, "dapi": dapi, "compose": compose}


def encode_image(image):
    image = Image.fromarray(image)
    buffered = BytesIO()
    image.save(buffered, format="PNG")
    img_str = "data:image/png;base64," + base64.b64encode(buffered.getvalue()).decode(
        "ascii"
    )
    return img_str
