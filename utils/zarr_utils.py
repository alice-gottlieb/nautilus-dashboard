# Mostly adapted from octopi-ml repo

from threading import Lock, RLock
import zipfile
import zarr

import random

import numpy as np
from PIL import Image
from io import BytesIO
import base64

class RemoteZipStore(zarr.ZipStore):
    def __init__(self, path, compression=zipfile.ZIP_STORED, allowZip64=True, mode='a',dimension_separator=None):
        # store properties
        self.path = None # TODO: This need to be handled properly for os.PathLike or file-like object
        self.compression = compression
        self.allowZip64 = allowZip64
        self.mode = mode
        self._dimension_separator = dimension_separator

        # Current understanding is that zipfile module in stdlib is not thread-safe,
        # and so locking is required for both read and write. However, this has not
        # been investigated in detail, perhaps no lock is needed if mode='r'.
        self.mutex = RLock()
        # open zip file
        self.zf = zipfile.ZipFile(path, mode=mode, compression=compression,allowZip64=allowZip64)


def generate_signed_url_v4(bucket, blob_name, method="GET", expiration=900, context=None):
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
def parse_slide(gcs,slide_img_url):
    file = gcs.open(slide_img_url, mode="rb")
    store = RemoteZipStore(file, mode='r')
    source = zarr.open(store=store, mode="r")
    print(source.tree())
    spot_images = source['/spot_images']
    print(spot_images)
    return spot_images

def get_image_from_zarr(spot_images, sample_id):
    array = spot_images[sample_id, :, 0, :, :]
    dapi = np.stack([array[2, :,:], array[1, :,:], array[0, :,:]], axis=2)
    bf = array[3, :, :]
    compose = (0.4 * np.stack([bf,]*3, axis=2) + 0.6*dapi).astype('uint8')
    return sample_id, bf, dapi, compose

def encode_image(image):
    image = Image.fromarray(image)
    buffered = BytesIO()
    image.save(buffered,format="PNG")
    img_str = 'data:image/png;base64,' + base64.b64encode(buffered.getvalue()).decode('ascii')
    return img_str
