# nautilus-dashboard
Dashboard for displaying and inspecting data from slides

## Dependencies
```
pip install polars gcsfs google-auth google-auth-httplib2 google-auth-oauthlib dash
```

## Setup
Create a `config.ini` file in the top directory of this repo with the following content:
```
[GCS]
gcs_storage_key=/path/to/your/gcs/key.json
bucket_url=gs://bucket-name
```

Modify `config.ini` such that `gcs_storage_key` points to your json GCS key, and `bucket_name` is `gs://` address of the relevant bucket.

# Run #
```
python ./app.py
```