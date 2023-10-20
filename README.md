# nautilus-dashboard
Dashboard for displaying and inspecting data from slides

## Dependencies
```
pip install polars gcsfs google-auth google-auth-httplib2 google-auth-oauthlib dash
```

## Setup
Modify `config.ini` such that `gcs_storage_key` points to your json GCS key, and `bucket_name` is `gs://` address of the relevant bucket.
