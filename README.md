# nautilus-dashboard
Dashboard for displaying and inspecting data from slides

## Dependencies
```
pip install polars gcsfs google-auth google-auth-httplib2 google-auth-oauthlib dash Flask-Caching dash-bootstrap-components google-api-python-client pillow 
```

## Setup
Create a `config.ini` file in the top directory of this repo with the following content:
```
[DISPLAY
spots_rows_per_page=2
spot_columns_per_page=5

[GCS]
gcs_storage_key=/path/to/your/gcs/key.json
bucket_url=gs://bucket-name

[TESTING]
cutoff=10
debug=True
```

Modify `config.ini` such that `gcs_storage_key` points to your json GCS key, and `bucket_name` is `gs://` address of the relevant bucket.
Additionally, set `cutoff` to a low number if testing things out.

# Run #
```
python app.py
```
