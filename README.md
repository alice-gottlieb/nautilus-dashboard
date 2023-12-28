# nautilus-dashboard
Dashboard for displaying and inspecting data from slides

## Dependencies
```
pip install polars gcsfs google-auth google-auth-httplib2 google-auth-oauthlib dash Flask-Caching dash-bootstrap-components google-api-python-client pillow zarr dash-auth
```

## Setup
Create a `config.ini` file in the top directory of this repo with the following content:
```
[DISPLAY]
spot_rows_per_page=10
spot_columns_per_page=10
cache_timeout=60
slides_per_page=200

[SLIDES]
slide_df_cache_dir=slide_df_cache/

[GCS]
gcs_storage_key=/path/to/your/service/account/key.json
bucket_url=gs://NAME-OF-ONE-BUCKET,gs://NAME-OF-ANOTHER-BUCKET(, possibly more)

[TESTING]
slide_count_cutoff=50
debug=True
```

Modify `config.ini` such that `gcs_storage_key` points to your json GCS key, and `bucket_name` is `gs://` address of the relevant bucket.

Additionally, set `cutoff` to a low number if testing things out.

### Skip the following step if you already have a csv of per-slide data
For performance purposes, copy `scripts/populate_cache.csv.py` to the root directory of the repository, edit the `bucket_name` variable inside near the beginning to the bucket you want to cache. This will generate a file
`slide_df_cache/slides.csv` that caches an initial first pass at per-slide data, with a set prediction threshold if any prediction data can be found. It will default to a version without prediction data otherwise.

# Run #
```
python app.py
```
# TODO (QoL Fixes)
* [ ] Add fov_count to FOVs page
* [ ] Change pagination
* [ ] Make padding look nice
* [ ] Lazy load images in tooltips 
* [ ] Allow resizing of individual image in their own pg
* [ ] Reorder and rename cols
* [ ] Dark mode TM
* [ ] Titles on each pg
* [ ] Favicon
* [ ] Shortened or more human readables titles for slides + FOVs?
* [ ] Check mobile compatibility
* [ ] Font size + font choice
* [ ] Prettier table styling
* [ ] Show a logo on the top left?
* [ ] Write Nautilus Dashboard
* [ ] Add a search or filtering bar?
* [ ] Add a button to download the data as a CSV or Excel file
* [ ] Add charts for slides
