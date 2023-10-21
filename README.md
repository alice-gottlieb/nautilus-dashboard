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