"""
I/O functions for demo. A lot of these require a storage_service and bucket_name, and also a gcs
(GCS File System)
For an example of how to define these so you can call these functions, check examples/gcs_example.py

You can set your bucket name, which should be gs:// url, as well as the path to your account key
JSON, via config.ini at the root of this repository.
"""

import polars as pl


def get_top_level_dirs(storage_service, bucket_name):
    """
    :brief: given a storage service and bucket name,
    returns a list of top level directories
    """
    blobs = storage_service.objects().list(bucket=bucket_name, delimiter="/").execute()
    dirs = blobs.get("prefixes", [])
    return dirs


def get_fov_image_list(storage_service, bucket_name, slide_name):
    """
    :brief: returns a list of filepaths to the files under
        slide_name/spot_detection_result/. File paths omit bucket name,
        and are in the form
        "[slide_name]/spot_detection_result/[row]_[col]_[z].[format]"
    """
    prefix = slide_name
    if not prefix.endswith("/"):
        prefix += "/"
    prefix += "spot_detection_result/"
    blobs = storage_service.objects().list(bucket=bucket_name, prefix=prefix).execute()
    fov_blobs = blobs.get("items", [])
    fov_imgs = []
    for b in fov_blobs:
        fov_imgs.append(b["name"])
    return fov_imgs


def get_initial_slide_df(storage_service, bucket_name, gcs):
    """
    :brief: returns a dataframe according to the req1 spec. mostly unpopulated because it
        takes a long time to do all the requisite file i/o
    """
    # get list of slide names
    slides = get_top_level_dirs(storage_service, bucket_name)

    slide_df = pl.DataFrame()

    for sl in slides:
        # list of fovs, mostly for fov count
        fov_imgs = get_fov_image_list(storage_service, bucket_name, sl)

        # initialize variables
        no_spots = None
        threshold = None
        predicted_positive = None
        predicted_negative = None
        predicted_unsure = None
        pos_annotated = None
        neg_annotated = None
        unsure_annotated = None
        total_annotated_positive_negative = None

        # file paths to segment/rbc tally files
        total_rbc_count_file_path = (
            bucket_name.strip("/") + "/" + sl.strip("/") + "/total number of RBCs.txt"
        )
        segmentation_stat_file_path = (
            bucket_name.strip("/") + "/" + sl.strip("/") + "/segmentation_stat.csv"
        )
        try:  # check for rbc count tally file
            with gcs.open(total_rbc_count_file_path, "r") as f:
                no_spots = int(f.read().strip())
        except:  # no rbc count tally file, try tallying segmentation_stat.csv
            print("no spot tally file found for " + str(sl))
            try:  # check for per-fov segmentation tally file
                with gcs.open(segmentation_stat_file_path, "rb") as f:
                    seg_stat_df = pl.read_csv(f)
                    no_spots = seg_stat_df.select(pl.sum("count")).item()
            except:  # otherwise, leave spot count null
                print("no segmentation_stat file found for " + str(sl))
        no_fovs = len(fov_imgs)
        slide_row_dict = {}

        # put variables in a dict
        slide_row_dict["slide_name"] = [sl.strip("/")]
        slide_row_dict["fov_count"] = [no_fovs]
        slide_row_dict["rbcs"] = [no_spots]
        slide_row_dict["threshold"] = [threshold]
        slide_row_dict["predicted_positive"] = [predicted_positive]
        slide_row_dict["predicted_negative"] = [predicted_negative]
        slide_row_dict["predicted_unsure"] = [predicted_unsure]
        slide_row_dict["pos_annotated"] = [pos_annotated]
        slide_row_dict["neg_annotated"] = [neg_annotated]
        slide_row_dict["unsure_annotated"] = [unsure_annotated]
        slide_row_dict["total_annotated_positive_negative"] = [
            total_annotated_positive_negative
        ]

        # turn into a row and cast nullable values to datatypes to ensure compliance
        slide_row = pl.DataFrame(slide_row_dict)
        slide_row = slide_row.with_columns(slide_row["rbcs"].cast(pl.Int64))
        slide_row = slide_row.with_columns(slide_row["threshold"].cast(pl.Float32))
        slide_row = slide_row.with_columns(
            slide_row["predicted_positive"].cast(pl.Int64)
        )
        slide_row = slide_row.with_columns(
            slide_row["predicted_negative"].cast(pl.Int64)
        )
        slide_row = slide_row.with_columns(slide_row["predicted_unsure"].cast(pl.Int64))
        slide_row = slide_row.with_columns(slide_row["pos_annotated"].cast(pl.Int64))
        slide_row = slide_row.with_columns(slide_row["neg_annotated"].cast(pl.Int64))
        slide_row = slide_row.with_columns(slide_row["unsure_annotated"].cast(pl.Int64))
        slide_row = slide_row.with_columns(
            slide_row["total_annotated_positive_negative"].cast(pl.Int64)
        )
        slide_df = pl.concat([slide_df, slide_row])
    return slide_df


#### NOTE: The image URIs given are in the form path/to/image/in/bucket (omitting bucket name)
#### so adjust your image displaying accordingly
def get_fovs_df(storage_service, bucket_name, list_of_slide_names):
    """
    :brief: given a list of slide names, gets a dataframe populated with their FOVs.
    :param list_of_slide_names: Slide names. Should be the names and only the names of
        top-level directories in the bucket
    """
    fovs = pl.DataFrame()
    for sl in list_of_slide_names:
        sl_fovs_dict = {}
        # get image uris
        fov_list = get_fov_image_list(storage_service, bucket_name, sl)
        sl_fovs_dict["image_uri"] = fov_list
        # same slide label for all fovs in a slide
        sl_fovs_dict["slide_label"] = [sl.strip("/")] * len(fov_list)
        # no default id in slide, so assign programmatically
        sl_fovs_dict["id_in_slide"] = list(range(len(fov_list)))
        timestamp = None
        try:  # get timestamp from slide name
            timestamp = "_".join(sl.split(".")[-2].split("_")[-2:])
        except:
            print("Unable to extract timestamp string from slide name: " + sl)

        # same timestamp over entire slide
        sl_fovs_dict["timestamp"] = [timestamp] * len(fov_list)

        sl_fovs_df = pl.DataFrame(sl_fovs_dict)

        # format timestamp column as datetime
        sl_fovs_df = sl_fovs_df.with_columns(
            sl_fovs_df["timestamp"].str.to_datetime("%Y-%m-%d_%H-%M-%S")
        )
        # add to total fov df
        fovs = pl.concat([fovs, sl_fovs_df])
    return fovs
