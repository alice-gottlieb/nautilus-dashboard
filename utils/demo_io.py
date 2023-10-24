"""
I/O functions for demo. A lot of these require a storage.Client/storage_service and bucket_name, and also a gcs
(GCS File System)
For an example of how to define these so you can call these functions, check examples/gcs_example.py

You can set your bucket name, which should be gs:// url, as well as the path to your account key
JSON, via config.ini at the root of this repository.
"""

import polars as pl
from utils.polars_helpers import hist_expr_builder, get_results_from_threshold
from PIL import Image
from io import BytesIO
import numpy as np


def list_blobs_with_prefix(
    storage_client, bucket_name, prefix, delimiter=None, cutoff=None
):
    """Returns a dict with two entries that are both lists, 'blobs' is
    filenames under the given prefix (folder) in the bucket bucket_name,
    and 'prefixes' is the list of directories under this prefix

    This can be used to list all blobs in a "folder", e.g. "public/".

    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:

        a/1.txt
        a/b/2.txt

    If you specify prefix ='a/', without a delimiter, you'll get back:

        a/1.txt
        a/b/2.txt

    However, if you specify prefix='a/' and delimiter='/', you'll get back
    only the file directly under 'a/':

        a/1.txt

    As part of the response, you'll also get back a blobs.prefixes entity
    that lists the "subfolders" under `a/`:

        a/b/
    """
    # Note: Client.list_blobs requires at least package version 1.17.0.
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter=delimiter)

    count = 0
    # Note: The call returns a response only when the iterator is consumed.
    retdict = {"blobs": [], "prefixes": []}
    for blob in blobs:
        if cutoff is not None and count >= cutoff:
            break
        retdict["blobs"].append(blob.name)
        count += 1

    count = 0
    if delimiter:
        for prefix in blobs.prefixes:
            if cutoff is not None and count >= cutoff:
                break
            retdict["prefixes"].append(prefix)
            count += 1

    return retdict


def get_histogram_df(file, column_name, ranges):
    """
    :brief: Return a histogram dataframe. Modified version to work with GCS
    :param file: file type
    :param column_name: column to generate histogram of
    :param ranges: list in form [(start1,end1),(start2,end2),...] of ranges
    :brief: returns a lazy-evaluable query that will generate a histogram dataframe
        with two columns, "hist_bin" (histogram ranges, each in the form of a two-item list),
        and "count" (the number of rows where the indicated column was in that range)
    """
    hist_df = (
        pl.read_csv(file)
        .select(pl.col(column_name), hist_expr_builder(column_name, ranges))
        .group_by("hist_bin")
        .count()
    )
    return hist_df


def get_top_level_dirs(
    client,
    bucket_name,
    cutoff=None,
    excluded_dirnames=[
        "npy_files/",
        "patient_slides_analysis/",
        "slide-overview/",
        "spot_detection_result/",
    ],
):
    """
    :brief: given a storage service and bucket name,
    returns a list of top level directories
    """
    items = list_blobs_with_prefix(
        client, bucket_name, "", delimiter="/", cutoff=cutoff
    )
    dirs = [item for item in items["prefixes"] if item not in excluded_dirnames]
    return dirs


def get_fov_image_list(client, bucket_name, slide_name):
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
    fov_imgs = list_blobs_with_prefix(client, bucket_name, prefix)["blobs"]
    return fov_imgs


def crop_spots_from_slide(storage_service, bucket_name, slide_name, coord_list):
    """
    :brief: returns a list of file objects corresponding to spots cropped from
        various FOVs in the same slide
    :param coord_list: list of integer 6-tuples in the form (fov_row, fov_col,
        fov_z, x, y, r) where the relevant fov image is
        [fov_row]_[fov_col]_[fov_z].jpg and (x,y,r) are integer 3-tuples
        where x and y are the center coordinate of the spot in pixels in the FOV
        and r is half the side length of the square in pixels
    :return: list of file objects corresponding to cropped images of the spots
    """
    spot_imgs = []
    fov_spot_indices = {}
    fov_spot_coord_lists = {}
    spot_index = 0
    for row, col, z, x, y, r in coord_list:  # set up individual lists of spots in
        # FOVs
        spot_imgs.append(None)
        fov_filename = str(row) + "_" + str(col) + "_" + str(z)
        try:
            fov_spot_indices[fov_filename].append(spot_index)
            fov_spot_coord_lists[fov_filename].append((x, y, r))
        except:  # new FOV encountered
            fov_spot_indices[fov_filename] = [spot_index]
            fov_spot_coord_lists[fov_filename] = [(x, y, r)]
        spot_index += 1
    for fov in fov_spot_coord_lists.keys():
        fov_uri = fov + ".jpg"
        fov_images = crop_spots_from_fov(
            storage_service, bucket_name, slide_name, fov_uri, fov_spot_coord_lists[fov]
        )
        for spot_img, global_index in zip(fov_images, fov_spot_indices[fov]):
            spot_imgs[global_index] = spot_img
    return spot_imgs


def crop_spots_from_fov(
    storage_service, bucket_name, slide_name, uri, coord_and_radius_list
):
    """
    :brief: returns a list of file objects corresponding to spots cropped from
        the FOV at uri
    :param uri: image uri of the FOV, omitting bucket/slide name and
        spot_detection_result directory name
    :param coord_and_radius_list: list of integer 3-tuples in the form (x,y,r)
        where x and y are the center coordinates of the spot in pixels in the
        FOV and r is half side length of the square in pixels
    :return: list of file objects corresponding to cropped images of the spots
    """
    image = get_image(storage_service, bucket_name, slide_name, uri)
    spot_images = []
    for x, y, r in coord_and_radius_list:
        left = x - r
        top = y - r
        right = x + r
        bottom = y + r

        spot_image = image.crop((left, top, right, bottom))
        spot_images.append(spot_image)
    return spot_images


def get_image(
    storage_service,
    bucket_name,
    slide_name,
    uri,
    resize_factor=1.0,
):
    """
    :brief: returns a file object corresponding to the image at the given uri
    :param uri: uri of the image, omitting bucket name
    """
    prefix = slide_name
    if not prefix.endswith("/"):
        prefix += "/"
    prefix += "spot_detection_result/"
    image = Image.open(
        BytesIO(
            (
                storage_service.objects()
                .get_media(bucket=bucket_name, object=(prefix + uri))
                .execute()
            )
        )
    )
    image = image.resize(
        (int(image.size[0] * resize_factor), int(image.size[1] * resize_factor))
    )
    print("Got image at " + str(bucket_name) + "/" + str(slide_name) + "/" + str(uri))
    return image


def get_initial_slide_df_with_predictions_only(client, bucket_name, gcs, cutoff=None):
    """
    :brief: returns a dataframe according to the req1 spec. mostly unpopulated because it
        takes a long time to do all the requisite file i/o
    """
    # get list of slide names
    if cutoff is not None:
        cutoff *= 2
    slide_files_raw = list_blobs_with_prefix(
        client, bucket_name, prefix="patient_slides_analysis", cutoff=cutoff
    )["blobs"]

    slides = [
        slidefile.split("/")[-1].strip(".npy")
        for slidefile in slide_files_raw
        if slidefile.endswith(".npy")
    ]

    slide_df = pl.DataFrame()

    for sl in slides:
        # list of fovs, mostly for fov count
        fov_imgs = get_fov_image_list(client, bucket_name, sl)

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


def get_initial_slide_df(client, bucket_name, gcs, cutoff=None):
    """
    :brief: returns a dataframe according to the req1 spec. mostly unpopulated because it
        takes a long time to do all the requisite file i/o
    """
    # get list of slide names
    slides = get_top_level_dirs(client, bucket_name, cutoff=cutoff)

    slide_df = pl.DataFrame()

    for sl in slides:
        # list of fovs, mostly for fov count
        fov_imgs = get_fov_image_list(client, bucket_name, sl)

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
def get_fovs_df(client, bucket_name, list_of_slide_names):
    """
    :brief: given a list of slide names, gets a dataframe populated with their FOVs.
    :param list_of_slide_names: Slide names. Should be the names and only the names of
        top-level directories in the bucket
    """
    fovs = pl.DataFrame()
    for sl in list_of_slide_names:
        sl_fovs_dict = {}
        # get image uris
        fov_list = get_fov_image_list(client, bucket_name, sl)

        # for i in range(
        #     len(fov_list)
        # ):  # stopgap measure using the authenticated url for demo, since these images aren't public
        #     fov_list[i] = (
        #         "https://storage.cloud.google.com/"
        #         + bucket_name.strip("/")
        #         + "/"
        #         + fov_list[i]
        #     )

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
        try:
            sl_fovs_df = sl_fovs_df.with_columns(
                sl_fovs_df["timestamp"].str.to_datetime("%Y-%m-%d_%H-%M-%S")
            )
        except:
            print(
                "Unable to convert timestamp column to datetime from slide name: " + sl
            )
        # add to total fov df
        fovs = pl.concat([fovs, sl_fovs_df])
    return fovs


def populate_slide_rows(
    client, bucket_name, gcs, slide_df, list_of_slide_names, set_threshold=None
):
    """
    :brief: Populate selected slides' rows with info that takes a longer time to compute
    :param slide_df: per-slide dataframe defined by get_initial_slide_df
    :param list_of_slide_names: list of slide names, not including bucket name
    :param set_threshold: either a scalar to apply as threshold to all slides, or a list of thresholds
        of the same length as list of slide names, to apply to each corresponding one
    :return: version of slide df with more detailed info populated
    """
    if type(set_threshold) is list:
        if len(set_threshold) != len(list_of_slide_names):
            raise ValueError(
                "length of set_threshold must be same as list of slide names if a list"
            )
    else:
        set_threshold = [set_threshold] * len(list_of_slide_names)

    new_slide_df = slide_df

    for sl, thresh in zip(list_of_slide_names, set_threshold):
        #### To set a new value in the row, set slide_row_dict[column_name][0]=value
        slide_row_dict = (
            slide_df.filter(pl.col("slide_name") == sl.strip("/"))
            .head(1)
            .to_dict(as_series=False)
        )
        if thresh is not None:  # update threshold if a new value is given
            slide_row_dict["threshold"][0] = thresh

        ### same count tally procedure, except we actually go as far as to open spot_data_raw
        total_rbc_count_file_path = (
            bucket_name.strip("/") + "/" + sl.strip("/") + "/total number of RBCs.txt"
        )
        segmentation_stat_file_path = (
            bucket_name.strip("/") + "/" + sl.strip("/") + "/segmentation_stat.csv"
        )
        spot_data_raw_file_path = (
            bucket_name.strip("/") + "/" + sl.strip("/") + "/spot_data_raw.csv"
        )
        try:  # check for rbc count tally file
            with gcs.open(total_rbc_count_file_path, "r") as f:
                slide_row_dict["rbcs"][0] = int(f.read().strip())
        except:  # no rbc count tally file, try tallying segmentation_stat.csv
            print("no spot tally file found for " + str(sl))
            try:  # check for per-fov segmentation tally file
                with gcs.open(segmentation_stat_file_path, "rb") as f:
                    seg_stat_df = pl.read_csv(f)
                    slide_row_dict["rbcs"][0] = seg_stat_df.select(
                        pl.sum("count")
                    ).item()
            except:  # otherwise, check length of spot_data_raw.csv
                print("no segmentation_stat file found for " + str(sl))
                try:
                    with gcs.open(spot_data_raw_file_path, "rb") as f:
                        spot_data_df = pl.read_csv(f)
                        slide_row_dict["rbcs"][0] = len(spot_data_df)
                except:
                    print("no spot_data_raw.csv found for " + str(sl))

        if thresh is not None:
            spot_df = get_spots_csv(bucket_name, gcs, sl)
            results = get_results_from_threshold(spot_df, thresh)
            for k in results.keys():
                slide_row_dict[k] = results[k]

        ### update row in dataframe to be returned
        for k in slide_row_dict.keys():
            if k == "slide_name":
                continue
            new_slide_df = new_slide_df.with_columns(
                pl.when(pl.col("slide_name") == sl.strip("/"))
                .then(slide_row_dict[k][0])
                .otherwise(pl.col(k))
                .alias(k)
            )

    return new_slide_df


def get_mapping_csv(bucket_name, gcs, slide_name):
    """
    :brief: returns a dataframe corresponding to the spot_data_raw.csv (which has coordinates and radii for a given spot)
    :param bucket_name: name of bucket
    :param gcs: GCS file system object
    :param slide_name: name of slide, not including bucket name
    :return spots_csv: polars dataframe corresponding to raw spot data for given slide
    """
    spot_data_raw_file_path = bucket_name.strip("/") + "/" + slide_name + "/mapping.csv"
    try:
        with gcs.open(spot_data_raw_file_path, "rb") as f:
            spots_csv = pl.read_csv(f)
            return spots_csv
    except:
        print("No mapping.csv found for " + str(slide_name))
        return None


# TODO: Pull spots data from patient_slides_analysis folder
# npy data of form [slide_name].npy
# npy data is currently too large to pull
def get_spots_csv(bucket_name, gcs, slide_name):
    """
    :brief: returns a dataframe corresponding to the spots data for a given slide
    :param bucket_name: name of bucket
    :param gcs: GCS file system object
    :param slide_name: name of slide, not including bucket name
    :return spots_csv: polars dataframe corresponding to spots data for given slide
    """
    spot_data_raw_file_path = (
        bucket_name.strip("/")
        + "/patient_slides_analysis/"
        + slide_name
        + "_ann_w_pred.csv"
    )
    try:
        with gcs.open(spot_data_raw_file_path, "rb") as f:
            spots_csv = pl.read_csv(f)
            return spots_csv
    except:
        print("No annotation/prediction csv found for " + str(slide_name))
        return None


def get_combined_spots_df(bucket_name, gcs, slide_name):
    """
    :brief: returns a dataframe corresponding to spots data for a given slide
        that has been combined to also have prediction scores
    """
    mapping_df = get_mapping_csv(bucket_name, gcs, slide_name)
    spot_data_df = get_spots_csv(bucket_name, gcs, slide_name)
    spot_data_df = spot_data_df.sort(pl.col("index"))
    ### Hacky version because global_index randomly has large offsets
    ### or multipliers for some slides? This relies on mapping.csv having
    ### global_index sorted by the index of the spot, which seems like
    ### a reasonable assumption when they're always the same length
    combined_spots_df = pl.concat([spot_data_df, mapping_df], how="horizontal")
    return combined_spots_df


def get_spots_npy(bucket_name, gcs, slide_name):
    """
    :brief: returns a dataframe corresponding to the spots data for a given slide
    :param bucket_name: name of bucket
    :param gcs: GCS File System object
    :param slide_name: name of slide, not including bucket name
    :return spots_npy: polars dataframe corresponding to spots data for given slide
        Shape of npy data is (# of images, 4, 31, 31)
            Axis 0: image number
            Axis 1: 4 channels (A, R, G, B)
            [Unsure if this is correct order, the final 3 appear to be RGB]
            Axis 2: 31 pixels
            Axis 3: 31 pixels
        To get an RGB image, use:
            Image.fromarray(spot_images_npy(*args)[n, 1:, :, :].T, 'RGB')
    """
    npy_data_raw_file_path = (
        bucket_name.strip("/") + "/patient_slides_analysis/" + slide_name + ".npy"
    )
    try:
        with gcs.open(npy_data_raw_file_path, "rb") as f:
            spots_npy = np.load(f)
            return spots_npy
    except:
        print("No npy found for " + str(slide_name))
        return None
