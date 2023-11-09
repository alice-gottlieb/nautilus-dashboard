"""
Collection of helper functions for polars operations, including FOV/spot selection.
"""
import polars as pl
import urllib.request
import numpy as np


def get_spots_from_fovs(fov_frame, fov_ids, rel_method, **kwargs):  # -> pl.Expr:
    """
    :brief: get a lazy-evaluable expression corresponding to the rows from
        the spot table corresponding to the listed slides
    :param fov_frame: dataframe-like (either expression or actual DF)
        containing per-FOV info. Column structure of this data:
        ====
        slide_label (string, unique ID for containing slide over multiple timestamps)
        id_in_slide (int, unique ID for FOV within slide)
        timestamp (datetime, time of slide timestep acquisition)
        ====
    :param fov_ids: list where each entry is a tuple in the form
        (slide_label,timestamp_start, timestamp_end, list_of_ids), where the list_of_ids
        is a list that
        can be empty to select all fovs within that timestamp range on that slide,
        where either of timestamp_start/end can be None to open the
        interval on that end. By default, timestamp intervals are assumed to be closed on the left
    :param rel_method: function that takes a pl.Expr corresponding to the appropriate
        FOVs as input and returns a pl.Expr corresponding to the appropriate rows (associated
        to the selected FOVs) from the spot table as output, taking any additional kwargs
        as input
    :return: pl.Expr corresponding to the desired rows from the spot table, which has column
        ====
        structure as follows:
        slide_label (string)
        timestamp (datetime)
        fov_id (int) (id within slide of associated fov)
        spot_id (int)
        score (float) (assigned probability that spot is a positive detection)
        (optional) coords_in_slide (float 2-tuple) (mm, mm) (for display of spot
            on deepzoom overview of slide)
        ====
    """
    # Extract the slide_label, timestamp, and id_in_slide columns from fov_frame
    fov_cols = ["slide_label", "id_in_slide", "timestamp"]
    fov_expr = pl.col(fov_cols).select(fov_frame)
    fov_filter = pl.col(fov_cols).is_not_null()

    # Create a filter expression for each slide_label and timestamp range
    for slide_label, timestamp_start, timestamp_end, fov_id_list in fov_ids:
        current_filter_expr = fov_expr["slide_label"] == slide_label

        if timestamp_start is not None:
            current_filter_expr &= fov_expr["timestamp"] >= timestamp_start
        if timestamp_end is not None:
            current_filter_expr &= fov_expr["timestamp"] < timestamp_end

        fov_filter |= current_filter_expr

    # Call the rel_method function with the selected FOVs to get the corresponding rows from the spot table
    spot_expr = rel_method(fov_expr.filter(fov_filter), **kwargs)

    return spot_expr


def default_relate_fovs_to_spots(rel_df, spot_df, fov_rows):  # ->pl.Expr:
    """
    :brief: given a pl.Expr corresponding to rows from the FOVs table,
        get a pl.Expr corresponding to rows from the spots table
    :param rel_df: Dataframe-like (expression or actual DF) corresponding
        to the relation table from FOVs to spots, with column structure
        as follows:
        ====
        slide_label (string, UID of slide over multiple timestamps)
        timestamp (datetime)
        fov_id (int) (id within slide of FOV)
        spot_id (int)
        (optional) coords_in_fov (int 2-tuple, pixels)
        ====
    :param spot_df: Dataframe-like corresponding to spot dataset, specification
        given in get_spots_from_fovs
    :param fov_rows: pl.Expr corresponding to appropriate rows from FOV dataset (structure
        given in get_spots_from_fovs)
    :return: pl.Expr corresponding to appropriate rows from spot_df
    """
    pass


def get_fovs_from_slides(
    slide_frame, slide_labels, rel_method, **kwargs
):  # -> pl.Expr:
    """
    :brief: get a lazy-evaluable expression corresponding to the
        rows from the FOV table corresponding to the listed slides
    :param slide_frame: dataframe-like (either expression or actual DF)
        containing per-slide-time-step info. Column structure of this data:
        ====
        label (string, unique ID for slide over multiple timestamps)
        name (string, human-readable)
        timestamp (datetime, for acquisition)
        total_rbc (int, est. total rbc counted on slide at this acquisition)
        ====
    :param slide_labels: list where each entry is a tuple (label, timestamp_start,
        timestamp_end), where either of timestamp_start/end can be None to open the
        interval on that end
    :param rel_method: function that takes a pl.Expr corresponding to
        the appropriate slide label/timestamp rows as input and returns a pl.Expr
        corresponding to the appropriate rows from the FOV table, taking any additional
        kwargs as input
    :return: pl.Expr corresponding to the desired rows from the FOV table
    """
    pass


def default_relate_slides_to_fovs(rel_df, fov_df, slide_rows):  # ->pl.Expr:
    """
    :brief: given a pl.Expr corresponding to rows from the slides table,
        get a pl.Expr corresponding to rows from the FOVs table
    :param rel_df: Dataframe-like (expression or actual DF) corresponding
        to the relation table from slides to FOVs, with column structure as
        follows:
        ====
        slide_label (string, unique ID of slide across timestamps)
        timestamp (datetime)
        fov_id_in_slide (int)
        indices_in_slide (int 3-tuple (x,y,z)) assumed from bottom left
        center_coords (float 3-tuple (x,y,z) (mm,mm,um)) coordinates
            of camera motor at time of acquisition
        (optional) corner_coords (float 4-tuple (x_top_left, y_top_left,
            x_bottom_right,y_bottom_right) (mm,mm,mm,mm))
        ====
    :param fov_df: Dataframe-like corresponding to the FOV data, with column
        structure given in get_spots_from_fovs
    :param slide_rows: pl.Expr corresponding to desired slide/timestamps in
        slide dataset (structure given in get_fovs_from_slides)
    :return: pl.Expr corresponding to the desired rows from fov_df
    """
    pass


def default_csv_downloader(url, savepath):
    """
    :brief: placeholder function that just calls
        urllib.request.urlretrieve to download a CSV.
        Meant to be substituted with other CSV-loading
        functions later on.
    :param url: URL for CSV
    :param savepath: filepath to save the CSV to
    :return: Filename under which the path can be found.
    """
    return urllib.request.urlretrieve(url, filename=savepath)[0]


def hist_expr_builder(column_name: str, ranges: list) -> pl.Expr:
    """
    :brief: Builds a pl.when(...).then(...).when(...).
        then(...)...otherwise(...).alias("hist_bin")
        expression for grouping rows into histogram bins
    :param column_name: Name of column to look at for values.
    :param ranges: list in the form [(start1,end1),(start2,end2),...)] of ranges, assumed
        closed on left only
    """
    range_expr = pl.when(pl.col(column_name) != pl.col(column_name)).then([0.0, 0.0])
    for start, end in ranges:
        range_expr = range_expr.when(
            pl.col(column_name).is_between(pl.lit(start), pl.lit(end), "left")
        ).then([start, end])
    range_expr = range_expr.otherwise([0.0, 0.0]).alias("hist_bin")
    return range_expr


def get_histogram_from_file(filepath, column_name, ranges):
    """
    :brief: Return a lazy-evaluable query that generates a histogram
        dataframe
    :param filepath: path to CSV
    :param column_name: column to generate histogram of
    :param ranges: list in form [(start1,end1),(start2,end2),...] of ranges
    :brief: returns a lazy-evaluable query that will generate a histogram dataframe
        with two columns, "hist_bin" (histogram ranges, each in the form of a two-item list),
        and "count" (the number of rows where the indicated column was in that range)
    """
    q = (
        pl.scan_csv(filepath)
        .select(pl.col(column_name), hist_expr_builder(column_name, ranges))
        .group_by("hist_bin")
        .count()
    )
    return q


def get_results_from_threshold(
    spot_df,
    threshold,
    ann_dict={"non-parasite": 0, "parasite": 1, "unsure": 2, "unlabeled": -1},
):
    """
    :brief: Given a dataframe of spots and a threshold, compute
        the number of positive/negative/unsure spots in a slide
    :param spot_df: dataframe containing spots. Should have
        columns "parasite output", "non-parasite output",
        "unsure output", "annotation"
    :param threshold: threshold between 0 and 1.0
    :return: dictionary of counts "predicted_positive",
    "predicted_negative", "predicted_unsure", "pos_annotated",
    "neg_annotated", "unsure_annotated", "total_annotated_positive_negative"
    """
    pred_pos = (
        spot_df.filter(pl.col("parasite output") > threshold).select(pl.count()).item()
    )
    pred_neg = (
        spot_df.filter(
            (pl.col("parasite output") < threshold)
            & (pl.col("non-parasite output") > pl.col("unsure output"))
        )
        .select(pl.count())
        .item()
    )
    pred_unsure = spot_df.select(pl.count()).item() - pred_pos - pred_neg
    ann_pos = (
        spot_df.filter(pl.col("annotation") == ann_dict["parasite"])
        .select(pl.count())
        .item()
    )
    ann_neg = (
        spot_df.filter(pl.col("annotation") == ann_dict["non-parasite"])
        .select(pl.count())
        .item()
    )
    ann_unsure = (
        spot_df.filter(pl.col("annotation") == ann_dict["unsure"])
        .select(pl.count())
        .item()
    )
    total_ann_pos_neg = ann_pos + ann_neg
    return {
        "predicted_positive": pred_pos,
        "predicted_negative": pred_neg,
        "predicted_unsure": pred_unsure,
        "pos_annotated": ann_pos,
        "neg_annotated": ann_neg,
        "unsure_annotated": ann_unsure,
        "total_annotated_positive_negative": total_ann_pos_neg,
    }


def get_detection_stats_vs_threshold(
    spot_df,
    thresholds_array,
    ann_dict={"non-parasite": 0, "parasite": 1, "unsure": 2, "unlabeled": -1},
):
    """
    :brief: get a dataframe with thresholds in one column and per-threshold
        annotated/predicted positive/negative/unsure data, along with
        false positive/false negative counts
    :param spot_df: dataframe containing spots. should have columns
        "parasite output", "non-parasite output", "unsure output",
        "annotation"
    :param thresholds array: numpy ndarray of thresholds
    """
    plot_df = pl.DataFrame({"threshold": thresholds_array})

    # create a dictionary of pl.Expr for each condition we want to count
    expr_dict = {
        "predicted_positive": pl.col("parasite output") > pl.col("threshold"),
        "predicted_negative": (pl.col("parasite output") < pl.col("threshold"))
        & (pl.col("non-parasite output") > pl.col("unsure output")),
    }

    expr_dict["false_positive"] = (expr_dict["predicted_positive"]) & (
        pl.col("annotation") == pl.lit(ann_dict["non-parasite"])
    )
    expr_dict["false_negative"] = (expr_dict["predicted_negative"]) & (
        pl.col("annotation") == pl.lit(ann_dict["parasite"])
    )

    df_list = []

    for column in expr_dict.keys():
        # counts per threshold of matching instances
        matching_df = (
            plot_df.lazy()
            .join(spot_df.lazy(), how="cross")
            .filter(expr_dict[column])
            .group_by("threshold", maintain_order=True)
            .agg(pl.count().alias(column))
        )
        df_list.append(matching_df)

    # collect all queries at once
    plot_df = pl.concat(df_list, how="align").collect(streaming=True)

    spot_count = spot_df.select(pl.count()).item()

    # get predicted_unsure counts
    plot_df = plot_df.with_columns(
        (
            spot_count - pl.col("predicted_positive") - pl.col("predicted_negative")
        ).alias("predicted_unsure")
    )

    # get annotation counts for ease of plotting

    ann_pos = (
        spot_df.filter(pl.col("annotation") == ann_dict["parasite"])
        .select(pl.count())
        .item()
    )
    ann_neg = (
        spot_df.filter(pl.col("annotation") == ann_dict["non-parasite"])
        .select(pl.count())
        .item()
    )
    ann_unsure = (
        spot_df.filter(pl.col("annotation") == ann_dict["unsure"])
        .select(pl.count())
        .item()
    )
    total_ann_pos_neg = ann_pos + ann_neg

    plot_df = plot_df.with_columns(
        pl.lit(ann_pos).alias("pos_annotated"),
        pl.lit(ann_neg).alias("neg_annotated"),
        pl.lit(ann_unsure).alias("unsure_annotated"),
        pl.lit(total_ann_pos_neg).alias("total_annotated_positive_negative"),
    )
    plot_df = plot_df.with_columns(pl.all().fill_null(strategy="zero"))

    return plot_df
