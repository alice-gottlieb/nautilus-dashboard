from dash import Dash, html, dash_table, dcc, callback, Output, Input, callback_context
import dash
import dash_auth
import dash_bootstrap_components as dbc
import pandas as pd
import polars as pl
import numpy as np
import datetime as dt
import plotly.express as px
import plotly.graph_objs as go
from google.cloud import storage
from configparser import ConfigParser
from google.oauth2 import service_account
from googleapiclient.discovery import build
from utils.demo_io import (
    get_initial_slide_df_with_predictions_only,
    get_initial_slide_df,
    get_fovs_df,
    get_top_level_dirs,
    populate_slide_rows,
    get_histogram_df,
    get_image,
    get_spots_csv,
    get_mapping_csv,
    crop_spots_from_slide,
    get_combined_spots_df,
)
from utils.polars_helpers import (
    get_detection_stats_vs_threshold,
    get_results_from_threshold,
)
from utils.zarr_utils import parse_slide, encode_image, get_images_from_zarr_built_in
from utils.img_embed_utils import generate_temporary_public_url
import polars as pl
from gcsfs import GCSFileSystem
from PIL import Image
from io import BytesIO
from flask_caching import Cache
import os
import json
import asyncio

VALID_USERNAME_PASSWORD_PAIRS = None

# parse in valid usernames/passwds
with open("users.json","r") as userfile:
    VALID_USERNAME_PASSWORD_PAIRS = json.load(userfile)

# Parse in key and bucket name from config file
cfp = ConfigParser()
cfp.read("config.ini")

service_account_key_json = cfp["GCS"]["gcs_storage_key"]
gs_urls = cfp["GCS"]["bucket_urls"].split(",")

for i in range(len(gs_urls)):
    gs_urls[i] = gs_urls[i].strip()

slide_df_cache_dir = "slide_df_cache/"
try:
    slide_df_cache_dir = cfp["SLIDES"]["slide_df_cache_dir"]
except:
    pass

cutoff = None
try:
    cutoff = int(cfp["TESTING"]["slide_count_cutoff"])
except:
    pass


slides_per_page=200
try:
    slides_per_page = int(cfp["DISPLAY"]["slides_per_page"])
except:
    pass

embedding_url_timeout = 20.0
try:
    embedding_url_timeout = float(cfp["DISPLAY"]["embedding_url_timeout"])
except:
    pass

spot_rows_per_page = 2
spot_columns_per_page = 5

try:
    spot_rows_per_page = int(cfp["DISPLAY"]["spot_rows_per_page"])
except:
    pass
try:
    spot_columns_per_page = int(cfp["DISPLAY"]["spot_columns_per_page"])
except:
    pass


spots_per_page = spot_rows_per_page * spot_columns_per_page

cache_timeout = 20
try:
    cache_timeout = int(cfp["DISPLAY"]["cache_timeout"])
except:
    pass

bucket_names = []

for url in gs_urls:
    bucket_names.append(url.replace("gs://", ""))

client = storage.Client.from_service_account_json(service_account_key_json)

debug = False

try:
    debug = cfp["TESTING"]["debug"]
    if debug == "true" or debug == "True":
        debug = True
except:
    pass


# Define GCS file system so files can be read
gcs = GCSFileSystem(token=service_account_key_json)

# Authenticate using the service account key file
credentials = service_account.Credentials.from_service_account_file(
    service_account_key_json, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Create a storage client
storage_service = build("storage", "v1", credentials=credentials)


slides_placeholder = pl.DataFrame(
    {
        "slide_name": ["slide1", "slide2"],
        "predicted_positive": [5, 6],
        "predicted_negative": [7, 8],
        "predicted_unsure": [9, 1],
        "threshold": [0.4, 0.5],
        "pos_annotated": [5, 7],
        "neg_annotated": [8, 9],
        "unsure_annotated": [9, 0],
        "total_annotated_positive_negative": [13, 16],
        "fov_count": [10, 11],
        "rbcs": [12, 13],
    }
)


# slide_label (string, unique ID for containing slide over multiple timestamps)
# id_in_slide (int, unique ID for FOV within slide)
# timestamp (datetime, time of slide timestep acquisition)
# image_uri (string, URI to image file)
fovs = pl.DataFrame(
    {
        "slide_label": ["slide1", "slide1", "slide1", "slide2", "slide2"],
        "id_in_slide": [1, 2, 3, 1, 4],
        "timestamp": [
            dt.datetime(2021, 1, 1),
            dt.datetime(2021, 1, 2),
            dt.datetime(2021, 1, 3),
            dt.datetime(2021, 1, 1),
            dt.datetime(2021, 1, 2),
        ],
        "image_uri": [
            "/assets/images/nautilus1_small.jpg",
            "/assets/images/nautilus1_tiny.jpg",
            "/assets/images/nautilus1_small.jpg",
            "/assets/images/nautilus1_tiny.jpg",
            "/assets/images/nautilus1_small.jpg",
        ],
    }
)


# Create the Dash app
if debug:
    app = Dash(
        __name__,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
        suppress_callback_exceptions=True,
    )
else:
    app = Dash(
        __name__,
        update_title=None,
        suppress_callback_exceptions=True,
        external_stylesheets=[dbc.themes.BOOTSTRAP],
    )

auth = dash_auth.BasicAuth(app,VALID_USERNAME_PASSWORD_PAIRS)

cache = Cache(
    app.server, config={"CACHE_TYPE": "filesystem", "CACHE_DIR": "cache-directory"}
)

@cache.memoize(timeout=cache_timeout)
def get_spot_channels(bucket_name, spot_dir_path, extension=".png"):
    channel_list = []
    channel_get_prefix = os.path.join(spot_dir_path, "0_")
    spot_zero_blobs = client.list_blobs(bucket_name, prefix=channel_get_prefix)
    for blob in spot_zero_blobs:
        channel_list.append(blob.name.split("_")[-1].split(".")[0])
    return channel_list

def get_spot_embeds(bucket_name, spot_dir_path ,spot_id_list, extension=".png"):
    channel_list = get_spot_channels(bucket_name, spot_dir_path, extension=extension)
    spot_imgs = []
    for spot_id in spot_id_list:
        spot_dict = {"spot_id":spot_id}
        spot_prefix = os.path.join(spot_dir_path, str(spot_id))
        for channel in channel_list:
            spot_dict[channel] = generate_temporary_public_url(client, bucket_name,spot_prefix+"_"+channel+extension, embedding_url_timeout)
        spot_imgs.append(spot_dict)
    return spot_imgs

@cache.memoize(timeout=cache_timeout)
def get_spots_from_zarr(slide_img_url, spot_id_list):
    """
    :brief: Returns a list of dicts in the form
        {"spot_id":spot id, "image_channel_1_label":image_channel_ndarray,...}
        for all channels in the image returned by our zarr methods
    """
    spot_image_zarr = parse_slide(gcs, slide_img_url)
    images = get_images_from_zarr_built_in(spot_image_zarr,spot_id_list)
    #for spot_id in spot_id_list:
    #    images.append(get_image_from_zarr(spot_image_zarr, spot_id))
    return images


@cache.memoize(timeout=cache_timeout)
def slide_df_cached(bucket_name, my_cutoff=None):
    slides = None
    try:
        slides = pl.read_csv(os.path.join(slide_df_cache_dir, bucket_name + ".csv"))
    except:
        slides = get_initial_slide_df_with_predictions_only(
            client, bucket_name, gcs, cutoff=my_cutoff
        )
    try:
        slides = slides.with_columns(
            (pl.col("predicted_positive") * (5e6) / pl.col("rbcs"))
            .cast(pl.Int64)
            .alias("positives/5M rbc")
        )
    except:  # if calculation of positive rate fails for whatever reason, skip
        pass
    # add a column for viewing FOVs/spots/charts
    # leave this in even after using get_initial_slide_df for the slides table
    try:
        slides.select(pl.col("slide_name"))
    except pl.exceptions.ColumnNotFoundError:
        try:
            slides = get_initial_slide_df(client, bucket_name, gcs, cutoff=my_cutoff)
        except:
            slides = slides_placeholder

    slides = slides.with_columns(
        pl.concat_str(
            [
                pl.lit("[View Charts](/"),
                pl.lit("chartsview_"),
                pl.lit(bucket_name),
                pl.lit("/"),
                pl.col("slide_name"),
                pl.lit("/)"),
            ]
        ).alias("view_charts"),
        pl.concat_str(
            [
                pl.lit("[View Spots](/"),
                pl.lit("spotsview_"),
                pl.lit(bucket_name),
                pl.lit("/"),
                pl.col("slide_name"),
                pl.lit("/)"),
            ]
        ).alias("view_spots"),
        pl.concat_str(
            [
                pl.lit("[View FOVs](/"),
                pl.lit("fovsview_"),
                pl.lit(bucket_name),
                pl.lit("/"),
                pl.col("slide_name"),
                pl.lit("/)"),
            ]
        ).alias("view_fovs"),
    )
    return slides


# Create the image-(parasite output) grid layout
def create_image_grid_and_options_and_display_count(
    bucket_name, slide_name, start_index, end_index, sort_method, channel
):
    spot_imgs, scores, page_display = spot_images_and_scores_and_display_count(
        bucket_name, slide_name, start_index, end_index, sort_method
    )

    # spot_imgs = spot_imgs[0]
    # spot_imgs, scores = ([Image.open("assets/images/nautilus1_tiny.jpg")],[0.99])
    image_rows = []
    channel_options = []
    if len(spot_imgs) > 0:
        for k in spot_imgs[0].keys():
            if k == "spot_id":
                continue
            channel_options.append(k)
    selected_channel = channel
    if selected_channel not in channel_options:
        selected_channel = channel_options[0]
    for i in range(spot_rows_per_page):
        image_row_list = []
        for j in range(spot_columns_per_page):
            try:
                image_row_list.append(
                    dbc.Col(
                        dbc.Card(
                            dbc.CardBody(
                                [
                                    html.Img(
                                        src=spot_imgs[i * spot_columns_per_page + j][
                                            selected_channel
                                        ],
                                        style={
                                            "width": "100px",
                                            "image-rendering": "pixelated",
                                        },
                                    ),
                                    html.P(f"{scores[i*spot_columns_per_page+j]:.2f}"),
                                ]
                            ),
                            className="image-cell",
                        )
                    )
                )
            except:
                image_row_list.append(dbc.Col(dbc.Card()))
        image_rows.append(dbc.Row(image_row_list))

    image_grid = html.Div(image_rows, className="image-grid")
    return image_grid, channel_options, page_display


@cache.memoize(timeout=cache_timeout)
def combined_spots_df(bucket_name, slide_name):
    return get_combined_spots_df(bucket_name, gcs, slide_name)


@cache.memoize(timeout=cache_timeout)
def spots_pred_csv_cached(bucket_name, slide_name):
    spot_df = get_spots_csv(bucket_name, gcs, slide_name)
    return spot_df


@cache.memoize(timeout=cache_timeout)
def spots_mapping_csv_cached(bucket_name, slide_name):
    mapping_df = get_mapping_csv(bucket_name, gcs, slide_name)
    return mapping_df


def spot_images_and_scores_and_display_count(
    bucket_name,
    slide_name,
    id_start,
    id_end,
    sort_method,
    embed_preferred=True,
    zarr_fallback=True,
    rel_path_to_embeds_in_slide="version3/",
    rel_path_to_zarr_in_slide="version1/spot_images.zip",
):
    try:
        spot_df = combined_spots_df(bucket_name, slide_name)
        spot_count = spot_df.select(pl.count()).item()
        if sort_method in ["ascending", "descending"]:
            descending = True
            if sort_method == "ascending":
                descending = False
            spot_df = (
                spot_df.lazy()
                .sort(pl.col("parasite output"), descending=descending)
                .with_row_count()
                .filter(
                    (pl.col("row_nr") >= pl.lit(id_start))
                    & (pl.col("row_nr") <= pl.lit(id_end))
                )
                .collect(streaming=True)
            )
        else:
            spot_df = (
                spot_df.lazy()
                .filter(
                    (pl.col("index") >= pl.lit(id_start))
                    & (pl.col("index") <= pl.lit(id_end))
                )
                .collect(streaming=True)
            )
        spot_ids = spot_df["index"].to_list()
        scores = spot_df["parasite output"].to_numpy()
    except:
        spot_df = spots_mapping_csv_cached(bucket_name, slide_name)
        spot_count = spot_df.select(pl.count()).item()
        spot_df = (
            spot_df.lazy()
            .with_row_count()
            .filter(
                (pl.col("row_nr") >= pl.lit(id_start))
                & (pl.col("row_nr") <= pl.lit(id_end))
            )
            .collect(streaming=True)
        )
        spot_ids = spot_df["row_nr"].to_list()
        scores = np.zeros(len(spot_df))
    if embed_preferred:
        try:
            spot_dir_path = os.path.join(slide_name, rel_path_to_embeds_in_slide)
            spot_imgs = get_spot_embeds(bucket_name, spot_dir_path ,spot_ids)
            print("got spot image embeds") 
            page_display_string =  (
                str(id_start) + "-" + str(id_end) + " of " + str(spot_count)
            )
            return (spot_imgs, scores, page_display_string)
        except:
            pass
    if zarr_fallback:
        try:
            zarr_path = (
                bucket_name.strip("/")
                + "/"
                + os.path.join(slide_name, rel_path_to_zarr_in_slide)
            )
            spot_imgs = get_spots_from_zarr(zarr_path, spot_ids)

            page_display_string = (
                str(id_start) + "-" + str(id_end) + " of " + str(spot_count)
            )
            return (spot_imgs, scores, page_display_string)
        except:  # default to cropping from jpeg
            pass
    spot_coords = []
    for spot in spot_df.rows(named=True):
        spot_coords.append(
            (
                spot["FOV_row"],
                spot["FOV_col"],
                spot["FOV_z"],
                spot["x"],
                spot["y"],
                spot["r"],
            )
        )
    spot_imgs = crop_spots_from_slide(
        storage_service, bucket_name, slide_name, spot_coords
    )
    for spot_id, image_index in zip(spot_ids, range(len(spot_imgs))):
        spot_imgs[image_index] = {"spot_id": spot_id, "compose": spot_imgs[image_index]}
    page_display_string = str(id_start) + "-" + str(id_end) + " of " + str(spot_count)
    return (spot_imgs, scores, page_display_string)


# Define the layout
spot_table_layout = html.Div(
    [
        html.H1("Spot Display"),
        html.P("", id="bucket-name-spots", title=""),
        html.P("", id="slide-name-spots", title=""),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Input(
                        id="pagination",
                        type="text",
                        debounce=True,
                        placeholder="Enter page number",
                    )
                ),
                dbc.Col(html.P(children=[""], id="no-slides-indicator")),
            ]
        ),
        dbc.Row(
            [
                dbc.Col(
                    dcc.Dropdown(
                        id="spot-image-channel-dropdown",
                        placeholder="Select Channel",
                        options=["compose"],
                        value="compose",
                        clearable=False,
                    )
                ),
                dbc.Col(
                    dcc.Dropdown(
                        id="spot-image-sorting-dropdown",
                        placeholder="Select Sorting Method",
                        options=["no sort", "ascending", "descending"],
                        value="no sort",
                        clearable=False,
                    )
                ),
            ]
        ),
        html.Div(id="image-grid-container"),
    ]
)


# Callback to update the displayed items based on pagination
@app.callback(
    Output("image-grid-container", "children"),
    Output("spot-image-channel-dropdown", "options"),
    Output("no-slides-indicator", "children"),
    Input("bucket-name-spots", "title"),
    Input("slide-name-spots", "title"),
    Input("pagination", "value"),
    Input("spot-image-sorting-dropdown", "value"),
    Input("spot-image-channel-dropdown", "value"),
)
def update_image_grid(bucket_name, slide_name, page, sort_method, channel):
    try:
        page = int(page)
    except (ValueError, TypeError):
        page = 1
    start_index = (page - 1) * spots_per_page
    end_index = page * spots_per_page
    return create_image_grid_and_options_and_display_count(
        bucket_name, slide_name, start_index, end_index, sort_method, channel
    )


# Define the layout of the chart page
graph_layout = html.Div(
    [
        html.H1("Prediction stats vs. Threshold"),
        dcc.Dropdown(
            id="y-axis-dropdown",
            multi=True,  # Allow multiple selections
            placeholder="Select Y-Axis",
        ),
        dcc.Graph(id="line-plot"),
    ]
)


@cache.memoize(timeout=cache_timeout)
def get_plot_df(bucket_name, slide_name):
    spot_df = get_spots_csv(bucket_name, gcs, slide_name)
    thresholds = np.linspace(0.0, 1.0, 200, endpoint=False)
    plot_df = get_detection_stats_vs_threshold(spot_df, thresholds)
    spot_count = len(spot_df)
    plot_df = plot_df.with_columns(
        (pl.col("predicted_positive") / pl.lit(spot_count)).alias("positive_rate"),
        (pl.col("predicted_negative") / pl.lit(spot_count)).alias("negative_rate"),
    )
    if plot_df["total_annotated_positive_negative"].item(0) > 0:
        plot_df = plot_df.with_columns(
            (
                pl.col("false_positive") / pl.col("total_annotated_positive_negative")
            ).alias("false_positive_rate"),
            (
                pl.col("false_negative") / pl.col("total_annotated_positive_negative")
            ).alias("false_negative_rate"),
        )

    plot_df = plot_df.to_pandas()

    return plot_df


# Define the callback function to update the line graph
@app.callback(Output("line-plot", "figure"), Input("y-axis-dropdown", "value"))
def update_line_plot(selected_y_columns):
    if selected_y_columns is None or len(selected_y_columns) == 0:
        return go.Figure()

    page_name = selected_y_columns[0].split("<>")[-1]
    bucket_name = selected_y_columns[0].split("<>")[-2]

    for i in range(len(selected_y_columns)):
        selected_y_columns[i] = selected_y_columns[i].split("<>")[0]

    plot_df = get_plot_df(bucket_name, page_name)

    fig = go.Figure()
    for column in selected_y_columns:
        fig.add_trace(
            go.Scatter(
                x=plot_df["threshold"],
                y=plot_df[column],
                mode="lines+markers",
                name=column,
            )
        )

    fig.update_layout(
        title="Plot of prediction stats for " + str(page_name) + " vs. Threshold",
        xaxis_title="Threshold",
        yaxis_title="Value",
    )

    return fig


app._favicon = "/assets/favicon.ico"

# TODO: Create dynamic title that changes based on the slide name
app.title = "Cephla - Nautilus Dashboard"

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content"),
    ]
)

# TODO: Display data from populate_slide_rows on cell/row select
index_page = html.Div(
    [
        html.H1("Cephla"),
        html.H2("Nautilus Dashboard"),
        dcc.Dropdown(
            id="bucket-name-dropdown",
            placeholder="Select Bucket",
            options=bucket_names,
            value=bucket_names[0],
            clearable=False,
        ),
        dash_table.DataTable(
            id="slides-table",
            # set view_fovs to display as markdown
            # Allows creation of a link to the FOVs page
            columns=[
                {"id": i, "name": i, "presentation": "markdown"}
                if i in ["view_fovs", "view_charts", "view_spots"]
                else {"name": i, "id": i, "selectable": True, "editable": True}
                if i == "threshold"
                else {"name": i, "id": i, "selectable": True}
                for i in slides_placeholder.columns
            ],
            data=slides_placeholder.to_pandas().to_dict("records"),
            # row_selectable="single",
            style_table={"overflowX": "scroll"},
            style_cell={
                "height": "auto",
                "minWidth": "0px",
                "maxWidth": "180px",
                "whiteSpace": "normal",
                "textAlign": "left",
                "overflow-y": "hidden",
            },
            # Show selected cell in blue
            style_data_conditional=[
                {
                    "if": {"state": "selected"},
                    "backgroundColor": "rgba(0, 116, 217, 0.3)",
                    "border": "1px solid blue",
                }
            ],
            tooltip_data=[
                {"slide_name": {"value": row["slide_name"], "type": "markdown"}}
                for row in slides_placeholder.rows(named=True)
            ],
            tooltip_duration=None,
            filter_action="native",
            sort_action="native",
            sort_mode="multi",
            # column_selectable="single",
            row_selectable="single",
            selected_columns=[],
            selected_rows=[],
            page_action="native",
            page_current=0,
            page_size=slides_per_page,
        ),
    ]
)


@app.callback(
    Output("slides-table", "data"),
    Output("slides-table", "columns"),
    Output("slides-table", "tooltip_data"),
    [
        Input("bucket-name-dropdown", "value"),
        Input("slides-table", "selected_rows"),
        Input("slides-table", "data"),
        Input("slides-table", "columns"),
        Input("slides-table", "tooltip_data"),
    ],
)
def update_slide_df_master(bucket_name, selected_rows, data, columns, tooltip_data):
    ctx = callback_context
    if ctx.triggered[0]["prop_id"] == ".":
        ret_data, ret_columns, ret_tooltip_data = switch_bucket(bucket_names[0])
        return ret_data, ret_columns, ret_tooltip_data
    if ctx.triggered[0]["prop_id"].split(".")[0] == "bucket-name-dropdown":
        ret_data, ret_columns, ret_tooltip_data = switch_bucket(bucket_name)
        return ret_data, ret_columns, ret_tooltip_data
    else:
        ret_data = update_slide_data(bucket_name, selected_rows, data)
        return ret_data, columns, tooltip_data


def switch_bucket(bucket_name):
    slides = slide_df_cached(bucket_name, cutoff)

    # drop cols which are all null from slides
    slides = slides[[s.name for s in slides if not (s.null_count() == slides.height)]]

    # Allows creation of a link to the FOVs page
    columns = [
        {"id": i, "name": i, "presentation": "markdown"}
        if i in ["view_fovs", "view_charts", "view_spots"]
        else {"name": i, "id": i, "selectable": True, "editable": True}
        if i == "threshold"
        else {"name": i, "id": i, "selectable": True}
        for i in slides.columns
    ]
    data = slides.to_pandas().to_dict("records")
    tooltip_data = [
        {"slide_name": {"value": row["slide_name"], "type": "markdown"}}
        for row in slides.rows(named=True)
    ]
    return (data, columns, tooltip_data)


def update_slide_data(bucket_name, selected_rows, data):
    if len(selected_rows) == 0:
        return data
    try:
        threshold = float(data[selected_rows[0]]["threshold"])
    except:
        threshold = 0.876
    if threshold < 0:
        threshold = 0
    if threshold > 1:
        threshold = 1
    relevant_row = data[selected_rows[0]]
    relevant_row["threshold"] = threshold
    spot_df = get_spots_csv(bucket_name, gcs, relevant_row["slide_name"])
    try:
        results = get_results_from_threshold(spot_df, threshold)
        for k in results.keys():
            relevant_row[k] = results[k]
    except:
        print(
            "failed to get prediction results for slide " + relevant_row["slide_name"]
        )
    try:
        relevant_row["positives/5M rbc"] = int(
            relevant_row["predicted_positive"] * (5e6) / int(relevant_row["rbcs"])
        )
    except:
        print("failed to divide by rbcs for slide " + relevant_row["slide_name"])
    data[selected_rows[0]] = relevant_row
    return data


# # Define the callback to update page-content based on the URL
@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    # view individual FOV
    if pathname and pathname[-5:] == ".jpg/":
        # get bucket name from the URL
        bucket_name = pathname.split("/")[-5]
        # get the slide name from the URL
        page_name = pathname.split("/")[-4]
        # get the image name from the URL
        image_name = pathname.split("/")[-2]
        # get the image from GCS
        image = get_image(
            storage_service, bucket_name, page_name, image_name, resize_factor=1.0
        )
        # # display the image
        return html.Div(
            [
                html.H1(f"FOV {image_name} from slide: {page_name}"),
                html.Br(),
                html.Img(
                    src=image,
                ),
            ]
        )
    # FOVs page
    elif pathname and pathname != "/" and "fovsview_" in pathname:
        page_name = pathname.split("/")[-2]  # extract slide name
        bucket_name = pathname.split("/")[-3].replace(
            "fovsview_", ""
        )  # Extract the bucket name from the URL
        # Dynamically create the content based on the page number
        fovs_df = get_fovs_df(client, bucket_name, [page_name])

        fovs_df = fovs_df.with_columns(
            pl.concat_str(
                [
                    pl.lit("[View FOV](/"),
                    pl.col("image_uri"),  # get the image name from the uri
                    pl.lit("/)"),
                ]
            ).alias("view_fov")
        )
        page_content = html.Div(
            [
                html.H1(f"FOVs from slide: {page_name}"),
                html.Div(
                    [
                        # FOVs table
                        dash_table.DataTable(
                            id="fovs-table",
                            columns=[
                                {"id": i, "name": i, "presentation": "markdown"}
                                if i == "view_fov"
                                else {"name": i, "id": i}
                                for i in fovs_df.columns
                            ],
                            data=fovs_df.to_pandas().to_dict("records"),
                            selected_rows=[1, 2, 3, 4],
                            style_table={"overflowX": "scroll"},
                            style_cell={
                                "height": "auto",
                                "minWidth": "0px",
                                "maxWidth": "180px",
                                "whiteSpace": "normal",
                            },
                            # Show selected cell in blue
                            style_data_conditional=[
                                {
                                    "if": {"state": "selected"},
                                    "backgroundColor": "rgba(0, 116, 217, 0.3)",
                                    "border": "1px solid blue",
                                }
                            ],
                            # show FOV image in tooltip, on hover over the image_uri column
                            # tooltip_data=[
                            #     {
                            #         "image_uri": {
                            #             # "value": "![Slide Image]({})".format(
                            #             #     row[
                            #             #         "image_uri"
                            #             #     ]  # directly embedding from google, images are too big for tooltips
                            #             #     # dash.get_relative_path(row["image_uri"])
                            #             # ),
                            #             "value": html.Img(
                            #                 src=get_image(
                            #                     storage_service,
                            #                     bucket_name,
                            #                     page_name,
                            #                     fovs_df["image_uri"][0].split("/")[-1],
                            #                     resize_factor=0.1,
                            #                 ),
                            #             ),
                            #             "type": "markdown",
                            #         }
                            #     }
                            #     for row in fovs_df.to_pandas().to_dict("records")
                            # ],
                            # tooltip_duration=None,
                            # tooltip_delay=None,
                        ),
                    ]
                ),
            ]
        )
        return page_content
    elif pathname and pathname != "/" and "chartsview_" in pathname:
        bucket_name = pathname.split("/")[-3].replace("chartsview_", "")
        page_name = pathname.split("/")[-2]  # extract slide name
        plot_df = get_plot_df(bucket_name, page_name)

        page_content = graph_layout
        columns = list(plot_df.columns)
        column_options = [
            {"label": col, "value": col + "<>" + bucket_name + "<>" + page_name}
            for col in columns
        ]
        page_content.children[1].options = column_options
        return page_content
    elif pathname and pathname != "/" and "spotsview_" in pathname:
        bucket_name = pathname.split("/")[-3].replace("spotsview_", "")
        page_name = pathname.split("/")[-2]  # extract slide name
        page_layout = spot_table_layout
        page_layout.children[1] = html.P(
            bucket_name, id="bucket-name-spots", title=bucket_name
        )
        page_layout.children[2] = html.P(
            page_name + " Spots", id="slide-name-spots", title=page_name
        )
        return page_layout
    else:
        return index_page


if __name__ == "__main__":
    app.run(debug=debug)
