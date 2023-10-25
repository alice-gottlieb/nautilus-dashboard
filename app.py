from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash
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
    get_fovs_df,
    get_top_level_dirs,
    populate_slide_rows,
    get_histogram_df,
    get_image,
    get_spots_csv,
    crop_spots_from_slide,
    get_combined_spots_df,
)
from utils.polars_helpers import (
    get_detection_stats_vs_threshold,
    get_results_from_threshold,
)
import polars as pl
from gcsfs import GCSFileSystem
from PIL import Image
from io import BytesIO
from flask_caching import Cache

# Parse in key and bucket name from config file
cfp = ConfigParser()
cfp.read("config.ini")

service_account_key_json = cfp["GCS"]["gcs_storage_key"]
gs_url = cfp["GCS"]["bucket_url"]


slide_df_cache_file = "slide_df_cache/slides.csv"
try:
    slide_df_cache_file = cfp["SLIDES"]["slide_df_cache_file"]
except:
    pass

cutoff = None
try:
    cutoff = int(cfp["TESTING"]["slide_count_cutoff"])
except:
    pass

spot_rows_per_page = 2
spot_columns_per_page = 5

try:
    spots_rows_per_page = int(cfp["DISPLAY"]["spot_rows_per_page"])
except:
    pass
try:
    spots_columns_per_page = int(cfp["DISPLAY"]["spot_columns_per_page"])
except:
    pass


spots_per_page = spot_rows_per_page * spot_columns_per_page

cache_timeout = 20
try:
    cache_timeout = int(cfp["DISPLAY"]["cache_timeout"])
except:
    pass

bucket_name = gs_url.replace("gs://", "")

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

cache = Cache(
    app.server, config={"CACHE_TYPE": "filesystem", "CACHE_DIR": "cache-directory"}
)


@cache.memoize(timeout=cache_timeout)
def slide_df_cached(my_cutoff=None):
    slides = None
    try:
        slides = pl.read_csv(slide_df_cache_file)
    except:
        slides = get_initial_slide_df_with_predictions_only(
            client, bucket_name, gcs, cutoff=my_cutoff
        )
    slides = slides.with_columns(
        (pl.col("predicted_positive") * (5e6) / pl.col("rbcs"))
        .cast(pl.Int64)
        .alias("positives/5M rbc")
    )

    # add a column for viewing FOVs/spots/charts
    # leave this in even after using get_initial_slide_df for the slides table
    slides = slides.with_columns(
        pl.concat_str(
            [
                pl.lit("[View Charts](/"),
                pl.lit("chartsview_"),
                pl.col("slide_name"),
                pl.lit("/)"),
            ]
        ).alias("view_charts"),
        pl.concat_str(
            [
                pl.lit("[View Spots](/"),
                pl.lit("spotsview_"),
                pl.col("slide_name"),
                pl.lit("/)"),
            ]
        ).alias("view_spots"),
        pl.concat_str(
            [
                pl.lit("[View FOVs](/"),
                pl.lit("fovsview_"),
                pl.col("slide_name"),
                pl.lit("/)"),
            ]
        ).alias("view_fovs"),
    )
    return slides


# Create the image-(parasite output) grid layout
def create_image_grid(slide_name, start_index, end_index, descending=True):
    spot_imgs, scores = spot_images_and_scores(
        slide_name, start_index, end_index, descending
    )

    spot_imgs = spot_imgs[0]
    # spot_imgs, scores = ([Image.open("assets/images/nautilus1_tiny.jpg")],[0.99])
    image_rows = []
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
                                        src=spot_imgs[i * spot_columns_per_page + j],
                                        style={
                                            "width": "200px",
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
    return image_grid


@cache.memoize(timeout=cache_timeout)
def combined_spots_df(slide_name):
    return get_combined_spots_df(bucket_name, gcs, slide_name)


@cache.memoize(timeout=cache_timeout)
def spot_images_and_scores(slide_name, id_start, id_end, descending=True):
    spot_df = combined_spots_df(slide_name)
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
    spot_imgs = (
        crop_spots_from_slide(storage_service, bucket_name, slide_name, spot_coords),
    )
    scores = spot_df["parasite output"].to_numpy()
    return (spot_imgs, scores)


# Define the layout
spot_table_layout = html.Div(
    [
        html.H1("Spot Display"),
        html.P("", id="slide-name-spots", title=""),
        html.Div(id="image-grid-container"),
        dcc.Input(
            id="pagination", type="text", debounce=True, placeholder="Enter page number"
        ),
    ]
)


# Callback to update the displayed items based on pagination
@app.callback(
    Output("image-grid-container", "children"),
    Input("slide-name-spots", "title"),
    Input("pagination", "value"),
)
def update_image_grid(slide_name, page):
    try:
        page = int(page)
    except (ValueError, TypeError):
        page = 1
    start_index = (page - 1) * spots_per_page
    end_index = page * spots_per_page
    return create_image_grid(slide_name, start_index, end_index)


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
def get_plot_df(slide_name):
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
@app.callback(Output("line-plot", "figure"), [Input("y-axis-dropdown", "value")])
def update_line_plot(selected_y_columns):
    if selected_y_columns is None or len(selected_y_columns) == 0:
        return go.Figure()

    page_name = selected_y_columns[0].split("<>")[-1]

    for i in range(len(selected_y_columns)):
        selected_y_columns[i] = selected_y_columns[i].split("<>")[0]

    plot_df = get_plot_df(page_name)

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
        html.H3("Table Placeholder", id="slides-table"),
    ]
)


@app.callback(
    Output("slides-table", "data"),
    [Input("slides-table", "selected_rows"), Input("slides-table", "data")],
)
def update_slide_data(selected_rows, data):
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
    results = get_results_from_threshold(spot_df, threshold)
    for k in results.keys():
        relevant_row[k] = results[k]
    relevant_row["positives/5M rbc"] = int(
        relevant_row["predicted_positive"] * (5e6) / relevant_row["rbcs"]
    )
    data[selected_rows[0]] = relevant_row
    return data


# # Define the callback to update page-content based on the URL
@app.callback(Output("page-content", "children"), Input("url", "pathname"))
def display_page(pathname):
    # view individual FOV
    if pathname and pathname[-5:] == ".jpg/":
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
        page_name = pathname.split("/")[-2].strip(
            "fovsview_"
        )  # Extract the slide name from the URL
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
        page_name = pathname.split("/")[-2].strip("chartsview_")
        plot_df = get_plot_df(page_name)

        page_content = graph_layout
        columns = list(plot_df.columns)
        column_options = [
            {"label": col, "value": col + "<>" + page_name} for col in columns
        ]
        page_content.children[1].options = column_options
        return page_content
    elif pathname and pathname != "/" and "spotsview_" in pathname:
        page_name = pathname.split("/")[-2].strip("spotsview_")
        page_layout = spot_table_layout
        page_layout.children[1] = html.P(
            page_name + " Spots", id="slide-name-spots", title=page_name
        )
        return page_layout
    else:
        slides = slide_df_cached(cutoff)

        # drop cols which are all null from slides
        slides = slides[
            [s.name for s in slides if not (s.null_count() == slides.height)]
        ]

        page_layout = index_page

        page_layout.children[2] = dash_table.DataTable(
            id="slides-table",
            # set view_fovs to display as markdown
            # Allows creation of a link to the FOVs page
            columns=[
                {"id": i, "name": i, "presentation": "markdown"}
                if i in ["view_fovs", "view_charts", "view_spots"]
                else {"name": i, "id": i, "selectable": True, "editable": True}
                if i == "threshold"
                else {"name": i, "id": i, "selectable": True}
                for i in slides.columns
            ],
            data=slides.to_pandas().to_dict("records"),
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
                for row in slides.rows(named=True)
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
            page_size=50,
        )

        return page_layout


if __name__ == "__main__":
    app.run_server(debug=debug)
