from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash
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
)
from utils.polars_helpers import get_detection_stats_vs_threshold
import polars as pl
from gcsfs import GCSFileSystem
from PIL import Image
from io import BytesIO

# Parse in key and bucket name from config file
cfp = ConfigParser()
cfp.read("config.ini")

service_account_key_json = cfp["GCS"]["gcs_storage_key"]
gs_url = cfp["GCS"]["bucket_url"]

cutoff = None
try:
    cutoff = int(cfp["TESTING"]["slide_count_cutoff"])
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


slides = pl.DataFrame(
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


slides = get_initial_slide_df_with_predictions_only(
    client, bucket_name, gcs, cutoff=cutoff
)

plot_df = None

# add a column for viewing FOVs
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


slides = slides[[s.name for s in slides if not (s.null_count() == slides.height)]]

# drop cols which are all null from slides
# slides = slides[[s.name for s in slides if not (s.null_count() == slides.height)]]

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
    app = Dash(__name__)
else:
    app = Dash(__name__, update_title=None, suppress_callback_exceptions=True)


# Define the layout of the web page
graph_layout = html.Div(
    [
        html.H1("Line Graph of Columns vs. Threshold"),
        dcc.Dropdown(
            id="y-axis-dropdown",
            multi=True,  # Allow multiple selections
            placeholder="Select Y-Axis",
        ),
        dcc.Graph(id="line-plot"),
    ]
)


# Define the callback function to update the line graph
@app.callback(Output("line-plot", "figure"), [Input("y-axis-dropdown", "value")])
def update_line_plot(selected_y_columns):
    if selected_y_columns is None:
        return go.Figure()

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
        title="Line Graph of Columns vs. Threshold",
        xaxis_title="Threshold",
        yaxis_title="Value",
    )

    return fig


# TODO: Create dynamic title that changes based on the slide name
app.title = "Nautilus Dashboard"

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content"),
    ]
)

# TODO: Display data from populate_slide_rows on cell/row select
index_page = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        dash_table.DataTable(
            id="slides-table",
            # set view_fovs to display as markdown
            # Allows creation of a link to the FOVs page
            columns=[
                {"id": i, "name": i, "presentation": "markdown"}
                if i in ["view_fovs", "view_charts", "view_spots"]
                else {"name": i, "id": i, "editable": True}
                if i == "threshold"
                else {"name": i, "id": i}
                for i in slides.columns
            ],
            data=slides.to_pandas().to_dict("records"),
            # row_selectable="single",
            selected_rows=[],
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
        ),
    ]
)


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
                            selected_rows=[],
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
        spot_df = get_spots_csv(bucket_name, gcs, page_name)
        thresholds = np.linspace(0.0, 1.0, 200, endpoint=False)
        global plot_df
        plot_df = get_detection_stats_vs_threshold(spot_df, thresholds)
        spot_count = len(spot_df)
        plot_df = plot_df.with_columns(
            (pl.col("predicted_positive") / pl.lit(spot_count)).alias("positive_rate"),
            (pl.col("predicted_negative") / pl.lit(spot_count)).alias("negative_rate"),
        )
        if plot_df["total_annotated_positive_negative"].item(0) > 0:
            plot_df = plot_df.with_columns(
                (
                    pl.col("false_positive")
                    / pl.col("total_annotated_positive_negative")
                ).alias("false_positive_rate"),
                (
                    pl.col("false_negative")
                    / pl.col("total_annotated_positive_negative")
                ).alias("false_negative_rate"),
            )

        plot_df = plot_df.to_pandas()

        columns = list(plot_df.columns)
        column_options = [{"label": col, "value": col} for col in columns]
        graph_layout.children[1].options = column_options
        return graph_layout
    else:
        return index_page


if __name__ == "__main__":
    app.run_server(debug=debug)
