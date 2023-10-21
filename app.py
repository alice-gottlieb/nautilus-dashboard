from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash
import pandas as pd
import polars as pl
import datetime as dt
import plotly.express as px
from google.cloud import storage
from configparser import ConfigParser
from google.oauth2 import service_account
from googleapiclient.discovery import build
from utils.demo_io import (
    get_initial_slide_df,
    get_fovs_df,
    get_top_level_dirs,
    populate_slide_rows,
    get_histogram_df,
    get_image,
)
import polars as pl
from gcsfs import GCSFileSystem
from PIL import Image
from io import BytesIO

# Parse in key and bucket name from config file
cfp = ConfigParser()
cfp.read("config.ini")

service_account_key_json = cfp["GCS"]["gcs_storage_key"]
gs_url = cfp["GCS"]["bucket_url"]

bucket_name = gs_url.replace("gs://", "")

# Define GCS file system so files can be read
gcs = GCSFileSystem(token=service_account_key_json)

# Authenticate using the service account key file
credentials = service_account.Credentials.from_service_account_file(
    service_account_key_json, scopes=["https://www.googleapis.com/auth/cloud-platform"]
)

# Create a storage client
storage_service = build("storage", "v1", credentials=credentials)


debug = True
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


slides = get_initial_slide_df(storage_service, bucket_name, gcs)

# add a column for viewing FOVs
# leave this in even after using get_initial_slide_df for the slides table
slides = slides.with_columns(
    pl.concat_str(
        [
            pl.lit("[View FOVs](/"),
            pl.col("slide_name"),
            pl.lit("/)"),
        ]
    ).alias("view_fovs")
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
    app = Dash(__name__)
else:
    app = Dash(__name__, update_title=None)

# TODO: Create dynamic title that changes based on the slide name
app.title = "Nautilus Dashboard"

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content"),
    ]
)

index_page = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        dash_table.DataTable(
            id="slides-table",
            # set view_fovs to display as markdown
            # Allows creation of a link to the FOVs page
            columns=[
                {"id": i, "name": i, "presentation": "markdown"}
                if i == "view_fovs"
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
    # if pathname.startswith("/image"):
    #     image_uri = pathname.split("/image/")[1]
    #     return html.Div(
    #         [
    #             html.Img(src=image_uri),  # Display the image
    #             html.Br(),
    #             dcc.Link(
    #                 "Go back to table", href="/"
    #             ),  # Link to go back to the DataTable
    #         ]
    #     )
    # make sure pathname is not None or pointing to index
    if pathname and pathname != "/":
        page_name = pathname.split("/")[-2]  # Extract the slide name from the URL
        # Dynamically create the content based on the page number
        fovs_df = get_fovs_df(storage_service, bucket_name, [page_name])
        page_content = html.Div(
            [
                html.H1(f"FOVs from slide: {page_name}"),
                html.Div(
                    [
                        # FOVs table
                        dash_table.DataTable(
                            id="fovs-table",
                            columns=[{"name": i, "id": i} for i in fovs_df.columns],
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
                            #         column: {
                            #             "value": dcc.Link(
                            #                 html.Img(
                            #                     src=row[
                            #                         "image_uri"
                            #                     ],  # Link to your compressed image
                            #                     style={
                            #                         "width": "100px",
                            #                         "height": "100px",
                            #                     },  # Adjust width and height
                            #                 ),
                            #                 href=row[
                            #                     "image_uri"
                            #                 ],  # Link to the detailed image
                            #             ),
                            #             "type": "text",  # Display as text (not markdown)
                            #         }
                            #         for column in fovs_df.columns
                            #     }
                            #     for row in fovs_df.to_dict("records")
                            # ],
                            # tooltip_duration=None,
                            # tooltip_delay=None,
                        ),
                    ]
                ),
                html.Br(),
                # display image
                html.Img(
                    src=get_image(
                        storage_service,
                        bucket_name,
                        page_name,
                        fovs_df["image_uri"][0].split("/")[-1],
                        resize_factor=0.1,
                    ),
                ),
            ]
        )
        return page_content
    else:
        return index_page


if __name__ == "__main__":
    app.run_server(debug=debug)
