from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash
import pandas as pd
import polars as pl
import datetime as dt
import plotly.express as px

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

# add a column for viewing FOVs
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
# set icon
# app._favicon = "favicon.ico"

app.layout = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        html.Div(id="page-content"),
    ]
)

# Define the layout of the app
index_page = html.Div(
    [
        dcc.Location(id="url", refresh=False),
        dash_table.DataTable(
            id="slides-table",
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
    if pathname and pathname != "/":
        page_name = pathname.split("/")[-2]  # Extract the page number from the URL
        # Dynamically create the content based on the page number
        page_content = html.Div(
            [
                html.H1(f"FOVs from slide: {page_name}"),
                html.Div(
                    [
                        # FOVs table
                        dash_table.DataTable(
                            id="fovs-table",
                            columns=[{"name": i, "id": i} for i in fovs.columns],
                            data=fovs.filter(
                                pl.col("slide_label") == page_name
                            )  # Replace this line with a call to get_fovs_from_slides
                            .to_pandas()
                            .to_dict("records"),
                            selected_rows=[],
                            style_table={"overflowX": "scroll"},
                            style_cell={
                                "height": "auto",
                                "minWidth": "0px",
                                "maxWidth": "180px",
                                "whiteSpace": "normal",
                            },
                            style_data_conditional=[
                                {
                                    "if": {"state": "selected"},
                                    "backgroundColor": "rgba(0, 116, 217, 0.3)",
                                    "border": "1px solid blue",
                                }
                            ],
                            tooltip_data=[
                                {
                                    "image_uri": {
                                        "value": "![Slide Image]({})".format(
                                            dash.get_relative_path(row["image_uri"])
                                        ),
                                        "type": "markdown",
                                    }
                                }
                                for row in fovs.to_pandas().to_dict("records")
                            ],
                            tooltip_duration=None,
                            tooltip_delay=None,
                        ),
                    ]
                ),
            ]
        )
        return page_content
    else:
        return index_page


if __name__ == "__main__":
    app.run_server(debug=debug)
