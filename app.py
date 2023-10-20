from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash
import pandas as pd
import polars as pl
import datetime as dt
import plotly.express as px

spots = pl.DataFrame(
    {
        "slide_label": ["1", "1", "2", "2"],
        "timestamp": [
            dt.datetime(2022, 1, 1, 12, 0, 0),
            dt.datetime(2022, 1, 1, 12, 1, 0),
            dt.datetime(2022, 1, 1, 12, 0, 0),
            dt.datetime(2022, 1, 1, 12, 1, 0),
        ],
        "fov_id": [1, 2, 1, 2],
        "spot_id": [1, 2, 3, 4],
        "score": [0.5, 0.6, 0.7, 0.8],
        # "coords_in_fov": [(10, 20), (0, 0), (30, 40), (50, 60)],
    }
)

spots_images = pl.DataFrame(
    {
        "slide_label": ["1", "1", "2", "2"],
        "timestamp": [
            dt.datetime(2022, 1, 1, 12, 0, 0),
            dt.datetime(2022, 1, 1, 12, 1, 0),
            dt.datetime(2022, 1, 1, 12, 0, 0),
            dt.datetime(2022, 1, 1, 12, 1, 0),
        ],
        "spot_id": [1, 2, 3, 4],
        "image_label": ["image1", "image2", "image3", "image4"],
        "image_uri": [
            "images/nautilus1.jpg",
            "images/nautilus1.jpg",
            "images/nautilus1.jpg",
            "images/nautilus1.jpg",
        ],
    }
)


# Create the Dash app
app = Dash(__name__)
# set the title
app.title = "Nautilus Dashboard"
# set icon
# app._favicon = "favicon.ico"

# Join spots and spots_images dataframes on spot_id
spots = spots.join(spots_images, on="spot_id", validate="1:1")

# Define the layout of the app
app.layout = html.Div(
    [
        dash_table.DataTable(
            id="table",
            columns=[{"name": i, "id": i} for i in spots.columns],
            data=spots.to_pandas().to_dict("records"),
            row_selectable="single",
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
            # TODO: Fix so that image path is determined by value in cell
            tooltip_data=[
                {
                    "image_uri": {
                        "value": "![Nautilus]({})".format(
                            dash.get_relative_path("/assets/images/nautilus1_tiny.jpg")
                        ),
                        "type": "markdown",
                    }
                }
                for row in spots.to_pandas().to_dict("records")
            ],
            tooltip_duration=None,
            tooltip_delay=None,
        )
    ]
)
if __name__ == "__main__":
    app.run_server(debug=True)
