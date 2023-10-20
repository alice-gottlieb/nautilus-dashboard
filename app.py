from dash import Dash, html, dash_table, dcc, callback, Output, Input
import dash
import pandas as pd
import polars as pl
import datetime as dt
import plotly.express as px

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
        "image_uri": [
            "/assets/images/nautilus1_tiny.jpg",
            "/assets/images/nautilus1_small.jpg",
        ],
    }
)


# Create the Dash app
app = Dash(__name__)
# set the title
app.title = "Nautilus Dashboard"
# set icon
# app._favicon = "favicon.ico"

# Define the layout of the app
app.layout = html.Div(
    [
        dash_table.DataTable(
            id="slides-table",
            columns=[{"name": i, "id": i} for i in slides.columns],
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
            tooltip_data=[
                {
                    "image_uri": {
                        "value": "![Nautilus]({})".format(
                            dash.get_relative_path(row["image_uri"])
                        ),
                        "type": "markdown",
                    }
                }
                for row in slides.to_pandas().to_dict("records")
            ],
            tooltip_duration=None,
            tooltip_delay=None,
        ),
    ]
)
if __name__ == "__main__":
    app.run_server(debug=True)
