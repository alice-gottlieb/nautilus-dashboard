from dash import Dash, dash_table
import pandas as pd

# Create a sample dataframe with 6 columns
df = pd.DataFrame(
    {
        "Column 1": [1, 2, 3, 4, 5],
        "Column 2": ["A", "B", "C", "D", "E"],
        "Column 3": [10.5, 20.5, 30.5, 40.5, 50.5],
        "Column 4": [True, False, True, False, True],
        "Column 5": ["X", "Y", "Z", "W", "V"],
        "Column 6": [100, 200, 300, 400, 500],
    }
)

# Create the Dash app
app = Dash(__name__)
# set the title
app.title = "Nautilus Dashboard"
# set icon
# app._favicon = "favicon.ico"

# Define the layout of the app
app.layout = dash_table.DataTable(
    id="table",
    columns=[{"name": i, "id": i} for i in df.columns],
    data=df.to_dict("records"),
)

# Run the app
if __name__ == "__main__":
    app.run_server(debug=True)
