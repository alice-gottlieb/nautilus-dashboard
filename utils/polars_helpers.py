"""
Collection of helper functions for lazy-loading and collating CSV data
"""
import polars as pl
import urllib.request

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
    return urllib.request.urlretrieve(url,filename=savepath)[0]

def hist_expr_builder(column_name: str, ranges: list) -> pl.Expr:
    """
    :brief: Builds a pl.when(...).then(...).when(...).
        then(...)...otherwise(...).alias("hist_bin")
        expression for grouping rows into histogram bins
    :param column_name: Name of column to look at for values.
    :param
    """
    range_expr = pl.when(pl.col(column_name) != pl.col(column_name)).then([0.0,0.0])
    for start, end in ranges:
        range_expr = range_expr.when(pl.col(column_name).is_between(pl.lit(start),pl.lit(end),'left')).then([start,end])
    range_expr = range_expr.otherwise([0.0,0.0]).alias("hist_bin")
    return range_expr

def get_histogram_from_file(filepath, column_name, ranges):
    """
    :brief: Return a lazy-evaluable query that will return
        a dataframe where each column is a range with a single row corresponding
        to how many rows there were in the original dataframe with column in that
        range
    :param filepath: path to CSV
    :param column: column to generate histogram of
    :param ranges: list in form [(start1,end1),(start2,end2),...] of ranges,
        
    """
    q = (pl.scan_csv(filepath).select(pl.col(column_name),hist_expr_builder(column_name,ranges)).group_by("hist_bin").count())
    return q
