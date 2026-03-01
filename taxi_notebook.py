import marimo

__generated_with = "0.20.2"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import duckdb
    import pandas as pd
    import plotly.express as px

    return duckdb, mo


@app.cell
def _(duckdb):
    # Point this at the .duckdb file dlt created
    conn = duckdb.connect("taxi_pipeline.duckdb")
    return (conn,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SHOW ALL TABLES;
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM taxi_pipeline_dataset.rides LIMIT 1000
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT SUM(tip_amt) FROM taxi_pipeline_dataset.rides
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT 
            ROUND(COUNT(*) FILTER (WHERE Payment_Type = 'Credit') * 100.0 / COUNT(*), 2) AS credit_card_pct
        FROM taxi_pipeline_dataset.rides
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT 
            MIN(trip_pickup_date_time) AS start_date,
            MAX(trip_dropoff_date_time) AS end_date
        FROM taxi_pipeline_dataset.rides
        """,
        engine=conn
    )
    return


if __name__ == "__main__":
    app.run()
