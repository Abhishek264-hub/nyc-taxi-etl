# NYC Taxi ETL Pipeline

A memory-efficient ETL pipeline built with Python, pandas and PyArrow. It processes millions of NYC Yellow Taxi trip records in small batches, so it runs on a laptop with under 4 GB of RAM.

## What it does
- Extract: reads Parquet files in 200,000-row batches, loading only the columns it needs
- Transform: removes invalid trips, converts miles to km, calculates trip duration, adds pickup hour, and joins pickup zone names
- Load: writes cleaned data to Parquet (one file per month) and saves summary CSVs
- Logs row counts and run time for every file

## Outputs
- output/avg_fare_by_hour.csv
- output/top_10_pickup_zones.csv
- output/trips_per_day.csv

## How to run
1. Download the Yellow Taxi Parquet files and the Taxi Zone Lookup CSV from the NYC TLC Trip Record Data page. Put them in the project folder.
2. Set up the environment:
       python3 -m venv venv
       source venv/bin/activate
       pip install -r requirements.txt
3. Run: python etl.py

## Tech
Python, pandas, PyArrow
