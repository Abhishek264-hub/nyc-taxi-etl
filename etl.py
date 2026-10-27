import glob 
import os
import time
from datetime import datetime

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


INPUT_PATTERN = "yellow_tripdata_2026-*.parquet"
ZONE_FILE = "taxi_zone_lookup.csv"
OUTPUT_DIR = "output"
LOG_FILE = "log_file.txt"
BATCH_SIZE = 200_000
YEAR = 2026
COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "PULocationID"
]


def log_progress(message):
    timestamp = datetime.now().strftime("%Y-%b-%d-%H:%M:%S")
    line = f"{timestamp},{message}"
    print(line)
    with open (LOG_FILE, "a") as f:
        f.write(line + "\n")


def extract_zones():
    zones = pd.read_csv(ZONE_FILE, usecols =["LocationID", "Zone"])
    return zones.rename(columns={"Zone": "pickup_zone"})


def extract_batches(file):
    """Yeild the parquet file in small pandas batches (low memory)."""
    pf=pq.ParquetFile(file)
    for batch in pf.iter_batches(batch_size = BATCH_SIZE, columns = COLUMNS):
        yield batch.to_pandas()


def transform(df, zones):
    df = df.dropna(subset=COLUMNS)

    mask = (
        (df["trip_distance"] > 0)
        & (df["fare_amount"] > 0)
        & (df["tpep_pickup_datetime"].dt.year == YEAR)
    )
    df = df[mask].copy()

    df["trip_distance_km"] = (df["trip_distance"] * 1.60934).round(2)

    minutes = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60
    df["trip_duration_minutes"] = minutes.round(2)

    # remove impossible trips (zero/negative time, or longer than 6 hours)
    df = df[
        (df["trip_duration_minutes"] > 0) & (df["trip_duration_minutes"] < 360)
    ].copy()

    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date

    df = df.merge(zones, left_on="PULocationID", right_on="LocationID", how="left")
    df["pickup_zone"] = df["pickup_zone"].fillna("Unknown")
    df = df.drop(columns=["LocationID", "trip_distance"])
    return df


def update_summary(summary, df):
    """Add this batch's counts to running totals (so we never need all data at once)."""
    parts = {
        "fare_sum": df.groupby("pickup_hour")["fare_amount"].sum(),
        "fare_cnt": df.groupby("pickup_hour")["fare_amount"].count(),
        "zones": df.groupby("pickup_zone").size(),
        "days": df.groupby("pickup_date").size(),
    }
    for key, part in parts.items():
        if key in summary:
            summary[key] = summary[key].add(part, fill_value=0)
        else:
            summary[key] = part


# ---------- LOAD ----------
def load_batch(writer, df, out_path):
    table = pa.Table.from_pandas(df, preserve_index=False)
    if writer is None:
        writer = pq.ParquetWriter(out_path, table.schema)  # overwrites old file
    writer.write_table(table)
    return writer


def load_summary(summary):
    avg_fare = (summary["fare_sum"] / summary["fare_cnt"]).round(2)
    avg_fare = avg_fare.rename("avg_fare").rename_axis("pickup_hour").reset_index()
    avg_fare.to_csv(f"{OUTPUT_DIR}/avg_fare_by_hour.csv", index=False)

    top_zones = (
        summary["zones"].astype(int).sort_values(ascending=False).head(10)
        .rename("trips").rename_axis("pickup_zone").reset_index()
    )
    top_zones.to_csv(f"{OUTPUT_DIR}/top_10_pickup_zones.csv", index=False)

    days = (
        summary["days"].astype(int).sort_index()
        .rename("trips").rename_axis("pickup_date").reset_index()
    )
    days.to_csv(f"{OUTPUT_DIR}/trips_per_day.csv", index=False)

    print("\nTop 10 pickup zones:")
    print(top_zones.to_string(index=False))


# ---------- MAIN ----------
def main():
    if not os.path.exists(ZONE_FILE):
        raise SystemExit(f"Missing {ZONE_FILE}. Download it from the TLC page first.")

    files = sorted(glob.glob(INPUT_PATTERN))
    if not files:
        raise SystemExit(f"No files match {INPUT_PATTERN}")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    job_start = time.time()
    log_progress("ETL Job Started")

    zones = extract_zones()
    summary = {}
    grand_in = grand_out = 0

    for file in files:
        month = file.replace("yellow_tripdata_", "").replace(".parquet", "")
        out_path = f"{OUTPUT_DIR}/clean_{month}.parquet"
        log_progress(f"Started {file}")
        file_start = time.time()

        writer = None
        rows_in = rows_out = 0

        for raw in extract_batches(file):          # Extract
            rows_in += len(raw)
            clean = transform(raw, zones)          # Transform
            rows_out += len(clean)
            if clean.empty:
                continue
            update_summary(summary, clean)
            writer = load_batch(writer, clean, out_path)   # Load

        if writer is not None:
            writer.close()

        grand_in += rows_in
        grand_out += rows_out
        seconds = round(time.time() - file_start, 1)
        log_progress(
            f"Finished {file}: rows_in={rows_in}, dropped={rows_in - rows_out}, "
            f"rows_out={rows_out}, seconds={seconds}"
        )

    if not summary:
        raise SystemExit("No valid rows found, nothing to summarize.")

    load_summary(summary)
    total = round(time.time() - job_start, 1)
    log_progress(f"ETL Job Ended: total_in={grand_in}, total_out={grand_out}, seconds={total}")


if __name__ == "__main__":
    main()