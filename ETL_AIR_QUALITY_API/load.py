# load.py
"""
Load transformed air quality CSV into Supabase table `air_quality_data`.

Behavior:
 - Reads data/staged/air_quality_transformed.csv
 - Batch inserts rows (batch_size=200)
 - Converts NaN -> None
 - Datetimes converted to ISO strings
 - Retries failed batches up to 2 retries
 - Prints summary of inserted rows
Requirements:
 - Set SUPABASE_URL and SUPABASE_KEY in environment (or .env)
"""
from __future__ import annotations

import os
import time
import math
import csv
from typing import List, Dict, Any

import pandas as pd
from dotenv import load_dotenv

# Supabase client
try:
    from supabase import create_client  # supabase-py
except Exception as e:
    raise ImportError("Please install supabase: pip install supabase") from e

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Please set SUPABASE_URL and SUPABASE_KEY in environment or .env")

TABLE_NAME = "air_quality_data"
CSV_PATH = os.getenv("TRANSFORMED_CSV", "data/staged/air_quality_transformed.csv")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "200"))
MAX_RETRIES = int(os.getenv("BATCH_MAX_RETRIES", "2"))
RETRY_BACKOFF = float(os.getenv("BATCH_RETRY_BACKOFF", "2.0"))  # seconds

sb = create_client(SUPABASE_URL, SUPABASE_KEY)


def _row_to_record(row: pd.Series) -> Dict[str, Any]:
    """
    Convert a DataFrame row to a dict suitable for Supabase insertion.
    - NaN -> None
    - pandas Timestamp -> ISO string (no timezone info)
    """
    rec = {}
    for col, val in row.items():
        if pd.isna(val):
            rec[col] = None
            continue
        # convert Timestamp -> ISO
        if hasattr(val, "isoformat"):
            try:
                # Use UTC ISO string without microseconds for readability
                rec[col] = val.isoformat()
                continue
            except Exception:
                pass
        # ensure python natives (float, int, str)
        if isinstance(val, (float, int, str, bool)):
            # convert NaN floats to None
            if isinstance(val, float) and math.isnan(val):
                rec[col] = None
            else:
                rec[col] = val
        else:
            rec[col] = val
    return rec


def load_csv_to_supabase(csv_path: str) -> None:
    df = pd.read_csv(csv_path, parse_dates=["time"], keep_default_na=True, na_values=["", "NA", "NaN"])
    if df.empty:
        print("No rows to load.")
        return

    # normalize columns to table schema names
    # expected columns in CSV: city,time,pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,uv_index,aqi_category,severity,risk,hour
    # Supabase table fields: aqi_category -> aqi_category, severity -> severity_score, risk -> risk_flag
    rename_map = {"severity": "severity_score", "risk": "risk_flag"}
    df = df.rename(columns=rename_map)

    # ensure column order (optional)
    expected_cols = [
        "city",
        "time",
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "uv_index",
        "aqi_category",
        "severity_score",
        "risk_flag",
        "hour",
    ]
    # keep only expected cols if present
    df = df[[c for c in expected_cols if c in df.columns]]

    total_rows = len(df)
    inserted_rows = 0
    failed_rows = 0

    records = []
    for idx, row in df.iterrows():
        records.append(_row_to_record(row))

    # batch insert with retries
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        attempt = 0
        success = False
        while attempt <= MAX_RETRIES and not success:
            try:
                attempt += 1
                # supabase-py insertion
                res = sb.table(TABLE_NAME).insert(batch).execute()
                # res appears as {'data': [...], 'status_code':200} depending on client; handle success heuristics
                # If response has error, raise
                if hasattr(res, "error") and res.error:
                    raise RuntimeError(f"Supabase error: {res.error}")
                if isinstance(res, dict) and res.get("status_code") and not (200 <= res["status_code"] < 300):
                    raise RuntimeError(f"Supabase status_code={res.get('status_code')} res={res}")
                # assume success if no exception
                success = True
                inserted_rows += len(batch)
                print(f"Inserted batch {i}-{i+len(batch)-1} ({len(batch)} rows) on attempt {attempt}")
            except Exception as e:
                print(f"Batch insert failed (attempt {attempt}/{MAX_RETRIES}) - error: {e}")
                if attempt > MAX_RETRIES:
                    print("Max retries exceeded for this batch. Skipping batch.")
                    failed_rows += len(batch)
                    break
                sleep = RETRY_BACKOFF * attempt
                print(f"Retrying batch after {sleep:.1f}s ...")
                time.sleep(sleep)

    # summary
    print("----- Load Summary -----")
    print(f"Total rows in CSV: {total_rows}")
    print(f"Inserted rows: {inserted_rows}")
    print(f"Failed rows: {failed_rows}")
    print("------------------------")


if __name__ == "__main__":
    load_csv_to_supabase(CSV_PATH)
