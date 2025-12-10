# ===========================
# load.py
# ===========================
# Purpose: Load transformed telco-customer dataset into Supabase using Supabase client

import os
import time
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv


# ----------------------------------------
# Supabase client
# ----------------------------------------
def get_supabase_client() -> Client:
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")

    if not url or not key:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

    return create_client(url, key)


# ----------------------------------------
# Step 1: Show CREATE TABLE SQL (run once in Supabase)
# ----------------------------------------
def print_create_table_sql():
    """
    Prints the SQL needed to create the table in Supabase.
    Run this SQL once in the Supabase SQL editor.
    """
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS telco_customer_churn_features (
        id BIGSERIAL PRIMARY KEY,
        tenure INTEGER,
        monthlycharges DOUBLE PRECISION,
        totalcharges DOUBLE PRECISION,
        churn TEXT,
        internetservice TEXT,
        contract TEXT,
        paymentmethod TEXT,
        tenure_group TEXT,
        monthly_charge_segment TEXT,
        has_internet_service INTEGER,
        is_multi_line_user INTEGER,
        contract_type_code INTEGER
    );
    """
    print("💡 Run this SQL in Supabase (SQL Editor) to create the table:\n")
    print(create_table_sql)


def check_table_exists(table_name: str = "telco_customer_churn_features"):
    """
    Tries a simple SELECT to see if the table exists.
    """
    try:
        supabase = get_supabase_client()
        supabase.table(table_name).select("*").limit(1).execute()
        print(f"✅ Table '{table_name}' exists in Supabase.")
    except Exception as e:
        print(f"⚠️  Could not verify table '{table_name}': {e}")
        print("ℹ️  Make sure you have created the table using the printed SQL.")


# ----------------------------------------
# Step 2: Load CSV data into Supabase table
# ----------------------------------------
def load_to_supabase(
    staged_path: str,
    table_name: str = "telco_customer_churn_features",
    batch_size: int = 200,
):

    # Convert to absolute path
    if not os.path.isabs(staged_path):
        staged_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), staged_path)
        )

    print(f"🔍 Looking for data file at: {staged_path}")

    if not os.path.exists(staged_path):
        print(f"❌ Error: File not found at {staged_path}")
        print("ℹ️  Please run transform.py first to generate the transformed data")
        return

    try:
        supabase = get_supabase_client()

        # Read CSV
        df = pd.read_csv(staged_path)

        # ----------------------------------------
        # Normalize column names to lowercase
        # ----------------------------------------
        df.columns = [c.strip().lower() for c in df.columns]

        # ----------------------------------------
        # Keep only columns needed for this table
        # ----------------------------------------
        needed_cols = [
            "tenure",
            "monthlycharges",
            "totalcharges",
            "churn",
            "internetservice",
            "contract",
            "paymentmethod",
            "tenure_group",
            "monthly_charge_segment",
            "has_internet_service",
            "is_multi_line_user",
            "contract_type_code",
        ]

        missing = [c for c in needed_cols if c not in df.columns]
        if missing:
            print(f"❌ Error: Missing columns in CSV: {missing}")
            return

        df = df[needed_cols].copy()

        # ----------------------------------------
        # Ensure numeric columns are numeric
        # ----------------------------------------
        numeric_cols = [
            "tenure",
            "monthlycharges",
            "totalcharges",
            "has_internet_service",
            "is_multi_line_user",
            "contract_type_code",
        ]

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        total_rows = len(df)
        print(f"📊 Loading {total_rows} rows into '{table_name}'...")

        # ----------------------------------------
        # Batch insert with retry logic
        # ----------------------------------------
        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i : i + batch_size].copy()

            # NaN -> None so Supabase stores NULL
            batch = batch.where(pd.notnull(batch), None)
            records = batch.to_dict("records")

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    supabase.table(table_name).insert(records).execute()
                    end = min(i + batch_size, total_rows)
                    print(f"✅ Inserted rows {i+1}-{end} of {total_rows}")
                    break  # success, leave retry loop
                except Exception as e:
                    msg = str(e)
                    if "EOF occurred in violation of protocol" in msg and attempt < max_retries - 1:
                        print(
                            f"⚠️  SSL error on batch {i//batch_size + 1}, "
                            f"retrying (attempt {attempt+2}/{max_retries})..."
                        )
                        time.sleep(2)
                        continue
                    else:
                        print(f"⚠️  Error in batch {i//batch_size + 1}: {e}")
                        break  # don't retry further for other errors

        print(f"🎯 Finished loading data into '{table_name}'.")

    except Exception as e:
        print(f"❌ Error loading data: {e}")


if __name__ == "__main__":
    # Path relative to the script location
    staged_csv_path = os.path.join("..", "data", "staged", "Telco-Customer_transformed.csv")

    # Show SQL so you can create the table in Supabase once
    print_create_table_sql()
    check_table_exists()

    # Load data
    load_to_supabase(staged_csv_path)
