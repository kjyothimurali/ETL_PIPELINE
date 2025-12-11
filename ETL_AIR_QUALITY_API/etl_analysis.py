# etl_analysis.py
"""
Analysis script: read air_quality_data from Supabase and produce:
 - summary_metrics.csv
 - city_risk_distribution.csv
 - pollution_trends.csv
 - PNG visualizations:
    - pm25_histogram.png
    - risk_flags_by_city.png
    - hourly_pm25_trends.png
    - severity_vs_pm25_scatter.png
Requirements:
 - SUPABASE_URL and SUPABASE_KEY in env/.env
 - pandas, matplotlib, python-dotenv, supabase
"""
from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv

try:
    from supabase import create_client
except Exception as e:
    raise ImportError("Please install supabase: pip install supabase") from e

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Please set SUPABASE_URL and SUPABASE_KEY in environment")

sb = create_client(SUPABASE_URL, SUPABASE_KEY)

OUTPUT_DIR = Path("data") / "processed"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
PLOTS_DIR = OUTPUT_DIR  # same folder for CSVs and PNGs

TABLE_NAME = "air_quality_data"


def fetch_table_as_df() -> pd.DataFrame:
    """
    Fetch entire table from Supabase into a pandas DataFrame.
    Note: For large datasets, implement pagination. For now, fetch all.
    """
    # supabase-py: sb.table(TABLE_NAME).select("*").execute()
    res = sb.table(TABLE_NAME).select("*").execute()
    if isinstance(res, dict) and res.get("error"):
        raise RuntimeError(f"Supabase error: {res['error']}")
    # result may be object with .data
    data = None
    if hasattr(res, "data"):
        data = res.data
    elif isinstance(res, dict) and "data" in res:
        data = res["data"]
    else:
        # fallback: try treating res as list
        data = res
    df = pd.DataFrame(data)
    # normalize column names if needed
    return df


def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute requested KPI metrics:
    - City with highest average PM2.5
    - City with highest severity score (average)
    - Percentage of High/Moderate/Low risk hours (overall)
    - Hour of day with worst AQI (use average pm2_5 per hour)
    Returns a single-row DataFrame with metrics.
    """
    # ensure types
    df["pm2_5"] = pd.to_numeric(df.get("pm2_5"), errors="coerce")
    df["severity_score"] = pd.to_numeric(df.get("severity_score"), errors="coerce")
    df["time"] = pd.to_datetime(df.get("time"), errors="coerce", utc=True)

    kpis = {}

    # City with highest avg PM2.5
    pm25_by_city = df.groupby("city")["pm2_5"].mean().dropna()
    if not pm25_by_city.empty:
        city_high_pm25 = pm25_by_city.idxmax()
        kpis["city_highest_avg_pm2_5"] = city_high_pm25
        kpis["highest_avg_pm2_5_value"] = pm25_by_city.max()
    else:
        kpis["city_highest_avg_pm2_5"] = None
        kpis["highest_avg_pm2_5_value"] = None

    # City with highest severity score (avg)
    sev_by_city = df.groupby("city")["severity_score"].mean().dropna()
    if not sev_by_city.empty:
        city_high_sev = sev_by_city.idxmax()
        kpis["city_highest_avg_severity"] = city_high_sev
        kpis["highest_avg_severity_value"] = sev_by_city.max()
    else:
        kpis["city_highest_avg_severity"] = None
        kpis["highest_avg_severity_value"] = None

    # Percentage of High/Moderate/Low risk hours
    # risk_flag values expected: "High Risk", "Moderate Risk", "Low Risk"
    risk_counts = df["risk_flag"].value_counts(dropna=True)
    total_risk = risk_counts.sum() if not risk_counts.empty else 0
    for flag in ["High Risk", "Moderate Risk", "Low Risk"]:
        pct = (risk_counts.get(flag, 0) / total_risk * 100) if total_risk > 0 else 0.0
        kpis[f"pct_{flag.replace(' ', '_').lower()}"] = pct

    # Hour of day with worst AQI (use mean pm2_5 by hour across all cities)
    if "time" in df.columns and not df["time"].isna().all():
        df["hour"] = df["time"].dt.hour
        hr_pm25 = df.groupby("hour")["pm2_5"].mean().dropna()
        if not hr_pm25.empty:
            worst_hr = int(hr_pm25.idxmax())
            kpis["hour_with_worst_avg_pm2_5"] = worst_hr
            kpis["worst_hour_avg_pm2_5"] = hr_pm25.max()
        else:
            kpis["hour_with_worst_avg_pm2_5"] = None
            kpis["worst_hour_avg_pm2_5"] = None
    else:
        kpis["hour_with_worst_avg_pm2_5"] = None
        kpis["worst_hour_avg_pm2_5"] = None

    return pd.DataFrame([kpis])


def city_risk_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each city, compute percentage distribution of risk_flag categories.
    Output columns: city, high_risk_pct, moderate_risk_pct, low_risk_pct
    """
    # ensure consistent risk_flag
    dist = []
    for city, group in df.groupby("city"):
        counts = group["risk_flag"].value_counts(dropna=True)
        total = counts.sum() if not counts.empty else 0
        high = counts.get("High Risk", 0)
        moderate = counts.get("Moderate Risk", 0)
        low = counts.get("Low Risk", 0)
        dist.append(
            {
                "city": city,
                "high_risk_pct": (high / total * 100) if total > 0 else 0.0,
                "moderate_risk_pct": (moderate / total * 100) if total > 0 else 0.0,
                "low_risk_pct": (low / total * 100) if total > 0 else 0.0,
            }
        )
    return pd.DataFrame(dist)


def pollution_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare time-series trends for each city (time, pm2_5, pm10, ozone)
    Aggregated at hourly time already in table. Return long-form DataFrame with columns:
    city, time, pm2_5, pm10, ozone
    """
    df["time"] = pd.to_datetime(df["time"], errors="coerce", utc=True)
    df = df.dropna(subset=["time"])
    out = df[["city", "time", "pm2_5", "pm10", "ozone"]].copy()
    # sort
    out = out.sort_values(["city", "time"]).reset_index(drop=True)
    return out


def save_csvs_and_plots(df: pd.DataFrame):
    # Ensure numeric conversions early
    df["pm2_5"] = pd.to_numeric(df.get("pm2_5"), errors="coerce")
    df["pm10"] = pd.to_numeric(df.get("pm10"), errors="coerce")
    df["severity_score"] = pd.to_numeric(df.get("severity_score"), errors="coerce")
    df["time"] = pd.to_datetime(df.get("time"), errors="coerce", utc=True)

    # CSV outputs
    summary_df = compute_kpis(df)
    summary_df.to_csv(OUTPUT_DIR / "summary_metrics.csv", index=False)

    risk_dist_df = city_risk_distribution(df)
    risk_dist_df.to_csv(OUTPUT_DIR / "city_risk_distribution.csv", index=False)

    trends_df = pollution_trends(df)
    trends_df.to_csv(OUTPUT_DIR / "pollution_trends.csv", index=False)

    # 1) Histogram of PM2.5
    plt.figure(figsize=(8, 5))
    df["pm2_5"].dropna().hist(bins=30)
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5 (µg/m³)")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "pm25_histogram.png", dpi=150)
    plt.close()

    # 2) Bar chart of risk flags per city
    plt.figure(figsize=(10, 6))
    risk_counts = df.groupby("city")["risk_flag"].value_counts().unstack(fill_value=0)
    # use pandas plotting (returns axes)
    ax = risk_counts.plot(kind="bar", stacked=False, figsize=(10, 6))
    ax.set_title("Risk Flags per City")
    ax.set_ylabel("Count")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "risk_flags_by_city.png", dpi=150)
    plt.close()

    # 3) Line chart of hourly PM2.5 trends (each city line)
    plt.figure(figsize=(12, 6))
    # group by city and compute hourly mean of pm2_5 (select column BEFORE resampling)
    for city, group in trends_df.groupby("city"):
        # make sure 'time' is datetime and set as index
        grp = group.set_index("time")
        # select numeric column explicitly, resample hourly with 'h' (lowercase)
        pm25_hourly = grp["pm2_5"].resample("h").mean()
        if pm25_hourly.dropna().empty:
            continue
        plt.plot(pm25_hourly.index, pm25_hourly.values, label=city)
    plt.legend()
    plt.title("Hourly PM2.5 Trends by City")
    plt.xlabel("Time (UTC)")
    plt.ylabel("PM2.5 (µg/m³)")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "hourly_pm25_trends.png", dpi=150)
    plt.close()

    # 4) Scatter: severity_score vs pm2_5
    plt.figure(figsize=(8, 6))
    plt.scatter(df["pm2_5"], df["severity_score"], alpha=0.6)
    plt.xlabel("PM2.5 (µg/m³)")
    plt.ylabel("Severity Score")
    plt.title("Severity Score vs PM2.5")
    plt.tight_layout()
    plt.savefig(PLOTS_DIR / "severity_vs_pm25_scatter.png", dpi=150)
    plt.close()

    print("CSV outputs and plots saved to:", OUTPUT_DIR)


def main():
    df = fetch_table_as_df()
    if df.empty:
        print("No data returned from Supabase table.")
        return
    save_csvs_and_plots(df)
    print("Analysis complete.")


if __name__ == "__main__":
    main()
