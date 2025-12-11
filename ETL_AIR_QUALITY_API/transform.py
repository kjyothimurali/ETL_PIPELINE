# transform.py
"""
Transform step for Urban Air Quality Monitoring ETL.

Fixes:
 - Automatically infer city name (prefer payload, fallback to filename)
 - Keeps original behavior: flatten hourly-style JSON when present
 - Robust to common OpenAQ/Open-Meteo shapes (best-effort)
 - Saves transformed CSV to data/staged/air_quality_transformed.csv
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import List, Optional

import pandas as pd

# Directories
RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
STAGED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_FILE = STAGED_DIR / "air_quality_transformed.csv"

# Mapping OpenAQ/Open-Meteo parameter names to our columns
POLLUTANT_MAPPING = {
    "pm10": "pm10",
    "pm25": "pm2_5",
    "pm2_5": "pm2_5",
    "co": "carbon_monoxide",
    "no2": "nitrogen_dioxide",
    "so2": "sulphur_dioxide",
    "o3": "ozone",
    "uv_index": "uv_index",
    "uvi": "uv_index",
    "uv": "uv_index",
}

POLLUTANT_COLS = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "uv_index"]


def calculate_aqi(pm2_5):
    """AQI category based on PM2.5"""
    if pm2_5 is None:
        return None
    try:
        pm = float(pm2_5)
    except Exception:
        return None
    if pm <= 50:
        return "Good"
    elif pm <= 100:
        return "Moderate"
    elif pm <= 200:
        return "Unhealthy"
    elif pm <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


def calculate_severity(row):
    """Pollution Severity Score using weighted pollutants"""
    def s(x):
        try:
            return float(x) if x is not None else 0.0
        except Exception:
            return 0.0
    return (
        s(row.get("pm2_5", 0)) * 5 +
        s(row.get("pm10", 0)) * 3 +
        s(row.get("nitrogen_dioxide", 0)) * 4 +
        s(row.get("sulphur_dioxide", 0)) * 4 +
        s(row.get("carbon_monoxide", 0)) * 2 +
        s(row.get("ozone", 0)) * 3
    )


def calculate_risk(severity):
    """Risk classification based on severity score"""
    try:
        sev = float(severity)
    except Exception:
        return None
    if sev > 400:
        return "High Risk"
    elif sev > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"


def _infer_city_from_payload(payload: dict) -> Optional[str]:
    """Try multiple common locations to extract a city name from payload"""
    if not isinstance(payload, dict):
        return None
    # common top-level keys
    if payload.get("city"):
        return payload.get("city")
    if payload.get("meta") and isinstance(payload["meta"], dict):
        # some APIs put city in meta
        if payload["meta"].get("city"):
            return payload["meta"].get("city")
    # OpenAQ v2 style
    results = payload.get("results")
    if isinstance(results, list) and results:
        first = results[0]
        if isinstance(first, dict):
            if first.get("city"):
                return first.get("city")
            if first.get("location"):
                return first.get("location")
    # OpenAQ v3 style
    locs = payload.get("locations")
    if isinstance(locs, list) and locs:
        first = locs[0]
        if isinstance(first, dict):
            if first.get("city"):
                return first.get("city")
            if first.get("name"):
                return first.get("name")
    # fallback None
    return None


def _infer_city_from_filename(path: Path) -> Optional[str]:
    """Infer city from filename convention: city_raw_timestamp.json"""
    name = path.stem  # filename without suffix
    # common pattern: <city>_raw_<ts>
    parts = name.split("_raw_")
    if parts and parts[0]:
        return parts[0].replace("_", " ").title()
    # fallback first token
    token = name.split("_")[0]
    if token:
        return token.replace("_", " ").title()
    return None


def flatten_city_json(file_path: str, city_name: Optional[str] = None) -> pd.DataFrame:
    """
    Flatten a single raw JSON file into a DataFrame with one row per timestamp.
    Tries to support both Open-Meteo (hourly arrays) and OpenAQ (results/locations).
    """
    p = Path(file_path)
    with open(p, "r", encoding="utf-8") as f:
        payload = json.load(f)

    # infer city if not provided
    city = city_name or _infer_city_from_payload(payload) or _infer_city_from_filename(p) or "Unknown"

    records = []

    # Case 1: Open-Meteo-like hourly arrays (payload['hourly'] with arrays)
    hourly = payload.get("hourly")
    if isinstance(hourly, dict) and hourly.get("time"):
        times = hourly.get("time", [])
        # try to find pollutant arrays with mapping keys; some payloads might already use our names
        for i, t in enumerate(times):
            rec = {"city": city, "time": pd.to_datetime(t)}
            for src_key, dest_col in POLLUTANT_MAPPING.items():
                # prefer exact key in hourly, else try dest_col directly
                values = None
                if src_key in hourly:
                    values = hourly.get(src_key)
                elif dest_col in hourly:
                    values = hourly.get(dest_col)
                if isinstance(values, list) and i < len(values):
                    rec[dest_col] = values[i]
                else:
                    rec[dest_col] = None
            records.append(rec)
        return pd.DataFrame(records)

    # Case 2: OpenAQ-like payload: results -> locations with measurements
    # We'll extract per-measurement and then aggregate to hourly later.
    measurements = []
    # v2 style
    if isinstance(payload, dict) and "results" in payload and isinstance(payload["results"], list):
        for loc in payload["results"]:
            loc_city = loc.get("city") or loc.get("location") or city
            for m in loc.get("measurements", []):
                parameter = m.get("parameter") or m.get("param") or m.get("name")
                value = m.get("value")
                # time may be under m['lastUpdated'] or m['date']['utc']
                t = m.get("lastUpdated") or (m.get("date") and m["date"].get("utc")) or m.get("date")
                measurements.append({"city": loc_city, "time": t, "parameter": parameter, "value": value})
    # v3 style: locations -> parameters (lastValue/lastUpdated) or measurements
    elif isinstance(payload, dict) and "locations" in payload and isinstance(payload["locations"], list):
        for loc in payload["locations"]:
            loc_city = loc.get("city") or loc.get("name") or city
            params = loc.get("parameters") or []
            for p_item in params:
                parameter = p_item.get("parameter") or p_item.get("name")
                value = p_item.get("lastValue") if "lastValue" in p_item else p_item.get("value")
                t = p_item.get("lastUpdated") or p_item.get("lastUpdatedAt") or loc.get("lastUpdated")
                measurements.append({"city": loc_city, "time": t, "parameter": parameter, "value": value})
            for m in loc.get("measurements", []) or []:
                parameter = m.get("parameter") or m.get("name")
                value = m.get("value")
                t = m.get("lastUpdated") or (m.get("date") and m["date"].get("utc")) or m.get("date")
                measurements.append({"city": loc_city, "time": t, "parameter": parameter, "value": value})
    else:
        # Unknown structure: try to find any measurements under keys
        # attempt to locate 'measurements' or 'values' anywhere in payload
        def _walk_for_measurements(obj):
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k.lower() in ("measurements", "values") and isinstance(v, list):
                        for m in v:
                            yield m
                    else:
                        yield from _walk_for_measurements(v)
            elif isinstance(obj, list):
                for item in obj:
                    yield from _walk_for_measurements(item)
        for m in _walk_for_measurements(payload):
            parameter = m.get("parameter") or m.get("name") or m.get("param")
            value = m.get("value")
            t = m.get("lastUpdated") or (m.get("date") and m["date"].get("utc")) or m.get("date")
            measurements.append({"city": city, "time": t, "parameter": parameter, "value": value})

    # If we collected raw measurements, pivot them into one row per city-hour
    if measurements:
        dfm = pd.DataFrame.from_records(measurements)
        # parse times robustly
        dfm["time"] = pd.to_datetime(dfm["time"], errors="coerce", utc=True)
        dfm = dfm.dropna(subset=["time"])
        if dfm.empty:
            return pd.DataFrame(columns=["city", "time"] + POLLUTANT_COLS)
        # floor to hour
        dfm["time_hour"] = dfm["time"].dt.floor("H")
        # normalize parameter names via mapping (lower/strip)
        dfm["parameter_norm"] = dfm["parameter"].astype(str).str.strip().str.lower().replace({
            k.lower(): v for k, v in POLLUTANT_MAPPING.items()
        })
        # map common variants (e.g., pm2.5 -> pm25)
        dfm["parameter_norm"] = dfm["parameter_norm"].str.replace(".", "_").str.replace("-", "_")
        dfm["parameter_norm"] = dfm["parameter_norm"].map(lambda x: POLLUTANT_MAPPING.get(x, x) if isinstance(x, str) else x)
        # convert value to numeric
        dfm["value_num"] = pd.to_numeric(dfm["value"], errors="coerce")

        # pivot mean of values per city-hour-parameter
        pv = (
            dfm.groupby(["city", "time_hour", "parameter_norm"])["value_num"]
            .mean()
            .reset_index()
            .pivot_table(index=["city", "time_hour"], columns="parameter_norm", values="value_num", aggfunc="first")
            .reset_index()
        )

        # ensure pollutant columns present
        for col in POLLUTANT_COLS:
            if col not in pv.columns:
                pv[col] = pd.NA

        pv = pv.rename(columns={"time_hour": "time"})
        # ensure time is datetime
        pv["time"] = pd.to_datetime(pv["time"], utc=True)
        # reorder to desired columns
        cols = ["city", "time"] + POLLUTANT_COLS
        return pv[cols]

    # If nothing matched, return empty df with expected columns
    return pd.DataFrame(columns=["city", "time"] + POLLUTANT_COLS)


def transform_files(file_paths: List[str]):
    """Transform multiple JSON files and save to CSV"""
    all_frames = []
    for fp in file_paths:
        df = flatten_city_json(fp)
        if not df.empty:
            all_frames.append(df)

    if not all_frames:
        print("No data to transform.")
        # save empty CSV with correct headers
        empty = pd.DataFrame(columns=["city", "time"] + POLLUTANT_COLS + ["aqi_category", "severity", "risk", "hour"])
        empty.to_csv(OUTPUT_FILE, index=False)
        return empty

    df_all = pd.concat(all_frames, ignore_index=True)

    # Ensure all pollutant columns exist
    for col in POLLUTANT_COLS:
        if col not in df_all.columns:
            df_all[col] = pd.NA

    # Convert pollutants to numeric
    for col in POLLUTANT_COLS:
        df_all[col] = pd.to_numeric(df_all[col], errors="coerce")

    # Remove rows where all pollutants are missing
    df_all = df_all.dropna(subset=POLLUTANT_COLS, how="all").reset_index(drop=True)

    # Derived features
    df_all["AQI"] = df_all["pm2_5"].apply(lambda x: calculate_aqi(x) if pd.notnull(x) else None)
    df_all["severity"] = df_all.apply(calculate_severity, axis=1)
    df_all["risk"] = df_all["severity"].apply(calculate_risk)
    df_all["hour"] = df_all["time"].dt.hour

    # Save CSV
    df_all.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Transformed data saved to {OUTPUT_FILE} (rows={len(df_all)})")
    return df_all


if __name__ == "__main__":
    json_files = sorted([str(p) for p in RAW_DIR.glob("*_raw_*") if p.suffix in (".json", ".txt")])
    if not json_files:
        print("No raw JSON files found in", RAW_DIR)
    else:
        transform_files(json_files)
