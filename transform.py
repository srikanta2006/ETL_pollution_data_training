# transform.py
"""
Transform step for Urban Air Quality Monitoring ETL.

- Reads all raw JSON files in data/raw/
- Supports Open-Meteo Air Quality data (hourly arrays)
- Flattens each city into 1 row per hour
- Adds derived features:
    * AQI category (PM2.5)
    * severity score
    * risk classification
    * hour of day
- Saves transformed CSV → data/staged/air_quality_transformed.csv
"""

import json
import os
from pathlib import Path
from datetime import datetime
import pandas as pd

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
STAGED_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_FILE = STAGED_DIR / "air_quality_transformed.csv"


def classify_aqi(pm2_5: float) -> str:
    """Return AQI category based on PM2.5 value."""
    if pm2_5 <= 50:
        return "Good"
    elif pm2_5 <= 100:
        return "Moderate"
    elif pm2_5 <= 200:
        return "Unhealthy"
    elif pm2_5 <= 300:
        return "Very Unhealthy"
    return "Hazardous"


def compute_severity(row):
    """Weighted pollution severity score."""
    return (
        (row["pm2_5"] * 5)
        + (row["pm10"] * 3)
        + (row["nitrogen_dioxide"] * 4)
        + (row["sulphur_dioxide"] * 4)
        + (row["carbon_monoxide"] * 2)
        + (row["ozone"] * 3)
    )


def classify_risk(severity: float) -> str:
    """Risk level based on severity."""
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    return "Low Risk"


def transform_file(path: Path):
    """Convert a single JSON file to a DataFrame."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"⚠️ Failed to read {path}: {e}")
        return None

    # Detect Open-Meteo hourly format
    if "hourly" not in data:
        print(f"⚠️ Skipping (invalid format): {path}")
        return None

    hourly = data["hourly"]

    # Extract city name from filename
    city = path.stem.split("_raw_")[0].replace("_", " ").title()

    # Required columns
    cols = [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "time",
    ]

    missing = [c for c in cols if c not in hourly]
    if missing:
        print(f"⚠️ Missing keys in {path}: {missing}")
        return None

    # Convert hourly dictionary to DataFrame
    df = pd.DataFrame(hourly)

    # Add city
    df["city"] = city

    # Convert timestamp
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Convert pollutant columns to numeric
    pollutant_cols = [
        "pm10",
        "pm2_5",
        "carbon_monoxide",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
    ]

    for col in pollutant_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Remove rows where all pollutants are missing
    df = df.dropna(subset=pollutant_cols, how="all")
    if df.empty:
        print(f"⚠️ {path} produced no valid rows.")
        return None

    # Derived features
    df["aqi_category"] = df["pm2_5"].apply(lambda x: classify_aqi(x) if pd.notna(x) else None)

    df["severity"] = df.apply(lambda r: compute_severity(r), axis=1)

    df["risk"] = df["severity"].apply(classify_risk)

    df["hour"] = df["time"].dt.hour

    return df


def run_transform():
    print("Starting AQI transform...")

    raw_files = list(RAW_DIR.glob("*.json"))
    print(f"Found {len(raw_files)} raw files to transform...")

    all_frames = []

    for file in raw_files:
        df = transform_file(file)
        if df is not None:
            all_frames.append(df)

    if not all_frames:
        print("❌ No valid records found. Nothing to transform.")
        OUTPUT_FILE.touch()
        print(f"Staged file created at: {OUTPUT_FILE}")
        return

    final_df = pd.concat(all_frames, ignore_index=True)
    final_df.to_csv(OUTPUT_FILE, index=False)

    print(f"✅ Transform complete. Saved to: {OUTPUT_FILE}")


if __name__ == "__main__":
    run_transform()
