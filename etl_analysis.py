# etl_analysis.py
from dotenv import load_dotenv
import os
import pandas as pd
from supabase import create_client
from pathlib import Path
import matplotlib.pyplot as plt

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

TABLE_NAME = "air_quality_data"

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in your .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)


def _extract_data_from_response(res):
    """Extract list-of-dicts from supabase response."""
    data = getattr(res, "data", None)
    if isinstance(data, list):
        return data

    try:
        if isinstance(res, dict) and "data" in res and isinstance(res["data"], list):
            return res["data"]
    except Exception:
        pass

    if isinstance(res, (list, tuple)):
        for item in res:
            if isinstance(item, list) and all(isinstance(x, dict) for x in item):
                return item
        if len(res) > 0 and isinstance(res[0], dict):
            return list(res)

    if hasattr(res, "json"):
        try:
            j = res.json()
            if isinstance(j, dict) and "data" in j and isinstance(j["data"], list):
                return j["data"]
        except Exception:
            pass

    return []


def fetch_table(limit: int | None = None) -> pd.DataFrame:
    """Fetch table from Supabase and return a cleaned DataFrame."""
    print(f"🔍 Fetching data from Supabase table '{TABLE_NAME}'...")
    query = supabase.table(TABLE_NAME).select("*")
    if limit:
        query = query.limit(limit)
    res = query.execute()

    data = _extract_data_from_response(res)
    df = pd.DataFrame(data)

    if df.empty:
        print("⚠️ Table is empty.")
        return df

    # Normalize and coerce types
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
        # create hour column if missing
        if "hour" not in df.columns:
            df["hour"] = df["time"].dt.hour
    else:
        # Ensure hour column exists (may be present separately)
        if "hour" in df.columns:
            df["hour"] = pd.to_numeric(df["hour"], errors="coerce")

    # Numeric columns to coerce
    float_cols = [
        "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
        "sulphur_dioxide", "ozone", "uv_index", "severity_score"
    ]
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize risk_flag column name variations
    if "risk" in df.columns and "risk_flag" not in df.columns:
        df = df.rename(columns={"risk": "risk_flag"})
    if "risk_flag" not in df.columns:
        # create unknown risk_flag if completely missing
        df["risk_flag"] = pd.NA

    # Ensure city column exists
    if "city" not in df.columns:
        df["city"] = "Unknown"

    return df


def safe_idxmax(series: pd.Series):
    """Return idxmax safely; if all NaN return None."""
    if series.dropna().empty:
        return None
    try:
        return series.idxmax()
    except Exception:
        return None


def analyze_and_save(df: pd.DataFrame):
    if df.empty:
        print("No data to analyze.")
        return

    print("ℹ️ Data loaded:", df.shape[0], "rows")

    # ---------- KPI METRICS ----------
    metrics = {}

    # city with highest avg pm2_5
    if "pm2_5" in df.columns:
        group_pm25 = df.groupby("city")["pm2_5"].mean()
        metrics["city_highest_avg_pm2_5"] = safe_idxmax(group_pm25)
    else:
        metrics["city_highest_avg_pm2_5"] = None

    # city with highest severity_score
    if "severity_score" in df.columns:
        group_sev = df.groupby("city")["severity_score"].mean()
        metrics["city_highest_severity"] = safe_idxmax(group_sev)
    else:
        metrics["city_highest_severity"] = None

    # risk distribution percentages
    if "risk_flag" in df.columns:
        risk_pct = (df["risk_flag"].fillna("Unknown").value_counts(normalize=True) * 100).to_dict()
        # canonical keys
        metrics["high_risk_pct"] = float(risk_pct.get("High Risk", 0))
        metrics["moderate_risk_pct"] = float(risk_pct.get("Moderate Risk", 0))
        metrics["low_risk_pct"] = float(risk_pct.get("Low Risk", 0))
        metrics["unknown_risk_pct"] = float(risk_pct.get("Unknown", 0))
    else:
        metrics["high_risk_pct"] = metrics["moderate_risk_pct"] = metrics["low_risk_pct"] = 0.0
        metrics["unknown_risk_pct"] = 100.0

    # worst hour by average pm2_5 (if hour exists)
    if "hour" in df.columns and "pm2_5" in df.columns:
        hour_group = df.groupby("hour")["pm2_5"].mean()
        metrics["worst_hour_pm2_5"] = safe_idxmax(hour_group)
    else:
        metrics["worst_hour_pm2_5"] = None

    # Save summary metrics
    summary_path = PROCESSED_DIR / "summary_metrics.csv"
    pd.DataFrame([metrics]).to_csv(summary_path, index=False)
    print(f"✅ summary_metrics.csv saved -> {summary_path}")

    # ---------- CITY POLLUTION TREND REPORT ----------
    trend_cols = [c for c in ["city", "time", "pm2_5", "pm10", "ozone"] if c in df.columns]
    if trend_cols:
        trend_df = df[trend_cols].copy()
        trend_df = trend_df.sort_values("time") if "time" in trend_df.columns else trend_df
        trend_path = PROCESSED_DIR / "pollution_trends.csv"
        trend_df.to_csv(trend_path, index=False)
        print(f"✅ pollution_trends.csv saved -> {trend_path}")
    else:
        print("⚠️ No trend columns available to save.")

    # ---------- CITY RISK DISTRIBUTION CSV ----------
    # produce tidy city-risk-count CSV (safe even if missing)
    risk_counts = df.copy()
    risk_counts["risk_flag"] = risk_counts["risk_flag"].fillna("Unknown")
    risk_dist = risk_counts.groupby(["city", "risk_flag"]).size().reset_index(name="count")
    risk_dist_path = PROCESSED_DIR / "city_risk_distribution.csv"
    risk_dist.to_csv(risk_dist_path, index=False)
    print(f"✅ city_risk_distribution.csv saved -> {risk_dist_path}")

    # Ensure the plots directory exists
    plots_dir = PROCESSED_DIR / "plots"
    plots_dir.mkdir(parents=True, exist_ok=True)

    # ---------- VISUALIZATIONS ----------
    # 1) Histogram of PM2.5
    try:
        if "pm2_5" in df.columns and not df["pm2_5"].dropna().empty:
            plt.figure(figsize=(8, 4))
            plt.hist(df["pm2_5"].dropna(), bins=30)
            plt.title("PM2.5 Distribution")
            plt.xlabel("PM2.5")
            plt.tight_layout()
            p = plots_dir / "pm2_5_histogram.png"
            plt.savefig(p)
            plt.close()
            print(f"📊 Saved {p}")
        else:
            print("⚠️ Skipping PM2.5 histogram — no numeric pm2_5 data.")
    except Exception as e:
        print("⚠️ PM2.5 histogram failed:", e)

    # 2) Bar chart: risk flags per city (pivot -> numeric)
    try:
        pivot = risk_dist.pivot(index="city", columns="risk_flag", values="count").fillna(0)
        if pivot.empty:
            print("⚠️ Skipping risk flags bar chart — pivot is empty.")
        else:
            # force numeric
            pivot_numeric = pivot.apply(pd.to_numeric, errors="coerce").fillna(0)
            plt.figure(figsize=(10, 5))
            pivot_numeric.plot(kind="bar")
            plt.title("Risk Flags per City")
            plt.ylabel("Count")
            plt.tight_layout()
            p = plots_dir / "risk_flags_per_city.png"
            plt.savefig(p)
            plt.close()
            print(f"📊 Saved {p}")
    except Exception as e:
        print("⚠️ Risk flags bar chart failed:", e)

    # 3) Line chart: hourly PM2.5 trend
    try:
        if "hour" in df.columns and "pm2_5" in df.columns:
            hourly = df.groupby("hour", as_index=False)["pm2_5"].mean().sort_values("hour")
            if not hourly.empty:
                plt.figure(figsize=(10, 4))
                plt.plot(hourly["hour"], hourly["pm2_5"], marker="o")
                plt.title("Hourly PM2.5 Trend")
                plt.xlabel("Hour")
                plt.ylabel("PM2.5")
                plt.tight_layout()
                p = plots_dir / "hourly_pm2_5_trend.png"
                plt.savefig(p)
                plt.close()
                print(f"📊 Saved {p}")
            else:
                print("⚠️ Skipping hourly PM2.5 plot — aggregated data empty.")
        else:
            print("⚠️ Skipping hourly PM2.5 — missing 'hour' or 'pm2_5' column.")
    except Exception as e:
        print("⚠️ Hourly PM2.5 plot failed:", e)

    # 4) Scatter: severity_score vs pm2_5
    try:
        if "severity_score" in df.columns and "pm2_5" in df.columns and not df[["severity_score", "pm2_5"]].dropna().empty:
            plt.figure(figsize=(8, 5))
            plt.scatter(df["pm2_5"], df["severity_score"], alpha=0.6)
            plt.title("Severity Score vs PM2.5")
            plt.xlabel("PM2.5")
            plt.ylabel("Severity Score")
            plt.tight_layout()
            p = plots_dir / "severity_vs_pm2_5.png"
            plt.savefig(p)
            plt.close()
            print(f"📊 Saved {p}")
        else:
            print("⚠️ Skipping scatter plot — missing severity_score or pm2_5 numeric data.")
    except Exception as e:
        print("⚠️ Scatter plot failed:", e)

    print("\n🎉 Analysis complete.")


def run_analysis(limit: int | None = None):
    df = fetch_table(limit)
    analyze_and_save(df)


if __name__ == "__main__":
    run_analysis()
