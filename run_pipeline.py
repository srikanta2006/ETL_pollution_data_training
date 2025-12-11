# run_pipeline.py
import time
from extract import fetch_all_cities
from transform import run_transform
from load import load_to_supabase
from etl_analysis import analyze_and_save, fetch_table


def run_full_pipeline():
    print("\n=== STEP 1: EXTRACT ===")
    extract_results = fetch_all_cities()
    time.sleep(1)

    # Only print summary (your transform reads raw files independently)
    for r in extract_results:
        if r.get("success") == "true":
            print(f"✔ Extracted: {r['city']} -> {r['raw_path']}")
        else:
            print(f"❌ Failed: {r['city']} ({r.get('error')})")

    print("\n=== STEP 2: TRANSFORM ===")
    run_transform()     # no args allowed

    staged_csv = "data/staged/air_quality_transformed.csv"

    print("\n=== STEP 3: LOAD TO SUPABASE ===")
    load_to_supabase(staged_csv)   # no batch_size argument allowed

    print("\n=== STEP 4: ANALYSIS ===")
    df = fetch_table()
    analyze_and_save(df)

    print("\n🎉 ETL Pipeline Completed Successfully\n")


if __name__ == "__main__":
    run_full_pipeline()
