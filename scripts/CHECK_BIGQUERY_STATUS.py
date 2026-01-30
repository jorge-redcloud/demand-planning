#!/usr/bin/env python3
"""
Check BigQuery Status
=====================
Run this to see what's currently deployed in BigQuery.

Usage: python3 scripts/CHECK_BIGQUERY_STATUS.py

Requires: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth
"""

import subprocess
import sys

# Configuration - UPDATE THESE
PROJECT_ID = "mimetic-maxim-443710-s2"  # <-- UPDATE THIS
DATASET = "aca_demand_planning"

def run_bq_query(query):
    """Run a BigQuery query and return results"""
    cmd = [
        "bq", "query",
        "--project_id", PROJECT_ID,
        "--use_legacy_sql=false",
        "--format=prettyjson",
        query
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        return result.stdout, result.stderr
    except Exception as e:
        return None, str(e)

def main():
    print("=" * 60)
    print("BIGQUERY STATUS CHECK")
    print("=" * 60)
    print(f"Project: {PROJECT_ID}")
    print(f"Dataset: {DATASET}")
    print()

    # Check if bq CLI is available
    result = subprocess.run(["which", "bq"], capture_output=True, text=True)
    if result.returncode != 0:
        print("❌ BigQuery CLI (bq) not found!")
        print("   Install with: gcloud components install bq")
        print("\n   Alternative: Use the BigQuery Console at:")
        print("   https://console.cloud.google.com/bigquery")
        return

    print("1. Checking tables in dataset...")
    query1 = f"""
    SELECT table_name, creation_time, row_count
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES`
    ORDER BY creation_time DESC
    """
    out, err = run_bq_query(query1)
    if err and "Not found" in err:
        print(f"   ❌ Dataset '{DATASET}' not found. Need to create it first.")
    elif out:
        print(out)
    else:
        print(f"   Error: {err}")

    print("\n2. Checking ML models...")
    query2 = f"""
    SELECT model_name, creation_time
    FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.ML_MODELS`
    ORDER BY creation_time DESC
    """
    out, err = run_bq_query(query2)
    if out:
        print(out)
    else:
        print(f"   No models found or error: {err}")

    print("\n3. Sample queries you can run manually:")
    print(f"""
    -- List all tables
    SELECT * FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES`;

    -- Check SKU predictions
    SELECT * FROM `{PROJECT_ID}.{DATASET}.sku_predictions_v2` LIMIT 10;

    -- Check model performance
    SELECT * FROM `{PROJECT_ID}.{DATASET}.model_evaluation_summary`;
    """)

if __name__ == "__main__":
    main()
