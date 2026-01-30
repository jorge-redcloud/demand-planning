#!/usr/bin/env python3
"""
Check Existing BigQuery Data
============================
Examine the redai_demand_forecast dataset structure

Run on your Mac: python3 scripts/CHECK_BIGQUERY_EXISTING.py
"""

import subprocess

PROJECT_ID = "mimetic-maxim-443710-s2"
DATASET = "redai_demand_forecast"

def run_bq(query):
    cmd = ["bq", "query", "--project_id", PROJECT_ID, "--use_legacy_sql=false", "--format=prettyjson", query]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return result.stdout, result.stderr

print("=" * 60)
print("CHECKING EXISTING BIGQUERY DATA")
print(f"Dataset: {PROJECT_ID}.{DATASET}")
print("=" * 60)

# 1. List all tables
print("\n[1] Tables in dataset:")
out, err = run_bq(f"""
SELECT table_name, row_count, TIMESTAMP_MILLIS(creation_time) as created
FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.TABLES`
ORDER BY table_name
""")
print(out or err)

# 2. List ML models
print("\n[2] ML Models:")
out, err = run_bq(f"""
SELECT model_name, TIMESTAMP_MILLIS(creation_time) as created
FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.ML_MODELS`
""")
print(out or err)

# 3. Check eval_summary structure
print("\n[3] eval_summary sample:")
out, err = run_bq(f"""
SELECT * FROM `{PROJECT_ID}.{DATASET}.eval_summary` LIMIT 5
""")
print(out or err)

# 4. Check features_sku_weekly structure
print("\n[4] features_sku_weekly columns:")
out, err = run_bq(f"""
SELECT column_name, data_type
FROM `{PROJECT_ID}.{DATASET}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = 'features_sku_weekly'
""")
print(out or err)

# 5. Check eval_sku structure
print("\n[5] eval_sku sample:")
out, err = run_bq(f"""
SELECT * FROM `{PROJECT_ID}.{DATASET}.eval_sku` LIMIT 5
""")
print(out or err)

print("\n" + "=" * 60)
print("Use this info to understand the existing schema")
print("=" * 60)
