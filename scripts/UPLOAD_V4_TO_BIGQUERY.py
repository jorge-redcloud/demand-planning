#!/usr/bin/env python3
"""
Upload V4 Model Results to BigQuery
===================================
Uploads V4 predictions alongside existing data in redai_demand_forecast

Tables to create/update:
- predictions_sku_v4: SKU-level predictions
- predictions_category_v4: Category-level predictions
- predictions_customer_v4: Customer-level predictions
- eval_v4_summary: V4 model evaluation summary
- model_versions: Track all model versions

Run on your Mac: python3 scripts/UPLOAD_V4_TO_BIGQUERY.py
"""

import subprocess
import pandas as pd
from pathlib import Path
from datetime import datetime

PROJECT_ID = "mimetic-maxim-443710-s2"
DATASET = "redai_demand_forecast"

SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
MODEL_DIR = BASE_PATH / 'model_evaluation'

def run_bq_command(cmd):
    """Run a bq command"""
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        print(f"  ⚠ Error: {result.stderr}")
    return result.returncode == 0

def upload_csv(csv_path, table_name, schema=None):
    """Upload CSV to BigQuery table"""
    print(f"\n  Uploading {csv_path.name} → {table_name}...")

    cmd = [
        "bq", "load",
        "--project_id", PROJECT_ID,
        "--source_format=CSV",
        "--skip_leading_rows=1",
        "--replace",  # Replace if exists
        f"{DATASET}.{table_name}",
        str(csv_path)
    ]

    if schema:
        cmd.append(schema)

    return run_bq_command(cmd)

def main():
    print("=" * 60)
    print("UPLOADING V4 MODEL RESULTS TO BIGQUERY")
    print(f"Dataset: {PROJECT_ID}.{DATASET}")
    print("=" * 60)

    # Check if files exist
    sku_v4 = MODEL_DIR / 'sku_predictions_v4.csv'
    cat_v4 = MODEL_DIR / 'category_predictions_v4.csv'
    cust_v4 = MODEL_DIR / 'customer_predictions_v4.csv'

    if not sku_v4.exists():
        print("❌ V4 prediction files not found! Run TRAIN_V4_MODELS.py first.")
        return

    print("\n[1/5] Preparing V4 prediction files...")

    # Load and prepare SKU predictions
    sku_df = pd.read_csv(sku_v4)
    sku_df['model_version'] = 'V4'
    sku_df['created_at'] = datetime.now().isoformat()
    sku_df.to_csv(MODEL_DIR / 'bq_sku_predictions_v4.csv', index=False)
    print(f"  ✓ SKU: {len(sku_df)} rows")

    # Load and prepare Category predictions
    cat_df = pd.read_csv(cat_v4)
    cat_df['model_version'] = 'V4'
    cat_df['created_at'] = datetime.now().isoformat()
    cat_df.to_csv(MODEL_DIR / 'bq_category_predictions_v4.csv', index=False)
    print(f"  ✓ Category: {len(cat_df)} rows")

    # Load and prepare Customer predictions
    cust_df = pd.read_csv(cust_v4)
    cust_df['model_version'] = 'V4'
    cust_df['created_at'] = datetime.now().isoformat()
    cust_df.to_csv(MODEL_DIR / 'bq_customer_predictions_v4.csv', index=False)
    print(f"  ✓ Customer: {len(cust_df)} rows")

    # Create evaluation summary
    print("\n[2/5] Creating evaluation summary...")
    eval_summary = pd.DataFrame([
        {
            'model_version': 'V4',
            'level': 'SKU',
            'wmape': 62.5,
            'total_predictions': len(sku_df),
            'unique_entities': sku_df['sku'].nunique(),
            'high_confidence': len(sku_df[sku_df['confidence'] == 'High'].groupby('sku')),
            'medium_confidence': len(sku_df[sku_df['confidence'] == 'Medium'].groupby('sku')),
            'low_confidence': len(sku_df[sku_df['confidence'] == 'Low'].groupby('sku')),
            'created_at': datetime.now().isoformat()
        },
        {
            'model_version': 'V4',
            'level': 'Category',
            'wmape': 54.3,
            'total_predictions': len(cat_df),
            'unique_entities': cat_df['category'].nunique(),
            'high_confidence': len(cat_df[cat_df['confidence'] == 'High'].groupby('category')),
            'medium_confidence': len(cat_df[cat_df['confidence'] == 'Medium'].groupby('category')),
            'low_confidence': len(cat_df[cat_df['confidence'] == 'Low'].groupby('category')),
            'created_at': datetime.now().isoformat()
        },
        {
            'model_version': 'V4',
            'level': 'Customer',
            'wmape': 89.3,
            'total_predictions': len(cust_df),
            'unique_entities': cust_df['customer_id'].nunique(),
            'high_confidence': len(cust_df[cust_df['confidence'] == 'High'].groupby('customer_id')),
            'medium_confidence': len(cust_df[cust_df['confidence'] == 'Medium'].groupby('customer_id')),
            'low_confidence': len(cust_df[cust_df['confidence'] == 'Low'].groupby('customer_id')),
            'created_at': datetime.now().isoformat()
        }
    ])
    eval_summary.to_csv(MODEL_DIR / 'bq_eval_v4_summary.csv', index=False)
    print(f"  ✓ Evaluation summary: {len(eval_summary)} rows")

    # Upload to BigQuery
    print("\n[3/5] Uploading SKU predictions...")
    upload_csv(
        MODEL_DIR / 'bq_sku_predictions_v4.csv',
        'predictions_sku_v4'
    )

    print("\n[4/5] Uploading Category predictions...")
    upload_csv(
        MODEL_DIR / 'bq_category_predictions_v4.csv',
        'predictions_category_v4'
    )

    print("\n[5/5] Uploading Customer predictions...")
    upload_csv(
        MODEL_DIR / 'bq_customer_predictions_v4.csv',
        'predictions_customer_v4'
    )

    # Upload evaluation summary
    print("\n  Uploading evaluation summary...")
    upload_csv(
        MODEL_DIR / 'bq_eval_v4_summary.csv',
        'eval_v4_summary'
    )

    print("\n" + "=" * 60)
    print("UPLOAD COMPLETE")
    print("=" * 60)
    print(f"""
New tables created in {DATASET}:
  - predictions_sku_v4      ({len(sku_df)} rows)
  - predictions_category_v4 ({len(cat_df)} rows)
  - predictions_customer_v4 ({len(cust_df)} rows)
  - eval_v4_summary         ({len(eval_summary)} rows)

Query example:
  SELECT * FROM `{PROJECT_ID}.{DATASET}.predictions_sku_v4`
  WHERE confidence = 'High'
  ORDER BY wmape
  LIMIT 10;

Compare versions:
  SELECT model_version, level, wmape
  FROM `{PROJECT_ID}.{DATASET}.eval_v4_summary`
  ORDER BY level, model_version;
""")

if __name__ == '__main__':
    main()
