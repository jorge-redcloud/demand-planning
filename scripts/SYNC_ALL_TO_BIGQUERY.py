#!/usr/bin/env python3
"""
SYNC ALL DATA TO BIGQUERY
=========================
Uploads all model versions and source data to BigQuery

Structure:
- predictions_sku: All SKU predictions with model_version column
- predictions_category: All category predictions with model_version column
- predictions_customer: All customer predictions with model_version column
- features_sku_weekly: Clean feature data (source)
- features_category_weekly: Category features (source)
- features_customer_weekly: Customer features (source)
- dim_products: Product dimension
- dim_customers: Customer dimension
- eval_all_versions: Evaluation metrics for all versions

Run on Mac: python3 scripts/SYNC_ALL_TO_BIGQUERY.py
"""

import subprocess
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import sys

PROJECT_ID = "mimetic-maxim-443710-s2"
DATASET = "redai_demand_forecast"

SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
MODEL_DIR = BASE_PATH / 'model_evaluation'
BQ_UPLOAD_DIR = BASE_PATH / 'bigquery_upload'
BQ_UPLOAD_DIR.mkdir(exist_ok=True)

def run_bq_load(csv_path, table_name, write_disposition='WRITE_TRUNCATE'):
    """Upload CSV to BigQuery"""
    print(f"  → Uploading {csv_path.name} to {table_name}...")

    cmd = [
        "bq", "load",
        "--project_id", PROJECT_ID,
        "--source_format=CSV",
        "--skip_leading_rows=1",
        f"--{write_disposition.lower().replace('_', '-')}",
        "--autodetect",
        f"{DATASET}.{table_name}",
        str(csv_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        print(f"    ⚠ Error: {result.stderr[:200]}")
        return False
    print(f"    ✓ Done")
    return True

def calculate_wmape(actual, predicted):
    return 100 * np.sum(np.abs(actual - predicted)) / np.sum(actual) if np.sum(actual) > 0 else 999

def main():
    print("=" * 70)
    print("SYNC ALL DATA TO BIGQUERY")
    print(f"Dataset: {PROJECT_ID}.{DATASET}")
    print("=" * 70)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # =========================================================================
    # PART 1: PREPARE V4 PREDICTIONS
    # =========================================================================
    print("\n[1/6] Preparing V4 predictions...")

    # SKU V4
    if (MODEL_DIR / 'sku_predictions_v4.csv').exists():
        sku_v4 = pd.read_csv(MODEL_DIR / 'sku_predictions_v4.csv')
        sku_v4['model_version'] = 'V4'
        sku_v4['uploaded_at'] = timestamp
        sku_v4.to_csv(BQ_UPLOAD_DIR / 'predictions_sku_v4.csv', index=False)
        print(f"  ✓ SKU V4: {len(sku_v4)} rows, {sku_v4['sku'].nunique()} SKUs")
    else:
        print("  ⚠ SKU V4 not found - run TRAIN_V4_MODELS.py first")
        sku_v4 = None

    # Category V4
    if (MODEL_DIR / 'category_predictions_v4.csv').exists():
        cat_v4 = pd.read_csv(MODEL_DIR / 'category_predictions_v4.csv')
        cat_v4['model_version'] = 'V4'
        cat_v4['uploaded_at'] = timestamp
        cat_v4.to_csv(BQ_UPLOAD_DIR / 'predictions_category_v4.csv', index=False)
        print(f"  ✓ Category V4: {len(cat_v4)} rows")
    else:
        cat_v4 = None

    # Customer V4
    if (MODEL_DIR / 'customer_predictions_v4.csv').exists():
        cust_v4 = pd.read_csv(MODEL_DIR / 'customer_predictions_v4.csv')
        cust_v4['model_version'] = 'V4'
        cust_v4['uploaded_at'] = timestamp
        cust_v4.to_csv(BQ_UPLOAD_DIR / 'predictions_customer_v4.csv', index=False)
        print(f"  ✓ Customer V4: {len(cust_v4)} rows")
    else:
        cust_v4 = None

    # =========================================================================
    # PART 2: PREPARE SOURCE DATA (FEATURES)
    # =========================================================================
    print("\n[2/6] Preparing source data (features)...")

    # SKU Weekly Features
    if (FEATURES_DIR / 'v2_features_weekly.csv').exists():
        features_sku = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
        features_sku['uploaded_at'] = timestamp
        features_sku.to_csv(BQ_UPLOAD_DIR / 'features_sku_weekly.csv', index=False)
        print(f"  ✓ SKU Features: {len(features_sku)} rows")

    # Category Weekly Features
    if (FEATURES_DIR / 'v2_features_category.csv').exists():
        features_cat = pd.read_csv(FEATURES_DIR / 'v2_features_category.csv')
        features_cat['uploaded_at'] = timestamp
        features_cat.to_csv(BQ_UPLOAD_DIR / 'features_category_weekly.csv', index=False)
        print(f"  ✓ Category Features: {len(features_cat)} rows")

    # Customer Weekly Features
    if (FEATURES_DIR / 'v2_features_sku_customer.csv').exists():
        features_cust = pd.read_csv(FEATURES_DIR / 'v2_features_sku_customer.csv')
        features_cust['uploaded_at'] = timestamp
        features_cust.to_csv(BQ_UPLOAD_DIR / 'features_customer_weekly.csv', index=False)
        print(f"  ✓ Customer Features: {len(features_cust)} rows")

    # =========================================================================
    # PART 3: PREPARE DIMENSION TABLES
    # =========================================================================
    print("\n[3/6] Preparing dimension tables...")

    # Products
    if (FEATURES_DIR / 'v2_dim_products.csv').exists():
        dim_products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')
        dim_products['uploaded_at'] = timestamp
        dim_products.to_csv(BQ_UPLOAD_DIR / 'dim_products.csv', index=False)
        print(f"  ✓ Products: {len(dim_products)} rows")

    # Customers
    if (FEATURES_DIR / 'v2_dim_customers.csv').exists():
        dim_customers = pd.read_csv(FEATURES_DIR / 'v2_dim_customers.csv')
        dim_customers['uploaded_at'] = timestamp
        dim_customers.to_csv(BQ_UPLOAD_DIR / 'dim_customers.csv', index=False)
        print(f"  ✓ Customers: {len(dim_customers)} rows")

    # =========================================================================
    # PART 4: CREATE EVALUATION SUMMARY
    # =========================================================================
    print("\n[4/6] Creating evaluation summary...")

    eval_rows = []

    # V4 SKU
    if sku_v4 is not None:
        wmape = calculate_wmape(sku_v4['actual'], sku_v4['predicted'])
        conf_counts = sku_v4.groupby('sku')['confidence'].first().value_counts()
        eval_rows.append({
            'model_version': 'V4',
            'level': 'SKU',
            'wmape': round(wmape, 2),
            'total_predictions': len(sku_v4),
            'unique_entities': sku_v4['sku'].nunique(),
            'high_confidence': int(conf_counts.get('High', 0)),
            'medium_confidence': int(conf_counts.get('Medium', 0)),
            'low_confidence': int(conf_counts.get('Low', 0)),
            'training_approach': 'Per-SKU with global fallback',
            'features_used': 'lag1,lag2,lag4,rolling_avg,price,price_change,seasonality',
            'uploaded_at': timestamp
        })

    # V4 Category
    if cat_v4 is not None:
        wmape = calculate_wmape(cat_v4['actual'], cat_v4['predicted'])
        conf_counts = cat_v4.groupby('category')['confidence'].first().value_counts()
        eval_rows.append({
            'model_version': 'V4',
            'level': 'Category',
            'wmape': round(wmape, 2),
            'total_predictions': len(cat_v4),
            'unique_entities': cat_v4['category'].nunique(),
            'high_confidence': int(conf_counts.get('High', 0)),
            'medium_confidence': int(conf_counts.get('Medium', 0)),
            'low_confidence': int(conf_counts.get('Low', 0)),
            'training_approach': 'Per-Category XGBoost',
            'features_used': 'lag1,lag2,lag4,rolling_avg,price,week_num',
            'uploaded_at': timestamp
        })

    # V4 Customer
    if cust_v4 is not None:
        wmape = calculate_wmape(cust_v4['actual'], cust_v4['predicted'])
        conf_counts = cust_v4.groupby('customer_id')['confidence'].first().value_counts()
        eval_rows.append({
            'model_version': 'V4',
            'level': 'Customer',
            'wmape': round(wmape, 2),
            'total_predictions': len(cust_v4),
            'unique_entities': cust_v4['customer_id'].nunique(),
            'high_confidence': int(conf_counts.get('High', 0)),
            'medium_confidence': int(conf_counts.get('Medium', 0)),
            'low_confidence': int(conf_counts.get('Low', 0)),
            'training_approach': 'Per-Customer with global fallback',
            'features_used': 'lag1,lag2,lag4,rolling_avg,week_num',
            'uploaded_at': timestamp
        })

    if eval_rows:
        eval_df = pd.DataFrame(eval_rows)
        eval_df.to_csv(BQ_UPLOAD_DIR / 'eval_v4_summary.csv', index=False)
        print(f"  ✓ Evaluation summary: {len(eval_df)} rows")

    # =========================================================================
    # PART 5: UPLOAD TO BIGQUERY
    # =========================================================================
    print("\n[5/6] Uploading to BigQuery...")

    uploads = [
        ('predictions_sku_v4.csv', 'predictions_sku_v4'),
        ('predictions_category_v4.csv', 'predictions_category_v4'),
        ('predictions_customer_v4.csv', 'predictions_customer_v4'),
        ('features_sku_weekly.csv', 'features_sku_weekly'),
        ('features_category_weekly.csv', 'features_category_weekly'),
        ('features_customer_weekly.csv', 'features_customer_weekly'),
        ('dim_products.csv', 'dim_products'),
        ('dim_customers.csv', 'dim_customers'),
        ('eval_v4_summary.csv', 'eval_v4_summary'),
    ]

    success_count = 0
    for csv_name, table_name in uploads:
        csv_path = BQ_UPLOAD_DIR / csv_name
        if csv_path.exists():
            if run_bq_load(csv_path, table_name):
                success_count += 1

    # =========================================================================
    # PART 6: SUMMARY
    # =========================================================================
    print("\n[6/6] Summary")
    print("\n" + "=" * 70)
    print("UPLOAD COMPLETE")
    print("=" * 70)
    print(f"  Successfully uploaded: {success_count}/{len(uploads)} tables")
    print(f"  Dataset: {PROJECT_ID}.{DATASET}")
    print(f"""
Tables updated/created:
  - predictions_sku_v4      (V4 SKU predictions)
  - predictions_category_v4 (V4 Category predictions)
  - predictions_customer_v4 (V4 Customer predictions)
  - features_sku_weekly     (Source: SKU features)
  - features_category_weekly(Source: Category features)
  - features_customer_weekly(Source: Customer features)
  - dim_products            (Product dimension)
  - dim_customers           (Customer dimension)
  - eval_v4_summary         (V4 evaluation metrics)

Query example:
  SELECT model_version, level, wmape, high_confidence
  FROM `{PROJECT_ID}.{DATASET}.eval_v4_summary`
  ORDER BY level;
""")

if __name__ == '__main__':
    main()
