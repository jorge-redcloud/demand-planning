#!/usr/bin/env python3
"""
SYNC ALL MODEL VERSIONS TO BIGQUERY
====================================
Uploads V1, V2, V3, V4 predictions to BigQuery with proper versioning

Structure:
- predictions_sku: All SKU predictions (V1, V2, V3, V4) with model_version column
- predictions_category: All category predictions with model_version column
- predictions_customer: All customer predictions with model_version column
- eval_all_versions: Evaluation metrics for all versions

Run: python3 scripts/SYNC_ALL_VERSIONS_TO_BIGQUERY.py
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

def calculate_wmape(actual, predicted):
    """Calculate Weighted Mean Absolute Percentage Error"""
    total_actual = np.sum(actual)
    if total_actual == 0:
        return 999
    return 100 * np.sum(np.abs(actual - predicted)) / total_actual

def run_bq_command(query):
    """Run a BigQuery SQL command"""
    cmd = ["bq", "query", "--nouse_legacy_sql", "--project_id", PROJECT_ID, query]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    return result.returncode == 0, result.stdout, result.stderr

def run_bq_load(csv_path, table_name, write_disposition='WRITE_TRUNCATE'):
    """Upload CSV to BigQuery"""
    print(f"  → Uploading {csv_path.name} to {table_name}...")

    disposition_flag = "--replace" if write_disposition == 'WRITE_TRUNCATE' else "--noreplace"

    cmd = [
        "bq", "load",
        "--project_id", PROJECT_ID,
        "--source_format=CSV",
        "--skip_leading_rows=1",
        disposition_flag,
        "--autodetect",
        f"{DATASET}.{table_name}",
        str(csv_path)
    ]

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
    if result.returncode != 0:
        print(f"    ⚠ Error: {result.stderr[:300]}")
        return False
    print(f"    ✓ Done")
    return True

def load_products():
    """Load product info for descriptions"""
    products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')
    # Use category_l1 as category
    products['category'] = products['category_l1'] if 'category_l1' in products.columns else 'Unknown'
    return products[['sku', 'name', 'category']].drop_duplicates().set_index('sku').to_dict('index')

def main():
    print("=" * 70)
    print("SYNC ALL MODEL VERSIONS TO BIGQUERY")
    print(f"Dataset: {PROJECT_ID}.{DATASET}")
    print("=" * 70)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    product_info = load_products()

    # =========================================================================
    # PART 1: PREPARE ALL SKU PREDICTIONS
    # =========================================================================
    print("\n[1/5] Preparing SKU predictions (all versions)...")

    all_sku = []

    # V1 - Original XGBoost
    v1_file = MODEL_DIR / 'sku_predictions_XGBoost.csv'
    if v1_file.exists():
        df = pd.read_csv(v1_file)
        df['model_version'] = 'V1'
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
        df['pct_error'] = df['pct_error'].fillna(0)
        # Add description if missing
        if 'description' not in df.columns:
            df['description'] = df['sku'].apply(lambda x: product_info.get(x, {}).get('name', 'Unknown'))
        all_sku.append(df[['sku', 'description', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V1: {len(df)} rows, {df['sku'].nunique()} SKUs")

    # V2 - With patterns
    v2_file = MODEL_DIR / 'sku_predictions_XGBoost_v2.csv'
    if v2_file.exists():
        df = pd.read_csv(v2_file)
        df['model_version'] = 'V2'
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        if 'pct_error' not in df.columns:
            df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
            df['pct_error'] = df['pct_error'].fillna(0)
        if 'description' not in df.columns:
            df['description'] = df['sku'].apply(lambda x: product_info.get(x, {}).get('name', 'Unknown'))
        all_sku.append(df[['sku', 'description', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V2: {len(df)} rows, {df['sku'].nunique()} SKUs")

    # V3 - With outlier handling and W47
    v3_file = MODEL_DIR / 'sku_predictions_XGBoost_v3.csv'
    if v3_file.exists():
        df = pd.read_csv(v3_file)
        df['model_version'] = 'V3'
        if 'abs_error' not in df.columns:
            df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        if 'pct_error' not in df.columns:
            df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
            df['pct_error'] = df['pct_error'].fillna(0)
        if 'description' not in df.columns:
            df['description'] = df['sku'].apply(lambda x: product_info.get(x, {}).get('name', 'Unknown'))
        all_sku.append(df[['sku', 'description', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V3: {len(df)} rows, {df['sku'].nunique()} SKUs")

    # V4 - Per-SKU models with price features
    v4_file = MODEL_DIR / 'sku_predictions_v4.csv'
    if v4_file.exists():
        df = pd.read_csv(v4_file)
        df['model_version'] = 'V4'
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
        df['pct_error'] = df['pct_error'].fillna(0)
        all_sku.append(df[['sku', 'description', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V4: {len(df)} rows, {df['sku'].nunique()} SKUs")

    if all_sku:
        sku_combined = pd.concat(all_sku, ignore_index=True)
        sku_combined['sku'] = sku_combined['sku'].astype(str)
        sku_combined['uploaded_at'] = timestamp
        sku_combined.to_csv(BQ_UPLOAD_DIR / 'all_predictions_sku.csv', index=False)
        print(f"  → Combined: {len(sku_combined)} total SKU predictions")

    # =========================================================================
    # PART 2: PREPARE ALL CATEGORY PREDICTIONS
    # =========================================================================
    print("\n[2/5] Preparing Category predictions (all versions)...")

    all_cat = []

    # V1
    v1_file = MODEL_DIR / 'category_predictions_XGBoost.csv'
    if v1_file.exists():
        df = pd.read_csv(v1_file)
        df['model_version'] = 'V1'
        if 'abs_error' not in df.columns:
            df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        if 'pct_error' not in df.columns:
            df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
            df['pct_error'] = df['pct_error'].fillna(0)
        all_cat.append(df[['category', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V1: {len(df)} rows")

    # V2
    v2_file = MODEL_DIR / 'category_predictions_XGBoost_v2.csv'
    if v2_file.exists():
        df = pd.read_csv(v2_file)
        df['model_version'] = 'V2'
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        if 'pct_error' not in df.columns:
            df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
            df['pct_error'] = df['pct_error'].fillna(0)
        all_cat.append(df[['category', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V2: {len(df)} rows")

    # V3
    v3_file = MODEL_DIR / 'category_predictions_XGBoost_v3.csv'
    if v3_file.exists():
        df = pd.read_csv(v3_file)
        df['model_version'] = 'V3'
        if 'abs_error' not in df.columns:
            df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        if 'pct_error' not in df.columns:
            df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
            df['pct_error'] = df['pct_error'].fillna(0)
        all_cat.append(df[['category', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V3: {len(df)} rows")

    # V4
    v4_file = MODEL_DIR / 'category_predictions_v4.csv'
    if v4_file.exists():
        df = pd.read_csv(v4_file)
        df['model_version'] = 'V4'
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
        df['pct_error'] = df['pct_error'].fillna(0)
        all_cat.append(df[['category', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V4: {len(df)} rows")

    if all_cat:
        cat_combined = pd.concat(all_cat, ignore_index=True)
        cat_combined['uploaded_at'] = timestamp
        cat_combined.to_csv(BQ_UPLOAD_DIR / 'all_predictions_category.csv', index=False)
        print(f"  → Combined: {len(cat_combined)} total Category predictions")

    # =========================================================================
    # PART 3: PREPARE ALL CUSTOMER PREDICTIONS
    # =========================================================================
    print("\n[3/5] Preparing Customer predictions (all versions)...")

    all_cust = []

    # V1
    v1_file = MODEL_DIR / 'customer_predictions_XGBoost.csv'
    if v1_file.exists():
        df = pd.read_csv(v1_file)
        df['model_version'] = 'V1'
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
        df['pct_error'] = df['pct_error'].fillna(0)
        all_cust.append(df[['customer_id', 'customer_name', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V1: {len(df)} rows")

    # V2
    v2_file = MODEL_DIR / 'customer_predictions_XGBoost_v2.csv'
    if v2_file.exists():
        df = pd.read_csv(v2_file)
        df['model_version'] = 'V2'
        # Handle different column names
        if 'master_customer_id' in df.columns:
            df = df.rename(columns={'master_customer_id': 'customer_id'})
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        if 'pct_error' not in df.columns:
            df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
            df['pct_error'] = df['pct_error'].fillna(0)
        all_cust.append(df[['customer_id', 'customer_name', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V2: {len(df)} rows")

    # V3
    v3_file = MODEL_DIR / 'customer_predictions_XGBoost_v3.csv'
    if v3_file.exists():
        df = pd.read_csv(v3_file)
        df['model_version'] = 'V3'
        if 'abs_error' not in df.columns:
            df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        if 'pct_error' not in df.columns:
            df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
            df['pct_error'] = df['pct_error'].fillna(0)
        all_cust.append(df[['customer_id', 'customer_name', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V3: {len(df)} rows")

    # V4
    v4_file = MODEL_DIR / 'customer_predictions_v4.csv'
    if v4_file.exists():
        df = pd.read_csv(v4_file)
        df['model_version'] = 'V4'
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
        df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
        df['pct_error'] = df['pct_error'].fillna(0)
        all_cust.append(df[['customer_id', 'customer_name', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error', 'model_version']])
        print(f"  ✓ V4: {len(df)} rows")

    if all_cust:
        cust_combined = pd.concat(all_cust, ignore_index=True)
        cust_combined['customer_id'] = cust_combined['customer_id'].astype(str)
        cust_combined['uploaded_at'] = timestamp
        cust_combined.to_csv(BQ_UPLOAD_DIR / 'all_predictions_customer.csv', index=False)
        print(f"  → Combined: {len(cust_combined)} total Customer predictions")

    # =========================================================================
    # PART 4: CREATE EVALUATION SUMMARY FOR ALL VERSIONS
    # =========================================================================
    print("\n[4/5] Creating evaluation summary for all versions...")

    eval_rows = []

    # Calculate WMAPE for each version/level combination
    for version, sku_file, cat_file, cust_file in [
        ('V1', 'sku_predictions_XGBoost.csv', 'category_predictions_XGBoost.csv', 'customer_predictions_XGBoost.csv'),
        ('V2', 'sku_predictions_XGBoost_v2.csv', 'category_predictions_XGBoost_v2.csv', 'customer_predictions_XGBoost_v2.csv'),
        ('V3', 'sku_predictions_XGBoost_v3.csv', 'category_predictions_XGBoost_v3.csv', 'customer_predictions_XGBoost_v3.csv'),
        ('V4', 'sku_predictions_v4.csv', 'category_predictions_v4.csv', 'customer_predictions_v4.csv'),
    ]:
        # SKU
        f = MODEL_DIR / sku_file
        if f.exists():
            df = pd.read_csv(f)
            wmape = calculate_wmape(df['actual'], df['predicted'])
            eval_rows.append({
                'model_version': version,
                'level': 'SKU',
                'wmape': round(wmape, 2),
                'total_predictions': len(df),
                'unique_entities': df['sku'].nunique(),
                'uploaded_at': timestamp
            })

        # Category
        f = MODEL_DIR / cat_file
        if f.exists():
            df = pd.read_csv(f)
            wmape = calculate_wmape(df['actual'], df['predicted'])
            eval_rows.append({
                'model_version': version,
                'level': 'Category',
                'wmape': round(wmape, 2),
                'total_predictions': len(df),
                'unique_entities': df['category'].nunique(),
                'uploaded_at': timestamp
            })

        # Customer
        f = MODEL_DIR / cust_file
        if f.exists():
            df = pd.read_csv(f)
            cust_col = 'master_customer_id' if 'master_customer_id' in df.columns else 'customer_id'
            wmape = calculate_wmape(df['actual'], df['predicted'])
            eval_rows.append({
                'model_version': version,
                'level': 'Customer',
                'wmape': round(wmape, 2),
                'total_predictions': len(df),
                'unique_entities': df[cust_col].nunique(),
                'uploaded_at': timestamp
            })

    if eval_rows:
        eval_df = pd.DataFrame(eval_rows)
        eval_df.to_csv(BQ_UPLOAD_DIR / 'eval_all_versions.csv', index=False)
        print(f"  ✓ Evaluation summary: {len(eval_df)} rows")

        # Print summary
        print("\n  Model Performance Summary:")
        print("  " + "-" * 50)
        for _, row in eval_df.iterrows():
            print(f"  {row['model_version']:4s} {row['level']:10s}: WMAPE={row['wmape']:6.1f}%, {row['unique_entities']:4d} entities")

    # =========================================================================
    # PART 5: UPLOAD TO BIGQUERY
    # =========================================================================
    print("\n[5/5] Uploading to BigQuery...")

    uploads = [
        ('all_predictions_sku.csv', 'predictions_sku_all'),
        ('all_predictions_category.csv', 'predictions_category_all'),
        ('all_predictions_customer.csv', 'predictions_customer_all'),
        ('eval_all_versions.csv', 'eval_all_versions'),
    ]

    success_count = 0
    for csv_name, table_name in uploads:
        csv_path = BQ_UPLOAD_DIR / csv_name
        if csv_path.exists():
            if run_bq_load(csv_path, table_name):
                success_count += 1

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 70)
    print("UPLOAD COMPLETE")
    print("=" * 70)
    print(f"  Successfully uploaded: {success_count}/{len(uploads)} tables")
    print(f"  Dataset: {PROJECT_ID}.{DATASET}")
    print(f"""
Tables created:
  - predictions_sku_all      (V1, V2, V3, V4 SKU predictions)
  - predictions_category_all (V1, V2, V3, V4 Category predictions)
  - predictions_customer_all (V1, V2, V3, V4 Customer predictions)
  - eval_all_versions        (Evaluation metrics for all versions)

Query examples:
  -- Compare versions
  SELECT model_version, level, wmape
  FROM `{PROJECT_ID}.{DATASET}.eval_all_versions`
  ORDER BY level, model_version;

  -- SKU predictions by version
  SELECT model_version, COUNT(*) as rows, AVG(pct_error) as avg_error
  FROM `{PROJECT_ID}.{DATASET}.predictions_sku_all`
  GROUP BY model_version
  ORDER BY model_version;
""")

if __name__ == '__main__':
    main()
