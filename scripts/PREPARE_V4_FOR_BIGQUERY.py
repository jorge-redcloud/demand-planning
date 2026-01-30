#!/usr/bin/env python3
"""
Prepare V4 Predictions for BigQuery Upload
==========================================
Formats V4 predictions to match existing BigQuery table schemas

Run: python3 scripts/PREPARE_V4_FOR_BIGQUERY.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
MODEL_DIR = BASE_PATH / 'model_evaluation'
FEATURES_DIR = BASE_PATH / 'features_v2'
UPLOAD_DIR = BASE_PATH / 'bigquery_upload'
UPLOAD_DIR.mkdir(exist_ok=True)

def main():
    print("Preparing V4 data for BigQuery...")

    # Load product info for descriptions
    products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')
    sku_desc = products[['sku', 'name']].drop_duplicates().set_index('sku')['name'].to_dict()

    # =========================================================================
    # SKU Predictions - Match existing schema:
    # sku, description, year_week, actual, predicted, abs_error, pct_error, model_version
    # =========================================================================
    print("\n  Processing SKU predictions...")
    sku_v4 = pd.read_csv(MODEL_DIR / 'sku_predictions_v4.csv')

    # Calculate errors if not present
    sku_v4['abs_error'] = np.abs(sku_v4['actual'] - sku_v4['predicted'])
    sku_v4['pct_error'] = 100 * sku_v4['abs_error'] / sku_v4['actual'].replace(0, np.nan)
    sku_v4['pct_error'] = sku_v4['pct_error'].fillna(0)

    # Get description if not present
    if 'description' not in sku_v4.columns:
        sku_v4['description'] = sku_v4['sku'].map(sku_desc).fillna('Unknown')

    # Format for BigQuery (match existing schema)
    sku_bq = sku_v4[['sku', 'description', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error']].copy()
    sku_bq['model_version'] = 'V4'
    sku_bq['sku'] = sku_bq['sku'].astype(str)  # Match STRING type in BQ

    sku_bq.to_csv(UPLOAD_DIR / 'v4_predictions_sku.csv', index=False)
    print(f"    ✓ {len(sku_bq)} rows → v4_predictions_sku.csv")

    # =========================================================================
    # Category Predictions - Match existing schema
    # =========================================================================
    print("\n  Processing Category predictions...")
    cat_v4 = pd.read_csv(MODEL_DIR / 'category_predictions_v4.csv')

    cat_v4['abs_error'] = np.abs(cat_v4['actual'] - cat_v4['predicted'])
    cat_v4['pct_error'] = 100 * cat_v4['abs_error'] / cat_v4['actual'].replace(0, np.nan)
    cat_v4['pct_error'] = cat_v4['pct_error'].fillna(0)

    cat_bq = cat_v4[['category', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error']].copy()
    cat_bq['model_version'] = 'V4'

    cat_bq.to_csv(UPLOAD_DIR / 'v4_predictions_category.csv', index=False)
    print(f"    ✓ {len(cat_bq)} rows → v4_predictions_category.csv")

    # =========================================================================
    # Customer Predictions - Match existing schema
    # =========================================================================
    print("\n  Processing Customer predictions...")
    cust_v4 = pd.read_csv(MODEL_DIR / 'customer_predictions_v4.csv')

    cust_v4['abs_error'] = np.abs(cust_v4['actual'] - cust_v4['predicted'])
    cust_v4['pct_error'] = 100 * cust_v4['abs_error'] / cust_v4['actual'].replace(0, np.nan)
    cust_v4['pct_error'] = cust_v4['pct_error'].fillna(0)

    cust_bq = cust_v4[['customer_id', 'customer_name', 'year_week', 'actual', 'predicted', 'abs_error', 'pct_error']].copy()
    cust_bq['model_version'] = 'V4'
    cust_bq['customer_id'] = cust_bq['customer_id'].astype(str)

    cust_bq.to_csv(UPLOAD_DIR / 'v4_predictions_customer.csv', index=False)
    print(f"    ✓ {len(cust_bq)} rows → v4_predictions_customer.csv")

    # =========================================================================
    # V4 Evaluation Summary
    # =========================================================================
    print("\n  Creating V4 evaluation summary...")

    def calc_wmape(actual, predicted):
        return 100 * np.sum(np.abs(actual - predicted)) / np.sum(actual) if np.sum(actual) > 0 else 0

    eval_rows = [
        {
            'level': 'SKU',
            'model': 'XGBoost',
            'model_version': 'V4',
            'n_predictions': len(sku_v4),
            'mae': sku_v4['abs_error'].mean(),
            'median_mape': sku_v4['pct_error'].median(),
            'mean_mape': sku_v4['pct_error'].mean(),
            'wmape': calc_wmape(sku_v4['actual'], sku_v4['predicted']),
            'rmse': np.sqrt((sku_v4['abs_error'] ** 2).mean()),
            'unique_entities': sku_v4['sku'].nunique(),
            'high_confidence': (sku_v4.groupby('sku')['confidence'].first() == 'High').sum(),
            'medium_confidence': (sku_v4.groupby('sku')['confidence'].first() == 'Medium').sum(),
            'low_confidence': (sku_v4.groupby('sku')['confidence'].first() == 'Low').sum(),
        },
        {
            'level': 'Category',
            'model': 'XGBoost',
            'model_version': 'V4',
            'n_predictions': len(cat_v4),
            'mae': cat_v4['abs_error'].mean(),
            'median_mape': cat_v4['pct_error'].median(),
            'mean_mape': cat_v4['pct_error'].mean(),
            'wmape': calc_wmape(cat_v4['actual'], cat_v4['predicted']),
            'rmse': np.sqrt((cat_v4['abs_error'] ** 2).mean()),
            'unique_entities': cat_v4['category'].nunique(),
            'high_confidence': (cat_v4.groupby('category')['confidence'].first() == 'High').sum(),
            'medium_confidence': (cat_v4.groupby('category')['confidence'].first() == 'Medium').sum(),
            'low_confidence': (cat_v4.groupby('category')['confidence'].first() == 'Low').sum(),
        },
        {
            'level': 'Customer',
            'model': 'XGBoost',
            'model_version': 'V4',
            'n_predictions': len(cust_v4),
            'mae': cust_v4['abs_error'].mean(),
            'median_mape': cust_v4['pct_error'].median(),
            'mean_mape': cust_v4['pct_error'].mean(),
            'wmape': calc_wmape(cust_v4['actual'], cust_v4['predicted']),
            'rmse': np.sqrt((cust_v4['abs_error'] ** 2).mean()),
            'unique_entities': cust_v4['customer_id'].nunique(),
            'high_confidence': (cust_v4.groupby('customer_id')['confidence'].first() == 'High').sum(),
            'medium_confidence': (cust_v4.groupby('customer_id')['confidence'].first() == 'Medium').sum(),
            'low_confidence': (cust_v4.groupby('customer_id')['confidence'].first() == 'Low').sum(),
        }
    ]

    eval_df = pd.DataFrame(eval_rows)
    eval_df.to_csv(UPLOAD_DIR / 'v4_eval_summary.csv', index=False)
    print(f"    ✓ {len(eval_df)} rows → v4_eval_summary.csv")

    print("\n✅ All V4 files ready in bigquery_upload/")

    # Print summary
    print("\n" + "=" * 50)
    print("V4 MODEL SUMMARY")
    print("=" * 50)
    print(f"SKU:      WMAPE={eval_rows[0]['wmape']:.1f}%, {eval_rows[0]['unique_entities']} entities")
    print(f"Category: WMAPE={eval_rows[1]['wmape']:.1f}%, {eval_rows[1]['unique_entities']} entities")
    print(f"Customer: WMAPE={eval_rows[2]['wmape']:.1f}%, {eval_rows[2]['unique_entities']} entities")

if __name__ == '__main__':
    main()
