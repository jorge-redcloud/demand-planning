#!/usr/bin/env python3
"""
Generate Dashboard Data V9
==========================
Uses GLOBAL model predictions (best performer: 62.6% WMAPE)

Run: python3 scripts/GENERATE_DASHBOARD_DATA_V9.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
MODEL_DIR = BASE_PATH / 'model_evaluation'
OUTPUT_FILE = BASE_PATH / 'dashboard_data_v9.js'

def calculate_wmape(actual, predicted):
    return 100 * np.sum(np.abs(actual - predicted)) / np.sum(actual) if np.sum(actual) > 0 else 999

def main():
    print("=" * 60)
    print("GENERATING DASHBOARD DATA V9")
    print("Using Global Model Predictions")
    print("=" * 60)

    # Load global model predictions
    print("\n[1/4] Loading data...")
    sku_preds = pd.read_csv(MODEL_DIR / 'sku_predictions_XGBoost.csv')
    weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
    products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')

    # Get descriptions from products (column is 'name' not 'description')
    sku_desc = products[['sku', 'name']].drop_duplicates().set_index('sku')['name'].to_dict()
    sku_cat = products[['sku', 'category_l1']].drop_duplicates().set_index('sku')['category_l1'].to_dict()

    print(f"  ✓ Predictions: {len(sku_preds)} rows, {sku_preds['sku'].nunique()} SKUs")

    # Get H1 actuals
    weekly['week_num'] = weekly['year_week'].str.extract(r'W(\d+)').astype(int)
    h1_data = weekly[weekly['week_num'] <= 26]
    print(f"  ✓ H1 actuals: {len(h1_data)} rows")

    # Calculate per-SKU metrics
    print("\n[2/4] Calculating SKU metrics...")
    sku_list = []

    for sku in sku_preds['sku'].unique():
        sku_pred = sku_preds[sku_preds['sku'] == sku]
        sku_h1 = h1_data[h1_data['sku'] == sku]

        # WMAPE
        wmape = calculate_wmape(sku_pred['actual'], sku_pred['predicted'])

        # H1 weeks count
        h1_weeks = len(sku_h1)

        # Confidence
        if wmape < 40 and h1_weeks >= 15:
            confidence = 'High'
        elif wmape < 60 and h1_weeks >= 10:
            confidence = 'Medium'
        else:
            confidence = 'Low'

        # Build H1 actuals series
        h1_actuals = {}
        for _, row in sku_h1.iterrows():
            h1_actuals[row['year_week']] = row['weekly_quantity']

        # Build H2 predictions/actuals series
        h2_actuals = {}
        h2_preds = {}
        for _, row in sku_pred.iterrows():
            h2_actuals[row['year_week']] = row['actual']
            h2_preds[row['year_week']] = round(row['predicted'], 1)

        sku_list.append({
            'sku': int(sku),
            'description': sku_desc.get(sku, f'SKU {sku}'),
            'category': sku_cat.get(sku, 'Unknown'),
            'wmape': round(wmape, 1),
            'h1_weeks': h1_weeks,
            'confidence': confidence,
            'h1_actuals': h1_actuals,
            'h2_actuals': h2_actuals,
            'h2_predictions': h2_preds
        })

    # Sort by confidence then WMAPE
    confidence_order = {'High': 0, 'Medium': 1, 'Low': 2}
    sku_list.sort(key=lambda x: (confidence_order[x['confidence']], x['wmape']))

    # Split into confidence lists
    sku_high = [s for s in sku_list if s['confidence'] == 'High']
    sku_medium = [s for s in sku_list if s['confidence'] == 'Medium']
    sku_low = [s for s in sku_list if s['confidence'] == 'Low']

    print(f"  ✓ High confidence: {len(sku_high)}")
    print(f"  ✓ Medium confidence: {len(sku_medium)}")
    print(f"  ✓ Low confidence: {len(sku_low)}")

    # Load category data
    print("\n[3/4] Processing Category data...")
    try:
        cat_preds = pd.read_csv(MODEL_DIR / 'category_predictions_XGBoost_v3.csv')
        cat_h1 = pd.read_csv(MODEL_DIR / 'category_h1_actuals_v3.csv')

        cat_list = []
        for cat in cat_preds['category'].unique():
            if pd.isna(cat):
                continue

            cat_pred = cat_preds[cat_preds['category'] == cat]
            cat_h1_data = cat_h1[cat_h1['category'] == cat]

            wmape = calculate_wmape(cat_pred['actual'], cat_pred['predicted'])
            h1_weeks = len(cat_h1_data)

            if wmape < 40 and h1_weeks >= 15:
                confidence = 'High'
            elif wmape < 60 and h1_weeks >= 10:
                confidence = 'Medium'
            else:
                confidence = 'Low'

            h1_actuals = dict(zip(cat_h1_data['year_week'], cat_h1_data['actual']))
            h2_actuals = dict(zip(cat_pred['year_week'], cat_pred['actual']))
            h2_preds = dict(zip(cat_pred['year_week'], cat_pred['predicted'].round(1)))

            cat_list.append({
                'category': cat,
                'wmape': round(wmape, 1),
                'h1_weeks': h1_weeks,
                'confidence': confidence,
                'h1_actuals': h1_actuals,
                'h2_actuals': h2_actuals,
                'h2_predictions': h2_preds
            })

        cat_list.sort(key=lambda x: (confidence_order[x['confidence']], x['wmape']))
        cat_high = [c for c in cat_list if c['confidence'] == 'High']
        cat_medium = [c for c in cat_list if c['confidence'] == 'Medium']
        cat_low = [c for c in cat_list if c['confidence'] == 'Low']

        print(f"  ✓ Categories: {len(cat_list)} (H:{len(cat_high)}, M:{len(cat_medium)}, L:{len(cat_low)})")
    except Exception as e:
        print(f"  ⚠ Could not load category data: {e}")
        cat_list = []
        cat_high = []
        cat_medium = []
        cat_low = []

    # Load customer data
    print("\n[4/4] Processing Customer data...")
    try:
        cust_preds = pd.read_csv(MODEL_DIR / 'customer_predictions_XGBoost_v3.csv')
        cust_h1 = pd.read_csv(MODEL_DIR / 'customer_h1_actuals_v3.csv')

        cust_list = []
        for cust in cust_preds['customer_id'].unique():
            cust_pred = cust_preds[cust_preds['customer_id'] == cust]
            cust_h1_data = cust_h1[cust_h1['customer_id'] == cust]

            wmape = calculate_wmape(cust_pred['actual'], cust_pred['predicted'])
            h1_weeks = len(cust_h1_data)

            if wmape < 40 and h1_weeks >= 15:
                confidence = 'High'
            elif wmape < 60 and h1_weeks >= 10:
                confidence = 'Medium'
            else:
                confidence = 'Low'

            # Get customer name
            cust_name = cust_pred['customer_name'].iloc[0] if 'customer_name' in cust_pred.columns else str(cust)

            h1_actuals = dict(zip(cust_h1_data['year_week'], cust_h1_data['actual']))
            h2_actuals = dict(zip(cust_pred['year_week'], cust_pred['actual']))
            h2_preds = dict(zip(cust_pred['year_week'], cust_pred['predicted'].round(1)))

            cust_list.append({
                'customer_id': str(cust),
                'customer_name': cust_name,
                'wmape': round(wmape, 1),
                'h1_weeks': h1_weeks,
                'confidence': confidence,
                'h1_actuals': h1_actuals,
                'h2_actuals': h2_actuals,
                'h2_predictions': h2_preds
            })

        cust_list.sort(key=lambda x: (confidence_order[x['confidence']], x['wmape']))
        cust_high = [c for c in cust_list if c['confidence'] == 'High']
        cust_medium = [c for c in cust_list if c['confidence'] == 'Medium']
        cust_low = [c for c in cust_list if c['confidence'] == 'Low']

        print(f"  ✓ Customers: {len(cust_list)} (H:{len(cust_high)}, M:{len(cust_medium)}, L:{len(cust_low)})")
    except Exception as e:
        print(f"  ⚠ Could not load customer data: {e}")
        cust_list = []
        cust_high = []
        cust_medium = []
        cust_low = []

    # Build dashboard data
    dashboard_data = {
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'model_version': 'Global XGBoost (62.6% WMAPE)',
        'skuListHigh': sku_high,
        'skuListMedium': sku_medium,
        'skuListLow': sku_low,
        'catListHigh': cat_high,
        'catListMedium': cat_medium,
        'catListLow': cat_low,
        'custListHigh': cust_high,
        'custListMedium': cust_medium,
        'custListLow': cust_low
    }

    # Write JavaScript file
    with open(OUTPUT_FILE, 'w') as f:
        f.write('// Dashboard Data V9 - Global Model\n')
        f.write(f'// Generated: {dashboard_data["generated"]}\n')
        f.write(f'// Model: {dashboard_data["model_version"]}\n\n')
        f.write('const dashboardData = ')
        f.write(json.dumps(dashboard_data, indent=2))
        f.write(';\n')

    print(f"\n✓ Saved: {OUTPUT_FILE}")
    print(f"  File size: {OUTPUT_FILE.stat().st_size / 1024:.1f} KB")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"SKUs:       {len(sku_list)} total (H:{len(sku_high)}, M:{len(sku_medium)}, L:{len(sku_low)})")
    print(f"Categories: {len(cat_list)} total (H:{len(cat_high)}, M:{len(cat_medium)}, L:{len(cat_low)})")
    print(f"Customers:  {len(cust_list)} total (H:{len(cust_high)}, M:{len(cust_medium)}, L:{len(cust_low)})")

if __name__ == '__main__':
    main()
