#!/usr/bin/env python3
"""
Generate Dashboard Data V10
===========================
Uses V4 model predictions - formatted for dashboard_v6.html

The dashboard expects DASHBOARD_DATA with:
- skuListHigh/Medium/Low: [{sku, description, wmape, confidence}, ...]
- skuH1[sku]: {week: value, ...}
- skuH2[sku]: {actual: {week: value}, predicted: {week: value}}
- skuMeta[sku]: {wmape, h1_weeks, confidence}
- allWeeks: ['2025-W01', ...]
- h1Weeks: ['2025-W01', ..., '2025-W26']

Run: python3 scripts/GENERATE_DASHBOARD_DATA_V10.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
MODEL_DIR = BASE_PATH / 'model_evaluation'
OUTPUT_FILE = BASE_PATH / 'dashboard_data_v10.js'

def main():
    print("=" * 60)
    print("GENERATING DASHBOARD DATA V10")
    print("Using V4 Model Predictions")
    print("=" * 60)

    # Load V4 predictions
    print("\n[1/5] Loading V4 predictions...")
    sku_preds = pd.read_csv(MODEL_DIR / 'sku_predictions_v4.csv')
    cat_preds = pd.read_csv(MODEL_DIR / 'category_predictions_v4.csv')
    cust_preds = pd.read_csv(MODEL_DIR / 'customer_predictions_v4.csv')

    # Load H1 actuals
    sku_h1 = pd.read_csv(MODEL_DIR / 'sku_h1_actuals_v4.csv')
    cat_h1 = pd.read_csv(MODEL_DIR / 'category_h1_actuals_v4.csv')
    cust_h1 = pd.read_csv(MODEL_DIR / 'customer_h1_actuals_v4.csv')

    print(f"  ✓ SKU predictions: {len(sku_preds)} rows, {sku_preds['sku'].nunique()} SKUs")
    print(f"  ✓ Category predictions: {len(cat_preds)} rows")
    print(f"  ✓ Customer predictions: {len(cust_preds)} rows")

    # Generate all weeks
    all_weeks = [f'2025-W{w:02d}' for w in range(1, 53)]
    h1_weeks = [f'2025-W{w:02d}' for w in range(1, 27)]

    confidence_order = {'High': 0, 'Medium': 1, 'Low': 2}

    # =========================================================================
    # Process SKUs
    # =========================================================================
    print("\n[2/5] Processing SKU data...")
    sku_list_high = []
    sku_list_medium = []
    sku_list_low = []
    sku_h1_data = {}
    sku_h2_data = {}
    sku_meta = {}

    for sku in sku_preds['sku'].unique():
        sku_pred = sku_preds[sku_preds['sku'] == sku]
        sku_actual = sku_h1[sku_h1['sku'] == sku]

        first_row = sku_pred.iloc[0]
        sku_id = str(int(sku))

        # Build H1 data
        h1_dict = {}
        for _, row in sku_actual.iterrows():
            h1_dict[row['year_week']] = float(row['weekly_quantity'])

        # Build H2 data
        h2_actual = {}
        h2_pred = {}
        for _, row in sku_pred.iterrows():
            h2_actual[row['year_week']] = float(row['actual'])
            h2_pred[row['year_week']] = float(row['predicted'])

        sku_h1_data[sku_id] = h1_dict
        sku_h2_data[sku_id] = {'actual': h2_actual, 'predicted': h2_pred}
        sku_meta[sku_id] = {
            'wmape': float(first_row['wmape']),
            'h1_weeks': int(first_row['h1_weeks']),
            'confidence': first_row['confidence'],
            'category': first_row.get('category', 'Unknown')
        }

        # List entry
        entry = {
            'id': sku_id,
            'sku': int(sku),
            'description': first_row.get('description', f'SKU {sku}'),
            'wmape': float(first_row['wmape']),
            'h1_weeks': int(first_row['h1_weeks']),
            'confidence': first_row['confidence'],
            'category': first_row.get('category', 'Unknown')
        }

        if first_row['confidence'] == 'High':
            sku_list_high.append(entry)
        elif first_row['confidence'] == 'Medium':
            sku_list_medium.append(entry)
        else:
            sku_list_low.append(entry)

    # Sort by WMAPE within each list
    sku_list_high.sort(key=lambda x: x['wmape'])
    sku_list_medium.sort(key=lambda x: x['wmape'])
    sku_list_low.sort(key=lambda x: x['wmape'])

    print(f"  ✓ High: {len(sku_list_high)}, Medium: {len(sku_list_medium)}, Low: {len(sku_list_low)}")

    # =========================================================================
    # Process Categories
    # =========================================================================
    print("\n[3/5] Processing Category data...")
    cat_list_high = []
    cat_list_medium = []
    cat_list_low = []
    cat_h1_data = {}
    cat_h2_data = {}
    cat_meta = {}

    for cat in cat_preds['category'].unique():
        cat_pred = cat_preds[cat_preds['category'] == cat]
        cat_actual = cat_h1[cat_h1['category'] == cat]

        first_row = cat_pred.iloc[0]
        cat_id = str(cat)

        h1_dict = {}
        for _, row in cat_actual.iterrows():
            h1_dict[row['year_week']] = float(row['weekly_quantity'])

        h2_actual = {}
        h2_pred = {}
        for _, row in cat_pred.iterrows():
            h2_actual[row['year_week']] = float(row['actual'])
            h2_pred[row['year_week']] = float(row['predicted'])

        cat_h1_data[cat_id] = h1_dict
        cat_h2_data[cat_id] = {'actual': h2_actual, 'predicted': h2_pred}
        cat_meta[cat_id] = {
            'wmape': float(first_row['wmape']),
            'h1_weeks': int(first_row['h1_weeks']),
            'confidence': first_row['confidence']
        }

        entry = {
            'id': cat_id,
            'category': cat,
            'wmape': float(first_row['wmape']),
            'h1_weeks': int(first_row['h1_weeks']),
            'confidence': first_row['confidence']
        }

        if first_row['confidence'] == 'High':
            cat_list_high.append(entry)
        elif first_row['confidence'] == 'Medium':
            cat_list_medium.append(entry)
        else:
            cat_list_low.append(entry)

    cat_list_high.sort(key=lambda x: x['wmape'])
    cat_list_medium.sort(key=lambda x: x['wmape'])
    cat_list_low.sort(key=lambda x: x['wmape'])

    print(f"  ✓ High: {len(cat_list_high)}, Medium: {len(cat_list_medium)}, Low: {len(cat_list_low)})")

    # =========================================================================
    # Process Customers
    # =========================================================================
    print("\n[4/5] Processing Customer data...")
    cust_list_high = []
    cust_list_medium = []
    cust_list_low = []
    cust_h1_data = {}
    cust_h2_data = {}
    cust_meta = {}

    for cust in cust_preds['customer_id'].unique():
        cust_pred = cust_preds[cust_preds['customer_id'] == cust]
        cust_actual = cust_h1[cust_h1['customer_id'] == cust]

        first_row = cust_pred.iloc[0]
        cust_id = str(cust)

        h1_dict = {}
        for _, row in cust_actual.iterrows():
            h1_dict[row['year_week']] = float(row['weekly_quantity'])

        h2_actual = {}
        h2_pred = {}
        for _, row in cust_pred.iterrows():
            h2_actual[row['year_week']] = float(row['actual'])
            h2_pred[row['year_week']] = float(row['predicted'])

        cust_h1_data[cust_id] = h1_dict
        cust_h2_data[cust_id] = {'actual': h2_actual, 'predicted': h2_pred}
        cust_meta[cust_id] = {
            'wmape': float(first_row['wmape']),
            'h1_weeks': int(first_row['h1_weeks']),
            'confidence': first_row['confidence']
        }

        entry = {
            'id': cust_id,
            'customer_id': cust_id,
            'customer_name': first_row.get('customer_name', str(cust)),
            'wmape': float(first_row['wmape']),
            'h1_weeks': int(first_row['h1_weeks']),
            'confidence': first_row['confidence']
        }

        if first_row['confidence'] == 'High':
            cust_list_high.append(entry)
        elif first_row['confidence'] == 'Medium':
            cust_list_medium.append(entry)
        else:
            cust_list_low.append(entry)

    cust_list_high.sort(key=lambda x: x['wmape'])
    cust_list_medium.sort(key=lambda x: x['wmape'])
    cust_list_low.sort(key=lambda x: x['wmape'])

    print(f"  ✓ High: {len(cust_list_high)}, Medium: {len(cust_list_medium)}, Low: {len(cust_list_low)}")

    # =========================================================================
    # Build final data structure
    # =========================================================================
    print("\n[5/5] Writing dashboard data...")

    dashboard_data = {
        'generated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'modelInfo': {
            'version': 'V4 Multi-Level',
            'skuWmape': 62.5,
            'catWmape': 54.3,
            'custWmape': 89.3
        },
        'allWeeks': all_weeks,
        'h1Weeks': h1_weeks,

        # SKU data
        'skuListHigh': sku_list_high,
        'skuListMedium': sku_list_medium,
        'skuListLow': sku_list_low,
        'skuH1': sku_h1_data,
        'skuH2': sku_h2_data,
        'skuMeta': sku_meta,

        # Category data
        'catListHigh': cat_list_high,
        'catListMedium': cat_list_medium,
        'catListLow': cat_list_low,
        'catH1': cat_h1_data,
        'catH2': cat_h2_data,
        'catMeta': cat_meta,

        # Customer data
        'custListHigh': cust_list_high,
        'custListMedium': cust_list_medium,
        'custListLow': cust_list_low,
        'custH1': cust_h1_data,
        'custH2': cust_h2_data,
        'custMeta': cust_meta
    }

    # Write JavaScript file - using DASHBOARD_DATA variable name
    with open(OUTPUT_FILE, 'w') as f:
        f.write('// Dashboard Data V10 - V4 Multi-Level Models\n')
        f.write(f'// Generated: {dashboard_data["generated"]}\n')
        f.write('// SKU WMAPE: 62.5% | Category WMAPE: 54.3% | Customer WMAPE: 89.3%\n\n')
        f.write('const DASHBOARD_DATA = ')
        f.write(json.dumps(dashboard_data, indent=2))
        f.write(';\n')

    file_size = OUTPUT_FILE.stat().st_size / 1024
    print(f"\n✓ Saved: {OUTPUT_FILE}")
    print(f"  File size: {file_size:.1f} KB")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"SKUs:       {len(sku_list_high) + len(sku_list_medium) + len(sku_list_low)} (H:{len(sku_list_high)}, M:{len(sku_list_medium)}, L:{len(sku_list_low)})")
    print(f"Categories: {len(cat_list_high) + len(cat_list_medium) + len(cat_list_low)} (H:{len(cat_list_high)}, M:{len(cat_list_medium)}, L:{len(cat_list_low)})")
    print(f"Customers:  {len(cust_list_high) + len(cust_list_medium) + len(cust_list_low)} (H:{len(cust_list_high)}, M:{len(cust_list_medium)}, L:{len(cust_list_low)})")

if __name__ == '__main__':
    main()
