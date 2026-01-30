#!/usr/bin/env python3
"""
GENERATE DASHBOARD DATA FOR ALL MODEL VERSIONS
===============================================
Creates dashboard_data_all_versions.js with V1, V2, V3, V4 data
for SKU, Category, and Customer levels.

The dashboard will have a version selector dropdown.
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
MODEL_DIR = BASE_PATH / 'model_evaluation'

def calculate_wmape(actual, predicted):
    """Calculate Weighted Mean Absolute Percentage Error"""
    total_actual = np.sum(actual)
    if total_actual == 0:
        return 999
    return 100 * np.sum(np.abs(actual - predicted)) / total_actual

def load_weekly_data():
    """Load the weekly aggregated data for H1/H2 context"""
    weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
    # Rename for consistency
    if 'weekly_quantity' in weekly.columns:
        weekly['total_quantity'] = weekly['weekly_quantity']
    return weekly

def load_category_weekly_data():
    """Load category-level weekly data for H1 context"""
    cat_file = FEATURES_DIR / 'v2_features_category.csv'
    if cat_file.exists():
        cat_weekly = pd.read_csv(cat_file)
        if 'weekly_quantity' in cat_weekly.columns:
            cat_weekly['total_quantity'] = cat_weekly['weekly_quantity']
        return cat_weekly
    return None

def load_customer_weekly_data():
    """Load customer-level weekly data for H1 context"""
    cust_file = FEATURES_DIR / 'v2_features_sku_customer.csv'
    if cust_file.exists():
        cust_data = pd.read_csv(cust_file)
        # Aggregate to customer level
        cust_weekly = cust_data.groupby(['customer_id', 'customer_name', 'year_week']).agg({
            'weekly_quantity': 'sum'
        }).reset_index()
        cust_weekly['total_quantity'] = cust_weekly['weekly_quantity']
        return cust_weekly
    return None

def get_h1_weeks():
    """H1 = weeks 1-26"""
    return [f"2025-W{w:02d}" for w in range(1, 27)]

def get_h2_weeks():
    """H2 = weeks 27-52"""
    return [f"2025-W{w:02d}" for w in range(27, 53)]

def get_all_weeks():
    """All weeks in 2025"""
    return [f"2025-W{w:02d}" for w in range(1, 53)]

def load_products():
    """Load product info"""
    products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')
    # Drop duplicates to ensure unique index
    products = products.drop_duplicates(subset=['sku'])
    return products.set_index('sku').to_dict('index')

def load_customers():
    """Load customer info"""
    customers = pd.read_csv(FEATURES_DIR / 'v2_dim_customers.csv')
    return customers

def process_sku_predictions(pred_file, weekly_data, product_info, version):
    """Process SKU predictions into dashboard format"""
    if not pred_file.exists():
        print(f"  ⚠ SKU file not found: {pred_file}")
        return None

    df = pd.read_csv(pred_file)
    h1_weeks = set(get_h1_weeks())
    h2_weeks = set(get_h2_weeks())

    # Ensure consistent columns
    if 'abs_error' not in df.columns:
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])
    if 'pct_error' not in df.columns:
        df['pct_error'] = 100 * df['abs_error'] / df['actual'].replace(0, np.nan)
        df['pct_error'] = df['pct_error'].fillna(0)

    # Build H1 data from weekly_data
    h1_data = weekly_data[weekly_data['year_week'].isin(h1_weeks)].copy()
    h1_by_sku = h1_data.groupby('sku').agg({
        'total_quantity': 'sum',
        'year_week': 'count'
    }).rename(columns={'year_week': 'h1_weeks', 'total_quantity': 'h1_total'})

    # Build SKU data structures
    sku_h1 = {}  # H1 actuals
    sku_h2 = {}  # H2 actuals and predictions
    sku_meta = {}  # Metadata per SKU

    # Get H1 actuals from weekly data
    for sku in df['sku'].unique():
        sku_str = str(sku)
        sku_weekly = weekly_data[weekly_data['sku'] == sku]

        # H1 data
        h1_rows = sku_weekly[sku_weekly['year_week'].isin(h1_weeks)]
        sku_h1[sku_str] = {
            row['year_week']: round(row['total_quantity'], 1)
            for _, row in h1_rows.iterrows()
        }

        # H2 data from predictions
        sku_preds = df[df['sku'] == sku]
        sku_h2[sku_str] = {
            'actual': {row['year_week']: round(row['actual'], 1) for _, row in sku_preds.iterrows()},
            'predicted': {row['year_week']: round(row['predicted'], 1) for _, row in sku_preds.iterrows()}
        }

        # Calculate WMAPE for this SKU
        if len(sku_preds) > 0:
            wmape = calculate_wmape(sku_preds['actual'].values, sku_preds['predicted'].values)
        else:
            wmape = 999

        # Get product info
        info = product_info.get(sku, {})
        h1_info = h1_by_sku.loc[sku] if sku in h1_by_sku.index else {'h1_weeks': 0, 'h1_total': 0}

        # Get model_type from predictions if available
        model_type = 'sku'  # default
        if 'model_type' in sku_preds.columns and len(sku_preds) > 0:
            model_type = sku_preds['model_type'].iloc[0]

        sku_meta[sku_str] = {
            'name': info.get('name', 'Unknown'),
            'category': info.get('category_l1', 'Unknown'),
            'wmape': round(wmape, 1),
            'h1_weeks': int(h1_info['h1_weeks']) if not pd.isna(h1_info['h1_weeks']) else 0,
            'h1_total': round(float(h1_info['h1_total']), 1) if not pd.isna(h1_info['h1_total']) else 0,
            'model_type': model_type
        }

    # Classify SKUs by confidence
    sku_list_high = []
    sku_list_medium = []
    sku_list_low = []

    for sku_str, meta in sku_meta.items():
        wmape = meta['wmape']
        h1_weeks = meta['h1_weeks']

        if wmape < 40 and h1_weeks >= 15:
            sku_list_high.append(sku_str)
        elif wmape < 60 and h1_weeks >= 10:
            sku_list_medium.append(sku_str)
        else:
            sku_list_low.append(sku_str)

    # Sort by WMAPE
    sku_list_high.sort(key=lambda x: sku_meta[x]['wmape'])
    sku_list_medium.sort(key=lambda x: sku_meta[x]['wmape'])
    sku_list_low.sort(key=lambda x: sku_meta[x]['wmape'])

    # Overall WMAPE
    overall_wmape = calculate_wmape(df['actual'].values, df['predicted'].values)

    return {
        'h1': sku_h1,
        'h2': sku_h2,
        'meta': sku_meta,
        'listHigh': sku_list_high,
        'listMedium': sku_list_medium,
        'listLow': sku_list_low,
        'wmape': round(overall_wmape, 1),
        'count': len(df['sku'].unique())
    }

def process_category_predictions(pred_file, cat_weekly_data, version):
    """Process category predictions into dashboard format"""
    if not pred_file.exists():
        print(f"  ⚠ Category file not found: {pred_file}")
        return None

    df = pd.read_csv(pred_file)
    h1_weeks = set(get_h1_weeks())

    if 'abs_error' not in df.columns:
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])

    cat_h1 = {}
    cat_h2 = {}
    cat_meta = {}

    for cat in df['category'].unique():
        cat_str = str(cat)

        # H1 - get from dedicated category weekly data
        if cat_weekly_data is not None:
            cat_rows = cat_weekly_data[cat_weekly_data['category'] == cat]
            h1_rows = cat_rows[cat_rows['year_week'].isin(h1_weeks)]
            h1_agg = h1_rows.groupby('year_week')['total_quantity'].sum()
            cat_h1[cat_str] = {wk: round(val, 1) for wk, val in h1_agg.items()}
        else:
            cat_h1[cat_str] = {}

        # H2 from predictions
        cat_preds = df[df['category'] == cat]
        cat_h2[cat_str] = {
            'actual': {row['year_week']: round(row['actual'], 1) for _, row in cat_preds.iterrows()},
            'predicted': {row['year_week']: round(row['predicted'], 1) for _, row in cat_preds.iterrows()}
        }

        # WMAPE
        wmape = calculate_wmape(cat_preds['actual'].values, cat_preds['predicted'].values) if len(cat_preds) > 0 else 999
        h1_weeks_count = len(h1_agg)

        cat_meta[cat_str] = {
            'wmape': round(wmape, 1),
            'h1_weeks': h1_weeks_count
        }

    # Classify
    cat_list_high = [c for c, m in cat_meta.items() if m['wmape'] < 40]
    cat_list_medium = [c for c, m in cat_meta.items() if 40 <= m['wmape'] < 60]
    cat_list_low = [c for c, m in cat_meta.items() if m['wmape'] >= 60]

    cat_list_high.sort(key=lambda x: cat_meta[x]['wmape'])
    cat_list_medium.sort(key=lambda x: cat_meta[x]['wmape'])
    cat_list_low.sort(key=lambda x: cat_meta[x]['wmape'])

    overall_wmape = calculate_wmape(df['actual'].values, df['predicted'].values)

    return {
        'h1': cat_h1,
        'h2': cat_h2,
        'meta': cat_meta,
        'listHigh': cat_list_high,
        'listMedium': cat_list_medium,
        'listLow': cat_list_low,
        'wmape': round(overall_wmape, 1),
        'count': len(df['category'].unique())
    }

def process_customer_predictions(pred_file, cust_weekly_data, customer_info, version):
    """Process customer predictions into dashboard format"""
    if not pred_file.exists():
        print(f"  ⚠ Customer file not found: {pred_file}")
        return None

    df = pd.read_csv(pred_file)
    h1_weeks = set(get_h1_weeks())

    # Handle column name variations
    if 'master_customer_id' in df.columns:
        df = df.rename(columns={'master_customer_id': 'customer_id'})

    if 'abs_error' not in df.columns:
        df['abs_error'] = np.abs(df['actual'] - df['predicted'])

    cust_h1 = {}
    cust_h2 = {}
    cust_meta = {}
    cust_names = {}

    # Get customer name mapping
    if 'customer_name' in df.columns:
        name_map = df.groupby('customer_id')['customer_name'].first().to_dict()
    else:
        name_map = {}

    for cust_id in df['customer_id'].unique():
        # Handle various customer ID formats
        try:
            if pd.notna(cust_id):
                cust_str = str(int(float(cust_id)))
            else:
                cust_str = str(cust_id)
        except (ValueError, TypeError):
            cust_str = str(cust_id)

        # H1 - get from dedicated customer weekly data
        if cust_weekly_data is not None:
            try:
                # Use string comparison directly since customer_id in weekly data is string
                cust_id_str = str(cust_id)
                cust_rows = cust_weekly_data[cust_weekly_data['customer_id'].astype(str) == cust_id_str]
                h1_rows = cust_rows[cust_rows['year_week'].isin(h1_weeks)]
                h1_agg = h1_rows.groupby('year_week')['total_quantity'].sum()
                cust_h1[cust_str] = {wk: round(val, 1) for wk, val in h1_agg.items()}
            except (ValueError, TypeError) as e:
                cust_h1[cust_str] = {}
        else:
            cust_h1[cust_str] = {}

        # H2 from predictions
        cust_preds = df[df['customer_id'] == cust_id]
        cust_h2[cust_str] = {
            'actual': {row['year_week']: round(row['actual'], 1) for _, row in cust_preds.iterrows()},
            'predicted': {row['year_week']: round(row['predicted'], 1) for _, row in cust_preds.iterrows()}
        }

        # WMAPE
        wmape = calculate_wmape(cust_preds['actual'].values, cust_preds['predicted'].values) if len(cust_preds) > 0 else 999

        cust_meta[cust_str] = {
            'wmape': round(wmape, 1),
            'h1_weeks': 0  # Customer H1 data not available at this level
        }

        cust_names[cust_str] = name_map.get(cust_id, f'Customer {cust_str}')

    # Classify
    cust_list_high = [c for c, m in cust_meta.items() if m['wmape'] < 40]
    cust_list_medium = [c for c, m in cust_meta.items() if 40 <= m['wmape'] < 80]
    cust_list_low = [c for c, m in cust_meta.items() if m['wmape'] >= 80]

    cust_list_high.sort(key=lambda x: cust_meta[x]['wmape'])
    cust_list_medium.sort(key=lambda x: cust_meta[x]['wmape'])
    cust_list_low.sort(key=lambda x: cust_meta[x]['wmape'])

    overall_wmape = calculate_wmape(df['actual'].values, df['predicted'].values)

    return {
        'h1': cust_h1,
        'h2': cust_h2,
        'meta': cust_meta,
        'names': cust_names,
        'listHigh': cust_list_high,
        'listMedium': cust_list_medium,
        'listLow': cust_list_low,
        'wmape': round(overall_wmape, 1),
        'count': len(df['customer_id'].unique())
    }

def main():
    print("=" * 70)
    print("GENERATING DASHBOARD DATA FOR ALL VERSIONS")
    print("=" * 70)

    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Load reference data
    print("\nLoading reference data...")
    weekly_data = load_weekly_data()
    cat_weekly_data = load_category_weekly_data()
    cust_weekly_data = load_customer_weekly_data()
    product_info = load_products()
    customer_info = load_customers()

    print(f"  SKU weekly: {len(weekly_data)} rows")
    if cat_weekly_data is not None:
        print(f"  Category weekly: {len(cat_weekly_data)} rows")
    if cust_weekly_data is not None:
        print(f"  Customer weekly: {len(cust_weekly_data)} rows")

    # Version file mappings
    versions = {
        'V1': {
            'sku': 'sku_predictions_XGBoost.csv',
            'category': 'category_predictions_XGBoost.csv',
            'customer': 'customer_predictions_XGBoost.csv'
        },
        'V2': {
            'sku': 'sku_predictions_XGBoost_v2.csv',
            'category': 'category_predictions_XGBoost_v2.csv',
            'customer': 'customer_predictions_XGBoost_v2.csv'
        },
        'V3': {
            'sku': 'sku_predictions_XGBoost_v3.csv',
            'category': 'category_predictions_XGBoost_v3.csv',
            'customer': 'customer_predictions_XGBoost_v3.csv'
        },
        'V4': {
            'sku': 'sku_predictions_v4.csv',
            'category': 'category_predictions_v4.csv',
            'customer': 'customer_predictions_v4.csv'
        }
    }

    all_data = {
        'generated': timestamp,
        'allWeeks': get_all_weeks(),
        'h1Weeks': get_h1_weeks(),
        'h2Weeks': get_h2_weeks(),
        'versions': {}
    }

    # Process each version
    for version, files in versions.items():
        print(f"\nProcessing {version}...")

        version_data = {}

        # SKU
        sku_file = MODEL_DIR / files['sku']
        sku_data = process_sku_predictions(sku_file, weekly_data, product_info, version)
        if sku_data:
            version_data['sku'] = sku_data
            print(f"  ✓ SKU: {sku_data['count']} SKUs, WMAPE={sku_data['wmape']}%")

        # Category
        cat_file = MODEL_DIR / files['category']
        cat_data = process_category_predictions(cat_file, cat_weekly_data, version)
        if cat_data:
            version_data['category'] = cat_data
            print(f"  ✓ Category: {cat_data['count']} categories, WMAPE={cat_data['wmape']}%")

        # Customer
        cust_file = MODEL_DIR / files['customer']
        cust_data = process_customer_predictions(cust_file, cust_weekly_data, customer_info, version)
        if cust_data:
            version_data['customer'] = cust_data
            print(f"  ✓ Customer: {cust_data['count']} customers, WMAPE={cust_data['wmape']}%")

        if version_data:
            all_data['versions'][version] = version_data

    # Write output
    output_file = BASE_PATH / 'dashboard_data_all_versions.js'

    print(f"\nWriting {output_file.name}...")

    with open(output_file, 'w') as f:
        f.write(f"// Dashboard Data - All Model Versions (V1, V2, V3, V4)\n")
        f.write(f"// Generated: {timestamp}\n")
        f.write("// Contains SKU, Category, and Customer predictions for all versions\n\n")
        f.write("const DASHBOARD_DATA = ")
        json.dump(all_data, f, indent=2)
        f.write(";\n")

    file_size = output_file.stat().st_size / (1024 * 1024)
    print(f"  ✓ Written: {file_size:.1f} MB")

    # Summary
    print("\n" + "=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"\nVersions processed: {list(all_data['versions'].keys())}")
    print("\nWMAPE by Version and Level:")
    print("-" * 40)
    for ver, data in all_data['versions'].items():
        sku_w = data.get('sku', {}).get('wmape', 'N/A')
        cat_w = data.get('category', {}).get('wmape', 'N/A')
        cust_w = data.get('customer', {}).get('wmape', 'N/A')
        print(f"  {ver}: SKU={sku_w}%, Category={cat_w}%, Customer={cust_w}%")

    print(f"\nOutput: {output_file}")

if __name__ == '__main__':
    main()
