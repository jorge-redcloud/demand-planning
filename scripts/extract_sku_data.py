#!/usr/bin/env python3
"""
SKU-Level Data Extraction for sku_demand_0 Model (v2)
======================================================
Handles multiple Excel file formats and extracts line-item data.

Output Tables:
- sku0_fact_lineitem.csv: Line-item transactions
- sku0_features_weekly.csv: Weekly SKU-level features with lags
- sku0_dim_products.csv: Product dimension with categories
- cat0_features_weekly.csv: Weekly category-level features
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
import re
warnings.filterwarnings('ignore')

# Configuration
BASE_PATH = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning")
DATA_PATH = BASE_PATH / "2025"
OUTPUT_SKU_PATH = BASE_PATH / "features_sku"
OUTPUT_CAT_PATH = BASE_PATH / "features_category"

OUTPUT_SKU_PATH.mkdir(exist_ok=True)
OUTPUT_CAT_PATH.mkdir(exist_ok=True)

MONTHS = [
    "January 2025", "February 2025", "March 2025", "April 2025",
    "May 2025", "June 2025", "July 2025", "August 2025",
    "September 2025", "October 2025", "November 2025", "December 2025"
]

REGIONS = {
    "CapeTown": "Cape Town",
    "Polokwane": "Polokwane",
    "George": "George",
    "Gauteng": "Gauteng",
    "Hardware": "Hardware"
}

def extract_region_from_filename(filename):
    for pattern, region in REGIONS.items():
        if pattern in filename:
            return region
    return "Unknown"

def find_summary_sheet(xl):
    """Find the summary sheet regardless of naming convention."""
    sheet_names = xl.sheet_names

    # Common patterns for summary sheets
    patterns = [
        'summary',
        'january', 'february', 'march', 'april', 'may', 'june',
        'july', 'august', 'september', 'october', 'november', 'december'
    ]

    for sheet in sheet_names:
        sheet_lower = sheet.lower()
        # Skip master files and debtors lists
        if 'debtor' in sheet_lower or 'master' in sheet_lower:
            continue
        # Check if it matches a summary pattern
        for pattern in patterns:
            if pattern in sheet_lower:
                return sheet

    # If still not found, return None
    return None

def extract_lineitems_from_file(filepath, region, month_date):
    """Extract line-item data from a single Excel file."""
    lineitems = []

    try:
        xl = pd.ExcelFile(filepath)

        # Find the summary sheet
        summary_sheet = find_summary_sheet(xl)
        if not summary_sheet:
            print(f"    Warning: No summary sheet found, using first sheet")
            summary_sheet = xl.sheet_names[0]

        # Read summary to get invoice metadata
        summary = pd.read_excel(xl, sheet_name=summary_sheet)

        # Normalize column names
        summary.columns = [str(c).strip().lower().replace(' ', '_').replace('.', '') for c in summary.columns]

        # Find the document number column
        doc_col = None
        for col in summary.columns:
            if 'document' in col or 'doc' in col:
                doc_col = col
                break

        # Find date column
        date_col = None
        for col in summary.columns:
            if col == 'date':
                date_col = col
                break

        # Find account column
        acc_col = None
        for col in summary.columns:
            if 'acc' in col:
                acc_col = col
                break

        # Find customer name column
        name_col = None
        for col in summary.columns:
            if 'debtor' in col or 'name' in col:
                name_col = col
                break

        # Create invoice lookup
        invoice_lookup = {}
        if doc_col:
            for _, row in summary.iterrows():
                doc_no = str(row.get(doc_col, '')).strip()
                if doc_no and doc_no != 'nan':
                    invoice_lookup[doc_no] = {
                        'date': row.get(date_col) if date_col else month_date,
                        'account': row.get(acc_col, '') if acc_col else '',
                        'customer_name': row.get(name_col, '') if name_col else ''
                    }

        # Process each invoice sheet
        processed_sheets = 0
        for sheet_name in xl.sheet_names:
            # Skip non-invoice sheets
            sheet_lower = sheet_name.lower()
            if any(x in sheet_lower for x in ['debtor', 'master', 'summary', 'sheet']):
                continue
            if any(month.lower()[:3] in sheet_lower for month in MONTHS):
                continue

            # Check if sheet name looks like an invoice number (mostly digits)
            clean_name = sheet_name.replace('.', '')
            if not (len(clean_name) >= 5 and clean_name.isdigit()):
                continue

            try:
                df = pd.read_excel(xl, sheet_name=sheet_name)

                # Normalize column names
                df.columns = [str(c).strip() for c in df.columns]

                # Find required columns
                stock_col = None
                for col in df.columns:
                    if 'stock' in col.lower() or col.lower() == 'sku':
                        stock_col = col
                        break

                desc_col = None
                for col in df.columns:
                    if 'desc' in col.lower():
                        desc_col = col
                        break

                qty_col = None
                for col in df.columns:
                    if 'quant' in col.lower() or col.lower() == 'qty':
                        qty_col = col
                        break

                price_col = None
                for col in df.columns:
                    if col.lower() == 'price':
                        price_col = col
                        break

                total_col = None
                for col in df.columns:
                    if 'total' in col.lower():
                        total_col = col
                        break

                if not stock_col or not qty_col:
                    continue

                # Get invoice metadata
                invoice_meta = invoice_lookup.get(sheet_name, {})
                invoice_date = invoice_meta.get('date', month_date)
                if pd.isna(invoice_date):
                    invoice_date = month_date
                customer_id = invoice_meta.get('account', '')
                customer_name = invoice_meta.get('customer_name', '')

                # Process line items
                for _, row in df.iterrows():
                    stock_code = row.get(stock_col)
                    if pd.isna(stock_code) or str(stock_code).strip() == '':
                        continue

                    qty = row.get(qty_col, 0) if qty_col else 0
                    if pd.isna(qty) or qty <= 0:
                        continue

                    try:
                        sku = str(int(float(stock_code)))
                    except:
                        sku = str(stock_code).strip()

                    lineitem = {
                        'invoice_id': sheet_name,
                        'order_date': invoice_date,
                        'customer_id': str(customer_id) if pd.notna(customer_id) else '',
                        'customer_name': str(customer_name) if pd.notna(customer_name) else '',
                        'region_name': region,
                        'sku': sku,
                        'description': str(row.get(desc_col, '')) if desc_col and pd.notna(row.get(desc_col)) else '',
                        'quantity': float(qty),
                        'unit_price': float(row.get(price_col, 0)) if price_col and pd.notna(row.get(price_col)) else 0,
                        'line_total': float(row.get(total_col, 0)) if total_col and pd.notna(row.get(total_col)) else 0,
                    }
                    lineitems.append(lineitem)

                processed_sheets += 1

            except Exception as e:
                continue

        print(f"    Processed {processed_sheets} invoice sheets")

    except Exception as e:
        print(f"    Error processing {filepath.name}: {e}")

    return lineitems

def load_product_master():
    """Load product master data."""
    products_file = BASE_PATH / "features" / "dim_products.csv"
    if products_file.exists():
        df = pd.read_csv(products_file)
        df['sku_clean'] = df['sku'].str.replace('ACP-', '', regex=False)
        return df.set_index('sku_clean').to_dict('index')
    return {}

def extract_all_lineitems():
    """Extract line items from all regional files."""
    all_lineitems = []

    print("=" * 60)
    print("SKU-LEVEL DATA EXTRACTION (v2)")
    print("=" * 60)

    for month_folder in MONTHS:
        month_path = DATA_PATH / month_folder
        if not month_path.exists():
            continue

        # Extract month date
        try:
            month_date = datetime.strptime(month_folder, "%B %Y")
        except:
            month_date = datetime.now()

        print(f"\nProcessing: {month_folder}")

        # Find regional files (prioritize -corrected versions)
        for filepath in month_path.glob("ZAF_ACA_*.xlsx"):
            # Skip non-corrected if corrected exists
            if '-corrected' not in filepath.name:
                corrected = filepath.parent / filepath.name.replace('.xlsx', '-corrected.xlsx')
                if corrected.exists():
                    continue

            # Skip .xlsm files
            if filepath.suffix == '.xlsm':
                continue

            region = extract_region_from_filename(filepath.name)
            print(f"  - {region}: {filepath.name}")

            lineitems = extract_lineitems_from_file(filepath, region, month_date)
            all_lineitems.extend(lineitems)
            print(f"    Extracted {len(lineitems)} line items")

    return pd.DataFrame(all_lineitems)

def engineer_sku_features(df_lineitems, products_lookup):
    """Engineer features at SKU level."""
    print("\n" + "=" * 60)
    print("FEATURE ENGINEERING - SKU LEVEL")
    print("=" * 60)

    df_lineitems['order_date'] = pd.to_datetime(df_lineitems['order_date'])
    df_lineitems['year_week'] = df_lineitems['order_date'].dt.strftime('%Y-W%V')

    # Aggregate to SKU-Week level
    sku_weekly = df_lineitems.groupby(['year_week', 'sku']).agg({
        'quantity': 'sum',
        'line_total': 'sum',
        'unit_price': 'mean',
        'invoice_id': 'nunique',
        'customer_id': 'nunique',
        'region_name': lambda x: x.mode()[0] if len(x) > 0 else 'Unknown'
    }).reset_index()

    sku_weekly.columns = [
        'year_week', 'sku', 'weekly_quantity', 'weekly_revenue',
        'avg_price', 'transaction_count', 'unique_customers', 'primary_region'
    ]

    # Temporal features
    sku_weekly['week_of_year'] = sku_weekly['year_week'].str.extract(r'W(\d+)').astype(int)
    sku_weekly['month'] = ((sku_weekly['week_of_year'] - 1) // 4) + 1
    sku_weekly['month'] = sku_weekly['month'].clip(1, 12)
    sku_weekly['quarter'] = ((sku_weekly['month'] - 1) // 3) + 1

    # Sort for lag calculations
    sku_weekly = sku_weekly.sort_values(['sku', 'year_week'])

    # Create lag features per SKU
    for lag in [1, 2, 4]:
        sku_weekly[f'quantity_lag_{lag}w'] = sku_weekly.groupby('sku')['weekly_quantity'].shift(lag)
        sku_weekly[f'revenue_lag_{lag}w'] = sku_weekly.groupby('sku')['weekly_revenue'].shift(lag)

    # Moving averages
    sku_weekly['quantity_ma_4w'] = sku_weekly.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.rolling(4, min_periods=1).mean()
    )
    sku_weekly['quantity_std_4w'] = sku_weekly.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.rolling(4, min_periods=1).std()
    )
    sku_weekly['quantity_diff_1w'] = sku_weekly.groupby('sku')['weekly_quantity'].diff()
    sku_weekly['price_change'] = sku_weekly.groupby('sku')['avg_price'].diff()

    # Add product attributes
    def get_product_attr(sku, attr, default='Unknown'):
        sku_clean = str(sku)
        if sku_clean in products_lookup:
            val = products_lookup[sku_clean].get(attr, default)
            return val if pd.notna(val) else default
        return default

    sku_weekly['brand'] = sku_weekly['sku'].apply(lambda x: get_product_attr(x, 'brand'))
    sku_weekly['category'] = sku_weekly['sku'].apply(
        lambda x: get_product_attr(x, 'categories', '').split('/')[-1] if get_product_attr(x, 'categories') else 'Unknown'
    )
    sku_weekly['manufacturer'] = sku_weekly['sku'].apply(lambda x: get_product_attr(x, 'manufacturer'))

    print(f"Generated {len(sku_weekly)} SKU-week records")
    print(f"Unique SKUs: {sku_weekly['sku'].nunique()}")
    print(f"Date range: {sku_weekly['year_week'].min()} to {sku_weekly['year_week'].max()}")

    return sku_weekly

def engineer_category_features(df_lineitems, products_lookup):
    """Engineer features at category level."""
    print("\n" + "=" * 60)
    print("FEATURE ENGINEERING - CATEGORY LEVEL")
    print("=" * 60)

    df_lineitems['order_date'] = pd.to_datetime(df_lineitems['order_date'])
    df_lineitems['year_week'] = df_lineitems['order_date'].dt.strftime('%Y-W%V')

    def get_category(sku):
        sku_clean = str(sku)
        if sku_clean in products_lookup:
            cats = products_lookup[sku_clean].get('categories', '')
            if cats and pd.notna(cats):
                parts = cats.split('/')
                return parts[-1] if parts else 'Unknown'
        return 'Unknown'

    df_lineitems['category'] = df_lineitems['sku'].apply(get_category)

    cat_weekly = df_lineitems.groupby(['year_week', 'category']).agg({
        'quantity': 'sum',
        'line_total': 'sum',
        'sku': 'nunique',
        'invoice_id': 'nunique',
        'customer_id': 'nunique'
    }).reset_index()

    cat_weekly.columns = [
        'year_week', 'category', 'weekly_quantity', 'weekly_revenue',
        'active_skus', 'transaction_count', 'unique_customers'
    ]

    cat_weekly['week_of_year'] = cat_weekly['year_week'].str.extract(r'W(\d+)').astype(int)
    cat_weekly['month'] = ((cat_weekly['week_of_year'] - 1) // 4) + 1
    cat_weekly['month'] = cat_weekly['month'].clip(1, 12)
    cat_weekly['quarter'] = ((cat_weekly['month'] - 1) // 3) + 1

    cat_weekly = cat_weekly.sort_values(['category', 'year_week'])

    for lag in [1, 2, 4]:
        cat_weekly[f'quantity_lag_{lag}w'] = cat_weekly.groupby('category')['weekly_quantity'].shift(lag)

    cat_weekly['quantity_ma_4w'] = cat_weekly.groupby('category')['weekly_quantity'].transform(
        lambda x: x.rolling(4, min_periods=1).mean()
    )

    print(f"Generated {len(cat_weekly)} category-week records")
    print(f"Unique categories: {cat_weekly['category'].nunique()}")

    return cat_weekly

def create_product_dimension(df_lineitems, products_lookup):
    """Create product dimension table."""
    print("\n" + "=" * 60)
    print("CREATING PRODUCT DIMENSION")
    print("=" * 60)

    unique_skus = df_lineitems[['sku', 'description']].drop_duplicates()

    def enrich_sku(row):
        sku_clean = str(row['sku'])
        if sku_clean in products_lookup:
            master = products_lookup[sku_clean]
            return pd.Series({
                'sku': f"ACP-{sku_clean}",
                'name': master.get('name', row['description']),
                'brand': master.get('brand', 'Unknown'),
                'manufacturer': master.get('manufacturer', 'Unknown'),
                'category_path': master.get('categories', ''),
                'price': master.get('price', 0),
                'fmcg': master.get('fmcg', 'Unknown')
            })
        return pd.Series({
            'sku': f"ACP-{sku_clean}",
            'name': row['description'],
            'brand': 'Unknown',
            'manufacturer': 'Unknown',
            'category_path': '',
            'price': 0,
            'fmcg': 'Unknown'
        })

    products_df = unique_skus.apply(enrich_sku, axis=1)

    def extract_categories(path):
        if not path or pd.isna(path):
            return 'Unknown', 'Unknown', 'Unknown'
        parts = [p.strip() for p in str(path).split('/') if p.strip()]
        cat_l1 = parts[1] if len(parts) > 1 else 'Unknown'
        cat_l2 = parts[2] if len(parts) > 2 else cat_l1
        cat_l3 = parts[-1] if len(parts) > 0 else 'Unknown'
        return cat_l1, cat_l2, cat_l3

    cats = products_df['category_path'].apply(extract_categories)
    products_df['category_l1'] = [c[0] for c in cats]
    products_df['category_l2'] = [c[1] for c in cats]
    products_df['category_l3'] = [c[2] for c in cats]

    print(f"Created product dimension with {len(products_df)} SKUs")
    print(f"Category L1 count: {products_df['category_l1'].nunique()}")

    return products_df

def main():
    print("Loading product master data...")
    products_lookup = load_product_master()
    print(f"Loaded {len(products_lookup)} products from master")

    df_lineitems = extract_all_lineitems()

    if len(df_lineitems) == 0:
        print("ERROR: No line items extracted!")
        return

    print(f"\n{'=' * 60}")
    print(f"TOTAL LINE ITEMS EXTRACTED: {len(df_lineitems):,}")
    print(f"{'=' * 60}")
    print(f"Unique SKUs: {df_lineitems['sku'].nunique()}")
    print(f"Unique Invoices: {df_lineitems['invoice_id'].nunique()}")
    print(f"Date range: {df_lineitems['order_date'].min()} to {df_lineitems['order_date'].max()}")
    print(f"Total quantity: {df_lineitems['quantity'].sum():,.0f} units")
    print(f"Total revenue: R{df_lineitems['line_total'].sum():,.2f}")
    print(f"\nBy Region:")
    print(df_lineitems.groupby('region_name')['quantity'].sum().to_string())

    sku_weekly = engineer_sku_features(df_lineitems.copy(), products_lookup)
    cat_weekly = engineer_category_features(df_lineitems.copy(), products_lookup)
    products_df = create_product_dimension(df_lineitems, products_lookup)

    print("\n" + "=" * 60)
    print("SAVING OUTPUTS")
    print("=" * 60)

    df_lineitems.to_csv(OUTPUT_SKU_PATH / "sku0_fact_lineitem.csv", index=False)
    print(f"✓ sku0_fact_lineitem.csv ({len(df_lineitems):,} rows)")

    sku_weekly.to_csv(OUTPUT_SKU_PATH / "sku0_features_weekly.csv", index=False)
    print(f"✓ sku0_features_weekly.csv ({len(sku_weekly):,} rows)")

    products_df.to_csv(OUTPUT_SKU_PATH / "sku0_dim_products.csv", index=False)
    print(f"✓ sku0_dim_products.csv ({len(products_df):,} rows)")

    cat_weekly.to_csv(OUTPUT_CAT_PATH / "cat0_features_weekly.csv", index=False)
    print(f"✓ cat0_features_weekly.csv ({len(cat_weekly):,} rows)")

    print("\n" + "=" * 60)
    print("EXTRACTION COMPLETE!")
    print("=" * 60)

    print("\n--- Sample SKU Features ---")
    print(sku_weekly[['year_week', 'sku', 'weekly_quantity', 'avg_price', 'quantity_lag_1w', 'category']].head(10).to_string())

if __name__ == "__main__":
    main()
