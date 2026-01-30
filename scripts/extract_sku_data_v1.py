#!/usr/bin/env python3
"""
SKU-Level Data Extraction v1 - WITH CUSTOMER DATA
==================================================
Extracts line-item data with proper customer linkage from Summary sheet.

Key Improvements over v0:
- Properly extracts customer_id (Account) from Summary sheet
- Extracts customer_name (Debtors Name) from Summary sheet
- Adds customer segmentation (bulk vs retail)
- Flags incomplete weeks
- Creates customer dimension table

Output Tables:
- v1_fact_lineitem.csv: Line-item transactions WITH customer data
- v1_features_weekly.csv: Weekly SKU-level features
- v1_features_sku_customer.csv: Weekly SKU × Customer features
- v1_dim_products.csv: Product dimension
- v1_dim_customers.csv: Customer dimension with segments
- v1_features_category.csv: Category-level features
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
import re
warnings.filterwarnings('ignore')

# Configuration - Use relative paths from script location
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent  # demand planning folder
DATA_PATH = BASE_PATH / "2025"
OUTPUT_PATH = BASE_PATH / "features_v1"
OUTPUT_PATH.mkdir(exist_ok=True)

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

# Expected weeks for completeness check
ALL_WEEKS_2025 = [f"2025-W{str(i).zfill(2)}" for i in range(1, 53)]


def extract_region_from_filename(filename):
    """Extract region from filename."""
    for pattern, region in REGIONS.items():
        if pattern in filename:
            return region
    return "Unknown"


def find_summary_sheet(xl):
    """Find the summary sheet regardless of naming convention."""
    sheet_names = xl.sheet_names

    # Priority patterns
    priority_patterns = ['summary']
    month_patterns = ['january', 'february', 'march', 'april', 'may', 'june',
                      'july', 'august', 'september', 'october', 'november', 'december']

    for sheet in sheet_names:
        sheet_lower = sheet.lower()
        # Skip master files and debtors lists
        if 'debtor' in sheet_lower or 'master' in sheet_lower:
            continue
        # Check priority patterns first
        for pattern in priority_patterns:
            if pattern in sheet_lower:
                return sheet

    # Then check month patterns
    for sheet in sheet_names:
        sheet_lower = sheet.lower()
        if 'debtor' in sheet_lower or 'master' in sheet_lower:
            continue
        for pattern in month_patterns:
            if pattern in sheet_lower:
                return sheet

    return None


def read_summary_sheet(xl, summary_sheet_name):
    """
    Read Summary sheet and extract invoice-to-customer mapping.

    Expected columns:
    - Document No. (float64) -> invoice_id
    - Date (datetime64) -> order_date
    - Account (float64) -> customer_id
    - Debtors Name (object) -> customer_name
    """
    try:
        summary = pd.read_excel(xl, sheet_name=summary_sheet_name)

        # Find columns (case-insensitive, handle variations)
        col_map = {}
        for col in summary.columns:
            col_lower = str(col).lower().strip()
            if 'document' in col_lower and 'no' in col_lower:
                col_map['invoice_id'] = col
            elif col_lower == 'date':
                col_map['date'] = col
            elif col_lower == 'account':
                col_map['customer_id'] = col
            elif 'debtor' in col_lower and 'name' in col_lower:
                col_map['customer_name'] = col
            elif col_lower == 'doc.total (incl)' or 'total' in col_lower:
                col_map['invoice_total'] = col

        # Create standardized dataframe
        invoice_lookup = {}
        for _, row in summary.iterrows():
            invoice_id = row.get(col_map.get('invoice_id'))
            if pd.notna(invoice_id):
                invoice_id_str = str(int(float(invoice_id)))
                invoice_lookup[invoice_id_str] = {
                    'order_date': row.get(col_map.get('date')),
                    'customer_id': str(int(float(row.get(col_map.get('customer_id'), 0)))) if pd.notna(row.get(col_map.get('customer_id'))) else '',
                    'customer_name': str(row.get(col_map.get('customer_name'), '')).strip() if pd.notna(row.get(col_map.get('customer_name'))) else '',
                    'invoice_total': float(row.get(col_map.get('invoice_total'), 0)) if pd.notna(row.get(col_map.get('invoice_total'))) else 0
                }

        return invoice_lookup
    except Exception as e:
        print(f"    Warning: Could not read Summary sheet: {e}")
        return {}


def read_debtors_masterfile(xl):
    """
    Read Debtors Masterfile for additional customer info.

    Expected columns:
    - ACC NO (int64) -> customer_id
    - NAME (object) -> customer_name
    - CONTACT PERSON (object)
    """
    try:
        # Find debtors sheet
        debtors_sheet = None
        for sheet in xl.sheet_names:
            if 'debtor' in sheet.lower() and 'master' in sheet.lower():
                debtors_sheet = sheet
                break

        if not debtors_sheet:
            return {}

        debtors = pd.read_excel(xl, sheet_name=debtors_sheet)

        # Find columns
        acc_col = None
        name_col = None
        contact_col = None

        for col in debtors.columns:
            col_lower = str(col).lower().strip()
            if 'acc' in col_lower and 'no' in col_lower:
                acc_col = col
            elif col_lower == 'name':
                name_col = col
            elif 'contact' in col_lower:
                contact_col = col

        if not acc_col:
            return {}

        customer_master = {}
        for _, row in debtors.iterrows():
            acc_no = row.get(acc_col)
            if pd.notna(acc_no):
                acc_id = str(int(float(acc_no)))
                customer_master[acc_id] = {
                    'master_name': str(row.get(name_col, '')).strip() if name_col and pd.notna(row.get(name_col)) else '',
                    'contact_person': str(row.get(contact_col, '')).strip() if contact_col and pd.notna(row.get(contact_col)) else ''
                }

        return customer_master
    except Exception as e:
        print(f"    Warning: Could not read Debtors Masterfile: {e}")
        return {}


def extract_lineitems_from_file(filepath, region, month_date):
    """Extract line-item data from a single Excel file with customer data."""
    lineitems = []
    customers_found = set()

    try:
        xl = pd.ExcelFile(filepath)

        # Find and read Summary sheet
        summary_sheet = find_summary_sheet(xl)
        if not summary_sheet:
            print(f"    Warning: No summary sheet found")
            return [], set()

        invoice_lookup = read_summary_sheet(xl, summary_sheet)
        customer_master = read_debtors_masterfile(xl)

        print(f"    Found {len(invoice_lookup)} invoices in Summary, {len(customer_master)} customers in Master")

        # Process each invoice sheet
        processed_sheets = 0
        for sheet_name in xl.sheet_names:
            # Skip non-invoice sheets
            sheet_lower = sheet_name.lower()
            if any(x in sheet_lower for x in ['debtor', 'master', 'summary', 'sheet']):
                continue
            if any(month.lower()[:3] in sheet_lower for month in MONTHS):
                continue

            # Check if sheet name looks like an invoice number
            clean_name = sheet_name.replace('.', '').replace(' ', '')
            if not (len(clean_name) >= 5 and clean_name.isdigit()):
                continue

            try:
                df = pd.read_excel(xl, sheet_name=sheet_name)

                # Find required columns
                col_map = {}
                for col in df.columns:
                    col_lower = str(col).lower().strip()
                    if 'stock' in col_lower or col_lower == 'sku':
                        col_map['sku'] = col
                    elif 'desc' in col_lower:
                        col_map['description'] = col
                    elif 'quant' in col_lower or col_lower == 'qty':
                        col_map['quantity'] = col
                    elif col_lower == 'price':
                        col_map['price'] = col
                    elif 'total' in col_lower:
                        col_map['total'] = col

                if 'sku' not in col_map or 'quantity' not in col_map:
                    continue

                # Get invoice metadata from Summary
                invoice_meta = invoice_lookup.get(clean_name, {})
                invoice_date = invoice_meta.get('order_date', month_date)
                if pd.isna(invoice_date):
                    invoice_date = month_date
                customer_id = invoice_meta.get('customer_id', '')
                customer_name = invoice_meta.get('customer_name', '')

                if customer_id:
                    customers_found.add(customer_id)

                # Enrich with master data
                master_info = customer_master.get(customer_id, {})
                if not customer_name and master_info.get('master_name'):
                    customer_name = master_info['master_name']

                # Process line items
                for _, row in df.iterrows():
                    stock_code = row.get(col_map['sku'])
                    if pd.isna(stock_code) or str(stock_code).strip() == '':
                        continue

                    qty = row.get(col_map['quantity'], 0)
                    if pd.isna(qty) or qty <= 0:
                        continue

                    try:
                        sku = str(int(float(stock_code)))
                    except:
                        sku = str(stock_code).strip()

                    price = row.get(col_map.get('price'), 0) if 'price' in col_map else 0
                    total = row.get(col_map.get('total'), 0) if 'total' in col_map else 0

                    lineitem = {
                        'invoice_id': clean_name,
                        'order_date': invoice_date,
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'region_name': region,
                        'sku': sku,
                        'description': str(row.get(col_map.get('description'), '')) if 'description' in col_map and pd.notna(row.get(col_map.get('description'))) else '',
                        'quantity': float(qty),
                        'unit_price': float(price) if pd.notna(price) else 0,
                        'line_total': float(total) if pd.notna(total) else 0,
                    }
                    lineitems.append(lineitem)

                processed_sheets += 1

            except Exception as e:
                continue

        print(f"    Processed {processed_sheets} invoice sheets, {len(customers_found)} unique customers")

    except Exception as e:
        print(f"    Error processing {filepath.name}: {e}")

    return lineitems, customers_found


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
    all_customers = set()

    print("=" * 70)
    print("SKU-LEVEL DATA EXTRACTION v1 - WITH CUSTOMER DATA")
    print("=" * 70)

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

        # Find regional files - handle multiple naming conventions
        # Pattern 1: ZAF_ACA_*.xlsx (Jan-May)
        # Pattern 2: ACA *.xlsx (June-Dec)
        all_files = list(month_path.glob("ZAF_ACA_*.xlsx")) + list(month_path.glob("ACA*.xlsx"))

        for filepath in all_files:
            # Skip non-corrected if corrected exists
            if '-corrected' not in filepath.name:
                corrected = filepath.parent / filepath.name.replace('.xlsx', '-corrected.xlsx')
                if corrected.exists():
                    continue

            if filepath.suffix == '.xlsm':
                continue

            region = extract_region_from_filename(filepath.name)
            print(f"  - {region}: {filepath.name}")

            lineitems, customers = extract_lineitems_from_file(filepath, region, month_date)
            all_lineitems.extend(lineitems)
            all_customers.update(customers)
            print(f"    Extracted {len(lineitems)} line items")

    print(f"\n{'=' * 70}")
    print(f"TOTAL: {len(all_lineitems):,} line items, {len(all_customers)} unique customers")
    print(f"{'=' * 70}")

    return pd.DataFrame(all_lineitems), all_customers


def create_customer_dimension(df_lineitems):
    """Create customer dimension with segmentation."""
    print("\n" + "=" * 70)
    print("CREATING CUSTOMER DIMENSION WITH SEGMENTATION")
    print("=" * 70)

    # Aggregate by customer
    customer_stats = df_lineitems.groupby('customer_id').agg({
        'invoice_id': 'nunique',
        'quantity': 'sum',
        'line_total': 'sum',
        'customer_name': 'first',
        'region_name': lambda x: x.mode()[0] if len(x) > 0 else 'Unknown'
    }).reset_index()

    customer_stats.columns = ['customer_id', 'total_orders', 'total_units', 'total_revenue', 'customer_name', 'primary_region']

    # Calculate average order size
    customer_stats['avg_order_units'] = customer_stats['total_units'] / customer_stats['total_orders']
    customer_stats['avg_order_value'] = customer_stats['total_revenue'] / customer_stats['total_orders']

    # Segment customers
    def segment_customer(row):
        avg_units = row['avg_order_units']
        if avg_units < 500:
            return 'Small Retailer'
        elif avg_units < 5000:
            return 'Medium Retailer'
        elif avg_units < 50000:
            return 'Large Retailer'
        else:
            return 'Bulk/Wholesale'

    customer_stats['customer_segment'] = customer_stats.apply(segment_customer, axis=1)

    # Order frequency
    def order_frequency(orders):
        if orders >= 8:
            return 'Weekly'
        elif orders >= 4:
            return 'Bi-weekly'
        elif orders >= 2:
            return 'Monthly'
        else:
            return 'Occasional'

    customer_stats['order_frequency'] = customer_stats['total_orders'].apply(order_frequency)

    print(f"\nCustomer Segments:")
    print(customer_stats.groupby('customer_segment').agg({
        'customer_id': 'count',
        'total_units': 'sum',
        'total_revenue': 'sum'
    }).rename(columns={'customer_id': 'count'}))

    return customer_stats


def flag_data_completeness(df_lineitems):
    """Flag weeks with incomplete data."""
    print("\n" + "=" * 70)
    print("FLAGGING DATA COMPLETENESS")
    print("=" * 70)

    df_lineitems['order_date'] = pd.to_datetime(df_lineitems['order_date'])
    df_lineitems['year_week'] = df_lineitems['order_date'].dt.strftime('%Y-W%V')

    # Calculate weekly stats
    weekly_stats = df_lineitems.groupby('year_week').agg({
        'quantity': 'sum',
        'invoice_id': 'nunique',
        'region_name': 'nunique',
        'sku': 'nunique'
    }).reset_index()
    weekly_stats.columns = ['year_week', 'total_units', 'invoices', 'regions', 'skus']

    # Determine thresholds (use median of high weeks)
    high_weeks = weekly_stats[weekly_stats['total_units'] > 1000000]
    if len(high_weeks) > 0:
        median_units = high_weeks['total_units'].median()
        median_invoices = high_weeks['invoices'].median()
    else:
        median_units = weekly_stats['total_units'].median()
        median_invoices = weekly_stats['invoices'].median()

    # Flag completeness
    def completeness_flag(row):
        if row['total_units'] > median_units * 0.5:
            return 'complete'
        elif row['total_units'] > median_units * 0.1:
            return 'partial'
        else:
            return 'minimal'

    weekly_stats['data_completeness'] = weekly_stats.apply(completeness_flag, axis=1)

    print("\nWeek Completeness:")
    print(weekly_stats[['year_week', 'total_units', 'invoices', 'regions', 'data_completeness']].to_string())

    # Add completeness flag to lineitems
    completeness_map = weekly_stats.set_index('year_week')['data_completeness'].to_dict()
    df_lineitems['data_completeness'] = df_lineitems['year_week'].map(completeness_map)

    return df_lineitems, weekly_stats


def engineer_sku_features(df_lineitems, products_lookup):
    """Engineer features at SKU level."""
    print("\n" + "=" * 70)
    print("FEATURE ENGINEERING - SKU LEVEL")
    print("=" * 70)

    df = df_lineitems.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year_week'] = df['order_date'].dt.strftime('%Y-W%V')

    # Aggregate to SKU-Week level
    sku_weekly = df.groupby(['year_week', 'sku']).agg({
        'quantity': 'sum',
        'line_total': 'sum',
        'unit_price': 'mean',
        'invoice_id': 'nunique',
        'customer_id': 'nunique',
        'region_name': lambda x: x.mode()[0] if len(x) > 0 else 'Unknown',
        'data_completeness': 'first'
    }).reset_index()

    sku_weekly.columns = [
        'year_week', 'sku', 'weekly_quantity', 'weekly_revenue',
        'avg_price', 'transaction_count', 'unique_customers', 'primary_region', 'data_completeness'
    ]

    # Temporal features
    sku_weekly['week_of_year'] = sku_weekly['year_week'].str.extract(r'W(\d+)').astype(int)
    sku_weekly['month'] = ((sku_weekly['week_of_year'] - 1) // 4) + 1
    sku_weekly['month'] = sku_weekly['month'].clip(1, 12)
    sku_weekly['quarter'] = ((sku_weekly['month'] - 1) // 3) + 1

    # Sort and create lags
    sku_weekly = sku_weekly.sort_values(['sku', 'year_week'])

    for lag in [1, 2, 4]:
        sku_weekly[f'quantity_lag_{lag}w'] = sku_weekly.groupby('sku')['weekly_quantity'].shift(lag)
        sku_weekly[f'revenue_lag_{lag}w'] = sku_weekly.groupby('sku')['weekly_revenue'].shift(lag)

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

    return sku_weekly


def engineer_sku_customer_features(df_lineitems, products_lookup):
    """Engineer features at SKU × Customer level."""
    print("\n" + "=" * 70)
    print("FEATURE ENGINEERING - SKU × CUSTOMER LEVEL")
    print("=" * 70)

    df = df_lineitems.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year_week'] = df['order_date'].dt.strftime('%Y-W%V')

    # Filter to customers with IDs
    df = df[df['customer_id'] != '']

    # Aggregate to SKU-Customer-Week level
    sku_cust_weekly = df.groupby(['year_week', 'sku', 'customer_id']).agg({
        'quantity': 'sum',
        'line_total': 'sum',
        'unit_price': 'mean',
        'invoice_id': 'nunique',
        'customer_name': 'first',
        'region_name': 'first',
        'data_completeness': 'first'
    }).reset_index()

    sku_cust_weekly.columns = [
        'year_week', 'sku', 'customer_id', 'weekly_quantity', 'weekly_revenue',
        'avg_price', 'order_count', 'customer_name', 'region_name', 'data_completeness'
    ]

    # Add temporal features
    sku_cust_weekly['week_of_year'] = sku_cust_weekly['year_week'].str.extract(r'W(\d+)').astype(int)

    # Add product attributes
    def get_category(sku):
        sku_clean = str(sku)
        if sku_clean in products_lookup:
            cats = products_lookup[sku_clean].get('categories', '')
            if cats and pd.notna(cats):
                return cats.split('/')[-1]
        return 'Unknown'

    sku_cust_weekly['category'] = sku_cust_weekly['sku'].apply(get_category)

    print(f"Generated {len(sku_cust_weekly)} SKU-Customer-Week records")

    return sku_cust_weekly


def engineer_category_features(df_lineitems, products_lookup):
    """Engineer features at category level."""
    print("\n" + "=" * 70)
    print("FEATURE ENGINEERING - CATEGORY LEVEL")
    print("=" * 70)

    df = df_lineitems.copy()
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year_week'] = df['order_date'].dt.strftime('%Y-W%V')

    def get_category(sku):
        sku_clean = str(sku)
        if sku_clean in products_lookup:
            cats = products_lookup[sku_clean].get('categories', '')
            if cats and pd.notna(cats):
                return cats.split('/')[-1]
        return 'Unknown'

    df['category'] = df['sku'].apply(get_category)

    cat_weekly = df.groupby(['year_week', 'category']).agg({
        'quantity': 'sum',
        'line_total': 'sum',
        'sku': 'nunique',
        'invoice_id': 'nunique',
        'customer_id': 'nunique',
        'data_completeness': 'first'
    }).reset_index()

    cat_weekly.columns = [
        'year_week', 'category', 'weekly_quantity', 'weekly_revenue',
        'active_skus', 'transaction_count', 'unique_customers', 'data_completeness'
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

    return cat_weekly


def create_product_dimension(df_lineitems, products_lookup):
    """Create product dimension table."""
    print("\n" + "=" * 70)
    print("CREATING PRODUCT DIMENSION")
    print("=" * 70)

    unique_skus = df_lineitems[['sku', 'description']].drop_duplicates()

    def enrich_sku(row):
        sku_clean = str(row['sku'])
        if sku_clean in products_lookup:
            master = products_lookup[sku_clean]
            return pd.Series({
                'sku': sku_clean,
                'name': master.get('name', row['description']),
                'brand': master.get('brand', 'Unknown'),
                'manufacturer': master.get('manufacturer', 'Unknown'),
                'category_path': master.get('categories', ''),
                'price': master.get('price', 0),
                'fmcg': master.get('fmcg', 'Unknown')
            })
        return pd.Series({
            'sku': sku_clean,
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

    return products_df


def main():
    print("Loading product master data...")
    products_lookup = load_product_master()
    print(f"Loaded {len(products_lookup)} products from master")

    # Extract all line items
    df_lineitems, all_customers = extract_all_lineitems()

    if len(df_lineitems) == 0:
        print("ERROR: No line items extracted!")
        return

    # Check customer extraction success
    customers_with_data = df_lineitems[df_lineitems['customer_id'] != '']['customer_id'].nunique()
    print(f"\n{'=' * 70}")
    print(f"CUSTOMER DATA EXTRACTION RESULTS")
    print(f"{'=' * 70}")
    print(f"Total line items: {len(df_lineitems):,}")
    print(f"Line items WITH customer_id: {len(df_lineitems[df_lineitems['customer_id'] != '']):,}")
    print(f"Line items WITHOUT customer_id: {len(df_lineitems[df_lineitems['customer_id'] == '']):,}")
    print(f"Unique customers extracted: {customers_with_data}")

    # Flag data completeness
    df_lineitems, weekly_stats = flag_data_completeness(df_lineitems)

    # Create customer dimension
    customer_dim = create_customer_dimension(df_lineitems[df_lineitems['customer_id'] != ''])

    # Add customer segment to lineitems
    segment_map = customer_dim.set_index('customer_id')['customer_segment'].to_dict()
    df_lineitems['customer_segment'] = df_lineitems['customer_id'].map(segment_map).fillna('Unknown')

    # Engineer features
    sku_weekly = engineer_sku_features(df_lineitems, products_lookup)
    sku_cust_weekly = engineer_sku_customer_features(df_lineitems, products_lookup)
    cat_weekly = engineer_category_features(df_lineitems, products_lookup)
    products_df = create_product_dimension(df_lineitems, products_lookup)

    # Save outputs
    print("\n" + "=" * 70)
    print("SAVING v1 OUTPUTS")
    print("=" * 70)

    df_lineitems.to_csv(OUTPUT_PATH / "v1_fact_lineitem.csv", index=False)
    print(f"✓ v1_fact_lineitem.csv ({len(df_lineitems):,} rows)")

    sku_weekly.to_csv(OUTPUT_PATH / "v1_features_weekly.csv", index=False)
    print(f"✓ v1_features_weekly.csv ({len(sku_weekly):,} rows)")

    sku_cust_weekly.to_csv(OUTPUT_PATH / "v1_features_sku_customer.csv", index=False)
    print(f"✓ v1_features_sku_customer.csv ({len(sku_cust_weekly):,} rows)")

    cat_weekly.to_csv(OUTPUT_PATH / "v1_features_category.csv", index=False)
    print(f"✓ v1_features_category.csv ({len(cat_weekly):,} rows)")

    customer_dim.to_csv(OUTPUT_PATH / "v1_dim_customers.csv", index=False)
    print(f"✓ v1_dim_customers.csv ({len(customer_dim):,} rows)")

    products_df.to_csv(OUTPUT_PATH / "v1_dim_products.csv", index=False)
    print(f"✓ v1_dim_products.csv ({len(products_df):,} rows)")

    weekly_stats.to_csv(OUTPUT_PATH / "v1_week_completeness.csv", index=False)
    print(f"✓ v1_week_completeness.csv ({len(weekly_stats):,} rows)")

    print("\n" + "=" * 70)
    print("v1 EXTRACTION COMPLETE!")
    print("=" * 70)

    print("\n--- Customer Segment Distribution ---")
    print(df_lineitems.groupby('customer_segment').agg({
        'quantity': 'sum',
        'line_total': 'sum'
    }).to_string())

    print("\n--- Data Completeness by Week ---")
    print(weekly_stats[['year_week', 'total_units', 'data_completeness']].to_string())


if __name__ == "__main__":
    main()
