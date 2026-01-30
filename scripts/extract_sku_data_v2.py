#!/usr/bin/env python3
"""
V2 SKU Data Extraction - Full Price Tracking + Buying Cycles
============================================================
Improvements over v1:
  - Fixed price column detection (UnitPrice, Price Incl., etc.)
  - Price history table (SKU × Week × Price tracking)
  - Buying cycle features per customer
  - Bulk buyer classification
  - Category hierarchy support

Output files:
  - v2_fact_lineitem.csv      - All transactions with prices
  - v2_price_history.csv      - SKU × Week price tracking
  - v2_customer_cycles.csv    - Customer buying patterns
  - v2_features_weekly.csv    - SKU × Week with price features
  - v2_features_sku_customer.csv - SKU × Customer × Week
  - v2_features_category.csv  - Category × Week
  - v2_dim_customers.csv      - Customer dimension with buyer type
  - v2_dim_products.csv       - Product dimension with price history
  - v2_week_completeness.csv  - Data quality flags
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent  # demand planning folder
OUTPUT_DIR = BASE_PATH / 'features_v2'

MONTHS = ['January', 'February', 'March', 'April', 'May', 'June',
          'July', 'August', 'September', 'October', 'November', 'December']

REGIONS = {
    'cape town': 'Cape Town',
    'capetown': 'Cape Town',
    'gauteng': 'Gauteng',
    'capital gauteng': 'Gauteng',
    'george': 'George',
    'hardware': 'Hardware',
    'polokwane': 'Polokwane',
    'limpopo': 'Polokwane'
}


def detect_price_column(df):
    """Detect price column with flexible matching"""
    for col in df.columns:
        col_lower = str(col).lower().strip()
        # Match: price, unitprice, unit_price, price incl, price incl., etc.
        if 'price' in col_lower and 'total' not in col_lower:
            return col
    return None


def detect_columns(df):
    """Detect all required columns with flexible matching"""
    col_map = {}
    for col in df.columns:
        col_lower = str(col).lower().strip().replace(' ', '').replace('.', '')

        # SKU/Stock code
        if any(x in col_lower for x in ['stockcode', 'stock_code', 'sku']):
            col_map['sku'] = col
        # Description
        elif 'desc' in col_lower:
            col_map['description'] = col
        # Quantity
        elif any(x in col_lower for x in ['quantity', 'qty', 'quant']):
            col_map['quantity'] = col
        # Unit Price (not total)
        elif 'price' in col_lower and 'total' not in col_lower:
            col_map['price'] = col
        # Line Total
        elif any(x in col_lower for x in ['total', 'linetotal', 'line_total', 'totalincl']):
            col_map['total'] = col

    return col_map


def get_region_from_filename(filename):
    """Extract region from filename"""
    name_lower = filename.lower()
    for key, region in REGIONS.items():
        if key in name_lower:
            return region
    return 'Unknown'


def find_summary_sheet(xl):
    """Find the Summary sheet in workbook"""
    for sheet in xl.sheet_names:
        sheet_lower = sheet.lower()
        if 'summary' in sheet_lower:
            return sheet
    return None


def read_summary_sheet(xl, sheet_name):
    """Read Summary sheet to get invoice-customer mappings"""
    try:
        df = pd.read_excel(xl, sheet_name=sheet_name)

        # Find columns
        date_col = None
        invoice_col = None
        customer_col = None
        account_col = None

        for col in df.columns:
            col_lower = str(col).lower().strip()
            if any(x in col_lower for x in ['date', 'inv date']):
                date_col = col
            elif any(x in col_lower for x in ['inv no', 'invoice', 'document']):
                invoice_col = col
            elif any(x in col_lower for x in ['debtor', 'customer', 'name']):
                customer_col = col
            elif any(x in col_lower for x in ['account', 'acc no', 'acc.no']):
                account_col = col

        if not invoice_col:
            return {}

        # Build lookup
        invoice_lookup = {}
        for _, row in df.iterrows():
            inv_no = row.get(invoice_col)
            if pd.isna(inv_no):
                continue

            inv_str = str(int(float(inv_no))) if isinstance(inv_no, (int, float)) else str(inv_no).strip()

            customer_id = ''
            if account_col and not pd.isna(row.get(account_col)):
                acc_val = row.get(account_col)
                if isinstance(acc_val, (int, float)):
                    customer_id = str(int(acc_val))
                else:
                    # Handle text account codes
                    customer_id = str(acc_val).strip()

            invoice_lookup[inv_str] = {
                'order_date': row.get(date_col) if date_col else None,
                'customer_id': customer_id,
                'customer_name': str(row.get(customer_col, '')).strip() if customer_col else ''
            }

        return invoice_lookup
    except Exception as e:
        print(f"      Error reading summary: {e}")
        return {}


def read_debtors_masterfile(xl):
    """Read Debtors Masterfile for customer info"""
    customer_master = {}
    for sheet in xl.sheet_names:
        if 'debtor' in sheet.lower() and 'master' in sheet.lower():
            try:
                df = pd.read_excel(xl, sheet_name=sheet)
                acc_col = None
                name_col = None

                for col in df.columns:
                    col_lower = str(col).lower()
                    if any(x in col_lower for x in ['acc', 'account', 'code']):
                        acc_col = col
                    elif any(x in col_lower for x in ['name', 'customer', 'debtor']):
                        name_col = col

                if acc_col and name_col:
                    for _, row in df.iterrows():
                        acc = row.get(acc_col)
                        if pd.isna(acc):
                            continue
                        acc_str = str(int(float(acc))) if isinstance(acc, (int, float)) else str(acc).strip()
                        customer_master[acc_str] = {
                            'master_name': str(row.get(name_col, '')).strip()
                        }
            except:
                pass
    return customer_master


def get_week_from_date(date_val, month_name, year=2025):
    """Convert date to ISO year-week"""
    if pd.isna(date_val):
        # Use first day of month as fallback
        month_num = MONTHS.index(month_name) + 1
        date_val = datetime(year, month_num, 1)
    elif isinstance(date_val, str):
        try:
            date_val = pd.to_datetime(date_val)
        except:
            month_num = MONTHS.index(month_name) + 1
            date_val = datetime(year, month_num, 1)

    if hasattr(date_val, 'isocalendar'):
        iso = date_val.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    return None


def process_file(filepath, month_name, year=2025):
    """Process a single Excel file - V2 with full price extraction"""
    region = get_region_from_filename(filepath.name)
    month_num = MONTHS.index(month_name) + 1
    month_date = datetime(year, month_num, 1)

    line_items = []
    customers_found = set()
    prices_captured = 0

    try:
        xl = pd.ExcelFile(filepath)

        # Find and read Summary sheet
        summary_sheet = find_summary_sheet(xl)
        if not summary_sheet:
            print(f"    Warning: No summary sheet found")
            return [], set(), 0

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

                # Use improved column detection
                col_map = detect_columns(df)

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
                        continue

                    # Get price - THIS IS THE KEY FIX
                    unit_price = 0
                    if 'price' in col_map:
                        price_val = row.get(col_map['price'], 0)
                        if not pd.isna(price_val):
                            try:
                                unit_price = float(price_val)
                                if unit_price > 0:
                                    prices_captured += 1
                            except:
                                unit_price = 0

                    # Calculate line total
                    line_total = 0
                    if 'total' in col_map:
                        total_val = row.get(col_map['total'], 0)
                        if not pd.isna(total_val):
                            try:
                                line_total = float(total_val)
                            except:
                                line_total = qty * unit_price
                    else:
                        line_total = qty * unit_price

                    # If we have total but no price, calculate price
                    if unit_price == 0 and line_total > 0 and qty > 0:
                        unit_price = line_total / qty
                        prices_captured += 1

                    description = str(row.get(col_map.get('description', ''), '')).strip()
                    year_week = get_week_from_date(invoice_date, month_name, year)

                    line_items.append({
                        'invoice_id': clean_name,
                        'order_date': invoice_date,
                        'customer_id': customer_id,
                        'customer_name': customer_name,
                        'region_name': region,
                        'sku': sku,
                        'description': description,
                        'quantity': float(qty),
                        'unit_price': unit_price,
                        'line_total': line_total,
                        'year_week': year_week
                    })

                processed_sheets += 1

            except Exception as e:
                continue

        print(f"    Processed {processed_sheets} invoice sheets, {len(line_items)} line items, {prices_captured} prices captured")

    except Exception as e:
        print(f"    Error processing file: {e}")

    return line_items, customers_found, prices_captured


def calculate_buying_cycles(df):
    """Calculate buying cycle features per customer"""
    cycles = []

    for customer_id in df['customer_id'].dropna().unique():
        if not customer_id:
            continue

        cust_data = df[df['customer_id'] == customer_id].copy()

        # Get unique order dates
        order_dates = pd.to_datetime(cust_data['order_date'].dropna().unique())
        order_dates = sorted(order_dates)

        if len(order_dates) < 2:
            avg_days_between = None
            cycle_regularity = 'One-time'
        else:
            # Calculate days between orders
            gaps = [(order_dates[i+1] - order_dates[i]).days for i in range(len(order_dates)-1)]
            avg_days_between = np.mean(gaps)
            std_days = np.std(gaps) if len(gaps) > 1 else 0

            # Classify regularity
            if avg_days_between <= 7 and std_days < 3:
                cycle_regularity = 'Weekly'
            elif avg_days_between <= 14 and std_days < 5:
                cycle_regularity = 'Bi-weekly'
            elif avg_days_between <= 35 and std_days < 10:
                cycle_regularity = 'Monthly'
            elif std_days > 20:
                cycle_regularity = 'Irregular'
            else:
                cycle_regularity = 'Sporadic'

        # Get SKU patterns
        top_skus = cust_data.groupby('sku')['quantity'].sum().nlargest(5).index.tolist()

        # Get total metrics
        total_orders = cust_data['invoice_id'].nunique()
        total_units = cust_data['quantity'].sum()
        total_revenue = cust_data['line_total'].sum()
        avg_order_value = total_revenue / total_orders if total_orders > 0 else 0

        # Classify buyer type
        if total_units > 100000:
            buyer_type = 'Bulk Buyer'
        elif avg_order_value > 50000:
            buyer_type = 'High-Value Buyer'
        elif total_orders >= 40:
            buyer_type = 'Frequent Buyer'
        elif total_orders >= 10:
            buyer_type = 'Regular Buyer'
        else:
            buyer_type = 'Occasional Buyer'

        cycles.append({
            'customer_id': customer_id,
            'customer_name': cust_data['customer_name'].iloc[0] if len(cust_data) > 0 else '',
            'primary_region': cust_data['region_name'].mode().iloc[0] if len(cust_data) > 0 else '',
            'total_orders': total_orders,
            'total_units': total_units,
            'total_revenue': total_revenue,
            'avg_order_value': avg_order_value,
            'avg_days_between_orders': avg_days_between,
            'cycle_regularity': cycle_regularity,
            'buyer_type': buyer_type,
            'top_skus': ','.join(top_skus[:5]),
            'first_order': min(order_dates).strftime('%Y-%m-%d') if order_dates else None,
            'last_order': max(order_dates).strftime('%Y-%m-%d') if order_dates else None,
            'active_weeks': cust_data['year_week'].nunique()
        })

    return pd.DataFrame(cycles)


def create_price_history(df):
    """Create SKU × Week price history table"""
    # Group by SKU and week, capture all price points
    price_data = df[df['unit_price'] > 0].groupby(['sku', 'year_week']).agg({
        'unit_price': ['mean', 'min', 'max', 'std', 'count'],
        'quantity': 'sum',
        'line_total': 'sum'
    }).reset_index()

    price_data.columns = ['sku', 'year_week', 'avg_price', 'min_price', 'max_price',
                          'price_std', 'price_observations', 'weekly_quantity', 'weekly_revenue']

    # Fill std with 0 where single observation
    price_data['price_std'] = price_data['price_std'].fillna(0)

    # Add price change indicators
    price_data = price_data.sort_values(['sku', 'year_week'])
    price_data['prev_avg_price'] = price_data.groupby('sku')['avg_price'].shift(1)
    price_data['price_change'] = price_data['avg_price'] - price_data['prev_avg_price']
    price_data['price_change_pct'] = (price_data['price_change'] / price_data['prev_avg_price'] * 100).round(2)

    return price_data


def segment_customer(row):
    """Classify customer by buying behavior"""
    total_units = row.get('total_units', 0)
    total_orders = row.get('total_orders', 0)
    avg_order = total_units / total_orders if total_orders > 0 else 0

    if total_units > 100000 or avg_order > 10000:
        return 'Bulk/Wholesale'
    elif avg_order > 2000:
        return 'Large Retailer'
    elif avg_order > 500:
        return 'Medium Retailer'
    else:
        return 'Small Retailer'


def main():
    print("=" * 60)
    print("V2 SKU DATA EXTRACTION")
    print("With Full Price Tracking + Buying Cycles")
    print("=" * 60)

    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Find all source files
    data_path = BASE_PATH / '2025'
    all_line_items = []
    all_customers = set()
    total_prices = 0

    for month_folder in sorted(data_path.iterdir()):
        if not month_folder.is_dir():
            continue

        month_name = month_folder.name.replace(' 2025', '')
        if month_name not in MONTHS:
            continue

        print(f"\n📁 Processing {month_folder.name}...")

        # Find Excel files - handle both naming patterns
        all_files = list(month_folder.glob("ZAF_ACA_*.xlsx")) + list(month_folder.glob("ACA*.xlsx"))

        for filepath in all_files:
            if filepath.name.startswith('~$'):
                continue
            print(f"  📄 {filepath.name}")
            items, customers, prices = process_file(filepath, month_name)
            all_line_items.extend(items)
            all_customers.update(customers)
            total_prices += prices

    print(f"\n{'=' * 60}")
    print(f"EXTRACTION COMPLETE")
    print(f"{'=' * 60}")
    print(f"Total line items: {len(all_line_items):,}")
    print(f"Prices captured: {total_prices:,} ({total_prices/len(all_line_items)*100:.1f}%)")
    print(f"Unique customers found: {len(all_customers):,}")

    # Create main DataFrame
    df = pd.DataFrame(all_line_items)

    # === CREATE OUTPUTS ===

    # 1. Fact table with data completeness
    print("\n📊 Creating fact table...")
    week_counts = df.groupby('year_week').size()
    median_count = week_counts.median()

    def get_completeness(week):
        count = week_counts.get(week, 0)
        if count >= median_count * 0.8:
            return 'complete'
        elif count >= median_count * 0.3:
            return 'partial'
        return 'minimal'

    df['data_completeness'] = df['year_week'].apply(get_completeness)

    # 2. Price History Table
    print("📊 Creating price history table...")
    price_history = create_price_history(df)
    print(f"   Price history: {len(price_history):,} SKU×Week records with prices")

    # 3. Customer Buying Cycles
    print("📊 Calculating buying cycles...")
    customer_cycles = calculate_buying_cycles(df)
    print(f"   Customer cycles: {len(customer_cycles):,} customers analyzed")

    # Add buyer type to customers
    customer_cycles['customer_segment'] = customer_cycles.apply(segment_customer, axis=1)

    # Merge segment back to fact table
    segment_map = customer_cycles.set_index('customer_id')['customer_segment'].to_dict()
    buyer_type_map = customer_cycles.set_index('customer_id')['buyer_type'].to_dict()
    df['customer_segment'] = df['customer_id'].map(segment_map)
    df['buyer_type'] = df['customer_id'].map(buyer_type_map)

    # 4. Weekly Features with Price
    print("📊 Creating weekly features...")
    weekly_features = df.groupby(['sku', 'year_week']).agg({
        'quantity': 'sum',
        'unit_price': 'mean',
        'line_total': 'sum',
        'invoice_id': 'nunique',
        'customer_id': 'nunique',
        'description': 'first',
        'data_completeness': 'first'
    }).reset_index()

    weekly_features.columns = ['sku', 'year_week', 'weekly_quantity', 'avg_unit_price',
                               'weekly_revenue', 'order_count', 'unique_customers',
                               'description', 'data_completeness']

    # Add lag features
    weekly_features = weekly_features.sort_values(['sku', 'year_week'])
    for lag in [1, 2, 4]:
        weekly_features[f'lag{lag}_quantity'] = weekly_features.groupby('sku')['weekly_quantity'].shift(lag)
        weekly_features[f'lag{lag}_price'] = weekly_features.groupby('sku')['avg_unit_price'].shift(lag)

    weekly_features['rolling_avg_4w'] = weekly_features.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.rolling(4, min_periods=1).mean()
    )
    weekly_features['price_rolling_avg_4w'] = weekly_features.groupby('sku')['avg_unit_price'].transform(
        lambda x: x.rolling(4, min_periods=1).mean()
    )

    # Price change indicator
    weekly_features['price_change'] = weekly_features['avg_unit_price'] - weekly_features['lag1_price']
    weekly_features['price_change_pct'] = (weekly_features['price_change'] / weekly_features['lag1_price'] * 100).round(2)

    # 5. SKU × Customer × Week Features
    print("📊 Creating SKU×Customer features...")
    sku_customer = df.groupby(['sku', 'customer_id', 'year_week']).agg({
        'quantity': 'sum',
        'unit_price': 'mean',
        'line_total': 'sum',
        'invoice_id': 'nunique',
        'customer_name': 'first',
        'customer_segment': 'first',
        'buyer_type': 'first',
        'data_completeness': 'first'
    }).reset_index()

    sku_customer.columns = ['sku', 'customer_id', 'year_week', 'weekly_quantity',
                            'avg_unit_price', 'weekly_revenue', 'order_count',
                            'customer_name', 'customer_segment', 'buyer_type', 'data_completeness']

    # 6. Category Features
    print("📊 Creating category features...")
    # Load product dimension for categories
    products_v1 = pd.read_csv(BASE_PATH / 'features_v1' / 'v1_dim_products.csv')
    # Handle duplicate SKUs by keeping first occurrence
    products_v1_dedup = products_v1.drop_duplicates(subset='sku', keep='first')
    sku_category = products_v1_dedup.set_index('sku')[['category_l1', 'category_l2']].to_dict('index')

    df['category_l1'] = df['sku'].astype(str).map(lambda x: sku_category.get(int(x), {}).get('category_l1', 'Unknown') if x.isdigit() else 'Unknown')
    df['category_l2'] = df['sku'].astype(str).map(lambda x: sku_category.get(int(x), {}).get('category_l2', 'Unknown') if x.isdigit() else 'Unknown')

    category_features = df.groupby(['category_l1', 'year_week']).agg({
        'quantity': 'sum',
        'unit_price': 'mean',
        'line_total': 'sum',
        'sku': 'nunique',
        'invoice_id': 'nunique',
        'data_completeness': 'first'
    }).reset_index()

    category_features.columns = ['category', 'year_week', 'weekly_quantity', 'avg_unit_price',
                                  'weekly_revenue', 'unique_skus', 'order_count', 'data_completeness']

    # 7. Customer Dimension
    print("📊 Creating customer dimension...")
    dim_customers = customer_cycles[['customer_id', 'customer_name', 'primary_region',
                                      'total_orders', 'total_units', 'total_revenue',
                                      'avg_order_value', 'avg_days_between_orders',
                                      'cycle_regularity', 'buyer_type', 'customer_segment',
                                      'first_order', 'last_order', 'active_weeks']].copy()

    # 8. Product Dimension with Price Stats
    print("📊 Creating product dimension...")
    product_stats = df.groupby('sku').agg({
        'description': 'first',
        'unit_price': ['mean', 'min', 'max', 'std'],
        'quantity': 'sum',
        'line_total': 'sum',
        'invoice_id': 'nunique',
        'year_week': 'nunique'
    }).reset_index()

    product_stats.columns = ['sku', 'name', 'avg_price', 'min_price', 'max_price',
                             'price_std', 'total_quantity', 'total_revenue',
                             'total_orders', 'active_weeks']

    # Merge with existing product info (convert both to string for merge)
    product_stats['sku'] = product_stats['sku'].astype(str)
    products_v1['sku'] = products_v1['sku'].astype(str)
    products_merged = product_stats.merge(
        products_v1[['sku', 'brand', 'manufacturer', 'category_path', 'fmcg',
                     'category_l1', 'category_l2', 'category_l3']],
        on='sku', how='left'
    )

    # Flag price volatility
    products_merged['price_volatility'] = (products_merged['price_std'] / products_merged['avg_price'] * 100).round(2)
    products_merged['price_volatility'] = products_merged['price_volatility'].fillna(0)

    # 9. Week Completeness
    print("📊 Creating week completeness table...")
    week_completeness = df.groupby('year_week').agg({
        'invoice_id': 'nunique',
        'quantity': 'sum',
        'line_total': 'sum',
        'sku': 'nunique',
        'customer_id': lambda x: x.dropna().nunique(),
        'region_name': 'nunique',
        'data_completeness': 'first',
        'unit_price': lambda x: (x > 0).sum()  # Count non-zero prices
    }).reset_index()

    week_completeness.columns = ['year_week', 'invoice_count', 'total_quantity',
                                  'total_revenue', 'unique_skus', 'unique_customers',
                                  'regions_active', 'data_completeness', 'prices_captured']

    week_completeness['price_coverage'] = (week_completeness['prices_captured'] /
                                            week_completeness['invoice_count'] * 100).round(1)

    # === SAVE OUTPUTS ===
    print(f"\n💾 Saving to {OUTPUT_DIR}...")

    df.to_csv(OUTPUT_DIR / 'v2_fact_lineitem.csv', index=False)
    print(f"   v2_fact_lineitem.csv: {len(df):,} rows")

    price_history.to_csv(OUTPUT_DIR / 'v2_price_history.csv', index=False)
    print(f"   v2_price_history.csv: {len(price_history):,} rows")

    customer_cycles.to_csv(OUTPUT_DIR / 'v2_customer_cycles.csv', index=False)
    print(f"   v2_customer_cycles.csv: {len(customer_cycles):,} rows")

    weekly_features.to_csv(OUTPUT_DIR / 'v2_features_weekly.csv', index=False)
    print(f"   v2_features_weekly.csv: {len(weekly_features):,} rows")

    sku_customer.to_csv(OUTPUT_DIR / 'v2_features_sku_customer.csv', index=False)
    print(f"   v2_features_sku_customer.csv: {len(sku_customer):,} rows")

    category_features.to_csv(OUTPUT_DIR / 'v2_features_category.csv', index=False)
    print(f"   v2_features_category.csv: {len(category_features):,} rows")

    dim_customers.to_csv(OUTPUT_DIR / 'v2_dim_customers.csv', index=False)
    print(f"   v2_dim_customers.csv: {len(dim_customers):,} rows")

    products_merged.to_csv(OUTPUT_DIR / 'v2_dim_products.csv', index=False)
    print(f"   v2_dim_products.csv: {len(products_merged):,} rows")

    week_completeness.to_csv(OUTPUT_DIR / 'v2_week_completeness.csv', index=False)
    print(f"   v2_week_completeness.csv: {len(week_completeness):,} rows")

    # === SUMMARY ===
    print(f"\n{'=' * 60}")
    print("V2 EXTRACTION SUMMARY")
    print(f"{'=' * 60}")
    print(f"Total transactions: {len(df):,}")
    print(f"Weeks covered: {df['year_week'].nunique()}")
    print(f"Unique SKUs: {df['sku'].nunique():,}")
    print(f"Unique customers: {len(dim_customers):,}")

    print(f"\n📊 PRICE COVERAGE:")
    prices_with_data = (df['unit_price'] > 0).sum()
    print(f"   Transactions with price: {prices_with_data:,} ({prices_with_data/len(df)*100:.1f}%)")
    print(f"   Price history records: {len(price_history):,}")

    print(f"\n👥 CUSTOMER SEGMENTS:")
    for segment, count in dim_customers['customer_segment'].value_counts().items():
        print(f"   {segment}: {count}")

    print(f"\n🔄 BUYER TYPES:")
    for btype, count in dim_customers['buyer_type'].value_counts().items():
        print(f"   {btype}: {count}")

    print(f"\n📈 BUYING CYCLE PATTERNS:")
    for cycle, count in dim_customers['cycle_regularity'].value_counts().items():
        print(f"   {cycle}: {count}")


if __name__ == '__main__':
    main()
