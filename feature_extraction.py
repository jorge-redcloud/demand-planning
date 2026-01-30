"""
RedAI Demand Forecasting - Feature Extraction Pipeline
======================================================
Extracts and engineers features from raw distributor data.
Outputs are BigQuery-ready CSV/Parquet files.
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from pathlib import Path
import json
import warnings
warnings.filterwarnings('ignore')

# Configuration
BASE_PATH = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning/2025")
OUTPUT_PATH = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning/features")
OUTPUT_PATH.mkdir(exist_ok=True)

REGIONS = {
    'ACWCP': 'Cape Town',
    'ACWGT': 'Gauteng',
    'ACWGE': 'George',
    'ACWPK': 'Polokwane',
    'ACWHW': 'Hardware'
}

MONTHS = ['January', 'February', 'March', 'April', 'May', 'June',
          'July', 'August', 'September', 'October', 'November', 'December']


# =============================================================================
# 1. DATA EXTRACTION
# =============================================================================

def extract_transactions_from_file(filepath, region_hint):
    """Extract transactions from Summary sheet of regional file."""
    try:
        xl = pd.ExcelFile(filepath, engine='openpyxl')
        if 'Summary' not in xl.sheet_names:
            return pd.DataFrame()

        df = pd.read_excel(filepath, sheet_name='Summary', engine='openpyxl')

        # Standardize columns
        col_map = {
            'Txan.Type': 'transaction_type',
            'Document No.': 'document_no',
            'Date': 'invoice_date',
            'Account': 'account_no',
            'Debtors Name': 'customer_name',
            'Doc.Total (Incl)': 'total_amount_incl',
            'S/Brch': 'branch_code'
        }
        df = df.rename(columns=col_map)

        # Clean data
        df['invoice_date'] = pd.to_datetime(df['invoice_date'], errors='coerce')
        df['total_amount_incl'] = pd.to_numeric(df['total_amount_incl'], errors='coerce')
        df['document_no'] = pd.to_numeric(df['document_no'], errors='coerce').astype('Int64')

        # Derive region from branch code or filename
        if 'branch_code' in df.columns:
            df['region_name'] = df['branch_code'].map(REGIONS)
        if df['region_name'].isna().all():
            df['region_name'] = region_hint

        # Filter valid transactions
        df = df[df['transaction_type'] == 'Account Sales']
        df = df.dropna(subset=['invoice_date', 'total_amount_incl'])

        return df[['document_no', 'invoice_date', 'account_no', 'customer_name',
                   'total_amount_incl', 'branch_code', 'region_name']]
    except Exception as e:
        print(f"  Error: {e}")
        return pd.DataFrame()


def extract_line_items_from_file(filepath):
    """Extract line items from individual account sheets."""
    try:
        xl = pd.ExcelFile(filepath, engine='openpyxl')
        all_items = []

        # Get customer sheet names (numeric account numbers)
        customer_sheets = [s for s in xl.sheet_names
                         if s not in ['Debtors Masterfile', 'Summary', 'Sheet1', 'Sheet2']
                         and s.replace('.', '').replace('-', '').isdigit()]

        for sheet in customer_sheets[:100]:  # Sample first 100
            try:
                df = pd.read_excel(filepath, sheet_name=sheet, engine='openpyxl')
                if len(df.columns) >= 6:
                    # Standardize column names
                    df.columns = ['stock_code', 'description', 'quantity', 'unit_price', 'discount', 'line_total'][:len(df.columns)]
                    df['account_no'] = sheet
                    df['stock_code'] = df['stock_code'].astype(str).str.strip()
                    all_items.append(df)
            except:
                continue

        if all_items:
            return pd.concat(all_items, ignore_index=True)
        return pd.DataFrame()
    except:
        return pd.DataFrame()


def load_all_transactions():
    """Load transactions from all months and regions."""
    print("=" * 70)
    print("EXTRACTING TRANSACTION DATA")
    print("=" * 70)

    all_transactions = []

    for month in MONTHS:
        month_path = BASE_PATH / f"{month} 2025"
        if not month_path.exists():
            continue

        print(f"\n{month} 2025:")
        files = list(month_path.glob("*.xlsx")) + list(month_path.glob("*.xlsm"))

        # Group by region, prefer corrected versions
        region_files = {}
        for fp in files:
            fname = fp.name.lower()
            if 'dub_' in fname or '_dt_' in fname:
                continue  # Skip product/customer/verification files

            # Determine region
            region = None
            if 'cape' in fname or 'capetown' in fname:
                region = 'Cape Town'
            elif 'gauteng' in fname:
                region = 'Gauteng'
            elif 'george' in fname:
                region = 'George'
            elif 'polokwane' in fname:
                region = 'Polokwane'
            elif 'hardware' in fname:
                region = 'Hardware'

            if region:
                # Prefer corrected or V2 versions
                if region not in region_files:
                    region_files[region] = fp
                elif 'corrected' in fname:
                    region_files[region] = fp
                elif 'v2' in fname and 'corrected' not in region_files[region].name.lower():
                    region_files[region] = fp

        for region, filepath in region_files.items():
            df = extract_transactions_from_file(filepath, region)
            if not df.empty:
                df['source_month'] = month
                all_transactions.append(df)
                print(f"  {region}: {len(df)} transactions")

    if all_transactions:
        combined = pd.concat(all_transactions, ignore_index=True)
        print(f"\n{'=' * 70}")
        print(f"Total transactions: {len(combined):,}")
        return combined
    return pd.DataFrame()


def load_product_catalog():
    """Load product master data."""
    product_file = BASE_PATH / "January 2025" / "DUB_PROD_Products-2025-02-03-0117.xlsx"
    if product_file.exists():
        df = pd.read_excel(product_file, engine='openpyxl')
        print(f"Loaded {len(df)} products with {len(df.columns)} columns")

        # Select key columns
        key_cols = [
            'sku', 'name', 'product_type', 'categories', 'category_ids',
            'brand', 'manufacturer', 'price', 'weight', 'color',
            'qty', 'is_in_stock', 'tax_class_name', 'fmcg',
            'created_at', 'updated_at', 'visibility', 'seller_id'
        ]
        available_cols = [c for c in key_cols if c in df.columns]
        return df[available_cols]
    return pd.DataFrame()


def load_customer_data():
    """Load customer master data."""
    customer_file = BASE_PATH / "January 2025" / "DUB_Customers-2025-02-03-0113.xlsx"
    if customer_file.exists():
        df = pd.read_excel(customer_file, engine='openpyxl')
        print(f"Loaded {len(df)} customers with {len(df.columns)} columns")

        # Select key columns
        key_cols = [
            'email', 'firstname', 'lastname', 'phone_number',
            'group_id', '_customer_group_code', '_tax_class_name',
            'taxvat', 'kyc_verified', 'category_commission',
            'website_id', 'store_id', 'created_at', 'updated_at'
        ]
        available_cols = [c for c in key_cols if c in df.columns]
        return df[available_cols]
    return pd.DataFrame()


# =============================================================================
# 2. FEATURE ENGINEERING
# =============================================================================

def create_temporal_features(df):
    """Create time-based features from transactions."""
    df = df.copy()
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])

    # Date parts
    df['year'] = df['invoice_date'].dt.year
    df['month'] = df['invoice_date'].dt.month
    df['week_of_year'] = df['invoice_date'].dt.isocalendar().week
    df['day_of_week'] = df['invoice_date'].dt.dayofweek
    df['day_of_month'] = df['invoice_date'].dt.day
    df['quarter'] = df['invoice_date'].dt.quarter

    # Binary flags
    df['is_weekend'] = df['day_of_week'].isin([5, 6]).astype(int)
    df['is_month_start'] = (df['day_of_month'] <= 7).astype(int)
    df['is_month_end'] = (df['day_of_month'] >= 25).astype(int)

    # Week start (for aggregation)
    df['week_start'] = df['invoice_date'] - pd.to_timedelta(df['day_of_week'], unit='D')

    return df


def aggregate_daily(df):
    """Aggregate to daily level by region."""
    daily = df.groupby(['invoice_date', 'region_name']).agg({
        'document_no': 'nunique',
        'total_amount_incl': ['sum', 'mean', 'std'],
        'account_no': 'nunique'
    }).reset_index()

    daily.columns = [
        'date', 'region_name',
        'transaction_count', 'daily_revenue', 'avg_transaction', 'transaction_std',
        'unique_customers'
    ]

    # Add date features
    daily['day_of_week'] = daily['date'].dt.dayofweek
    daily['day_of_month'] = daily['date'].dt.day
    daily['month'] = daily['date'].dt.month
    daily['is_weekend'] = daily['day_of_week'].isin([5, 6]).astype(int)

    return daily


def aggregate_weekly(df):
    """Aggregate to weekly level by region."""
    weekly = df.groupby(['week_start', 'region_name']).agg({
        'document_no': 'nunique',
        'total_amount_incl': ['sum', 'mean', 'std', 'min', 'max'],
        'account_no': 'nunique',
        'invoice_date': ['min', 'max']
    }).reset_index()

    weekly.columns = [
        'week_start', 'region_name',
        'transaction_count',
        'weekly_revenue', 'avg_transaction', 'transaction_std', 'min_transaction', 'max_transaction',
        'unique_customers',
        'first_day', 'last_day'
    ]

    # Add week features
    weekly['week_of_year'] = weekly['week_start'].dt.isocalendar().week
    weekly['month'] = weekly['week_start'].dt.month
    weekly['quarter'] = weekly['week_start'].dt.quarter
    weekly['trading_days'] = (weekly['last_day'] - weekly['first_day']).dt.days + 1

    return weekly


def create_lag_features(df, group_col='region_name', value_col='weekly_revenue', lags=[1, 2, 3, 4, 8]):
    """Create lag features for time series."""
    df = df.sort_values(['region_name', 'week_start']).copy()

    for lag in lags:
        df[f'{value_col}_lag_{lag}w'] = df.groupby(group_col)[value_col].shift(lag)

    # Rolling statistics
    df[f'{value_col}_ma_4w'] = df.groupby(group_col)[value_col].transform(
        lambda x: x.rolling(4, min_periods=1).mean().shift(1)
    )
    df[f'{value_col}_ma_8w'] = df.groupby(group_col)[value_col].transform(
        lambda x: x.rolling(8, min_periods=1).mean().shift(1)
    )
    df[f'{value_col}_std_4w'] = df.groupby(group_col)[value_col].transform(
        lambda x: x.rolling(4, min_periods=1).std().shift(1)
    )

    # Trend features
    df[f'{value_col}_diff_1w'] = df.groupby(group_col)[value_col].diff(1)
    df[f'{value_col}_pct_change_4w'] = df.groupby(group_col)[value_col].pct_change(4)

    return df


def create_customer_features(df):
    """Create customer-level features (RFM-style)."""
    max_date = df['invoice_date'].max()

    customer_features = df.groupby(['account_no', 'region_name']).agg({
        # Recency
        'invoice_date': ['max', 'min', 'count'],
        # Monetary
        'total_amount_incl': ['sum', 'mean', 'std', 'min', 'max'],
        # Frequency
        'document_no': 'nunique'
    }).reset_index()

    customer_features.columns = [
        'account_no', 'region_name',
        'last_purchase_date', 'first_purchase_date', 'purchase_count',
        'lifetime_value', 'avg_order_value', 'order_value_std', 'min_order', 'max_order',
        'total_transactions'
    ]

    # Derived features
    customer_features['days_since_last_purchase'] = (max_date - customer_features['last_purchase_date']).dt.days
    customer_features['customer_tenure_days'] = (max_date - customer_features['first_purchase_date']).dt.days
    customer_features['purchase_frequency'] = customer_features['total_transactions'] / (customer_features['customer_tenure_days'] / 30 + 1)

    return customer_features


def create_regional_features(weekly_df):
    """Create cross-regional comparison features."""
    # Total weekly revenue across all regions
    total_weekly = weekly_df.groupby('week_start')['weekly_revenue'].transform('sum')
    weekly_df['revenue_share'] = weekly_df['weekly_revenue'] / total_weekly
    weekly_df['transaction_share'] = weekly_df['transaction_count'] / weekly_df.groupby('week_start')['transaction_count'].transform('sum')

    # Regional rank
    weekly_df['revenue_rank'] = weekly_df.groupby('week_start')['weekly_revenue'].rank(ascending=False)

    # WoW growth
    weekly_df['wow_growth'] = weekly_df.groupby('region_name')['weekly_revenue'].pct_change(1)

    return weekly_df


# =============================================================================
# 3. MAIN EXECUTION
# =============================================================================

if __name__ == "__main__":
    print("\n" + "=" * 70)
    print("REDAI DEMAND FORECASTING - FEATURE EXTRACTION")
    print("=" * 70)

    # Step 1: Load raw data
    transactions = load_all_transactions()
    products = load_product_catalog()
    customers = load_customer_data()

    if transactions.empty:
        print("ERROR: No transaction data loaded!")
        exit(1)

    # Step 2: Create temporal features
    print("\n" + "=" * 70)
    print("CREATING FEATURES")
    print("=" * 70)

    print("\n[1] Adding temporal features...")
    transactions = create_temporal_features(transactions)

    # Step 3: Aggregate data
    print("[2] Creating daily aggregates...")
    daily_data = aggregate_daily(transactions)
    print(f"    {len(daily_data)} daily records")

    print("[3] Creating weekly aggregates...")
    weekly_data = aggregate_weekly(transactions)
    print(f"    {len(weekly_data)} weekly records")

    # Step 4: Create lag features
    print("[4] Creating lag features...")
    weekly_data = create_lag_features(weekly_data)

    # Step 5: Create regional features
    print("[5] Creating regional features...")
    weekly_data = create_regional_features(weekly_data)

    # Step 6: Create customer features
    print("[6] Creating customer features...")
    customer_features = create_customer_features(transactions)
    print(f"    {len(customer_features)} customer records")

    # Step 7: Create total (all regions) aggregates
    print("[7] Creating total aggregates...")
    weekly_total = transactions.groupby('week_start').agg({
        'document_no': 'nunique',
        'total_amount_incl': ['sum', 'mean'],
        'account_no': 'nunique',
        'region_name': 'nunique'
    }).reset_index()
    weekly_total.columns = ['week_start', 'transaction_count', 'weekly_revenue', 'avg_transaction', 'unique_customers', 'active_regions']
    weekly_total = weekly_total.sort_values('week_start')

    # Add lag features to total
    for lag in [1, 2, 3, 4]:
        weekly_total[f'revenue_lag_{lag}w'] = weekly_total['weekly_revenue'].shift(lag)
    weekly_total['revenue_ma_4w'] = weekly_total['weekly_revenue'].rolling(4, min_periods=1).mean().shift(1)
    weekly_total['revenue_diff_1w'] = weekly_total['weekly_revenue'].diff(1)

    print(f"    {len(weekly_total)} total weekly records")

    # =============================================================================
    # SAVE OUTPUTS
    # =============================================================================
    print("\n" + "=" * 70)
    print("SAVING FEATURE TABLES")
    print("=" * 70)

    # Save as CSV (BigQuery compatible)
    transactions[['document_no', 'invoice_date', 'account_no', 'customer_name',
                  'total_amount_incl', 'region_name', 'year', 'month', 'week_of_year',
                  'day_of_week', 'day_of_month', 'is_weekend', 'is_month_start', 'is_month_end', 'week_start']
                ].to_csv(OUTPUT_PATH / 'fact_transactions.csv', index=False)
    print(f"  fact_transactions.csv: {len(transactions):,} rows")

    daily_data.to_csv(OUTPUT_PATH / 'features_daily.csv', index=False)
    print(f"  features_daily.csv: {len(daily_data):,} rows")

    weekly_data.to_csv(OUTPUT_PATH / 'features_weekly_regional.csv', index=False)
    print(f"  features_weekly_regional.csv: {len(weekly_data):,} rows")

    weekly_total.to_csv(OUTPUT_PATH / 'features_weekly_total.csv', index=False)
    print(f"  features_weekly_total.csv: {len(weekly_total):,} rows")

    customer_features.to_csv(OUTPUT_PATH / 'features_customers.csv', index=False)
    print(f"  features_customers.csv: {len(customer_features):,} rows")

    if not products.empty:
        products.to_csv(OUTPUT_PATH / 'dim_products.csv', index=False)
        print(f"  dim_products.csv: {len(products):,} rows")

    if not customers.empty:
        customers.to_csv(OUTPUT_PATH / 'dim_customers.csv', index=False)
        print(f"  dim_customers.csv: {len(customers):,} rows")

    # =============================================================================
    # DATA SUMMARY
    # =============================================================================
    print("\n" + "=" * 70)
    print("DATA SUMMARY")
    print("=" * 70)

    print(f"""
    FACT TABLE
    ----------
    Total transactions: {len(transactions):,}
    Date range: {transactions['invoice_date'].min().date()} to {transactions['invoice_date'].max().date()}
    Unique customers: {transactions['account_no'].nunique():,}
    Unique invoices: {transactions['document_no'].nunique():,}
    Total revenue: R{transactions['total_amount_incl'].sum():,.0f}

    DIMENSIONS
    ----------
    Regions: {transactions['region_name'].unique().tolist()}
    Products in catalog: {len(products):,}
    Customers in master: {len(customers):,}

    FEATURE TABLES
    --------------
    Daily features: {len(daily_data):,} rows x {len(daily_data.columns)} columns
    Weekly regional: {len(weekly_data):,} rows x {len(weekly_data.columns)} columns
    Weekly total: {len(weekly_total):,} rows x {len(weekly_total.columns)} columns
    Customer features: {len(customer_features):,} rows x {len(customer_features.columns)} columns

    COLUMNS IN WEEKLY REGIONAL:
    {list(weekly_data.columns)}

    COLUMNS IN CUSTOMER FEATURES:
    {list(customer_features.columns)}
    """)

    # Save schema documentation
    schema = {
        'fact_transactions': {
            'description': 'Individual sales transactions',
            'columns': list(transactions.columns),
            'row_count': len(transactions),
            'primary_key': 'document_no'
        },
        'features_daily': {
            'description': 'Daily aggregated features by region',
            'columns': list(daily_data.columns),
            'row_count': len(daily_data),
            'primary_key': ['date', 'region_name']
        },
        'features_weekly_regional': {
            'description': 'Weekly aggregated features by region with lags',
            'columns': list(weekly_data.columns),
            'row_count': len(weekly_data),
            'primary_key': ['week_start', 'region_name']
        },
        'features_weekly_total': {
            'description': 'Weekly aggregated features for all regions combined',
            'columns': list(weekly_total.columns),
            'row_count': len(weekly_total),
            'primary_key': 'week_start'
        },
        'features_customers': {
            'description': 'Customer-level RFM features',
            'columns': list(customer_features.columns),
            'row_count': len(customer_features),
            'primary_key': ['account_no', 'region_name']
        },
        'dim_products': {
            'description': 'Product master data',
            'columns': list(products.columns) if not products.empty else [],
            'row_count': len(products)
        },
        'dim_customers': {
            'description': 'Customer master data',
            'columns': list(customers.columns) if not customers.empty else [],
            'row_count': len(customers)
        }
    }

    with open(OUTPUT_PATH / 'schema.json', 'w') as f:
        json.dump(schema, f, indent=2, default=str)
    print(f"\n  schema.json: Data dictionary saved")

    print("\n" + "=" * 70)
    print("FEATURE EXTRACTION COMPLETE")
    print(f"Output location: {OUTPUT_PATH}")
    print("=" * 70)
