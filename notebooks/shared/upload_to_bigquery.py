"""
Upload clean transactions to BigQuery

Run this script after data prep to upload the clean transactions table.

Usage:
    python upload_to_bigquery.py

Or import and use:
    from upload_to_bigquery import upload_transactions
    upload_transactions()
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import CONFIG, STORAGE_CLIENT, BQ_CLIENT, upload_to_bigquery, upload_df_to_gcs
import pandas as pd


def upload_transactions(local_path: str = None):
    """
    Upload clean transactions table to BigQuery.

    Args:
        local_path: Path to transactions_clean.csv (defaults to features_v2/)
    """
    if local_path is None:
        local_path = os.path.join(CONFIG.LOCAL_DATA_DIR, 'features_v2', 'transactions_clean.csv')

    print("=" * 60)
    print("UPLOADING CLEAN TRANSACTIONS TO BIGQUERY")
    print("=" * 60)

    # Load data
    print(f"\nLoading: {local_path}")
    df = pd.read_csv(local_path)
    print(f"Rows: {len(df):,}")
    print(f"Columns: {list(df.columns)}")

    # Verify no missing values
    missing = df.isnull().sum().sum()
    if missing > 0:
        print(f"\n⚠ WARNING: {missing} missing values found!")
    else:
        print("\n✓ No missing values")

    # Upload to GCS
    if STORAGE_CLIENT:
        print("\n1. Uploading to GCS...")
        gcs_path = f"{CONFIG.GCS_PROCESSED}transactions_clean.csv"
        upload_df_to_gcs(df, gcs_path)
    else:
        print("\n1. GCS not available, skipping GCS upload")

    # Upload to BigQuery
    if BQ_CLIENT:
        print("\n2. Uploading to BigQuery...")
        upload_to_bigquery(df, 'transactions_clean', if_exists='replace')

        print("\n" + "=" * 60)
        print("SUCCESS!")
        print("=" * 60)
        print(f"\nTable: {CONFIG.bq_table('transactions_clean')}")
        print(f"Rows:  {len(df):,}")

        print("\nQuery example:")
        print(f"""
SELECT
    year_week,
    COUNT(DISTINCT invoice_id) as orders,
    COUNT(DISTINCT customer_id) as customers,
    SUM(quantity) as total_qty,
    SUM(line_total) as revenue
FROM `{CONFIG.bq_table('transactions_clean')}`
GROUP BY year_week
ORDER BY year_week
LIMIT 10
""")
    else:
        print("\n2. BigQuery not available, skipping upload")
        print("\nTo enable BigQuery, run:")
        print("  gcloud auth application-default login")

    return df


def upload_all_tables():
    """Upload all main tables to BigQuery."""
    tables = [
        ('transactions_clean.csv', 'transactions_clean'),
        ('v2_fact_lineitem.csv', 'fact_lineitem'),
        ('v2_dim_products.csv', 'dim_products'),
        ('v2_dim_customers.csv', 'dim_customers'),
    ]

    base_path = os.path.join(CONFIG.LOCAL_DATA_DIR, 'features_v2')

    for filename, table_name in tables:
        filepath = os.path.join(base_path, filename)
        if os.path.exists(filepath):
            print(f"\n{'='*60}")
            print(f"Uploading: {filename} → {table_name}")
            print('='*60)

            df = pd.read_csv(filepath, low_memory=False)
            print(f"Rows: {len(df):,}")

            if BQ_CLIENT:
                upload_to_bigquery(df, table_name, if_exists='replace')
            else:
                print("BigQuery not available")
        else:
            print(f"File not found: {filepath}")


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--all', action='store_true', help='Upload all tables')
    args = parser.parse_args()

    if args.all:
        upload_all_tables()
    else:
        upload_transactions()
