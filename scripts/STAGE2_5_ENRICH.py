#!/usr/bin/env python3
"""
STAGE 2.5: DATA ENRICHMENT
==========================
Enriches extracted data by:
1. Inferring missing prices from same-SKU transactions (ONLY when line_total is also 0)
2. Filling missing regions from filename patterns
3. Flagging data quality issues
4. Creating enrichment audit trail

Output:
- Enriched CSV files (sku0_fact_lineitem_enriched.csv, etc.)
- Enrichment report (stage2_5_enrichment.json)
- Data quality flags for transparency
"""

import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuration
BASE_PATH = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning")
FEATURES_SKU_PATH = BASE_PATH / "features_sku"
FEATURES_CAT_PATH = BASE_PATH / "features_category"
OUTPUT_PATH = BASE_PATH / "features_enriched"
OUTPUT_PATH.mkdir(exist_ok=True)

def load_data():
    """Load extracted data."""
    df = pd.read_csv(FEATURES_SKU_PATH / "sku0_fact_lineitem.csv")
    products = pd.read_csv(BASE_PATH / "features" / "dim_products.csv")
    products['sku_clean'] = products['sku'].str.replace('ACP-', '')
    return df, products

def enrich_prices(df, products):
    """
    Enrich missing prices using multiple strategies:
    1. Same SKU average price from other transactions
    2. Product master price as fallback

    IMPORTANT: Only modify rows where BOTH unit_price AND line_total are 0/missing.
    Preserve original line_total when it exists.
    """
    print("\n" + "=" * 60)
    print("PRICE ENRICHMENT")
    print("=" * 60)

    # Add enrichment tracking columns
    df['price_source'] = 'original'
    df['price_inferred'] = False
    df['original_unit_price'] = df['unit_price']
    df['original_line_total'] = df['line_total']

    # Identify TRULY missing revenue (both price and total are 0 or missing)
    truly_missing_mask = (df['unit_price'] == 0) & (df['line_total'] == 0)
    truly_missing_count = truly_missing_mask.sum()

    # Also track rows with price=0 but line_total>0 (they already have revenue)
    has_total_but_no_price = (df['unit_price'] == 0) & (df['line_total'] > 0)

    print(f"\nTotal zero-price line items: {(df['unit_price'] == 0).sum():,}")
    print(f"  - With line_total > 0 (already have revenue): {has_total_but_no_price.sum():,}")
    print(f"  - With line_total = 0 (truly missing): {truly_missing_count:,}")

    # Strategy 1: Infer from same-SKU transactions
    # Calculate average price per SKU from non-zero transactions
    # Use line_total / quantity to get effective price
    df['effective_price'] = np.where(
        df['quantity'] > 0,
        df['line_total'] / df['quantity'],
        df['unit_price']
    )

    sku_avg_price = df[df['effective_price'] > 0].groupby('sku')['effective_price'].agg(['mean', 'std', 'count'])
    sku_avg_price.columns = ['avg_price', 'price_std', 'price_samples']
    sku_avg_price = sku_avg_price.reset_index()

    # Merge average prices
    df = df.merge(sku_avg_price[['sku', 'avg_price', 'price_samples']], on='sku', how='left')

    # Apply inference ONLY where truly missing
    can_infer_mask = truly_missing_mask & df['avg_price'].notna()
    infer_count = can_infer_mask.sum()

    df.loc[can_infer_mask, 'unit_price'] = df.loc[can_infer_mask, 'avg_price']
    df.loc[can_infer_mask, 'line_total'] = df.loc[can_infer_mask, 'quantity'] * df.loc[can_infer_mask, 'unit_price']
    df.loc[can_infer_mask, 'price_source'] = 'inferred_from_sku_avg'
    df.loc[can_infer_mask, 'price_inferred'] = True

    print(f"\nInferred from SKU average: {infer_count:,}")

    # Strategy 2: Product master price for remaining truly missing
    master_prices = products[products['price'] > 0].set_index('sku_clean')['price'].to_dict()

    still_missing_mask = (df['unit_price'] == 0) & (df['line_total'] == 0)
    can_use_master = still_missing_mask & df['sku'].isin(master_prices.keys())
    master_count = can_use_master.sum()

    if master_count > 0:
        df.loc[can_use_master, 'unit_price'] = df.loc[can_use_master, 'sku'].map(master_prices)
        df.loc[can_use_master, 'line_total'] = df.loc[can_use_master, 'quantity'] * df.loc[can_use_master, 'unit_price']
        df.loc[can_use_master, 'price_source'] = 'inferred_from_master'
        df.loc[can_use_master, 'price_inferred'] = True
        print(f"Inferred from product master: {master_count:,}")

    # Mark the ones with price=0 but line_total>0 as "has_total_only"
    df.loc[has_total_but_no_price, 'price_source'] = 'has_total_no_unit_price'

    # Backfill unit_price from line_total for display
    backfill_mask = has_total_but_no_price & (df['quantity'] > 0)
    df.loc[backfill_mask, 'unit_price'] = df.loc[backfill_mask, 'line_total'] / df.loc[backfill_mask, 'quantity']
    df.loc[backfill_mask, 'price_inferred'] = True

    # Summary
    final_zero_revenue = ((df['unit_price'] == 0) & (df['line_total'] == 0)).sum()
    print(f"Remaining with zero revenue: {final_zero_revenue:,}")

    # Mark unfixable
    df.loc[(df['unit_price'] == 0) & (df['line_total'] == 0), 'price_source'] = 'missing_no_reference'

    # Clean up temp columns
    df = df.drop(columns=['avg_price', 'price_samples', 'effective_price'], errors='ignore')

    return df, {
        'original_zero_price': int((df['original_unit_price'] == 0).sum()),
        'truly_missing_revenue': int(truly_missing_count),
        'had_total_but_no_price': int(has_total_but_no_price.sum()),
        'inferred_from_sku': int(infer_count),
        'inferred_from_master': int(master_count),
        'remaining_zero_revenue': int(final_zero_revenue),
        'enrichment_rate': round((1 - final_zero_revenue/truly_missing_count) * 100, 2) if truly_missing_count > 0 else 100
    }

def enrich_regions(df):
    """Fix 'Unknown' regions where possible."""
    print("\n" + "=" * 60)
    print("REGION ENRICHMENT")
    print("=" * 60)

    df['region_source'] = 'original'
    df['original_region'] = df['region_name']

    unknown_mask = df['region_name'] == 'Unknown'
    unknown_count = unknown_mask.sum()
    print(f"\nUnknown region line items: {unknown_count:,}")

    df.loc[unknown_mask, 'region_source'] = 'unknown_unfixable'

    return df, {
        'original_unknown': int(unknown_count),
        'fixed': 0,
        'remaining_unknown': int(unknown_count)
    }

def enrich_customers(df):
    """Flag empty customer IDs."""
    print("\n" + "=" * 60)
    print("CUSTOMER ENRICHMENT")
    print("=" * 60)

    df['customer_source'] = 'original'

    empty_mask = (df['customer_id'] == '') | df['customer_id'].isna()
    empty_count = empty_mask.sum()
    print(f"\nEmpty customer IDs: {empty_count:,}")

    df.loc[empty_mask, 'customer_source'] = 'missing'

    return df, {
        'original_empty': int(empty_count),
        'remaining_empty': int(empty_count)
    }

def add_data_quality_flags(df):
    """Add comprehensive data quality flags."""
    print("\n" + "=" * 60)
    print("DATA QUALITY FLAGS")
    print("=" * 60)

    # Create quality score (0-100)
    df['dq_score'] = 100

    # Deduct for issues
    df.loc[df['price_inferred'] == True, 'dq_score'] -= 10  # Inferred price
    df.loc[(df['unit_price'] == 0) & (df['line_total'] == 0), 'dq_score'] -= 30  # Missing revenue
    df.loc[df['region_name'] == 'Unknown', 'dq_score'] -= 20  # Unknown region
    df.loc[(df['customer_id'] == '') | df['customer_id'].isna(), 'dq_score'] -= 10  # Missing customer

    df['dq_score'] = df['dq_score'].clip(lower=0)

    df['dq_tier'] = pd.cut(df['dq_score'],
                           bins=[-1, 50, 70, 90, 100],
                           labels=['poor', 'fair', 'good', 'excellent'])

    tier_counts = df['dq_tier'].value_counts()
    print("\nData Quality Distribution:")
    for tier in ['excellent', 'good', 'fair', 'poor']:
        if tier in tier_counts.index:
            count = tier_counts[tier]
            pct = count / len(df) * 100
            print(f"  {tier:10}: {count:>8,} ({pct:>5.1f}%)")

    return df

def regenerate_features(df, products):
    """Regenerate weekly features from enriched data."""
    print("\n" + "=" * 60)
    print("REGENERATING FEATURES")
    print("=" * 60)

    df['order_date'] = pd.to_datetime(df['order_date'])
    df['year_week'] = df['order_date'].dt.strftime('%Y-W%V')

    # SKU Weekly Features
    sku_weekly = df.groupby(['year_week', 'sku']).agg({
        'quantity': 'sum',
        'line_total': 'sum',
        'unit_price': 'mean',
        'invoice_id': 'nunique',
        'customer_id': 'nunique',
        'region_name': lambda x: x.mode()[0] if len(x) > 0 else 'Unknown',
        'price_inferred': 'mean',
        'dq_score': 'mean'
    }).reset_index()

    sku_weekly.columns = [
        'year_week', 'sku', 'weekly_quantity', 'weekly_revenue',
        'avg_price', 'transaction_count', 'unique_customers', 'primary_region',
        'pct_price_inferred', 'avg_dq_score'
    ]

    # Temporal features
    sku_weekly['week_of_year'] = sku_weekly['year_week'].str.extract(r'W(\d+)').astype(int)
    sku_weekly['month'] = ((sku_weekly['week_of_year'] - 1) // 4) + 1
    sku_weekly['month'] = sku_weekly['month'].clip(1, 12)
    sku_weekly['quarter'] = ((sku_weekly['month'] - 1) // 3) + 1

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
    products_lookup = products.set_index('sku_clean').to_dict('index')

    def get_attr(sku, attr, default='Unknown'):
        if str(sku) in products_lookup:
            val = products_lookup[str(sku)].get(attr, default)
            return val if pd.notna(val) else default
        return default

    sku_weekly['brand'] = sku_weekly['sku'].apply(lambda x: get_attr(x, 'brand'))
    sku_weekly['category'] = sku_weekly['sku'].apply(
        lambda x: get_attr(x, 'categories', '').split('/')[-1] if get_attr(x, 'categories') else 'Unknown'
    )
    sku_weekly['manufacturer'] = sku_weekly['sku'].apply(lambda x: get_attr(x, 'manufacturer'))

    print(f"Generated {len(sku_weekly):,} SKU-week records")

    # Category Weekly Features
    df['category'] = df['sku'].apply(
        lambda x: get_attr(x, 'categories', '').split('/')[-1] if get_attr(x, 'categories') else 'Unknown'
    )

    cat_weekly = df.groupby(['year_week', 'category']).agg({
        'quantity': 'sum',
        'line_total': 'sum',
        'sku': 'nunique',
        'invoice_id': 'nunique',
        'customer_id': 'nunique',
        'dq_score': 'mean'
    }).reset_index()

    cat_weekly.columns = [
        'year_week', 'category', 'weekly_quantity', 'weekly_revenue',
        'active_skus', 'transaction_count', 'unique_customers', 'avg_dq_score'
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

    print(f"Generated {len(cat_weekly):,} category-week records")

    return sku_weekly, cat_weekly

def main():
    print("=" * 70)
    print("STAGE 2.5: DATA ENRICHMENT")
    print("=" * 70)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Load data
    print("\nLoading data...")
    df, products = load_data()
    original_revenue = df['line_total'].sum()
    print(f"Loaded {len(df):,} line items")
    print(f"Original revenue: R{original_revenue:,.2f}")

    # Enrichment report
    report = {
        'stage': '2.5',
        'name': 'Data Enrichment',
        'generated_at': datetime.now().isoformat(),
        'before': {
            'line_items': len(df),
            'total_revenue': round(original_revenue, 2),
            'zero_price_count': int((df['unit_price'] == 0).sum()),
            'zero_revenue_count': int((df['line_total'] == 0).sum()),
            'unknown_region_count': int((df['region_name'] == 'Unknown').sum()),
        },
        'enrichment': {}
    }

    # Run enrichment
    df, price_stats = enrich_prices(df, products)
    report['enrichment']['prices'] = price_stats

    df, region_stats = enrich_regions(df)
    report['enrichment']['regions'] = region_stats

    df, customer_stats = enrich_customers(df)
    report['enrichment']['customers'] = customer_stats

    # Add quality flags
    df = add_data_quality_flags(df)

    # Calculate after stats
    enriched_revenue = df['line_total'].sum()
    report['after'] = {
        'line_items': len(df),
        'total_revenue': round(enriched_revenue, 2),
        'revenue_increase': round(enriched_revenue - original_revenue, 2),
        'revenue_increase_pct': round((enriched_revenue - original_revenue) / original_revenue * 100, 2) if original_revenue > 0 else 0,
        'zero_revenue_count': int((df['line_total'] == 0).sum()),
        'dq_score_avg': round(df['dq_score'].mean(), 2),
    }

    # Regenerate features
    sku_weekly, cat_weekly = regenerate_features(df, products)

    # Save enriched files
    print("\n" + "=" * 60)
    print("SAVING ENRICHED FILES")
    print("=" * 60)

    df.to_csv(OUTPUT_PATH / "sku0_fact_lineitem_enriched.csv", index=False)
    print(f"✓ sku0_fact_lineitem_enriched.csv ({len(df):,} rows)")

    sku_weekly.to_csv(OUTPUT_PATH / "sku0_features_weekly_enriched.csv", index=False)
    print(f"✓ sku0_features_weekly_enriched.csv ({len(sku_weekly):,} rows)")

    cat_weekly.to_csv(OUTPUT_PATH / "cat0_features_weekly_enriched.csv", index=False)
    print(f"✓ cat0_features_weekly_enriched.csv ({len(cat_weekly):,} rows)")

    report_path = BASE_PATH / "stage2_5_enrichment.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"✓ Report saved to {report_path}")

    # Final summary
    print("\n" + "=" * 70)
    print("ENRICHMENT SUMMARY")
    print("=" * 70)

    revenue_change = enriched_revenue - original_revenue
    change_sign = "+" if revenue_change >= 0 else ""

    print(f"""
┌─────────────────────────────────────────────────────────────────┐
│ BEFORE ENRICHMENT                                               │
├─────────────────────────────────────────────────────────────────┤
│ Total Revenue:      R{original_revenue:>14,.2f}                 │
│ Zero-price items:   {report['before']['zero_price_count']:>10,}                               │
│ Zero-revenue items: {report['before']['zero_revenue_count']:>10,}                               │
│ Unknown regions:    {report['before']['unknown_region_count']:>10,}                               │
├─────────────────────────────────────────────────────────────────┤
│ AFTER ENRICHMENT                                                │
├─────────────────────────────────────────────────────────────────┤
│ Total Revenue:      R{enriched_revenue:>14,.2f}                 │
│ Revenue Change:     R{change_sign}{revenue_change:>13,.2f} ({change_sign}{report['after']['revenue_increase_pct']:.1f}%)    │
│ Zero-revenue items: {report['after']['zero_revenue_count']:>10,}                               │
│ Avg DQ Score:       {report['after']['dq_score_avg']:>10.1f}                               │
├─────────────────────────────────────────────────────────────────┤
│ PRICE ENRICHMENT BREAKDOWN                                      │
├─────────────────────────────────────────────────────────────────┤
│ Had line_total but no unit_price: {price_stats['had_total_but_no_price']:>7,} (preserved)    │
│ Truly missing (both zero):        {price_stats['truly_missing_revenue']:>7,}                 │
│ Inferred from SKU avg:            {price_stats['inferred_from_sku']:>7,}                 │
│ Inferred from master:             {price_stats['inferred_from_master']:>7,}                 │
│ Could not infer:                  {price_stats['remaining_zero_revenue']:>7,}                 │
│ Enrichment rate:                  {price_stats['enrichment_rate']:>7.1f}%                │
└─────────────────────────────────────────────────────────────────┘
""")

    print("\nNext steps:")
    print("  1. Review enriched data in features_enriched/")
    print("  2. Upload ALL stages to BQ: ./scripts/ALL_STAGES_BIGQUERY.sh")
    print()

if __name__ == "__main__":
    main()
