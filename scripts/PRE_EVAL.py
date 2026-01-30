#!/usr/bin/env python3
"""
PRE-EVALUATION SCRIPT
=====================
Run this BEFORE deploying to BigQuery to validate all data files.
Produces checksums and counts that can be compared post-upload.

Usage:
    python3 scripts/PRE_EVAL.py

Output:
    - Console summary
    - pre_eval_report.json (machine-readable for comparison)
"""

import pandas as pd
import json
import hashlib
from pathlib import Path
from datetime import datetime

# Configuration
BASE_PATH = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning")
FEATURES_REV_PATH = BASE_PATH / "features"
FEATURES_SKU_PATH = BASE_PATH / "features_sku"
FEATURES_CAT_PATH = BASE_PATH / "features_category"

def get_file_hash(filepath):
    """Get MD5 hash of file for integrity check."""
    hash_md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()[:12]  # Short hash

def analyze_csv(filepath, name):
    """Analyze a CSV file and return stats."""
    if not filepath.exists():
        return {"exists": False, "name": name}

    df = pd.read_csv(filepath)

    stats = {
        "name": name,
        "exists": True,
        "filepath": str(filepath),
        "rows": len(df),
        "columns": len(df.columns),
        "column_names": list(df.columns),
        "file_hash": get_file_hash(filepath),
        "file_size_kb": round(filepath.stat().st_size / 1024, 1)
    }

    # Add specific stats based on file type
    if 'sku' in df.columns:
        stats["unique_skus"] = df['sku'].nunique()
    if 'invoice_id' in df.columns:
        stats["unique_invoices"] = df['invoice_id'].nunique()
    if 'customer_id' in df.columns:
        stats["unique_customers"] = df['customer_id'].nunique()
    if 'category' in df.columns:
        stats["unique_categories"] = df['category'].nunique()
    if 'region_name' in df.columns:
        stats["unique_regions"] = df['region_name'].nunique()
        stats["regions"] = df['region_name'].unique().tolist()
    if 'year_week' in df.columns:
        stats["unique_weeks"] = df['year_week'].nunique()
        stats["week_range"] = [df['year_week'].min(), df['year_week'].max()]
    if 'quantity' in df.columns:
        stats["total_quantity"] = int(df['quantity'].sum())
    if 'weekly_quantity' in df.columns:
        stats["total_weekly_quantity"] = int(df['weekly_quantity'].sum())
    if 'line_total' in df.columns:
        stats["total_revenue"] = round(df['line_total'].sum(), 2)
    if 'weekly_revenue' in df.columns:
        stats["total_weekly_revenue"] = round(df['weekly_revenue'].sum(), 2)
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'])
        stats["date_range"] = [str(df['order_date'].min().date()), str(df['order_date'].max().date())]

    return stats

def print_file_stats(stats, indent=2):
    """Pretty print file statistics."""
    prefix = " " * indent

    if not stats.get("exists"):
        print(f"{prefix}❌ FILE NOT FOUND: {stats['name']}")
        return

    print(f"{prefix}📄 {stats['name']}")
    print(f"{prefix}   Rows: {stats['rows']:,}")
    print(f"{prefix}   Columns: {stats['columns']}")
    print(f"{prefix}   Size: {stats['file_size_kb']} KB")
    print(f"{prefix}   Hash: {stats['file_hash']}")

    # Print specific stats
    if 'unique_skus' in stats:
        print(f"{prefix}   Unique SKUs: {stats['unique_skus']:,}")
    if 'unique_invoices' in stats:
        print(f"{prefix}   Unique Invoices: {stats['unique_invoices']:,}")
    if 'unique_customers' in stats:
        print(f"{prefix}   Unique Customers: {stats['unique_customers']:,}")
    if 'unique_categories' in stats:
        print(f"{prefix}   Unique Categories: {stats['unique_categories']}")
    if 'unique_regions' in stats:
        print(f"{prefix}   Unique Regions: {stats['unique_regions']} {stats.get('regions', [])}")
    if 'unique_weeks' in stats:
        print(f"{prefix}   Unique Weeks: {stats['unique_weeks']} ({stats['week_range'][0]} to {stats['week_range'][1]})")
    if 'total_quantity' in stats:
        print(f"{prefix}   Total Quantity: {stats['total_quantity']:,} units")
    if 'total_weekly_quantity' in stats:
        print(f"{prefix}   Total Weekly Quantity: {stats['total_weekly_quantity']:,} units")
    if 'total_revenue' in stats:
        print(f"{prefix}   Total Revenue: R{stats['total_revenue']:,.2f}")
    if 'total_weekly_revenue' in stats:
        print(f"{prefix}   Total Weekly Revenue: R{stats['total_weekly_revenue']:,.2f}")
    if 'date_range' in stats:
        print(f"{prefix}   Date Range: {stats['date_range'][0]} to {stats['date_range'][1]}")

def main():
    print("=" * 70)
    print("PRE-EVALUATION REPORT")
    print("=" * 70)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Base Path: {BASE_PATH}")
    print()

    report = {
        "generated_at": datetime.now().isoformat(),
        "models": {}
    }

    # =========================================================================
    # REV0 - Revenue Model
    # =========================================================================
    print("=" * 70)
    print("MODEL: rev0 (Revenue Forecast)")
    print("=" * 70)

    rev0_files = {
        "fact_transactions": FEATURES_REV_PATH / "fact_transactions.csv",
        "features_weekly_regional": FEATURES_REV_PATH / "features_weekly_regional.csv",
        "features_weekly_total": FEATURES_REV_PATH / "features_weekly_total.csv",
        "features_customers": FEATURES_REV_PATH / "features_customers.csv",
        "dim_products": FEATURES_REV_PATH / "dim_products.csv",
        "dim_customers": FEATURES_REV_PATH / "dim_customers.csv",
    }

    rev0_stats = {}
    for name, path in rev0_files.items():
        stats = analyze_csv(path, name)
        rev0_stats[name] = stats
        print_file_stats(stats)
        print()

    report["models"]["rev0"] = rev0_stats

    # =========================================================================
    # SKU_DEMAND_0 - SKU Demand Model
    # =========================================================================
    print("=" * 70)
    print("MODEL: sku_demand_0 (SKU Demand Forecast)")
    print("=" * 70)

    sku0_files = {
        "sku0_fact_lineitem": FEATURES_SKU_PATH / "sku0_fact_lineitem.csv",
        "sku0_features_weekly": FEATURES_SKU_PATH / "sku0_features_weekly.csv",
        "sku0_dim_products": FEATURES_SKU_PATH / "sku0_dim_products.csv",
    }

    sku0_stats = {}
    for name, path in sku0_files.items():
        stats = analyze_csv(path, name)
        sku0_stats[name] = stats
        print_file_stats(stats)
        print()

    report["models"]["sku_demand_0"] = sku0_stats

    # =========================================================================
    # CAT_DEMAND_0 - Category Demand Model
    # =========================================================================
    print("=" * 70)
    print("MODEL: cat_demand_0 (Category Demand Forecast)")
    print("=" * 70)

    cat0_files = {
        "cat0_features_weekly": FEATURES_CAT_PATH / "cat0_features_weekly.csv",
    }

    cat0_stats = {}
    for name, path in cat0_files.items():
        stats = analyze_csv(path, name)
        cat0_stats[name] = stats
        print_file_stats(stats)
        print()

    report["models"]["cat_demand_0"] = cat0_stats

    # =========================================================================
    # TOTALS SUMMARY
    # =========================================================================
    print("=" * 70)
    print("TOTALS SUMMARY (Use these to verify BigQuery upload)")
    print("=" * 70)

    # Calculate totals
    totals = {
        "rev0": {
            "tables": len([s for s in rev0_stats.values() if s.get("exists")]),
            "total_rows": sum(s.get("rows", 0) for s in rev0_stats.values() if s.get("exists")),
        },
        "sku_demand_0": {
            "tables": len([s for s in sku0_stats.values() if s.get("exists")]),
            "total_rows": sum(s.get("rows", 0) for s in sku0_stats.values() if s.get("exists")),
            "total_line_items": sku0_stats.get("sku0_fact_lineitem", {}).get("rows", 0),
            "unique_skus": sku0_stats.get("sku0_fact_lineitem", {}).get("unique_skus", 0),
            "unique_invoices": sku0_stats.get("sku0_fact_lineitem", {}).get("unique_invoices", 0),
            "total_quantity": sku0_stats.get("sku0_fact_lineitem", {}).get("total_quantity", 0),
            "total_revenue": sku0_stats.get("sku0_fact_lineitem", {}).get("total_revenue", 0),
            "weeks_of_data": sku0_stats.get("sku0_features_weekly", {}).get("unique_weeks", 0),
        },
        "cat_demand_0": {
            "tables": len([s for s in cat0_stats.values() if s.get("exists")]),
            "total_rows": sum(s.get("rows", 0) for s in cat0_stats.values() if s.get("exists")),
            "unique_categories": cat0_stats.get("cat0_features_weekly", {}).get("unique_categories", 0),
        }
    }

    report["totals"] = totals

    print()
    print("┌─────────────────────────────────────────────────────────────────┐")
    print("│ VERIFICATION CHECKSUMS                                         │")
    print("├─────────────────────────────────────────────────────────────────┤")
    print(f"│ rev0                                                            │")
    print(f"│   Tables: {totals['rev0']['tables']}                                                      │")
    print(f"│   Total Rows: {totals['rev0']['total_rows']:,}                                            │")
    print("├─────────────────────────────────────────────────────────────────┤")
    print(f"│ sku_demand_0                                                    │")
    print(f"│   Tables: {totals['sku_demand_0']['tables']}                                                      │")
    print(f"│   Line Items: {totals['sku_demand_0']['total_line_items']:,}                                       │")
    print(f"│   Unique SKUs: {totals['sku_demand_0']['unique_skus']:,}                                           │")
    print(f"│   Unique Invoices: {totals['sku_demand_0']['unique_invoices']:,}                                     │")
    print(f"│   Total Quantity: {totals['sku_demand_0']['total_quantity']:,} units                          │")
    print(f"│   Total Revenue: R{totals['sku_demand_0']['total_revenue']:,.2f}                       │")
    print(f"│   Weeks of Data: {totals['sku_demand_0']['weeks_of_data']}                                             │")
    print("├─────────────────────────────────────────────────────────────────┤")
    print(f"│ cat_demand_0                                                    │")
    print(f"│   Tables: {totals['cat_demand_0']['tables']}                                                      │")
    print(f"│   Total Rows: {totals['cat_demand_0']['total_rows']}                                               │")
    print(f"│   Unique Categories: {totals['cat_demand_0']['unique_categories']}                                        │")
    print("└─────────────────────────────────────────────────────────────────┘")

    # File hashes for verification
    print()
    print("FILE HASHES (verify integrity after upload):")
    print("-" * 50)
    for model_name, model_stats in [("rev0", rev0_stats), ("sku_demand_0", sku0_stats), ("cat_demand_0", cat0_stats)]:
        for file_name, stats in model_stats.items():
            if stats.get("exists"):
                print(f"  {file_name}: {stats['file_hash']}")

    # Save report to JSON
    report_path = BASE_PATH / "pre_eval_report.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print()
    print(f"Report saved to: {report_path}")
    print()
    print("=" * 70)
    print("PRE-EVALUATION COMPLETE")
    print("=" * 70)
    print()
    print("Next steps:")
    print("  1. Review the counts above")
    print("  2. Run: ./scripts/SKU_DEMAND_SETUP.sh")
    print("  3. Run: python3 scripts/POST_EVAL.py (to verify upload)")
    print()

if __name__ == "__main__":
    main()
