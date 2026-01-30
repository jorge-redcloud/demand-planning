#!/usr/bin/env python3
"""
STAGE 2: EXTRACTION EVALUATION
==============================
Evaluates the extracted CSV files and compares against Stage 1 (raw data).

Shows:
- What we extracted vs what was in raw files
- Data loss % at each dimension
- Reasons for data loss (missing SKUs, unknown regions, etc.)

Output:
- Console summary with comparison
- stage2_extraction_eval.json
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuration
BASE_PATH = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning")
FEATURES_SKU_PATH = BASE_PATH / "features_sku"
FEATURES_CAT_PATH = BASE_PATH / "features_category"

def load_stage1_report():
    """Load Stage 1 raw data report."""
    report_path = BASE_PATH / "stage1_raw_eval.json"
    if report_path.exists():
        with open(report_path) as f:
            return json.load(f)
    return None

def analyze_extraction():
    """Analyze extracted CSV files."""
    result = {
        "stage": 2,
        "name": "Extraction Pipeline Output",
        "generated_at": datetime.now().isoformat(),
        "files": {},
        "totals": {},
        "by_region": {},
        "data_quality": {}
    }

    # Load fact_lineitem
    lineitem_path = FEATURES_SKU_PATH / "sku0_fact_lineitem.csv"
    if lineitem_path.exists():
        df = pd.read_csv(lineitem_path)

        result["files"]["sku0_fact_lineitem"] = {
            "rows": len(df),
            "columns": len(df.columns)
        }

        result["totals"] = {
            "total_line_items": len(df),
            "total_invoices": df['invoice_id'].nunique(),
            "total_quantity": int(df['quantity'].sum()),
            "total_revenue": round(df['line_total'].sum(), 2),
            "unique_skus": df['sku'].nunique(),
            "unique_customers": df['customer_id'].nunique(),
            "unique_regions": df['region_name'].nunique(),
            "regions": df['region_name'].unique().tolist(),
            "date_range": [str(df['order_date'].min()), str(df['order_date'].max())]
        }

        # By region breakdown
        for region in df['region_name'].unique():
            region_df = df[df['region_name'] == region]
            result["by_region"][region] = {
                "line_items": len(region_df),
                "invoices": region_df['invoice_id'].nunique(),
                "quantity": int(region_df['quantity'].sum()),
                "revenue": round(region_df['line_total'].sum(), 2),
                "skus": region_df['sku'].nunique()
            }

        # Data quality checks
        result["data_quality"] = {
            "null_skus": int(df['sku'].isna().sum()),
            "null_quantities": int(df['quantity'].isna().sum()),
            "zero_quantities": int((df['quantity'] == 0).sum()),
            "negative_quantities": int((df['quantity'] < 0).sum()),
            "null_prices": int(df['unit_price'].isna().sum()),
            "zero_prices": int((df['unit_price'] == 0).sum()),
            "unknown_region_count": int((df['region_name'] == 'Unknown').sum()),
            "empty_customer_ids": int((df['customer_id'] == '').sum() + df['customer_id'].isna().sum()),
        }

    # Load features_weekly
    weekly_path = FEATURES_SKU_PATH / "sku0_features_weekly.csv"
    if weekly_path.exists():
        df_weekly = pd.read_csv(weekly_path)
        result["files"]["sku0_features_weekly"] = {
            "rows": len(df_weekly),
            "unique_skus": df_weekly['sku'].nunique(),
            "unique_weeks": df_weekly['year_week'].nunique(),
            "week_range": [df_weekly['year_week'].min(), df_weekly['year_week'].max()]
        }

    # Load category features
    cat_path = FEATURES_CAT_PATH / "cat0_features_weekly.csv"
    if cat_path.exists():
        df_cat = pd.read_csv(cat_path)
        result["files"]["cat0_features_weekly"] = {
            "rows": len(df_cat),
            "unique_categories": df_cat['category'].nunique(),
            "categories": df_cat['category'].unique().tolist()
        }

    return result

def compare_stages(stage1, stage2):
    """Compare Stage 1 and Stage 2 metrics."""
    comparison = {
        "line_items": {
            "raw": stage1["totals"]["total_line_items"],
            "extracted": stage2["totals"]["total_line_items"],
            "diff": stage2["totals"]["total_line_items"] - stage1["totals"]["total_line_items"],
            "pct": round((stage2["totals"]["total_line_items"] / stage1["totals"]["total_line_items"]) * 100, 2) if stage1["totals"]["total_line_items"] > 0 else 0
        },
        "invoices": {
            "raw": stage1["totals"]["total_invoices"],
            "extracted": stage2["totals"]["total_invoices"],
            "diff": stage2["totals"]["total_invoices"] - stage1["totals"]["total_invoices"],
            "pct": round((stage2["totals"]["total_invoices"] / stage1["totals"]["total_invoices"]) * 100, 2) if stage1["totals"]["total_invoices"] > 0 else 0
        },
        "quantity": {
            "raw": stage1["totals"]["total_quantity"],
            "extracted": stage2["totals"]["total_quantity"],
            "diff": stage2["totals"]["total_quantity"] - stage1["totals"]["total_quantity"],
            "pct": round((stage2["totals"]["total_quantity"] / stage1["totals"]["total_quantity"]) * 100, 2) if stage1["totals"]["total_quantity"] > 0 else 0
        },
        "revenue": {
            "raw": stage1["totals"]["total_revenue"],
            "extracted": stage2["totals"]["total_revenue"],
            "diff": round(stage2["totals"]["total_revenue"] - stage1["totals"]["total_revenue"], 2),
            "pct": round((stage2["totals"]["total_revenue"] / stage1["totals"]["total_revenue"]) * 100, 2) if stage1["totals"]["total_revenue"] > 0 else 0
        }
    }

    # By region comparison
    comparison["by_region"] = {}
    for region in set(list(stage1.get("by_region", {}).keys()) + list(stage2.get("by_region", {}).keys())):
        raw = stage1.get("by_region", {}).get(region, {})
        ext = stage2.get("by_region", {}).get(region, {})

        raw_qty = raw.get("quantity", 0)
        ext_qty = ext.get("quantity", 0)

        comparison["by_region"][region] = {
            "raw_qty": raw_qty,
            "extracted_qty": ext_qty,
            "diff": ext_qty - raw_qty,
            "pct": round((ext_qty / raw_qty) * 100, 2) if raw_qty > 0 else (100 if ext_qty > 0 else 0)
        }

    return comparison

def main():
    print("=" * 70)
    print("STAGE 2: EXTRACTION EVALUATION")
    print("=" * 70)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Load Stage 1 report
    stage1 = load_stage1_report()
    if not stage1:
        print("❌ ERROR: Stage 1 report not found!")
        print("   Run: python3 scripts/STAGE1_RAW_EVAL.py first")
        return

    print("✓ Loaded Stage 1 (Raw Data) report")

    # Analyze Stage 2
    stage2 = analyze_extraction()
    print("✓ Analyzed Stage 2 (Extraction) files")

    # Compare
    comparison = compare_stages(stage1, stage2)

    # Print comparison
    print("\n" + "=" * 70)
    print("STAGE 1 vs STAGE 2 COMPARISON")
    print("=" * 70)

    print("\n┌────────────────────────────────────────────────────────────────────────┐")
    print("│ METRIC              │ STAGE 1 (Raw)  │ STAGE 2 (Extract) │ CAPTURE %  │")
    print("├────────────────────────────────────────────────────────────────────────┤")

    for metric, data in comparison.items():
        if metric == "by_region":
            continue
        raw = data["raw"]
        ext = data["extracted"]
        pct = data["pct"]

        # Format numbers
        if metric == "revenue":
            raw_str = f"R{raw:>13,.0f}"
            ext_str = f"R{ext:>13,.0f}"
        else:
            raw_str = f"{raw:>14,}"
            ext_str = f"{ext:>14,}"

        # Color code percentage
        if pct >= 99:
            pct_str = f"✅ {pct:>5.1f}%"
        elif pct >= 95:
            pct_str = f"⚠️  {pct:>5.1f}%"
        else:
            pct_str = f"❌ {pct:>5.1f}%"

        print(f"│ {metric.upper():18} │ {raw_str:>14} │ {ext_str:>17} │ {pct_str:>10} │")

    print("└────────────────────────────────────────────────────────────────────────┘")

    # By region breakdown
    print("\n BY REGION COMPARISON:")
    print("-" * 70)
    print(f"  {'REGION':<15} │ {'RAW QTY':>12} │ {'EXTRACTED':>12} │ {'CAPTURE %':>10}")
    print("-" * 70)

    for region, data in sorted(comparison["by_region"].items()):
        pct = data["pct"]
        if pct >= 99:
            status = "✅"
        elif pct >= 95:
            status = "⚠️"
        elif pct == 0:
            status = "🆕"  # New in extraction
        else:
            status = "❌"

        print(f"  {region:<15} │ {data['raw_qty']:>12,} │ {data['extracted_qty']:>12,} │ {status} {pct:>6.1f}%")

    # Data quality issues
    print("\n DATA QUALITY ISSUES:")
    print("-" * 70)
    dq = stage2.get("data_quality", {})
    issues = []
    if dq.get("null_skus", 0) > 0:
        issues.append(f"  • Null SKUs: {dq['null_skus']:,}")
    if dq.get("zero_quantities", 0) > 0:
        issues.append(f"  • Zero quantities: {dq['zero_quantities']:,}")
    if dq.get("zero_prices", 0) > 0:
        issues.append(f"  • Zero prices: {dq['zero_prices']:,}")
    if dq.get("unknown_region_count", 0) > 0:
        issues.append(f"  • Unknown region: {dq['unknown_region_count']:,} line items")
    if dq.get("empty_customer_ids", 0) > 0:
        issues.append(f"  • Empty customer IDs: {dq['empty_customer_ids']:,}")

    if issues:
        for issue in issues:
            print(issue)
    else:
        print("  ✅ No major data quality issues found")

    # Summary
    print("\n" + "=" * 70)
    print("EXTRACTION SUMMARY")
    print("=" * 70)

    overall_capture = comparison["line_items"]["pct"]
    if overall_capture >= 99:
        print(f"\n✅ EXCELLENT: Captured {overall_capture:.1f}% of raw data")
    elif overall_capture >= 95:
        print(f"\n⚠️  GOOD: Captured {overall_capture:.1f}% of raw data (minor loss)")
    elif overall_capture >= 90:
        print(f"\n⚠️  ACCEPTABLE: Captured {overall_capture:.1f}% of raw data")
    else:
        print(f"\n❌ WARNING: Only captured {overall_capture:.1f}% of raw data")

    # Explain discrepancies
    if comparison["line_items"]["diff"] != 0:
        diff = comparison["line_items"]["diff"]
        if diff > 0:
            print(f"\n   +{diff:,} extra line items (likely from duplicate file processing)")
        else:
            print(f"\n   {diff:,} line items lost. Possible reasons:")
            print("     - Files with different structure not fully parsed")
            print("     - Null/invalid SKUs filtered out")
            print("     - Zero/negative quantities excluded")

    # Save report
    stage2["comparison"] = comparison
    report_path = BASE_PATH / "stage2_extraction_eval.json"
    with open(report_path, "w") as f:
        json.dump(stage2, f, indent=2, default=str)

    print(f"\n📊 Report saved to: {report_path}")

    print("\n" + "=" * 70)
    print("STAGE 2 COMPLETE")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Upload to BQ:   ./scripts/SKU_DEMAND_SETUP.sh")
    print("  2. Run Stage 3:    ./scripts/STAGE3_BIGQUERY_EVAL.sh")
    print()

if __name__ == "__main__":
    main()
