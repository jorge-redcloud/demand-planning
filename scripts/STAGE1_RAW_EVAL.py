#!/usr/bin/env python3
"""
STAGE 1: RAW DATA EVALUATION
============================
Scans all raw Excel files BEFORE extraction to establish baseline metrics.

This creates the "source of truth" numbers that we compare against:
- Stage 2: Post-extraction (CSV files)
- Stage 3: Post-upload (BigQuery)

Output:
- Console summary
- stage1_raw_eval.json (machine-readable)
"""

import pandas as pd
import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import warnings
warnings.filterwarnings('ignore')

# Configuration
BASE_PATH = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning")
DATA_PATH = BASE_PATH / "2025"

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
    patterns = ['summary', 'january', 'february', 'march', 'april', 'may', 'june',
                'july', 'august', 'september', 'october', 'november', 'december']

    for sheet in xl.sheet_names:
        sheet_lower = sheet.lower()
        if 'debtor' in sheet_lower or 'master' in sheet_lower:
            continue
        for pattern in patterns:
            if pattern in sheet_lower:
                return sheet
    return None

def count_invoice_sheets(xl):
    """Count sheets that look like invoice numbers."""
    count = 0
    for sheet_name in xl.sheet_names:
        sheet_lower = sheet_name.lower()
        if any(x in sheet_lower for x in ['debtor', 'master', 'summary', 'sheet']):
            continue
        if any(month.lower()[:3] in sheet_lower for month in MONTHS):
            continue
        clean_name = sheet_name.replace('.', '')
        if len(clean_name) >= 5 and clean_name.isdigit():
            count += 1
    return count

def analyze_raw_file(filepath, region):
    """Analyze a single raw Excel file."""
    result = {
        "filepath": str(filepath),
        "filename": filepath.name,
        "region": region,
        "status": "ok",
        "invoice_count": 0,
        "line_item_count": 0,
        "total_quantity": 0,
        "total_revenue": 0,
        "unique_skus": set(),
        "unique_customers": set(),
        "errors": []
    }

    try:
        xl = pd.ExcelFile(filepath)
        result["sheet_count"] = len(xl.sheet_names)

        # Find summary sheet
        summary_sheet = find_summary_sheet(xl)
        if summary_sheet:
            try:
                summary = pd.read_excel(xl, sheet_name=summary_sheet)
                summary.columns = [str(c).strip().lower().replace(' ', '_').replace('.', '') for c in summary.columns]

                # Count invoices from summary
                doc_col = None
                for col in summary.columns:
                    if 'document' in col or 'doc' in col:
                        doc_col = col
                        break

                if doc_col:
                    result["invoice_count"] = summary[doc_col].dropna().nunique()

                # Get customer count from summary
                acc_col = None
                for col in summary.columns:
                    if 'acc' in col:
                        acc_col = col
                        break
                if acc_col:
                    for val in summary[acc_col].dropna():
                        result["unique_customers"].add(str(val))

            except Exception as e:
                result["errors"].append(f"Summary sheet error: {e}")

        # Count invoice sheets
        invoice_sheet_count = count_invoice_sheets(xl)
        if invoice_sheet_count > result["invoice_count"]:
            result["invoice_count"] = invoice_sheet_count

        # Sample some invoice sheets to estimate line items
        sampled = 0
        sample_lineitems = 0
        sample_qty = 0
        sample_revenue = 0

        for sheet_name in xl.sheet_names:
            sheet_lower = sheet_name.lower()
            if any(x in sheet_lower for x in ['debtor', 'master', 'summary', 'sheet']):
                continue
            if any(month.lower()[:3] in sheet_lower for month in MONTHS):
                continue
            clean_name = sheet_name.replace('.', '')
            if not (len(clean_name) >= 5 and clean_name.isdigit()):
                continue

            try:
                df = pd.read_excel(xl, sheet_name=sheet_name)
                df.columns = [str(c).strip() for c in df.columns]

                # Find columns
                stock_col = None
                for col in df.columns:
                    if 'stock' in col.lower() or col.lower() == 'sku':
                        stock_col = col
                        break

                qty_col = None
                for col in df.columns:
                    if 'quant' in col.lower() or col.lower() == 'qty':
                        qty_col = col
                        break

                total_col = None
                for col in df.columns:
                    if 'total' in col.lower():
                        total_col = col
                        break

                if stock_col:
                    # Count valid line items (non-null SKU)
                    valid_rows = df[stock_col].dropna()
                    sample_lineitems += len(valid_rows)

                    # Collect unique SKUs
                    for sku in valid_rows:
                        try:
                            result["unique_skus"].add(str(int(float(sku))))
                        except:
                            result["unique_skus"].add(str(sku).strip())

                if qty_col:
                    qty_sum = pd.to_numeric(df[qty_col], errors='coerce').fillna(0)
                    sample_qty += qty_sum[qty_sum > 0].sum()

                if total_col:
                    rev_sum = pd.to_numeric(df[total_col], errors='coerce').fillna(0)
                    sample_revenue += rev_sum[rev_sum > 0].sum()

                sampled += 1

            except Exception as e:
                continue

        # Use sampled data (we're scanning all sheets, so this is actual not estimated)
        result["line_item_count"] = sample_lineitems
        result["total_quantity"] = int(sample_qty)
        result["total_revenue"] = round(sample_revenue, 2)

    except Exception as e:
        result["status"] = "error"
        result["errors"].append(str(e))

    # Convert sets to counts for JSON serialization
    result["unique_sku_count"] = len(result["unique_skus"])
    result["unique_customer_count"] = len(result["unique_customers"])
    del result["unique_skus"]
    del result["unique_customers"]

    return result

def main():
    print("=" * 70)
    print("STAGE 1: RAW DATA EVALUATION")
    print("=" * 70)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Scanning: {DATA_PATH}")
    print()

    report = {
        "stage": 1,
        "name": "Raw Excel Files",
        "generated_at": datetime.now().isoformat(),
        "files": [],
        "by_region": {},
        "by_month": {},
        "totals": {}
    }

    # Aggregate counters
    totals = {
        "files_scanned": 0,
        "files_with_errors": 0,
        "total_invoices": 0,
        "total_line_items": 0,
        "total_quantity": 0,
        "total_revenue": 0,
        "unique_skus": set(),
    }

    by_region = defaultdict(lambda: {
        "files": 0, "invoices": 0, "line_items": 0,
        "quantity": 0, "revenue": 0, "skus": set()
    })

    by_month = defaultdict(lambda: {
        "files": 0, "invoices": 0, "line_items": 0,
        "quantity": 0, "revenue": 0
    })

    # Scan all files
    for month_folder in MONTHS:
        month_path = DATA_PATH / month_folder
        if not month_path.exists():
            continue

        print(f"\n📁 {month_folder}")
        print("-" * 50)

        # Find regional files (prioritize -corrected versions)
        for filepath in sorted(month_path.glob("ZAF_ACA_*.xlsx")):
            # Skip non-corrected if corrected exists
            if '-corrected' not in filepath.name and ' - corrected' not in filepath.name:
                corrected1 = filepath.parent / filepath.name.replace('.xlsx', '-corrected.xlsx')
                corrected2 = filepath.parent / filepath.name.replace('.xlsx', ' - corrected.xlsx')
                if corrected1.exists() or corrected2.exists():
                    continue

            # Skip .xlsm files
            if filepath.suffix == '.xlsm':
                continue

            region = extract_region_from_filename(filepath.name)
            print(f"  📄 {filepath.name[:50]}...")

            result = analyze_raw_file(filepath, region)
            report["files"].append(result)

            # Update totals
            totals["files_scanned"] += 1
            if result["status"] != "ok":
                totals["files_with_errors"] += 1

            totals["total_invoices"] += result["invoice_count"]
            totals["total_line_items"] += result["line_item_count"]
            totals["total_quantity"] += result["total_quantity"]
            totals["total_revenue"] += result["total_revenue"]

            # Update by region
            by_region[region]["files"] += 1
            by_region[region]["invoices"] += result["invoice_count"]
            by_region[region]["line_items"] += result["line_item_count"]
            by_region[region]["quantity"] += result["total_quantity"]
            by_region[region]["revenue"] += result["total_revenue"]

            # Update by month
            by_month[month_folder]["files"] += 1
            by_month[month_folder]["invoices"] += result["invoice_count"]
            by_month[month_folder]["line_items"] += result["line_item_count"]
            by_month[month_folder]["quantity"] += result["total_quantity"]
            by_month[month_folder]["revenue"] += result["total_revenue"]

            print(f"     Invoices: {result['invoice_count']:,} | Lines: {result['line_item_count']:,} | "
                  f"Qty: {result['total_quantity']:,} | SKUs: {result['unique_sku_count']}")

    # Finalize report
    report["totals"] = {
        "files_scanned": totals["files_scanned"],
        "files_with_errors": totals["files_with_errors"],
        "total_invoices": totals["total_invoices"],
        "total_line_items": totals["total_line_items"],
        "total_quantity": totals["total_quantity"],
        "total_revenue": round(totals["total_revenue"], 2),
    }

    # Convert by_region (remove sets for JSON)
    for region, data in by_region.items():
        if "skus" in data:
            del data["skus"]
        report["by_region"][region] = data

    report["by_month"] = dict(by_month)

    # Print summary
    print("\n" + "=" * 70)
    print("STAGE 1 SUMMARY: RAW DATA TOTALS")
    print("=" * 70)

    print("\n┌─────────────────────────────────────────────────────────────────┐")
    print("│ RAW DATA BASELINE (Source of Truth)                            │")
    print("├─────────────────────────────────────────────────────────────────┤")
    print(f"│ Files Scanned:     {totals['files_scanned']:>10}                               │")
    print(f"│ Total Invoices:    {totals['total_invoices']:>10,}                               │")
    print(f"│ Total Line Items:  {totals['total_line_items']:>10,}                               │")
    print(f"│ Total Quantity:    {totals['total_quantity']:>10,} units                         │")
    print(f"│ Total Revenue:     R{totals['total_revenue']:>15,.2f}                    │")
    print("└─────────────────────────────────────────────────────────────────┘")

    print("\n BY REGION:")
    print("-" * 60)
    for region, data in sorted(by_region.items()):
        print(f"  {region:15} | Files: {data['files']:2} | Invoices: {data['invoices']:>6,} | "
              f"Lines: {data['line_items']:>7,} | Qty: {data['quantity']:>12,}")

    print("\n BY MONTH:")
    print("-" * 60)
    for month, data in by_month.items():
        print(f"  {month:15} | Files: {data['files']:2} | Invoices: {data['invoices']:>6,} | "
              f"Lines: {data['line_items']:>7,} | Qty: {data['quantity']:>12,}")

    # Save report
    report_path = BASE_PATH / "stage1_raw_eval.json"
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"\n📊 Report saved to: {report_path}")

    print("\n" + "=" * 70)
    print("STAGE 1 COMPLETE")
    print("=" * 70)
    print("\nNext steps:")
    print("  1. Run extraction: python3 scripts/extract_sku_data.py")
    print("  2. Run Stage 2:    python3 scripts/STAGE2_EXTRACTION_EVAL.py")
    print("  3. Upload to BQ:   ./scripts/SKU_DEMAND_SETUP.sh")
    print("  4. Run Stage 3:    ./scripts/STAGE3_BIGQUERY_EVAL.sh")
    print()

if __name__ == "__main__":
    main()
