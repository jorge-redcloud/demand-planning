#!/usr/bin/env python3
"""
bigquery_prevalidate.py - Validate and fix CSV files before BigQuery upload

Usage:
    python bigquery_prevalidate.py <csv_file>           # Validate only
    python bigquery_prevalidate.py <csv_file> --fix     # Validate and fix
    python bigquery_prevalidate.py --all                # Validate all CSVs in current dir
    python bigquery_prevalidate.py --all --fix          # Fix all CSVs

Known issues this script handles:
    1. Python list syntax ['value'] in CSV columns
    2. NaN, None, inf string values
    3. Float IDs that should be integers
    4. Mixed-type customer IDs (numeric + alphanumeric)
    5. Date format verification for year_week columns
"""

import pandas as pd
import numpy as np
import sys
import os
import glob
import re


def validate_csv_for_bigquery(filepath, verbose=True):
    """
    Validate a CSV file for BigQuery compatibility.
    Returns (is_valid, issues_list)
    """
    if verbose:
        print(f"\n{'='*60}")
        print(f"Validating: {filepath}")
        print('='*60)

    try:
        df = pd.read_csv(filepath, nrows=5000)  # Sample first 5000 rows
    except Exception as e:
        return False, [f"❌ Could not read file: {e}"]

    issues = []
    warnings = []

    # Check 1: Python list syntax in any column
    for col in df.columns:
        sample = df[col].dropna().astype(str).head(100)
        if sample.str.contains(r"^\[.*\]$", regex=True).any():
            issues.append(f"⚠️  Column '{col}' contains Python list syntax (e.g., ['value'])")

    # Check 2: NaN/None/inf values in numeric columns
    for col in df.select_dtypes(include=[np.number]).columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            warnings.append(f"ℹ️  Column '{col}' has {null_count} NULL values (OK for BigQuery)")
        if np.isinf(df[col]).any():
            issues.append(f"⚠️  Column '{col}' contains inf/-inf values (BigQuery will fail)")

    # Check 3: String columns with problematic NULL representations
    for col in df.select_dtypes(include=['object']).columns:
        problematic = df[col].isin(['nan', 'NaN', 'None', 'none', 'NULL', 'null'])
        if problematic.sum() > 0:
            issues.append(f"⚠️  Column '{col}' has {problematic.sum()} string NULL values ('nan', 'None', etc)")

    # Check 4: ID columns - detect mixed types
    id_cols = [c for c in df.columns if '_id' in c.lower() or c.lower() == 'sku']
    for col in id_cols:
        if df[col].dtype == float:
            # Check if all values are actually integers
            non_null = df[col].dropna()
            if len(non_null) > 0 and (non_null % 1 == 0).all():
                warnings.append(f"ℹ️  ID column '{col}' is float but all integers (will convert to INT64)")
            else:
                issues.append(f"⚠️  ID column '{col}' has non-integer values")
        elif df[col].dtype == object:
            # Check for mixed numeric/alphanumeric
            numeric_mask = df[col].astype(str).str.match(r'^\d+\.?\d*$', na=False)
            alpha_mask = df[col].astype(str).str.match(r'^[A-Za-z_]', na=False)
            if numeric_mask.any() and alpha_mask.any():
                num_count = numeric_mask.sum()
                alpha_count = alpha_mask.sum()
                warnings.append(f"ℹ️  Column '{col}' has MIXED types: {num_count} numeric, {alpha_count} alphanumeric → use STRING")

    # Check 5: Date/week columns
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'week' in c.lower()]
    for col in date_cols:
        sample = df[col].dropna().astype(str).head(5)
        if col.lower() == 'year_week' or col.endswith('_week'):
            if sample.str.match(r'^\d{4}-W\d{2}$').all():
                warnings.append(f"ℹ️  Column '{col}' uses ISO week format (YYYY-Wnn) → load as STRING")
            elif not sample.str.match(r'^\d{4}-W\d{2}$').any():
                issues.append(f"⚠️  Column '{col}' has unexpected format: {sample.tolist()}")
        if verbose:
            print(f"   Date column '{col}' samples: {sample.head(3).tolist()}")

    # Check 6: Column names
    bad_names = [c for c in df.columns if ' ' in c]
    if bad_names:
        issues.append(f"⚠️  Column names with spaces (will cause issues): {bad_names}")

    # Print results
    if verbose:
        print(f"\nRows sampled: {len(df)}")
        print(f"Columns: {len(df.columns)}")

        if warnings:
            print(f"\nWARNINGS ({len(warnings)}):")
            for w in warnings:
                print(f"  {w}")

        if issues:
            print(f"\nISSUES FOUND ({len(issues)}):")
            for issue in issues:
                print(f"  {issue}")
            print("\n→ Run with --fix to auto-repair these issues")
        else:
            print("\n✓ No critical issues found - safe to upload")

    return len(issues) == 0, issues


def fix_csv_for_bigquery(input_path, output_path=None):
    """
    Fix common BigQuery compatibility issues in a CSV file.
    """
    if output_path is None:
        # Overwrite the original file
        output_path = input_path

    print(f"\n{'='*60}")
    print(f"Fixing: {input_path}")
    print('='*60)

    df = pd.read_csv(input_path)
    fixes_applied = []

    # Fix 1: Replace inf values
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    for col in numeric_cols:
        if np.isinf(df[col]).any():
            df[col] = df[col].replace([np.inf, -np.inf], np.nan)
            fixes_applied.append(f"Replaced inf values in '{col}'")

    # Fix 2: Convert Python list strings to comma-separated
    for col in df.columns:
        if df[col].dtype == object:
            mask = df[col].astype(str).str.match(r"^\[.*\]$", na=False)
            if mask.any():
                df[col] = df[col].astype(str).str.replace(r"[\[\]']", '', regex=True).str.strip()
                fixes_applied.append(f"Fixed list syntax in '{col}'")

    # Fix 3: Convert float ID columns to int (where possible)
    id_cols = [c for c in df.columns if c.endswith('_id') and c != 'original_customer_id']
    id_cols += ['sku'] if 'sku' in df.columns else []

    for col in id_cols:
        if col in df.columns and df[col].dtype == float:
            # Only convert if all non-null values are integers
            non_null = df[col].dropna()
            if len(non_null) > 0 and (non_null % 1 == 0).all():
                df[col] = df[col].astype('Int64')  # Nullable integer
                fixes_applied.append(f"Converted '{col}' to integer")

    # Fix 4: Replace string nulls
    string_cols = df.select_dtypes(include=['object']).columns
    for col in string_cols:
        mask = df[col].isin(['nan', 'NaN', 'None', 'none', 'NULL', 'null'])
        if mask.any():
            df.loc[mask, col] = np.nan
            fixes_applied.append(f"Replaced string NULLs in '{col}'")

    # Fix 5: Standardize column names (lowercase, no spaces)
    old_cols = list(df.columns)
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    renamed = [(old, new) for old, new in zip(old_cols, df.columns) if old != new]
    if renamed:
        fixes_applied.append(f"Renamed columns: {renamed}")

    # Save
    df.to_csv(output_path, index=False, na_rep='')

    print(f"\nFixes applied ({len(fixes_applied)}):")
    for fix in fixes_applied:
        print(f"  ✓ {fix}")
    print(f"\n✓ Saved to: {output_path}")

    return output_path


def main():
    if len(sys.argv) < 2 or '--help' in sys.argv or '-h' in sys.argv:
        print(__doc__)
        print("\nExamples:")
        print("  python bigquery_prevalidate.py customer_master_mapping.csv")
        print("  python bigquery_prevalidate.py customer_master_mapping.csv --fix")
        print("  python bigquery_prevalidate.py --all")
        print("  python bigquery_prevalidate.py --all --fix")
        sys.exit(0)

    do_fix = '--fix' in sys.argv
    do_all = '--all' in sys.argv

    if do_all:
        # Find all CSV files in current directory and subdirectories
        csv_files = glob.glob('**/*.csv', recursive=True)
        print(f"Found {len(csv_files)} CSV files")

        results = {'valid': [], 'invalid': [], 'fixed': []}

        for csv_file in csv_files:
            is_valid, issues = validate_csv_for_bigquery(csv_file, verbose=False)
            if is_valid:
                results['valid'].append(csv_file)
            else:
                results['invalid'].append((csv_file, issues))
                if do_fix:
                    fix_csv_for_bigquery(csv_file)
                    results['fixed'].append(csv_file)

        # Summary
        print(f"\n{'='*60}")
        print("SUMMARY")
        print('='*60)
        print(f"✓ Valid files: {len(results['valid'])}")
        print(f"⚠️ Files with issues: {len(results['invalid'])}")
        if do_fix:
            print(f"🔧 Files fixed: {len(results['fixed'])}")

        if results['invalid'] and not do_fix:
            print("\nFiles needing fixes:")
            for filepath, issues in results['invalid']:
                print(f"\n  {filepath}:")
                for issue in issues:
                    print(f"    {issue}")
    else:
        # Single file mode
        filepath = [arg for arg in sys.argv[1:] if not arg.startswith('--')][0]

        if not os.path.exists(filepath):
            print(f"Error: File not found: {filepath}")
            sys.exit(1)

        is_valid, issues = validate_csv_for_bigquery(filepath)

        if do_fix and not is_valid:
            fix_csv_for_bigquery(filepath)
            # Re-validate
            print("\n--- Re-validating after fix ---")
            validate_csv_for_bigquery(filepath)


if __name__ == '__main__':
    main()
