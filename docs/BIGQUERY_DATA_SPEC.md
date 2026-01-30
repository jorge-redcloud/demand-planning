# BigQuery Data Upload Specification
## RedAI ACA Demand Planning

**Last Updated:** January 2026
**Purpose:** Ensure consistent data types and formats when uploading CSV files to BigQuery

---

## Known Issues & Solutions

### 1. Date/Time Formats

BigQuery is strict about date and timestamp formats. Here are the formats used in this project:

| Column Pattern | Current Format | BigQuery Type | Notes |
|---------------|----------------|---------------|-------|
| `order_date`, `invoice_date` | `2025-04-01 00:00:00` | `TIMESTAMP` | Include time component |
| `week_start` | `2025-01-06` | `DATE` | ISO 8601 format |
| `year_week` | `2025-W14` | `STRING` | ISO week format, keep as STRING |

**⚠️ CRITICAL:** The `year_week` column uses ISO format `YYYY-Wnn` which BigQuery cannot parse as a date. Always load as `STRING` and extract week number using:
```sql
CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num
```

### 2. Python List Syntax in CSV

**Problem:** Some columns contain Python list representations like `['592', '573']`

**Affected Files:**
- `customer_master_mapping.csv` → `original_ids` column

**Solution:** Before upload, convert to comma-separated string:
```python
# Fix before exporting to CSV
df['original_ids'] = df['original_ids'].apply(
    lambda x: ','.join(map(str, x)) if isinstance(x, list) else str(x).strip("[]'").replace("', '", ",")
)
```

Or in BigQuery after upload, parse with:
```sql
SPLIT(REGEXP_REPLACE(original_ids, r"[\[\]' ]", ''), ',') AS original_id_array
```

### 3. Numeric Precision & Mixed-Type IDs

| Column Pattern | Python Type | BigQuery Type | Notes |
|---------------|-------------|---------------|-------|
| `master_customer_id`, `sku` | int | `INT64` | Pure numeric |
| `original_customer_id` | mixed | `STRING` | **Contains alphanumeric IDs!** |
| `quantity`, `*_count` | float | `INT64` or `FLOAT64` | Round if needed |
| `*_price`, `*_revenue`, `*_total` | float | `FLOAT64` | Allow decimals |
| `*_pct`, `*_error`, `mape` | float | `FLOAT64` | Percentages as decimals |

**⚠️ CRITICAL - MIXED CUSTOMER IDS:**
The `original_customer_id` column contains BOTH numeric IDs (e.g., `592`) AND alphanumeric IDs (e.g., `HB_CUT001`, `HB_HOP001`).

- **63 out of 804 original customer IDs are alphanumeric** (HB_* prefix pattern)
- Always load `original_customer_id` as `STRING`
- The `master_customer_id` is always numeric `INT64`

```sql
-- Safe join using STRING comparison
SELECT * FROM fact_lineitem f
JOIN customer_id_lookup c
  ON CAST(f.customer_id AS STRING) = c.original_customer_id
```

### 4. NULL/Empty Value Handling

| CSV Value | BigQuery Interpretation | Recommendation |
|-----------|------------------------|----------------|
| Empty string `""` | Empty string (not NULL) | Replace with NULL if numeric |
| `NaN` | String "NaN" | Replace with NULL before upload |
| `None` | String "None" | Replace with NULL before upload |
| `inf`, `-inf` | String | Replace with NULL or cap value |

**Pre-upload Python cleanup:**
```python
import numpy as np

# Replace problematic values
df = df.replace([np.inf, -np.inf], np.nan)
df = df.replace(['NaN', 'None', 'nan', 'none', ''], np.nan)

# For CSV export, use na_rep to control NULL representation
df.to_csv('output.csv', index=False, na_rep='')
```

---

## Table Schemas

### Core Tables

#### `fact_lineitem`
```sql
CREATE TABLE fact_lineitem (
  invoice_id STRING NOT NULL,
  order_date TIMESTAMP NOT NULL,
  customer_id STRING NOT NULL,  -- STRING because some IDs are alphanumeric (HB_*)
  customer_name STRING,
  region_name STRING,
  sku INT64 NOT NULL,
  description STRING,
  quantity FLOAT64,
  unit_price FLOAT64,
  line_total FLOAT64,
  year_week STRING NOT NULL,  -- Keep as STRING (YYYY-Wnn format)
  data_completeness STRING,
  customer_segment STRING,
  buyer_type STRING,
  category_l1 STRING,
  category_l2 STRING
);
```

#### `customer_master_mapping`
```sql
CREATE TABLE customer_master_mapping (
  master_customer_id INT64 NOT NULL,
  customer_name STRING NOT NULL,
  original_ids STRING,  -- Comma-separated, NOT array syntax
  num_ids INT64,
  total_volume FLOAT64
);
```

#### `customer_id_lookup`
```sql
CREATE TABLE customer_id_lookup (
  original_customer_id STRING NOT NULL,  -- STRING: contains both numeric (592) and alphanumeric (HB_CUT001)
  master_customer_id INT64 NOT NULL,
  customer_name STRING
);
```

#### `features_weekly` (SKU-level)
```sql
CREATE TABLE features_weekly (
  sku INT64 NOT NULL,
  year_week STRING NOT NULL,
  weekly_quantity FLOAT64,
  avg_unit_price FLOAT64,
  weekly_revenue FLOAT64,
  order_count INT64,
  unique_customers INT64,
  description STRING,
  data_completeness STRING,
  lag1_quantity FLOAT64,
  lag2_quantity FLOAT64,
  lag4_quantity FLOAT64,
  rolling_avg_4w FLOAT64,
  price_change FLOAT64,
  price_change_pct FLOAT64
);
```

#### `features_customer_normalized`
```sql
CREATE TABLE features_customer_normalized (
  master_customer_id INT64 NOT NULL,
  customer_name STRING,
  year_week STRING NOT NULL,
  quantity FLOAT64,
  revenue FLOAT64,
  order_count INT64,
  sku_count INT64,
  avg_unit_price FLOAT64,
  lag1_quantity FLOAT64,
  lag2_quantity FLOAT64,
  lag4_quantity FLOAT64,
  rolling_avg_4w FLOAT64,
  week_num INT64,
  period STRING  -- 'H1' or 'H2'
);
```

#### `predictions_customer`
```sql
CREATE TABLE predictions_customer (
  master_customer_id INT64 NOT NULL,
  customer_name STRING,
  year_week STRING NOT NULL,
  actual FLOAT64,
  predicted FLOAT64,
  error FLOAT64,
  abs_error FLOAT64,
  pct_error FLOAT64
);
```

---

## Upload Commands

### Using bq load (recommended)

```bash
# Basic upload with autodetect
bq load --source_format=CSV --autodetect \
  PROJECT:DATASET.table_name \
  ./file.csv

# Upload with explicit schema (safer)
bq load --source_format=CSV \
  --skip_leading_rows=1 \
  PROJECT:DATASET.table_name \
  ./file.csv \
  schema.json

# Replace existing table
bq load --source_format=CSV --autodetect --replace \
  PROJECT:DATASET.table_name \
  ./file.csv
```

### Schema JSON Example

Create `customer_lookup_schema.json`:
```json
[
  {"name": "original_customer_id", "type": "INT64", "mode": "REQUIRED"},
  {"name": "master_customer_id", "type": "INT64", "mode": "REQUIRED"},
  {"name": "customer_name", "type": "STRING", "mode": "NULLABLE"}
]
```

---

## Pre-Upload Validation Script

Run this Python script before any BigQuery upload:

```python
"""
bigquery_prevalidate.py - Validate CSV files before BigQuery upload
"""
import pandas as pd
import numpy as np
import sys
import re

def validate_csv_for_bigquery(filepath):
    """Validate and report issues with a CSV file for BigQuery upload."""

    print(f"\n{'='*60}")
    print(f"Validating: {filepath}")
    print('='*60)

    df = pd.read_csv(filepath, nrows=1000)  # Sample first 1000 rows
    issues = []

    # Check 1: Python list syntax
    for col in df.columns:
        sample = df[col].dropna().astype(str).head(10)
        if sample.str.contains(r"^\[.*\]$", regex=True).any():
            issues.append(f"⚠️  Column '{col}' contains Python list syntax")

    # Check 2: NaN/None/inf values
    for col in df.select_dtypes(include=[np.number]).columns:
        if df[col].isna().sum() > 0:
            print(f"ℹ️  Column '{col}' has {df[col].isna().sum()} NULL values")
        if np.isinf(df[col]).any():
            issues.append(f"⚠️  Column '{col}' contains inf values")

    # Check 3: String columns with 'nan', 'None'
    for col in df.select_dtypes(include=['object']).columns:
        problematic = df[col].isin(['nan', 'NaN', 'None', 'none', 'NULL'])
        if problematic.sum() > 0:
            issues.append(f"⚠️  Column '{col}' has {problematic.sum()} string NULL values")

    # Check 4: ID columns as floats
    id_cols = [c for c in df.columns if c.endswith('_id') or c == 'sku']
    for col in id_cols:
        if df[col].dtype == float:
            if (df[col] % 1 != 0).any():
                issues.append(f"⚠️  ID column '{col}' has decimal values")
            else:
                print(f"ℹ️  ID column '{col}' is float but all integers (will cast to INT64)")

    # Check 5: Date columns
    date_cols = [c for c in df.columns if 'date' in c.lower() or 'week' in c.lower()]
    for col in date_cols:
        sample = df[col].dropna().astype(str).head(5)
        print(f"ℹ️  Date column '{col}' sample: {sample.tolist()}")
        if col.endswith('_week') or col == 'year_week':
            if sample.str.match(r'^\d{4}-W\d{2}$').all():
                print(f"   ✓ ISO week format detected - load as STRING")

    # Check 6: Column names (BigQuery prefers lowercase, no spaces)
    bad_names = [c for c in df.columns if ' ' in c or c != c.lower()]
    if bad_names:
        issues.append(f"⚠️  Column names with spaces/uppercase: {bad_names}")

    # Summary
    print(f"\n{'='*60}")
    if issues:
        print("ISSUES FOUND:")
        for issue in issues:
            print(f"  {issue}")
        print("\nRun fix_csv_for_bigquery() to auto-fix these issues")
        return False
    else:
        print("✓ No issues found - safe to upload")
        return True


def fix_csv_for_bigquery(input_path, output_path=None):
    """Fix common BigQuery compatibility issues in a CSV file."""

    if output_path is None:
        output_path = input_path.replace('.csv', '_bq.csv')

    print(f"\nFixing: {input_path}")
    df = pd.read_csv(input_path)

    # Fix 1: Replace inf values
    df = df.replace([np.inf, -np.inf], np.nan)

    # Fix 2: Convert Python list strings to comma-separated
    for col in df.columns:
        if df[col].dtype == object:
            # Check if column has list-like strings
            mask = df[col].astype(str).str.match(r"^\[.*\]$", na=False)
            if mask.any():
                df[col] = df[col].astype(str).str.replace(r"[\[\]']", '', regex=True)
                print(f"  Fixed list syntax in '{col}'")

    # Fix 3: Convert ID columns to int
    id_cols = [c for c in df.columns if c.endswith('_id') or c == 'sku']
    for col in id_cols:
        if df[col].dtype == float:
            df[col] = df[col].astype('Int64')  # Nullable integer
            print(f"  Converted '{col}' to integer")

    # Fix 4: Lowercase column names
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Fix 5: Replace string nulls
    df = df.replace(['nan', 'NaN', 'None', 'none', 'NULL', 'null'], np.nan)

    # Save
    df.to_csv(output_path, index=False, na_rep='')
    print(f"  Saved to: {output_path}")

    return output_path


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python bigquery_prevalidate.py <csv_file> [--fix]")
        sys.exit(1)

    filepath = sys.argv[1]

    if '--fix' in sys.argv:
        fix_csv_for_bigquery(filepath)
    else:
        validate_csv_for_bigquery(filepath)
```

---

## Common SQL Transformations

### Extract Week Number from year_week
```sql
CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num
```

### Convert year_week to DATE (first day of week)
```sql
PARSE_DATE('%G-W%V', CONCAT(year_week, '-1')) AS week_start_date
```

### Handle NULL in Lag Features
```sql
COALESCE(lag1_quantity, 0) AS lag1_quantity,
COALESCE(rolling_avg_4w, weekly_quantity) AS rolling_avg_4w
```

### Safe Division (avoid divide by zero)
```sql
SAFE_DIVIDE(ABS(actual - predicted), actual) * 100 AS pct_error
```

### Parse Original IDs Array (after fixing list syntax)
```sql
SPLIT(original_ids, ',') AS original_id_array
```

---

## Checklist Before Upload

- [ ] Run `bigquery_prevalidate.py` on each CSV file
- [ ] Fix any issues using `--fix` flag or manually
- [ ] Verify `year_week` columns will load as STRING
- [ ] Verify ID columns are integers (no `.0` decimals)
- [ ] Verify no Python list syntax `[...]` in any column
- [ ] Verify no `NaN`, `None`, `inf` string values
- [ ] Test load with `--dry_run` first if available
- [ ] Use explicit schema JSON for critical tables

---

## File-Specific Notes

### customer_master_mapping.csv
- **Fix Required:** `original_ids` column has `['592']` format
- **Action:** Run `fix_csv_for_bigquery()` or manually replace list syntax

### customer_id_lookup.csv
- ✓ Clean format, ready for upload

### v2_fact_lineitem.csv
- `order_date` is TIMESTAMP format (`2025-04-01 00:00:00`)
- `year_week` is STRING format (`2025-W14`)
- ✓ Should load correctly with autodetect

### customer_predictions_XGBoost_normalized.csv
- ✓ Clean format, ready for upload
- Note: `pct_error` can have large values (>100%) for poor predictions
