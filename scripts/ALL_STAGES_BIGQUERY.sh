#!/bin/bash
# =============================================================================
# ALL_STAGES_BIGQUERY.sh - Upload all pipeline stages to BigQuery
# =============================================================================
# This script uploads data from ALL stages for full traceability:
#
#   stage1_raw      - Raw file metadata (from STAGE1_RAW_EVAL.py)
#   stage2_extract  - Extracted data (from extract_sku_data.py)
#   stage2_5_enrich - Enriched data (from STAGE2_5_ENRICH.py)
#
# Each stage is preserved for comparison and audit purposes.
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
BUCKET_NAME="demand_planning_aca"
DATASET_NAME="demand_forecasting"
BASE_PATH="/sessions/affectionate-pensive-goodall/mnt/demand planning"

echo "=============================================="
echo "ALL STAGES BIGQUERY UPLOAD"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Dataset: $DATASET_NAME"
echo ""

# =============================================================================
# STEP 1: Rename existing tables to rev0_ prefix (if not already done)
# =============================================================================
echo "=============================================="
echo "STEP 1: Ensuring rev0_ table structure"
echo "=============================================="

rename_if_exists() {
    local old_name=$1
    local new_name=$2
    if bq show "${PROJECT_ID}:${DATASET_NAME}.${old_name}" > /dev/null 2>&1; then
        if ! bq show "${PROJECT_ID}:${DATASET_NAME}.${new_name}" > /dev/null 2>&1; then
            echo "  Renaming $old_name -> $new_name"
            bq cp -f "${PROJECT_ID}:${DATASET_NAME}.${old_name}" "${PROJECT_ID}:${DATASET_NAME}.${new_name}" 2>/dev/null || true
            bq rm -f -t "${PROJECT_ID}:${DATASET_NAME}.${old_name}" 2>/dev/null || true
        fi
    fi
}

rename_if_exists "fact_transactions" "rev0_fact_transactions"
rename_if_exists "features_weekly_regional" "rev0_features_weekly"
rename_if_exists "features_weekly_total" "rev0_features_weekly_total"
rename_if_exists "features_customers" "rev0_features_customers"
rename_if_exists "dim_products" "rev0_dim_products"
rename_if_exists "dim_customers" "rev0_dim_customers"

echo "  ✓ rev0_ structure ready"
echo ""

# =============================================================================
# STEP 2: Upload Stage 2 (Extraction) data
# =============================================================================
echo "=============================================="
echo "STEP 2: Uploading Stage 2 (Extraction) data"
echo "=============================================="

# Upload to GCS
echo "  Uploading to GCS..."
gsutil -m cp "${BASE_PATH}/features_sku/"*.csv "gs://${BUCKET_NAME}/stage2_extract/" 2>/dev/null || true
gsutil -m cp "${BASE_PATH}/features_category/"*.csv "gs://${BUCKET_NAME}/stage2_extract/" 2>/dev/null || true

# Create BigQuery tables
echo "  Creating BigQuery tables..."

# Stage 2: Extracted fact lineitem
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_sku0_fact_lineitem" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_fact_lineitem.csv"
echo "    ✓ stage2_sku0_fact_lineitem"

# Stage 2: SKU weekly features
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_sku0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_features_weekly.csv"
echo "    ✓ stage2_sku0_features_weekly"

# Stage 2: Products dimension
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_sku0_dim_products" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_dim_products.csv"
echo "    ✓ stage2_sku0_dim_products"

# Stage 2: Category features
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_cat0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_extract/cat0_features_weekly.csv"
echo "    ✓ stage2_cat0_features_weekly"

echo ""

# =============================================================================
# STEP 3: Upload Stage 2.5 (Enriched) data
# =============================================================================
echo "=============================================="
echo "STEP 3: Uploading Stage 2.5 (Enriched) data"
echo "=============================================="

# Upload to GCS
echo "  Uploading to GCS..."
gsutil -m cp "${BASE_PATH}/features_enriched/"*.csv "gs://${BUCKET_NAME}/stage2_5_enrich/" 2>/dev/null || true

# Create BigQuery tables
echo "  Creating BigQuery tables..."

# Stage 2.5: Enriched fact lineitem (THIS IS THE PRIMARY TABLE FOR MODELS)
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_fact_lineitem" \
  "gs://${BUCKET_NAME}/stage2_5_enrich/sku0_fact_lineitem_enriched.csv"
echo "    ✓ sku0_fact_lineitem (enriched - primary)"

# Stage 2.5: Enriched SKU weekly features
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_5_enrich/sku0_features_weekly_enriched.csv"
echo "    ✓ sku0_features_weekly (enriched - primary)"

# Stage 2.5: Enriched category features
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.cat0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_5_enrich/cat0_features_weekly_enriched.csv"
echo "    ✓ cat0_features_weekly (enriched - primary)"

# Also copy dim_products (unchanged from Stage 2)
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_dim_products" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_dim_products.csv"
echo "    ✓ sku0_dim_products"

echo ""

# =============================================================================
# STEP 4: Upload Stage Reports as tables (for querying)
# =============================================================================
echo "=============================================="
echo "STEP 4: Uploading Stage Reports"
echo "=============================================="

# Convert JSON reports to CSV for BigQuery
python3 << 'PYEOF'
import json
import pandas as pd
from pathlib import Path

base = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning")

# Stage 1 report
s1_path = base / "stage1_raw_eval.json"
if s1_path.exists():
    with open(s1_path) as f:
        s1 = json.load(f)
    # Create summary table
    s1_df = pd.DataFrame([{
        'stage': 1,
        'name': 'Raw Excel Files',
        'files_scanned': s1['totals']['files_scanned'],
        'total_invoices': s1['totals']['total_invoices'],
        'total_line_items': s1['totals']['total_line_items'],
        'total_quantity': s1['totals']['total_quantity'],
        'total_revenue': s1['totals']['total_revenue'],
        'generated_at': s1['generated_at']
    }])
    s1_df.to_csv(base / "stage_reports" / "stage1_summary.csv", index=False)
    print("  ✓ Stage 1 summary")

# Stage 2 report
s2_path = base / "stage2_extraction_eval.json"
if s2_path.exists():
    with open(s2_path) as f:
        s2 = json.load(f)
    s2_df = pd.DataFrame([{
        'stage': 2,
        'name': 'Extraction Pipeline',
        'total_line_items': s2['totals']['total_line_items'],
        'total_invoices': s2['totals']['total_invoices'],
        'total_quantity': s2['totals']['total_quantity'],
        'total_revenue': s2['totals']['total_revenue'],
        'unique_skus': s2['totals']['unique_skus'],
        'generated_at': s2['generated_at']
    }])
    s2_df.to_csv(base / "stage_reports" / "stage2_summary.csv", index=False)
    print("  ✓ Stage 2 summary")

# Stage 2.5 report
s25_path = base / "stage2_5_enrichment.json"
if s25_path.exists():
    with open(s25_path) as f:
        s25 = json.load(f)
    s25_df = pd.DataFrame([{
        'stage': 2.5,
        'name': 'Data Enrichment',
        'total_line_items': s25['after']['line_items'],
        'total_revenue_before': s25['before']['total_revenue'],
        'total_revenue_after': s25['after']['total_revenue'],
        'revenue_increase': s25['after']['revenue_increase'],
        'zero_revenue_before': s25['before'].get('zero_revenue_count', 0),
        'zero_revenue_after': s25['after']['zero_revenue_count'],
        'enrichment_rate': s25['enrichment']['prices']['enrichment_rate'],
        'avg_dq_score': s25['after']['dq_score_avg'],
        'generated_at': s25['generated_at']
    }])
    s25_df.to_csv(base / "stage_reports" / "stage2_5_summary.csv", index=False)
    print("  ✓ Stage 2.5 summary")

# Combined pipeline summary
pipeline_df = pd.concat([
    pd.DataFrame([{'stage': 1, 'metric': 'line_items', 'value': s1['totals']['total_line_items']}]),
    pd.DataFrame([{'stage': 1, 'metric': 'revenue', 'value': s1['totals']['total_revenue']}]),
    pd.DataFrame([{'stage': 2, 'metric': 'line_items', 'value': s2['totals']['total_line_items']}]),
    pd.DataFrame([{'stage': 2, 'metric': 'revenue', 'value': s2['totals']['total_revenue']}]),
    pd.DataFrame([{'stage': 2.5, 'metric': 'line_items', 'value': s25['after']['line_items']}]),
    pd.DataFrame([{'stage': 2.5, 'metric': 'revenue', 'value': s25['after']['total_revenue']}]),
])
pipeline_df.to_csv(base / "stage_reports" / "pipeline_summary.csv", index=False)
print("  ✓ Pipeline summary")
PYEOF

# Create stage_reports directory if needed
mkdir -p "${BASE_PATH}/stage_reports"

# Re-run python to create files
python3 << 'PYEOF'
import json
import pandas as pd
from pathlib import Path

base = Path("/sessions/affectionate-pensive-goodall/mnt/demand planning")
reports_dir = base / "stage_reports"
reports_dir.mkdir(exist_ok=True)

# Stage 1 report
s1_path = base / "stage1_raw_eval.json"
if s1_path.exists():
    with open(s1_path) as f:
        s1 = json.load(f)
    s1_df = pd.DataFrame([{
        'stage': 1,
        'name': 'Raw Excel Files',
        'files_scanned': s1['totals']['files_scanned'],
        'total_invoices': s1['totals']['total_invoices'],
        'total_line_items': s1['totals']['total_line_items'],
        'total_quantity': s1['totals']['total_quantity'],
        'total_revenue': s1['totals']['total_revenue'],
        'generated_at': s1['generated_at']
    }])
    s1_df.to_csv(reports_dir / "stage1_summary.csv", index=False)

# Stage 2 report
s2_path = base / "stage2_extraction_eval.json"
if s2_path.exists():
    with open(s2_path) as f:
        s2 = json.load(f)
    s2_df = pd.DataFrame([{
        'stage': 2,
        'name': 'Extraction Pipeline',
        'total_line_items': s2['totals']['total_line_items'],
        'total_invoices': s2['totals']['total_invoices'],
        'total_quantity': s2['totals']['total_quantity'],
        'total_revenue': s2['totals']['total_revenue'],
        'unique_skus': s2['totals']['unique_skus'],
        'generated_at': s2['generated_at']
    }])
    s2_df.to_csv(reports_dir / "stage2_summary.csv", index=False)

# Stage 2.5 report
s25_path = base / "stage2_5_enrichment.json"
if s25_path.exists():
    with open(s25_path) as f:
        s25 = json.load(f)
    s25_df = pd.DataFrame([{
        'stage': 2.5,
        'name': 'Data Enrichment',
        'total_line_items': s25['after']['line_items'],
        'total_revenue_before': s25['before']['total_revenue'],
        'total_revenue_after': s25['after']['total_revenue'],
        'revenue_increase': s25['after']['revenue_increase'],
        'zero_revenue_before': s25['before'].get('zero_revenue_count', 0),
        'zero_revenue_after': s25['after']['zero_revenue_count'],
        'enrichment_rate': s25['enrichment']['prices']['enrichment_rate'],
        'avg_dq_score': s25['after']['dq_score_avg'],
        'generated_at': s25['generated_at']
    }])
    s25_df.to_csv(reports_dir / "stage2_5_summary.csv", index=False)

print("  Reports created")
PYEOF

# Upload reports to GCS and BQ
gsutil -m cp "${BASE_PATH}/stage_reports/"*.csv "gs://${BUCKET_NAME}/stage_reports/" 2>/dev/null || true

bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.pipeline_stage1_summary" \
  "gs://${BUCKET_NAME}/stage_reports/stage1_summary.csv" 2>/dev/null || true

bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.pipeline_stage2_summary" \
  "gs://${BUCKET_NAME}/stage_reports/stage2_summary.csv" 2>/dev/null || true

bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.pipeline_stage2_5_summary" \
  "gs://${BUCKET_NAME}/stage_reports/stage2_5_summary.csv" 2>/dev/null || true

echo "    ✓ Stage reports uploaded"
echo ""

# =============================================================================
# STEP 5: Create ML Models (using enriched data)
# =============================================================================
echo "=============================================="
echo "STEP 5: Creating ML Models"
echo "=============================================="

# SKU-level XGBoost model
echo "  Creating sku0_model_xgboost..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.sku0_model_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1,
  l1_reg=0.1,
  l2_reg=0.1
) AS
SELECT
  weekly_quantity,
  avg_price,
  COALESCE(quantity_lag_1w, 0) as quantity_lag_1w,
  COALESCE(quantity_lag_2w, 0) as quantity_lag_2w,
  COALESCE(quantity_lag_4w, 0) as quantity_lag_4w,
  COALESCE(quantity_ma_4w, weekly_quantity) as quantity_ma_4w,
  COALESCE(quantity_std_4w, 0) as quantity_std_4w,
  COALESCE(quantity_diff_1w, 0) as quantity_diff_1w,
  COALESCE(price_change, 0) as price_change,
  week_of_year,
  month,
  quarter,
  transaction_count,
  unique_customers,
  primary_region,
  category,
  brand,
  avg_dq_score
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
WHERE weekly_quantity > 0
" 2>/dev/null || echo "    (model creation in progress)"

# SKU-level ARIMA+ for top SKUs
echo "  Creating sku0_model_arima..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.sku0_model_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='year_week',
  time_series_data_col='weekly_quantity',
  time_series_id_col='sku',
  auto_arima=TRUE
) AS
SELECT year_week, sku, weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
WHERE sku IN (
  SELECT sku FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
  GROUP BY sku
  ORDER BY SUM(weekly_quantity) DESC
  LIMIT 50
)
ORDER BY year_week
" 2>/dev/null || echo "    (model creation in progress)"

# Category XGBoost
echo "  Creating cat0_model_xgboost..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.cat0_model_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=50,
  learn_rate=0.1
) AS
SELECT
  weekly_quantity,
  weekly_revenue,
  active_skus,
  transaction_count,
  unique_customers,
  week_of_year,
  month,
  quarter,
  COALESCE(quantity_lag_1w, 0) as quantity_lag_1w,
  COALESCE(quantity_lag_2w, 0) as quantity_lag_2w,
  COALESCE(quantity_lag_4w, 0) as quantity_lag_4w,
  COALESCE(quantity_ma_4w, weekly_quantity) as quantity_ma_4w,
  category,
  avg_dq_score
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
WHERE weekly_quantity > 0
" 2>/dev/null || echo "    (model creation in progress)"

# Category ARIMA
echo "  Creating cat0_model_arima..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.cat0_model_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='year_week',
  time_series_data_col='weekly_quantity',
  time_series_id_col='category',
  auto_arima=TRUE
) AS
SELECT year_week, category, weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
ORDER BY year_week
" 2>/dev/null || echo "    (model creation in progress)"

echo ""

# =============================================================================
# STEP 6: Verify structure
# =============================================================================
echo "=============================================="
echo "STEP 6: Verifying BigQuery Structure"
echo "=============================================="

bq query --use_legacy_sql=false --format=pretty "
SELECT
  table_name,
  table_type,
  CASE
    WHEN table_name LIKE 'stage2_%' THEN 'Stage 2 (Extract)'
    WHEN table_name LIKE 'pipeline_%' THEN 'Pipeline Reports'
    WHEN table_name LIKE 'rev0_%' THEN 'Rev0 (Legacy)'
    WHEN table_name LIKE 'sku0_%' OR table_name LIKE 'cat0_%' THEN 'Primary (Enriched)'
    ELSE 'Other'
  END as category
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.TABLES\`
ORDER BY category, table_name
"

echo ""
echo "=============================================="
echo "UPLOAD COMPLETE!"
echo "=============================================="
echo ""
echo "BigQuery Structure:"
echo "  📁 Stage 2 (Extract)   - stage2_* tables (raw extraction)"
echo "  📁 Primary (Enriched)  - sku0_*, cat0_* tables (for models)"
echo "  📁 Pipeline Reports    - pipeline_stage*_summary tables"
echo "  📁 Rev0 (Legacy)       - rev0_* tables (original revenue model)"
echo ""
echo "ML Models:"
echo "  - sku0_model_xgboost (SKU demand prediction)"
echo "  - sku0_model_arima (SKU time series forecast)"
echo "  - cat0_model_xgboost (Category demand prediction)"
echo "  - cat0_model_arima (Category time series forecast)"
echo ""
echo "To verify data integrity, run:"
echo "  ./scripts/STAGE3_BIGQUERY_EVAL.sh"
echo ""
