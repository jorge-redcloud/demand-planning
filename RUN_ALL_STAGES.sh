#!/bin/bash
# =============================================================================
# RUN_ALL_STAGES.sh - Complete pipeline: Upload all stages to BigQuery
# =============================================================================
# Run this on your local machine where gcloud/bq are installed.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated
#   - bq CLI available
#   - Access to project mimetic-maxim-443710-s2
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
BUCKET_NAME="demand_planning_aca"
DATASET_NAME="demand_forecasting"

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "=============================================="
echo "COMPLETE PIPELINE: ALL STAGES TO BIGQUERY"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Bucket: gs://$BUCKET_NAME"
echo "Dataset: $DATASET_NAME"
echo "Data Path: $SCRIPT_DIR"
echo ""

# Verify gcloud is available
if ! command -v gcloud &> /dev/null; then
    echo "ERROR: gcloud CLI not found. Please install Google Cloud SDK."
    exit 1
fi

if ! command -v bq &> /dev/null; then
    echo "ERROR: bq CLI not found. Please install Google Cloud SDK."
    exit 1
fi

# Set project
gcloud config set project $PROJECT_ID

# =============================================================================
# STEP 1: Upload files to GCS
# =============================================================================
echo "=============================================="
echo "STEP 1: Uploading files to GCS"
echo "=============================================="

# Create bucket if doesn't exist
gsutil ls "gs://${BUCKET_NAME}" 2>/dev/null || gsutil mb -p $PROJECT_ID "gs://${BUCKET_NAME}"

# Upload Stage 2 (Extraction) files
echo "  Uploading Stage 2 files..."
gsutil -m cp "$SCRIPT_DIR/features_sku/"*.csv "gs://${BUCKET_NAME}/stage2_extract/"
gsutil -m cp "$SCRIPT_DIR/features_category/"*.csv "gs://${BUCKET_NAME}/stage2_extract/"

# Upload Stage 2.5 (Enriched) files
echo "  Uploading Stage 2.5 files..."
gsutil -m cp "$SCRIPT_DIR/features_enriched/"*.csv "gs://${BUCKET_NAME}/stage2_5_enrich/"

echo "  ✓ Files uploaded to GCS"
echo ""

# =============================================================================
# STEP 2: Create BigQuery tables - Stage 2 (Extraction)
# =============================================================================
echo "=============================================="
echo "STEP 2: Creating Stage 2 (Extraction) tables"
echo "=============================================="

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_sku0_fact_lineitem" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_fact_lineitem.csv"
echo "  ✓ stage2_sku0_fact_lineitem"

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_sku0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_features_weekly.csv"
echo "  ✓ stage2_sku0_features_weekly"

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_sku0_dim_products" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_dim_products.csv"
echo "  ✓ stage2_sku0_dim_products"

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.stage2_cat0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_extract/cat0_features_weekly.csv"
echo "  ✓ stage2_cat0_features_weekly"

echo ""

# =============================================================================
# STEP 3: Create BigQuery tables - Stage 2.5 (Enriched) - PRIMARY
# =============================================================================
echo "=============================================="
echo "STEP 3: Creating Stage 2.5 (Enriched) tables"
echo "=============================================="

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_fact_lineitem" \
  "gs://${BUCKET_NAME}/stage2_5_enrich/sku0_fact_lineitem_enriched.csv"
echo "  ✓ sku0_fact_lineitem (PRIMARY)"

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_5_enrich/sku0_features_weekly_enriched.csv"
echo "  ✓ sku0_features_weekly (PRIMARY)"

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.cat0_features_weekly" \
  "gs://${BUCKET_NAME}/stage2_5_enrich/cat0_features_weekly_enriched.csv"
echo "  ✓ cat0_features_weekly (PRIMARY)"

bq load --autodetect --replace --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_dim_products" \
  "gs://${BUCKET_NAME}/stage2_extract/sku0_dim_products.csv"
echo "  ✓ sku0_dim_products"

echo ""

# =============================================================================
# STEP 4: Create ML Models
# =============================================================================
echo "=============================================="
echo "STEP 4: Creating ML Models"
echo "=============================================="

echo "  Creating sku0_model_xgboost..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.sku0_model_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1
) AS
SELECT
  weekly_quantity,
  avg_price,
  COALESCE(quantity_lag_1w, 0) as quantity_lag_1w,
  COALESCE(quantity_lag_2w, 0) as quantity_lag_2w,
  COALESCE(quantity_lag_4w, 0) as quantity_lag_4w,
  COALESCE(quantity_ma_4w, weekly_quantity) as quantity_ma_4w,
  week_of_year,
  month,
  quarter,
  transaction_count,
  primary_region,
  category,
  brand
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
WHERE weekly_quantity > 0
"
echo "  ✓ sku0_model_xgboost"

echo "  Creating sku0_model_arima (top 50 SKUs)..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.sku0_model_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='sku',
  auto_arima=TRUE
) AS
SELECT
  PARSE_DATE('%G-W%V', year_week) as week_date,
  sku,
  weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
WHERE sku IN (
  SELECT sku FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
  GROUP BY sku ORDER BY SUM(weekly_quantity) DESC LIMIT 50
)
ORDER BY week_date
"
echo "  ✓ sku0_model_arima"

echo "  Creating cat0_model_xgboost..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.cat0_model_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=50
) AS
SELECT
  weekly_quantity,
  weekly_revenue,
  active_skus,
  transaction_count,
  week_of_year,
  month,
  quarter,
  COALESCE(quantity_lag_1w, 0) as quantity_lag_1w,
  COALESCE(quantity_ma_4w, weekly_quantity) as quantity_ma_4w,
  category
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
WHERE weekly_quantity > 0
"
echo "  ✓ cat0_model_xgboost"

echo "  Creating cat0_model_arima..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.cat0_model_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='category',
  auto_arima=TRUE
) AS
SELECT
  PARSE_DATE('%G-W%V', year_week) as week_date,
  category,
  weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
ORDER BY week_date
"
echo "  ✓ cat0_model_arima"

echo ""

# =============================================================================
# STEP 5: Verify
# =============================================================================
echo "=============================================="
echo "STEP 5: Verifying Upload"
echo "=============================================="

echo ""
echo "Tables created:"
bq query --use_legacy_sql=false --format=pretty "
SELECT table_name, table_type
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.TABLES\`
WHERE table_name LIKE 'sku0%' OR table_name LIKE 'cat0%' OR table_name LIKE 'stage2%'
ORDER BY table_name
"

echo ""
echo "Row counts:"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  'sku0_fact_lineitem' as table_name, COUNT(*) as rows
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_fact_lineitem\`
UNION ALL
SELECT
  'sku0_features_weekly', COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
UNION ALL
SELECT
  'cat0_features_weekly', COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
"

echo ""
echo "=============================================="
echo "PIPELINE COMPLETE!"
echo "=============================================="
echo ""
echo "Expected row counts:"
echo "  sku0_fact_lineitem:    119,337"
echo "  sku0_features_weekly:    4,869"
echo "  cat0_features_weekly:      285"
echo ""
echo "To test forecasts:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM ML.FORECAST(MODEL demand_forecasting.sku0_model_arima, STRUCT(4 AS horizon))'"
echo ""
