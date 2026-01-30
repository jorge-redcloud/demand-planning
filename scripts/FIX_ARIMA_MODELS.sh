#!/bin/bash
# =============================================================================
# FIX_ARIMA_MODELS.sh - Fix ARIMA models with proper DATE conversion
# =============================================================================
# Run this after the main pipeline if ARIMA models failed due to year_week type
# =============================================================================

set -e

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"

echo "=============================================="
echo "FIXING ARIMA MODELS - Converting year_week to DATE"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_NAME"
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
echo "=============================================="
echo "VERIFYING MODELS"
echo "=============================================="

bq query --use_legacy_sql=false --format=pretty "
SELECT
  model_name,
  model_type,
  TIMESTAMP_MILLIS(creation_time) as created
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.MODELS\`
WHERE model_name LIKE 'sku0%' OR model_name LIKE 'cat0%'
ORDER BY model_name
"

echo ""
echo "=============================================="
echo "ARIMA MODELS FIXED!"
echo "=============================================="
echo ""
echo "Test SKU forecast with:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM ML.FORECAST(MODEL \`${PROJECT_ID}.${DATASET_NAME}.sku0_model_arima\`, STRUCT(4 AS horizon)) LIMIT 10'"
echo ""
echo "Test Category forecast with:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM ML.FORECAST(MODEL \`${PROJECT_ID}.${DATASET_NAME}.cat0_model_arima\`, STRUCT(4 AS horizon)) LIMIT 10'"
echo ""
