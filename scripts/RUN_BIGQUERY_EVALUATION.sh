#!/bin/bash
# ============================================================================
# RUN BIGQUERY MODEL EVALUATION
# ============================================================================
# This script executes the model evaluation in BigQuery ML
#
# Prerequisites:
#   1. gcloud CLI installed and authenticated
#   2. V2 data deployed to BigQuery (run DEPLOY_V2_TO_BIGQUERY.sh first)
#   3. BigQuery ML API enabled
#
# Usage:
#   ./RUN_BIGQUERY_EVALUATION.sh [PROJECT_ID] [DATASET_NAME]
#
# Example:
#   ./RUN_BIGQUERY_EVALUATION.sh my-gcp-project redai_aca_features
# ============================================================================

set -e

# Configuration
PROJECT_ID="${1:-your-project-id}"
DATASET_NAME="${2:-redai_aca_features}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SQL_FILE="${SCRIPT_DIR}/BIGQUERY_MODEL_EVALUATION.sql"

echo "=============================================="
echo "BIGQUERY ML MODEL EVALUATION"
echo "=============================================="
echo "Project:  ${PROJECT_ID}"
echo "Dataset:  ${DATASET_NAME}"
echo "=============================================="

# Check if gcloud is installed
if ! command -v gcloud &> /dev/null; then
    echo "❌ Error: gcloud CLI not installed"
    echo "   Install from: https://cloud.google.com/sdk/docs/install"
    exit 1
fi

# Check if bq is installed
if ! command -v bq &> /dev/null; then
    echo "❌ Error: bq CLI not installed"
    echo "   It should be included with gcloud SDK"
    exit 1
fi

# Set project
echo ""
echo "🔧 Setting project to ${PROJECT_ID}..."
gcloud config set project ${PROJECT_ID}

# Replace placeholders in SQL
echo "📝 Preparing SQL script..."
TEMP_SQL="/tmp/bigquery_eval_$(date +%s).sql"
sed "s/\${DATASET_NAME}/${DATASET_NAME}/g" "${SQL_FILE}" > "${TEMP_SQL}"

# ============================================================================
# STEP 1: Create Views
# ============================================================================
echo ""
echo "📊 STEP 1: Creating data views..."

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
-- Training view
CREATE OR REPLACE VIEW \`${PROJECT_ID}.${DATASET_NAME}.train_sku_weekly\` AS
SELECT
  sku,
  year_week,
  CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
  weekly_quantity,
  weekly_revenue,
  avg_unit_price,
  order_count,
  customer_count,
  COALESCE(lag1_quantity, 0) AS lag1_quantity,
  COALESCE(lag2_quantity, 0) AS lag2_quantity,
  COALESCE(lag4_quantity, 0) AS lag4_quantity,
  COALESCE(rolling_avg_4w, weekly_quantity) AS rolling_avg_4w
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_weekly\`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= 26;
EOF

echo "✅ Training view created"

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
-- Test view
CREATE OR REPLACE VIEW \`${PROJECT_ID}.${DATASET_NAME}.test_sku_weekly\` AS
SELECT
  sku,
  year_week,
  CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
  weekly_quantity,
  weekly_revenue,
  avg_unit_price,
  order_count,
  customer_count,
  COALESCE(lag1_quantity, 0) AS lag1_quantity,
  COALESCE(lag2_quantity, 0) AS lag2_quantity,
  COALESCE(lag4_quantity, 0) AS lag4_quantity,
  COALESCE(rolling_avg_4w, weekly_quantity) AS rolling_avg_4w
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_weekly\`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= 27;
EOF

echo "✅ Test view created"

# ============================================================================
# STEP 2: Train XGBoost Model
# ============================================================================
echo ""
echo "🤖 STEP 2: Training XGBoost SKU model..."

bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_sku_xgboost\`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['weekly_quantity'],
  num_parallel_tree = 100,
  max_tree_depth = 5,
  learn_rate = 0.1,
  l1_reg = 0.1,
  l2_reg = 0.1,
  early_stop = TRUE,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  weekly_quantity,
  lag1_quantity,
  lag2_quantity,
  lag4_quantity,
  rolling_avg_4w,
  avg_unit_price,
  week_num
FROM \`${PROJECT_ID}.${DATASET_NAME}.train_sku_weekly\`
WHERE lag1_quantity IS NOT NULL;
EOF

echo "✅ XGBoost model trained"

# ============================================================================
# STEP 3: Train ARIMA Models
# ============================================================================
echo ""
echo "📈 STEP 3: Training ARIMA models..."

# SKU ARIMA
echo "   Training SKU ARIMA..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_sku_arima\`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'year_week',
  time_series_data_col = 'weekly_quantity',
  time_series_id_col = 'sku',
  auto_arima = TRUE,
  data_frequency = 'WEEKLY',
  holiday_region = 'ZA'
) AS
SELECT sku, year_week, weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.train_sku_weekly\`
WHERE sku IN (
  SELECT sku
  FROM \`${PROJECT_ID}.${DATASET_NAME}.train_sku_weekly\`
  GROUP BY sku
  HAVING COUNT(*) >= 20
);
EOF
echo "   ✅ SKU ARIMA trained"

# Category ARIMA
echo "   Training Category ARIMA..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_category_arima\`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'year_week',
  time_series_data_col = 'weekly_quantity',
  time_series_id_col = 'category',
  auto_arima = TRUE,
  data_frequency = 'WEEKLY',
  holiday_region = 'ZA'
) AS
SELECT category, year_week, weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_category\`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= 26;
EOF
echo "   ✅ Category ARIMA trained"

# Customer ARIMA
echo "   Training Customer ARIMA..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_customer_arima\`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'year_week',
  time_series_data_col = 'weekly_quantity',
  time_series_id_col = 'customer_id',
  auto_arima = TRUE,
  data_frequency = 'WEEKLY'
) AS
SELECT customer_id, year_week, weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_customer\`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= 26
  AND customer_id IN (
    SELECT customer_id
    FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_customer\`
    WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= 26
    GROUP BY customer_id
    HAVING COUNT(*) >= 15
  );
EOF
echo "   ✅ Customer ARIMA trained"

# ============================================================================
# STEP 4: Evaluate Models
# ============================================================================
echo ""
echo "📊 STEP 4: Evaluating models..."

# XGBoost evaluation
echo "   Evaluating XGBoost SKU model..."
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_NAME}.eval_sku_xgboost\` AS
WITH predictions AS (
  SELECT *
  FROM ML.PREDICT(
    MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_sku_xgboost\`,
    (SELECT * FROM \`${PROJECT_ID}.${DATASET_NAME}.test_sku_weekly\` WHERE lag1_quantity IS NOT NULL)
  )
)
SELECT
  sku,
  year_week,
  weekly_quantity AS actual,
  predicted_weekly_quantity AS predicted,
  ABS(weekly_quantity - predicted_weekly_quantity) AS abs_error,
  SAFE_DIVIDE(ABS(weekly_quantity - predicted_weekly_quantity), weekly_quantity) * 100 AS pct_error
FROM predictions;
EOF

# Get XGBoost metrics
echo ""
echo "=============================================="
echo "MODEL EVALUATION RESULTS"
echo "=============================================="
echo ""
echo "📈 XGBoost SKU Model:"
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
SELECT
  'XGBoost_SKU' AS model,
  COUNT(*) AS n_predictions,
  COUNT(DISTINCT sku) AS n_skus,
  ROUND(AVG(abs_error), 0) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 0) AS rmse,
  ROUND(APPROX_QUANTILES(pct_error, 100)[OFFSET(50)], 1) AS mape_median
FROM \`${PROJECT_ID}.${DATASET_NAME}.eval_sku_xgboost\`
WHERE actual > 0;
EOF

# Baseline: Moving Average
echo ""
echo "📊 Moving Average (4-week) Baseline:"
bq query --use_legacy_sql=false --project_id=${PROJECT_ID} <<EOF
WITH last_4_weeks AS (
  SELECT sku, AVG(weekly_quantity) AS ma4
  FROM \`${PROJECT_ID}.${DATASET_NAME}.train_sku_weekly\`
  WHERE week_num BETWEEN 23 AND 26
  GROUP BY sku
),
test AS (
  SELECT sku, year_week, weekly_quantity
  FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_weekly\`
  WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= 27
),
eval AS (
  SELECT t.weekly_quantity AS actual, l.ma4 AS predicted
  FROM test t JOIN last_4_weeks l ON t.sku = l.sku
)
SELECT
  'MA_4Week_SKU' AS model,
  COUNT(*) AS n_predictions,
  ROUND(AVG(ABS(actual - predicted)), 0) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 0) AS rmse,
  ROUND(APPROX_QUANTILES(SAFE_DIVIDE(ABS(actual - predicted), actual) * 100, 100)[OFFSET(50)], 1) AS mape_median
FROM eval
WHERE actual > 0;
EOF

echo ""
echo "=============================================="
echo "✅ EVALUATION COMPLETE"
echo "=============================================="
echo ""
echo "Results tables created in BigQuery:"
echo "  - ${DATASET_NAME}.eval_sku_xgboost"
echo "  - ${DATASET_NAME}.model_evaluation_summary"
echo ""
echo "Models available:"
echo "  - ${DATASET_NAME}.model_sku_xgboost"
echo "  - ${DATASET_NAME}.model_sku_arima"
echo "  - ${DATASET_NAME}.model_category_arima"
echo "  - ${DATASET_NAME}.model_customer_arima"
echo ""
echo "To generate forecasts, run:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM ML.FORECAST(MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_sku_arima\`, STRUCT(4 AS horizon))'"

# Cleanup
rm -f "${TEMP_SQL}"
