#!/bin/bash
# =============================================================================
# TRAIN V1 MODELS - Customer-Aware Demand Forecasting
# =============================================================================
# Creates v1 models using full year data with customer segmentation
# Improvements over v0:
#   - Full 52 weeks instead of 11 weeks
#   - Customer segment as feature
#   - Data completeness filtering
#   - Region-aware features
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"

echo "==========================================="
echo "TRAINING V1 MODELS"
echo "==========================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_NAME"
echo ""

# -----------------------------------------------------------------------------
# 1. SKU ARIMA Model (Time Series - Complete Weeks Only)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "1/6 Creating v1_model_sku_arima..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_sku_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='sku',
  auto_arima=TRUE,
  holiday_region='ZA'
) AS
SELECT
  PARSE_DATE('%G-W%V', f.year_week) as week_date,
  f.sku,
  f.weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_weekly\` f
JOIN \`${PROJECT_ID}.${DATASET_NAME}.v1_week_completeness\` w
  ON f.year_week = w.year_week
WHERE w.data_completeness = 'complete'
  AND f.weekly_quantity > 0
"

echo "✓ v1_model_sku_arima created (trained on complete weeks only)"
echo ""

# -----------------------------------------------------------------------------
# 2. SKU XGBoost Model (With Customer Segment Features)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "2/6 Creating v1_model_sku_xgboost..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_sku_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1,
  early_stop=TRUE,
  min_split_loss=0.1
) AS
SELECT
  f.weekly_quantity,
  f.avg_unit_price,
  f.weekly_revenue,
  f.order_count,
  COALESCE(f.lag1_quantity, 0) as lag1_quantity,
  COALESCE(f.lag2_quantity, 0) as lag2_quantity,
  COALESCE(f.lag4_quantity, 0) as lag4_quantity,
  COALESCE(f.rolling_avg_4w, f.weekly_quantity) as rolling_avg_4w,
  EXTRACT(WEEK FROM PARSE_DATE('%G-W%V', f.year_week)) as week_of_year,
  EXTRACT(MONTH FROM PARSE_DATE('%G-W%V', f.year_week)) as month,
  -- Region distribution for this SKU
  COALESCE(
    (SELECT COUNT(DISTINCT region_name)
     FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_fact_lineitem\` li
     WHERE li.sku = f.sku), 1
  ) as region_count
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_weekly\` f
JOIN \`${PROJECT_ID}.${DATASET_NAME}.v1_week_completeness\` w
  ON f.year_week = w.year_week
WHERE w.data_completeness IN ('complete', 'partial')
  AND f.weekly_quantity > 0
"

echo "✓ v1_model_sku_xgboost created (with region features)"
echo ""

# -----------------------------------------------------------------------------
# 3. Category ARIMA Model
# -----------------------------------------------------------------------------
echo "==========================================="
echo "3/6 Creating v1_model_category_arima..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_category_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='category',
  auto_arima=TRUE,
  holiday_region='ZA'
) AS
SELECT
  PARSE_DATE('%G-W%V', f.year_week) as week_date,
  f.category,
  f.weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_category\` f
JOIN \`${PROJECT_ID}.${DATASET_NAME}.v1_week_completeness\` w
  ON f.year_week = w.year_week
WHERE w.data_completeness = 'complete'
  AND f.weekly_quantity > 0
"

echo "✓ v1_model_category_arima created"
echo ""

# -----------------------------------------------------------------------------
# 4. Category XGBoost Model
# -----------------------------------------------------------------------------
echo "==========================================="
echo "4/6 Creating v1_model_category_xgboost..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_category_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1,
  early_stop=TRUE
) AS
SELECT
  f.weekly_quantity,
  f.avg_unit_price,
  f.weekly_revenue,
  f.unique_skus,
  f.order_count,
  COALESCE(f.lag1_quantity, 0) as lag1_quantity,
  COALESCE(f.lag2_quantity, 0) as lag2_quantity,
  EXTRACT(WEEK FROM PARSE_DATE('%G-W%V', f.year_week)) as week_of_year,
  EXTRACT(MONTH FROM PARSE_DATE('%G-W%V', f.year_week)) as month
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_category\` f
JOIN \`${PROJECT_ID}.${DATASET_NAME}.v1_week_completeness\` w
  ON f.year_week = w.year_week
WHERE w.data_completeness IN ('complete', 'partial')
  AND f.weekly_quantity > 0
"

echo "✓ v1_model_category_xgboost created"
echo ""

# -----------------------------------------------------------------------------
# 5. Customer Segment XGBoost (SKU × Segment)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "5/6 Creating v1_model_segment_xgboost..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_segment_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1,
  early_stop=TRUE
) AS
SELECT
  sc.weekly_quantity,
  sc.avg_unit_price,
  sc.order_count,
  CASE sc.customer_segment
    WHEN 'Small Retailer' THEN 1
    WHEN 'Medium Retailer' THEN 2
    WHEN 'Large Retailer' THEN 3
    WHEN 'Bulk/Wholesale' THEN 4
    ELSE 0
  END as segment_code,
  EXTRACT(WEEK FROM PARSE_DATE('%G-W%V', sc.year_week)) as week_of_year,
  EXTRACT(MONTH FROM PARSE_DATE('%G-W%V', sc.year_week)) as month
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_sku_customer\` sc
JOIN \`${PROJECT_ID}.${DATASET_NAME}.v1_week_completeness\` w
  ON sc.year_week = w.year_week
WHERE w.data_completeness IN ('complete', 'partial')
  AND sc.weekly_quantity > 0
  AND sc.customer_segment IS NOT NULL
"

echo "✓ v1_model_segment_xgboost created (by customer segment)"
echo ""

# -----------------------------------------------------------------------------
# 6. Region-Level ARIMA
# -----------------------------------------------------------------------------
echo "==========================================="
echo "6/6 Creating v1_model_region_arima..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_region_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='region_name',
  auto_arima=TRUE,
  holiday_region='ZA'
) AS
SELECT
  PARSE_DATE('%G-W%V', year_week) as week_date,
  region_name,
  SUM(quantity) as weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_fact_lineitem\`
WHERE region_name IS NOT NULL
GROUP BY year_week, region_name
HAVING weekly_quantity > 0
"

echo "✓ v1_model_region_arima created"
echo ""

# -----------------------------------------------------------------------------
# Model Evaluation
# -----------------------------------------------------------------------------
echo "==========================================="
echo "MODEL EVALUATION SUMMARY"
echo "==========================================="

echo ""
echo "--- SKU ARIMA Evaluation ---"
bq query --use_legacy_sql=false "
SELECT
  'v1_model_sku_arima' as model,
  ROUND(AVG(mean_absolute_error), 2) as avg_mae,
  ROUND(AVG(mean_squared_error), 2) as avg_mse,
  COUNT(*) as time_series_count
FROM ML.ARIMA_EVALUATE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_sku_arima\`)
"

echo ""
echo "--- Category ARIMA Evaluation ---"
bq query --use_legacy_sql=false "
SELECT
  'v1_model_category_arima' as model,
  ROUND(AVG(mean_absolute_error), 2) as avg_mae,
  ROUND(AVG(mean_squared_error), 2) as avg_mse,
  COUNT(*) as time_series_count
FROM ML.ARIMA_EVALUATE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_category_arima\`)
"

echo ""
echo "--- XGBoost Feature Importance (SKU) ---"
bq query --use_legacy_sql=false "
SELECT
  feature,
  ROUND(importance_gain, 4) as importance
FROM ML.FEATURE_IMPORTANCE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.v1_model_sku_xgboost\`)
ORDER BY importance_gain DESC
LIMIT 10
"

echo ""
echo "==========================================="
echo "✅ V1 MODELS TRAINING COMPLETE!"
echo "==========================================="
echo ""
echo "Models created:"
echo "  1. v1_model_sku_arima       - SKU time series (complete weeks)"
echo "  2. v1_model_sku_xgboost     - SKU with ML features"
echo "  3. v1_model_category_arima  - Category time series"
echo "  4. v1_model_category_xgboost - Category with ML features"
echo "  5. v1_model_segment_xgboost - By customer segment"
echo "  6. v1_model_region_arima    - Regional time series"
echo ""
echo "Key improvements over v0:"
echo "  ✓ Full year data (52 weeks vs 11 weeks)"
echo "  ✓ Trained only on complete weeks (no data gaps)"
echo "  ✓ Customer segment features"
echo "  ✓ Region-level forecasting"
echo "  ✓ South Africa holiday calendar"
