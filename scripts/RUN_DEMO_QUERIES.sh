#!/bin/bash
# =============================================================================
# RUN_DEMO_QUERIES.sh - Execute demo queries to show model capabilities
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="demand_forecasting"

echo "=============================================="
echo "DEMO QUERIES - ACA Demand Forecasting Models"
echo "=============================================="
echo ""

# -----------------------------------------------------------------------------
echo "1. TOP 10 SKUs BY FORECASTED DEMAND (Next 4 Weeks)"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  sku,
  ROUND(SUM(forecast_value)) as total_predicted_units_4w
FROM ML.FORECAST(
  MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_arima\`,
  STRUCT(4 AS horizon)
)
GROUP BY sku
ORDER BY total_predicted_units_4w DESC
LIMIT 10
"
echo ""

# -----------------------------------------------------------------------------
echo "2. CATEGORY FORECAST (Next 4 Weeks)"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  category,
  forecast_timestamp,
  ROUND(forecast_value) as predicted_units,
  ROUND(prediction_interval_lower_bound) as lower_95,
  ROUND(prediction_interval_upper_bound) as upper_95
FROM ML.FORECAST(
  MODEL \`${PROJECT_ID}.${DATASET}.cat0_model_arima\`,
  STRUCT(4 AS horizon)
)
ORDER BY category, forecast_timestamp
"
echo ""

# -----------------------------------------------------------------------------
echo "3. FEATURE IMPORTANCE - What Drives SKU Demand?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  feature,
  ROUND(importance_weight, 4) as weight,
  ROUND(importance_gain, 4) as gain
FROM ML.FEATURE_IMPORTANCE(
  MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_xgboost\`
)
ORDER BY importance_weight DESC
LIMIT 15
"
echo ""

# -----------------------------------------------------------------------------
echo "4. TOP 15 SKUs BY HISTORICAL VOLUME"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  sku,
  category,
  brand,
  FORMAT('%\\'d', CAST(SUM(weekly_quantity) AS INT64)) as total_units,
  FORMAT('R %\\'d', CAST(SUM(weekly_revenue) AS INT64)) as total_revenue,
  COUNT(*) as weeks_active
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
GROUP BY sku, category, brand
ORDER BY SUM(weekly_quantity) DESC
LIMIT 15
"
echo ""

# -----------------------------------------------------------------------------
echo "5. CATEGORY SUMMARY"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  category,
  FORMAT('%\\'d', CAST(SUM(weekly_quantity) AS INT64)) as total_units,
  FORMAT('R %\\'d', CAST(SUM(weekly_revenue) AS INT64)) as total_revenue,
  COUNT(DISTINCT sku) as sku_count
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
GROUP BY category
ORDER BY SUM(weekly_quantity) DESC
"
echo ""

# -----------------------------------------------------------------------------
echo "6. WEEKLY TREND (Last 8 Weeks)"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  year_week,
  FORMAT('%\\'d', CAST(SUM(weekly_quantity) AS INT64)) as total_units,
  FORMAT('R %\\'d', CAST(SUM(weekly_revenue) AS INT64)) as total_revenue,
  COUNT(DISTINCT sku) as active_skus
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
GROUP BY year_week
ORDER BY year_week DESC
LIMIT 8
"
echo ""

# -----------------------------------------------------------------------------
echo "7. DATA QUALITY SUMMARY"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  ROUND(AVG(avg_dq_score), 1) as avg_dq_score,
  ROUND(AVG(pct_price_inferred) * 100, 1) as pct_prices_inferred,
  FORMAT('%\\'d', COUNT(*)) as total_sku_weeks
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
"
echo ""

# -----------------------------------------------------------------------------
echo "8. SAMPLE SKU DETAIL FORECAST (SKU 10024)"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  sku,
  forecast_timestamp,
  ROUND(forecast_value) as predicted_units,
  ROUND(standard_error) as uncertainty,
  ROUND(prediction_interval_lower_bound) as lower_95,
  ROUND(prediction_interval_upper_bound) as upper_95
FROM ML.FORECAST(
  MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_arima\`,
  STRUCT(8 AS horizon)
)
WHERE sku = '10024'
ORDER BY forecast_timestamp
"
echo ""

echo "=============================================="
echo "DEMO QUERIES COMPLETE"
echo "=============================================="
