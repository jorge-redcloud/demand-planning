#!/bin/bash
# =============================================================================
# EXPORT_FOR_CHARTS.sh - Export data for visualization
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="demand_forecasting"

echo "Exporting data for charts..."

# 1. Weekly totals (historical)
echo "1. Weekly totals..."
bq query --use_legacy_sql=false --format=csv "
SELECT
  year_week,
  CAST(SUM(weekly_quantity) AS INT64) as total_units,
  CAST(SUM(weekly_revenue) AS INT64) as total_revenue,
  COUNT(DISTINCT sku) as active_skus
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
GROUP BY year_week
ORDER BY year_week
" > /tmp/weekly_totals.csv

# 2. Top SKUs historical
echo "2. Top SKUs history..."
bq query --use_legacy_sql=false --format=csv "
SELECT
  sku,
  year_week,
  CAST(weekly_quantity AS INT64) as units,
  category,
  brand
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
WHERE sku IN (
  SELECT sku FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
  GROUP BY sku ORDER BY SUM(weekly_quantity) DESC LIMIT 10
)
ORDER BY sku, year_week
" > /tmp/top_skus_history.csv

# 3. SKU forecasts
echo "3. SKU forecasts..."
bq query --use_legacy_sql=false --format=csv "
SELECT
  sku,
  CAST(forecast_timestamp AS STRING) as forecast_date,
  CAST(forecast_value AS INT64) as predicted_units,
  CAST(prediction_interval_lower_bound AS INT64) as lower_bound,
  CAST(prediction_interval_upper_bound AS INT64) as upper_bound
FROM ML.FORECAST(
  MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_arima\`,
  STRUCT(4 AS horizon)
)
ORDER BY sku, forecast_timestamp
" > /tmp/sku_forecasts.csv

# 4. Category historical
echo "4. Category history..."
bq query --use_legacy_sql=false --format=csv "
SELECT
  category,
  year_week,
  CAST(weekly_quantity AS INT64) as units,
  CAST(weekly_revenue AS INT64) as revenue
FROM \`${PROJECT_ID}.${DATASET}.cat0_features_weekly\`
ORDER BY category, year_week
" > /tmp/category_history.csv

# 5. Category forecasts
echo "5. Category forecasts..."
bq query --use_legacy_sql=false --format=csv "
SELECT
  category,
  CAST(forecast_timestamp AS STRING) as forecast_date,
  CAST(forecast_value AS INT64) as predicted_units,
  CAST(prediction_interval_lower_bound AS INT64) as lower_bound,
  CAST(prediction_interval_upper_bound AS INT64) as upper_bound
FROM ML.FORECAST(
  MODEL \`${PROJECT_ID}.${DATASET}.cat0_model_arima\`,
  STRUCT(4 AS horizon)
)
ORDER BY category, forecast_timestamp
" > /tmp/category_forecasts.csv

# 6. Feature importance
echo "6. Feature importance..."
bq query --use_legacy_sql=false --format=csv "
SELECT
  feature,
  ROUND(importance_weight, 2) as weight
FROM ML.FEATURE_IMPORTANCE(
  MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_xgboost\`
)
ORDER BY importance_weight DESC
" > /tmp/feature_importance.csv

# 7. Historical vs Forecast comparison for top SKUs
echo "7. Forecast comparison..."
bq query --use_legacy_sql=false --format=csv "
WITH historical AS (
  SELECT
    sku,
    CAST(AVG(weekly_quantity) AS INT64) as hist_avg,
    CAST(MIN(weekly_quantity) AS INT64) as hist_min,
    CAST(MAX(weekly_quantity) AS INT64) as hist_max,
    COUNT(*) as weeks
  FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
  GROUP BY sku
),
forecasts AS (
  SELECT
    sku,
    CAST(AVG(forecast_value) AS INT64) as forecast_avg
  FROM ML.FORECAST(
    MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_arima\`,
    STRUCT(4 AS horizon)
  )
  GROUP BY sku
)
SELECT
  h.sku,
  h.weeks,
  h.hist_avg,
  h.hist_min,
  h.hist_max,
  f.forecast_avg
FROM historical h
JOIN forecasts f ON h.sku = f.sku
ORDER BY h.hist_avg DESC
LIMIT 20
" > /tmp/forecast_comparison.csv

echo ""
echo "Data exported to /tmp/*.csv"
echo ""

# Show the data
echo "=== WEEKLY TOTALS ==="
cat /tmp/weekly_totals.csv
echo ""
echo "=== TOP SKUs HISTORY ==="
head -30 /tmp/top_skus_history.csv
echo ""
echo "=== FORECAST COMPARISON ==="
cat /tmp/forecast_comparison.csv
echo ""
echo "=== FEATURE IMPORTANCE ==="
cat /tmp/feature_importance.csv
