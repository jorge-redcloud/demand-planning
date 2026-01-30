#!/bin/bash
# =============================================================================
# VALIDATE_FORECASTS.sh - Compare predictions vs historical actuals
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="demand_forecasting"

echo "=============================================="
echo "FORECAST VALIDATION - Predictions vs Actuals"
echo "=============================================="
echo ""

# -----------------------------------------------------------------------------
echo "1. TOP 10 SKUs: HISTORICAL WEEKLY PATTERN"
echo "   (What did these SKUs actually do week by week?)"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  sku,
  year_week,
  CAST(weekly_quantity AS INT64) as actual_units,
  CAST(weekly_revenue AS INT64) as revenue,
  ROUND(avg_price, 2) as avg_price
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
WHERE sku IN ('11119', '10675', '12130', '12064', '11184')
ORDER BY sku, year_week
"
echo ""

# -----------------------------------------------------------------------------
echo "2. TOP SKUs: SUMMARY STATS (Avg, Min, Max per week)"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  sku,
  COUNT(*) as weeks_of_data,
  CAST(AVG(weekly_quantity) AS INT64) as avg_weekly_units,
  CAST(MIN(weekly_quantity) AS INT64) as min_weekly_units,
  CAST(MAX(weekly_quantity) AS INT64) as max_weekly_units,
  CAST(STDDEV(weekly_quantity) AS INT64) as std_dev
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
WHERE sku IN ('11119', '10675', '12130', '12064', '11184', '11271', '11151', '11311', '10679', '10659')
GROUP BY sku
ORDER BY avg_weekly_units DESC
"
echo ""

# -----------------------------------------------------------------------------
echo "3. FORECAST vs HISTORICAL AVG (Top 10 SKUs)"
echo "   (Is the forecast realistic compared to history?)"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
WITH historical AS (
  SELECT
    sku,
    CAST(AVG(weekly_quantity) AS INT64) as hist_avg_weekly,
    CAST(MIN(weekly_quantity) AS INT64) as hist_min,
    CAST(MAX(weekly_quantity) AS INT64) as hist_max,
    COUNT(*) as weeks_of_data
  FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
  GROUP BY sku
),
forecasts AS (
  SELECT
    sku,
    CAST(AVG(forecast_value) AS INT64) as forecast_avg_weekly
  FROM ML.FORECAST(
    MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_arima\`,
    STRUCT(4 AS horizon)
  )
  GROUP BY sku
)
SELECT
  h.sku,
  h.weeks_of_data,
  h.hist_avg_weekly,
  h.hist_min,
  h.hist_max,
  f.forecast_avg_weekly,
  ROUND((f.forecast_avg_weekly - h.hist_avg_weekly) / h.hist_avg_weekly * 100, 1) as pct_diff_from_avg
FROM historical h
JOIN forecasts f ON h.sku = f.sku
ORDER BY h.hist_avg_weekly DESC
LIMIT 15
"
echo ""

# -----------------------------------------------------------------------------
echo "4. CATEGORY: HISTORICAL vs FORECAST"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
WITH historical AS (
  SELECT
    category,
    CAST(AVG(weekly_quantity) AS INT64) as hist_avg_weekly,
    CAST(MIN(weekly_quantity) AS INT64) as hist_min,
    CAST(MAX(weekly_quantity) AS INT64) as hist_max,
    COUNT(*) as weeks_of_data
  FROM \`${PROJECT_ID}.${DATASET}.cat0_features_weekly\`
  GROUP BY category
),
forecasts AS (
  SELECT
    category,
    CAST(AVG(forecast_value) AS INT64) as forecast_avg_weekly
  FROM ML.FORECAST(
    MODEL \`${PROJECT_ID}.${DATASET}.cat0_model_arima\`,
    STRUCT(4 AS horizon)
  )
  GROUP BY category
)
SELECT
  h.category,
  h.weeks_of_data,
  h.hist_avg_weekly,
  h.hist_min,
  h.hist_max,
  f.forecast_avg_weekly,
  ROUND((f.forecast_avg_weekly - h.hist_avg_weekly) / NULLIF(h.hist_avg_weekly, 0) * 100, 1) as pct_diff
FROM historical h
JOIN forecasts f ON h.category = f.category
WHERE h.hist_avg_weekly > 1000
ORDER BY h.hist_avg_weekly DESC
LIMIT 20
"
echo ""

# -----------------------------------------------------------------------------
echo "5. WEEKLY TREND: Did demand go up or down over time?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  year_week,
  CAST(SUM(weekly_quantity) AS INT64) as total_units,
  COUNT(DISTINCT sku) as active_skus,
  CAST(SUM(weekly_revenue) AS INT64) as total_revenue
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
GROUP BY year_week
ORDER BY year_week
"
echo ""

# -----------------------------------------------------------------------------
echo "6. SKU 11119 (TOP SKU) - Detailed History vs Forecast"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
-- Historical actuals
SELECT
  'ACTUAL' as type,
  year_week as period,
  CAST(weekly_quantity AS INT64) as units,
  NULL as lower_bound,
  NULL as upper_bound
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
WHERE sku = '11119'

UNION ALL

-- Forecasts
SELECT
  'FORECAST' as type,
  CAST(forecast_timestamp AS STRING) as period,
  CAST(forecast_value AS INT64) as units,
  CAST(prediction_interval_lower_bound AS INT64) as lower_bound,
  CAST(prediction_interval_upper_bound AS INT64) as upper_bound
FROM ML.FORECAST(
  MODEL \`${PROJECT_ID}.${DATASET}.sku0_model_arima\`,
  STRUCT(4 AS horizon)
)
WHERE sku = '11119'

ORDER BY period
"
echo ""

echo "=============================================="
echo "VALIDATION COMPLETE"
echo "=============================================="
