-- =============================================================================
-- V2 MODEL USAGE QUERIES
-- =============================================================================
-- These queries show how to use the trained models programmatically
-- Run these in BigQuery Console or via bq command line
-- =============================================================================

-- =============================================================================
-- 1. FORECAST DEMAND BY SKU (Next 4 Weeks)
-- =============================================================================
-- Use ARIMA model to predict future demand for each SKU

SELECT
  sku,
  forecast_timestamp,
  ROUND(forecast_value, 0) as predicted_quantity,
  ROUND(prediction_interval_lower_bound, 0) as lower_bound,
  ROUND(prediction_interval_upper_bound, 0) as upper_bound,
  ROUND(standard_error, 2) as uncertainty
FROM ML.FORECAST(
  MODEL `mimetic-maxim-443710-s2.demand_forecasting.v2_model_sku_demand_arima`,
  STRUCT(4 AS horizon, 0.9 AS confidence_level)
)
ORDER BY sku, forecast_timestamp;


-- =============================================================================
-- 2. FORECAST DEMAND BY CATEGORY (Next 8 Weeks)
-- =============================================================================

SELECT
  category,
  forecast_timestamp,
  ROUND(forecast_value, 0) as predicted_quantity,
  ROUND(prediction_interval_lower_bound, 0) as lower_bound,
  ROUND(prediction_interval_upper_bound, 0) as upper_bound
FROM ML.FORECAST(
  MODEL `mimetic-maxim-443710-s2.demand_forecasting.v2_model_category_demand_arima`,
  STRUCT(8 AS horizon, 0.9 AS confidence_level)
)
ORDER BY category, forecast_timestamp;


-- =============================================================================
-- 3. PREDICT DEMAND FOR SPECIFIC SKU WITH PRICE CHANGE
-- =============================================================================
-- Use XGBoost model to see impact of price on demand

-- Example: What if we change price by 10%?
SELECT
  ROUND(predicted_weekly_quantity, 0) as predicted_demand,
  avg_unit_price as current_price,
  lag1_quantity,
  rolling_avg_4w
FROM ML.PREDICT(
  MODEL `mimetic-maxim-443710-s2.demand_forecasting.v2_model_sku_demand_xgb`,
  (
    SELECT
      100.0 as avg_unit_price,        -- Set price
      1000.0 as lag1_quantity,        -- Last week's sales
      900.0 as lag2_quantity,         -- 2 weeks ago
      800.0 as lag4_quantity,         -- 4 weeks ago
      100.0 as lag1_price,            -- Last week's price
      100.0 as lag2_price,            -- 2 weeks ago price
      950.0 as rolling_avg_4w,        -- 4-week rolling avg
      100.0 as price_rolling_avg,     -- 4-week price avg
      0.0 as price_change_pct,        -- No price change
      5 as order_count,               -- Typical order count
      10 as unique_customers,         -- Typical customers
      45 as week_of_year,             -- Week 45
      11 as month                     -- November
  )
);


-- =============================================================================
-- 4. FORECAST BULK BUYER DEMAND
-- =============================================================================

SELECT
  customer_id,
  forecast_timestamp,
  ROUND(forecast_value, 0) as predicted_quantity,
  ROUND(prediction_interval_lower_bound, 0) as lower_bound,
  ROUND(prediction_interval_upper_bound, 0) as upper_bound
FROM ML.FORECAST(
  MODEL `mimetic-maxim-443710-s2.demand_forecasting.v2_model_bulk_buyer_arima`,
  STRUCT(4 AS horizon, 0.9 AS confidence_level)
)
ORDER BY customer_id, forecast_timestamp
LIMIT 100;


-- =============================================================================
-- 5. PREDICT CUSTOMER BUYING CYCLE
-- =============================================================================
-- Classify new customers based on their behavior

SELECT
  customer_id,
  predicted_cycle_regularity,
  predicted_cycle_regularity_probs
FROM ML.PREDICT(
  MODEL `mimetic-maxim-443710-s2.demand_forecasting.v2_model_buying_cycle_classifier`,
  (
    SELECT
      customer_id,
      total_orders,
      COALESCE(avg_days_between_orders, 30) as avg_days_between,
      total_units,
      total_revenue,
      avg_order_value,
      active_weeks
    FROM `mimetic-maxim-443710-s2.demand_forecasting.v2_dim_customers`
    WHERE total_orders >= 2
    LIMIT 20
  )
);


-- =============================================================================
-- 6. TOP SKUs TO RESTOCK (High Demand Forecast)
-- =============================================================================

WITH forecasts AS (
  SELECT
    sku,
    SUM(forecast_value) as total_predicted_4w
  FROM ML.FORECAST(
    MODEL `mimetic-maxim-443710-s2.demand_forecasting.v2_model_sku_demand_arima`,
    STRUCT(4 AS horizon)
  )
  GROUP BY sku
),
current_sales AS (
  SELECT
    sku,
    SUM(weekly_quantity) as last_4w_actual
  FROM `mimetic-maxim-443710-s2.demand_forecasting.v2_features_weekly`
  WHERE year_week >= '2025-W48'
  GROUP BY sku
)
SELECT
  f.sku,
  p.name,
  p.category_l1,
  ROUND(c.last_4w_actual, 0) as last_4w_actual,
  ROUND(f.total_predicted_4w, 0) as next_4w_forecast,
  ROUND((f.total_predicted_4w - c.last_4w_actual) / c.last_4w_actual * 100, 1) as growth_pct
FROM forecasts f
JOIN current_sales c ON f.sku = c.sku
JOIN `mimetic-maxim-443710-s2.demand_forecasting.v2_dim_products` p ON f.sku = p.sku
WHERE c.last_4w_actual > 100
ORDER BY f.total_predicted_4w DESC
LIMIT 20;


-- =============================================================================
-- 7. BULK BUYERS AT RISK (Irregular Patterns)
-- =============================================================================

SELECT
  c.customer_id,
  c.customer_name,
  c.primary_region,
  c.buyer_type,
  c.cycle_regularity,
  c.avg_days_between_orders,
  c.last_order,
  DATE_DIFF(CURRENT_DATE(), PARSE_DATE('%Y-%m-%d', c.last_order), DAY) as days_since_last_order,
  ROUND(c.total_revenue, 0) as total_revenue
FROM `mimetic-maxim-443710-s2.demand_forecasting.v2_dim_customers` c
WHERE c.buyer_type IN ('Bulk Buyer', 'High-Value Buyer')
  AND c.cycle_regularity = 'Irregular'
ORDER BY c.total_revenue DESC
LIMIT 20;


-- =============================================================================
-- 8. PRICE SENSITIVITY ANALYSIS
-- =============================================================================
-- Which SKUs are most sensitive to price changes?

SELECT
  sku,
  COUNT(*) as price_change_events,
  AVG(price_change_pct) as avg_price_change,
  AVG(CASE
    WHEN price_change_pct > 0 THEN
      (weekly_quantity - LAG(weekly_quantity) OVER (PARTITION BY sku ORDER BY year_week)) / weekly_quantity * 100
    ELSE NULL
  END) as avg_demand_response_to_increase
FROM `mimetic-maxim-443710-s2.demand_forecasting.v2_price_history`
WHERE ABS(price_change_pct) > 5  -- Significant price changes
GROUP BY sku
HAVING COUNT(*) >= 3
ORDER BY avg_demand_response_to_increase DESC
LIMIT 20;


-- =============================================================================
-- 9. WEEKLY SUMMARY FOR DASHBOARD
-- =============================================================================

SELECT
  year_week,
  invoice_count,
  ROUND(total_quantity, 0) as total_units,
  ROUND(total_revenue, 0) as total_revenue,
  unique_skus,
  unique_customers,
  data_completeness,
  ROUND(price_coverage, 1) as price_coverage_pct
FROM `mimetic-maxim-443710-s2.demand_forecasting.v2_week_completeness`
ORDER BY year_week;


-- =============================================================================
-- 10. EXPORT FORECAST DATA FOR VISUALIZATION
-- =============================================================================

-- Create a view for easy visualization
CREATE OR REPLACE VIEW `mimetic-maxim-443710-s2.demand_forecasting.v2_sku_forecast_view` AS
WITH actuals AS (
  SELECT
    sku,
    year_week,
    weekly_quantity,
    avg_unit_price,
    'actual' as data_type
  FROM `mimetic-maxim-443710-s2.demand_forecasting.v2_features_weekly`
),
forecasts AS (
  SELECT
    sku,
    FORMAT_DATE('%G-W%V', DATE(forecast_timestamp)) as year_week,
    forecast_value as weekly_quantity,
    NULL as avg_unit_price,
    'forecast' as data_type
  FROM ML.FORECAST(
    MODEL `mimetic-maxim-443710-s2.demand_forecasting.v2_model_sku_demand_arima`,
    STRUCT(8 AS horizon)
  )
)
SELECT * FROM actuals
UNION ALL
SELECT * FROM forecasts;
