-- ============================================================================
-- BIGQUERY ML: CUSTOMER DEMAND FORECASTING WITH NORMALIZED CUSTOMERS
-- ============================================================================
-- RedAI Demand Forecasting - ACA Dataset
--
-- This script updates customer-level models to use the normalized
-- master_customer_id, which consolidates customers who have multiple
-- customer_ids but the same trading name.
--
-- Prerequisites:
--   1. Upload the customer master mapping to BigQuery:
--      - customer_master_mapping.csv → ${DATASET_ID}.customer_master_mapping
--      - customer_id_lookup.csv → ${DATASET_ID}.customer_id_lookup
--   2. Original fact_lineitem table must exist
--
-- Key Changes from Original:
--   - Aggregates by master_customer_id instead of customer_id
--   - Uses customer_name as the primary identifier (via lookup)
--   - Consolidates 804 original customer_ids into 770 unique customers
-- ============================================================================

-- Configuration (update these for your environment)
-- DECLARE PROJECT_ID STRING DEFAULT 'your-project-id';
-- DECLARE DATASET_ID STRING DEFAULT 'redai_aca_features';

-- ============================================================================
-- STEP 1: CREATE/UPLOAD MAPPING TABLES
-- ============================================================================

-- 1.1 Customer Master Mapping Table (upload from customer_master_mapping.csv)
-- Schema:
--   master_customer_id: INT64 (1-770)
--   customer_name: STRING
--   original_ids: STRING (comma-separated original customer_ids)
--   num_ids: INT64
--   total_volume: FLOAT64

-- Upload command:
-- bq load --source_format=CSV --autodetect \
--   ${PROJECT_ID}:${DATASET_ID}.customer_master_mapping \
--   customer_master_mapping.csv

-- 1.2 Customer ID Lookup Table (upload from customer_id_lookup.csv)
-- Schema:
--   original_customer_id: INT64
--   master_customer_id: INT64
--   customer_name: STRING

-- Upload command:
-- bq load --source_format=CSV --autodetect \
--   ${PROJECT_ID}:${DATASET_ID}.customer_id_lookup \
--   customer_id_lookup.csv


-- ============================================================================
-- STEP 2: CREATE NORMALIZED CUSTOMER FEATURE TABLE
-- ============================================================================

-- 2.1 Create normalized customer weekly features
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized` AS
WITH
-- Join fact_lineitem with customer lookup to get master_customer_id
-- NOTE: customer_id can be numeric OR alphanumeric (e.g., HB_CUT001)
-- Both fact_lineitem.customer_id and customer_id_lookup.original_customer_id are STRING
lineitem_with_master AS (
  SELECT
    f.*,
    COALESCE(c.master_customer_id, CAST(f.customer_id AS INT64)) AS master_customer_id,
    COALESCE(c.customer_name, f.customer_name) AS normalized_customer_name
  FROM `${PROJECT_ID}.${DATASET_ID}.fact_lineitem` f
  LEFT JOIN `${PROJECT_ID}.${DATASET_ID}.customer_id_lookup` c
    ON CAST(f.customer_id AS STRING) = c.original_customer_id
),
-- Aggregate by master_customer_id and year_week
weekly_agg AS (
  SELECT
    master_customer_id,
    normalized_customer_name AS customer_name,
    year_week,
    SUM(quantity) AS quantity,
    SUM(total_price) AS revenue,
    COUNT(DISTINCT invoice_number) AS order_count,
    COUNT(DISTINCT sku) AS sku_count,
    AVG(unit_price) AS avg_unit_price
  FROM lineitem_with_master
  GROUP BY master_customer_id, normalized_customer_name, year_week
),
-- Add lag features
with_lags AS (
  SELECT
    *,
    LAG(quantity, 1) OVER (PARTITION BY master_customer_id ORDER BY year_week) AS lag1_quantity,
    LAG(quantity, 2) OVER (PARTITION BY master_customer_id ORDER BY year_week) AS lag2_quantity,
    LAG(quantity, 4) OVER (PARTITION BY master_customer_id ORDER BY year_week) AS lag4_quantity,
    AVG(quantity) OVER (
      PARTITION BY master_customer_id
      ORDER BY year_week
      ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) AS rolling_avg_4w,
    CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
    CASE WHEN year_week <= '2025-W26' THEN 'H1' ELSE 'H2' END AS period
  FROM weekly_agg
)
SELECT * FROM with_lags
WHERE lag1_quantity IS NOT NULL;


-- ============================================================================
-- STEP 3: TRAIN XGBOOST MODEL FOR NORMALIZED CUSTOMERS
-- ============================================================================

-- 3.1 XGBoost Model using normalized customer data
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_customer_normalized`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['quantity'],
  num_parallel_tree = 100,
  max_tree_depth = 5,
  learn_rate = 0.1,
  l1_reg = 0.1,
  l2_reg = 0.1,
  early_stop = TRUE,
  min_split_loss = 0,
  data_split_method = 'NO_SPLIT'  -- We handle train/test split manually
) AS
SELECT
  quantity,
  lag1_quantity,
  lag2_quantity,
  lag4_quantity,
  rolling_avg_4w,
  avg_unit_price,
  order_count,
  week_num
FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized`
WHERE period = 'H1'
  AND lag4_quantity IS NOT NULL;  -- Ensure all features present


-- ============================================================================
-- STEP 4: TRAIN ARIMA+ MODEL FOR NORMALIZED CUSTOMERS
-- ============================================================================

-- 4.1 ARIMA+ Time Series Model
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.arima_customer_normalized`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'year_week',
  time_series_data_col = 'quantity',
  time_series_id_col = 'master_customer_id',
  auto_arima = TRUE,
  data_frequency = 'WEEKLY',
  holiday_region = 'ZA'  -- South Africa holidays
) AS
SELECT
  master_customer_id,
  year_week,
  quantity
FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized`
WHERE period = 'H1'
  AND master_customer_id IN (
    -- Only train on customers with sufficient history (15+ weeks)
    SELECT master_customer_id
    FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized`
    WHERE period = 'H1'
    GROUP BY master_customer_id
    HAVING COUNT(*) >= 15
  );


-- ============================================================================
-- STEP 5: GENERATE PREDICTIONS FOR H2
-- ============================================================================

-- 5.1 XGBoost Predictions for H2
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_customer_xgb_normalized` AS
SELECT
  f.master_customer_id,
  f.customer_name,
  f.year_week,
  f.quantity AS actual,
  p.predicted_quantity AS predicted,
  ABS(f.quantity - p.predicted_quantity) AS abs_error,
  CASE
    WHEN f.quantity > 0
    THEN ABS(f.quantity - p.predicted_quantity) / f.quantity * 100
    ELSE NULL
  END AS pct_error
FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized` f
JOIN ML.PREDICT(
  MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_customer_normalized`,
  (SELECT
     master_customer_id,
     year_week,
     lag1_quantity,
     lag2_quantity,
     lag4_quantity,
     rolling_avg_4w,
     avg_unit_price,
     order_count,
     week_num
   FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized`
   WHERE period = 'H2'
     AND lag4_quantity IS NOT NULL)
) p ON f.master_customer_id = p.master_customer_id AND f.year_week = p.year_week
WHERE f.period = 'H2';

-- 5.2 ARIMA+ Predictions for H2
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_customer_arima_normalized` AS
WITH forecasts AS (
  SELECT
    master_customer_id,
    forecast_timestamp AS year_week,
    forecast_value AS predicted,
    prediction_interval_lower_bound AS lower_bound,
    prediction_interval_upper_bound AS upper_bound
  FROM ML.FORECAST(
    MODEL `${PROJECT_ID}.${DATASET_ID}.arima_customer_normalized`,
    STRUCT(26 AS horizon, 0.80 AS confidence_level)  -- 26 weeks for H2
  )
)
SELECT
  f.master_customer_id,
  f.customer_name,
  f.year_week,
  f.quantity AS actual,
  fc.predicted,
  fc.lower_bound,
  fc.upper_bound,
  ABS(f.quantity - fc.predicted) AS abs_error,
  CASE
    WHEN f.quantity > 0
    THEN ABS(f.quantity - fc.predicted) / f.quantity * 100
    ELSE NULL
  END AS pct_error
FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized` f
JOIN forecasts fc
  ON f.master_customer_id = fc.master_customer_id
  AND f.year_week = fc.year_week
WHERE f.period = 'H2';


-- ============================================================================
-- STEP 6: MODEL EVALUATION
-- ============================================================================

-- 6.1 XGBoost Evaluation Metrics
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.eval_customer_xgb_normalized` AS
SELECT
  'Customer_Normalized' AS level,
  'XGBoost' AS model,
  COUNT(*) AS n_predictions,
  COUNT(DISTINCT master_customer_id) AS n_customers,
  AVG(abs_error) AS mae,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS median_mape,
  AVG(pct_error) AS mean_mape,
  SQRT(AVG(POW(actual - predicted, 2))) AS rmse
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_customer_xgb_normalized`
WHERE pct_error IS NOT NULL;

-- 6.2 ARIMA+ Evaluation Metrics
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.eval_customer_arima_normalized` AS
SELECT
  'Customer_Normalized' AS level,
  'ARIMA_PLUS' AS model,
  COUNT(*) AS n_predictions,
  COUNT(DISTINCT master_customer_id) AS n_customers,
  AVG(abs_error) AS mae,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS median_mape,
  AVG(pct_error) AS mean_mape,
  SQRT(AVG(POW(actual - predicted, 2))) AS rmse
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_customer_arima_normalized`
WHERE pct_error IS NOT NULL;

-- 6.3 Combined Evaluation Summary
SELECT
  model,
  level,
  n_customers,
  n_predictions,
  ROUND(mae, 0) AS mae,
  ROUND(median_mape, 1) AS median_mape_pct,
  ROUND(mean_mape, 1) AS mean_mape_pct,
  ROUND(rmse, 0) AS rmse
FROM (
  SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.eval_customer_xgb_normalized`
  UNION ALL
  SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.eval_customer_arima_normalized`
)
ORDER BY median_mape;


-- ============================================================================
-- STEP 7: PER-CUSTOMER MAPE (for dashboard ranking)
-- ============================================================================

-- 7.1 MAPE per Customer (XGBoost)
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.mape_by_customer_normalized` AS
SELECT
  master_customer_id,
  ANY_VALUE(customer_name) AS customer_name,
  COUNT(*) AS n_weeks,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS median_mape,
  AVG(pct_error) AS mean_mape,
  SUM(actual) AS total_actual_volume,
  SUM(predicted) AS total_predicted_volume
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_customer_xgb_normalized`
WHERE pct_error IS NOT NULL
GROUP BY master_customer_id
ORDER BY median_mape;

-- 7.2 Top 50 Customers by Volume with MAPE
SELECT
  master_customer_id,
  customer_name,
  n_weeks,
  ROUND(median_mape, 1) AS mape_pct,
  ROUND(total_actual_volume, 0) AS h2_actual_volume
FROM `${PROJECT_ID}.${DATASET_ID}.mape_by_customer_normalized`
ORDER BY total_actual_volume DESC
LIMIT 50;


-- ============================================================================
-- STEP 8: PRODUCTION FORECASTING (Next 4 Weeks)
-- ============================================================================

-- 8.1 Next 4 Weeks Customer Forecast
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.forecast_customer_normalized_next4w` AS
SELECT
  master_customer_id,
  forecast_timestamp AS forecast_week,
  ROUND(forecast_value, 0) AS predicted_quantity,
  ROUND(prediction_interval_lower_bound, 0) AS lower_bound,
  ROUND(prediction_interval_upper_bound, 0) AS upper_bound
FROM ML.FORECAST(
  MODEL `${PROJECT_ID}.${DATASET_ID}.arima_customer_normalized`,
  STRUCT(4 AS horizon, 0.80 AS confidence_level)
)
ORDER BY master_customer_id, forecast_week;

-- 8.2 Forecast with Customer Names
SELECT
  f.master_customer_id,
  m.customer_name,
  f.forecast_week,
  f.predicted_quantity,
  f.lower_bound,
  f.upper_bound
FROM `${PROJECT_ID}.${DATASET_ID}.forecast_customer_normalized_next4w` f
JOIN `${PROJECT_ID}.${DATASET_ID}.customer_master_mapping` m
  ON f.master_customer_id = m.master_customer_id
ORDER BY f.forecast_week, f.predicted_quantity DESC;


-- ============================================================================
-- STEP 9: COMPARISON WITH OLD (NON-NORMALIZED) MODEL
-- ============================================================================

-- Compare the two approaches side by side
SELECT
  'Original (customer_id)' AS approach,
  n_predictions,
  n_customers AS entities,
  ROUND(median_mape, 1) AS mape_pct,
  ROUND(mae, 0) AS mae
FROM `${PROJECT_ID}.${DATASET_ID}.eval_customer`
WHERE model = 'XGBoost'

UNION ALL

SELECT
  'Normalized (master_customer_id)' AS approach,
  n_predictions,
  n_customers AS entities,
  ROUND(median_mape, 1) AS mape_pct,
  ROUND(mae, 0) AS mae
FROM `${PROJECT_ID}.${DATASET_ID}.eval_customer_xgb_normalized`;
