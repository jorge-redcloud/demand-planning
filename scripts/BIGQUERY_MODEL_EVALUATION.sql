-- ============================================================================
-- BIGQUERY ML MODEL EVALUATION FRAMEWORK
-- ============================================================================
-- RedAI Demand Forecasting - ACA Dataset
--
-- This script implements model training and evaluation in BigQuery ML
-- for SKU, Category, and Customer level demand forecasting.
--
-- Prerequisites:
--   1. V2 data must be deployed to BigQuery (run DEPLOY_V2_TO_BIGQUERY.sh)
--   2. Dataset: redai_aca_features
--
-- Execution Environment: Google BigQuery
-- Estimated Cost: ~$5-10 for full evaluation
-- ============================================================================

-- Configuration
DECLARE PROJECT_ID STRING DEFAULT 'your-project-id';
DECLARE DATASET_NAME STRING DEFAULT 'redai_aca_features';
DECLARE TRAIN_END_WEEK INT64 DEFAULT 26;  -- Train on W01-W26
DECLARE TEST_START_WEEK INT64 DEFAULT 27; -- Test on W27-W52

-- ============================================================================
-- PART 1: DATA PREPARATION
-- ============================================================================

-- 1.1 Create training view (H1: W01-W26)
CREATE OR REPLACE VIEW `${DATASET_NAME}.train_sku_weekly` AS
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
FROM `${DATASET_NAME}.v2_features_weekly`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= TRAIN_END_WEEK;

-- 1.2 Create test view (H2: W27-W52)
CREATE OR REPLACE VIEW `${DATASET_NAME}.test_sku_weekly` AS
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
FROM `${DATASET_NAME}.v2_features_weekly`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= TEST_START_WEEK;

-- 1.3 Create category training data
CREATE OR REPLACE VIEW `${DATASET_NAME}.train_category_weekly` AS
SELECT
  category,
  year_week,
  CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
  weekly_quantity,
  weekly_revenue
FROM `${DATASET_NAME}.v2_features_category`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= TRAIN_END_WEEK;

-- 1.4 Create customer training data
CREATE OR REPLACE VIEW `${DATASET_NAME}.train_customer_weekly` AS
SELECT
  customer_id,
  year_week,
  CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
  weekly_quantity,
  weekly_revenue,
  sku_count
FROM `${DATASET_NAME}.v2_features_customer`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= TRAIN_END_WEEK;


-- ============================================================================
-- PART 2: SKU-LEVEL MODELS
-- ============================================================================

-- 2.1 XGBoost Model for SKU Demand (Best performer in local evaluation)
CREATE OR REPLACE MODEL `${DATASET_NAME}.model_sku_xgboost`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['weekly_quantity'],
  num_parallel_tree = 100,
  max_tree_depth = 5,
  learn_rate = 0.1,
  l1_reg = 0.1,
  l2_reg = 0.1,
  early_stop = TRUE,
  min_split_loss = 0,
  data_split_method = 'NO_SPLIT'  -- We handle split manually
) AS
SELECT
  weekly_quantity,
  lag1_quantity,
  lag2_quantity,
  lag4_quantity,
  rolling_avg_4w,
  avg_unit_price,
  week_num
FROM `${DATASET_NAME}.train_sku_weekly`
WHERE lag1_quantity IS NOT NULL;  -- Need lag features

-- 2.2 ARIMA+ Model for Top SKUs (Time series approach)
-- Note: ARIMA_PLUS works best with sufficient history per SKU
CREATE OR REPLACE MODEL `${DATASET_NAME}.model_sku_arima`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'year_week',
  time_series_data_col = 'weekly_quantity',
  time_series_id_col = 'sku',
  auto_arima = TRUE,
  data_frequency = 'WEEKLY',
  holiday_region = 'ZA'  -- South Africa holidays
) AS
SELECT
  sku,
  year_week,
  weekly_quantity
FROM `${DATASET_NAME}.train_sku_weekly`
WHERE sku IN (
  -- Only train on SKUs with sufficient history (20+ weeks)
  SELECT sku
  FROM `${DATASET_NAME}.train_sku_weekly`
  GROUP BY sku
  HAVING COUNT(*) >= 20
);


-- ============================================================================
-- PART 3: CATEGORY-LEVEL MODELS
-- ============================================================================

-- 3.1 ARIMA+ for Category Demand
CREATE OR REPLACE MODEL `${DATASET_NAME}.model_category_arima`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'year_week',
  time_series_data_col = 'weekly_quantity',
  time_series_id_col = 'category',
  auto_arima = TRUE,
  data_frequency = 'WEEKLY',
  holiday_region = 'ZA'
) AS
SELECT
  category,
  year_week,
  weekly_quantity
FROM `${DATASET_NAME}.train_category_weekly`;


-- ============================================================================
-- PART 4: CUSTOMER-LEVEL MODELS
-- ============================================================================

-- 4.1 ARIMA+ for Customer Demand
CREATE OR REPLACE MODEL `${DATASET_NAME}.model_customer_arima`
OPTIONS(
  model_type = 'ARIMA_PLUS',
  time_series_timestamp_col = 'year_week',
  time_series_data_col = 'weekly_quantity',
  time_series_id_col = 'customer_id',
  auto_arima = TRUE,
  data_frequency = 'WEEKLY'
) AS
SELECT
  customer_id,
  year_week,
  weekly_quantity
FROM `${DATASET_NAME}.train_customer_weekly`
WHERE customer_id IN (
  -- Only train on customers with sufficient history
  SELECT customer_id
  FROM `${DATASET_NAME}.train_customer_weekly`
  GROUP BY customer_id
  HAVING COUNT(*) >= 15
);

-- 4.2 XGBoost for Customer Demand
CREATE OR REPLACE MODEL `${DATASET_NAME}.model_customer_xgboost`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['weekly_quantity'],
  num_parallel_tree = 100,
  max_tree_depth = 5,
  learn_rate = 0.1,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  weekly_quantity,
  week_num,
  sku_count,
  weekly_revenue
FROM `${DATASET_NAME}.train_customer_weekly`;


-- ============================================================================
-- PART 5: MODEL EVALUATION
-- ============================================================================

-- 5.1 Evaluate XGBoost SKU Model
CREATE OR REPLACE TABLE `${DATASET_NAME}.eval_sku_xgboost` AS
SELECT
  t.sku,
  t.year_week,
  t.weekly_quantity AS actual,
  p.predicted_weekly_quantity AS predicted,
  ABS(t.weekly_quantity - p.predicted_weekly_quantity) AS abs_error,
  SAFE_DIVIDE(ABS(t.weekly_quantity - p.predicted_weekly_quantity), t.weekly_quantity) * 100 AS pct_error
FROM `${DATASET_NAME}.test_sku_weekly` t
JOIN ML.PREDICT(
  MODEL `${DATASET_NAME}.model_sku_xgboost`,
  (SELECT * FROM `${DATASET_NAME}.test_sku_weekly` WHERE lag1_quantity IS NOT NULL)
) p
ON t.sku = p.sku AND t.year_week = p.year_week;

-- 5.2 XGBoost SKU Metrics Summary
SELECT
  'XGBoost_SKU' AS model_name,
  COUNT(*) AS predictions,
  COUNT(DISTINCT sku) AS skus_evaluated,
  ROUND(AVG(abs_error), 2) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 2) AS rmse,
  ROUND(APPROX_QUANTILES(pct_error, 100)[OFFSET(50)], 2) AS mape_median
FROM `${DATASET_NAME}.eval_sku_xgboost`
WHERE actual > 0;

-- 5.3 Evaluate ARIMA SKU Model (forecast H2)
CREATE OR REPLACE TABLE `${DATASET_NAME}.eval_sku_arima` AS
WITH forecasts AS (
  SELECT
    sku,
    forecast_timestamp AS year_week,
    forecast_value AS predicted
  FROM ML.FORECAST(
    MODEL `${DATASET_NAME}.model_sku_arima`,
    STRUCT(26 AS horizon, 0.95 AS confidence_level)
  )
)
SELECT
  t.sku,
  t.year_week,
  t.weekly_quantity AS actual,
  f.predicted,
  ABS(t.weekly_quantity - f.predicted) AS abs_error,
  SAFE_DIVIDE(ABS(t.weekly_quantity - f.predicted), t.weekly_quantity) * 100 AS pct_error
FROM `${DATASET_NAME}.test_sku_weekly` t
JOIN forecasts f
ON t.sku = f.sku AND t.year_week = f.year_week;

-- 5.4 ARIMA SKU Metrics Summary
SELECT
  'ARIMA_SKU' AS model_name,
  COUNT(*) AS predictions,
  COUNT(DISTINCT sku) AS skus_evaluated,
  ROUND(AVG(abs_error), 2) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 2) AS rmse,
  ROUND(APPROX_QUANTILES(pct_error, 100)[OFFSET(50)], 2) AS mape_median
FROM `${DATASET_NAME}.eval_sku_arima`
WHERE actual > 0;

-- 5.5 Evaluate Category ARIMA Model
CREATE OR REPLACE TABLE `${DATASET_NAME}.eval_category_arima` AS
WITH forecasts AS (
  SELECT
    category,
    forecast_timestamp AS year_week,
    forecast_value AS predicted
  FROM ML.FORECAST(
    MODEL `${DATASET_NAME}.model_category_arima`,
    STRUCT(26 AS horizon, 0.95 AS confidence_level)
  )
),
test_data AS (
  SELECT
    category,
    year_week,
    weekly_quantity
  FROM `${DATASET_NAME}.v2_features_category`
  WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= 27
)
SELECT
  t.category,
  t.year_week,
  t.weekly_quantity AS actual,
  f.predicted,
  ABS(t.weekly_quantity - f.predicted) AS abs_error,
  SAFE_DIVIDE(ABS(t.weekly_quantity - f.predicted), t.weekly_quantity) * 100 AS pct_error
FROM test_data t
JOIN forecasts f
ON t.category = f.category AND t.year_week = f.year_week;

-- 5.6 Category ARIMA Metrics Summary
SELECT
  'ARIMA_Category' AS model_name,
  COUNT(*) AS predictions,
  COUNT(DISTINCT category) AS categories_evaluated,
  ROUND(AVG(abs_error), 2) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 2) AS rmse,
  ROUND(APPROX_QUANTILES(pct_error, 100)[OFFSET(50)], 2) AS mape_median
FROM `${DATASET_NAME}.eval_category_arima`
WHERE actual > 0;

-- 5.7 Evaluate Customer ARIMA Model
CREATE OR REPLACE TABLE `${DATASET_NAME}.eval_customer_arima` AS
WITH forecasts AS (
  SELECT
    customer_id,
    forecast_timestamp AS year_week,
    forecast_value AS predicted
  FROM ML.FORECAST(
    MODEL `${DATASET_NAME}.model_customer_arima`,
    STRUCT(26 AS horizon, 0.95 AS confidence_level)
  )
),
test_data AS (
  SELECT
    customer_id,
    year_week,
    weekly_quantity
  FROM `${DATASET_NAME}.v2_features_customer`
  WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= 27
)
SELECT
  t.customer_id,
  t.year_week,
  t.weekly_quantity AS actual,
  f.predicted,
  ABS(t.weekly_quantity - f.predicted) AS abs_error,
  SAFE_DIVIDE(ABS(t.weekly_quantity - f.predicted), t.weekly_quantity) * 100 AS pct_error
FROM test_data t
JOIN forecasts f
ON t.customer_id = f.customer_id AND t.year_week = f.year_week;

-- 5.8 Customer ARIMA Metrics Summary
SELECT
  'ARIMA_Customer' AS model_name,
  COUNT(*) AS predictions,
  COUNT(DISTINCT customer_id) AS customers_evaluated,
  ROUND(AVG(abs_error), 2) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 2) AS rmse,
  ROUND(APPROX_QUANTILES(pct_error, 100)[OFFSET(50)], 2) AS mape_median
FROM `${DATASET_NAME}.eval_customer_arima`
WHERE actual > 0;


-- ============================================================================
-- PART 6: CONSOLIDATED RESULTS
-- ============================================================================

-- 6.1 All Model Results Comparison
CREATE OR REPLACE TABLE `${DATASET_NAME}.model_evaluation_summary` AS
SELECT 'XGBoost' AS model, 'SKU' AS level,
       AVG(abs_error) AS mae,
       SQRT(AVG(POW(actual-predicted, 2))) AS rmse,
       APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS mape,
       COUNT(*) AS n_predictions,
       COUNT(DISTINCT sku) AS n_entities
FROM `${DATASET_NAME}.eval_sku_xgboost` WHERE actual > 0

UNION ALL

SELECT 'ARIMA_PLUS' AS model, 'SKU' AS level,
       AVG(abs_error) AS mae,
       SQRT(AVG(POW(actual-predicted, 2))) AS rmse,
       APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS mape,
       COUNT(*) AS n_predictions,
       COUNT(DISTINCT sku) AS n_entities
FROM `${DATASET_NAME}.eval_sku_arima` WHERE actual > 0

UNION ALL

SELECT 'ARIMA_PLUS' AS model, 'Category' AS level,
       AVG(abs_error) AS mae,
       SQRT(AVG(POW(actual-predicted, 2))) AS rmse,
       APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS mape,
       COUNT(*) AS n_predictions,
       COUNT(DISTINCT category) AS n_entities
FROM `${DATASET_NAME}.eval_category_arima` WHERE actual > 0

UNION ALL

SELECT 'ARIMA_PLUS' AS model, 'Customer' AS level,
       AVG(abs_error) AS mae,
       SQRT(AVG(POW(actual-predicted, 2))) AS rmse,
       APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS mape,
       COUNT(*) AS n_predictions,
       COUNT(DISTINCT customer_id) AS n_entities
FROM `${DATASET_NAME}.eval_customer_arima` WHERE actual > 0;

-- View final results
SELECT
  level,
  model,
  ROUND(mae, 0) AS mae,
  ROUND(mape, 1) AS mape_pct,
  ROUND(rmse, 0) AS rmse,
  n_entities AS entities_evaluated,
  n_predictions
FROM `${DATASET_NAME}.model_evaluation_summary`
ORDER BY level, mape;


-- ============================================================================
-- PART 7: PRODUCTION FORECASTING QUERIES
-- ============================================================================

-- 7.1 Generate Next 4 Weeks SKU Forecast
CREATE OR REPLACE TABLE `${DATASET_NAME}.forecast_sku_next4w` AS
SELECT
  sku,
  forecast_timestamp AS forecast_week,
  ROUND(forecast_value, 0) AS predicted_quantity,
  ROUND(prediction_interval_lower_bound, 0) AS lower_bound,
  ROUND(prediction_interval_upper_bound, 0) AS upper_bound
FROM ML.FORECAST(
  MODEL `${DATASET_NAME}.model_sku_arima`,
  STRUCT(4 AS horizon, 0.80 AS confidence_level)
)
ORDER BY sku, forecast_week;

-- 7.2 Generate Next 4 Weeks Category Forecast
CREATE OR REPLACE TABLE `${DATASET_NAME}.forecast_category_next4w` AS
SELECT
  category,
  forecast_timestamp AS forecast_week,
  ROUND(forecast_value, 0) AS predicted_quantity,
  ROUND(prediction_interval_lower_bound, 0) AS lower_bound,
  ROUND(prediction_interval_upper_bound, 0) AS upper_bound
FROM ML.FORECAST(
  MODEL `${DATASET_NAME}.model_category_arima`,
  STRUCT(4 AS horizon, 0.80 AS confidence_level)
)
ORDER BY category, forecast_week;

-- 7.3 Generate Next 4 Weeks Customer Forecast
CREATE OR REPLACE TABLE `${DATASET_NAME}.forecast_customer_next4w` AS
SELECT
  customer_id,
  forecast_timestamp AS forecast_week,
  ROUND(forecast_value, 0) AS predicted_quantity,
  ROUND(prediction_interval_lower_bound, 0) AS lower_bound,
  ROUND(prediction_interval_upper_bound, 0) AS upper_bound
FROM ML.FORECAST(
  MODEL `${DATASET_NAME}.model_customer_arima`,
  STRUCT(4 AS horizon, 0.80 AS confidence_level)
)
ORDER BY customer_id, forecast_week;


-- ============================================================================
-- PART 8: BASELINE MODELS FOR COMPARISON
-- ============================================================================

-- 8.1 Moving Average Baseline (4-week) for SKU
CREATE OR REPLACE TABLE `${DATASET_NAME}.eval_sku_ma4` AS
WITH last_4_weeks AS (
  SELECT
    sku,
    AVG(weekly_quantity) AS ma4_prediction
  FROM `${DATASET_NAME}.train_sku_weekly`
  WHERE week_num BETWEEN 23 AND 26  -- Last 4 weeks of training
  GROUP BY sku
),
test_data AS (
  SELECT
    sku,
    year_week,
    weekly_quantity
  FROM `${DATASET_NAME}.v2_features_weekly`
  WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= 27
)
SELECT
  t.sku,
  t.year_week,
  t.weekly_quantity AS actual,
  l.ma4_prediction AS predicted,
  ABS(t.weekly_quantity - l.ma4_prediction) AS abs_error,
  SAFE_DIVIDE(ABS(t.weekly_quantity - l.ma4_prediction), t.weekly_quantity) * 100 AS pct_error
FROM test_data t
JOIN last_4_weeks l ON t.sku = l.sku;

-- MA4 Metrics
SELECT
  'MA_4Week' AS model_name,
  'SKU' AS level,
  COUNT(*) AS predictions,
  COUNT(DISTINCT sku) AS skus_evaluated,
  ROUND(AVG(abs_error), 2) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 2) AS rmse,
  ROUND(APPROX_QUANTILES(pct_error, 100)[OFFSET(50)], 2) AS mape_median
FROM `${DATASET_NAME}.eval_sku_ma4`
WHERE actual > 0;

-- 8.2 Naive Last Value Baseline for SKU
CREATE OR REPLACE TABLE `${DATASET_NAME}.eval_sku_naive` AS
WITH last_value AS (
  SELECT
    sku,
    weekly_quantity AS naive_prediction
  FROM `${DATASET_NAME}.train_sku_weekly`
  WHERE week_num = 26  -- Last week of training
),
test_data AS (
  SELECT
    sku,
    year_week,
    weekly_quantity
  FROM `${DATASET_NAME}.v2_features_weekly`
  WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= 27
)
SELECT
  t.sku,
  t.year_week,
  t.weekly_quantity AS actual,
  l.naive_prediction AS predicted,
  ABS(t.weekly_quantity - l.naive_prediction) AS abs_error,
  SAFE_DIVIDE(ABS(t.weekly_quantity - l.naive_prediction), t.weekly_quantity) * 100 AS pct_error
FROM test_data t
JOIN last_value l ON t.sku = l.sku;

-- Naive Metrics
SELECT
  'Naive_Last' AS model_name,
  'SKU' AS level,
  COUNT(*) AS predictions,
  COUNT(DISTINCT sku) AS skus_evaluated,
  ROUND(AVG(abs_error), 2) AS mae,
  ROUND(SQRT(AVG(POW(actual - predicted, 2))), 2) AS rmse,
  ROUND(APPROX_QUANTILES(pct_error, 100)[OFFSET(50)], 2) AS mape_median
FROM `${DATASET_NAME}.eval_sku_naive`
WHERE actual > 0;
