-- ============================================================
-- BIGQUERY ML: XGBOOST DEMAND FORECASTING - ALL LEVELS
-- ============================================================
-- This script creates XGBoost models for SKU, Category, and Customer
-- levels using the same methodology and features.
--
-- Prerequisites:
--   1. Upload feature data to BigQuery (see upload_to_bigquery.sh)
--   2. Set your project and dataset variables below
--
-- Usage:
--   bq query --use_legacy_sql=false < bigquery_xgboost_all_levels.sql
-- ============================================================

-- Set your project and dataset
-- DECLARE project_id STRING DEFAULT 'your-project-id';
-- DECLARE dataset_id STRING DEFAULT 'redai_demand_forecast';

-- ============================================================
-- STEP 1: CREATE FEATURE TABLES
-- ============================================================

-- 1.1 SKU-level weekly aggregation with features
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.features_sku_weekly` AS
WITH weekly_agg AS (
  SELECT
    sku,
    year_week,
    SUM(quantity) as quantity,
    ANY_VALUE(description) as description,
    ANY_VALUE(category_l1) as category
  FROM `${PROJECT_ID}.${DATASET_ID}.fact_lineitem`
  GROUP BY sku, year_week
),
with_lags AS (
  SELECT
    *,
    LAG(quantity, 1) OVER (PARTITION BY sku ORDER BY year_week) as lag1,
    LAG(quantity, 2) OVER (PARTITION BY sku ORDER BY year_week) as lag2,
    LAG(quantity, 4) OVER (PARTITION BY sku ORDER BY year_week) as lag4,
    AVG(quantity) OVER (
      PARTITION BY sku
      ORDER BY year_week
      ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) as rolling_avg_4w,
    CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) as week_num,
    CASE WHEN year_week <= '2025-W26' THEN 'H1' ELSE 'H2' END as period
  FROM weekly_agg
)
SELECT * FROM with_lags
WHERE lag1 IS NOT NULL;  -- Need at least 1 lag for features

-- 1.2 Category-level weekly aggregation with features
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.features_category_weekly` AS
WITH weekly_agg AS (
  SELECT
    category_l1 as category,
    year_week,
    SUM(quantity) as quantity
  FROM `${PROJECT_ID}.${DATASET_ID}.fact_lineitem`
  WHERE category_l1 IS NOT NULL
  GROUP BY category_l1, year_week
),
with_lags AS (
  SELECT
    *,
    LAG(quantity, 1) OVER (PARTITION BY category ORDER BY year_week) as lag1,
    LAG(quantity, 2) OVER (PARTITION BY category ORDER BY year_week) as lag2,
    LAG(quantity, 4) OVER (PARTITION BY category ORDER BY year_week) as lag4,
    AVG(quantity) OVER (
      PARTITION BY category
      ORDER BY year_week
      ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) as rolling_avg_4w,
    CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) as week_num,
    CASE WHEN year_week <= '2025-W26' THEN 'H1' ELSE 'H2' END as period
  FROM weekly_agg
)
SELECT * FROM with_lags
WHERE lag1 IS NOT NULL;

-- 1.3 Customer-level weekly aggregation with features
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.features_customer_weekly` AS
WITH weekly_agg AS (
  SELECT
    customer_id,
    year_week,
    SUM(quantity) as quantity,
    ANY_VALUE(customer_name) as customer_name
  FROM `${PROJECT_ID}.${DATASET_ID}.fact_lineitem`
  GROUP BY customer_id, year_week
),
-- Get top 50 customers by volume
top_customers AS (
  SELECT customer_id
  FROM weekly_agg
  GROUP BY customer_id
  ORDER BY SUM(quantity) DESC
  LIMIT 50
),
with_lags AS (
  SELECT
    w.*,
    LAG(w.quantity, 1) OVER (PARTITION BY w.customer_id ORDER BY w.year_week) as lag1,
    LAG(w.quantity, 2) OVER (PARTITION BY w.customer_id ORDER BY w.year_week) as lag2,
    LAG(w.quantity, 4) OVER (PARTITION BY w.customer_id ORDER BY w.year_week) as lag4,
    AVG(w.quantity) OVER (
      PARTITION BY w.customer_id
      ORDER BY w.year_week
      ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) as rolling_avg_4w,
    CAST(REGEXP_EXTRACT(w.year_week, r'W(\d+)') AS INT64) as week_num,
    CASE WHEN w.year_week <= '2025-W26' THEN 'H1' ELSE 'H2' END as period
  FROM weekly_agg w
  INNER JOIN top_customers tc ON w.customer_id = tc.customer_id
)
SELECT * FROM with_lags
WHERE lag1 IS NOT NULL;


-- ============================================================
-- STEP 2: TRAIN XGBOOST MODELS
-- ============================================================

-- 2.1 SKU-Level XGBoost Model
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_sku_model`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['quantity'],
  num_parallel_tree = 1,
  max_iterations = 50,
  tree_method = 'HIST',
  early_stop = TRUE,
  min_split_loss = 0,
  max_tree_depth = 3,
  learn_rate = 0.1,
  data_split_method = 'NO_SPLIT'  -- We do our own train/test split
) AS
SELECT
  quantity,
  lag1,
  lag2,
  lag4,
  rolling_avg_4w,
  week_num
FROM `${PROJECT_ID}.${DATASET_ID}.features_sku_weekly`
WHERE period = 'H1'
  AND lag4 IS NOT NULL;  -- Ensure we have all features

-- 2.2 Category-Level XGBoost Model
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_category_model`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['quantity'],
  num_parallel_tree = 1,
  max_iterations = 50,
  tree_method = 'HIST',
  early_stop = TRUE,
  min_split_loss = 0,
  max_tree_depth = 3,
  learn_rate = 0.1,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  quantity,
  lag1,
  lag2,
  lag4,
  rolling_avg_4w,
  week_num
FROM `${PROJECT_ID}.${DATASET_ID}.features_category_weekly`
WHERE period = 'H1'
  AND lag4 IS NOT NULL;

-- 2.3 Customer-Level XGBoost Model
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_customer_model`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['quantity'],
  num_parallel_tree = 1,
  max_iterations = 50,
  tree_method = 'HIST',
  early_stop = TRUE,
  min_split_loss = 0,
  max_tree_depth = 3,
  learn_rate = 0.1,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  quantity,
  lag1,
  lag2,
  lag4,
  rolling_avg_4w,
  week_num
FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_weekly`
WHERE period = 'H1'
  AND lag4 IS NOT NULL;


-- ============================================================
-- STEP 3: GENERATE PREDICTIONS FOR H2
-- ============================================================

-- 3.1 SKU Predictions
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_sku` AS
SELECT
  f.sku,
  f.description,
  f.year_week,
  f.quantity as actual,
  p.predicted_quantity as predicted,
  ABS(f.quantity - p.predicted_quantity) as abs_error,
  CASE
    WHEN f.quantity > 0
    THEN ABS(f.quantity - p.predicted_quantity) / f.quantity * 100
    ELSE NULL
  END as pct_error
FROM `${PROJECT_ID}.${DATASET_ID}.features_sku_weekly` f
JOIN ML.PREDICT(
  MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_sku_model`,
  (SELECT sku, year_week, lag1, lag2, lag4, rolling_avg_4w, week_num
   FROM `${PROJECT_ID}.${DATASET_ID}.features_sku_weekly`
   WHERE period = 'H2')
) p ON f.sku = p.sku AND f.year_week = p.year_week
WHERE f.period = 'H2';

-- 3.2 Category Predictions
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_category` AS
SELECT
  f.category,
  f.year_week,
  f.quantity as actual,
  p.predicted_quantity as predicted,
  ABS(f.quantity - p.predicted_quantity) as abs_error,
  CASE
    WHEN f.quantity > 0
    THEN ABS(f.quantity - p.predicted_quantity) / f.quantity * 100
    ELSE NULL
  END as pct_error
FROM `${PROJECT_ID}.${DATASET_ID}.features_category_weekly` f
JOIN ML.PREDICT(
  MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_category_model`,
  (SELECT category, year_week, lag1, lag2, lag4, rolling_avg_4w, week_num
   FROM `${PROJECT_ID}.${DATASET_ID}.features_category_weekly`
   WHERE period = 'H2')
) p ON f.category = p.category AND f.year_week = p.year_week
WHERE f.period = 'H2';

-- 3.3 Customer Predictions
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_customer` AS
SELECT
  f.customer_id,
  f.customer_name,
  f.year_week,
  f.quantity as actual,
  p.predicted_quantity as predicted,
  ABS(f.quantity - p.predicted_quantity) as abs_error,
  CASE
    WHEN f.quantity > 0
    THEN ABS(f.quantity - p.predicted_quantity) / f.quantity * 100
    ELSE NULL
  END as pct_error
FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_weekly` f
JOIN ML.PREDICT(
  MODEL `${PROJECT_ID}.${DATASET_ID}.xgb_customer_model`,
  (SELECT customer_id, year_week, lag1, lag2, lag4, rolling_avg_4w, week_num
   FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_weekly`
   WHERE period = 'H2')
) p ON f.customer_id = p.customer_id AND f.year_week = p.year_week
WHERE f.period = 'H2';


-- ============================================================
-- STEP 4: MODEL EVALUATION METRICS
-- ============================================================

-- 4.1 SKU Model Evaluation
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.eval_sku` AS
SELECT
  'SKU' as level,
  'XGBoost' as model,
  COUNT(*) as n_predictions,
  AVG(abs_error) as mae,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] as median_mape,
  AVG(pct_error) as mean_mape,
  SQRT(AVG(POW(actual - predicted, 2))) as rmse
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku`
WHERE pct_error IS NOT NULL;

-- 4.2 Category Model Evaluation
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.eval_category` AS
SELECT
  'Category' as level,
  'XGBoost' as model,
  COUNT(*) as n_predictions,
  AVG(abs_error) as mae,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] as median_mape,
  AVG(pct_error) as mean_mape,
  SQRT(AVG(POW(actual - predicted, 2))) as rmse
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_category`
WHERE pct_error IS NOT NULL;

-- 4.3 Customer Model Evaluation
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.eval_customer` AS
SELECT
  'Customer' as level,
  'XGBoost' as model,
  COUNT(*) as n_predictions,
  AVG(abs_error) as mae,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] as median_mape,
  AVG(pct_error) as mean_mape,
  SQRT(AVG(POW(actual - predicted, 2))) as rmse
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_customer`
WHERE pct_error IS NOT NULL;

-- 4.4 Combined Evaluation Summary
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.eval_summary` AS
SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.eval_sku`
UNION ALL
SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.eval_category`
UNION ALL
SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.eval_customer`;


-- ============================================================
-- STEP 5: PER-ENTITY MAPE (for dashboard ranking)
-- ============================================================

-- 5.1 MAPE per SKU
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.mape_by_sku` AS
SELECT
  sku,
  ANY_VALUE(description) as description,
  COUNT(*) as n_weeks,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] as median_mape,
  AVG(pct_error) as mean_mape
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku`
WHERE pct_error IS NOT NULL
GROUP BY sku
ORDER BY median_mape;

-- 5.2 MAPE per Category
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.mape_by_category` AS
SELECT
  category,
  COUNT(*) as n_weeks,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] as median_mape,
  AVG(pct_error) as mean_mape
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_category`
WHERE pct_error IS NOT NULL
GROUP BY category
ORDER BY median_mape;

-- 5.3 MAPE per Customer
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.mape_by_customer` AS
SELECT
  customer_id,
  ANY_VALUE(customer_name) as customer_name,
  COUNT(*) as n_weeks,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] as median_mape,
  AVG(pct_error) as mean_mape
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_customer`
WHERE pct_error IS NOT NULL
GROUP BY customer_id
ORDER BY median_mape;


-- ============================================================
-- STEP 6: VIEW RESULTS
-- ============================================================

-- View overall model performance
SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.eval_summary`;

-- View top 10 best-predicted SKUs
SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.mape_by_sku` LIMIT 10;

-- View category performance
SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.mape_by_category`;

-- View top 10 best-predicted customers
SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.mape_by_customer` LIMIT 10;
