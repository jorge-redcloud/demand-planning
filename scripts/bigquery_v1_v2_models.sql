-- ============================================================================
-- BIGQUERY ML: V1 BASELINE AND V2 ENHANCED MODELS
-- ============================================================================
-- RedAI ACA Demand Forecasting
--
-- This script creates and maintains both V1 (baseline) and V2 (enhanced) models
-- in BigQuery for comparison and production use.
--
-- Model Versioning Strategy:
--   V1 = Baseline XGBoost (current production)
--   V2 = Enhanced with pattern-specific handling and seasonality
--
-- IMPORTANT: Follow BIGQUERY_DATA_SPEC.md for data type requirements:
--   - year_week: STRING (YYYY-Wnn format)
--   - original_customer_id: STRING (mixed numeric/alphanumeric)
--   - master_customer_id: INT64
-- ============================================================================

-- Configuration (replace with your values)
-- DECLARE PROJECT_ID STRING DEFAULT 'your-project-id';
-- DECLARE DATASET_ID STRING DEFAULT 'redai_aca_features';


-- ============================================================================
-- PART 1: PATTERN CLASSIFICATION TABLE
-- ============================================================================

-- 1.1 Create SKU pattern classification
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.sku_pattern_classification` AS
WITH sku_stats AS (
  SELECT
    sku,
    AVG(weekly_quantity) AS mean_qty,
    STDDEV(weekly_quantity) AS std_qty,
    COUNT(*) AS n_weeks,
    MIN(weekly_quantity) AS min_qty,
    MAX(weekly_quantity) AS max_qty
  FROM `${PROJECT_ID}.${DATASET_ID}.features_weekly`
  GROUP BY sku
),
with_metrics AS (
  SELECT
    *,
    SAFE_DIVIDE(std_qty, GREATEST(mean_qty, 1)) AS cv,
    SAFE_DIVIDE(max_qty, GREATEST(min_qty, 1)) AS range_ratio
  FROM sku_stats
)
SELECT
  sku,
  mean_qty,
  std_qty,
  cv,
  n_weeks,
  range_ratio,
  CASE
    WHEN n_weeks < 10 THEN 'insufficient_data'
    WHEN cv < 0.3 AND mean_qty > 50 THEN 'stable'
    WHEN cv < 0.7 THEN 'cyclical'
    WHEN range_ratio > 10 THEN 'bulk_oneoff'
    ELSE 'high_variance'
  END AS pattern,
  CASE
    WHEN n_weeks >= 20 THEN 'full'
    WHEN n_weeks >= 10 THEN 'marginal'
    ELSE 'insufficient'
  END AS data_sufficiency,
  CASE
    WHEN n_weeks < 10 THEN 'none'
    WHEN cv < 0.7 THEN 'V2_cyclical'  -- Cyclical and stable use V2
    ELSE 'V1_baseline'  -- Bulk and high_variance use V1
  END AS recommended_model
FROM with_metrics;


-- ============================================================================
-- PART 2: CUSTOMER DATA SUFFICIENCY TABLE
-- ============================================================================

-- 2.1 Create customer data sufficiency classification
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.customer_data_sufficiency` AS
WITH cust_weekly AS (
  SELECT
    c.master_customer_id,
    f.year_week,
    SUM(f.quantity) AS quantity
  FROM `${PROJECT_ID}.${DATASET_ID}.fact_lineitem` f
  LEFT JOIN `${PROJECT_ID}.${DATASET_ID}.customer_id_lookup` c
    ON CAST(f.customer_id AS STRING) = CAST(c.original_customer_id AS STRING)
  GROUP BY c.master_customer_id, f.year_week
),
cust_stats AS (
  SELECT
    master_customer_id,
    AVG(quantity) AS mean_qty,
    STDDEV(quantity) AS std_qty,
    COUNT(*) AS n_weeks
  FROM cust_weekly
  WHERE master_customer_id IS NOT NULL
  GROUP BY master_customer_id
)
SELECT
  master_customer_id,
  mean_qty,
  std_qty,
  n_weeks,
  SAFE_DIVIDE(std_qty, GREATEST(mean_qty, 1)) AS cv,
  CASE
    WHEN n_weeks >= 20 THEN 'full'
    WHEN n_weeks >= 10 THEN 'marginal'
    ELSE 'insufficient'
  END AS data_sufficiency
FROM cust_stats;


-- ============================================================================
-- PART 3: V1 BASELINE MODELS (Keep Existing)
-- ============================================================================

-- 3.1 V1 SKU XGBoost Model
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.model_sku_xgboost_v1`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['weekly_quantity'],
  num_parallel_tree = 100,
  max_tree_depth = 5,
  learn_rate = 0.1,
  l1_reg = 0.1,
  l2_reg = 0.1,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  weekly_quantity,
  lag1_quantity,
  lag2_quantity,
  lag4_quantity,
  rolling_avg_4w,
  avg_unit_price,
  CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
  order_count
FROM `${PROJECT_ID}.${DATASET_ID}.features_weekly`
WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= 26
  AND lag4_quantity IS NOT NULL;


-- 3.2 V1 Customer XGBoost Model
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.model_customer_xgboost_v1`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['quantity'],
  num_parallel_tree = 100,
  max_tree_depth = 5,
  learn_rate = 0.1,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  quantity,
  lag1_quantity,
  lag2_quantity,
  lag4_quantity,
  rolling_avg_4w,
  avg_unit_price,
  week_num,
  order_count
FROM `${PROJECT_ID}.${DATASET_ID}.features_customer_normalized`
WHERE period = 'H1'
  AND lag4_quantity IS NOT NULL;


-- ============================================================================
-- PART 4: V2 ENHANCED MODELS
-- ============================================================================

-- 4.1 Create V2 feature table for SKU (with seasonality)
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.features_sku_v2` AS
WITH base AS (
  SELECT
    f.*,
    CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
    p.pattern,
    p.cv,
    p.data_sufficiency
  FROM `${PROJECT_ID}.${DATASET_ID}.features_weekly` f
  LEFT JOIN `${PROJECT_ID}.${DATASET_ID}.sku_pattern_classification` p
    ON f.sku = p.sku
),
with_w47 AS (
  SELECT
    sku,
    weekly_quantity AS w47_qty
  FROM base
  WHERE week_num = 47
),
non_w47_avg AS (
  SELECT
    sku,
    AVG(weekly_quantity) AS avg_qty_non_w47
  FROM base
  WHERE week_num != 47
  GROUP BY sku
)
SELECT
  b.*,
  MOD(b.week_num - 1, 4) + 1 AS week_of_month,
  IF(b.week_num IN (4, 8, 13, 17, 22, 26, 30, 35, 39, 43, 48, 52), 1, 0) AS is_month_end,
  IF(b.week_num IN (13, 26, 39, 52), 1, 0) AS is_quarter_end,
  IF(b.week_num = 47, 1, 0) AS is_w47,
  IF(b.week_num IN (47, 48, 49, 50, 51, 52), 1, 0) AS is_holiday_season,
  COALESCE(
    LEAST(SAFE_DIVIDE(w.w47_qty, GREATEST(n.avg_qty_non_w47, 1)), 20),
    1
  ) AS w47_historical_mult,
  IF(b.week_num <= 26, 'H1', 'H2') AS period
FROM base b
LEFT JOIN with_w47 w ON b.sku = w.sku
LEFT JOIN non_w47_avg n ON b.sku = n.sku;


-- 4.2 V2 XGBoost Model for Cyclical Patterns
CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.model_sku_xgboost_v2_cyclical`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['weekly_quantity'],
  num_parallel_tree = 150,
  max_tree_depth = 6,
  learn_rate = 0.08,
  min_split_loss = 0,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  weekly_quantity,
  lag1_quantity,
  lag2_quantity,
  lag4_quantity,
  rolling_avg_4w,
  avg_unit_price,
  week_num,
  order_count,
  week_of_month,
  is_month_end,
  is_quarter_end,
  is_w47,
  is_holiday_season,
  w47_historical_mult,
  cv
FROM `${PROJECT_ID}.${DATASET_ID}.features_sku_v2`
WHERE period = 'H1'
  AND pattern = 'cyclical'
  AND data_sufficiency IN ('full', 'marginal')
  AND lag4_quantity IS NOT NULL;


-- 4.3 V2 Category XGBoost Model (with seasonality)
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.features_category_v2` AS
WITH base AS (
  SELECT
    *,
    CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
    LAG(weekly_quantity, 1) OVER (PARTITION BY category ORDER BY year_week) AS lag1_quantity,
    LAG(weekly_quantity, 2) OVER (PARTITION BY category ORDER BY year_week) AS lag2_quantity,
    LAG(weekly_quantity, 4) OVER (PARTITION BY category ORDER BY year_week) AS lag4_quantity,
    AVG(weekly_quantity) OVER (
      PARTITION BY category ORDER BY year_week ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
    ) AS rolling_avg_4w
  FROM `${PROJECT_ID}.${DATASET_ID}.features_category`
),
with_w47 AS (
  SELECT category, weekly_quantity AS w47_qty
  FROM base WHERE week_num = 47
),
non_w47_avg AS (
  SELECT category, AVG(weekly_quantity) AS avg_qty FROM base WHERE week_num != 47 GROUP BY category
)
SELECT
  b.*,
  MOD(b.week_num - 1, 4) + 1 AS week_of_month,
  IF(b.week_num = 47, 1, 0) AS is_w47,
  IF(b.week_num IN (47, 48, 49, 50, 51, 52), 1, 0) AS is_holiday_season,
  COALESCE(LEAST(SAFE_DIVIDE(w.w47_qty, GREATEST(n.avg_qty, 1)), 20), 1) AS w47_historical_mult,
  IF(b.week_num <= 26, 'H1', 'H2') AS period
FROM base b
LEFT JOIN with_w47 w ON b.category = w.category
LEFT JOIN non_w47_avg n ON b.category = n.category;

CREATE OR REPLACE MODEL `${PROJECT_ID}.${DATASET_ID}.model_category_xgboost_v2`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['weekly_quantity'],
  num_parallel_tree = 150,
  max_tree_depth = 5,
  learn_rate = 0.08,
  data_split_method = 'NO_SPLIT'
) AS
SELECT
  weekly_quantity,
  lag1_quantity,
  lag2_quantity,
  lag4_quantity,
  rolling_avg_4w,
  week_num,
  week_of_month,
  is_w47,
  is_holiday_season,
  w47_historical_mult
FROM `${PROJECT_ID}.${DATASET_ID}.features_category_v2`
WHERE period = 'H1'
  AND lag4_quantity IS NOT NULL;


-- ============================================================================
-- PART 5: GENERATE PREDICTIONS (V1 and V2)
-- ============================================================================

-- 5.1 V1 SKU Predictions
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_sku_v1` AS
SELECT
  t.sku,
  t.year_week,
  t.weekly_quantity AS actual,
  p.predicted_weekly_quantity AS predicted,
  ABS(t.weekly_quantity - p.predicted_weekly_quantity) AS abs_error,
  SAFE_DIVIDE(ABS(t.weekly_quantity - p.predicted_weekly_quantity), t.weekly_quantity) * 100 AS pct_error,
  'V1_baseline' AS model_version
FROM `${PROJECT_ID}.${DATASET_ID}.features_weekly` t
JOIN ML.PREDICT(
  MODEL `${PROJECT_ID}.${DATASET_ID}.model_sku_xgboost_v1`,
  (SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.features_weekly`
   WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) >= 27
     AND lag4_quantity IS NOT NULL)
) p ON t.sku = p.sku AND t.year_week = p.year_week
WHERE CAST(REGEXP_EXTRACT(t.year_week, r'W(\d+)') AS INT64) >= 27;

-- 5.2 V2 SKU Predictions (Cyclical only)
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_sku_v2_cyclical` AS
SELECT
  t.sku,
  t.year_week,
  t.weekly_quantity AS actual,
  GREATEST(p.predicted_weekly_quantity, 0) AS predicted,  -- No negative predictions
  ABS(t.weekly_quantity - GREATEST(p.predicted_weekly_quantity, 0)) AS abs_error,
  SAFE_DIVIDE(
    ABS(t.weekly_quantity - GREATEST(p.predicted_weekly_quantity, 0)),
    t.weekly_quantity
  ) * 100 AS pct_error,
  'V2_cyclical' AS model_version,
  t.pattern,
  t.data_sufficiency
FROM `${PROJECT_ID}.${DATASET_ID}.features_sku_v2` t
JOIN ML.PREDICT(
  MODEL `${PROJECT_ID}.${DATASET_ID}.model_sku_xgboost_v2_cyclical`,
  (SELECT * FROM `${PROJECT_ID}.${DATASET_ID}.features_sku_v2`
   WHERE period = 'H2'
     AND pattern = 'cyclical'
     AND data_sufficiency IN ('full', 'marginal')
     AND lag4_quantity IS NOT NULL)
) p ON t.sku = p.sku AND t.year_week = p.year_week
WHERE t.period = 'H2' AND t.pattern = 'cyclical';

-- 5.3 Combined Hybrid Predictions (V2 for cyclical, V1 for others)
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.predictions_sku_hybrid` AS
-- V2 cyclical predictions
SELECT
  sku, year_week, actual, predicted, pct_error, model_version, pattern, data_sufficiency
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku_v2_cyclical`

UNION ALL

-- V1 predictions for non-cyclical
SELECT
  v1.sku,
  v1.year_week,
  v1.actual,
  v1.predicted,
  v1.pct_error,
  'V1_baseline' AS model_version,
  p.pattern,
  p.data_sufficiency
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku_v1` v1
JOIN `${PROJECT_ID}.${DATASET_ID}.sku_pattern_classification` p ON v1.sku = p.sku
WHERE p.pattern != 'cyclical';


-- ============================================================================
-- PART 6: MODEL EVALUATION COMPARISON
-- ============================================================================

-- 6.1 V1 vs V2 Comparison
CREATE OR REPLACE TABLE `${PROJECT_ID}.${DATASET_ID}.model_comparison` AS
SELECT
  'V1_baseline' AS model,
  'SKU' AS level,
  COUNT(*) AS predictions,
  COUNT(DISTINCT sku) AS entities,
  AVG(pct_error) AS mean_mape,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS median_mape
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku_v1`
WHERE actual > 0

UNION ALL

SELECT
  'V2_cyclical' AS model,
  'SKU' AS level,
  COUNT(*) AS predictions,
  COUNT(DISTINCT sku) AS entities,
  AVG(pct_error) AS mean_mape,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS median_mape
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku_v2_cyclical`
WHERE actual > 0

UNION ALL

SELECT
  'Hybrid' AS model,
  'SKU' AS level,
  COUNT(*) AS predictions,
  COUNT(DISTINCT sku) AS entities,
  AVG(pct_error) AS mean_mape,
  APPROX_QUANTILES(pct_error, 100)[OFFSET(50)] AS median_mape
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku_hybrid`
WHERE actual > 0;

-- View comparison
SELECT
  model,
  level,
  entities,
  predictions,
  ROUND(median_mape, 1) AS mape_pct
FROM `${PROJECT_ID}.${DATASET_ID}.model_comparison`
ORDER BY level, model;


-- ============================================================================
-- PART 7: DASHBOARD SUPPORT VIEWS
-- ============================================================================

-- 7.1 SKU predictions with metadata for dashboard
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.dashboard_sku_predictions` AS
SELECT
  p.sku,
  p.year_week,
  p.actual,
  p.predicted,
  p.pct_error,
  p.model_version,
  c.pattern,
  c.data_sufficiency,
  CASE
    WHEN c.data_sufficiency = 'insufficient' THEN 'none'
    WHEN c.pattern = 'cyclical' THEN 'high'
    WHEN c.pattern = 'bulk_oneoff' THEN 'medium'
    ELSE 'low'
  END AS confidence_level,
  CASE
    WHEN c.data_sufficiency = 'insufficient' THEN 'Insufficient data for prediction'
    WHEN c.data_sufficiency = 'marginal' THEN 'Limited historical data'
    WHEN c.pattern = 'bulk_oneoff' THEN 'Irregular purchasing pattern'
    ELSE NULL
  END AS disclaimer
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku_hybrid` p
JOIN `${PROJECT_ID}.${DATASET_ID}.sku_pattern_classification` c ON p.sku = c.sku;

-- 7.2 MAPE summary by SKU for dashboard
CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET_ID}.dashboard_sku_mape` AS
SELECT
  p.sku,
  c.pattern,
  c.data_sufficiency,
  p.model_version,
  APPROX_QUANTILES(p.pct_error, 100)[OFFSET(50)] AS mape,
  SUM(p.actual) AS total_volume,
  COUNT(*) AS n_weeks
FROM `${PROJECT_ID}.${DATASET_ID}.predictions_sku_hybrid` p
JOIN `${PROJECT_ID}.${DATASET_ID}.sku_pattern_classification` c ON p.sku = c.sku
WHERE p.actual > 0
GROUP BY p.sku, c.pattern, c.data_sufficiency, p.model_version;
