# BigQuery Setup Guide
## Step-by-Step: Data Loading & Model Training

---

# PREREQUISITES

1. **Google Cloud Project** with BigQuery enabled
2. **BigQuery ML** enabled (it's on by default)
3. **gcloud CLI** installed and authenticated
4. **Service account** with BigQuery Admin role (or Data Editor + Job User)

```bash
# Authenticate (if not already)
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
```

---

# STEP 1: CREATE DATASET

```bash
# Create a dataset for the demand forecasting project
bq mk --dataset \
    --description "RedAI Demand Forecasting" \
    --location US \
    YOUR_PROJECT_ID:demand_forecasting
```

Or in the BigQuery Console:
1. Go to BigQuery Console
2. Click your project name
3. Click "CREATE DATASET"
4. Name: `demand_forecasting`
5. Location: Choose your region
6. Click "Create Dataset"

---

# STEP 2: UPLOAD CSV FILES TO GCS

First, upload the feature files to Google Cloud Storage:

```bash
# Create a bucket (if you don't have one)
gsutil mb -l US gs://YOUR_BUCKET_NAME

# Upload all feature files
gsutil cp features/*.csv gs://YOUR_BUCKET_NAME/demand_forecasting/
```

This uploads:
- `fact_transactions.csv`
- `features_weekly_regional.csv`
- `features_weekly_total.csv`
- `features_customers.csv`
- `features_daily.csv`
- `dim_products.csv`
- `dim_customers.csv`

---

# STEP 3: CREATE TABLES & LOAD DATA

## Option A: Using bq command line

```bash
# 1. Load fact_transactions
bq load \
    --source_format=CSV \
    --autodetect \
    --skip_leading_rows=1 \
    demand_forecasting.fact_transactions \
    gs://YOUR_BUCKET_NAME/demand_forecasting/fact_transactions.csv

# 2. Load features_weekly_regional (main ML training table)
bq load \
    --source_format=CSV \
    --autodetect \
    --skip_leading_rows=1 \
    demand_forecasting.features_weekly_regional \
    gs://YOUR_BUCKET_NAME/demand_forecasting/features_weekly_regional.csv

# 3. Load features_weekly_total
bq load \
    --source_format=CSV \
    --autodetect \
    --skip_leading_rows=1 \
    demand_forecasting.features_weekly_total \
    gs://YOUR_BUCKET_NAME/demand_forecasting/features_weekly_total.csv

# 4. Load features_customers
bq load \
    --source_format=CSV \
    --autodetect \
    --skip_leading_rows=1 \
    demand_forecasting.features_customers \
    gs://YOUR_BUCKET_NAME/demand_forecasting/features_customers.csv

# 5. Load dim_products
bq load \
    --source_format=CSV \
    --autodetect \
    --skip_leading_rows=1 \
    demand_forecasting.dim_products \
    gs://YOUR_BUCKET_NAME/demand_forecasting/dim_products.csv

# 6. Load dim_customers
bq load \
    --source_format=CSV \
    --autodetect \
    --skip_leading_rows=1 \
    demand_forecasting.dim_customers \
    gs://YOUR_BUCKET_NAME/demand_forecasting/dim_customers.csv
```

## Option B: Using BigQuery Console UI

1. Go to BigQuery Console
2. Click on your dataset `demand_forecasting`
3. Click "CREATE TABLE"
4. Source: Google Cloud Storage
5. Browse to your CSV file
6. File format: CSV
7. Table name: (e.g., `features_weekly_regional`)
8. Schema: Check "Auto detect"
9. Advanced options: Skip header rows = 1
10. Click "Create Table"

Repeat for each CSV file.

---

# STEP 4: VERIFY DATA LOADED

```sql
-- Check row counts
SELECT 'fact_transactions' as table_name, COUNT(*) as rows FROM `demand_forecasting.fact_transactions`
UNION ALL
SELECT 'features_weekly_regional', COUNT(*) FROM `demand_forecasting.features_weekly_regional`
UNION ALL
SELECT 'features_weekly_total', COUNT(*) FROM `demand_forecasting.features_weekly_total`
UNION ALL
SELECT 'features_customers', COUNT(*) FROM `demand_forecasting.features_customers`
UNION ALL
SELECT 'dim_products', COUNT(*) FROM `demand_forecasting.dim_products`
UNION ALL
SELECT 'dim_customers', COUNT(*) FROM `demand_forecasting.dim_customers`;
```

Expected output:
| table_name | rows |
|------------|------|
| fact_transactions | 8,691 |
| features_weekly_regional | 141 |
| features_weekly_total | 51 |
| features_customers | 188 |
| dim_products | 1,350 |
| dim_customers | 1,407 |

```sql
-- Preview the main training table
SELECT *
FROM `demand_forecasting.features_weekly_regional`
ORDER BY week_start DESC
LIMIT 10;
```

---

# STEP 5: CREATE ARIMA+ MODEL

This is the time-series forecasting model:

```sql
-- Create ARIMA+ model for each region
CREATE OR REPLACE MODEL `demand_forecasting.model_arima`
OPTIONS(
    model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'week_start',
    time_series_data_col = 'weekly_revenue',
    time_series_id_col = 'region_name',
    auto_arima = TRUE,
    data_frequency = 'WEEKLY',
    clean_spikes_and_dips = TRUE,
    adjust_step_changes = TRUE,
    decompose_time_series = TRUE
) AS
SELECT
    CAST(week_start AS TIMESTAMP) AS week_start,
    region_name,
    weekly_revenue
FROM `demand_forecasting.features_weekly_regional`
WHERE weekly_revenue IS NOT NULL
ORDER BY week_start;
```

**Wait ~2-5 minutes** for training to complete.

Check training status:
```sql
SELECT * FROM ML.TRAINING_INFO(MODEL `demand_forecasting.model_arima`);
```

---

# STEP 6: CREATE XGBOOST MODEL

This is the feature-based regression model:

```sql
-- Create XGBoost model with lag features
CREATE OR REPLACE MODEL `demand_forecasting.model_xgboost`
OPTIONS(
    model_type = 'BOOSTED_TREE_REGRESSOR',
    input_label_cols = ['weekly_revenue'],
    max_iterations = 50,
    learn_rate = 0.1,
    max_tree_depth = 6,
    subsample = 0.8,
    min_split_loss = 0,
    data_split_method = 'SEQ',
    data_split_eval_fraction = 0.15
) AS
SELECT
    -- Target
    weekly_revenue,

    -- Temporal features
    week_of_year,
    month,
    quarter,

    -- Lag features (most important)
    weekly_revenue_lag_1w,
    weekly_revenue_lag_2w,
    weekly_revenue_lag_4w,

    -- Rolling statistics
    weekly_revenue_ma_4w,
    weekly_revenue_std_4w,

    -- Trend features
    weekly_revenue_diff_1w,
    weekly_revenue_pct_change_4w,

    -- Regional features
    region_name,
    revenue_share,
    transaction_count,
    unique_customers

FROM `demand_forecasting.features_weekly_regional`
WHERE weekly_revenue_lag_4w IS NOT NULL;  -- Need 4 weeks of history
```

**Wait ~3-5 minutes** for training.

---

# STEP 7: CREATE BASELINE LINEAR MODEL

```sql
-- Simple linear regression for comparison
CREATE OR REPLACE MODEL `demand_forecasting.model_linear`
OPTIONS(
    model_type = 'LINEAR_REG',
    input_label_cols = ['weekly_revenue'],
    optimize_strategy = 'NORMAL_EQUATION',
    l2_reg = 0.1
) AS
SELECT
    weekly_revenue,
    weekly_revenue_lag_1w,
    weekly_revenue_lag_4w,
    weekly_revenue_ma_4w,
    month,
    region_name
FROM `demand_forecasting.features_weekly_regional`
WHERE weekly_revenue_lag_4w IS NOT NULL;
```

---

# STEP 8: GENERATE FORECASTS

## 8.1 ARIMA+ Forecasts (8 weeks ahead)

```sql
-- Generate 8-week forecast with confidence intervals
SELECT
    forecast_timestamp,
    time_series_id AS region_name,
    ROUND(forecast_value, 0) AS forecast_revenue,
    ROUND(prediction_interval_lower_bound, 0) AS lower_bound,
    ROUND(prediction_interval_upper_bound, 0) AS upper_bound,
    ROUND(standard_error, 0) AS std_error
FROM ML.FORECAST(
    MODEL `demand_forecasting.model_arima`,
    STRUCT(8 AS horizon, 0.95 AS confidence_level)
)
ORDER BY time_series_id, forecast_timestamp;
```

## 8.2 XGBoost Predictions (for historical evaluation)

```sql
-- Predict on the most recent data
SELECT
    week_start,
    region_name,
    weekly_revenue AS actual,
    ROUND(predicted_weekly_revenue, 0) AS predicted,
    ROUND(ABS(weekly_revenue - predicted_weekly_revenue), 0) AS abs_error,
    ROUND(ABS(weekly_revenue - predicted_weekly_revenue) / weekly_revenue * 100, 1) AS pct_error
FROM ML.PREDICT(
    MODEL `demand_forecasting.model_xgboost`,
    (SELECT * FROM `demand_forecasting.features_weekly_regional`
     WHERE weekly_revenue_lag_4w IS NOT NULL)
)
ORDER BY week_start DESC
LIMIT 20;
```

---

# STEP 9: EVALUATE MODELS

## 9.1 ARIMA+ Model Evaluation

```sql
-- Get model coefficients and fit statistics
SELECT *
FROM ML.ARIMA_COEFFICIENTS(MODEL `demand_forecasting.model_arima`);

-- Get evaluation metrics
SELECT *
FROM ML.ARIMA_EVALUATE(MODEL `demand_forecasting.model_arima`);
```

## 9.2 XGBoost Model Evaluation

```sql
-- Get training metrics
SELECT *
FROM ML.EVALUATE(MODEL `demand_forecasting.model_xgboost`);

-- Get feature importance
SELECT *
FROM ML.FEATURE_IMPORTANCE(MODEL `demand_forecasting.model_xgboost`)
ORDER BY importance_weight DESC;
```

## 9.3 Compare All Models

```sql
-- Full model comparison on holdout data
WITH holdout AS (
    SELECT *
    FROM `demand_forecasting.features_weekly_regional`
    WHERE week_start >= '2025-11-01'
      AND weekly_revenue_lag_4w IS NOT NULL
),

arima_preds AS (
    SELECT
        forecast_timestamp AS week_start,
        time_series_id AS region_name,
        forecast_value AS pred_arima
    FROM ML.FORECAST(MODEL `demand_forecasting.model_arima`, STRUCT(8 AS horizon))
),

xgboost_preds AS (
    SELECT
        week_start,
        region_name,
        predicted_weekly_revenue AS pred_xgboost
    FROM ML.PREDICT(MODEL `demand_forecasting.model_xgboost`, (SELECT * FROM holdout))
),

linear_preds AS (
    SELECT
        week_start,
        region_name,
        predicted_weekly_revenue AS pred_linear
    FROM ML.PREDICT(MODEL `demand_forecasting.model_linear`, (SELECT * FROM holdout))
)

SELECT
    'ARIMA+' AS model,
    ROUND(AVG(ABS(h.weekly_revenue - a.pred_arima)), 0) AS mae,
    ROUND(SQRT(AVG(POW(h.weekly_revenue - a.pred_arima, 2))), 0) AS rmse,
    ROUND(SUM(ABS(h.weekly_revenue - a.pred_arima)) / SUM(h.weekly_revenue) * 100, 1) AS wmape
FROM holdout h
LEFT JOIN arima_preds a ON CAST(h.week_start AS TIMESTAMP) = a.week_start AND h.region_name = a.region_name
WHERE a.pred_arima IS NOT NULL

UNION ALL

SELECT
    'XGBoost',
    ROUND(AVG(ABS(h.weekly_revenue - x.pred_xgboost)), 0),
    ROUND(SQRT(AVG(POW(h.weekly_revenue - x.pred_xgboost, 2))), 0),
    ROUND(SUM(ABS(h.weekly_revenue - x.pred_xgboost)) / SUM(h.weekly_revenue) * 100, 1)
FROM holdout h
JOIN xgboost_preds x USING (week_start, region_name)

UNION ALL

SELECT
    'Linear',
    ROUND(AVG(ABS(h.weekly_revenue - l.pred_linear)), 0),
    ROUND(SQRT(AVG(POW(h.weekly_revenue - l.pred_linear, 2))), 0),
    ROUND(SUM(ABS(h.weekly_revenue - l.pred_linear)) / SUM(h.weekly_revenue) * 100, 1)
FROM holdout h
JOIN linear_preds l USING (week_start, region_name);
```

---

# STEP 10: SAVE FORECASTS TO TABLE

```sql
-- Create a forecasts table for downstream use
CREATE OR REPLACE TABLE `demand_forecasting.forecasts_output` AS
SELECT
    CURRENT_TIMESTAMP() AS generated_at,
    'model_arima' AS model_name,
    forecast_timestamp AS week_start,
    time_series_id AS region_name,
    ROUND(forecast_value, 0) AS revenue_forecast,
    ROUND(prediction_interval_lower_bound, 0) AS revenue_lower_95,
    ROUND(prediction_interval_upper_bound, 0) AS revenue_upper_95,
    ROUND(standard_error / forecast_value, 4) AS uncertainty_ratio
FROM ML.FORECAST(
    MODEL `demand_forecasting.model_arima`,
    STRUCT(8 AS horizon, 0.95 AS confidence_level)
);

-- Verify
SELECT * FROM `demand_forecasting.forecasts_output` ORDER BY region_name, week_start;
```

---

# QUICK REFERENCE

## All Commands in Order

```bash
# 1. Create dataset
bq mk --dataset YOUR_PROJECT_ID:demand_forecasting

# 2. Upload to GCS
gsutil cp features/*.csv gs://YOUR_BUCKET/demand_forecasting/

# 3. Load tables
bq load --autodetect --skip_leading_rows=1 demand_forecasting.features_weekly_regional gs://YOUR_BUCKET/demand_forecasting/features_weekly_regional.csv
```

```sql
-- 4. Create ARIMA model
CREATE OR REPLACE MODEL demand_forecasting.model_arima ...

-- 5. Create XGBoost model
CREATE OR REPLACE MODEL demand_forecasting.model_xgboost ...

-- 6. Generate forecasts
SELECT * FROM ML.FORECAST(MODEL demand_forecasting.model_arima, STRUCT(8 AS horizon));

-- 7. Evaluate
SELECT * FROM ML.EVALUATE(MODEL demand_forecasting.model_xgboost);
```

## Estimated Costs

| Operation | Cost |
|-----------|------|
| Storage (all tables) | ~$0.02/month |
| ARIMA training | ~$5 per training |
| XGBoost training | ~$10 per training |
| Forecasting queries | ~$0.01 per query |

---

# TROUBLESHOOTING

| Error | Solution |
|-------|----------|
| "Table not found" | Check dataset name and table name |
| "Invalid timestamp" | Cast week_start: `CAST(week_start AS TIMESTAMP)` |
| "NULL values in label" | Add `WHERE weekly_revenue IS NOT NULL` |
| "Not enough training data" | Need at least 2 full cycles (8+ weeks) |
| "Model training failed" | Check for NaN/Inf values in features |

---

*Created: 2026-01-20*
