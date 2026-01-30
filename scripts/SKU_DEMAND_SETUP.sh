#!/bin/bash
# =============================================================================
# SKU_DEMAND_SETUP.sh - Deploy sku_demand_0 and cat_demand_0 models
# =============================================================================
# This script:
# 1. Renames existing tables to rev0_ prefix
# 2. Uploads SKU-level data to GCS
# 3. Creates BigQuery tables with sku0_ and cat0_ prefixes
# 4. Creates forecasting models
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
BUCKET_NAME="demand_planning_aca"
DATASET_NAME="demand_forecasting"
FEATURES_SKU_PATH="/sessions/affectionate-pensive-goodall/mnt/demand planning/features_sku"
FEATURES_CAT_PATH="/sessions/affectionate-pensive-goodall/mnt/demand planning/features_category"
FEATURES_REV_PATH="/sessions/affectionate-pensive-goodall/mnt/demand planning/features"

echo "=============================================="
echo "SKU DEMAND MODEL SETUP"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Dataset: $DATASET_NAME"
echo ""

# =============================================================================
# STEP 1: Rename existing tables to rev0_ prefix
# =============================================================================
echo "=============================================="
echo "STEP 1: Restructuring existing tables to rev0_"
echo "=============================================="

# Function to rename table (copy + delete)
rename_table() {
    local old_name=$1
    local new_name=$2

    # Check if old table exists
    if bq show "${PROJECT_ID}:${DATASET_NAME}.${old_name}" > /dev/null 2>&1; then
        echo "  Renaming $old_name -> $new_name"
        bq cp -f "${PROJECT_ID}:${DATASET_NAME}.${old_name}" "${PROJECT_ID}:${DATASET_NAME}.${new_name}" 2>/dev/null || true
        bq rm -f -t "${PROJECT_ID}:${DATASET_NAME}.${old_name}" 2>/dev/null || true
    else
        echo "  Skipping $old_name (doesn't exist)"
    fi
}

# Rename old tables
rename_table "fact_transactions" "rev0_fact_transactions"
rename_table "features_weekly_regional" "rev0_features_weekly"
rename_table "features_weekly_total" "rev0_features_weekly_total"
rename_table "features_customers" "rev0_features_customers"
rename_table "dim_products" "rev0_dim_products"
rename_table "dim_customers" "rev0_dim_customers"

# Rename old models
echo "  Renaming models..."
bq query --use_legacy_sql=false "DROP MODEL IF EXISTS \`${PROJECT_ID}.${DATASET_NAME}.model_arima\`" 2>/dev/null || true
bq query --use_legacy_sql=false "DROP MODEL IF EXISTS \`${PROJECT_ID}.${DATASET_NAME}.model_xgboost\`" 2>/dev/null || true

echo ""

# =============================================================================
# STEP 2: Upload SKU data to GCS
# =============================================================================
echo "=============================================="
echo "STEP 2: Uploading SKU data to GCS"
echo "=============================================="

# Create folders in bucket
gsutil -m cp "${FEATURES_SKU_PATH}/"*.csv "gs://${BUCKET_NAME}/sku_demand_0/"
gsutil -m cp "${FEATURES_CAT_PATH}/"*.csv "gs://${BUCKET_NAME}/cat_demand_0/"

echo "  ✓ Uploaded SKU feature files"
echo "  ✓ Uploaded Category feature files"
echo ""

# =============================================================================
# STEP 3: Create BigQuery tables
# =============================================================================
echo "=============================================="
echo "STEP 3: Creating BigQuery tables"
echo "=============================================="

# SKU Line Item Fact Table
echo "Creating sku0_fact_lineitem..."
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_fact_lineitem" \
  "gs://${BUCKET_NAME}/sku_demand_0/sku0_fact_lineitem.csv"

# SKU Weekly Features
echo "Creating sku0_features_weekly..."
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_features_weekly" \
  "gs://${BUCKET_NAME}/sku_demand_0/sku0_features_weekly.csv"

# SKU Products Dimension
echo "Creating sku0_dim_products..."
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.sku0_dim_products" \
  "gs://${BUCKET_NAME}/sku_demand_0/sku0_dim_products.csv"

# Category Weekly Features
echo "Creating cat0_features_weekly..."
bq load --autodetect --replace \
  --source_format=CSV \
  "${PROJECT_ID}:${DATASET_NAME}.cat0_features_weekly" \
  "gs://${BUCKET_NAME}/cat_demand_0/cat0_features_weekly.csv"

echo "  ✓ All tables created"
echo ""

# =============================================================================
# STEP 4: Create rev0 models (if features exist)
# =============================================================================
echo "=============================================="
echo "STEP 4: Creating rev0 models"
echo "=============================================="

# Check if rev0 features table exists
if bq show "${PROJECT_ID}:${DATASET_NAME}.rev0_features_weekly" > /dev/null 2>&1; then
    echo "Creating rev0_model_arima..."
    bq query --use_legacy_sql=false "
    CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.rev0_model_arima\`
    OPTIONS(
      model_type='ARIMA_PLUS',
      time_series_timestamp_col='year_week',
      time_series_data_col='weekly_revenue',
      time_series_id_col='region_name',
      auto_arima=TRUE,
      holiday_region='ZA'
    ) AS
    SELECT year_week, region_name, weekly_revenue
    FROM \`${PROJECT_ID}.${DATASET_NAME}.rev0_features_weekly\`
    ORDER BY year_week
    "

    echo "Creating rev0_model_xgboost..."
    bq query --use_legacy_sql=false "
    CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.rev0_model_xgboost\`
    OPTIONS(
      model_type='BOOSTED_TREE_REGRESSOR',
      input_label_cols=['weekly_revenue'],
      max_iterations=50,
      learn_rate=0.1
    ) AS
    SELECT
      weekly_revenue, weekly_revenue_lag_1w, weekly_revenue_lag_2w, weekly_revenue_lag_4w,
      weekly_revenue_ma_4w, week_of_year, month, quarter,
      region_name, revenue_share, transaction_count, unique_customers
    FROM \`${PROJECT_ID}.${DATASET_NAME}.rev0_features_weekly\`
    WHERE weekly_revenue_lag_4w IS NOT NULL
    "
else
    echo "  Skipping rev0 models (features table not found)"
fi

echo ""

# =============================================================================
# STEP 5: Create SKU demand models
# =============================================================================
echo "=============================================="
echo "STEP 5: Creating sku_demand_0 models"
echo "=============================================="

# SKU-level XGBoost model (predicts weekly_quantity)
echo "Creating sku0_model_xgboost..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.sku0_model_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1,
  l1_reg=0.1,
  l2_reg=0.1
) AS
SELECT
  weekly_quantity,
  avg_price,
  quantity_lag_1w,
  quantity_lag_2w,
  quantity_lag_4w,
  quantity_ma_4w,
  quantity_std_4w,
  quantity_diff_1w,
  price_change,
  week_of_year,
  month,
  quarter,
  transaction_count,
  unique_customers,
  primary_region,
  category,
  brand
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
WHERE quantity_lag_4w IS NOT NULL
"

# SKU-level ARIMA+ for top SKUs
echo "Creating sku0_model_arima (top 50 SKUs by volume)..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.sku0_model_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='year_week',
  time_series_data_col='weekly_quantity',
  time_series_id_col='sku',
  auto_arima=TRUE
) AS
SELECT year_week, sku, weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
WHERE sku IN (
  SELECT sku FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
  GROUP BY sku
  ORDER BY SUM(weekly_quantity) DESC
  LIMIT 50
)
ORDER BY year_week
"

echo ""

# =============================================================================
# STEP 6: Create Category demand models
# =============================================================================
echo "=============================================="
echo "STEP 6: Creating cat_demand_0 models"
echo "=============================================="

echo "Creating cat0_model_xgboost..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.cat0_model_xgboost\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=50,
  learn_rate=0.1
) AS
SELECT
  weekly_quantity,
  weekly_revenue,
  active_skus,
  transaction_count,
  unique_customers,
  week_of_year,
  month,
  quarter,
  quantity_lag_1w,
  quantity_lag_2w,
  quantity_lag_4w,
  quantity_ma_4w,
  category
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
WHERE quantity_lag_4w IS NOT NULL
"

echo "Creating cat0_model_arima..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.cat0_model_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='year_week',
  time_series_data_col='weekly_quantity',
  time_series_id_col='category',
  auto_arima=TRUE
) AS
SELECT year_week, category, weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
ORDER BY year_week
"

echo ""

# =============================================================================
# STEP 7: Verify structure
# =============================================================================
echo "=============================================="
echo "STEP 7: Verifying BigQuery structure"
echo "=============================================="

bq query --use_legacy_sql=false "
SELECT
  table_name,
  table_type,
  TIMESTAMP_MILLIS(creation_time) as created
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.TABLES\`
ORDER BY
  CASE
    WHEN table_name LIKE 'rev0%' THEN 1
    WHEN table_name LIKE 'sku0%' THEN 2
    WHEN table_name LIKE 'cat0%' THEN 3
    ELSE 4
  END,
  table_name
"

echo ""
echo "=============================================="
echo "SETUP COMPLETE!"
echo "=============================================="
echo ""
echo "Model Registry:"
echo "  rev0  - Revenue Forecast (regional weekly revenue)"
echo "  sku0  - SKU Demand Forecast (SKU weekly quantity)"
echo "  cat0  - Category Demand Forecast (category weekly quantity)"
echo ""
echo "Tables created:"
echo "  - rev0_* (6 tables + 2 models)"
echo "  - sku0_* (3 tables + 2 models)"
echo "  - cat0_* (1 table + 2 models)"
echo ""
echo "To generate forecasts, run:"
echo ""
echo "  -- SKU-level forecast (top 50 SKUs, 4 weeks ahead)"
echo "  SELECT * FROM ML.FORECAST(MODEL \`${DATASET_NAME}.sku0_model_arima\`, STRUCT(4 AS horizon));"
echo ""
echo "  -- Category-level forecast (all categories, 4 weeks ahead)"
echo "  SELECT * FROM ML.FORECAST(MODEL \`${DATASET_NAME}.cat0_model_arima\`, STRUCT(4 AS horizon));"
echo ""
echo "  -- What-if: Price reduction impact"
echo "  SELECT * FROM ML.PREDICT(MODEL \`${DATASET_NAME}.sku0_model_xgboost\`,"
echo "    (SELECT *, avg_price * 0.9 AS avg_price FROM \`${DATASET_NAME}.sku0_features_weekly\` WHERE sku = '10032'));"
echo ""
