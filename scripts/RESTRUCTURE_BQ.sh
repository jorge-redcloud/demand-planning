#!/bin/bash
# =============================================================================
# RESTRUCTURE_BQ.sh - Rename existing tables to rev0_ prefix
# =============================================================================
# This script renames the existing tables in BigQuery to follow our naming
# convention: rev0_ prefix for the Revenue Forecast v0 model.
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"

echo "=============================================="
echo "BigQuery Table Restructuring"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_NAME"
echo ""

# Function to copy table with new name
copy_table() {
    local old_name=$1
    local new_name=$2
    echo "Copying $old_name -> $new_name..."
    bq cp -f "${PROJECT_ID}:${DATASET_NAME}.${old_name}" "${PROJECT_ID}:${DATASET_NAME}.${new_name}"
}

# Function to delete old table
delete_table() {
    local table_name=$1
    echo "Deleting old table: $table_name..."
    bq rm -f -t "${PROJECT_ID}:${DATASET_NAME}.${table_name}"
}

echo "Step 1: Renaming fact and dimension tables..."
echo "----------------------------------------------"

# Rename tables (copy then delete old)
# fact_transactions -> rev0_fact_transactions
copy_table "fact_transactions" "rev0_fact_transactions"

# features_weekly_regional -> rev0_features_weekly
copy_table "features_weekly_regional" "rev0_features_weekly"

# features_weekly_total -> rev0_features_weekly_total
copy_table "features_weekly_total" "rev0_features_weekly_total"

# features_customers -> rev0_features_customers
copy_table "features_customers" "rev0_features_customers"

# dim_products -> rev0_dim_products
copy_table "dim_products" "rev0_dim_products"

# dim_customers -> rev0_dim_customers
copy_table "dim_customers" "rev0_dim_customers"

echo ""
echo "Step 2: Deleting old tables..."
echo "----------------------------------------------"

delete_table "fact_transactions"
delete_table "features_weekly_regional"
delete_table "features_weekly_total"
delete_table "features_customers"
delete_table "dim_products"
delete_table "dim_customers"

echo ""
echo "Step 3: Recreating models with rev0_ prefix..."
echo "----------------------------------------------"

# Drop old models if they exist
echo "Dropping old models..."
bq query --use_legacy_sql=false "DROP MODEL IF EXISTS \`${PROJECT_ID}.${DATASET_NAME}.model_arima\`"
bq query --use_legacy_sql=false "DROP MODEL IF EXISTS \`${PROJECT_ID}.${DATASET_NAME}.model_xgboost\`"

# Create ARIMA+ model with rev0_ prefix
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
SELECT
  year_week,
  region_name,
  weekly_revenue
FROM \`${PROJECT_ID}.${DATASET_NAME}.rev0_features_weekly\`
ORDER BY year_week
"

# Create XGBoost model with rev0_ prefix
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
  weekly_revenue,
  weekly_revenue_lag_1w,
  weekly_revenue_lag_2w,
  weekly_revenue_lag_4w,
  weekly_revenue_ma_4w,
  weekly_revenue_std_4w,
  weekly_revenue_diff_1w,
  week_of_year,
  month,
  quarter,
  region_name,
  revenue_share,
  transaction_count,
  unique_customers
FROM \`${PROJECT_ID}.${DATASET_NAME}.rev0_features_weekly\`
WHERE weekly_revenue_lag_4w IS NOT NULL
"

echo ""
echo "Step 4: Verifying new structure..."
echo "----------------------------------------------"

bq query --use_legacy_sql=false "
SELECT
  table_name,
  table_type,
  TIMESTAMP_MILLIS(creation_time) as created
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.TABLES\`
ORDER BY table_name
"

echo ""
echo "=============================================="
echo "RESTRUCTURE COMPLETE!"
echo "=============================================="
echo ""
echo "New table structure:"
echo "  - rev0_fact_transactions"
echo "  - rev0_features_weekly"
echo "  - rev0_features_weekly_total"
echo "  - rev0_features_customers"
echo "  - rev0_dim_products"
echo "  - rev0_dim_customers"
echo "  - rev0_model_arima"
echo "  - rev0_model_xgboost"
echo ""
