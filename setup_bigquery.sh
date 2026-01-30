#!/bin/bash
# =============================================================================
# RedAI Demand Forecasting - BigQuery Setup Script
# =============================================================================
# Usage: ./setup_bigquery.sh YOUR_PROJECT_ID YOUR_BUCKET_NAME
# =============================================================================

set -e  # Exit on error

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 PROJECT_ID BUCKET_NAME"
    echo "Example: $0 my-gcp-project my-data-bucket"
    exit 1
fi

PROJECT_ID=$1
BUCKET_NAME=$2
DATASET_NAME="demand_forecasting"
FEATURES_DIR="./features"

echo "=============================================="
echo "RedAI BigQuery Setup"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Bucket: $BUCKET_NAME"
echo "Dataset: $DATASET_NAME"
echo "=============================================="

# Step 1: Set project
echo ""
echo "[1/6] Setting GCP project..."
gcloud config set project $PROJECT_ID

# Step 2: Create dataset
echo ""
echo "[2/6] Creating BigQuery dataset..."
bq mk --dataset \
    --description "RedAI Demand Forecasting" \
    --location US \
    ${PROJECT_ID}:${DATASET_NAME} 2>/dev/null || echo "Dataset already exists"

# Step 3: Upload to GCS
echo ""
echo "[3/6] Uploading CSV files to GCS..."
gsutil -m cp ${FEATURES_DIR}/*.csv gs://${BUCKET_NAME}/demand_forecasting/

# Step 4: Load tables
echo ""
echo "[4/6] Loading tables into BigQuery..."

TABLES=(
    "fact_transactions"
    "features_weekly_regional"
    "features_weekly_total"
    "features_daily"
    "features_customers"
    "dim_products"
    "dim_customers"
)

for TABLE in "${TABLES[@]}"; do
    echo "  Loading ${TABLE}..."
    bq load \
        --source_format=CSV \
        --autodetect \
        --skip_leading_rows=1 \
        --replace \
        ${DATASET_NAME}.${TABLE} \
        gs://${BUCKET_NAME}/demand_forecasting/${TABLE}.csv
done

# Step 5: Verify load
echo ""
echo "[5/6] Verifying data load..."
bq query --use_legacy_sql=false "
SELECT 'fact_transactions' as tbl, COUNT(*) as rows FROM \`${PROJECT_ID}.${DATASET_NAME}.fact_transactions\`
UNION ALL SELECT 'features_weekly_regional', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.features_weekly_regional\`
UNION ALL SELECT 'features_customers', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.features_customers\`
UNION ALL SELECT 'dim_products', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.dim_products\`
"

# Step 6: Create models
echo ""
echo "[6/6] Creating ML models..."

echo "  Creating ARIMA+ model (this may take 2-5 minutes)..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_arima\`
OPTIONS(
    model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'week_start',
    time_series_data_col = 'weekly_revenue',
    time_series_id_col = 'region_name',
    auto_arima = TRUE,
    data_frequency = 'WEEKLY',
    clean_spikes_and_dips = TRUE
) AS
SELECT
    CAST(week_start AS TIMESTAMP) AS week_start,
    region_name,
    weekly_revenue
FROM \`${PROJECT_ID}.${DATASET_NAME}.features_weekly_regional\`
WHERE weekly_revenue IS NOT NULL
ORDER BY week_start;
"

echo "  Creating XGBoost model (this may take 3-5 minutes)..."
bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_xgboost\`
OPTIONS(
    model_type = 'BOOSTED_TREE_REGRESSOR',
    input_label_cols = ['weekly_revenue'],
    max_iterations = 50,
    learn_rate = 0.1,
    max_tree_depth = 6
) AS
SELECT
    weekly_revenue,
    week_of_year, month, quarter,
    weekly_revenue_lag_1w, weekly_revenue_lag_2w, weekly_revenue_lag_4w,
    weekly_revenue_ma_4w, weekly_revenue_std_4w,
    weekly_revenue_diff_1w,
    region_name, revenue_share, transaction_count
FROM \`${PROJECT_ID}.${DATASET_NAME}.features_weekly_regional\`
WHERE weekly_revenue_lag_4w IS NOT NULL;
"

echo ""
echo "=============================================="
echo "SETUP COMPLETE!"
echo "=============================================="
echo ""
echo "Next steps:"
echo "1. Generate forecasts:"
echo "   bq query --use_legacy_sql=false 'SELECT * FROM ML.FORECAST(MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_arima\`, STRUCT(8 AS horizon))'"
echo ""
echo "2. Evaluate models:"
echo "   bq query --use_legacy_sql=false 'SELECT * FROM ML.EVALUATE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.model_xgboost\`)'"
echo ""
echo "3. View in BigQuery Console:"
echo "   https://console.cloud.google.com/bigquery?project=${PROJECT_ID}"
echo ""
