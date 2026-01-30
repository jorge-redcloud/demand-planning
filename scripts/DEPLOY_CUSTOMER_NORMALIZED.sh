#!/bin/bash
# ============================================================================
# DEPLOY NORMALIZED CUSTOMER DATA TO BIGQUERY
# ============================================================================
# This script uploads the customer normalization mapping tables and runs
# the updated BigQuery ML models for customer demand forecasting.
#
# Prerequisites:
#   1. Google Cloud SDK (gcloud) installed and authenticated
#   2. BigQuery permissions for the target project/dataset
#   3. The following CSV files must exist in the model_evaluation directory:
#      - customer_master_mapping.csv
#      - customer_id_lookup.csv
#
# Usage:
#   ./DEPLOY_CUSTOMER_NORMALIZED.sh <PROJECT_ID> <DATASET_ID>
#
# Example:
#   ./DEPLOY_CUSTOMER_NORMALIZED.sh my-gcp-project redai_aca_features
# ============================================================================

set -e

# Check arguments
if [ $# -ne 2 ]; then
    echo "Usage: $0 <PROJECT_ID> <DATASET_ID>"
    echo "Example: $0 my-gcp-project redai_aca_features"
    exit 1
fi

PROJECT_ID=$1
DATASET_ID=$2

# Paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$SCRIPT_DIR/../model_evaluation"

echo "============================================"
echo "DEPLOYING NORMALIZED CUSTOMER DATA"
echo "============================================"
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_ID"
echo "Data Dir: $DATA_DIR"
echo "============================================"

# Step 1: Verify CSV files exist
echo ""
echo "[1/4] Verifying CSV files..."
if [ ! -f "$DATA_DIR/customer_master_mapping.csv" ]; then
    echo "ERROR: customer_master_mapping.csv not found in $DATA_DIR"
    exit 1
fi

if [ ! -f "$DATA_DIR/customer_id_lookup.csv" ]; then
    echo "ERROR: customer_id_lookup.csv not found in $DATA_DIR"
    # Try to find it elsewhere
    if [ -f "$SCRIPT_DIR/../customer_id_lookup.csv" ]; then
        cp "$SCRIPT_DIR/../customer_id_lookup.csv" "$DATA_DIR/"
        echo "Found and copied customer_id_lookup.csv from parent directory"
    else
        exit 1
    fi
fi

echo "✓ CSV files found"

# Step 2: Upload customer_master_mapping.csv
echo ""
echo "[2/4] Uploading customer_master_mapping.csv..."
bq load \
    --source_format=CSV \
    --autodetect \
    --replace \
    "${PROJECT_ID}:${DATASET_ID}.customer_master_mapping" \
    "$DATA_DIR/customer_master_mapping.csv"
echo "✓ customer_master_mapping uploaded"

# Step 3: Upload customer_id_lookup.csv
echo ""
echo "[3/4] Uploading customer_id_lookup.csv..."
bq load \
    --source_format=CSV \
    --autodetect \
    --replace \
    "${PROJECT_ID}:${DATASET_ID}.customer_id_lookup" \
    "$DATA_DIR/customer_id_lookup.csv"
echo "✓ customer_id_lookup uploaded"

# Step 4: Run the BigQuery ML script
echo ""
echo "[4/4] Running BigQuery ML models..."
echo "This may take several minutes..."

# Replace placeholders in SQL file and execute
SQL_FILE="$SCRIPT_DIR/bigquery_customer_normalized.sql"
sed "s/\${PROJECT_ID}/${PROJECT_ID}/g; s/\${DATASET_ID}/${DATASET_ID}/g" "$SQL_FILE" | \
    bq query --use_legacy_sql=false --max_rows=1000

echo ""
echo "============================================"
echo "DEPLOYMENT COMPLETE"
echo "============================================"
echo ""
echo "New tables/models created:"
echo "  - ${DATASET_ID}.customer_master_mapping (mapping table)"
echo "  - ${DATASET_ID}.customer_id_lookup (lookup table)"
echo "  - ${DATASET_ID}.features_customer_normalized (features)"
echo "  - ${DATASET_ID}.xgb_customer_normalized (XGBoost model)"
echo "  - ${DATASET_ID}.arima_customer_normalized (ARIMA+ model)"
echo "  - ${DATASET_ID}.predictions_customer_xgb_normalized"
echo "  - ${DATASET_ID}.predictions_customer_arima_normalized"
echo "  - ${DATASET_ID}.eval_customer_xgb_normalized"
echo "  - ${DATASET_ID}.eval_customer_arima_normalized"
echo "  - ${DATASET_ID}.mape_by_customer_normalized"
echo "  - ${DATASET_ID}.forecast_customer_normalized_next4w"
echo ""
echo "To view results, run:"
echo "  bq query --use_legacy_sql=false 'SELECT * FROM ${DATASET_ID}.mape_by_customer_normalized LIMIT 10'"
echo ""
