#!/bin/bash
# ============================================================
# UPLOAD DATA TO BIGQUERY FOR DEMAND FORECASTING
# ============================================================
# This script uploads the feature data to BigQuery and runs
# the XGBoost models for SKU, Category, and Customer levels.
#
# Prerequisites:
#   - Google Cloud SDK installed (gcloud, bq commands)
#   - Authenticated: gcloud auth login
#   - Project set: gcloud config set project YOUR_PROJECT_ID
#
# Usage:
#   ./upload_to_bigquery.sh YOUR_PROJECT_ID DATASET_NAME
#
# Example:
#   ./upload_to_bigquery.sh redai-demo redai_demand_forecast
# ============================================================

set -e  # Exit on error

# Check arguments
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 PROJECT_ID DATASET_NAME"
    echo "Example: $0 redai-demo redai_demand_forecast"
    exit 1
fi

PROJECT_ID=$1
DATASET_ID=$2
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$(dirname "$SCRIPT_DIR")/features_v2"

echo "============================================================"
echo "BIGQUERY DATA UPLOAD - DEMAND FORECASTING"
echo "============================================================"
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_ID"
echo "Data Dir: $DATA_DIR"
echo ""

# ============================================================
# STEP 1: Create Dataset
# ============================================================
echo "1. Creating dataset (if not exists)..."
bq --project_id=$PROJECT_ID mk --dataset \
    --description "RedAI Demand Forecasting Models" \
    --location US \
    $PROJECT_ID:$DATASET_ID 2>/dev/null || echo "   Dataset already exists"

# ============================================================
# STEP 2: Upload Source Data with explicit schema
# ============================================================
echo ""
echo "2. Uploading source data..."

# Create schema file for fact_lineitem (customer_id as STRING, order_date as STRING to handle mixed formats)
cat > /tmp/fact_lineitem_schema.json << 'EOF'
[
  {"name": "invoice_id", "type": "STRING"},
  {"name": "order_date", "type": "STRING"},
  {"name": "customer_id", "type": "STRING"},
  {"name": "customer_name", "type": "STRING"},
  {"name": "region_name", "type": "STRING"},
  {"name": "sku", "type": "STRING"},
  {"name": "description", "type": "STRING"},
  {"name": "quantity", "type": "FLOAT64"},
  {"name": "unit_price", "type": "FLOAT64"},
  {"name": "line_total", "type": "FLOAT64"},
  {"name": "year_week", "type": "STRING"},
  {"name": "data_completeness", "type": "STRING"},
  {"name": "customer_segment", "type": "STRING"},
  {"name": "buyer_type", "type": "STRING"},
  {"name": "category_l1", "type": "STRING"},
  {"name": "category_l2", "type": "STRING"}
]
EOF

# Upload fact_lineitem with explicit schema
echo "   Uploading fact_lineitem.csv (with explicit schema)..."
bq --project_id=$PROJECT_ID load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --replace \
    --schema=/tmp/fact_lineitem_schema.json \
    $DATASET_ID.fact_lineitem \
    "$DATA_DIR/v2_fact_lineitem.csv"

# Upload dim_products (autodetect is fine)
echo "   Uploading dim_products.csv..."
bq --project_id=$PROJECT_ID load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --replace \
    --autodetect \
    $DATASET_ID.dim_products \
    "$DATA_DIR/v2_dim_products.csv"

# Create schema for dim_customers (customer_id as STRING)
cat > /tmp/dim_customers_schema.json << 'EOF'
[
  {"name": "customer_id", "type": "STRING"},
  {"name": "customer_name", "type": "STRING"},
  {"name": "primary_region", "type": "STRING"},
  {"name": "total_orders", "type": "INT64"},
  {"name": "total_units", "type": "FLOAT64"},
  {"name": "total_revenue", "type": "FLOAT64"},
  {"name": "avg_order_value", "type": "FLOAT64"},
  {"name": "avg_days_between_orders", "type": "FLOAT64"},
  {"name": "cycle_regularity", "type": "STRING"},
  {"name": "buyer_type", "type": "STRING"},
  {"name": "customer_segment", "type": "STRING"},
  {"name": "first_order", "type": "STRING"},
  {"name": "last_order", "type": "STRING"},
  {"name": "active_weeks", "type": "INT64"}
]
EOF

# Upload dim_customers with explicit schema
echo "   Uploading dim_customers.csv (with explicit schema)..."
bq --project_id=$PROJECT_ID load \
    --source_format=CSV \
    --skip_leading_rows=1 \
    --replace \
    --schema=/tmp/dim_customers_schema.json \
    $DATASET_ID.dim_customers \
    "$DATA_DIR/v2_dim_customers.csv"

echo "   Data upload complete!"

# ============================================================
# STEP 3: Run BigQuery ML Script
# ============================================================
echo ""
echo "3. Running BigQuery ML models..."
echo "   This will:"
echo "   - Create feature tables for SKU, Category, Customer"
echo "   - Train 3 XGBoost models"
echo "   - Generate H2 predictions"
echo "   - Calculate evaluation metrics"
echo ""

# Replace variables in SQL script and run
SQL_FILE="$SCRIPT_DIR/bigquery_xgboost_all_levels.sql"
if [ -f "$SQL_FILE" ]; then
    # Create temp file with substituted variables
    TEMP_SQL=$(mktemp)
    sed -e "s/\${PROJECT_ID}/$PROJECT_ID/g" \
        -e "s/\${DATASET_ID}/$DATASET_ID/g" \
        "$SQL_FILE" > "$TEMP_SQL"

    echo "   Running SQL script..."
    bq --project_id=$PROJECT_ID query \
        --use_legacy_sql=false \
        --max_rows=1000 \
        < "$TEMP_SQL"

    rm "$TEMP_SQL"
else
    echo "   ERROR: SQL file not found: $SQL_FILE"
    exit 1
fi

# Clean up temp schema files
rm -f /tmp/fact_lineitem_schema.json /tmp/dim_customers_schema.json

# ============================================================
# STEP 4: Export Results
# ============================================================
echo ""
echo "4. Exporting evaluation summary..."

bq --project_id=$PROJECT_ID query \
    --use_legacy_sql=false \
    --format=prettyjson \
    "SELECT * FROM \`$PROJECT_ID.$DATASET_ID.eval_summary\`"

# ============================================================
# STEP 5: Summary
# ============================================================
echo ""
echo "============================================================"
echo "BIGQUERY SETUP COMPLETE"
echo "============================================================"
echo ""
echo "Tables created:"
echo "  - $DATASET_ID.fact_lineitem (source data)"
echo "  - $DATASET_ID.features_sku_weekly"
echo "  - $DATASET_ID.features_category_weekly"
echo "  - $DATASET_ID.features_customer_weekly"
echo ""
echo "Models created:"
echo "  - $DATASET_ID.xgb_sku_model"
echo "  - $DATASET_ID.xgb_category_model"
echo "  - $DATASET_ID.xgb_customer_model"
echo ""
echo "Prediction tables:"
echo "  - $DATASET_ID.predictions_sku"
echo "  - $DATASET_ID.predictions_category"
echo "  - $DATASET_ID.predictions_customer"
echo ""
echo "Evaluation tables:"
echo "  - $DATASET_ID.eval_summary"
echo "  - $DATASET_ID.mape_by_sku"
echo "  - $DATASET_ID.mape_by_category"
echo "  - $DATASET_ID.mape_by_customer"
echo ""
echo "To view results in BigQuery Console:"
echo "  https://console.cloud.google.com/bigquery?project=$PROJECT_ID"
echo ""
