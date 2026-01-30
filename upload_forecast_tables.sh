#!/bin/bash
# =============================================================================
# Upload Forecast Aggregation Tables to BigQuery EU
# =============================================================================
#
# Uploads all forecast tables (SKU, Category, Customer) with H1/H2 splits
# to the europe-west6 dataset for Vertex AI training
#
# Usage:
#   ./upload_forecast_tables.sh
#
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="redai_demand_forecast_eu"  # europe-west6
LOCAL_DIR="features_v2"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "============================================================"
echo "  Upload Forecast Tables to BigQuery EU"
echo "============================================================"
echo ""

# Check bq command
if ! command -v bq &> /dev/null; then
    echo -e "${RED}Error: bq command not found. Install Google Cloud SDK.${NC}"
    exit 1
fi

gcloud config set project $PROJECT_ID 2>/dev/null
echo -e "${GREEN}✓ Project: $PROJECT_ID${NC}"
echo -e "${GREEN}✓ Dataset: $DATASET (europe-west6)${NC}"
echo ""

upload_table() {
    local csv_file=$1
    local table_name=$2

    if [ ! -f "$csv_file" ]; then
        echo -e "${RED}✗ File not found: $csv_file${NC}"
        return 1
    fi

    local rows=$(wc -l < "$csv_file")
    rows=$((rows - 1))

    echo "------------------------------------------------------------"
    echo "Uploading: $csv_file"
    echo "  → Table: $DATASET.$table_name"
    echo "  → Rows:  $rows"

    bq load --source_format=CSV --autodetect --replace \
        "$DATASET.$table_name" "$csv_file"

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Uploaded: $table_name${NC}"
    else
        echo -e "${RED}✗ Failed: $table_name${NC}"
        return 1
    fi
    echo ""
}

echo "============================================================"
echo "  SKU Level Forecasts"
echo "============================================================"
upload_table "$LOCAL_DIR/forecast_sku_daily.csv" "forecast_sku_daily"
upload_table "$LOCAL_DIR/forecast_sku_daily_H1.csv" "forecast_sku_daily_H1"
upload_table "$LOCAL_DIR/forecast_sku_daily_H2.csv" "forecast_sku_daily_H2"
upload_table "$LOCAL_DIR/forecast_sku_weekly.csv" "forecast_sku_weekly"
upload_table "$LOCAL_DIR/forecast_sku_weekly_H1.csv" "forecast_sku_weekly_H1"
upload_table "$LOCAL_DIR/forecast_sku_weekly_H2.csv" "forecast_sku_weekly_H2"

echo "============================================================"
echo "  Category Level Forecasts"
echo "============================================================"
upload_table "$LOCAL_DIR/forecast_category_daily.csv" "forecast_category_daily"
upload_table "$LOCAL_DIR/forecast_category_daily_H1.csv" "forecast_category_daily_H1"
upload_table "$LOCAL_DIR/forecast_category_daily_H2.csv" "forecast_category_daily_H2"
upload_table "$LOCAL_DIR/forecast_category_weekly.csv" "forecast_category_weekly"
upload_table "$LOCAL_DIR/forecast_category_weekly_H1.csv" "forecast_category_weekly_H1"
upload_table "$LOCAL_DIR/forecast_category_weekly_H2.csv" "forecast_category_weekly_H2"

echo "============================================================"
echo "  Customer Level Forecasts"
echo "============================================================"
upload_table "$LOCAL_DIR/forecast_customer_daily.csv" "forecast_customer_daily"
upload_table "$LOCAL_DIR/forecast_customer_daily_H1.csv" "forecast_customer_daily_H1"
upload_table "$LOCAL_DIR/forecast_customer_daily_H2.csv" "forecast_customer_daily_H2"
upload_table "$LOCAL_DIR/forecast_customer_weekly.csv" "forecast_customer_weekly"
upload_table "$LOCAL_DIR/forecast_customer_weekly_H1.csv" "forecast_customer_weekly_H1"
upload_table "$LOCAL_DIR/forecast_customer_weekly_H2.csv" "forecast_customer_weekly_H2"

echo ""
echo "============================================================"
echo -e "${GREEN}DONE!${NC}"
echo "============================================================"
echo ""
echo "Tables ready for Vertex AI Forecasting:"
echo ""
echo "  Training (H1 - Weeks 01-26):"
echo "    • $DATASET.forecast_sku_weekly_H1"
echo "    • $DATASET.forecast_category_weekly_H1"
echo "    • $DATASET.forecast_customer_weekly_H1"
echo ""
echo "  Validation (H2 - Weeks 27-52):"
echo "    • $DATASET.forecast_sku_weekly_H2"
echo "    • $DATASET.forecast_category_weekly_H2"
echo "    • $DATASET.forecast_customer_weekly_H2"
echo ""
echo "Vertex AI Configuration:"
echo "  • Series identifier: sku / category_l1 / customer_id"
echo "  • Timestamp column: date"
echo "  • Target column: revenue"
echo ""
