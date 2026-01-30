#!/bin/bash
# =============================================================================
# Upload Clean Transactions to BigQuery
# =============================================================================
#
# Usage:
#   ./upload_to_bigquery.sh                    # Upload transactions_clean only
#   ./upload_to_bigquery.sh --all              # Upload all tables
#
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="redai_demand_forecast"  # europe-west6
BUCKET="demand_planning_aca"
LOCAL_DIR="features_v2"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "============================================================"
echo "  ACA Demand Planning - Upload to BigQuery"
echo "============================================================"
echo ""

# Check bq command
if ! command -v bq &> /dev/null; then
    echo -e "${RED}Error: bq command not found. Install Google Cloud SDK.${NC}"
    exit 1
fi

gcloud config set project $PROJECT_ID 2>/dev/null
echo -e "${GREEN}✓ Project: $PROJECT_ID${NC}"
echo ""

# Schema for transactions_clean (customer_id is now INTEGER)
create_transactions_schema() {
    cat > /tmp/transactions_schema.json << 'SCHEMA'
[
  {"name": "invoice_id", "type": "INTEGER"},
  {"name": "order_date", "type": "DATE"},
  {"name": "customer_id", "type": "INTEGER"},
  {"name": "original_customer_id", "type": "STRING"},
  {"name": "customer_name", "type": "STRING"},
  {"name": "region_name", "type": "STRING"},
  {"name": "sku", "type": "INTEGER"},
  {"name": "description", "type": "STRING"},
  {"name": "quantity", "type": "INTEGER"},
  {"name": "unit_price", "type": "FLOAT"},
  {"name": "line_total", "type": "FLOAT"},
  {"name": "year_week", "type": "STRING"},
  {"name": "data_completeness", "type": "STRING"},
  {"name": "customer_segment", "type": "STRING"},
  {"name": "buyer_type", "type": "STRING"},
  {"name": "category_l1", "type": "STRING"},
  {"name": "category_l2", "type": "STRING"}
]
SCHEMA
    echo "/tmp/transactions_schema.json"
}

upload_table() {
    local csv_file=$1
    local table_name=$2
    local schema_file=$3

    if [ ! -f "$csv_file" ]; then
        echo -e "${RED}✗ File not found: $csv_file${NC}"
        return 1
    fi

    local rows=$(wc -l < "$csv_file")
    rows=$((rows - 1))

    echo "------------------------------------------------------------"
    echo "Uploading: $csv_file"
    echo "  → Table: $PROJECT_ID.$DATASET.$table_name"
    echo "  → Rows:  $rows"

    if [ -n "$schema_file" ]; then
        bq load --source_format=CSV --skip_leading_rows=1 --replace \
            --schema="$schema_file" "$DATASET.$table_name" "$csv_file"
    else
        bq load --source_format=CSV --autodetect --replace \
            "$DATASET.$table_name" "$csv_file"
    fi

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ Uploaded: $table_name${NC}"
    else
        echo -e "${RED}✗ Failed: $table_name${NC}"
        return 1
    fi
    echo ""
}

# Main
SCHEMA_FILE=$(create_transactions_schema)

if [ "$1" == "--all" ]; then
    echo "Uploading ALL tables..."
    upload_table "$LOCAL_DIR/transactions_clean.csv" "transactions_clean" "$SCHEMA_FILE"
    upload_table "$LOCAL_DIR/v2_dim_products.csv" "dim_products" ""
    upload_table "$LOCAL_DIR/v2_dim_customers.csv" "dim_customers" ""
    upload_table "customer_id_lookup.csv" "customer_id_lookup" ""
    upload_table "customer_master_mapping.csv" "customer_master_mapping" ""
else
    echo "Uploading transactions_clean..."
    upload_table "$LOCAL_DIR/transactions_clean.csv" "transactions_clean" "$SCHEMA_FILE"
fi

echo "============================================================"
echo -e "${GREEN}DONE!${NC}"
echo "============================================================"
echo ""
echo "Query example:"
echo ""
echo "  bq query --use_legacy_sql=false '"
echo "  SELECT year_week,"
echo "         COUNT(DISTINCT customer_id) as customers,"
echo "         COUNT(DISTINCT sku) as skus,"
echo "         SUM(quantity) as total_qty,"
echo "         SUM(line_total) as revenue"
echo "  FROM \`$PROJECT_ID.$DATASET.transactions_clean\`"
echo "  GROUP BY year_week"
echo "  ORDER BY year_week"
echo "  LIMIT 10'"
echo ""
