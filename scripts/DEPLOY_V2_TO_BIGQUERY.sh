#!/bin/bash
# =============================================================================
# DEPLOY V2 DATA TO BIGQUERY
# =============================================================================
# V2 includes: 100% price coverage, buying cycles, bulk buyer classification
# Run from: demand planning folder
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"
FEATURES_DIR="features_v2"

echo "==========================================="
echo "DEPLOYING V2 DATA TO BIGQUERY"
echo "==========================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_NAME"
echo ""

# Check if features_v2 folder exists
if [ ! -d "$FEATURES_DIR" ]; then
    echo "ERROR: $FEATURES_DIR directory not found!"
    echo "Run extract_sku_data_v2.py first"
    exit 1
fi

echo "V2 output files:"
ls -la $FEATURES_DIR/*.csv
echo ""

# -----------------------------------------------------------------------------
# 1. Upload Fact Table (explicit schema - customer_id is STRING)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "1/9 Uploading v2_fact_lineitem..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_fact_lineitem \
  ${FEATURES_DIR}/v2_fact_lineitem.csv \
  invoice_id:STRING,order_date:STRING,customer_id:STRING,customer_name:STRING,region_name:STRING,sku:STRING,description:STRING,quantity:FLOAT,unit_price:FLOAT,line_total:FLOAT,year_week:STRING,data_completeness:STRING,customer_segment:STRING,buyer_type:STRING,category_l1:STRING,category_l2:STRING

echo "✓ v2_fact_lineitem uploaded"
echo ""

# -----------------------------------------------------------------------------
# 2. Upload Price History (KEY NEW TABLE)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "2/9 Uploading v2_price_history..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_price_history \
  ${FEATURES_DIR}/v2_price_history.csv \
  sku:STRING,year_week:STRING,avg_price:FLOAT,min_price:FLOAT,max_price:FLOAT,price_std:FLOAT,price_observations:INTEGER,weekly_quantity:FLOAT,weekly_revenue:FLOAT,prev_avg_price:FLOAT,price_change:FLOAT,price_change_pct:FLOAT

echo "✓ v2_price_history uploaded"
echo ""

# -----------------------------------------------------------------------------
# 3. Upload Customer Cycles (KEY NEW TABLE)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "3/9 Uploading v2_customer_cycles..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_customer_cycles \
  ${FEATURES_DIR}/v2_customer_cycles.csv \
  customer_id:STRING,customer_name:STRING,primary_region:STRING,total_orders:INTEGER,total_units:FLOAT,total_revenue:FLOAT,avg_order_value:FLOAT,avg_days_between_orders:FLOAT,cycle_regularity:STRING,buyer_type:STRING,top_skus:STRING,first_order:STRING,last_order:STRING,active_weeks:INTEGER,customer_segment:STRING

echo "✓ v2_customer_cycles uploaded"
echo ""

# -----------------------------------------------------------------------------
# 4. Upload Weekly Features (with price features)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "4/9 Uploading v2_features_weekly..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_features_weekly \
  ${FEATURES_DIR}/v2_features_weekly.csv \
  sku:STRING,year_week:STRING,weekly_quantity:FLOAT,avg_unit_price:FLOAT,weekly_revenue:FLOAT,order_count:INTEGER,unique_customers:INTEGER,description:STRING,data_completeness:STRING,lag1_quantity:FLOAT,lag1_price:FLOAT,lag2_quantity:FLOAT,lag2_price:FLOAT,lag4_quantity:FLOAT,lag4_price:FLOAT,rolling_avg_4w:FLOAT,price_rolling_avg_4w:FLOAT,price_change:FLOAT,price_change_pct:FLOAT

echo "✓ v2_features_weekly uploaded"
echo ""

# -----------------------------------------------------------------------------
# 5. Upload SKU × Customer Features
# -----------------------------------------------------------------------------
echo "==========================================="
echo "5/9 Uploading v2_features_sku_customer..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_features_sku_customer \
  ${FEATURES_DIR}/v2_features_sku_customer.csv \
  sku:STRING,customer_id:STRING,year_week:STRING,weekly_quantity:FLOAT,avg_unit_price:FLOAT,weekly_revenue:FLOAT,order_count:INTEGER,customer_name:STRING,customer_segment:STRING,buyer_type:STRING,data_completeness:STRING

echo "✓ v2_features_sku_customer uploaded"
echo ""

# -----------------------------------------------------------------------------
# 6. Upload Category Features
# -----------------------------------------------------------------------------
echo "==========================================="
echo "6/9 Uploading v2_features_category..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_features_category \
  ${FEATURES_DIR}/v2_features_category.csv \
  category:STRING,year_week:STRING,weekly_quantity:FLOAT,avg_unit_price:FLOAT,weekly_revenue:FLOAT,unique_skus:INTEGER,order_count:INTEGER,data_completeness:STRING

echo "✓ v2_features_category uploaded"
echo ""

# -----------------------------------------------------------------------------
# 7. Upload Customer Dimension (with buyer types)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "7/9 Uploading v2_dim_customers..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_dim_customers \
  ${FEATURES_DIR}/v2_dim_customers.csv \
  customer_id:STRING,customer_name:STRING,primary_region:STRING,total_orders:INTEGER,total_units:FLOAT,total_revenue:FLOAT,avg_order_value:FLOAT,avg_days_between_orders:FLOAT,cycle_regularity:STRING,buyer_type:STRING,customer_segment:STRING,first_order:STRING,last_order:STRING,active_weeks:INTEGER

echo "✓ v2_dim_customers uploaded"
echo ""

# -----------------------------------------------------------------------------
# 8. Upload Product Dimension (with price stats)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "8/9 Uploading v2_dim_products..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_dim_products \
  ${FEATURES_DIR}/v2_dim_products.csv \
  sku:STRING,name:STRING,avg_price:FLOAT,min_price:FLOAT,max_price:FLOAT,price_std:FLOAT,total_quantity:FLOAT,total_revenue:FLOAT,total_orders:INTEGER,active_weeks:INTEGER,brand:STRING,manufacturer:STRING,category_path:STRING,fmcg:STRING,category_l1:STRING,category_l2:STRING,category_l3:STRING,price_volatility:FLOAT

echo "✓ v2_dim_products uploaded"
echo ""

# -----------------------------------------------------------------------------
# 9. Upload Week Completeness
# -----------------------------------------------------------------------------
echo "==========================================="
echo "9/9 Uploading v2_week_completeness..."
echo "==========================================="
bq load --source_format=CSV --skip_leading_rows=1 --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v2_week_completeness \
  ${FEATURES_DIR}/v2_week_completeness.csv \
  year_week:STRING,invoice_count:INTEGER,total_quantity:FLOAT,total_revenue:FLOAT,unique_skus:INTEGER,unique_customers:INTEGER,regions_active:INTEGER,data_completeness:STRING,prices_captured:INTEGER,price_coverage:FLOAT

echo "✓ v2_week_completeness uploaded"
echo ""

# -----------------------------------------------------------------------------
# Verify Upload
# -----------------------------------------------------------------------------
echo "==========================================="
echo "VERIFICATION - Table Row Counts"
echo "==========================================="

bq query --use_legacy_sql=false "
SELECT table_name, row_count FROM (
  SELECT 'v2_fact_lineitem' as table_name, COUNT(*) as row_count
  FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_fact_lineitem\`
  UNION ALL SELECT 'v2_price_history', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_price_history\`
  UNION ALL SELECT 'v2_customer_cycles', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_customer_cycles\`
  UNION ALL SELECT 'v2_features_weekly', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_weekly\`
  UNION ALL SELECT 'v2_features_sku_customer', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_sku_customer\`
  UNION ALL SELECT 'v2_features_category', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_category\`
  UNION ALL SELECT 'v2_dim_customers', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_dim_customers\`
  UNION ALL SELECT 'v2_dim_products', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_dim_products\`
  UNION ALL SELECT 'v2_week_completeness', COUNT(*) FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_week_completeness\`
)
ORDER BY table_name
"

echo ""
echo "==========================================="
echo "✅ V2 DEPLOYMENT COMPLETE!"
echo "==========================================="
echo ""
echo "Key V2 improvements:"
echo "  ✓ 100% price coverage (was 27% in v1)"
echo "  ✓ Price history tracking by SKU × Week"
echo "  ✓ Customer buying cycles (Weekly/Monthly/Irregular)"
echo "  ✓ Bulk buyer classification"
echo "  ✓ 605 customers identified (was 225 in v1)"
echo ""
echo "New tables:"
echo "  - v2_price_history       (33K SKU×Week price records)"
echo "  - v2_customer_cycles     (605 customers with cycle patterns)"
echo ""
echo "Next step: Run TRAIN_V2_MODELS.sh"
