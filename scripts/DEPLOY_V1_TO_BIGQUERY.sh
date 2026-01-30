#!/bin/bash
# =============================================================================
# DEPLOY V1 DATA TO BIGQUERY
# =============================================================================
# Uploads the v1 extraction outputs (with customer data) to BigQuery
# Run from: demand planning folder
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"
FEATURES_DIR="features_v1"

echo "==========================================="
echo "DEPLOYING V1 DATA TO BIGQUERY"
echo "==========================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_NAME"
echo ""

# Check if features_v1 folder exists
if [ ! -d "$FEATURES_DIR" ]; then
    echo "ERROR: $FEATURES_DIR directory not found!"
    echo "Run extract_sku_data_v1.py first"
    exit 1
fi

echo "Found v1 output files:"
ls -la $FEATURES_DIR/*.csv
echo ""

# -----------------------------------------------------------------------------
# 1. Upload Fact Table (Line Items with Customer Data)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "1/7 Uploading v1_fact_lineitem..."
echo "==========================================="
bq load --source_format=CSV --autodetect --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v1_fact_lineitem \
  ${FEATURES_DIR}/v1_fact_lineitem.csv

echo "✓ v1_fact_lineitem uploaded"
echo ""

# -----------------------------------------------------------------------------
# 2. Upload Weekly Features (SKU × Week)
# -----------------------------------------------------------------------------
echo "==========================================="
echo "2/7 Uploading v1_features_weekly..."
echo "==========================================="
bq load --source_format=CSV --autodetect --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v1_features_weekly \
  ${FEATURES_DIR}/v1_features_weekly.csv

echo "✓ v1_features_weekly uploaded"
echo ""

# -----------------------------------------------------------------------------
# 3. Upload SKU × Customer × Week Features
# -----------------------------------------------------------------------------
echo "==========================================="
echo "3/7 Uploading v1_features_sku_customer..."
echo "==========================================="
bq load --source_format=CSV --autodetect --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v1_features_sku_customer \
  ${FEATURES_DIR}/v1_features_sku_customer.csv

echo "✓ v1_features_sku_customer uploaded"
echo ""

# -----------------------------------------------------------------------------
# 4. Upload Category Features
# -----------------------------------------------------------------------------
echo "==========================================="
echo "4/7 Uploading v1_features_category..."
echo "==========================================="
bq load --source_format=CSV --autodetect --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v1_features_category \
  ${FEATURES_DIR}/v1_features_category.csv

echo "✓ v1_features_category uploaded"
echo ""

# -----------------------------------------------------------------------------
# 5. Upload Customer Dimension
# -----------------------------------------------------------------------------
echo "==========================================="
echo "5/7 Uploading v1_dim_customers..."
echo "==========================================="
bq load --source_format=CSV --autodetect --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v1_dim_customers \
  ${FEATURES_DIR}/v1_dim_customers.csv

echo "✓ v1_dim_customers uploaded"
echo ""

# -----------------------------------------------------------------------------
# 6. Upload Product Dimension
# -----------------------------------------------------------------------------
echo "==========================================="
echo "6/7 Uploading v1_dim_products..."
echo "==========================================="
bq load --source_format=CSV --autodetect --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v1_dim_products \
  ${FEATURES_DIR}/v1_dim_products.csv

echo "✓ v1_dim_products uploaded"
echo ""

# -----------------------------------------------------------------------------
# 7. Upload Week Completeness Flags
# -----------------------------------------------------------------------------
echo "==========================================="
echo "7/7 Uploading v1_week_completeness..."
echo "==========================================="
bq load --source_format=CSV --autodetect --replace \
  ${PROJECT_ID}:${DATASET_NAME}.v1_week_completeness \
  ${FEATURES_DIR}/v1_week_completeness.csv

echo "✓ v1_week_completeness uploaded"
echo ""

# -----------------------------------------------------------------------------
# Verify Upload
# -----------------------------------------------------------------------------
echo "==========================================="
echo "VERIFICATION - Table Row Counts"
echo "==========================================="

bq query --use_legacy_sql=false "
SELECT
  'v1_fact_lineitem' as table_name,
  COUNT(*) as row_count
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_fact_lineitem\`
UNION ALL
SELECT
  'v1_features_weekly',
  COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_weekly\`
UNION ALL
SELECT
  'v1_features_sku_customer',
  COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_sku_customer\`
UNION ALL
SELECT
  'v1_features_category',
  COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_features_category\`
UNION ALL
SELECT
  'v1_dim_customers',
  COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_dim_customers\`
UNION ALL
SELECT
  'v1_dim_products',
  COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_dim_products\`
UNION ALL
SELECT
  'v1_week_completeness',
  COUNT(*)
FROM \`${PROJECT_ID}.${DATASET_NAME}.v1_week_completeness\`
ORDER BY table_name
"

echo ""
echo "==========================================="
echo "✅ V1 DEPLOYMENT COMPLETE!"
echo "==========================================="
echo ""
echo "New tables in BigQuery:"
echo "  - ${DATASET_NAME}.v1_fact_lineitem       (431K transactions)"
echo "  - ${DATASET_NAME}.v1_features_weekly     (33K SKU×Week)"
echo "  - ${DATASET_NAME}.v1_features_sku_customer (101K SKU×Customer×Week)"
echo "  - ${DATASET_NAME}.v1_features_category   (1.8K Category×Week)"
echo "  - ${DATASET_NAME}.v1_dim_customers       (225 customers)"
echo "  - ${DATASET_NAME}.v1_dim_products        (2.3K products)"
echo "  - ${DATASET_NAME}.v1_week_completeness   (52 weeks)"
echo ""
echo "Next step: Run TRAIN_V1_MODELS.sh to create v1 forecasting models"
