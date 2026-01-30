#!/bin/bash
# ============================================================================
# DEPLOY TO BIGQUERY - Comprehensive Deployment Script
# ============================================================================
# RedAI ACA Demand Planning
#
# This script validates all CSV files, fixes common issues, and deploys
# data and models to BigQuery.
#
# Prerequisites:
#   1. Google Cloud SDK (gcloud) installed and authenticated
#   2. BigQuery permissions for the target project/dataset
#   3. Python 3 with pandas, numpy
#
# Usage:
#   ./DEPLOY_TO_BIGQUERY.sh <PROJECT_ID> <DATASET_ID> [--dry-run] [--skip-models]
#
# Options:
#   --dry-run      Validate only, don't upload to BigQuery
#   --skip-models  Skip BigQuery ML model training (just upload data)
#   --force        Skip validation and upload anyway (not recommended)
#
# Example:
#   ./DEPLOY_TO_BIGQUERY.sh my-gcp-project redai_aca_features
#   ./DEPLOY_TO_BIGQUERY.sh my-gcp-project redai_aca_features --dry-run
# ============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse arguments
PROJECT_ID=""
DATASET_ID=""
DRY_RUN=false
SKIP_MODELS=false
FORCE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        --skip-models)
            SKIP_MODELS=true
            shift
            ;;
        --force)
            FORCE=true
            shift
            ;;
        -h|--help)
            head -35 "$0" | tail -30
            exit 0
            ;;
        *)
            if [ -z "$PROJECT_ID" ]; then
                PROJECT_ID=$1
            elif [ -z "$DATASET_ID" ]; then
                DATASET_ID=$1
            fi
            shift
            ;;
    esac
done

# Validate arguments
if [ -z "$PROJECT_ID" ] || [ -z "$DATASET_ID" ]; then
    echo -e "${RED}Error: Missing required arguments${NC}"
    echo "Usage: $0 <PROJECT_ID> <DATASET_ID> [--dry-run] [--skip-models]"
    exit 1
fi

# Get script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}"
echo "============================================================"
echo "          BIGQUERY DEPLOYMENT - RedAI ACA"
echo "============================================================"
echo -e "${NC}"
echo "Project ID:   $PROJECT_ID"
echo "Dataset ID:   $DATASET_ID"
echo "Project Root: $PROJECT_ROOT"
echo "Dry Run:      $DRY_RUN"
echo "Skip Models:  $SKIP_MODELS"
echo ""

# ============================================================================
# PHASE 1: VALIDATION
# ============================================================================
echo -e "${BLUE}[PHASE 1/4] VALIDATING CSV FILES${NC}"
echo "============================================================"

cd "$PROJECT_ROOT"

# Run Python validation script
VALIDATION_RESULT=$(python3 "$SCRIPT_DIR/bigquery_prevalidate.py" --all 2>&1)
echo "$VALIDATION_RESULT"

# Check for critical issues
if echo "$VALIDATION_RESULT" | grep -q "⚠️"; then
    ISSUE_COUNT=$(echo "$VALIDATION_RESULT" | grep -c "⚠️" || true)
    echo ""
    echo -e "${YELLOW}Found $ISSUE_COUNT potential issues${NC}"

    if [ "$FORCE" = false ]; then
        echo ""
        read -p "Do you want to auto-fix these issues? (y/n) " -n 1 -r
        echo ""
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            echo "Running auto-fix..."
            python3 "$SCRIPT_DIR/bigquery_prevalidate.py" --all --fix
            echo -e "${GREEN}✓ Issues fixed${NC}"
        else
            echo -e "${RED}Aborting deployment. Fix issues manually or use --force${NC}"
            exit 1
        fi
    else
        echo -e "${YELLOW}Skipping fixes due to --force flag${NC}"
    fi
else
    echo -e "${GREEN}✓ All files validated successfully${NC}"
fi

if [ "$DRY_RUN" = true ]; then
    echo ""
    echo -e "${YELLOW}DRY RUN MODE - Skipping actual upload${NC}"
    echo "Remove --dry-run flag to perform actual deployment"
    exit 0
fi

# ============================================================================
# PHASE 2: CREATE DATASET IF NOT EXISTS
# ============================================================================
echo ""
echo -e "${BLUE}[PHASE 2/4] PREPARING BIGQUERY DATASET${NC}"
echo "============================================================"

# Check if dataset exists
if bq show "${PROJECT_ID}:${DATASET_ID}" > /dev/null 2>&1; then
    echo -e "${GREEN}✓ Dataset ${DATASET_ID} exists${NC}"
else
    echo "Creating dataset ${DATASET_ID}..."
    bq mk --dataset "${PROJECT_ID}:${DATASET_ID}"
    echo -e "${GREEN}✓ Dataset created${NC}"
fi

# ============================================================================
# PHASE 3: UPLOAD CSV FILES
# ============================================================================
echo ""
echo -e "${BLUE}[PHASE 3/4] UPLOADING DATA TO BIGQUERY${NC}"
echo "============================================================"

upload_csv() {
    local file=$1
    local table=$2
    local schema_file=$3

    echo -n "  Uploading $table... "

    if [ -f "$file" ]; then
        if [ -n "$schema_file" ] && [ -f "$schema_file" ]; then
            bq load --source_format=CSV --skip_leading_rows=1 --replace \
                "${PROJECT_ID}:${DATASET_ID}.${table}" "$file" "$schema_file" 2>/dev/null
        else
            bq load --source_format=CSV --autodetect --replace \
                "${PROJECT_ID}:${DATASET_ID}.${table}" "$file" 2>/dev/null
        fi
        echo -e "${GREEN}✓${NC}"
    else
        echo -e "${RED}✗ File not found: $file${NC}"
        return 1
    fi
}

# Upload core data files
echo "Uploading core data tables..."
upload_csv "features_v2/v2_fact_lineitem.csv" "fact_lineitem"
upload_csv "features_v2/v2_features_weekly.csv" "features_weekly"
upload_csv "features_v2/v2_features_category.csv" "features_category"
upload_csv "features_v2/v2_dim_products.csv" "dim_products"
upload_csv "features_v2/v2_dim_customers.csv" "dim_customers"

# Upload customer normalization tables
echo ""
echo "Uploading customer normalization tables..."
upload_csv "customer_master_mapping.csv" "customer_master_mapping"
upload_csv "customer_id_lookup.csv" "customer_id_lookup"

# Upload model predictions
echo ""
echo "Uploading model predictions..."
upload_csv "model_evaluation/sku_predictions_XGBoost.csv" "predictions_sku_xgboost"
upload_csv "model_evaluation/category_predictions_XGBoost.csv" "predictions_category_xgboost"
upload_csv "model_evaluation/customer_predictions_XGBoost_normalized.csv" "predictions_customer_xgboost"

# Upload MAPE summaries
echo ""
echo "Uploading MAPE summaries..."
upload_csv "model_evaluation/sku_mape_summary.csv" "mape_sku"
upload_csv "model_evaluation/category_mape_summary.csv" "mape_category"
upload_csv "model_evaluation/customer_mape_summary.csv" "mape_customer"
upload_csv "model_evaluation/model_summary.csv" "model_summary"

echo ""
echo -e "${GREEN}✓ All data uploaded successfully${NC}"

# ============================================================================
# PHASE 4: TRAIN BIGQUERY ML MODELS (Optional)
# ============================================================================
if [ "$SKIP_MODELS" = true ]; then
    echo ""
    echo -e "${YELLOW}[PHASE 4/4] SKIPPING BIGQUERY ML MODELS (--skip-models)${NC}"
else
    echo ""
    echo -e "${BLUE}[PHASE 4/4] TRAINING BIGQUERY ML MODELS${NC}"
    echo "============================================================"
    echo "This may take 5-15 minutes..."

    # Create normalized customer features view
    echo "  Creating features_customer_normalized..."
    bq query --use_legacy_sql=false --replace << EOSQL
    CREATE OR REPLACE TABLE \`${PROJECT_ID}.${DATASET_ID}.features_customer_normalized\` AS
    WITH
    lineitem_with_master AS (
      SELECT
        f.*,
        COALESCE(c.master_customer_id, SAFE_CAST(f.customer_id AS INT64)) AS master_customer_id,
        COALESCE(c.customer_name, f.customer_name) AS normalized_customer_name
      FROM \`${PROJECT_ID}.${DATASET_ID}.fact_lineitem\` f
      LEFT JOIN \`${PROJECT_ID}.${DATASET_ID}.customer_id_lookup\` c
        ON CAST(f.customer_id AS STRING) = CAST(c.original_customer_id AS STRING)
    ),
    weekly_agg AS (
      SELECT
        master_customer_id,
        normalized_customer_name AS customer_name,
        year_week,
        SUM(quantity) AS quantity,
        SUM(line_total) AS revenue,
        COUNT(DISTINCT invoice_id) AS order_count,
        COUNT(DISTINCT sku) AS sku_count,
        AVG(unit_price) AS avg_unit_price
      FROM lineitem_with_master
      GROUP BY master_customer_id, normalized_customer_name, year_week
    ),
    with_lags AS (
      SELECT
        *,
        LAG(quantity, 1) OVER (PARTITION BY master_customer_id ORDER BY year_week) AS lag1_quantity,
        LAG(quantity, 2) OVER (PARTITION BY master_customer_id ORDER BY year_week) AS lag2_quantity,
        LAG(quantity, 4) OVER (PARTITION BY master_customer_id ORDER BY year_week) AS lag4_quantity,
        AVG(quantity) OVER (
          PARTITION BY master_customer_id
          ORDER BY year_week
          ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
        ) AS rolling_avg_4w,
        CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
        CASE WHEN year_week <= '2025-W26' THEN 'H1' ELSE 'H2' END AS period
      FROM weekly_agg
    )
    SELECT * FROM with_lags
    WHERE lag1_quantity IS NOT NULL;
EOSQL
    echo -e "  ${GREEN}✓${NC}"

    # Train XGBoost model for SKU level
    echo "  Training SKU XGBoost model..."
    bq query --use_legacy_sql=false << EOSQL
    CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_ID}.model_sku_xgboost\`
    OPTIONS(
      model_type = 'BOOSTED_TREE_REGRESSOR',
      input_label_cols = ['weekly_quantity'],
      num_parallel_tree = 100,
      max_tree_depth = 5,
      learn_rate = 0.1,
      data_split_method = 'NO_SPLIT'
    ) AS
    SELECT
      weekly_quantity,
      lag1_quantity,
      lag2_quantity,
      lag4_quantity,
      rolling_avg_4w,
      avg_unit_price,
      CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) AS week_num,
      order_count
    FROM \`${PROJECT_ID}.${DATASET_ID}.features_weekly\`
    WHERE CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) <= 26
      AND lag4_quantity IS NOT NULL;
EOSQL
    echo -e "  ${GREEN}✓${NC}"

    # Train XGBoost model for Customer level
    echo "  Training Customer XGBoost model..."
    bq query --use_legacy_sql=false << EOSQL
    CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_ID}.model_customer_xgboost\`
    OPTIONS(
      model_type = 'BOOSTED_TREE_REGRESSOR',
      input_label_cols = ['quantity'],
      num_parallel_tree = 100,
      max_tree_depth = 5,
      learn_rate = 0.1,
      data_split_method = 'NO_SPLIT'
    ) AS
    SELECT
      quantity,
      lag1_quantity,
      lag2_quantity,
      lag4_quantity,
      rolling_avg_4w,
      avg_unit_price,
      week_num,
      order_count
    FROM \`${PROJECT_ID}.${DATASET_ID}.features_customer_normalized\`
    WHERE period = 'H1'
      AND lag4_quantity IS NOT NULL;
EOSQL
    echo -e "  ${GREEN}✓${NC}"

    echo ""
    echo -e "${GREEN}✓ BigQuery ML models trained successfully${NC}"
fi

# ============================================================================
# SUMMARY
# ============================================================================
echo ""
echo -e "${BLUE}"
echo "============================================================"
echo "          DEPLOYMENT COMPLETE"
echo "============================================================"
echo -e "${NC}"
echo "Project:  $PROJECT_ID"
echo "Dataset:  $DATASET_ID"
echo ""
echo "Tables uploaded:"
echo "  - fact_lineitem"
echo "  - features_weekly"
echo "  - features_category"
echo "  - features_customer_normalized"
echo "  - customer_master_mapping"
echo "  - customer_id_lookup"
echo "  - predictions_sku_xgboost"
echo "  - predictions_category_xgboost"
echo "  - predictions_customer_xgboost"
echo "  - mape_sku, mape_category, mape_customer"
echo "  - model_summary"
echo ""
if [ "$SKIP_MODELS" = false ]; then
echo "Models created:"
echo "  - model_sku_xgboost"
echo "  - model_customer_xgboost"
echo ""
fi
echo "To query the data:"
echo "  bq query --use_legacy_sql=false \\"
echo "    'SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.model_summary\`'"
echo ""
echo "To view predictions:"
echo "  bq query --use_legacy_sql=false \\"
echo "    'SELECT * FROM \`${PROJECT_ID}.${DATASET_ID}.predictions_sku_xgboost\` LIMIT 10'"
echo ""
