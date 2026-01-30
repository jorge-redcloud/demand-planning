#!/bin/bash
# =============================================================================
# POST_EVAL.sh - Verify BigQuery upload against pre-evaluation report
# =============================================================================
# Run this AFTER SKU_DEMAND_SETUP.sh to verify data integrity
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"

echo "=============================================="
echo "POST-EVALUATION: BigQuery Verification"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_NAME"
echo ""

echo "=============================================="
echo "TABLE ROW COUNTS"
echo "=============================================="

# Get row counts for all tables
bq query --use_legacy_sql=false --format=pretty "
SELECT
  table_name,
  CASE table_type
    WHEN 'BASE TABLE' THEN 'TABLE'
    ELSE table_type
  END as type,
  row_count
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.TABLES\`
LEFT JOIN (
  SELECT table_name as tn, SUM(row_count) as row_count
  FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.PARTITIONS\`
  GROUP BY table_name
) USING (table_name)
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
echo "SKU_DEMAND_0 VERIFICATION"
echo "=============================================="

echo ""
echo "Expected from PRE_EVAL:"
echo "  Line Items: 119,337"
echo "  Unique SKUs: 1,158"
echo "  Unique Invoices: 16,358"
echo "  Total Quantity: 18,843,432 units"
echo "  Total Revenue: R3,470,354,406.73"
echo ""

echo "Actual in BigQuery:"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  COUNT(*) as line_items,
  COUNT(DISTINCT sku) as unique_skus,
  COUNT(DISTINCT invoice_id) as unique_invoices,
  CAST(SUM(quantity) AS INT64) as total_quantity,
  ROUND(SUM(line_total), 2) as total_revenue
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_fact_lineitem\`
"

echo ""
echo "=============================================="
echo "SKU FEATURES VERIFICATION"
echo "=============================================="

echo ""
echo "Expected: 4,869 rows, 1,158 SKUs, 11 weeks"
echo ""
echo "Actual:"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT sku) as unique_skus,
  COUNT(DISTINCT year_week) as unique_weeks,
  MIN(year_week) as min_week,
  MAX(year_week) as max_week
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_features_weekly\`
"

echo ""
echo "=============================================="
echo "CATEGORY FEATURES VERIFICATION"
echo "=============================================="

echo ""
echo "Expected: 285 rows, 46 categories"
echo ""
echo "Actual:"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT category) as unique_categories,
  COUNT(DISTINCT year_week) as unique_weeks
FROM \`${PROJECT_ID}.${DATASET_NAME}.cat0_features_weekly\`
"

echo ""
echo "=============================================="
echo "MODEL STATUS"
echo "=============================================="

bq query --use_legacy_sql=false --format=pretty "
SELECT
  model_name,
  model_type,
  TIMESTAMP_MILLIS(creation_time) as created
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.MODELS\`
ORDER BY model_name
"

echo ""
echo "=============================================="
echo "VERIFICATION COMPLETE"
echo "=============================================="
echo ""
echo "Compare the 'Actual' values above with the 'Expected' values."
echo "If they match, the upload was successful!"
echo ""
