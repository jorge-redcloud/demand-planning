#!/bin/bash
# Check existing BigQuery data and setup versioned structure
# ==========================================================
# Run: chmod +x scripts/CHECK_AND_SETUP_BIGQUERY.sh && ./scripts/CHECK_AND_SETUP_BIGQUERY.sh

PROJECT="mimetic-maxim-443710-s2"
DATASET="redai_demand_forecast"

echo "=============================================="
echo "BIGQUERY DATA CHECK & SETUP"
echo "Dataset: $PROJECT:$DATASET"
echo "=============================================="

# 1. Check existing predictions structure
echo ""
echo "[1/6] Checking existing predictions_sku structure..."
bq show --schema $PROJECT:$DATASET.predictions_sku

echo ""
echo "[2/6] Sample from predictions_sku (check if versioned)..."
bq query --nouse_legacy_sql "
SELECT * FROM \`$PROJECT.$DATASET.predictions_sku\` LIMIT 5
"

echo ""
echo "[3/6] Checking eval_summary..."
bq query --nouse_legacy_sql "
SELECT * FROM \`$PROJECT.$DATASET.eval_summary\` LIMIT 10
"

echo ""
echo "[4/6] Checking features_sku_weekly structure..."
bq show --schema $PROJECT:$DATASET.features_sku_weekly

echo ""
echo "[5/6] Row counts for all tables..."
bq query --nouse_legacy_sql "
SELECT table_id, row_count
FROM \`$PROJECT.$DATASET.__TABLES__\`
ORDER BY table_id
"

echo ""
echo "[6/6] Check if model_version column exists in predictions..."
bq query --nouse_legacy_sql "
SELECT column_name, data_type
FROM \`$PROJECT.$DATASET.INFORMATION_SCHEMA.COLUMNS\`
WHERE table_name = 'predictions_sku'
"

echo ""
echo "=============================================="
echo "CHECK COMPLETE - Copy output above"
echo "=============================================="
