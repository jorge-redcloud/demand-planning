#!/bin/bash
# Sync V4 Models to BigQuery
# ===========================
# This script:
# 1. Marks existing predictions as V1
# 2. Uploads V4 predictions (appending to existing tables)
# 3. Updates evaluation summary
#
# Run: chmod +x scripts/SYNC_V4_TO_BIGQUERY.sh && ./scripts/SYNC_V4_TO_BIGQUERY.sh

PROJECT="mimetic-maxim-443710-s2"
DATASET="redai_demand_forecast"
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "=============================================="
echo "SYNC V4 MODELS TO BIGQUERY"
echo "Project: $PROJECT"
echo "Dataset: $DATASET"
echo "=============================================="

# Step 1: Add model_version column to existing tables and mark as V1
echo ""
echo "[1/5] Adding model_version column to existing predictions..."

# SKU
bq query --nouse_legacy_sql "
ALTER TABLE \`$PROJECT.$DATASET.predictions_sku\`
ADD COLUMN IF NOT EXISTS model_version STRING;
"
bq query --nouse_legacy_sql "
UPDATE \`$PROJECT.$DATASET.predictions_sku\`
SET model_version = 'V1'
WHERE model_version IS NULL;
"
echo "  ✓ predictions_sku updated"

# Category
bq query --nouse_legacy_sql "
ALTER TABLE \`$PROJECT.$DATASET.predictions_category\`
ADD COLUMN IF NOT EXISTS model_version STRING;
"
bq query --nouse_legacy_sql "
UPDATE \`$PROJECT.$DATASET.predictions_category\`
SET model_version = 'V1'
WHERE model_version IS NULL;
"
echo "  ✓ predictions_category updated"

# Customer
bq query --nouse_legacy_sql "
ALTER TABLE \`$PROJECT.$DATASET.predictions_customer\`
ADD COLUMN IF NOT EXISTS model_version STRING;
"
bq query --nouse_legacy_sql "
UPDATE \`$PROJECT.$DATASET.predictions_customer\`
SET model_version = 'V1'
WHERE model_version IS NULL;
"
echo "  ✓ predictions_customer updated"

# Step 2: Prepare V4 data files (run Python script)
echo ""
echo "[2/5] Preparing V4 data files..."
python3 "$BASE_DIR/scripts/PREPARE_V4_FOR_BIGQUERY.py"

# Step 3: Upload V4 SKU predictions (append)
echo ""
echo "[3/5] Uploading V4 SKU predictions..."
bq load --source_format=CSV --skip_leading_rows=1 --noreplace \
  $DATASET.predictions_sku \
  "$BASE_DIR/bigquery_upload/v4_predictions_sku.csv"

# Step 4: Upload V4 Category predictions (append)
echo ""
echo "[4/5] Uploading V4 Category predictions..."
bq load --source_format=CSV --skip_leading_rows=1 --noreplace \
  $DATASET.predictions_category \
  "$BASE_DIR/bigquery_upload/v4_predictions_category.csv"

# Step 5: Upload V4 Customer predictions (append)
echo ""
echo "[5/5] Uploading V4 Customer predictions..."
bq load --source_format=CSV --skip_leading_rows=1 --noreplace \
  $DATASET.predictions_customer \
  "$BASE_DIR/bigquery_upload/v4_predictions_customer.csv"

# Verify
echo ""
echo "=============================================="
echo "VERIFICATION"
echo "=============================================="

echo ""
echo "Predictions by version:"
bq query --nouse_legacy_sql "
SELECT 'SKU' as level, model_version, COUNT(*) as rows
FROM \`$PROJECT.$DATASET.predictions_sku\`
GROUP BY model_version
UNION ALL
SELECT 'Category', model_version, COUNT(*)
FROM \`$PROJECT.$DATASET.predictions_category\`
GROUP BY model_version
UNION ALL
SELECT 'Customer', model_version, COUNT(*)
FROM \`$PROJECT.$DATASET.predictions_customer\`
GROUP BY model_version
ORDER BY level, model_version
"

echo ""
echo "=============================================="
echo "SYNC COMPLETE!"
echo "=============================================="
