#!/bin/bash
# =============================================================================
# Move BigQuery Dataset from US to Europe-West6
# =============================================================================
#
# BigQuery no permite copiar entre regiones directamente.
# Este script: Export → GCS (EU) → Import a nuevo dataset
#
# Usage:
#   ./move_to_eu.sh
#
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
SOURCE_DATASET="redai_demand_forecast"
TARGET_DATASET="redai_demand_forecast_eu"
TARGET_LOCATION="europe-west6"
GCS_BUCKET="gs://demand_planning_aca"
GCS_EXPORT_PATH="$GCS_BUCKET/bq_export"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "============================================================"
echo "  Move BigQuery Dataset to Europe-West6"
echo "============================================================"
echo ""

# Set project
gcloud config set project $PROJECT_ID 2>/dev/null

# Check source location
echo "Checking current dataset location..."
CURRENT_LOCATION=$(bq show --format=json $SOURCE_DATASET 2>/dev/null | python3 -c "import sys,json; print(json.load(sys.stdin).get('location','unknown'))" 2>/dev/null)
echo "  $SOURCE_DATASET is in: $CURRENT_LOCATION"

if [ "$CURRENT_LOCATION" == "$TARGET_LOCATION" ]; then
    echo -e "${GREEN}✓ Dataset is already in $TARGET_LOCATION!${NC}"
    exit 0
fi

echo ""
echo "  Source: $SOURCE_DATASET ($CURRENT_LOCATION)"
echo "  Target: $TARGET_DATASET ($TARGET_LOCATION)"
echo "  GCS:    $GCS_EXPORT_PATH"
echo ""

# Get list of tables
echo "Getting list of tables..."
TABLES=$(bq ls --format=sparse $SOURCE_DATASET 2>/dev/null | awk 'NR>0 {print $1}' | grep -v "^$" | grep -v "tableId")
TABLE_COUNT=$(echo "$TABLES" | grep -c ".")
echo "Found $TABLE_COUNT tables to migrate"
echo ""

read -p "Continue with migration? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    exit 1
fi

# Step 1: Create target dataset
echo ""
echo "Step 1: Creating target dataset in $TARGET_LOCATION..."
if bq show $TARGET_DATASET > /dev/null 2>&1; then
    echo -e "${YELLOW}⚠ Target dataset already exists, will add tables to it${NC}"
else
    bq mk --location=$TARGET_LOCATION --dataset $PROJECT_ID:$TARGET_DATASET
    echo -e "${GREEN}✓ Created $TARGET_DATASET${NC}"
fi

# Step 2: Export and import each table
echo ""
echo "Step 2: Migrating tables..."
echo "------------------------------------------------------------"

MIGRATED=0
FAILED=0

for TABLE in $TABLES; do
    if [ -z "$TABLE" ]; then
        continue
    fi

    echo ""
    echo "[$TABLE]"

    # Export to GCS
    EXPORT_URI="$GCS_EXPORT_PATH/${TABLE}/*.json.gz"
    echo "  Exporting to GCS..."
    bq extract --destination_format=NEWLINE_DELIMITED_JSON --compression=GZIP \
        "$SOURCE_DATASET.$TABLE" "$GCS_EXPORT_PATH/${TABLE}/*.json.gz" 2>/dev/null

    if [ $? -ne 0 ]; then
        echo -e "  ${RED}✗ Export failed${NC}"
        ((FAILED++))
        continue
    fi

    # Import to new dataset
    echo "  Importing to $TARGET_DATASET..."
    bq load --source_format=NEWLINE_DELIMITED_JSON --autodetect --replace \
        "$TARGET_DATASET.$TABLE" "$GCS_EXPORT_PATH/${TABLE}/*.json.gz" 2>/dev/null

    if [ $? -eq 0 ]; then
        echo -e "  ${GREEN}✓ Migrated${NC}"
        ((MIGRATED++))
    else
        echo -e "  ${RED}✗ Import failed${NC}"
        ((FAILED++))
    fi
done

echo ""
echo "------------------------------------------------------------"

# Step 3: Cleanup GCS
echo ""
echo "Step 3: Cleaning up GCS export files..."
gsutil -m rm -r "$GCS_EXPORT_PATH" 2>/dev/null
echo "Done"

# Summary
echo ""
echo "============================================================"
echo "  SUMMARY"
echo "============================================================"
echo -e "  Migrated: ${GREEN}$MIGRATED${NC} tables"
if [ $FAILED -gt 0 ]; then
    echo -e "  Failed:   ${RED}$FAILED${NC} tables"
fi
echo ""
echo "  New dataset: $PROJECT_ID.$TARGET_DATASET"
echo "  Location:    $TARGET_LOCATION"
echo ""

# Verify
echo "Tables in new dataset:"
bq ls --format=sparse $TARGET_DATASET

echo ""
echo "============================================================"
echo ""
echo "Next steps:"
echo "  1. Verify data in new dataset"
echo "  2. Update your notebooks/scripts to use: $TARGET_DATASET"
echo "  3. Delete old dataset: bq rm -r -f $SOURCE_DATASET"
echo ""
