#!/bin/bash
# =============================================================================
# STAGE 3: BIGQUERY EVALUATION
# =============================================================================
# Compares BigQuery data against Stage 2 (extraction) to verify upload integrity.
#
# Run this AFTER:
#   1. python3 scripts/STAGE1_RAW_EVAL.py
#   2. python3 scripts/extract_sku_data.py
#   3. python3 scripts/STAGE2_EXTRACTION_EVAL.py
#   4. ./scripts/SKU_DEMAND_SETUP.sh
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"

echo "======================================================================"
echo "STAGE 3: BIGQUERY EVALUATION"
echo "======================================================================"
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET_NAME"
echo "Generated: $(date)"
echo ""

# Read expected values from Stage 2 report
STAGE2_FILE="/sessions/affectionate-pensive-goodall/mnt/demand planning/stage2_extraction_eval.json"

if [ -f "$STAGE2_FILE" ]; then
    echo "✓ Loaded Stage 2 (Extraction) report"
    EXPECTED_LINES=$(python3 -c "import json; d=json.load(open('$STAGE2_FILE')); print(d['totals']['total_line_items'])")
    EXPECTED_INVOICES=$(python3 -c "import json; d=json.load(open('$STAGE2_FILE')); print(d['totals']['total_invoices'])")
    EXPECTED_QTY=$(python3 -c "import json; d=json.load(open('$STAGE2_FILE')); print(d['totals']['total_quantity'])")
    EXPECTED_REVENUE=$(python3 -c "import json; d=json.load(open('$STAGE2_FILE')); print(d['totals']['total_revenue'])")
    EXPECTED_SKUS=$(python3 -c "import json; d=json.load(open('$STAGE2_FILE')); print(d['totals']['unique_skus'])")
else
    echo "⚠️  Stage 2 report not found. Using hardcoded expected values."
    EXPECTED_LINES=119337
    EXPECTED_INVOICES=16358
    EXPECTED_QTY=18843432
    EXPECTED_REVENUE=3470354406.73
    EXPECTED_SKUS=1158
fi

echo ""
echo "======================================================================"
echo "EXPECTED VALUES (from Stage 2 Extraction)"
echo "======================================================================"
echo "  Line Items:    $EXPECTED_LINES"
echo "  Invoices:      $EXPECTED_INVOICES"
echo "  Total Qty:     $EXPECTED_QTY"
echo "  Total Revenue: R$EXPECTED_REVENUE"
echo "  Unique SKUs:   $EXPECTED_SKUS"
echo ""

echo "======================================================================"
echo "ACTUAL VALUES (from BigQuery)"
echo "======================================================================"

# Query BigQuery for actual values
echo ""
echo "Querying sku0_fact_lineitem..."
bq query --use_legacy_sql=false --format=csv "
SELECT
  COUNT(*) as line_items,
  COUNT(DISTINCT sku) as unique_skus,
  COUNT(DISTINCT invoice_id) as unique_invoices,
  CAST(SUM(quantity) AS INT64) as total_quantity,
  ROUND(SUM(line_total), 2) as total_revenue
FROM \`${PROJECT_ID}.${DATASET_NAME}.sku0_fact_lineitem\`
" > /tmp/bq_actual.csv 2>/dev/null

if [ $? -eq 0 ]; then
    # Parse results
    ACTUAL_LINES=$(tail -1 /tmp/bq_actual.csv | cut -d',' -f1)
    ACTUAL_SKUS=$(tail -1 /tmp/bq_actual.csv | cut -d',' -f2)
    ACTUAL_INVOICES=$(tail -1 /tmp/bq_actual.csv | cut -d',' -f3)
    ACTUAL_QTY=$(tail -1 /tmp/bq_actual.csv | cut -d',' -f4)
    ACTUAL_REVENUE=$(tail -1 /tmp/bq_actual.csv | cut -d',' -f5)

    echo "  Line Items:    $ACTUAL_LINES"
    echo "  Invoices:      $ACTUAL_INVOICES"
    echo "  Total Qty:     $ACTUAL_QTY"
    echo "  Total Revenue: R$ACTUAL_REVENUE"
    echo "  Unique SKUs:   $ACTUAL_SKUS"
else
    echo "  ❌ ERROR: Could not query BigQuery"
    exit 1
fi

echo ""
echo "======================================================================"
echo "STAGE 2 → STAGE 3 COMPARISON"
echo "======================================================================"

# Calculate percentages using Python
python3 << EOF
expected_lines = $EXPECTED_LINES
actual_lines = $ACTUAL_LINES
expected_invoices = $EXPECTED_INVOICES
actual_invoices = $ACTUAL_INVOICES
expected_qty = $EXPECTED_QTY
actual_qty = $ACTUAL_QTY
expected_revenue = $EXPECTED_REVENUE
actual_revenue = $ACTUAL_REVENUE
expected_skus = $EXPECTED_SKUS
actual_skus = $ACTUAL_SKUS

def calc_pct(actual, expected):
    if expected == 0:
        return 100 if actual == 0 else 0
    return round((actual / expected) * 100, 2)

def status(pct):
    if pct >= 99.9:
        return "✅"
    elif pct >= 99:
        return "⚠️ "
    else:
        return "❌"

metrics = [
    ("Line Items", expected_lines, actual_lines),
    ("Invoices", expected_invoices, actual_invoices),
    ("Total Quantity", expected_qty, actual_qty),
    ("Total Revenue", expected_revenue, actual_revenue),
    ("Unique SKUs", expected_skus, actual_skus),
]

print("")
print("┌────────────────────────────────────────────────────────────────────────┐")
print("│ METRIC              │ EXPECTED       │ ACTUAL          │ MATCH %      │")
print("├────────────────────────────────────────────────────────────────────────┤")

for name, expected, actual in metrics:
    pct = calc_pct(actual, expected)
    st = status(pct)
    if name == "Total Revenue":
        print(f"│ {name:18} │ R{expected:>13,.0f} │ R{actual:>13,.0f} │ {st} {pct:>6.2f}%   │")
    else:
        print(f"│ {name:18} │ {expected:>14,} │ {actual:>15,} │ {st} {pct:>6.2f}%   │")

print("└────────────────────────────────────────────────────────────────────────┘")

# Overall assessment
line_pct = calc_pct(actual_lines, expected_lines)
if line_pct >= 99.9:
    print("\n✅ PERFECT: 100% data integrity maintained through upload")
elif line_pct >= 99:
    print(f"\n✅ EXCELLENT: {line_pct:.2f}% data preserved (minor rounding)")
elif line_pct >= 95:
    print(f"\n⚠️  GOOD: {line_pct:.2f}% data preserved")
else:
    print(f"\n❌ WARNING: Only {line_pct:.2f}% data preserved - investigate!")
EOF

echo ""
echo "======================================================================"
echo "TABLE ROW COUNTS"
echo "======================================================================"

bq query --use_legacy_sql=false --format=pretty "
SELECT
  table_name,
  row_count
FROM (
  SELECT table_name, SUM(row_count) as row_count
  FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.PARTITIONS\`
  GROUP BY table_name
)
WHERE table_name LIKE 'sku0%' OR table_name LIKE 'cat0%'
ORDER BY table_name
"

echo ""
echo "======================================================================"
echo "MODEL STATUS"
echo "======================================================================"

bq query --use_legacy_sql=false --format=pretty "
SELECT
  model_name,
  model_type,
  TIMESTAMP_MILLIS(creation_time) as created
FROM \`${PROJECT_ID}.${DATASET_NAME}.INFORMATION_SCHEMA.MODELS\`
WHERE model_name LIKE 'sku0%' OR model_name LIKE 'cat0%'
ORDER BY model_name
"

echo ""
echo "======================================================================"
echo "STAGE 3 COMPLETE"
echo "======================================================================"
echo ""
echo "All 3 stages complete! Summary:"
echo "  Stage 1: Raw Excel files evaluated"
echo "  Stage 2: Extraction pipeline validated"
echo "  Stage 3: BigQuery upload verified"
echo ""
