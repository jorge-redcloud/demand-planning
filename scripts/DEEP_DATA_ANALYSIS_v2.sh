#!/bin/bash
# =============================================================================
# DEEP_DATA_ANALYSIS_v2.sh - Fixed column names based on actual schema
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="demand_forecasting"

echo "=============================================="
echo "DEEP DATA ANALYSIS v2 - Understanding the Data"
echo "=============================================="
echo ""

# -----------------------------------------------------------------------------
echo "1. ALL WEEKS - What data do we actually have?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  year_week,
  COUNT(DISTINCT sku) as unique_skus,
  CAST(SUM(quantity) AS INT64) as total_units,
  CAST(SUM(line_total) AS INT64) as total_revenue,
  COUNT(*) as line_items,
  COUNT(DISTINCT invoice_id) as unique_invoices
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY year_week
ORDER BY year_week
"
echo ""

# -----------------------------------------------------------------------------
echo "2. CUSTOMER DATA - Do we have customer info?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  CASE
    WHEN customer_id IS NULL THEN 'NULL'
    WHEN customer_id = '' THEN 'EMPTY'
    ELSE 'HAS_ID'
  END as customer_status,
  CASE
    WHEN customer_name IS NULL THEN 'NULL'
    WHEN customer_name = '' THEN 'EMPTY'
    ELSE 'HAS_NAME'
  END as name_status,
  COUNT(*) as line_items,
  COUNT(DISTINCT invoice_id) as invoices,
  CAST(SUM(quantity) AS INT64) as total_units
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY 1, 2
ORDER BY line_items DESC
"
echo ""

# -----------------------------------------------------------------------------
echo "3. REGION DISTRIBUTION - Where is the data from?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  region_name,
  COUNT(*) as line_items,
  COUNT(DISTINCT invoice_id) as invoices,
  CAST(SUM(quantity) AS INT64) as total_units,
  CAST(SUM(line_total) AS INT64) as total_revenue,
  COUNT(DISTINCT sku) as unique_skus
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY region_name
ORDER BY total_units DESC
"
echo ""

# -----------------------------------------------------------------------------
echo "4. WEEK-BY-WEEK BY REGION - Where are the gaps?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  year_week,
  region_name,
  COUNT(DISTINCT invoice_id) as invoices,
  CAST(SUM(quantity) AS INT64) as units,
  CAST(SUM(line_total) AS INT64) as revenue
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY year_week, region_name
ORDER BY year_week, units DESC
"
echo ""

# -----------------------------------------------------------------------------
echo "5. INVOICE SIZE DISTRIBUTION - Bulk vs Regular Orders"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
WITH invoice_sizes AS (
  SELECT
    invoice_id,
    year_week,
    region_name,
    COUNT(*) as line_items,
    CAST(SUM(quantity) AS INT64) as total_units,
    CAST(SUM(line_total) AS INT64) as total_revenue
  FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
  GROUP BY invoice_id, year_week, region_name
)
SELECT
  CASE
    WHEN total_units < 100 THEN '1. Small (<100 units)'
    WHEN total_units < 1000 THEN '2. Medium (100-1K)'
    WHEN total_units < 10000 THEN '3. Large (1K-10K)'
    WHEN total_units < 100000 THEN '4. Bulk (10K-100K)'
    ELSE '5. Mega (>100K)'
  END as order_size,
  COUNT(*) as invoice_count,
  CAST(SUM(total_units) AS INT64) as total_units,
  CAST(SUM(total_revenue) AS INT64) as total_revenue,
  ROUND(SUM(total_units) * 100.0 / (SELECT SUM(quantity) FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`), 1) as pct_of_units
FROM invoice_sizes
GROUP BY 1
ORDER BY 1
"
echo ""

# -----------------------------------------------------------------------------
echo "6. TOP 30 INVOICES BY SIZE - Who are the bulk buyers?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  invoice_id,
  year_week,
  region_name,
  customer_id,
  customer_name,
  COUNT(*) as line_items,
  CAST(SUM(quantity) AS INT64) as total_units,
  CAST(SUM(line_total) AS INT64) as total_revenue
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY invoice_id, year_week, region_name, customer_id, customer_name
ORDER BY total_units DESC
LIMIT 30
"
echo ""

# -----------------------------------------------------------------------------
echo "7. CATEGORY BY WEEK - What sells when?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  year_week,
  category,
  CAST(SUM(quantity) AS INT64) as units,
  CAST(SUM(line_total) AS INT64) as revenue
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
WHERE category IS NOT NULL
GROUP BY year_week, category
HAVING SUM(quantity) > 10000
ORDER BY year_week, units DESC
"
echo ""

# -----------------------------------------------------------------------------
echo "8. DATA GAPS - Which weeks are MISSING from source files?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
WITH all_weeks AS (
  SELECT week FROM UNNEST(['2025-W01','2025-W02','2025-W03','2025-W04','2025-W05',
    '2025-W06','2025-W07','2025-W08','2025-W09','2025-W10','2025-W11','2025-W12',
    '2025-W13','2025-W14','2025-W15','2025-W16','2025-W17','2025-W18']) as week
),
data_weeks AS (
  SELECT DISTINCT year_week FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
)
SELECT
  a.week,
  CASE WHEN d.year_week IS NULL THEN '❌ MISSING' ELSE '✓ Has Data' END as status
FROM all_weeks a
LEFT JOIN data_weeks d ON a.week = d.year_week
ORDER BY a.week
"
echo ""

# -----------------------------------------------------------------------------
echo "9. REGION × WEEK HEATMAP - Full Coverage Check"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  region_name,
  COUNTIF(year_week = '2025-W01') as W01,
  COUNTIF(year_week = '2025-W02') as W02,
  COUNTIF(year_week = '2025-W03') as W03,
  COUNTIF(year_week = '2025-W04') as W04,
  COUNTIF(year_week = '2025-W05') as W05,
  COUNTIF(year_week = '2025-W09') as W09,
  COUNTIF(year_week = '2025-W14') as W14,
  COUNTIF(year_week = '2025-W15') as W15,
  COUNTIF(year_week = '2025-W16') as W16,
  COUNTIF(year_week = '2025-W17') as W17,
  COUNTIF(year_week = '2025-W18') as W18
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY region_name
ORDER BY region_name
"
echo ""

echo "=============================================="
echo "ANALYSIS COMPLETE"
echo "=============================================="
