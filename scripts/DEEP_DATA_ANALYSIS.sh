#!/bin/bash
# =============================================================================
# DEEP_DATA_ANALYSIS.sh - Understand data gaps, customers, and patterns
# =============================================================================

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="demand_forecasting"

echo "=============================================="
echo "DEEP DATA ANALYSIS - Understanding the Gaps"
echo "=============================================="
echo ""

# -----------------------------------------------------------------------------
echo "1. ALL WEEKS - What data do we actually have?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  year_week,
  COUNT(DISTINCT sku) as unique_skus,
  CAST(SUM(weekly_quantity) AS INT64) as total_units,
  CAST(SUM(weekly_revenue) AS INT64) as total_revenue,
  CAST(SUM(transaction_count) AS INT64) as total_transactions
FROM \`${PROJECT_ID}.${DATASET}.sku0_features_weekly\`
GROUP BY year_week
ORDER BY year_week
"
echo ""

# -----------------------------------------------------------------------------
echo "2. DO WE HAVE CUSTOMER DATA? Check the raw line items"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  COUNT(*) as total_rows,
  COUNT(DISTINCT invoice_number) as unique_invoices,
  COUNT(DISTINCT sku) as unique_skus,
  COUNT(DISTINCT customer_id) as unique_customers,
  COUNT(DISTINCT primary_region) as unique_regions
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
"
echo ""

# -----------------------------------------------------------------------------
echo "3. CUSTOMER DISTRIBUTION - Who are the buyers?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  CASE
    WHEN customer_id IS NULL OR customer_id = '' THEN 'NO_CUSTOMER_ID'
    ELSE customer_id
  END as customer,
  COUNT(*) as line_items,
  COUNT(DISTINCT invoice_number) as invoices,
  CAST(SUM(quantity) AS INT64) as total_units,
  CAST(SUM(line_total) AS INT64) as total_revenue
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY customer
ORDER BY total_units DESC
LIMIT 20
"
echo ""

# -----------------------------------------------------------------------------
echo "4. REGION DISTRIBUTION - Where is the data from?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  primary_region,
  COUNT(*) as line_items,
  COUNT(DISTINCT invoice_number) as invoices,
  CAST(SUM(quantity) AS INT64) as total_units,
  CAST(SUM(line_total) AS INT64) as total_revenue,
  COUNT(DISTINCT sku) as unique_skus
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY primary_region
ORDER BY total_units DESC
"
echo ""

# -----------------------------------------------------------------------------
echo "5. WEEK-BY-WEEK BY REGION - Where are the gaps?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  year_week,
  primary_region,
  COUNT(DISTINCT invoice_number) as invoices,
  CAST(SUM(quantity) AS INT64) as units
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY year_week, primary_region
ORDER BY year_week, primary_region
"
echo ""

# -----------------------------------------------------------------------------
echo "6. INVOICE SIZE DISTRIBUTION - Bulk vs Regular Orders"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
WITH invoice_sizes AS (
  SELECT
    invoice_number,
    year_week,
    COUNT(*) as line_items,
    CAST(SUM(quantity) AS INT64) as total_units,
    CAST(SUM(line_total) AS INT64) as total_revenue
  FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
  GROUP BY invoice_number, year_week
)
SELECT
  CASE
    WHEN total_units < 100 THEN 'Small (<100 units)'
    WHEN total_units < 1000 THEN 'Medium (100-1K)'
    WHEN total_units < 10000 THEN 'Large (1K-10K)'
    WHEN total_units < 100000 THEN 'Bulk (10K-100K)'
    ELSE 'Mega (>100K)'
  END as order_size,
  COUNT(*) as invoice_count,
  CAST(SUM(total_units) AS INT64) as total_units,
  CAST(SUM(total_revenue) AS INT64) as total_revenue,
  ROUND(SUM(total_units) * 100.0 / SUM(SUM(total_units)) OVER(), 1) as pct_of_units
FROM invoice_sizes
GROUP BY 1
ORDER BY
  CASE
    WHEN order_size = 'Small (<100 units)' THEN 1
    WHEN order_size = 'Medium (100-1K)' THEN 2
    WHEN order_size = 'Large (1K-10K)' THEN 3
    WHEN order_size = 'Bulk (10K-100K)' THEN 4
    ELSE 5
  END
"
echo ""

# -----------------------------------------------------------------------------
echo "7. TOP 20 INVOICES BY SIZE - Who are the bulk buyers?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  invoice_number,
  year_week,
  primary_region,
  customer_id,
  COUNT(*) as line_items,
  CAST(SUM(quantity) AS INT64) as total_units,
  CAST(SUM(line_total) AS INT64) as total_revenue
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY invoice_number, year_week, primary_region, customer_id
ORDER BY total_units DESC
LIMIT 20
"
echo ""

# -----------------------------------------------------------------------------
echo "8. WHAT MONTHS/FILES DO WE HAVE DATA FROM?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT
  SUBSTR(year_week, 1, 4) as year,
  SUBSTR(year_week, 6, 2) as week_num,
  year_week,
  COUNT(DISTINCT invoice_number) as invoices,
  CAST(SUM(quantity) AS INT64) as units
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
GROUP BY year_week
ORDER BY year_week
"
echo ""

# -----------------------------------------------------------------------------
echo "9. SAMPLE RAW DATA - What does a line item look like?"
echo "----------------------------------------------"
bq query --use_legacy_sql=false --format=pretty "
SELECT *
FROM \`${PROJECT_ID}.${DATASET}.sku0_fact_lineitem\`
LIMIT 5
"
echo ""

echo "=============================================="
echo "ANALYSIS COMPLETE"
echo "=============================================="
