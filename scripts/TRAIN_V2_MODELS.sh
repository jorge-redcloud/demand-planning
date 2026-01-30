#!/bin/bash
# =============================================================================
# TRAIN V2 MODELS - Price-Aware + Buying Cycle Forecasting
# =============================================================================
# V2 Model Features:
#   - Full price history and price change indicators
#   - Customer buying cycles (Weekly/Monthly/Irregular)
#   - Bulk vs Standard buyer segmentation
#   - Category and SKU level predictions
#   - Buying cycle predictions per customer
# =============================================================================

set -e

# Configuration
PROJECT_ID="mimetic-maxim-443710-s2"
DATASET_NAME="demand_forecasting"

echo "==========================================="
echo "TRAINING V2 MODELS"
echo "Price-Aware + Buying Cycle Forecasting"
echo "==========================================="
echo ""

# =============================================================================
# MODEL 1: SKU DEMAND ARIMA (Time Series)
# =============================================================================
echo "==========================================="
echo "1/8 Creating v2_model_sku_demand_arima..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_sku_demand_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='sku',
  auto_arima=TRUE,
  holiday_region='ZA'
) AS
SELECT
  PARSE_DATE('%G-W%V', f.year_week) as week_date,
  f.sku,
  f.weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_weekly\` f
WHERE f.weekly_quantity > 0
  AND f.data_completeness IN ('complete', 'partial')
"

echo "✓ v2_model_sku_demand_arima created"
echo ""

# =============================================================================
# MODEL 2: SKU DEMAND WITH PRICE (XGBoost)
# =============================================================================
echo "==========================================="
echo "2/8 Creating v2_model_sku_demand_xgb..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_sku_demand_xgb\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1,
  early_stop=TRUE,
  min_split_loss=0.1
) AS
SELECT
  f.weekly_quantity,
  f.avg_unit_price,
  COALESCE(f.lag1_quantity, 0) as lag1_quantity,
  COALESCE(f.lag2_quantity, 0) as lag2_quantity,
  COALESCE(f.lag4_quantity, 0) as lag4_quantity,
  COALESCE(f.lag1_price, f.avg_unit_price) as lag1_price,
  COALESCE(f.lag2_price, f.avg_unit_price) as lag2_price,
  COALESCE(f.rolling_avg_4w, f.weekly_quantity) as rolling_avg_4w,
  COALESCE(f.price_rolling_avg_4w, f.avg_unit_price) as price_rolling_avg,
  COALESCE(f.price_change_pct, 0) as price_change_pct,
  f.order_count,
  f.unique_customers,
  EXTRACT(WEEK FROM PARSE_DATE('%G-W%V', f.year_week)) as week_of_year,
  EXTRACT(MONTH FROM PARSE_DATE('%G-W%V', f.year_week)) as month
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_weekly\` f
WHERE f.weekly_quantity > 0
  AND f.data_completeness IN ('complete', 'partial')
  AND f.avg_unit_price > 0
"

echo "✓ v2_model_sku_demand_xgb created (with price features)"
echo ""

# =============================================================================
# MODEL 3: CATEGORY DEMAND ARIMA
# =============================================================================
echo "==========================================="
echo "3/8 Creating v2_model_category_demand_arima..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_category_demand_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='category',
  auto_arima=TRUE,
  holiday_region='ZA'
) AS
SELECT
  PARSE_DATE('%G-W%V', f.year_week) as week_date,
  f.category,
  f.weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_category\` f
WHERE f.weekly_quantity > 0
  AND f.data_completeness IN ('complete', 'partial')
"

echo "✓ v2_model_category_demand_arima created"
echo ""

# =============================================================================
# MODEL 4: CUSTOMER SEGMENT DEMAND (Bulk vs Standard)
# =============================================================================
echo "==========================================="
echo "4/8 Creating v2_model_segment_demand_xgb..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_segment_demand_xgb\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['weekly_quantity'],
  max_iterations=100,
  learn_rate=0.1,
  early_stop=TRUE
) AS
SELECT
  sc.weekly_quantity,
  sc.avg_unit_price,
  sc.order_count,
  CASE sc.customer_segment
    WHEN 'Small Retailer' THEN 1
    WHEN 'Medium Retailer' THEN 2
    WHEN 'Large Retailer' THEN 3
    WHEN 'Bulk/Wholesale' THEN 4
    ELSE 0
  END as segment_code,
  CASE sc.buyer_type
    WHEN 'Occasional Buyer' THEN 1
    WHEN 'Regular Buyer' THEN 2
    WHEN 'Frequent Buyer' THEN 3
    WHEN 'High-Value Buyer' THEN 4
    WHEN 'Bulk Buyer' THEN 5
    ELSE 0
  END as buyer_type_code,
  EXTRACT(WEEK FROM PARSE_DATE('%G-W%V', sc.year_week)) as week_of_year,
  EXTRACT(MONTH FROM PARSE_DATE('%G-W%V', sc.year_week)) as month
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_sku_customer\` sc
WHERE sc.weekly_quantity > 0
  AND sc.data_completeness IN ('complete', 'partial')
  AND sc.customer_segment IS NOT NULL
"

echo "✓ v2_model_segment_demand_xgb created (Bulk vs Standard)"
echo ""

# =============================================================================
# MODEL 5: BULK BUYER DEMAND ARIMA
# =============================================================================
echo "==========================================="
echo "5/8 Creating v2_model_bulk_buyer_arima..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_bulk_buyer_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='customer_id',
  auto_arima=TRUE,
  holiday_region='ZA'
) AS
SELECT
  PARSE_DATE('%G-W%V', sc.year_week) as week_date,
  sc.customer_id,
  SUM(sc.weekly_quantity) as weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_sku_customer\` sc
WHERE sc.customer_segment = 'Bulk/Wholesale'
  AND sc.weekly_quantity > 0
GROUP BY sc.year_week, sc.customer_id
"

echo "✓ v2_model_bulk_buyer_arima created"
echo ""

# =============================================================================
# MODEL 6: STANDARD BUYER DEMAND ARIMA
# =============================================================================
echo "==========================================="
echo "6/8 Creating v2_model_standard_buyer_arima..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_standard_buyer_arima\`
OPTIONS(
  model_type='ARIMA_PLUS',
  time_series_timestamp_col='week_date',
  time_series_data_col='weekly_quantity',
  time_series_id_col='customer_id',
  auto_arima=TRUE,
  holiday_region='ZA'
) AS
SELECT
  PARSE_DATE('%G-W%V', sc.year_week) as week_date,
  sc.customer_id,
  SUM(sc.weekly_quantity) as weekly_quantity
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_features_sku_customer\` sc
WHERE sc.customer_segment IN ('Small Retailer', 'Medium Retailer', 'Large Retailer')
  AND sc.weekly_quantity > 0
GROUP BY sc.year_week, sc.customer_id
HAVING COUNT(DISTINCT sc.year_week) >= 4  -- Need enough data points
"

echo "✓ v2_model_standard_buyer_arima created"
echo ""

# =============================================================================
# MODEL 7: BUYING CYCLE PREDICTION (Classification)
# =============================================================================
echo "==========================================="
echo "7/8 Creating v2_model_buying_cycle_classifier..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_buying_cycle_classifier\`
OPTIONS(
  model_type='BOOSTED_TREE_CLASSIFIER',
  input_label_cols=['cycle_regularity'],
  max_iterations=50,
  learn_rate=0.1
) AS
SELECT
  cycle_regularity,
  total_orders,
  COALESCE(avg_days_between_orders, 0) as avg_days_between,
  total_units,
  total_revenue,
  avg_order_value,
  active_weeks
FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_customer_cycles\`
WHERE cycle_regularity IS NOT NULL
  AND total_orders >= 2
"

echo "✓ v2_model_buying_cycle_classifier created"
echo ""

# =============================================================================
# MODEL 8: PRICE ELASTICITY MODEL
# =============================================================================
echo "==========================================="
echo "8/8 Creating v2_model_price_elasticity..."
echo "==========================================="

bq query --use_legacy_sql=false "
CREATE OR REPLACE MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_price_elasticity\`
OPTIONS(
  model_type='BOOSTED_TREE_REGRESSOR',
  input_label_cols=['quantity_change_pct'],
  max_iterations=100,
  learn_rate=0.1,
  early_stop=TRUE
) AS
WITH price_changes AS (
  SELECT
    sku,
    year_week,
    avg_price,
    prev_avg_price,
    price_change_pct,
    weekly_quantity,
    LAG(weekly_quantity) OVER (PARTITION BY sku ORDER BY year_week) as prev_quantity
  FROM \`${PROJECT_ID}.${DATASET_NAME}.v2_price_history\`
  WHERE prev_avg_price > 0
)
SELECT
  COALESCE(price_change_pct, 0) as price_change_pct,
  avg_price,
  prev_avg_price,
  weekly_quantity,
  COALESCE(prev_quantity, weekly_quantity) as prev_quantity,
  SAFE_DIVIDE((weekly_quantity - prev_quantity), prev_quantity) * 100 as quantity_change_pct
FROM price_changes
WHERE prev_quantity > 0
  AND price_change_pct IS NOT NULL
  AND ABS(price_change_pct) > 0.1  -- Focus on meaningful price changes
"

echo "✓ v2_model_price_elasticity created"
echo ""

# =============================================================================
# MODEL EVALUATION
# =============================================================================
echo "==========================================="
echo "MODEL EVALUATION SUMMARY"
echo "==========================================="

echo ""
echo "--- SKU ARIMA Evaluation ---"
bq query --use_legacy_sql=false "
SELECT
  'v2_model_sku_demand_arima' as model,
  ROUND(AVG(mean_absolute_error), 2) as avg_mae,
  ROUND(AVG(mean_squared_error), 2) as avg_mse,
  COUNT(*) as time_series_count
FROM ML.ARIMA_EVALUATE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_sku_demand_arima\`)
"

echo ""
echo "--- Category ARIMA Evaluation ---"
bq query --use_legacy_sql=false "
SELECT
  'v2_model_category_demand_arima' as model,
  ROUND(AVG(mean_absolute_error), 2) as avg_mae,
  ROUND(AVG(mean_squared_error), 2) as avg_mse,
  COUNT(*) as time_series_count
FROM ML.ARIMA_EVALUATE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_category_demand_arima\`)
"

echo ""
echo "--- XGBoost Feature Importance (with Price) ---"
bq query --use_legacy_sql=false "
SELECT
  feature,
  ROUND(importance_gain, 4) as importance
FROM ML.FEATURE_IMPORTANCE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_sku_demand_xgb\`)
ORDER BY importance_gain DESC
LIMIT 10
"

echo ""
echo "--- Buying Cycle Classifier Metrics ---"
bq query --use_legacy_sql=false "
SELECT *
FROM ML.EVALUATE(MODEL \`${PROJECT_ID}.${DATASET_NAME}.v2_model_buying_cycle_classifier\`)
"

echo ""
echo "==========================================="
echo "✅ V2 MODELS TRAINING COMPLETE!"
echo "==========================================="
echo ""
echo "Models created:"
echo ""
echo "DEMAND FORECASTING:"
echo "  1. v2_model_sku_demand_arima      - SKU time series"
echo "  2. v2_model_sku_demand_xgb        - SKU with price features"
echo "  3. v2_model_category_demand_arima - Category time series"
echo "  4. v2_model_segment_demand_xgb    - By buyer segment"
echo ""
echo "BUYER-SPECIFIC MODELS:"
echo "  5. v2_model_bulk_buyer_arima      - Bulk buyer forecasting"
echo "  6. v2_model_standard_buyer_arima  - Standard buyer forecasting"
echo ""
echo "BEHAVIORAL MODELS:"
echo "  7. v2_model_buying_cycle_classifier - Predict customer cycle"
echo "  8. v2_model_price_elasticity        - Price sensitivity"
echo ""
echo "Key V2 model improvements:"
echo "  ✓ Price as core feature (100% coverage)"
echo "  ✓ Separate models for Bulk vs Standard buyers"
echo "  ✓ Buying cycle prediction"
echo "  ✓ Price elasticity analysis"
