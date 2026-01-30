-- BigQuery: Add Version Column and Update Structure
-- ==================================================
-- Run these queries in BigQuery Console or via bq command
-- Project: mimetic-maxim-443710-s2
-- Dataset: redai_demand_forecast

-- ============================================================================
-- STEP 1: Add model_version column to existing predictions (mark as V1)
-- ============================================================================

-- SKU Predictions: Add version column
ALTER TABLE `mimetic-maxim-443710-s2.redai_demand_forecast.predictions_sku`
ADD COLUMN IF NOT EXISTS model_version STRING;

UPDATE `mimetic-maxim-443710-s2.redai_demand_forecast.predictions_sku`
SET model_version = 'V1'
WHERE model_version IS NULL;

-- Category Predictions: Add version column
ALTER TABLE `mimetic-maxim-443710-s2.redai_demand_forecast.predictions_category`
ADD COLUMN IF NOT EXISTS model_version STRING;

UPDATE `mimetic-maxim-443710-s2.redai_demand_forecast.predictions_category`
SET model_version = 'V1'
WHERE model_version IS NULL;

-- Customer Predictions: Add version column
ALTER TABLE `mimetic-maxim-443710-s2.redai_demand_forecast.predictions_customer`
ADD COLUMN IF NOT EXISTS model_version STRING;

UPDATE `mimetic-maxim-443710-s2.redai_demand_forecast.predictions_customer`
SET model_version = 'V1'
WHERE model_version IS NULL;

-- ============================================================================
-- STEP 2: Update eval_summary to include version
-- ============================================================================

ALTER TABLE `mimetic-maxim-443710-s2.redai_demand_forecast.eval_summary`
ADD COLUMN IF NOT EXISTS model_version STRING;

UPDATE `mimetic-maxim-443710-s2.redai_demand_forecast.eval_summary`
SET model_version = 'V1'
WHERE model_version IS NULL;

-- ============================================================================
-- STEP 3: Verify the changes
-- ============================================================================

-- Check predictions_sku now has version
SELECT model_version, COUNT(*) as row_count
FROM `mimetic-maxim-443710-s2.redai_demand_forecast.predictions_sku`
GROUP BY model_version;

-- Check eval_summary
SELECT * FROM `mimetic-maxim-443710-s2.redai_demand_forecast.eval_summary`;
