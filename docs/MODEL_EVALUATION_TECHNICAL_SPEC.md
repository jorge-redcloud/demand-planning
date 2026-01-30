# Model Evaluation Technical Specification

**RedAI Demand Forecasting - ACA Dataset**
**Version:** 2.0
**Date:** January 2026
**Author:** RedAI Data Science Team

---

## 1. Executive Summary

This document provides technical specifications for the demand forecasting model evaluation framework. The framework evaluates XGBoost forecasting models at **SKU, Category, and Customer levels** using a walk-forward backtesting methodology on historical ACA (South African FMCG distributor) transaction data.

### Key Results (XGBoost - All Levels)

| Forecast Level | Model | Best MAPE | Worst MAPE | Notes |
|----------------|-------|-----------|------------|-------|
| **SKU** | XGBoost | 15.0% | 215.0% | Top 50 SKUs by volume |
| **Category** | XGBoost | 29.0% | 63.4% | 9 product categories |
| **Customer** | XGBoost | 49.0% | 406.8% | Top 30 customers by volume |

**Key Insight:** Category-level forecasting shows the most consistent performance (29-63% MAPE) because aggregation smooths out individual SKU/customer volatility.

---

## 2. Data Specifications

### 2.1 Dataset Overview

| Metric | Value |
|--------|-------|
| Total Line Items | 436,176 |
| Unique SKUs | 1,575 |
| Unique Customers | 1,142 |
| Unique Categories | 9 |
| Date Range | 2024-W03 to 2025-W52 |
| Training Period (H1) | 2024-W03 to 2025-W26 (29 weeks) |
| Test Period (H2) | 2025-W27 to 2025-W52 (26 weeks) |

### 2.2 Data Sources

```
features_v2/
├── v2_fact_lineitem.csv      # Transaction-level data (436K rows)
├── v2_dim_products.csv       # Product master with categories
├── v2_dim_customers.csv      # Customer master data
└── v2_features_weekly.csv    # Pre-computed weekly features
```

### 2.3 Feature Engineering

All three model levels (SKU, Category, Customer) use **identical features** for fair comparison:

| Feature | Description | Formula |
|---------|-------------|---------|
| `lag1` | Previous week demand | y(t-1) |
| `lag2` | 2 weeks ago demand | y(t-2) |
| `lag4` | 4 weeks ago demand (monthly pattern) | y(t-4) |
| `rolling_avg_4w` | 4-week rolling average | mean(y(t-1), y(t-2), y(t-3), y(t-4)) |
| `week_num` | Week of year (seasonality) | EXTRACT(WEEK FROM date) |

**Feature Creation SQL:**
```sql
SELECT
  *,
  LAG(quantity, 1) OVER (PARTITION BY entity ORDER BY year_week) as lag1,
  LAG(quantity, 2) OVER (PARTITION BY entity ORDER BY year_week) as lag2,
  LAG(quantity, 4) OVER (PARTITION BY entity ORDER BY year_week) as lag4,
  AVG(quantity) OVER (
    PARTITION BY entity
    ORDER BY year_week
    ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING
  ) as rolling_avg_4w,
  CAST(REGEXP_EXTRACT(year_week, r'W(\d+)') AS INT64) as week_num
FROM weekly_aggregated_data
```

---

## 3. Validation Methodology

### 3.1 Walk-Forward Validation

```
Full Timeline:     2024-W03 ──────────────────────────────────── 2025-W52
                         │                                          │
Training (H1):     2024-W03 ────────── 2025-W26                     │
                         │              │                           │
                         └── 29 weeks ──┘                           │
                                                                    │
Testing (H2):                          2025-W27 ─────────────── 2025-W52
                                            │                       │
                                            └── 26 weeks forecast ──┘
```

### 3.2 Walk-Forward Process

For each week in H2, the model:
1. Uses features calculated from **all available history** up to that point
2. Makes a prediction for the current week
3. Updates history with the **actual value** (not the prediction)
4. Moves to the next week

This ensures realistic evaluation - the model sees actuals as they become available, just like in production.

### 3.3 Why This Approach?

| Aspect | Benefit |
|--------|---------|
| **No Data Leakage** | Test data is strictly in the future |
| **Realistic** | Simulates actual production forecasting |
| **Fair Comparison** | Same methodology across all levels |
| **Seasonal Validation** | H1→H2 tests ability to generalize across seasons |

---

## 4. Model Specifications

### 4.1 XGBoost Configuration

**Same configuration for all three levels:**

```python
GradientBoostingRegressor(
    n_estimators=50,      # Number of boosting rounds
    max_depth=3,          # Shallow trees to prevent overfitting
    learning_rate=0.1,    # Step size shrinkage
    random_state=42       # Reproducibility
)
```

**BigQuery ML Equivalent:**
```sql
CREATE MODEL `project.dataset.xgb_model`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['quantity'],
  max_iterations = 50,
  max_tree_depth = 3,
  learn_rate = 0.1,
  tree_method = 'HIST',
  early_stop = TRUE
) AS
SELECT quantity, lag1, lag2, lag4, rolling_avg_4w, week_num
FROM training_data;
```

### 4.2 Feature Importance

Based on XGBoost feature importance:

| Feature | Importance | Interpretation |
|---------|------------|----------------|
| `lag1` | ~35% | Most recent week is strongest predictor |
| `rolling_avg_4w` | ~25% | Recent trend matters |
| `lag4` | ~20% | Monthly patterns exist |
| `week_num` | ~12% | Some seasonality |
| `lag2` | ~8% | 2-week pattern weaker |

---

## 5. Evaluation Metrics

### 5.1 Primary Metric: MAPE

**Mean Absolute Percentage Error (using median for robustness):**

```
MAPE = median( |Actual - Predicted| / Actual × 100 )
```

We use **median** instead of mean because:
- Resistant to outliers (e.g., promotional spikes)
- Better represents "typical" prediction accuracy
- Mean MAPE can be skewed by a few large errors

### 5.2 Secondary Metrics

| Metric | Formula | Use Case |
|--------|---------|----------|
| **MAE** | mean(\|Actual - Predicted\|) | Interpretable error in units |
| **RMSE** | √(mean((Actual - Predicted)²)) | Penalizes large errors |
| **Bias** | mean(Predicted - Actual) | Detects systematic over/under prediction |

### 5.3 Results by Level

**SKU Level (Top 50 by volume):**
```
Best:  SKU 12130 (BB CEMENT 32.5R)     - MAPE: 15.0%
Worst: Various low-volume SKUs         - MAPE: 215.0%
```

**Category Level (9 categories):**
```
Best:  Home & Garden                   - MAPE: 29.0%
Worst: Unknown                         - MAPE: 63.4%
```

**Customer Level (Top 30 by volume):**
```
Best:  Customer 46                     - MAPE: 49.0%
Worst: Various volatile customers      - MAPE: 406.8%
```

---

## 6. BigQuery ML Implementation

### 6.1 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    BigQuery Dataset                         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  Source Data:                                               │
│  ├── fact_lineitem (transaction data)                       │
│  ├── dim_products                                           │
│  └── dim_customers                                          │
│                                                             │
│  Feature Tables:                                            │
│  ├── features_sku_weekly                                    │
│  ├── features_category_weekly                               │
│  └── features_customer_weekly                               │
│                                                             │
│  Models:                                                    │
│  ├── xgb_sku_model                                          │
│  ├── xgb_category_model                                     │
│  └── xgb_customer_model                                     │
│                                                             │
│  Predictions:                                               │
│  ├── predictions_sku                                        │
│  ├── predictions_category                                   │
│  └── predictions_customer                                   │
│                                                             │
│  Evaluation:                                                │
│  ├── eval_summary                                           │
│  ├── mape_by_sku                                            │
│  ├── mape_by_category                                       │
│  └── mape_by_customer                                       │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### 6.2 Deployment Instructions

**Prerequisites:**
- Google Cloud SDK installed (`gcloud`, `bq` commands)
- Authenticated: `gcloud auth login`
- BigQuery API enabled

**Deploy:**
```bash
# Navigate to scripts directory
cd scripts/

# Run deployment (replace with your project)
./upload_to_bigquery.sh YOUR_PROJECT_ID redai_demand_forecast
```

**What it does:**
1. Creates BigQuery dataset
2. Uploads source data (fact_lineitem, dim_products, dim_customers)
3. Creates feature tables with lag features
4. Trains 3 XGBoost models
5. Generates H2 predictions
6. Calculates evaluation metrics

### 6.3 Querying Predictions

```sql
-- Get predictions for a specific SKU
SELECT year_week, actual, predicted, pct_error
FROM `project.dataset.predictions_sku`
WHERE sku = '12130'
ORDER BY year_week;

-- Get model performance summary
SELECT * FROM `project.dataset.eval_summary`;

-- Get best-performing categories
SELECT * FROM `project.dataset.mape_by_category`
ORDER BY median_mape;
```

---

## 7. Files Reference

### 7.1 Scripts

| File | Purpose |
|------|---------|
| `scripts/bigquery_xgboost_all_levels.sql` | Full BigQuery ML implementation |
| `scripts/upload_to_bigquery.sh` | Deployment script |

### 7.2 Model Outputs (Local)

| File | Description |
|------|-------------|
| `model_evaluation/sku_predictions_XGBoost.csv` | SKU-level predictions |
| `model_evaluation/category_predictions_XGBoost.csv` | Category-level predictions |
| `model_evaluation/customer_predictions_XGBoost.csv` | Customer-level predictions |
| `model_evaluation/sku_h1_actuals.csv` | H1 training data for SKUs |
| `model_evaluation/category_h1_actuals_v2.csv` | H1 training data for categories |
| `model_evaluation/customer_h1_actuals_v2.csv` | H1 training data for customers |

### 7.3 Dashboard

| File | Description |
|------|-------------|
| `demand_forecast_dashboard.html` | Interactive visualization |
| `dashboard_data_v2.js` | Dashboard data (JSON) |

---

## 8. Recommendations

### 8.1 For Production Deployment

1. **Use BigQuery ML** for scalability - handles all 1,575 SKUs efficiently
2. **Retrain monthly** - models should be refreshed with new data
3. **Monitor MAPE drift** - alert if performance degrades >10%
4. **Focus on Category level** - most reliable predictions for planning

### 8.2 Model Improvements

| Improvement | Expected Impact | Effort |
|-------------|-----------------|--------|
| Add price features | +5-10% accuracy | Low |
| Add promotion flags | +10-15% for promo SKUs | Medium |
| Add weather data | +2-5% for seasonal items | Medium |
| Ensemble methods | +3-5% overall | High |
| Deep learning (LSTM) | Variable | High |

### 8.3 Business Application

| Use Case | Recommended Level | Why |
|----------|-------------------|-----|
| **Inventory Planning** | Category | Most stable, 29-63% MAPE |
| **Replenishment** | SKU (top movers) | Use SKUs with <30% MAPE |
| **Customer Allocation** | Customer | High variance - use with caution |
| **Promotion Planning** | SKU + Category | Combine for campaign planning |

---

## 9. Appendix

### 9.1 MAPE Interpretation Guide

| MAPE Range | Interpretation | Business Impact |
|------------|----------------|-----------------|
| < 20% | Excellent | Reliable for operational decisions |
| 20-40% | Good | Useful for planning, add safety stock |
| 40-60% | Fair | Directional guidance only |
| > 60% | Poor | Investigate data quality or patterns |
| > 100% | Very Poor | Model predicting wrong direction |

### 9.2 Why MAPE > 100% Happens

MAPE > 100% means predictions are more than double (or less than half) of actuals:

```
Example:
  Actual = 306 units
  Predicted = 19,703 units
  MAPE = |306 - 19,703| / 306 = 6,337%
```

**Common causes:**
- Very volatile demand patterns (bulk ordering customers)
- Sporadic/intermittent demand
- Promotional spikes not captured
- New products with limited history

**Recommendation:** For entities with MAPE > 100%, use simple methods (e.g., safety stock = 2x average) instead of model predictions.
