# ACA Demand Planning - Project Status Summary
**Last Updated:** 2026-01-22

---

## 🎯 Project Overview

Building XGBoost-based demand forecasting models for ACA (wholesale distribution) at three levels:
- **SKU Level** - Individual product forecasting
- **Category Level** - Product category aggregation
- **Customer Level** - Customer demand patterns

---

## 📊 Current Model Performance

### Global Model (Best Performer)
| Metric | Value | Notes |
|--------|-------|-------|
| **WMAPE** | 62.6% | Weighted Mean Absolute Percentage Error |
| **MAPE (median)** | 50.8% | Less sensitive to outliers |
| **W47 WMAPE** | 32.8% | Black Friday predictions |
| SKUs Covered | 1,366 | All SKUs with H2 data |
| Predictions | 18,895 | Full H2 coverage (W27-W52) |

### Per-SKU Models (By Confidence)
| Level | WMAPE | High Confidence | Medium | Low |
|-------|-------|-----------------|--------|-----|
| SKU | 73.1% | 12 | 138 | 502 |
| Category | 61.7% | 0 | 4 | 5 |
| Customer | 84.8% | 0 | 20 | 485 |

**Confidence Criteria:**
- HIGH: WMAPE < 40% AND ≥15 H1 training weeks
- MEDIUM: WMAPE < 60% AND ≥10 H1 training weeks
- LOW: Otherwise

---

## 🧪 V3 Model Experiments Summary

### Key Finding
**Global model approach beats per-SKU models:**

| Approach | WMAPE | Why |
|----------|-------|-----|
| Global Model (1 model for all SKUs) | 62.6% | Learns cross-SKU patterns, more training data |
| Per-SKU Models (652 individual models) | 73.1% | Limited training data per SKU |

### V3 Experiments Conducted

| Version | Features | WMAPE | Result |
|---------|----------|-------|--------|
| V3 | 22 features + winsorization | 69.5% | ❌ Worse (overfitting) |
| V3.1 | 12 features + pattern-based | 75.0% | ❌ Worse |
| V3.2 | V2 features + W47 factor | 72.6% | ❌ Worse |
| V3.3 | Global model + W47 feature | 62.6% | ✅ Best approach |

### Lessons Learned
1. **More features ≠ better** - V3's 22 features caused overfitting
2. **Winsorization hurts** - Removing outliers removes signal
3. **Global model wins** - One model trained on all data outperforms per-SKU models
4. **W47 as feature has 0 importance** - Because W47 is in test (H2), not train (H1)
5. **Rolling average most important** - 56% feature importance

### Feature Importance (Global Model)
```
rolling_avg_4w:    0.562 ████████████████████
avg_unit_price:    0.217 ████████
lag2_quantity:     0.108 ████
week_num:          0.047 ██
lag4_quantity:     0.034 █
lag1_quantity:     0.032 █
is_w47:            0.000 (in test, not train)
```

---

## 📁 Data Pipeline

### Source Data
```
/demand planning/
├── features_v2/
│   ├── v2_features_weekly.csv      # 33,308 rows, 1,575 SKUs
│   ├── v2_features_sku_customer.csv # Customer × SKU × Week
│   ├── v2_dim_products.csv         # Product dimension with categories
│   ├── v2_dim_customers.csv        # Customer dimension
│   └── v2_price_history.csv        # Price tracking
```

### Model Outputs
```
/demand planning/model_evaluation/
├── sku_predictions_XGBoost.csv        # Best model (62.6% WMAPE)
├── best_sku_predictions.csv           # Same as above
├── category_predictions_XGBoost_v3.csv # 232 predictions
├── customer_predictions_XGBoost_v3.csv # 9,245 predictions
└── model_comparison.csv               # All model results
```

### Dashboard Files
```
/demand planning/
├── dashboard_v6.html          # Latest dashboard with filters + W47 toggle
├── dashboard_data_v8.js       # Data file with confidence lists
└── v3_*.txt                   # Training logs
```

---

## 🔧 Issues Found & Fixed

### Issue 1: MAPE Calculation Bug
- **Problem:** Used MEDIAN instead of MEAN, hiding large errors
- **Fix:** Switched to WMAPE (Weighted MAPE)

### Issue 2: Data Sufficiency Miscalculation
- **Problem:** `n_weeks` counted total weeks (H1+H2), not just training data
- **Fix:** Now counts only H1 training weeks

### Issue 3: False High Confidence Labels
- **Problem:** 634 SKUs marked "full" but many had 0 H1 weeks
- **Fix:** Confidence now based on actual H1 weeks + WMAPE performance

### Issue 4: Per-SKU vs Global Model
- **Problem:** Per-SKU models have limited training data
- **Fix:** Global model approach performs better (62.6% vs 73.1%)

---

## 🚀 Model Versions

### V2 (Current Best)
- **Approach:** Global XGBoost model trained on all SKUs
- **Features:** lag1, lag2, lag4, rolling_avg_4w, avg_unit_price, week_num
- **WMAPE:** 62.6%
- **MAPE (median):** 50.8%

### V3 Experiments
- Multiple approaches tested (per-SKU, enhanced features, outlier removal)
- None beat V2 global model
- See experiments table above

### Recommended for Production: V2 Global Model
The global model approach is superior because:
1. More training data per model
2. Learns cross-SKU patterns
3. Simpler to deploy and maintain
4. Better generalization

---

## 📤 BigQuery Status

### Check Status:
Run: `python3 scripts/CHECK_BIGQUERY_STATUS.py`
(Update PROJECT_ID in script first)

### BigQuery Files Ready:
```
/scripts/
├── bigquery_v1_v2_models.sql       # V1/V2 model SQL
├── bigquery_xgboost_all_levels.sql # All-levels deployment
├── BIGQUERY_DATA_SPEC.md           # Data type specifications
└── bigquery_prevalidate.py         # CSV validation script
```

---

## 🛠️ Scripts Created

| Script | Purpose |
|--------|---------|
| `TRAIN_ALL_MODELS.py` | Train per-SKU XGBoost models |
| `TRAIN_V3_MODELS.py` | V3 with 22 features (overfitting) |
| `TRAIN_V3_1_MODELS.py` | V3.1 with pattern-based models |
| `TRAIN_V3_2_MODELS.py` | V3.2 with W47 factor |
| `TRAIN_V3_3_GLOBAL.py` | V3.3 global model (best) |
| `MODEL_EVALUATION.py` | Multi-model comparison framework |
| `CHECK_BIGQUERY_STATUS.py` | Check BigQuery deployment |
| `bigquery_prevalidate.py` | Validate CSVs before upload |
| `extract_sku_data_v2.py` | Feature extraction |

---

## 📈 Dashboard Features (v6)

1. ✅ SKU, Category, Customer tabs
2. ✅ Confidence filter (High/Medium/Low checkboxes)
3. ✅ Items ordered by WMAPE within confidence level
4. ✅ WMAPE displayed (not misleading MAPE)
5. ✅ H1 training weeks shown
6. ✅ W47 Black Friday toggle added
7. ✅ Continuous lines (zeros for missing weeks)

---

## 🎯 Next Steps

1. **Regenerate dashboard data** with global model predictions
2. **Deploy to BigQuery:**
   - Update PROJECT_ID in CHECK_BIGQUERY_STATUS.py
   - Run validation: `python3 scripts/bigquery_prevalidate.py`
   - Upload predictions CSV
3. **For your app:**
   - Use global model approach
   - Key features: rolling_avg_4w, avg_unit_price, lag2_quantity
   - Weekly batch predictions recommended

---

## 📋 Quick Commands

```bash
# Check current model performance
cd "/mnt/demand planning"
python3 scripts/MODEL_EVALUATION.py

# Train global model
python3 scripts/TRAIN_V3_3_GLOBAL.py

# Check BigQuery status (update PROJECT_ID first)
python3 scripts/CHECK_BIGQUERY_STATUS.py

# View dashboard
open dashboard_v6.html
```
