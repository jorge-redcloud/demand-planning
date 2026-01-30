# Model Versioning & V2 Enhancement Plan
## RedAI ACA Demand Forecasting

**Created:** January 2026
**Status:** Planning Document

---

## Executive Summary

This document defines our model versioning strategy, introduces business logic enhancements for V2 models, and establishes data sufficiency thresholds for model eligibility.

---

## 1. Model Version Definitions

### V1 Baseline Models (Current)

**Purpose:** Establish performance baseline for comparison

| Level | Model | MAPE | Predictions | Notes |
|-------|-------|------|-------------|-------|
| SKU | XGBoost | 48.9% | 16,976 | Trained on all SKUs with sufficient lag features |
| Category | XGBoost | 53.6% | 232 | All 9 categories |
| Customer | XGBoost | 72.9% | 16,793 | 770 normalized customers |

**V1 Features:**
- Lag features: lag1, lag2, lag4 quantities
- Rolling average: 4-week rolling mean
- Price features: avg_unit_price
- Temporal: week_num
- Order metrics: order_count

**V1 Limitations:**
- No pattern-specific handling (treats cyclical and bulk the same)
- No seasonality adjustment for W47 spike
- No data sufficiency filtering
- Single model for all patterns

---

### V2 Enhanced Models (Proposed)

**Purpose:** Improve accuracy through business logic and pattern-specific modeling

#### V2 Enhancements:

1. **Data Sufficiency Thresholds**
2. **Pattern Classification** (cyclical vs bulk/one-off)
3. **Seasonality Handling** (W47 Black Friday spike)
4. **Confidence Scoring**

---

## 2. Data Sufficiency Thresholds

### Threshold Definitions

| Tier | Weeks Required | Model Eligibility | Dashboard Display |
|------|----------------|-------------------|-------------------|
| **Full** | ≥20 weeks | V2 optimized model | Show prediction with confidence |
| **Marginal** | 10-19 weeks | V1 baseline model | Show prediction with disclaimer |
| **Insufficient** | <10 weeks | No model | Show "Insufficient data" message |

### Rationale:
- **20 weeks**: Enough history for lag4 features + seasonality detection
- **10 weeks**: Minimum for basic lag features (lag1, lag2)
- **<10 weeks**: Not enough for reliable time series forecasting

### Current Data Distribution:

| Level | Full (≥20) | Marginal (10-19) | Insufficient (<10) |
|-------|------------|------------------|-------------------|
| SKU | 635 (40.3%) | 234 (14.9%) | 706 (44.8%) |
| Customer | 602 (78.2%) | 131 (17.0%) | 37 (4.8%) |
| Category | 9 (100%) | 0 | 0 |

---

## 3. Pattern Classification

### Pattern Types

Based on coefficient of variation (CV) and range ratio analysis:

| Pattern | CV Range | Range Ratio | Count | % | Model Strategy |
|---------|----------|-------------|-------|---|----------------|
| **Stable** | <0.3 | - | 9 | 0.6% | Standard XGBoost |
| **Cyclical** | 0.3-0.7 | - | 108 | 6.9% | XGBoost + seasonality features |
| **Bulk/One-off** | >0.7 | >10 | 746 | 47.4% | Moving average + spike detection |
| **High Variance** | >0.7 | ≤10 | 6 | 0.4% | Ensemble with wider confidence |
| **Insufficient** | - | - | 706 | 44.8% | Baseline only or no model |

### Classification Logic:

```python
def classify_pattern(mean, std, n_weeks, min_qty, max_qty):
    cv = std / mean if mean > 0 else 0
    range_ratio = max_qty / max(min_qty, 1)

    if n_weeks < 10:
        return 'insufficient_data'
    elif cv < 0.3:
        return 'stable'
    elif cv < 0.7:
        return 'cyclical'
    elif range_ratio > 10:
        return 'bulk_oneoff'
    else:
        return 'high_variance'
```

### Model Strategy by Pattern:

#### Stable SKUs (CV < 0.3)
- **Model:** Standard XGBoost with lag features
- **Expected MAPE:** 20-35%
- **Features:** lag1, lag2, lag4, rolling_avg_4w, week_num

#### Cyclical SKUs (CV 0.3-0.7)
- **Model:** XGBoost with enhanced seasonality
- **Expected MAPE:** 35-50%
- **Additional Features:**
  - week_of_month
  - is_month_end
  - is_quarter_end
  - holiday_proximity (W47 flag)

#### Bulk/One-off SKUs (Range Ratio > 10)
- **Model:** Hybrid approach
- **Strategy:**
  - Use median (not mean) for baseline
  - Detect "spike weeks" using z-score > 2
  - For non-spike weeks: predict median
  - For potential spike weeks: flag as "high uncertainty"
- **Expected MAPE:** 60-100% (inherently unpredictable)

#### High Variance SKUs
- **Model:** Ensemble of XGBoost + simple baselines
- **Strategy:** Wide prediction intervals, conservative estimates

---

## 4. W47 Seasonality Handling (Black Friday Spike)

### Observed Pattern:

| Week | Total Quantity | vs W46 |
|------|----------------|--------|
| W46 | 1,986,929 | baseline |
| W47 | 8,831,633 | **+344%** |
| W48 | 3,591,661 | +81% |
| W49 | 1,326,219 | -33% |

### Root Cause:
- W47 corresponds to Black Friday / end-of-November promotional period
- Top SKUs show 400-1800% increases vs their average

### Handling Strategy:

#### Option A: Seasonality Feature (Recommended)
Add explicit W47 multiplier based on historical data:

```python
# Calculate SKU-specific W47 multiplier
w47_multiplier = sku_w47_qty / sku_avg_qty_non_w47

# Add as feature
df['is_w47'] = (df['week_num'] == 47).astype(int)
df['w47_multiplier'] = df['sku'].map(sku_w47_multipliers)
```

#### Option B: Exclude W47 from Training
- Train model on non-W47 data
- Apply separate W47 uplift model
- Risk: Loses predictive signal from W47 patterns

#### Option C: Quantile Regression
- Instead of point predictions, predict 10th, 50th, 90th percentiles
- W47 would naturally have wider intervals

### Recommendation:
Use **Option A** with W47 flag feature and SKU-specific multipliers for V2.

---

## 5. V2 Model Architecture

### Feature Engineering (V2)

```python
# V2 Feature Set
features_v2 = {
    # Core lag features (from V1)
    'lag1_quantity',
    'lag2_quantity',
    'lag4_quantity',
    'rolling_avg_4w',
    'avg_unit_price',
    'order_count',

    # Temporal features (V2 enhanced)
    'week_num',
    'week_of_month',        # 1-5
    'is_month_end',         # last week of month
    'is_quarter_end',       # W13, W26, W39, W52

    # Seasonality features (V2 new)
    'is_w47',               # Black Friday flag
    'is_holiday_season',    # W47-W52
    'w47_historical_mult',  # SKU-specific W47 multiplier

    # Pattern features (V2 new)
    'pattern_type',         # encoded: stable/cyclical/bulk
    'cv_score',             # coefficient of variation
    'data_sufficiency',     # full/marginal/insufficient
}
```

### Model Selection Logic (V2)

```
IF data_sufficiency == 'insufficient':
    → No prediction, show "Insufficient data"

ELIF data_sufficiency == 'marginal':
    → Use V1 baseline model
    → Show prediction with disclaimer

ELIF pattern_type == 'bulk_oneoff':
    → Use median-based model with spike detection
    → Show wide confidence intervals

ELIF pattern_type == 'cyclical':
    → Use XGBoost with seasonality features
    → Include W47 multiplier

ELSE (stable or high_variance):
    → Use standard XGBoost
    → Normal confidence intervals
```

---

## 6. BigQuery Deployment Strategy

### Tables to Create:

| Table | Purpose |
|-------|---------|
| `model_v1_sku_baseline` | V1 SKU predictions (reference) |
| `model_v1_category_baseline` | V1 Category predictions |
| `model_v1_customer_baseline` | V1 Customer predictions |
| `model_v2_sku_optimized` | V2 SKU predictions |
| `model_v2_category_optimized` | V2 Category predictions |
| `model_v2_customer_optimized` | V2 Customer predictions |
| `sku_pattern_classification` | Pattern type per SKU |
| `entity_data_sufficiency` | Sufficiency tier per entity |
| `model_comparison` | V1 vs V2 MAPE comparison |

### BigQuery ML Models:

```sql
-- V1 Baseline (keep existing)
model_sku_xgboost_v1
model_customer_xgboost_v1

-- V2 Enhanced
model_sku_xgboost_v2_stable
model_sku_xgboost_v2_cyclical
model_customer_xgboost_v2
```

---

## 7. Dashboard Integration

### Display Logic:

```javascript
function getModelDisplay(entity, entityType) {
    const sufficiency = getDataSufficiency(entity, entityType);
    const pattern = getPattern(entity, entityType);

    if (sufficiency === 'insufficient') {
        return {
            showPrediction: false,
            message: "Insufficient data for prediction",
            confidence: null
        };
    }

    if (sufficiency === 'marginal') {
        return {
            showPrediction: true,
            model: 'V1_baseline',
            disclaimer: "Limited data - using baseline model",
            confidence: 'low'
        };
    }

    // Full sufficiency - use V2
    return {
        showPrediction: true,
        model: 'V2_optimized',
        disclaimer: null,
        confidence: pattern === 'bulk_oneoff' ? 'medium' : 'high'
    };
}
```

### Visual Indicators:

| Confidence | Color | Icon |
|------------|-------|------|
| High (V2 stable/cyclical) | Green | ✓ |
| Medium (V2 bulk) | Yellow | ⚠ |
| Low (V1 marginal) | Orange | ! |
| None (insufficient) | Gray | ✗ |

---

## 8. Implementation Phases

### Phase 1: Document & Preserve V1 ✓
- [x] Document current V1 model performance
- [x] Save V1 predictions as baseline
- [x] Create BigQuery tables for V1

### Phase 2: Pattern Analysis ✓
- [x] Classify all SKUs by pattern
- [x] Analyze W47 spike
- [x] Calculate data sufficiency

### Phase 3: V2 Model Development
- [ ] Build V2 feature engineering pipeline
- [ ] Train pattern-specific models
- [ ] Implement W47 seasonality handling
- [ ] Validate V2 vs V1 improvement

### Phase 4: BigQuery Deployment
- [ ] Upload pattern classifications
- [ ] Create V2 model tables
- [ ] Update evaluation queries
- [ ] Follow BIGQUERY_DATA_SPEC.md

### Phase 5: Dashboard Update
- [ ] Add model version indicator
- [ ] Implement fallback logic
- [ ] Add confidence badges
- [ ] Show disclaimers for marginal data

---

## 9. Success Metrics

### Target Improvements:

| Metric | V1 Baseline | V2 Target | Measurement |
|--------|-------------|-----------|-------------|
| SKU MAPE (stable) | 48.9% | <35% | On stable SKUs only |
| SKU MAPE (cyclical) | 48.9% | <45% | On cyclical SKUs |
| Customer MAPE | 72.9% | <60% | On sufficient data |
| Coverage | 55% | 70% | Entities with predictions |

### Quality Gates:
- V2 must outperform V1 on entities with full data
- No regression on marginal data entities
- Clear user communication for insufficient data

---

## 10. Risk Mitigation

| Risk | Mitigation |
|------|------------|
| V2 performs worse than V1 | Keep V1 as fallback, A/B test |
| W47 handling overfits | Use cross-validation across years |
| Pattern misclassification | Add manual override capability |
| BigQuery type errors | Follow BIGQUERY_DATA_SPEC.md strictly |

---

## Appendix: File Locations

| File | Description |
|------|-------------|
| `model_evaluation/sku_pattern_analysis.csv` | SKU pattern classifications |
| `model_evaluation/customer_data_sufficiency.csv` | Customer sufficiency tiers |
| `model_evaluation/model_summary.csv` | V1 baseline metrics |
| `scripts/BIGQUERY_DATA_SPEC.md` | BigQuery type specifications |
| `scripts/DEPLOY_TO_BIGQUERY.sh` | Deployment script |
