# V3 Model Strategy - Killer Version
**Goal: Reduce WMAPE from 73% → <40% (SKU level)**

---

## 📊 Current Performance Baseline

| Level | Current WMAPE | Target WMAPE | Improvement Needed |
|-------|---------------|--------------|-------------------|
| SKU | 73.1% | <40% | -45% |
| Category | 61.7% | <35% | -43% |
| Customer | 84.8% | <50% | -41% |

---

## 🎯 V3 Improvement Strategies

### Strategy 1: Outlier Detection & Removal
**Expected Impact: -10-15% WMAPE**

```python
# IQR-based outlier detection
def remove_outliers(df, column, multiplier=1.5):
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower = Q1 - multiplier * IQR
    upper = Q3 + multiplier * IQR
    return df[(df[column] >= lower) & (df[column] <= upper)]

# Z-score method for extreme outliers
def remove_zscore_outliers(df, column, threshold=3):
    z_scores = (df[column] - df[column].mean()) / df[column].std()
    return df[abs(z_scores) < threshold]
```

**Implementation:**
1. Calculate per-SKU statistics (mean, std, IQR)
2. Flag outlier weeks (Z-score > 3 OR outside 1.5×IQR)
3. Option A: Remove outliers from training
4. Option B: Cap outliers at percentile bounds (winsorization)
5. Keep outlier flag as a feature (is_outlier_week)

---

### Strategy 2: Data Interpolation for Sparse SKUs
**Expected Impact: -5-10% WMAPE**

```python
def interpolate_sparse_data(df, sku_col, week_col, value_col):
    """Fill gaps in sparse time series"""
    all_weeks = generate_all_weeks()
    
    for sku in df[sku_col].unique():
        sku_data = df[df[sku_col] == sku]
        
        # Create full week range
        full_range = pd.DataFrame({week_col: all_weeks})
        merged = full_range.merge(sku_data, on=week_col, how='left')
        
        # Interpolation methods:
        # 1. Linear interpolation for short gaps (≤2 weeks)
        merged[value_col] = merged[value_col].interpolate(method='linear', limit=2)
        
        # 2. Seasonal interpolation for longer gaps
        # Use same week from previous pattern
        
        # 3. Zero-fill for truly inactive periods
        # (consecutive gaps > 4 weeks = likely discontinued)
```

**Implementation:**
1. Identify gap patterns (random vs seasonal vs discontinued)
2. Short gaps (1-2 weeks): Linear interpolation
3. Seasonal gaps: Use historical same-week values
4. Long gaps (>4 weeks): Mark as inactive, don't interpolate

---

### Strategy 3: Daily-Level Features → Weekly Aggregation
**Expected Impact: -10-15% WMAPE**

Currently we aggregate to weekly immediately. Instead:

```python
# Extract daily-level features BEFORE weekly aggregation
daily_features = {
    'peak_day_of_week': most_common_order_day,  # Mon=0, Sun=6
    'weekend_ratio': weekend_qty / total_qty,
    'day_concentration': max_day_qty / total_qty,  # How concentrated
    'order_spread': unique_order_days / 7,  # Days with orders
    'early_week_bias': (mon+tue+wed_qty) / total_qty,
    'late_week_bias': (thu+fri_qty) / total_qty,
}

# Volatility features
daily_volatility = {
    'daily_cv': daily_std / daily_mean,
    'intra_week_range': (max_daily - min_daily) / mean_daily,
}
```

**Implementation:**
1. Load raw transaction data with timestamps
2. Calculate daily patterns before aggregation
3. Add day-of-week features to weekly model
4. Consider: Some products spike on specific days (e.g., Friday restocking)

---

### Strategy 4: Enhanced Seasonality Features
**Expected Impact: -8-12% WMAPE**

```python
# Calendar features
seasonality_features = {
    # Week position
    'week_of_month': (day_of_month - 1) // 7 + 1,  # 1-5
    'is_month_start': week_of_month == 1,
    'is_month_end': week_of_month >= 4,
    
    # Special periods
    'is_w47': year_week == 'W47',  # Black Friday
    'is_w48_w52': week_num >= 48,  # Holiday season
    'is_w01_w02': week_num <= 2,   # New year slowdown
    'is_easter_week': calculate_easter_week(year),
    
    # Historical multipliers
    'w47_historical_mult': historical_w47_avg / overall_avg,
    'holiday_season_mult': w48_52_avg / overall_avg,
}

# Cyclical encoding (preserves continuity)
def cyclical_encode(value, max_value):
    sin_val = np.sin(2 * np.pi * value / max_value)
    cos_val = np.cos(2 * np.pi * value / max_value)
    return sin_val, cos_val

# week_sin, week_cos = cyclical_encode(week_num, 52)
```

---

### Strategy 5: Feature Engineering Improvements
**Expected Impact: -5-10% WMAPE**

```python
# Current features (V2)
v2_features = ['lag1', 'lag2', 'lag4', 'rolling_avg_4w']

# V3 enhanced features
v3_features = {
    # Lag features (expanded)
    'lag1', 'lag2', 'lag3', 'lag4', 'lag8', 'lag12',
    'lag52',  # Same week last year (if available)
    
    # Rolling statistics (multiple windows)
    'rolling_mean_4w', 'rolling_mean_8w', 'rolling_mean_12w',
    'rolling_std_4w', 'rolling_std_8w',
    'rolling_min_4w', 'rolling_max_4w',
    
    # Trend features
    'trend_4w': (current - lag4) / lag4,  # 4-week trend
    'trend_8w': (current - lag8) / lag8,
    'momentum': lag1 - lag2,  # Recent direction
    
    # Volatility features
    'cv_4w': rolling_std_4w / rolling_mean_4w,
    'range_ratio_4w': (rolling_max - rolling_min) / rolling_mean,
    
    # Price features (if available)
    'price_change_4w',
    'price_vs_avg',
    
    # Cross-sectional features
    'sku_rank_in_category',  # Relative performance
    'category_growth_rate',
}
```

---

### Strategy 6: Model Architecture Improvements
**Expected Impact: -5-8% WMAPE**

```python
# Current: Single GradientBoostingRegressor
# V3: Ensemble of specialized models

from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

# Model 1: XGBoost with tuned hyperparameters
xgb_model = XGBRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.05,
    subsample=0.8,
    colsample_bytree=0.8,
    reg_alpha=0.1,
    reg_lambda=1.0,
    random_state=42
)

# Model 2: LightGBM for speed + accuracy
lgbm_model = LGBMRegressor(
    n_estimators=200,
    max_depth=8,
    learning_rate=0.05,
    num_leaves=31,
    feature_fraction=0.8,
    bagging_fraction=0.8,
    bagging_freq=5,
    random_state=42
)

# Model 3: Pattern-specific models
cyclical_model = ...   # Optimized for cyclical patterns
bulk_model = ...       # Optimized for bulk/one-off patterns

# Ensemble: Weighted average based on pattern
def ensemble_predict(sku_pattern, X):
    if sku_pattern == 'cyclical':
        weights = [0.4, 0.4, 0.2]  # XGB, LGBM, Cyclical
    else:
        weights = [0.3, 0.3, 0.4]  # XGB, LGBM, Bulk
    
    preds = [m.predict(X) for m in models]
    return np.average(preds, weights=weights, axis=0)
```

---

### Strategy 7: Cross-Validation & Hyperparameter Tuning
**Expected Impact: -3-5% WMAPE**

```python
from sklearn.model_selection import TimeSeriesSplit
from sklearn.model_selection import RandomizedSearchCV

# Time-series aware cross-validation
tscv = TimeSeriesSplit(n_splits=5)

# Hyperparameter search space
param_dist = {
    'n_estimators': [100, 200, 300],
    'max_depth': [4, 6, 8, 10],
    'learning_rate': [0.01, 0.05, 0.1],
    'subsample': [0.7, 0.8, 0.9],
    'colsample_bytree': [0.7, 0.8, 0.9],
}

# Search
search = RandomizedSearchCV(
    XGBRegressor(),
    param_dist,
    n_iter=50,
    cv=tscv,
    scoring='neg_mean_absolute_percentage_error',
    random_state=42
)
search.fit(X_train, y_train)
best_model = search.best_estimator_
```

---

## 📋 V3 Implementation Plan

### Phase 1: Data Preparation (Day 1)
- [ ] Load raw daily transaction data
- [ ] Calculate daily-level features
- [ ] Implement outlier detection
- [ ] Implement interpolation for sparse data

### Phase 2: Feature Engineering (Day 1-2)
- [ ] Add all V3 features
- [ ] Add seasonality features (W47, holidays)
- [ ] Add cyclical encoding
- [ ] Add cross-sectional features

### Phase 3: Model Training (Day 2)
- [ ] Train XGBoost with new features
- [ ] Train LightGBM model
- [ ] Train pattern-specific models
- [ ] Implement ensemble

### Phase 4: Validation & Tuning (Day 2-3)
- [ ] Cross-validation
- [ ] Hyperparameter tuning
- [ ] Compare V2 vs V3 performance

### Phase 5: Deployment (Day 3)
- [ ] Generate V3 predictions
- [ ] Update dashboard
- [ ] Deploy to BigQuery

---

## 🎯 Expected Results

| Strategy | Expected Improvement |
|----------|---------------------|
| Outlier Removal | -10 to -15% |
| Data Interpolation | -5 to -10% |
| Daily Features | -10 to -15% |
| Enhanced Seasonality | -8 to -12% |
| Feature Engineering | -5 to -10% |
| Model Architecture | -5 to -8% |
| Hyperparameter Tuning | -3 to -5% |

**Cumulative Target: 73% → <40% WMAPE**

---

## 🔧 Quick Start Script

```bash
# Run V3 model training
python3 scripts/TRAIN_V3_MODELS.py

# Compare V2 vs V3
python3 scripts/COMPARE_V2_V3.py

# Deploy to BigQuery
python3 scripts/DEPLOY_V3_BIGQUERY.py
```

