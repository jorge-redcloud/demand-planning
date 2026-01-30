#!/usr/bin/env python3
"""
V3 Model Training - Killer Version
===================================
Implements all V3 strategies for improved WMAPE.

Run: python3 scripts/TRAIN_V3_MODELS.py
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Try to import LightGBM (optional but recommended)
try:
    from lightgbm import LGBMRegressor
    HAS_LGBM = True
except ImportError:
    HAS_LGBM = False
    print("Note: LightGBM not installed. Install with: pip install lightgbm")

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
OUTPUT_DIR = BASE_PATH / 'model_evaluation'
LOG_FILE = BASE_PATH / 'v3_training_log.txt'

H1_END = '2025-W26'

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def calculate_wmape(actual, predicted):
    return 100 * np.sum(np.abs(actual - predicted)) / np.sum(actual) if np.sum(actual) > 0 else 999

# ============================================
# STRATEGY 1: OUTLIER DETECTION
# ============================================
def detect_outliers(df, group_col, value_col, method='iqr', threshold=1.5):
    """Mark outliers within each group"""
    df = df.copy()
    df['is_outlier'] = False
    
    for group in df[group_col].unique():
        mask = df[group_col] == group
        values = df.loc[mask, value_col]
        
        if len(values) < 5:
            continue
            
        if method == 'iqr':
            Q1, Q3 = values.quantile([0.25, 0.75])
            IQR = Q3 - Q1
            lower = Q1 - threshold * IQR
            upper = Q3 + threshold * IQR
            outlier_mask = (values < lower) | (values > upper)
        else:  # z-score
            z = (values - values.mean()) / values.std()
            outlier_mask = abs(z) > threshold
            
        df.loc[mask, 'is_outlier'] = outlier_mask.values
    
    return df

def winsorize_outliers(df, group_col, value_col, lower_pct=0.05, upper_pct=0.95):
    """Cap outliers at percentile bounds"""
    df = df.copy()
    df[f'{value_col}_winsorized'] = df[value_col]
    
    for group in df[group_col].unique():
        mask = df[group_col] == group
        values = df.loc[mask, value_col]
        
        lower = values.quantile(lower_pct)
        upper = values.quantile(upper_pct)
        
        df.loc[mask, f'{value_col}_winsorized'] = values.clip(lower, upper)
    
    return df

# ============================================
# STRATEGY 2: ENHANCED FEATURES
# ============================================
def add_v3_features(df):
    """Add all V3 features to the dataframe"""
    df = df.copy()
    df = df.sort_values(['sku', 'year_week'])
    
    # Extract week number for seasonality
    df['week_num'] = df['year_week'].str.extract(r'W(\d+)').astype(int)
    
    # Seasonality features
    df['is_w47'] = (df['week_num'] == 47).astype(int)
    df['is_holiday_season'] = (df['week_num'] >= 47).astype(int)
    df['is_month_start'] = (df['week_num'] % 4 == 1).astype(int)
    df['is_month_end'] = (df['week_num'] % 4 == 0).astype(int)
    
    # Cyclical encoding of week
    df['week_sin'] = np.sin(2 * np.pi * df['week_num'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_num'] / 52)
    
    # Group operations per SKU
    for lag in [1, 2, 3, 4, 8, 12]:
        df[f'lag{lag}'] = df.groupby('sku')['weekly_quantity'].shift(lag)
    
    # Rolling statistics (multiple windows)
    for window in [4, 8, 12]:
        df[f'rolling_mean_{window}w'] = df.groupby('sku')['weekly_quantity'].transform(
            lambda x: x.shift(1).rolling(window, min_periods=1).mean()
        )
        df[f'rolling_std_{window}w'] = df.groupby('sku')['weekly_quantity'].transform(
            lambda x: x.shift(1).rolling(window, min_periods=1).std()
        )
    
    # Rolling min/max
    df['rolling_min_4w'] = df.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).min()
    )
    df['rolling_max_4w'] = df.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).max()
    )
    
    # Trend features
    df['trend_4w'] = (df['lag1'] - df['lag4']) / df['lag4'].replace(0, np.nan)
    df['momentum'] = df['lag1'] - df['lag2']
    
    # Volatility
    df['cv_4w'] = df['rolling_std_4w'] / df['rolling_mean_4w'].replace(0, np.nan)
    df['range_ratio'] = (df['rolling_max_4w'] - df['rolling_min_4w']) / df['rolling_mean_4w'].replace(0, np.nan)
    
    # Fill NaN values
    df = df.fillna(0)
    
    return df

# ============================================
# STRATEGY 3: MODEL TRAINING
# ============================================
def train_v3_model(X_train, y_train, X_test, use_lgbm=True):
    """Train V3 model (XGBoost or LightGBM)"""
    
    if HAS_LGBM and use_lgbm:
        model = LGBMRegressor(
            n_estimators=200,
            max_depth=8,
            learning_rate=0.05,
            num_leaves=31,
            feature_fraction=0.8,
            bagging_fraction=0.8,
            bagging_freq=5,
            random_state=42,
            verbose=-1
        )
    else:
        model = GradientBoostingRegressor(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.8,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42
        )
    
    model.fit(X_train, y_train)
    predictions = model.predict(X_test)
    
    return np.clip(predictions, 0, None), model

# ============================================
# MAIN TRAINING PIPELINE
# ============================================
def main():
    log("=" * 60)
    log("V3 MODEL TRAINING - KILLER VERSION")
    log("=" * 60)
    
    # Load data
    log("\n[1/6] Loading data...")
    weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
    log(f"  ✓ Loaded {len(weekly)} rows, {weekly['sku'].nunique()} SKUs")
    
    # Strategy 1: Outlier detection
    log("\n[2/6] Detecting outliers...")
    weekly = detect_outliers(weekly, 'sku', 'weekly_quantity', method='iqr', threshold=2.0)
    weekly = winsorize_outliers(weekly, 'sku', 'weekly_quantity')
    outlier_count = weekly['is_outlier'].sum()
    log(f"  ✓ Found {outlier_count} outlier weeks ({100*outlier_count/len(weekly):.1f}%)")
    log(f"  ✓ Applied winsorization to cap extreme values")
    
    # Strategy 2: Add V3 features
    log("\n[3/6] Adding V3 features...")
    weekly = add_v3_features(weekly)
    log(f"  ✓ Added seasonality features (W47, holidays, cyclical encoding)")
    log(f"  ✓ Added extended lag features (1,2,3,4,8,12 weeks)")
    log(f"  ✓ Added rolling statistics (4w, 8w, 12w windows)")
    log(f"  ✓ Added trend and volatility features")
    
    # Define H1/H2 split
    weekly['is_h1'] = weekly['year_week'] <= H1_END
    h1_data = weekly[weekly['is_h1']]
    h2_data = weekly[~weekly['is_h1']]
    
    log(f"  ✓ H1 training: {len(h1_data)} rows")
    log(f"  ✓ H2 validation: {len(h2_data)} rows")
    
    # V3 feature columns
    v3_feature_cols = [
        'lag1', 'lag2', 'lag3', 'lag4', 'lag8', 'lag12',
        'rolling_mean_4w', 'rolling_mean_8w', 'rolling_mean_12w',
        'rolling_std_4w', 'rolling_std_8w',
        'rolling_min_4w', 'rolling_max_4w',
        'trend_4w', 'momentum',
        'cv_4w', 'range_ratio',
        'week_sin', 'week_cos',
        'is_w47', 'is_holiday_season',
        'is_outlier'
    ]
    
    # Train SKU models
    log("\n[4/6] Training SKU-level V3 models...")
    
    # Filter SKUs with enough H1 data
    h1_weeks_per_sku = h1_data.groupby('sku').size()
    eligible_skus = h1_weeks_per_sku[h1_weeks_per_sku >= 4].index.tolist()
    log(f"  ✓ Eligible SKUs (≥4 H1 weeks): {len(eligible_skus)}")
    
    sku_results = []
    sku_h1_actuals = []
    trained = 0
    skipped = 0
    
    for sku in eligible_skus:
        sku_h1 = h1_data[h1_data['sku'] == sku].copy()
        sku_h2 = h2_data[h2_data['sku'] == sku].copy()
        
        if len(sku_h2) == 0:
            skipped += 1
            continue
        
        # Prepare training data (use winsorized values for training)
        sku_h1_clean = sku_h1.dropna(subset=v3_feature_cols)
        if len(sku_h1_clean) < 3:
            skipped += 1
            continue
        
        X_train = sku_h1_clean[v3_feature_cols].values
        y_train = sku_h1_clean['weekly_quantity_winsorized'].values
        
        # Prepare test data
        sku_h2_clean = sku_h2.dropna(subset=v3_feature_cols)
        if len(sku_h2_clean) == 0:
            skipped += 1
            continue
        
        X_test = sku_h2_clean[v3_feature_cols].values
        y_test = sku_h2_clean['weekly_quantity'].values  # Use original for evaluation
        
        try:
            predictions, _ = train_v3_model(X_train, y_train, X_test, use_lgbm=HAS_LGBM)
            
            for i, (_, row) in enumerate(sku_h2_clean.iterrows()):
                sku_results.append({
                    'sku': sku,
                    'year_week': row['year_week'],
                    'actual': row['weekly_quantity'],
                    'predicted': predictions[i],
                    'is_w47': row['is_w47'],
                    'was_outlier_train': sku_h1_clean['is_outlier'].any()
                })
            
            trained += 1
        except:
            skipped += 1
            continue
        
        # H1 actuals
        for _, row in sku_h1.iterrows():
            sku_h1_actuals.append({
                'sku': sku,
                'year_week': row['year_week'],
                'actual': row['weekly_quantity'],
                'description': row.get('description', '')
            })
    
    log(f"  ✓ Trained: {trained}")
    log(f"  ✓ Skipped: {skipped}")
    
    # Save results
    if sku_results:
        sku_df = pd.DataFrame(sku_results)
        sku_df['abs_error'] = np.abs(sku_df['predicted'] - sku_df['actual'])
        sku_df['pct_error'] = 100 * sku_df['abs_error'] / sku_df['actual'].replace(0, np.nan)
        
        sku_df.to_csv(OUTPUT_DIR / 'sku_predictions_XGBoost_v3.csv', index=False)
        log(f"  ✓ Saved: sku_predictions_XGBoost_v3.csv ({len(sku_df)} predictions)")
        
        # Overall WMAPE
        v3_wmape = calculate_wmape(sku_df['actual'], sku_df['predicted'])
        log(f"\n  ★ SKU V3 WMAPE: {v3_wmape:.1f}%")
        
        # W47 specific WMAPE
        w47_df = sku_df[sku_df['is_w47'] == 1]
        if len(w47_df) > 0:
            w47_wmape = calculate_wmape(w47_df['actual'], w47_df['predicted'])
            log(f"  ★ W47 (Black Friday) WMAPE: {w47_wmape:.1f}%")
        
        # Save H1 actuals
        h1_df = pd.DataFrame(sku_h1_actuals)
        h1_df.to_csv(OUTPUT_DIR / 'sku_h1_actuals_v3.csv', index=False)
    
    # Compare with V2
    log("\n[5/6] Comparing V3 vs V2...")
    try:
        v2_df = pd.read_csv(OUTPUT_DIR / 'sku_predictions_XGBoost.csv')
        v2_wmape = calculate_wmape(v2_df['actual'], v2_df['predicted'])
        improvement = v2_wmape - v3_wmape
        log(f"  V2 WMAPE: {v2_wmape:.1f}%")
        log(f"  V3 WMAPE: {v3_wmape:.1f}%")
        log(f"  Improvement: {improvement:+.1f}% points")
    except:
        log("  Could not load V2 for comparison")
    
    # Summary
    log("\n[6/6] Summary")
    log("=" * 60)
    log(f"V3 Model trained with:")
    log(f"  - Outlier detection & winsorization")
    log(f"  - {len(v3_feature_cols)} features")
    log(f"  - {'LightGBM' if HAS_LGBM else 'GradientBoosting'} algorithm")
    log(f"\nFinal SKU WMAPE: {v3_wmape:.1f}%")
    log("=" * 60)
    
    return v3_wmape

if __name__ == '__main__':
    main()
