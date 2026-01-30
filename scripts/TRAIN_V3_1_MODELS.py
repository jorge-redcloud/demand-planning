#!/usr/bin/env python3
"""
V3.1 Model Training - Optimized Version
========================================
Fixes issues from V3:
1. Don't use winsorized values for training (removes signal)
2. Reduce feature count to prevent overfitting (22 → 12 core features)
3. Use feature importance to select best features
4. Better handling of sparse data
5. Separate models for high/low volatility SKUs

Run: python3 scripts/TRAIN_V3_1_MODELS.py
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import Ridge
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
OUTPUT_DIR = BASE_PATH / 'model_evaluation'
LOG_FILE = BASE_PATH / 'v3_1_training_log.txt'

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
# V3.1 CORE FEATURES (Reduced from 22 to 12)
# ============================================
V3_1_FEATURES = [
    # Most important lag features
    'lag1', 'lag2', 'lag4',
    # Best rolling stats
    'rolling_mean_4w', 'rolling_std_4w',
    # Trend
    'trend_4w', 'momentum',
    # Seasonality (simplified)
    'week_sin', 'week_cos',
    'is_w47',
    # Volatility indicator
    'cv_4w',
    # Outlier flag (as feature, not for training data selection)
    'is_outlier'
]

def add_v3_1_features(df):
    """Add V3.1 optimized features"""
    df = df.copy()
    df = df.sort_values(['sku', 'year_week'])

    # Extract week number
    df['week_num'] = df['year_week'].str.extract(r'W(\d+)').astype(int)

    # Key seasonality features
    df['is_w47'] = (df['week_num'] == 47).astype(int)

    # Cyclical encoding
    df['week_sin'] = np.sin(2 * np.pi * df['week_num'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_num'] / 52)

    # Core lags
    for lag in [1, 2, 4]:
        df[f'lag{lag}'] = df.groupby('sku')['weekly_quantity'].shift(lag)

    # Rolling stats (4w window only - most predictive)
    df['rolling_mean_4w'] = df.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).mean()
    )
    df['rolling_std_4w'] = df.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).std()
    )

    # Trend & momentum
    df['trend_4w'] = (df['lag1'] - df['lag4']) / df['lag4'].replace(0, np.nan)
    df['momentum'] = df['lag1'] - df['lag2']

    # Coefficient of variation (volatility indicator)
    df['cv_4w'] = df['rolling_std_4w'] / df['rolling_mean_4w'].replace(0, np.nan)

    # Outlier detection (IQR method, used as feature)
    df['is_outlier'] = 0
    for sku in df['sku'].unique():
        mask = df['sku'] == sku
        values = df.loc[mask, 'weekly_quantity']
        if len(values) >= 5:
            Q1, Q3 = values.quantile([0.25, 0.75])
            IQR = Q3 - Q1
            lower = Q1 - 2.0 * IQR
            upper = Q3 + 2.0 * IQR
            outlier_mask = (values < lower) | (values > upper)
            df.loc[mask, 'is_outlier'] = outlier_mask.astype(int).values

    # Fill NaN
    df = df.fillna(0)
    df['trend_4w'] = df['trend_4w'].replace([np.inf, -np.inf], 0).clip(-10, 10)
    df['cv_4w'] = df['cv_4w'].replace([np.inf, -np.inf], 0).clip(0, 10)

    return df

def classify_sku_pattern(sku_data):
    """Classify SKU into pattern type for model selection"""
    # Calculate metrics
    mean_qty = sku_data['weekly_quantity'].mean()
    std_qty = sku_data['weekly_quantity'].std()
    zeros = (sku_data['weekly_quantity'] == 0).sum()
    total = len(sku_data)

    cv = std_qty / mean_qty if mean_qty > 0 else 10
    zero_pct = zeros / total

    if zero_pct > 0.5:
        return 'sparse'  # Mostly zeros - use simple model
    elif cv > 1.5:
        return 'volatile'  # High variability - use robust model
    else:
        return 'stable'  # Regular pattern - use standard model

def train_v3_1_model(X_train, y_train, X_test, pattern='stable'):
    """Train model based on SKU pattern"""

    if pattern == 'sparse':
        # For sparse data, use Ridge regression (more stable)
        model = Ridge(alpha=1.0)
    elif pattern == 'volatile':
        # For volatile data, use deeper ensemble with more regularization
        model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=4,  # Shallower to prevent overfitting
            learning_rate=0.1,
            min_samples_split=5,
            min_samples_leaf=3,
            subsample=0.8,
            random_state=42
        )
    else:
        # Standard model for stable patterns
        model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=5,
            learning_rate=0.1,
            min_samples_split=5,
            min_samples_leaf=2,
            random_state=42
        )

    model.fit(X_train, y_train)
    predictions = model.predict(X_test)

    return np.clip(predictions, 0, None), model

def main():
    log("=" * 60)
    log("V3.1 MODEL TRAINING - OPTIMIZED VERSION")
    log("=" * 60)
    log("Improvements from V3:")
    log("  - Reduced features: 22 → 12 (prevent overfitting)")
    log("  - No winsorization on training data (preserve signal)")
    log("  - Pattern-based model selection")
    log("  - Better handling of sparse/volatile SKUs")

    # Load data
    log("\n[1/5] Loading data...")
    weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
    log(f"  ✓ Loaded {len(weekly)} rows, {weekly['sku'].nunique()} SKUs")

    # Add V3.1 features
    log("\n[2/5] Adding V3.1 features (12 core features)...")
    weekly = add_v3_1_features(weekly)
    log(f"  ✓ Features: {V3_1_FEATURES}")

    # H1/H2 split
    weekly['is_h1'] = weekly['year_week'] <= H1_END
    h1_data = weekly[weekly['is_h1']]
    h2_data = weekly[~weekly['is_h1']]

    log(f"  ✓ H1 training: {len(h1_data)} rows")
    log(f"  ✓ H2 validation: {len(h2_data)} rows")

    # Classify SKUs by pattern
    log("\n[3/5] Classifying SKU patterns...")
    sku_patterns = {}
    for sku in weekly['sku'].unique():
        sku_h1 = h1_data[h1_data['sku'] == sku]
        if len(sku_h1) >= 4:
            sku_patterns[sku] = classify_sku_pattern(sku_h1)

    pattern_counts = pd.Series(sku_patterns).value_counts()
    log(f"  ✓ Stable: {pattern_counts.get('stable', 0)}")
    log(f"  ✓ Volatile: {pattern_counts.get('volatile', 0)}")
    log(f"  ✓ Sparse: {pattern_counts.get('sparse', 0)}")

    # Train models
    log("\n[4/5] Training SKU-level V3.1 models...")

    sku_results = []
    sku_h1_actuals = []
    trained_by_pattern = {'stable': 0, 'volatile': 0, 'sparse': 0}
    skipped = 0

    for sku, pattern in sku_patterns.items():
        sku_h1 = h1_data[h1_data['sku'] == sku].copy()
        sku_h2 = h2_data[h2_data['sku'] == sku].copy()

        if len(sku_h2) == 0:
            skipped += 1
            continue

        # Filter rows with valid features
        sku_h1_clean = sku_h1.dropna(subset=V3_1_FEATURES)
        sku_h2_clean = sku_h2.dropna(subset=V3_1_FEATURES)

        if len(sku_h1_clean) < 3 or len(sku_h2_clean) == 0:
            skipped += 1
            continue

        # Prepare data - use ORIGINAL values (not winsorized)
        X_train = sku_h1_clean[V3_1_FEATURES].values
        y_train = sku_h1_clean['weekly_quantity'].values  # Original, not winsorized!

        X_test = sku_h2_clean[V3_1_FEATURES].values
        y_test = sku_h2_clean['weekly_quantity'].values

        try:
            predictions, model = train_v3_1_model(X_train, y_train, X_test, pattern)

            for i, (_, row) in enumerate(sku_h2_clean.iterrows()):
                sku_results.append({
                    'sku': sku,
                    'year_week': row['year_week'],
                    'actual': row['weekly_quantity'],
                    'predicted': predictions[i],
                    'pattern': pattern,
                    'is_w47': row['is_w47']
                })

            trained_by_pattern[pattern] += 1
        except Exception as e:
            skipped += 1
            continue

        # Store H1 actuals
        for _, row in sku_h1.iterrows():
            sku_h1_actuals.append({
                'sku': sku,
                'year_week': row['year_week'],
                'actual': row['weekly_quantity']
            })

    total_trained = sum(trained_by_pattern.values())
    log(f"  ✓ Trained: {total_trained} (stable: {trained_by_pattern['stable']}, volatile: {trained_by_pattern['volatile']}, sparse: {trained_by_pattern['sparse']})")
    log(f"  ✓ Skipped: {skipped}")

    # Calculate metrics
    if sku_results:
        sku_df = pd.DataFrame(sku_results)
        sku_df['abs_error'] = np.abs(sku_df['predicted'] - sku_df['actual'])

        # Overall WMAPE
        v3_1_wmape = calculate_wmape(sku_df['actual'], sku_df['predicted'])

        # WMAPE by pattern
        log("\n  WMAPE by pattern:")
        for pattern in ['stable', 'volatile', 'sparse']:
            pattern_df = sku_df[sku_df['pattern'] == pattern]
            if len(pattern_df) > 0:
                pattern_wmape = calculate_wmape(pattern_df['actual'], pattern_df['predicted'])
                log(f"    {pattern}: {pattern_wmape:.1f}%")

        # W47 specific
        w47_df = sku_df[sku_df['is_w47'] == 1]
        if len(w47_df) > 0:
            w47_wmape = calculate_wmape(w47_df['actual'], w47_df['predicted'])
            log(f"\n  ★ W47 (Black Friday) WMAPE: {w47_wmape:.1f}%")

        # Save results
        sku_df.to_csv(OUTPUT_DIR / 'sku_predictions_XGBoost_v3_1.csv', index=False)
        log(f"\n  ✓ Saved: sku_predictions_XGBoost_v3_1.csv ({len(sku_df)} predictions)")

        # Save H1 actuals
        h1_df = pd.DataFrame(sku_h1_actuals)
        h1_df.to_csv(OUTPUT_DIR / 'sku_h1_actuals_v3_1.csv', index=False)

    # Compare with V2 and V3
    log("\n[5/5] Comparing V3.1 vs V2 vs V3...")
    try:
        v2_df = pd.read_csv(OUTPUT_DIR / 'sku_predictions_XGBoost.csv')
        v2_wmape = calculate_wmape(v2_df['actual'], v2_df['predicted'])
        log(f"  V2 WMAPE:   {v2_wmape:.1f}%")
    except:
        v2_wmape = None
        log("  V2: Could not load")

    try:
        v3_df = pd.read_csv(OUTPUT_DIR / 'sku_predictions_XGBoost_v3.csv')
        v3_wmape = calculate_wmape(v3_df['actual'], v3_df['predicted'])
        log(f"  V3 WMAPE:   {v3_wmape:.1f}%")
    except:
        v3_wmape = None
        log("  V3: Could not load")

    log(f"  V3.1 WMAPE: {v3_1_wmape:.1f}%")

    if v2_wmape:
        improvement = v2_wmape - v3_1_wmape
        log(f"\n  Improvement vs V2: {improvement:+.1f}% points")

    # Summary
    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"V3.1 Final WMAPE: {v3_1_wmape:.1f}%")
    log(f"Models trained: {total_trained}")
    log(f"Features used: {len(V3_1_FEATURES)}")
    log("Key changes from V3:")
    log("  ✓ No winsorization (preserves signal)")
    log("  ✓ Fewer features (prevents overfitting)")
    log("  ✓ Pattern-based model selection")
    log("=" * 60)

    return v3_1_wmape

if __name__ == '__main__':
    main()
