#!/usr/bin/env python3
"""
V3.3 Model Training - Global Model Approach
============================================
DISCOVERY: The original V2 with 57.2% WMAPE used a GLOBAL model
trained on ALL SKUs combined, not per-SKU models.

This script replicates that approach and adds:
1. Same global model architecture
2. W47 seasonality feature
3. Week number as feature (already in original)

Run: python3 scripts/TRAIN_V3_3_GLOBAL.py
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
OUTPUT_DIR = BASE_PATH / 'model_evaluation'
LOG_FILE = BASE_PATH / 'v3_3_training_log.txt'

H1_END_WEEK = 26  # Train on W01-W26
H2_START_WEEK = 27  # Test on W27-W52

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def calculate_wmape(actual, predicted):
    return 100 * np.sum(np.abs(actual - predicted)) / np.sum(actual) if np.sum(actual) > 0 else 999

def main():
    log("=" * 60)
    log("V3.3 MODEL TRAINING - GLOBAL MODEL APPROACH")
    log("=" * 60)
    log("Using the same approach as original V2 (57.2% WMAPE):")
    log("  - ONE global model trained on ALL SKUs")
    log("  - Not per-SKU individual models")

    # Load data
    log("\n[1/5] Loading data...")
    weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
    log(f"  ✓ Loaded {len(weekly)} rows, {weekly['sku'].nunique()} SKUs")

    # Extract week number
    weekly['week_num'] = weekly['year_week'].str.extract(r'W(\d+)').astype(int)

    # Add W47 feature (V3.3 enhancement)
    weekly['is_w47'] = (weekly['week_num'] == 47).astype(int)

    # Split H1/H2
    train = weekly[weekly['week_num'] <= H1_END_WEEK].copy()
    test = weekly[weekly['week_num'] > H1_END_WEEK].copy()

    log(f"  ✓ H1 training: {len(train)} rows ({train['sku'].nunique()} SKUs)")
    log(f"  ✓ H2 test: {len(test)} rows ({test['sku'].nunique()} SKUs)")

    # Define features - SAME as original V2 plus W47
    # Original: lag1_quantity, lag2_quantity, lag4_quantity, rolling_avg_4w, avg_unit_price, week_num
    feature_cols = ['lag1_quantity', 'lag2_quantity', 'lag4_quantity',
                    'rolling_avg_4w', 'avg_unit_price', 'week_num']

    # V3.3 adds W47 feature
    v3_3_feature_cols = feature_cols + ['is_w47']

    # Filter to rows with all features
    log("\n[2/5] Preparing training data...")
    available_features = [c for c in v3_3_feature_cols if c in train.columns]
    train_valid = train.dropna(subset=[c for c in available_features if c in train.columns])

    log(f"  ✓ Training rows with features: {len(train_valid)}")
    log(f"  ✓ Features: {available_features}")

    # Prepare training data
    X_train = train_valid[available_features].fillna(0)
    y_train = train_valid['weekly_quantity']

    # Train GLOBAL model (same as original V2)
    log("\n[3/5] Training GLOBAL XGBoost model...")
    model = GradientBoostingRegressor(
        n_estimators=100,
        max_depth=5,
        random_state=42
    )
    model.fit(X_train, y_train)
    log("  ✓ Model trained on ALL SKUs combined")

    # Feature importance
    if hasattr(model, 'feature_importances_'):
        importance = dict(zip(available_features, model.feature_importances_))
        sorted_imp = sorted(importance.items(), key=lambda x: x[1], reverse=True)
        log("\n  Feature importance:")
        for feat, imp in sorted_imp:
            log(f"    {feat}: {imp:.3f}")

    # Predict for test set
    log("\n[4/5] Predicting for H2 test set...")
    predictions = []

    for sku in test['sku'].unique():
        test_sku = test[test['sku'] == sku]

        if len(test_sku) == 0:
            continue

        X_test = test_sku[available_features].fillna(0)
        preds = model.predict(X_test)

        for i, (_, row) in enumerate(test_sku.iterrows()):
            predictions.append({
                'sku': sku,
                'year_week': row['year_week'],
                'actual': row['weekly_quantity'],
                'predicted': max(0, preds[i]),
                'is_w47': row['is_w47'],
                'week_num': row['week_num']
            })

    sku_df = pd.DataFrame(predictions)
    log(f"  ✓ Generated {len(sku_df)} predictions for {sku_df['sku'].nunique()} SKUs")

    # Calculate metrics
    log("\n[5/5] Calculating metrics...")
    sku_df['abs_error'] = np.abs(sku_df['predicted'] - sku_df['actual'])
    sku_df['pct_error'] = 100 * sku_df['abs_error'] / sku_df['actual'].replace(0, np.nan)

    # Overall WMAPE
    v3_3_wmape = calculate_wmape(sku_df['actual'], sku_df['predicted'])
    log(f"\n  ★ V3.3 Overall WMAPE: {v3_3_wmape:.1f}%")

    # Non-W47 WMAPE
    non_w47 = sku_df[sku_df['is_w47'] == 0]
    non_w47_wmape = calculate_wmape(non_w47['actual'], non_w47['predicted'])
    log(f"  ★ Non-W47 WMAPE: {non_w47_wmape:.1f}%")

    # W47 WMAPE
    w47_df = sku_df[sku_df['is_w47'] == 1]
    if len(w47_df) > 0:
        w47_wmape = calculate_wmape(w47_df['actual'], w47_df['predicted'])
        log(f"  ★ W47 (Black Friday) WMAPE: {w47_wmape:.1f}%")

    # Save results
    sku_df.to_csv(OUTPUT_DIR / 'sku_predictions_XGBoost_v3_3.csv', index=False)
    log(f"\n  ✓ Saved: sku_predictions_XGBoost_v3_3.csv")

    # Compare with V2 original
    log("\n" + "-" * 40)
    log("COMPARISON WITH V2 ORIGINAL")
    log("-" * 40)

    try:
        v2_df = pd.read_csv(OUTPUT_DIR / 'sku_predictions_XGBoost.csv')
        v2_wmape = calculate_wmape(v2_df['actual'], v2_df['predicted'])
        log(f"  V2 Original WMAPE: {v2_wmape:.1f}% ({len(v2_df)} predictions, {v2_df['sku'].nunique()} SKUs)")
        log(f"  V3.3 WMAPE: {v3_3_wmape:.1f}% ({len(sku_df)} predictions, {sku_df['sku'].nunique()} SKUs)")

        improvement = v2_wmape - v3_3_wmape
        if improvement > 0:
            log(f"\n  ✅ V3.3 BEATS V2 by {improvement:.1f}% points!")
        elif abs(improvement) < 0.5:
            log(f"\n  ≈ V3.3 matches V2 (difference: {improvement:+.1f}%)")
        else:
            log(f"\n  ❌ V3.3 is {-improvement:.1f}% worse than V2")
    except:
        log("  Could not load V2 for comparison")

    # Summary
    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log(f"V3.3 Final WMAPE: {v3_3_wmape:.1f}%")
    log(f"Model: GLOBAL XGBoost (trained on all SKUs)")
    log(f"Features: {len(available_features)}")
    log("=" * 60)

    return v3_3_wmape

if __name__ == '__main__':
    main()
