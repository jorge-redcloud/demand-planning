#!/usr/bin/env python3
"""
V3 HYBRID MODEL - Best of Both Worlds
======================================
Problem:
- Global model ignores SKU/category identity (62.6% WMAPE but no product awareness)
- Per-SKU models lack training data (73.1% WMAPE)

Solution: HYBRID approach
1. Train per-CATEGORY models (enough data per category)
2. Use SKU-level features (lags, rolling avg)
3. Fall back to global model for sparse categories

This gives us:
- Category-specific patterns (seasonality, price sensitivity)
- SKU-level historical features
- Sufficient training data

Run: python3 scripts/TRAIN_V3_HYBRID.py
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
OUTPUT_DIR = BASE_PATH / 'model_evaluation'
LOG_FILE = BASE_PATH / 'v3_hybrid_training_log.txt'

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
    log("V3 HYBRID MODEL - Per-Category + SKU Features")
    log("=" * 60)

    # Load data
    log("\n[1/5] Loading data...")
    weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
    products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')

    # Add category to weekly data
    sku_cat = products[['sku', 'category_l1']].drop_duplicates().set_index('sku')['category_l1'].to_dict()
    weekly['category'] = weekly['sku'].map(sku_cat).fillna('Unknown')

    # Extract week number
    weekly['week_num'] = weekly['year_week'].str.extract(r'W(\d+)').astype(int)

    log(f"  ✓ Loaded {len(weekly)} rows, {weekly['sku'].nunique()} SKUs")
    log(f"  ✓ Categories: {weekly['category'].nunique()}")

    # Features
    feature_cols = ['lag1_quantity', 'lag2_quantity', 'lag4_quantity',
                    'rolling_avg_4w', 'avg_unit_price', 'week_num']

    # Split H1/H2
    train = weekly[weekly['week_num'] <= 26].copy()
    test = weekly[weekly['week_num'] > 26].copy()

    log(f"  ✓ H1 training: {len(train)} rows")
    log(f"  ✓ H2 test: {len(test)} rows")

    # Train global fallback model first
    log("\n[2/5] Training GLOBAL fallback model...")
    train_valid = train.dropna(subset=feature_cols)
    X_train_global = train_valid[feature_cols].fillna(0)
    y_train_global = train_valid['weekly_quantity']

    global_model = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
    global_model.fit(X_train_global, y_train_global)
    log("  ✓ Global model trained")

    # Train per-category models
    log("\n[3/5] Training per-CATEGORY models...")
    category_models = {}
    category_stats = []

    for cat in weekly['category'].unique():
        cat_train = train[train['category'] == cat].dropna(subset=feature_cols)

        if len(cat_train) < 50:  # Not enough data, use global
            log(f"  ⚠ {cat}: Only {len(cat_train)} rows - using GLOBAL model")
            category_models[cat] = None  # Will use global fallback
            category_stats.append({'category': cat, 'model': 'global', 'train_rows': len(cat_train)})
            continue

        X_train = cat_train[feature_cols].fillna(0)
        y_train = cat_train['weekly_quantity']

        model = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
        model.fit(X_train, y_train)
        category_models[cat] = model

        log(f"  ✓ {cat}: Trained on {len(cat_train)} rows")
        category_stats.append({'category': cat, 'model': 'category', 'train_rows': len(cat_train)})

    # Predict
    log("\n[4/5] Predicting for H2...")
    predictions = []

    for cat in test['category'].unique():
        cat_test = test[test['category'] == cat].dropna(subset=feature_cols)

        if len(cat_test) == 0:
            continue

        X_test = cat_test[feature_cols].fillna(0)

        # Use category model if available, else global
        if cat in category_models and category_models[cat] is not None:
            model = category_models[cat]
            model_type = 'category'
        else:
            model = global_model
            model_type = 'global'

        preds = np.clip(model.predict(X_test), 0, None)

        for i, (_, row) in enumerate(cat_test.iterrows()):
            predictions.append({
                'sku': row['sku'],
                'category': cat,
                'year_week': row['year_week'],
                'actual': row['weekly_quantity'],
                'predicted': preds[i],
                'model_type': model_type
            })

    results_df = pd.DataFrame(predictions)
    log(f"  ✓ Generated {len(results_df)} predictions")

    # Calculate metrics
    log("\n[5/5] Calculating metrics...")

    # Overall WMAPE
    wmape_overall = calculate_wmape(results_df['actual'], results_df['predicted'])
    log(f"\n  ★ HYBRID Overall WMAPE: {wmape_overall:.1f}%")

    # By model type
    for model_type in ['category', 'global']:
        subset = results_df[results_df['model_type'] == model_type]
        if len(subset) > 0:
            wmape = calculate_wmape(subset['actual'], subset['predicted'])
            log(f"  ★ {model_type.capitalize()} model WMAPE: {wmape:.1f}% ({len(subset)} predictions)")

    # By category
    log("\n  Per-category WMAPE:")
    for cat in results_df['category'].unique():
        cat_results = results_df[results_df['category'] == cat]
        wmape = calculate_wmape(cat_results['actual'], cat_results['predicted'])
        model_type = cat_results['model_type'].iloc[0]
        log(f"    {cat}: {wmape:.1f}% ({model_type})")

    # Save results
    results_df.to_csv(OUTPUT_DIR / 'sku_predictions_XGBoost_hybrid.csv', index=False)
    log(f"\n  ✓ Saved: sku_predictions_XGBoost_hybrid.csv")

    # Compare
    log("\n" + "-" * 40)
    log("COMPARISON")
    log("-" * 40)
    log(f"  Global-only model:  62.6% WMAPE")
    log(f"  Per-SKU models:     73.1% WMAPE")
    log(f"  HYBRID model:       {wmape_overall:.1f}% WMAPE")

    if wmape_overall < 62.6:
        log(f"\n  ✅ HYBRID WINS by {62.6 - wmape_overall:.1f}% points!")
    else:
        log(f"\n  ❌ Hybrid is {wmape_overall - 62.6:.1f}% worse")

    # Summary
    log("\n" + "=" * 60)
    log("SUMMARY")
    log("=" * 60)
    log("HYBRID approach trains:")
    log("  1. One model per CATEGORY (enough training data)")
    log("  2. Uses SKU-level features (lags, rolling avg)")
    log("  3. Falls back to global for sparse categories")
    log("=" * 60)

    return wmape_overall

if __name__ == '__main__':
    main()
