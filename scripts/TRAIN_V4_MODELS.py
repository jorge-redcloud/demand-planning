#!/usr/bin/env python3
"""
V4 MODEL TRAINING - Comprehensive Multi-Level Forecasting
==========================================================

Architecture:
1. Per-SKU models (excluding Unknown category from training, but predict for all)
2. Per-Category models (9 categories including Unknown)
3. Per-Customer models

Features:
- Daily granularity aggregated to weekly (day-of-week patterns)
- Price change features (week-over-week, trend)
- Lag features (1, 2, 4 weeks)
- Rolling statistics (mean, std, min, max)
- Seasonality (week number, W47 flag)

Output: Predictions for H2 (W27-W52) with confidence scores

Run: python3 scripts/TRAIN_V4_MODELS.py
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from pathlib import Path
from datetime import datetime
import warnings
import json
warnings.filterwarnings('ignore')

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
OUTPUT_DIR = BASE_PATH / 'model_evaluation'
LOG_FILE = BASE_PATH / 'v4_training_log.txt'

H1_END_WEEK = 26

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def calculate_wmape(actual, predicted):
    actual = np.array(actual)
    predicted = np.array(predicted)
    return 100 * np.sum(np.abs(actual - predicted)) / np.sum(actual) if np.sum(actual) > 0 else 999

def get_confidence(wmape, h1_weeks):
    """Determine confidence level based on WMAPE and training data availability"""
    if wmape < 40 and h1_weeks >= 15:
        return 'High'
    elif wmape < 60 and h1_weeks >= 10:
        return 'Medium'
    else:
        return 'Low'

def add_v4_features(df):
    """Add V4 enhanced features including price changes"""
    df = df.copy()
    df = df.sort_values(['sku', 'year_week'])

    # Extract week number
    df['week_num'] = df['year_week'].str.extract(r'W(\d+)').astype(int)

    # Seasonality features
    df['is_w47'] = (df['week_num'] == 47).astype(int)
    df['is_holiday_season'] = ((df['week_num'] >= 45) | (df['week_num'] <= 2)).astype(int)
    df['week_sin'] = np.sin(2 * np.pi * df['week_num'] / 52)
    df['week_cos'] = np.cos(2 * np.pi * df['week_num'] / 52)

    # Price change features
    if 'avg_unit_price' in df.columns:
        df['price_lag1'] = df.groupby('sku')['avg_unit_price'].shift(1)
        df['price_change'] = df['avg_unit_price'] - df['price_lag1']
        df['price_change_pct'] = df['price_change'] / df['price_lag1'].replace(0, np.nan) * 100
        df['price_trend_4w'] = df.groupby('sku')['avg_unit_price'].transform(
            lambda x: x.rolling(4, min_periods=1).mean()
        ) - df['avg_unit_price']

    # Additional rolling features
    df['rolling_std_4w'] = df.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).std()
    )
    df['rolling_min_4w'] = df.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).min()
    )
    df['rolling_max_4w'] = df.groupby('sku')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).max()
    )

    # Coefficient of variation (demand volatility)
    df['cv_4w'] = df['rolling_std_4w'] / df['rolling_avg_4w'].replace(0, np.nan)

    # Fill NaN
    df = df.fillna(0)
    df['price_change_pct'] = df['price_change_pct'].replace([np.inf, -np.inf], 0).clip(-100, 100)
    df['cv_4w'] = df['cv_4w'].replace([np.inf, -np.inf], 0).clip(0, 10)

    return df

def train_model(X_train, y_train):
    """Train XGBoost model"""
    model = GradientBoostingRegressor(
        n_estimators=100,
        max_depth=5,
        learning_rate=0.1,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42
    )
    model.fit(X_train, y_train)
    return model

def main():
    log("=" * 70)
    log("V4 MODEL TRAINING - Comprehensive Multi-Level Forecasting")
    log("=" * 70)

    # Load data
    log("\n[1/7] Loading data...")
    weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
    products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')

    # Add category
    sku_cat = products[['sku', 'category_l1']].drop_duplicates().set_index('sku')['category_l1'].to_dict()
    sku_name = products[['sku', 'name']].drop_duplicates().set_index('sku')['name'].to_dict()
    weekly['category'] = weekly['sku'].map(sku_cat).fillna('Unknown')

    log(f"  ✓ Loaded {len(weekly)} rows, {weekly['sku'].nunique()} SKUs")
    log(f"  ✓ Categories: {weekly['category'].nunique()}")

    # Add V4 features
    log("\n[2/7] Adding V4 features...")
    weekly = add_v4_features(weekly)

    # V4 Feature set
    v4_features = [
        # Core lag features
        'lag1_quantity', 'lag2_quantity', 'lag4_quantity', 'rolling_avg_4w',
        # Price features
        'avg_unit_price', 'price_change_pct', 'price_trend_4w',
        # Rolling stats
        'rolling_std_4w', 'rolling_min_4w', 'rolling_max_4w', 'cv_4w',
        # Seasonality
        'week_num', 'week_sin', 'week_cos', 'is_w47', 'is_holiday_season'
    ]

    # Filter to available features
    v4_features = [f for f in v4_features if f in weekly.columns]
    log(f"  ✓ Features: {len(v4_features)}")

    # Split H1/H2
    train_all = weekly[weekly['week_num'] <= H1_END_WEEK].copy()
    test_all = weekly[weekly['week_num'] > H1_END_WEEK].copy()

    log(f"  ✓ H1 training: {len(train_all)} rows")
    log(f"  ✓ H2 test: {len(test_all)} rows")

    # =========================================================================
    # PART 1: PER-SKU MODELS
    # =========================================================================
    log("\n[3/7] Training per-SKU models...")
    log("  Strategy: Train on non-Unknown SKUs, but predict for ALL")

    # Training data: exclude Unknown category
    train_sku = train_all[train_all['category'] != 'Unknown'].copy()
    log(f"  ✓ Training SKUs (excl. Unknown): {train_sku['sku'].nunique()}")

    # Count H1 weeks per SKU
    h1_weeks_per_sku = train_all.groupby('sku').size().to_dict()

    sku_results = []
    sku_models = {}
    trained_count = 0

    # Train individual SKU models for SKUs with enough data
    for sku in train_sku['sku'].unique():
        sku_train = train_sku[train_sku['sku'] == sku].dropna(subset=v4_features[:4])  # Core features

        if len(sku_train) < 4:
            continue

        X_train = sku_train[v4_features].fillna(0)
        y_train = sku_train['weekly_quantity']

        try:
            model = train_model(X_train, y_train)
            sku_models[sku] = model
            trained_count += 1
        except:
            continue

    log(f"  ✓ Trained {trained_count} individual SKU models")

    # Train global fallback model (on non-Unknown)
    train_valid = train_sku.dropna(subset=v4_features[:4])
    X_global = train_valid[v4_features].fillna(0)
    y_global = train_valid['weekly_quantity']
    global_model = train_model(X_global, y_global)
    log("  ✓ Trained global fallback model")

    # Predict for ALL SKUs (including Unknown)
    for sku in test_all['sku'].unique():
        sku_test = test_all[test_all['sku'] == sku].dropna(subset=v4_features[:4])

        if len(sku_test) == 0:
            continue

        X_test = sku_test[v4_features].fillna(0)

        # Use SKU-specific model if available, else global
        if sku in sku_models:
            model = sku_models[sku]
            model_type = 'sku'
        else:
            model = global_model
            model_type = 'global'

        preds = np.clip(model.predict(X_test), 0, None)

        for i, (_, row) in enumerate(sku_test.iterrows()):
            sku_results.append({
                'sku': int(sku),
                'description': sku_name.get(sku, f'SKU {sku}'),
                'category': row['category'],
                'year_week': row['year_week'],
                'actual': row['weekly_quantity'],
                'predicted': round(preds[i], 1),
                'model_type': model_type,
                'h1_weeks': h1_weeks_per_sku.get(sku, 0)
            })

    sku_df = pd.DataFrame(sku_results)

    # Calculate per-SKU WMAPE and confidence
    sku_wmape = sku_df.groupby('sku').apply(
        lambda x: calculate_wmape(x['actual'], x['predicted'])
    ).to_dict()

    sku_df['wmape'] = sku_df['sku'].map(sku_wmape)
    sku_df['confidence'] = sku_df.apply(
        lambda x: get_confidence(x['wmape'], x['h1_weeks']), axis=1
    )

    # Overall metrics
    sku_wmape_overall = calculate_wmape(sku_df['actual'], sku_df['predicted'])
    log(f"\n  ★ SKU Overall WMAPE: {sku_wmape_overall:.1f}%")

    # By category
    for cat in sku_df['category'].unique():
        cat_data = sku_df[sku_df['category'] == cat]
        cat_wmape = calculate_wmape(cat_data['actual'], cat_data['predicted'])
        log(f"    {cat}: {cat_wmape:.1f}%")

    # Confidence distribution
    conf_dist = sku_df.groupby('sku')['confidence'].first().value_counts()
    log(f"\n  Confidence: High={conf_dist.get('High', 0)}, Medium={conf_dist.get('Medium', 0)}, Low={conf_dist.get('Low', 0)}")

    # Save
    sku_df.to_csv(OUTPUT_DIR / 'sku_predictions_v4.csv', index=False)
    log(f"  ✓ Saved: sku_predictions_v4.csv ({len(sku_df)} predictions)")

    # =========================================================================
    # PART 2: PER-CATEGORY MODELS
    # =========================================================================
    log("\n[4/7] Training per-Category models...")

    # Aggregate by category
    cat_weekly = weekly.groupby(['category', 'year_week', 'week_num']).agg({
        'weekly_quantity': 'sum',
        'avg_unit_price': 'mean'
    }).reset_index()

    # Add lag features for categories
    cat_weekly = cat_weekly.sort_values(['category', 'year_week'])
    cat_weekly['lag1'] = cat_weekly.groupby('category')['weekly_quantity'].shift(1)
    cat_weekly['lag2'] = cat_weekly.groupby('category')['weekly_quantity'].shift(2)
    cat_weekly['lag4'] = cat_weekly.groupby('category')['weekly_quantity'].shift(4)
    cat_weekly['rolling_avg'] = cat_weekly.groupby('category')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).mean()
    )

    cat_features = ['lag1', 'lag2', 'lag4', 'rolling_avg', 'avg_unit_price', 'week_num']

    cat_train = cat_weekly[cat_weekly['week_num'] <= H1_END_WEEK]
    cat_test = cat_weekly[cat_weekly['week_num'] > H1_END_WEEK]

    cat_results = []
    h1_weeks_per_cat = cat_train.groupby('category').size().to_dict()

    for cat in cat_weekly['category'].unique():
        cat_tr = cat_train[cat_train['category'] == cat].dropna(subset=cat_features[:4])
        cat_te = cat_test[cat_test['category'] == cat].dropna(subset=cat_features[:4])

        if len(cat_tr) < 4 or len(cat_te) == 0:
            continue

        X_train = cat_tr[cat_features].fillna(0)
        y_train = cat_tr['weekly_quantity']
        X_test = cat_te[cat_features].fillna(0)

        model = train_model(X_train, y_train)
        preds = np.clip(model.predict(X_test), 0, None)

        for i, (_, row) in enumerate(cat_te.iterrows()):
            cat_results.append({
                'category': cat,
                'year_week': row['year_week'],
                'actual': row['weekly_quantity'],
                'predicted': round(preds[i], 1),
                'h1_weeks': h1_weeks_per_cat.get(cat, 0)
            })

    cat_df = pd.DataFrame(cat_results)

    # Calculate per-category WMAPE
    cat_wmape_dict = cat_df.groupby('category').apply(
        lambda x: calculate_wmape(x['actual'], x['predicted'])
    ).to_dict()

    cat_df['wmape'] = cat_df['category'].map(cat_wmape_dict)
    cat_df['confidence'] = cat_df.apply(
        lambda x: get_confidence(x['wmape'], x['h1_weeks']), axis=1
    )

    cat_wmape_overall = calculate_wmape(cat_df['actual'], cat_df['predicted'])
    log(f"\n  ★ Category Overall WMAPE: {cat_wmape_overall:.1f}%")

    for cat in cat_df['category'].unique():
        wmape = cat_wmape_dict[cat]
        log(f"    {cat}: {wmape:.1f}%")

    cat_df.to_csv(OUTPUT_DIR / 'category_predictions_v4.csv', index=False)
    log(f"  ✓ Saved: category_predictions_v4.csv ({len(cat_df)} predictions)")

    # =========================================================================
    # PART 3: PER-CUSTOMER MODELS
    # =========================================================================
    log("\n[5/7] Training per-Customer models...")

    try:
        cust_sku = pd.read_csv(FEATURES_DIR / 'v2_features_sku_customer.csv')
        customers = pd.read_csv(FEATURES_DIR / 'v2_dim_customers.csv')
        cust_names = customers.set_index('customer_id')['customer_name'].to_dict()

        # Aggregate by customer and week
        cust_weekly = cust_sku.groupby(['customer_id', 'year_week']).agg({
            'weekly_quantity': 'sum'
        }).reset_index()

        cust_weekly['week_num'] = cust_weekly['year_week'].str.extract(r'W(\d+)').astype(int)

        # Add lag features
        cust_weekly = cust_weekly.sort_values(['customer_id', 'year_week'])
        cust_weekly['lag1'] = cust_weekly.groupby('customer_id')['weekly_quantity'].shift(1)
        cust_weekly['lag2'] = cust_weekly.groupby('customer_id')['weekly_quantity'].shift(2)
        cust_weekly['lag4'] = cust_weekly.groupby('customer_id')['weekly_quantity'].shift(4)
        cust_weekly['rolling_avg'] = cust_weekly.groupby('customer_id')['weekly_quantity'].transform(
            lambda x: x.shift(1).rolling(4, min_periods=1).mean()
        )

        cust_features = ['lag1', 'lag2', 'lag4', 'rolling_avg', 'week_num']

        cust_train = cust_weekly[cust_weekly['week_num'] <= H1_END_WEEK]
        cust_test = cust_weekly[cust_weekly['week_num'] > H1_END_WEEK]

        h1_weeks_per_cust = cust_train.groupby('customer_id').size().to_dict()

        # Train global customer model
        cust_train_valid = cust_train.dropna(subset=cust_features[:4])
        X_global_cust = cust_train_valid[cust_features].fillna(0)
        y_global_cust = cust_train_valid['weekly_quantity']
        global_cust_model = train_model(X_global_cust, y_global_cust)

        cust_results = []
        cust_models = {}

        # Train per-customer models for customers with enough data
        for cust in cust_train['customer_id'].unique():
            cust_tr = cust_train[cust_train['customer_id'] == cust].dropna(subset=cust_features[:4])

            if len(cust_tr) >= 8:  # Need sufficient data
                X_train = cust_tr[cust_features].fillna(0)
                y_train = cust_tr['weekly_quantity']
                try:
                    cust_models[cust] = train_model(X_train, y_train)
                except:
                    pass

        log(f"  ✓ Trained {len(cust_models)} individual customer models")

        # Predict for all customers
        for cust in cust_test['customer_id'].unique():
            cust_te = cust_test[cust_test['customer_id'] == cust].dropna(subset=cust_features[:4])

            if len(cust_te) == 0:
                continue

            X_test = cust_te[cust_features].fillna(0)

            if cust in cust_models:
                model = cust_models[cust]
                model_type = 'customer'
            else:
                model = global_cust_model
                model_type = 'global'

            preds = np.clip(model.predict(X_test), 0, None)

            for i, (_, row) in enumerate(cust_te.iterrows()):
                cust_results.append({
                    'customer_id': str(cust),
                    'customer_name': cust_names.get(cust, str(cust)),
                    'year_week': row['year_week'],
                    'actual': row['weekly_quantity'],
                    'predicted': round(preds[i], 1),
                    'model_type': model_type,
                    'h1_weeks': h1_weeks_per_cust.get(cust, 0)
                })

        cust_df = pd.DataFrame(cust_results)

        # Calculate per-customer WMAPE
        cust_wmape_dict = cust_df.groupby('customer_id').apply(
            lambda x: calculate_wmape(x['actual'], x['predicted'])
        ).to_dict()

        cust_df['wmape'] = cust_df['customer_id'].map(cust_wmape_dict)
        cust_df['confidence'] = cust_df.apply(
            lambda x: get_confidence(x['wmape'], x['h1_weeks']), axis=1
        )

        cust_wmape_overall = calculate_wmape(cust_df['actual'], cust_df['predicted'])
        log(f"\n  ★ Customer Overall WMAPE: {cust_wmape_overall:.1f}%")

        conf_dist = cust_df.groupby('customer_id')['confidence'].first().value_counts()
        log(f"  Confidence: High={conf_dist.get('High', 0)}, Medium={conf_dist.get('Medium', 0)}, Low={conf_dist.get('Low', 0)}")

        cust_df.to_csv(OUTPUT_DIR / 'customer_predictions_v4.csv', index=False)
        log(f"  ✓ Saved: customer_predictions_v4.csv ({len(cust_df)} predictions)")

    except Exception as e:
        log(f"  ⚠ Customer models failed: {e}")
        cust_df = pd.DataFrame()
        cust_wmape_overall = None

    # =========================================================================
    # PART 4: SAVE H1 ACTUALS FOR DASHBOARD
    # =========================================================================
    log("\n[6/7] Saving H1 actuals for dashboard...")

    # SKU H1 actuals
    h1_actuals = train_all[['sku', 'year_week', 'weekly_quantity', 'category']].copy()
    h1_actuals['description'] = h1_actuals['sku'].map(sku_name)
    h1_actuals.to_csv(OUTPUT_DIR / 'sku_h1_actuals_v4.csv', index=False)

    # Category H1 actuals
    cat_h1 = cat_train[['category', 'year_week', 'weekly_quantity']].copy()
    cat_h1.to_csv(OUTPUT_DIR / 'category_h1_actuals_v4.csv', index=False)

    # Customer H1 actuals
    if 'cust_train' in dir():
        cust_h1 = cust_train[['customer_id', 'year_week', 'weekly_quantity']].copy()
        cust_h1['customer_name'] = cust_h1['customer_id'].map(cust_names)
        cust_h1.to_csv(OUTPUT_DIR / 'customer_h1_actuals_v4.csv', index=False)

    log("  ✓ Saved H1 actuals")

    # =========================================================================
    # SUMMARY
    # =========================================================================
    log("\n[7/7] Summary")
    log("\n" + "=" * 70)
    log("V4 MODEL TRAINING COMPLETE")
    log("=" * 70)

    log(f"\n  SKU Level:")
    log(f"    WMAPE: {sku_wmape_overall:.1f}%")
    log(f"    SKUs: {sku_df['sku'].nunique()}")
    log(f"    Predictions: {len(sku_df)}")

    log(f"\n  Category Level:")
    log(f"    WMAPE: {cat_wmape_overall:.1f}%")
    log(f"    Categories: {cat_df['category'].nunique()}")
    log(f"    Predictions: {len(cat_df)}")

    if cust_wmape_overall:
        log(f"\n  Customer Level:")
        log(f"    WMAPE: {cust_wmape_overall:.1f}%")
        log(f"    Customers: {cust_df['customer_id'].nunique()}")
        log(f"    Predictions: {len(cust_df)}")

    log(f"\n  Files saved to: {OUTPUT_DIR}")
    log("=" * 70)

    return {
        'sku_wmape': sku_wmape_overall,
        'cat_wmape': cat_wmape_overall,
        'cust_wmape': cust_wmape_overall
    }

if __name__ == '__main__':
    main()
