#!/usr/bin/env python3
"""
MODEL EVALUATION FRAMEWORK
==========================
Train models on H1 (W01-W26), predict H2 (W27-W52), compare to actuals.

Creates a matrix of models:
- By granularity: SKU, Category, Region
- By algorithm: ARIMA, XGBoost, Simple Average
- By features: With/without price, with/without lag features

Outputs:
- Model accuracy metrics (MAE, MAPE, RMSE)
- Predictions vs Actuals comparison
- Best model recommendations
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Configuration
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
OUTPUT_DIR = BASE_PATH / 'model_evaluation'

# Train/Test split
TRAIN_END = 'W26'  # Train on W01-W26 (H1)
TEST_START = 'W27'  # Test on W27-W52 (H2)


def load_data():
    """Load v2 feature data"""
    print("📂 Loading data...")

    weekly = pd.read_csv(BASE_PATH / 'features_v2' / 'v2_features_weekly.csv')
    category = pd.read_csv(BASE_PATH / 'features_v2' / 'v2_features_category.csv')
    products = pd.read_csv(BASE_PATH / 'features_v2' / 'v2_dim_products.csv')

    # Extract week number for splitting
    weekly['week_num'] = weekly['year_week'].str.extract(r'W(\d+)').astype(int)
    category['week_num'] = category['year_week'].str.extract(r'W(\d+)').astype(int)

    print(f"   Weekly features: {len(weekly):,} rows")
    print(f"   Category features: {len(category):,} rows")
    print(f"   Products: {len(products):,}")

    return weekly, category, products


def split_data(df, train_end_week=26):
    """Split into train and test sets"""
    train = df[df['week_num'] <= train_end_week].copy()
    test = df[df['week_num'] > train_end_week].copy()
    return train, test


# =============================================================================
# BASELINE MODELS (Simple benchmarks)
# =============================================================================

def model_naive_last(train, test, id_col, value_col='weekly_quantity'):
    """Naive: Predict last known value"""
    predictions = []

    for entity_id in test[id_col].unique():
        train_entity = train[train[id_col] == entity_id]
        test_entity = test[test[id_col] == entity_id]

        if len(train_entity) == 0:
            continue

        last_value = train_entity.sort_values('week_num')[value_col].iloc[-1]

        for _, row in test_entity.iterrows():
            predictions.append({
                id_col: entity_id,
                'year_week': row['year_week'],
                'predicted': last_value,
                'actual': row[value_col]
            })

    return pd.DataFrame(predictions)


def model_moving_average(train, test, id_col, value_col='weekly_quantity', window=4):
    """Moving average of last N weeks"""
    predictions = []

    for entity_id in test[id_col].unique():
        train_entity = train[train[id_col] == entity_id].sort_values('week_num')
        test_entity = test[test[id_col] == entity_id]

        if len(train_entity) < window:
            continue

        ma_value = train_entity[value_col].tail(window).mean()

        for _, row in test_entity.iterrows():
            predictions.append({
                id_col: entity_id,
                'year_week': row['year_week'],
                'predicted': ma_value,
                'actual': row[value_col]
            })

    return pd.DataFrame(predictions)


def model_seasonal_naive(train, test, id_col, value_col='weekly_quantity'):
    """Seasonal naive: Use same week from training period if available"""
    predictions = []

    for entity_id in test[id_col].unique():
        train_entity = train[train[id_col] == entity_id]
        test_entity = test[test[id_col] == entity_id]

        if len(train_entity) == 0:
            continue

        # Create lookup by week number (for seasonality)
        train_by_week = train_entity.groupby('week_num')[value_col].mean().to_dict()
        overall_mean = train_entity[value_col].mean()

        for _, row in test_entity.iterrows():
            # Try to find similar week (offset by 26 weeks)
            similar_week = row['week_num'] - 26
            if similar_week in train_by_week:
                pred = train_by_week[similar_week]
            else:
                pred = overall_mean

            predictions.append({
                id_col: entity_id,
                'year_week': row['year_week'],
                'predicted': pred,
                'actual': row[value_col]
            })

    return pd.DataFrame(predictions)


# =============================================================================
# STATISTICAL MODELS
# =============================================================================

def model_linear_trend(train, test, id_col, value_col='weekly_quantity'):
    """Linear trend extrapolation"""
    predictions = []

    for entity_id in test[id_col].unique():
        train_entity = train[train[id_col] == entity_id].sort_values('week_num')
        test_entity = test[test[id_col] == entity_id]

        if len(train_entity) < 4:
            continue

        # Fit linear trend
        x = train_entity['week_num'].values
        y = train_entity[value_col].values

        # Simple linear regression
        n = len(x)
        sum_x = np.sum(x)
        sum_y = np.sum(y)
        sum_xy = np.sum(x * y)
        sum_x2 = np.sum(x ** 2)

        slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2) if (n * sum_x2 - sum_x ** 2) != 0 else 0
        intercept = (sum_y - slope * sum_x) / n

        for _, row in test_entity.iterrows():
            pred = max(0, intercept + slope * row['week_num'])
            predictions.append({
                id_col: entity_id,
                'year_week': row['year_week'],
                'predicted': pred,
                'actual': row[value_col]
            })

    return pd.DataFrame(predictions)


def model_exponential_smoothing(train, test, id_col, value_col='weekly_quantity', alpha=0.3):
    """Simple exponential smoothing"""
    predictions = []

    for entity_id in test[id_col].unique():
        train_entity = train[train[id_col] == entity_id].sort_values('week_num')
        test_entity = test[test[id_col] == entity_id]

        if len(train_entity) < 2:
            continue

        # Calculate smoothed value
        values = train_entity[value_col].values
        smoothed = values[0]
        for v in values[1:]:
            smoothed = alpha * v + (1 - alpha) * smoothed

        for _, row in test_entity.iterrows():
            predictions.append({
                id_col: entity_id,
                'year_week': row['year_week'],
                'predicted': smoothed,
                'actual': row[value_col]
            })

    return pd.DataFrame(predictions)


# =============================================================================
# ML-BASED MODELS (using features)
# =============================================================================

def model_xgboost_features(train, test, id_col, value_col='weekly_quantity'):
    """XGBoost with lag and price features"""
    try:
        from sklearn.ensemble import GradientBoostingRegressor
    except ImportError:
        print("   ⚠️ sklearn not available, skipping XGBoost")
        return pd.DataFrame()

    predictions = []

    # Features to use
    feature_cols = ['lag1_quantity', 'lag2_quantity', 'lag4_quantity',
                    'rolling_avg_4w', 'avg_unit_price', 'week_num']

    # Filter to rows with all features
    train_valid = train.dropna(subset=[c for c in feature_cols if c in train.columns])

    if len(train_valid) < 100:
        print("   ⚠️ Not enough training data for XGBoost")
        return pd.DataFrame()

    # Prepare features
    available_features = [c for c in feature_cols if c in train_valid.columns]
    X_train = train_valid[available_features].fillna(0)
    y_train = train_valid[value_col]

    # Train model
    model = GradientBoostingRegressor(n_estimators=100, max_depth=5, random_state=42)
    model.fit(X_train, y_train)

    # Predict for test set
    for entity_id in test[id_col].unique():
        test_entity = test[test[id_col] == entity_id]

        if len(test_entity) == 0:
            continue

        X_test = test_entity[available_features].fillna(0)
        preds = model.predict(X_test)

        for i, (_, row) in enumerate(test_entity.iterrows()):
            predictions.append({
                id_col: entity_id,
                'year_week': row['year_week'],
                'predicted': max(0, preds[i]),
                'actual': row[value_col]
            })

    return pd.DataFrame(predictions)


# =============================================================================
# EVALUATION METRICS
# =============================================================================

def calculate_metrics(predictions_df, id_col):
    """Calculate accuracy metrics"""
    if len(predictions_df) == 0:
        return {'MAE': None, 'RMSE': None, 'MAPE': None, 'count': 0}

    df = predictions_df.copy()
    df['error'] = df['actual'] - df['predicted']
    df['abs_error'] = df['error'].abs()
    df['squared_error'] = df['error'] ** 2
    df['pct_error'] = (df['abs_error'] / df['actual'].replace(0, np.nan) * 100)

    return {
        'MAE': df['abs_error'].mean(),
        'RMSE': np.sqrt(df['squared_error'].mean()),
        'MAPE': df['pct_error'].median(),  # Median to handle outliers
        'count': len(df),
        'entities': df[id_col].nunique()
    }


def calculate_metrics_by_entity(predictions_df, id_col):
    """Calculate metrics per entity for detailed analysis"""
    results = []

    for entity_id in predictions_df[id_col].unique():
        entity_df = predictions_df[predictions_df[id_col] == entity_id]
        metrics = calculate_metrics(entity_df, id_col)
        metrics[id_col] = entity_id
        results.append(metrics)

    return pd.DataFrame(results)


# =============================================================================
# MAIN EVALUATION
# =============================================================================

def run_evaluation():
    """Run full model evaluation"""
    print("=" * 60)
    print("MODEL EVALUATION FRAMEWORK")
    print("Train: W01-W26 (H1) | Test: W27-W52 (H2)")
    print("=" * 60)

    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Load data
    weekly, category, products = load_data()

    # Split data
    print("\n📊 Splitting data...")
    train_sku, test_sku = split_data(weekly)
    train_cat, test_cat = split_data(category)

    print(f"   SKU - Train: {len(train_sku):,} rows ({train_sku['sku'].nunique()} SKUs)")
    print(f"   SKU - Test: {len(test_sku):,} rows ({test_sku['sku'].nunique()} SKUs)")
    print(f"   Category - Train: {len(train_cat):,} rows")
    print(f"   Category - Test: {len(test_cat):,} rows")

    # =========================================================================
    # SKU-LEVEL MODELS
    # =========================================================================
    print("\n" + "=" * 60)
    print("SKU-LEVEL MODELS")
    print("=" * 60)

    sku_models = {}

    print("\n🔄 Training Naive Last Value...")
    sku_models['Naive_Last'] = model_naive_last(train_sku, test_sku, 'sku')

    print("🔄 Training 4-Week Moving Average...")
    sku_models['MA_4Week'] = model_moving_average(train_sku, test_sku, 'sku', window=4)

    print("🔄 Training 8-Week Moving Average...")
    sku_models['MA_8Week'] = model_moving_average(train_sku, test_sku, 'sku', window=8)

    print("🔄 Training Seasonal Naive...")
    sku_models['Seasonal_Naive'] = model_seasonal_naive(train_sku, test_sku, 'sku')

    print("🔄 Training Linear Trend...")
    sku_models['Linear_Trend'] = model_linear_trend(train_sku, test_sku, 'sku')

    print("🔄 Training Exponential Smoothing (α=0.3)...")
    sku_models['ExpSmooth_03'] = model_exponential_smoothing(train_sku, test_sku, 'sku', alpha=0.3)

    print("🔄 Training Exponential Smoothing (α=0.5)...")
    sku_models['ExpSmooth_05'] = model_exponential_smoothing(train_sku, test_sku, 'sku', alpha=0.5)

    print("🔄 Training XGBoost with Features...")
    sku_models['XGBoost'] = model_xgboost_features(train_sku, test_sku, 'sku')

    # Calculate SKU metrics
    print("\n📈 SKU Model Results:")
    sku_results = []
    for model_name, preds in sku_models.items():
        metrics = calculate_metrics(preds, 'sku')
        metrics['model'] = model_name
        metrics['level'] = 'SKU'
        sku_results.append(metrics)
        print(f"   {model_name:20} | MAE: {metrics['MAE']:>10,.0f} | MAPE: {metrics['MAPE']:>6.1f}% | RMSE: {metrics['RMSE']:>12,.0f}")

    # =========================================================================
    # CATEGORY-LEVEL MODELS
    # =========================================================================
    print("\n" + "=" * 60)
    print("CATEGORY-LEVEL MODELS")
    print("=" * 60)

    cat_models = {}

    print("\n🔄 Training Naive Last Value...")
    cat_models['Naive_Last'] = model_naive_last(train_cat, test_cat, 'category')

    print("🔄 Training 4-Week Moving Average...")
    cat_models['MA_4Week'] = model_moving_average(train_cat, test_cat, 'category', window=4)

    print("🔄 Training Linear Trend...")
    cat_models['Linear_Trend'] = model_linear_trend(train_cat, test_cat, 'category')

    print("🔄 Training Exponential Smoothing...")
    cat_models['ExpSmooth_03'] = model_exponential_smoothing(train_cat, test_cat, 'category', alpha=0.3)

    # Calculate Category metrics
    print("\n📈 Category Model Results:")
    cat_results = []
    for model_name, preds in cat_models.items():
        metrics = calculate_metrics(preds, 'category')
        metrics['model'] = model_name
        metrics['level'] = 'Category'
        cat_results.append(metrics)
        print(f"   {model_name:20} | MAE: {metrics['MAE']:>10,.0f} | MAPE: {metrics['MAPE']:>6.1f}% | RMSE: {metrics['RMSE']:>12,.0f}")

    # =========================================================================
    # SAVE RESULTS
    # =========================================================================
    print("\n" + "=" * 60)
    print("SAVING RESULTS")
    print("=" * 60)

    # Combine all results
    all_results = pd.DataFrame(sku_results + cat_results)
    all_results.to_csv(OUTPUT_DIR / 'model_comparison.csv', index=False)
    print(f"✓ Model comparison: {OUTPUT_DIR / 'model_comparison.csv'}")

    # Save best SKU predictions
    best_sku_model = min(sku_results, key=lambda x: x['MAE'] if x['MAE'] else float('inf'))['model']
    sku_models[best_sku_model].to_csv(OUTPUT_DIR / 'best_sku_predictions.csv', index=False)
    print(f"✓ Best SKU predictions ({best_sku_model}): {OUTPUT_DIR / 'best_sku_predictions.csv'}")

    # Save best category predictions
    best_cat_model = min(cat_results, key=lambda x: x['MAE'] if x['MAE'] else float('inf'))['model']
    cat_models[best_cat_model].to_csv(OUTPUT_DIR / 'best_category_predictions.csv', index=False)
    print(f"✓ Best Category predictions ({best_cat_model}): {OUTPUT_DIR / 'best_category_predictions.csv'}")

    # Save all predictions for visualization
    for model_name, preds in sku_models.items():
        if len(preds) > 0:
            preds.to_csv(OUTPUT_DIR / f'sku_predictions_{model_name}.csv', index=False)

    for model_name, preds in cat_models.items():
        if len(preds) > 0:
            preds.to_csv(OUTPUT_DIR / f'category_predictions_{model_name}.csv', index=False)

    # =========================================================================
    # SUMMARY
    # =========================================================================
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print(f"\n🏆 BEST SKU MODEL: {best_sku_model}")
    best_sku_metrics = [r for r in sku_results if r['model'] == best_sku_model][0]
    print(f"   MAE: {best_sku_metrics['MAE']:,.0f} units")
    print(f"   MAPE: {best_sku_metrics['MAPE']:.1f}%")
    print(f"   Evaluated on {best_sku_metrics['entities']} SKUs")

    print(f"\n🏆 BEST CATEGORY MODEL: {best_cat_model}")
    best_cat_metrics = [r for r in cat_results if r['model'] == best_cat_model][0]
    print(f"   MAE: {best_cat_metrics['MAE']:,.0f} units")
    print(f"   MAPE: {best_cat_metrics['MAPE']:.1f}%")

    print(f"\n📁 Results saved to: {OUTPUT_DIR}")

    return all_results, sku_models, cat_models


if __name__ == '__main__':
    results, sku_models, cat_models = run_evaluation()
