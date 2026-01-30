#!/usr/bin/env python3
"""
ACA Demand Planning - Model Training Script
============================================
This script trains XGBoost models for SKU, Category, and Customer levels.

RUN THIS SCRIPT: python3 scripts/TRAIN_ALL_MODELS.py

The output will be logged to: model_training_log.txt
"""

import pandas as pd
import numpy as np
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error
import warnings
import sys
from datetime import datetime
from pathlib import Path

warnings.filterwarnings('ignore')

# Setup paths
SCRIPT_DIR = Path(__file__).parent.resolve()
BASE_PATH = SCRIPT_DIR.parent
FEATURES_DIR = BASE_PATH / 'features_v2'
OUTPUT_DIR = BASE_PATH / 'model_evaluation'
OUTPUT_DIR.mkdir(exist_ok=True)

# Logging
LOG_FILE = BASE_PATH / 'model_training_log.txt'

def log(msg):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, 'a') as f:
        f.write(line + '\n')

def calculate_wmape(actual, predicted):
    """Weighted Mean Absolute Percentage Error"""
    return 100 * np.sum(np.abs(actual - predicted)) / np.sum(actual)

def train_xgboost_model(X_train, y_train, X_test):
    """Train XGBoost (GradientBoosting) model"""
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
    return np.clip(predictions, 0, None)  # No negative predictions

def main():
    log("=" * 60)
    log("ACA DEMAND PLANNING - MODEL TRAINING")
    log("=" * 60)
    
    # Load features
    log("\n[1/4] Loading feature data...")
    try:
        weekly = pd.read_csv(FEATURES_DIR / 'v2_features_weekly.csv')
        log(f"  ✓ Loaded weekly features: {len(weekly)} rows, {weekly['sku'].nunique()} SKUs")
        log(f"  ✓ Week range: {weekly['year_week'].min()} to {weekly['year_week'].max()}")
    except Exception as e:
        log(f"  ✗ ERROR loading features: {e}")
        return
    
    # Define H1 (training) and H2 (validation) periods
    H1_END = '2025-W26'
    weekly['is_h1'] = weekly['year_week'] <= H1_END
    
    h1_data = weekly[weekly['is_h1']]
    h2_data = weekly[~weekly['is_h1']]
    
    log(f"  ✓ H1 (training): {len(h1_data)} rows")
    log(f"  ✓ H2 (validation): {len(h2_data)} rows")
    
    # =========================================
    # SKU LEVEL MODELS
    # =========================================
    log("\n[2/4] Training SKU-level XGBoost models...")
    
    # Count H1 weeks per SKU for data sufficiency
    h1_weeks_per_sku = h1_data.groupby('sku').size().reset_index(name='h1_weeks')
    
    # Features to use
    feature_cols = ['lag1_quantity', 'lag2_quantity', 'lag4_quantity', 'rolling_avg_4w']
    
    # Only train for SKUs with enough data
    eligible_skus = h1_weeks_per_sku[h1_weeks_per_sku['h1_weeks'] >= 4]['sku'].tolist()
    log(f"  ✓ Eligible SKUs (≥4 H1 weeks): {len(eligible_skus)}")
    
    sku_results = []
    sku_h1_actuals = []
    trained_count = 0
    skipped_count = 0
    
    for sku in eligible_skus:
        sku_h1 = h1_data[h1_data['sku'] == sku].copy()
        sku_h2 = h2_data[h2_data['sku'] == sku].copy()
        
        # Skip if no H2 data to validate
        if len(sku_h2) == 0:
            skipped_count += 1
            continue
        
        # Prepare training data
        sku_h1_clean = sku_h1.dropna(subset=feature_cols + ['weekly_quantity'])
        if len(sku_h1_clean) < 3:
            skipped_count += 1
            continue
        
        X_train = sku_h1_clean[feature_cols].values
        y_train = sku_h1_clean['weekly_quantity'].values
        
        # Prepare test data
        sku_h2_clean = sku_h2.dropna(subset=feature_cols)
        if len(sku_h2_clean) == 0:
            skipped_count += 1
            continue
        
        X_test = sku_h2_clean[feature_cols].values
        y_test = sku_h2_clean['weekly_quantity'].values
        
        # Train and predict
        try:
            predictions = train_xgboost_model(X_train, y_train, X_test)
            
            for i, (_, row) in enumerate(sku_h2_clean.iterrows()):
                sku_results.append({
                    'sku': sku,
                    'year_week': row['year_week'],
                    'actual': row['weekly_quantity'],
                    'predicted': predictions[i],
                    'abs_error': abs(predictions[i] - row['weekly_quantity']),
                    'pct_error': 100 * abs(predictions[i] - row['weekly_quantity']) / row['weekly_quantity'] if row['weekly_quantity'] > 0 else 0
                })
            
            trained_count += 1
        except Exception as e:
            skipped_count += 1
            continue
        
        # Store H1 actuals
        for _, row in sku_h1.iterrows():
            sku_h1_actuals.append({
                'sku': sku,
                'year_week': row['year_week'],
                'actual': row['weekly_quantity'],
                'description': row.get('description', '')
            })
    
    log(f"  ✓ Trained models: {trained_count}")
    log(f"  ✓ Skipped (insufficient data): {skipped_count}")
    
    # Save SKU results
    if sku_results:
        sku_df = pd.DataFrame(sku_results)
        sku_df.to_csv(OUTPUT_DIR / 'sku_predictions_XGBoost_v3.csv', index=False)
        log(f"  ✓ Saved: sku_predictions_XGBoost_v3.csv ({len(sku_df)} predictions)")
        
        # Calculate overall WMAPE
        overall_wmape = calculate_wmape(sku_df['actual'], sku_df['predicted'])
        log(f"  ✓ SKU Overall WMAPE: {overall_wmape:.1f}%")
        
        # Save H1 actuals
        h1_df = pd.DataFrame(sku_h1_actuals)
        h1_df.to_csv(OUTPUT_DIR / 'sku_h1_actuals_v3.csv', index=False)
        log(f"  ✓ Saved: sku_h1_actuals_v3.csv")
    
    # =========================================
    # CATEGORY LEVEL MODELS
    # =========================================
    log("\n[3/4] Training Category-level XGBoost models...")
    
    # Aggregate by category
    # First, get category mapping from products
    try:
        products = pd.read_csv(FEATURES_DIR / 'v2_dim_products.csv')
        sku_to_cat = products[['sku', 'category']].drop_duplicates().set_index('sku')['category'].to_dict()
        weekly['category'] = weekly['sku'].map(sku_to_cat)
    except:
        log("  ! Could not load category mapping, using 'Unknown'")
        weekly['category'] = 'Unknown'
    
    # Aggregate weekly data by category
    cat_weekly = weekly.groupby(['category', 'year_week']).agg({
        'weekly_quantity': 'sum',
        'is_h1': 'first'
    }).reset_index()
    
    # Add lag features for categories
    cat_weekly = cat_weekly.sort_values(['category', 'year_week'])
    cat_weekly['lag1'] = cat_weekly.groupby('category')['weekly_quantity'].shift(1)
    cat_weekly['lag2'] = cat_weekly.groupby('category')['weekly_quantity'].shift(2)
    cat_weekly['lag4'] = cat_weekly.groupby('category')['weekly_quantity'].shift(4)
    cat_weekly['rolling_avg'] = cat_weekly.groupby('category')['weekly_quantity'].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).mean()
    )
    
    cat_h1 = cat_weekly[cat_weekly['is_h1']]
    cat_h2 = cat_weekly[~cat_weekly['is_h1']]
    
    cat_feature_cols = ['lag1', 'lag2', 'lag4', 'rolling_avg']
    cat_results = []
    cat_h1_actuals = []
    
    for cat in cat_weekly['category'].unique():
        if pd.isna(cat):
            continue
            
        cat_train = cat_h1[cat_h1['category'] == cat].dropna(subset=cat_feature_cols)
        cat_test = cat_h2[cat_h2['category'] == cat].dropna(subset=cat_feature_cols)
        
        if len(cat_train) < 3 or len(cat_test) == 0:
            continue
        
        X_train = cat_train[cat_feature_cols].values
        y_train = cat_train['weekly_quantity'].values
        X_test = cat_test[cat_feature_cols].values
        
        try:
            predictions = train_xgboost_model(X_train, y_train, X_test)
            
            for i, (_, row) in enumerate(cat_test.iterrows()):
                cat_results.append({
                    'category': cat,
                    'year_week': row['year_week'],
                    'actual': row['weekly_quantity'],
                    'predicted': predictions[i],
                    'abs_error': abs(predictions[i] - row['weekly_quantity']),
                    'pct_error': 100 * abs(predictions[i] - row['weekly_quantity']) / row['weekly_quantity'] if row['weekly_quantity'] > 0 else 0
                })
        except:
            continue
        
        # Store H1 actuals
        for _, row in cat_h1[cat_h1['category'] == cat].iterrows():
            cat_h1_actuals.append({
                'category': cat,
                'year_week': row['year_week'],
                'actual': row['weekly_quantity']
            })
    
    if cat_results:
        cat_df = pd.DataFrame(cat_results)
        cat_df.to_csv(OUTPUT_DIR / 'category_predictions_XGBoost_v3.csv', index=False)
        log(f"  ✓ Saved: category_predictions_XGBoost_v3.csv ({len(cat_df)} predictions)")
        
        cat_wmape = calculate_wmape(cat_df['actual'], cat_df['predicted'])
        log(f"  ✓ Category Overall WMAPE: {cat_wmape:.1f}%")
        
        cat_h1_df = pd.DataFrame(cat_h1_actuals)
        cat_h1_df.to_csv(OUTPUT_DIR / 'category_h1_actuals_v3.csv', index=False)
        log(f"  ✓ Saved: category_h1_actuals_v3.csv")
    
    # =========================================
    # CUSTOMER LEVEL MODELS
    # =========================================
    log("\n[4/4] Training Customer-level XGBoost models...")
    
    # Load customer features
    try:
        cust_sku = pd.read_csv(FEATURES_DIR / 'v2_features_sku_customer.csv')
        customers = pd.read_csv(FEATURES_DIR / 'v2_dim_customers.csv')
        cust_names = customers.set_index('customer_id')['customer_name'].to_dict()
        log(f"  ✓ Loaded customer data: {cust_sku['customer_id'].nunique()} customers")
    except Exception as e:
        log(f"  ! Could not load customer features: {e}")
        cust_sku = None
    
    if cust_sku is not None:
        # Aggregate by customer and week
        cust_weekly = cust_sku.groupby(['customer_id', 'year_week']).agg({
            'weekly_quantity': 'sum'
        }).reset_index()
        
        cust_weekly['is_h1'] = cust_weekly['year_week'] <= H1_END
        
        # Add lag features
        cust_weekly = cust_weekly.sort_values(['customer_id', 'year_week'])
        cust_weekly['lag1'] = cust_weekly.groupby('customer_id')['weekly_quantity'].shift(1)
        cust_weekly['lag2'] = cust_weekly.groupby('customer_id')['weekly_quantity'].shift(2)
        cust_weekly['lag4'] = cust_weekly.groupby('customer_id')['weekly_quantity'].shift(4)
        cust_weekly['rolling_avg'] = cust_weekly.groupby('customer_id')['weekly_quantity'].transform(
            lambda x: x.shift(1).rolling(4, min_periods=1).mean()
        )
        
        cust_h1 = cust_weekly[cust_weekly['is_h1']]
        cust_h2 = cust_weekly[~cust_weekly['is_h1']]
        
        cust_feature_cols = ['lag1', 'lag2', 'lag4', 'rolling_avg']
        cust_results = []
        cust_h1_actuals = []
        
        # Count H1 weeks per customer
        h1_weeks_per_cust = cust_h1.groupby('customer_id').size().reset_index(name='h1_weeks')
        eligible_custs = h1_weeks_per_cust[h1_weeks_per_cust['h1_weeks'] >= 4]['customer_id'].tolist()
        log(f"  ✓ Eligible customers (≥4 H1 weeks): {len(eligible_custs)}")
        
        trained_cust = 0
        for cust in eligible_custs:
            cust_train = cust_h1[cust_h1['customer_id'] == cust].dropna(subset=cust_feature_cols)
            cust_test = cust_h2[cust_h2['customer_id'] == cust].dropna(subset=cust_feature_cols)
            
            if len(cust_train) < 3 or len(cust_test) == 0:
                continue
            
            X_train = cust_train[cust_feature_cols].values
            y_train = cust_train['weekly_quantity'].values
            X_test = cust_test[cust_feature_cols].values
            
            try:
                predictions = train_xgboost_model(X_train, y_train, X_test)
                
                for i, (_, row) in enumerate(cust_test.iterrows()):
                    cust_results.append({
                        'customer_id': cust,
                        'customer_name': cust_names.get(cust, str(cust)),
                        'year_week': row['year_week'],
                        'actual': row['weekly_quantity'],
                        'predicted': predictions[i],
                        'abs_error': abs(predictions[i] - row['weekly_quantity']),
                        'pct_error': 100 * abs(predictions[i] - row['weekly_quantity']) / row['weekly_quantity'] if row['weekly_quantity'] > 0 else 0
                    })
                trained_cust += 1
            except:
                continue
            
            # Store H1 actuals
            for _, row in cust_h1[cust_h1['customer_id'] == cust].iterrows():
                cust_h1_actuals.append({
                    'customer_id': cust,
                    'customer_name': cust_names.get(cust, str(cust)),
                    'year_week': row['year_week'],
                    'actual': row['weekly_quantity']
                })
        
        log(f"  ✓ Trained customer models: {trained_cust}")
        
        if cust_results:
            cust_df = pd.DataFrame(cust_results)
            cust_df.to_csv(OUTPUT_DIR / 'customer_predictions_XGBoost_v3.csv', index=False)
            log(f"  ✓ Saved: customer_predictions_XGBoost_v3.csv ({len(cust_df)} predictions)")
            
            cust_wmape = calculate_wmape(cust_df['actual'], cust_df['predicted'])
            log(f"  ✓ Customer Overall WMAPE: {cust_wmape:.1f}%")
            
            cust_h1_df = pd.DataFrame(cust_h1_actuals)
            cust_h1_df.to_csv(OUTPUT_DIR / 'customer_h1_actuals_v3.csv', index=False)
            log(f"  ✓ Saved: customer_h1_actuals_v3.csv")
    
    # =========================================
    # SUMMARY
    # =========================================
    log("\n" + "=" * 60)
    log("TRAINING COMPLETE")
    log("=" * 60)
    log(f"Output files saved to: {OUTPUT_DIR}")
    log(f"Log file: {LOG_FILE}")
    log("\nNext step: Run the dashboard data generator")

if __name__ == '__main__':
    main()
