"""
=============================================================================
XGBoost Forecasting Model - Deployable to Vertex AI Endpoint
=============================================================================
Entrena un modelo XGBoost para forecasting que SÍ se puede desplegar
a un endpoint para predicciones en tiempo real.

Este approach:
1. Crea features de time series (lags, rolling means, etc.)
2. Entrena XGBoost como modelo de regresión
3. Registra el modelo en Vertex AI Model Registry
4. Despliega a endpoint

Usage:
    python 07_xgboost_forecast_endpoint.py
=============================================================================
"""

import pandas as pd
import numpy as np
from google.cloud import bigquery, storage, aiplatform
from sklearn.preprocessing import LabelEncoder
import xgboost as xgb
import joblib
import os
from datetime import datetime

# Configuration
PROJECT_ID = "mimetic-maxim-443710-s2"
REGION = "europe-west4"
BUCKET_NAME = "demand_planning_aca"
BUCKET = f"gs://{BUCKET_NAME}"
DATASET_ID = "redai_demand_forecast_eu"

print("=" * 60)
print("  XGBoost Forecasting - Deployable Model")
print("=" * 60)

# Initialize
aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=BUCKET)
bq_client = bigquery.Client(project=PROJECT_ID)

# =============================================================================
# Step 1: Load and prepare data
# =============================================================================
print("\n1. Loading training data from BigQuery...")

query = f"""
SELECT * FROM `{PROJECT_ID}.{DATASET_ID}.forecast_sku_weekly_H1`
ORDER BY sku, date
"""
df = bq_client.query(query).to_dataframe()
print(f"   Loaded {len(df):,} rows")

# =============================================================================
# Step 2: Feature Engineering
# =============================================================================
print("\n2. Creating time series features...")

def create_ts_features(df, target_col='revenue', group_col='sku'):
    """Create lag features and rolling statistics for time series"""
    df = df.sort_values([group_col, 'date']).copy()
    
    # Date features
    df['date'] = pd.to_datetime(df['date'])
    df['week_of_year'] = df['date'].dt.isocalendar().week.astype(int)
    df['month'] = df['date'].dt.month
    df['quarter'] = df['date'].dt.quarter
    
    # Lag features (previous weeks' revenue)
    for lag in [1, 2, 3, 4]:
        df[f'revenue_lag_{lag}'] = df.groupby(group_col)[target_col].shift(lag)
    
    # Rolling statistics
    df['revenue_rolling_mean_4'] = df.groupby(group_col)[target_col].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).mean()
    )
    df['revenue_rolling_std_4'] = df.groupby(group_col)[target_col].transform(
        lambda x: x.shift(1).rolling(4, min_periods=1).std()
    )
    
    # Quantity features (also useful predictors)
    df['quantity_lag_1'] = df.groupby(group_col)['quantity'].shift(1)
    df['orders_lag_1'] = df.groupby(group_col)['num_orders'].shift(1)
    
    return df

df = create_ts_features(df)

# Drop rows with NaN (first few weeks per SKU due to lags)
df_clean = df.dropna()
print(f"   After feature engineering: {len(df_clean):,} rows")

# =============================================================================
# Step 3: Encode categorical features
# =============================================================================
print("\n3. Encoding categorical features...")

# Encode SKU and category
le_sku = LabelEncoder()
le_cat = LabelEncoder()

df_clean['sku_encoded'] = le_sku.fit_transform(df_clean['sku'].astype(str))
df_clean['category_encoded'] = le_cat.fit_transform(df_clean['category'].astype(str))

# =============================================================================
# Step 4: Prepare training data
# =============================================================================
print("\n4. Preparing training data...")

feature_cols = [
    'sku_encoded', 'category_encoded',
    'week_of_year', 'month', 'quarter',
    'revenue_lag_1', 'revenue_lag_2', 'revenue_lag_3', 'revenue_lag_4',
    'revenue_rolling_mean_4', 'revenue_rolling_std_4',
    'quantity_lag_1', 'orders_lag_1'
]

X = df_clean[feature_cols].values
y = df_clean['revenue'].values

print(f"   Features: {len(feature_cols)}")
print(f"   Training samples: {len(X):,}")

# =============================================================================
# Step 5: Train XGBoost
# =============================================================================
print("\n5. Training XGBoost model...")

model = xgb.XGBRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    n_jobs=-1
)

model.fit(X, y)
print("   ✓ Model trained")

# =============================================================================
# Step 6: Save model artifacts
# =============================================================================
print("\n6. Saving model artifacts...")

# Create artifacts directory
os.makedirs('model_artifacts', exist_ok=True)

# Save model
model.save_model('model_artifacts/model.bst')

# Save encoders
joblib.dump(le_sku, 'model_artifacts/le_sku.joblib')
joblib.dump(le_cat, 'model_artifacts/le_cat.joblib')

# Save feature columns
joblib.dump(feature_cols, 'model_artifacts/feature_cols.joblib')

# Save SKU mapping for reference
sku_mapping = pd.DataFrame({
    'sku': le_sku.classes_,
    'sku_encoded': range(len(le_sku.classes_))
})
sku_mapping.to_csv('model_artifacts/sku_mapping.csv', index=False)

print("   ✓ Artifacts saved to model_artifacts/")

# =============================================================================
# Step 7: Create custom prediction container
# =============================================================================
print("\n7. Creating prediction script...")

predictor_code = '''
import joblib
import xgboost as xgb
import numpy as np
import os

class Predictor:
    """Custom predictor for XGBoost forecasting model"""
    
    def __init__(self):
        self._model = None
        self._le_sku = None
        self._le_cat = None
        self._feature_cols = None
    
    def load(self, artifacts_path):
        """Load model and encoders"""
        self._model = xgb.XGBRegressor()
        self._model.load_model(os.path.join(artifacts_path, 'model.bst'))
        self._le_sku = joblib.load(os.path.join(artifacts_path, 'le_sku.joblib'))
        self._le_cat = joblib.load(os.path.join(artifacts_path, 'le_cat.joblib'))
        self._feature_cols = joblib.load(os.path.join(artifacts_path, 'feature_cols.joblib'))
    
    def predict(self, instances):
        """
        Make predictions.
        
        instances: list of dicts with keys:
            - sku: SKU code
            - category: category name
            - week_of_year, month, quarter: time features
            - revenue_lag_1, revenue_lag_2, etc.: historical revenue
            - quantity_lag_1, orders_lag_1: historical metrics
        """
        predictions = []
        
        for instance in instances:
            # Encode categorical features
            try:
                sku_enc = self._le_sku.transform([str(instance['sku'])])[0]
            except ValueError:
                sku_enc = -1  # Unknown SKU
            
            try:
                cat_enc = self._le_cat.transform([str(instance['category'])])[0]
            except ValueError:
                cat_enc = -1  # Unknown category
            
            # Build feature vector
            features = [
                sku_enc,
                cat_enc,
                instance.get('week_of_year', 1),
                instance.get('month', 1),
                instance.get('quarter', 1),
                instance.get('revenue_lag_1', 0),
                instance.get('revenue_lag_2', 0),
                instance.get('revenue_lag_3', 0),
                instance.get('revenue_lag_4', 0),
                instance.get('revenue_rolling_mean_4', 0),
                instance.get('revenue_rolling_std_4', 0),
                instance.get('quantity_lag_1', 0),
                instance.get('orders_lag_1', 0),
            ]
            
            # Predict
            pred = self._model.predict(np.array([features]))[0]
            predictions.append({'predicted_revenue': float(pred)})
        
        return predictions
'''

with open('model_artifacts/predictor.py', 'w') as f:
    f.write(predictor_code)

print("   ✓ Predictor script created")

# =============================================================================
# Step 8: Upload to GCS
# =============================================================================
print("\n8. Uploading artifacts to GCS...")

storage_client = storage.Client(project=PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)

artifact_uri = f"{BUCKET}/models/xgboost_forecast/{datetime.now().strftime('%Y%m%d_%H%M%S')}"

for filename in os.listdir('model_artifacts'):
    blob = bucket.blob(f"models/xgboost_forecast/{datetime.now().strftime('%Y%m%d_%H%M%S')}/{filename}")
    blob.upload_from_filename(f'model_artifacts/{filename}')
    print(f"   Uploaded: {filename}")

print(f"   Artifact URI: {artifact_uri}")

# =============================================================================
# Step 9: Register model in Vertex AI
# =============================================================================
print("\n9. Registering model in Vertex AI...")

model_vertex = aiplatform.Model.upload(
    display_name="xgboost_sku_weekly_forecast",
    artifact_uri=artifact_uri,
    serving_container_image_uri="us-docker.pkg.dev/vertex-ai/prediction/xgboost-cpu.1-7:latest",
)

print(f"   ✓ Model registered: {model_vertex.resource_name}")

# =============================================================================
# Step 10: Deploy to endpoint
# =============================================================================
print("\n10. Deploying to endpoint...")

endpoint = model_vertex.deploy(
    deployed_model_display_name="xgboost_sku_forecast",
    machine_type="n1-standard-2",
    min_replica_count=1,
    max_replica_count=2,
    sync=True,
)

print(f"   ✓ Endpoint: {endpoint.resource_name}")

# =============================================================================
# Summary
# =============================================================================
print("\n" + "=" * 60)
print("  DEPLOYMENT COMPLETE!")
print("=" * 60)
print(f"""
Model:    {model_vertex.resource_name}
Endpoint: {endpoint.resource_name}

Example prediction request:
    
    from google.cloud import aiplatform
    
    endpoint = aiplatform.Endpoint('{endpoint.resource_name}')
    
    prediction = endpoint.predict(instances=[{{
        'sku': '10002',
        'category': 'Hardware',
        'week_of_year': 27,
        'month': 7,
        'quarter': 3,
        'revenue_lag_1': 2500000,
        'revenue_lag_2': 2300000,
        'revenue_lag_3': 2100000,
        'revenue_lag_4': 2000000,
        'revenue_rolling_mean_4': 2225000,
        'revenue_rolling_std_4': 180000,
        'quantity_lag_1': 2000,
        'orders_lag_1': 30
    }}])
    
    print(prediction)
""")
