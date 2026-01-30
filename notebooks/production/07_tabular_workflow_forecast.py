"""
=============================================================================
Vertex AI Tabular Workflows - Time Series Forecasting
=============================================================================
Este script entrena un modelo de forecasting usando Tabular Workflows,
que SÍ permite deployment a endpoint (a diferencia del managed service).

Usage:
    python 07_tabular_workflow_forecast.py
=============================================================================
"""

from google.cloud import aiplatform
from google.cloud.aiplatform import hyperparameter_tuning as hpt
import pandas as pd
from datetime import datetime

# Configuration
PROJECT_ID = "mimetic-maxim-443710-s2"
REGION = "europe-west4"  # Tabular Workflows available here
BUCKET = "gs://demand_planning_aca"
DATASET_ID = "redai_demand_forecast_eu"

# Initialize Vertex AI
aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=BUCKET)

print("=" * 60)
print("  Vertex AI Tabular Workflows - Forecasting")
print("=" * 60)

# =============================================================================
# Step 1: Create or get Dataset
# =============================================================================
print("\n1. Setting up training data...")

# BigQuery source for training data (H1)
BQ_SOURCE = f"bq://{PROJECT_ID}.{DATASET_ID}.forecast_sku_weekly_H1"

# Create Vertex AI Dataset
dataset = aiplatform.TimeSeriesDataset.create(
    display_name="forecast_sku_weekly_H1",
    bq_source=BQ_SOURCE,
)
print(f"   Dataset created: {dataset.resource_name}")

# =============================================================================
# Step 2: Configure Training Job
# =============================================================================
print("\n2. Configuring training job...")

# Define column specs
column_specs = {
    "sku": "categorical",           # Series identifier
    "description": "categorical",
    "category": "categorical",
    "year_week": "categorical",
    "period": "categorical",
    "quantity": "numeric",
    "num_orders": "numeric",
    "num_customers": "numeric",
    # "revenue" is target - not included here
}

# Create AutoML Forecasting Training Job
training_job = aiplatform.AutoMLForecastingTrainingJob(
    display_name=f"sku_weekly_forecast_{datetime.now().strftime('%Y%m%d_%H%M')}",
    optimization_objective="minimize-rmse",
    column_specs=column_specs,
)

print(f"   Training job configured")

# =============================================================================
# Step 3: Run Training
# =============================================================================
print("\n3. Starting training...")
print("   This will take 1-3 hours...")

model = training_job.run(
    dataset=dataset,
    target_column="revenue",
    time_column="date",
    time_series_identifier_column="sku",
    unavailable_at_forecast_columns=["quantity", "num_orders", "num_customers"],
    available_at_forecast_columns=[],
    forecast_horizon=26,  # 26 weeks (H2)
    data_granularity_unit="week",
    data_granularity_count=1,
    training_fraction_split=0.8,
    validation_fraction_split=0.1,
    test_fraction_split=0.1,
    budget_milli_node_hours=1000,  # 1 hour budget
    model_display_name="sku_weekly_forecast_model",
    sync=True,  # Wait for completion
)

print(f"\n   ✓ Model trained: {model.resource_name}")

# =============================================================================
# Step 4: Deploy to Endpoint
# =============================================================================
print("\n4. Deploying to endpoint...")

endpoint = model.deploy(
    deployed_model_display_name="sku_weekly_forecast_endpoint",
    machine_type="n1-standard-4",
    min_replica_count=1,
    max_replica_count=1,
    sync=True,
)

print(f"\n   ✓ Endpoint deployed: {endpoint.resource_name}")

# =============================================================================
# Summary
# =============================================================================
print("\n" + "=" * 60)
print("  DEPLOYMENT COMPLETE")
print("=" * 60)
print(f"""
Model:    {model.resource_name}
Endpoint: {endpoint.resource_name}

To make predictions:
    endpoint.predict(instances=[...])

To get endpoint in console:
    https://console.cloud.google.com/vertex-ai/endpoints?project={PROJECT_ID}
""")
