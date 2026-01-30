#!/bin/bash
# BigQuery Setup Script
# Run this on your Mac to create the dataset and tables

PROJECT_ID="mimetic-maxim-443710-s2"
DATASET="aca_demand_planning"
LOCATION="US"

echo "=============================================="
echo "BIGQUERY SETUP"
echo "=============================================="
echo "Project: $PROJECT_ID"
echo "Dataset: $DATASET"
echo ""

# Step 1: Create dataset
echo "[1/4] Creating dataset..."
bq --project_id=$PROJECT_ID mk --dataset --location=$LOCATION $DATASET

# Step 2: Create tables schema
echo "[2/4] Creating tables..."

# SKU Predictions table
bq --project_id=$PROJECT_ID mk --table $DATASET.sku_predictions \
    sku:INTEGER,year_week:STRING,actual:FLOAT,predicted:FLOAT,wmape:FLOAT,confidence:STRING,category:STRING

# Category Predictions table
bq --project_id=$PROJECT_ID mk --table $DATASET.category_predictions \
    category:STRING,year_week:STRING,actual:FLOAT,predicted:FLOAT,wmape:FLOAT

# Customer Predictions table
bq --project_id=$PROJECT_ID mk --table $DATASET.customer_predictions \
    customer_id:STRING,customer_name:STRING,year_week:STRING,actual:FLOAT,predicted:FLOAT,wmape:FLOAT

# Model metadata table
bq --project_id=$PROJECT_ID mk --table $DATASET.model_metadata \
    model_version:STRING,model_type:STRING,trained_at:TIMESTAMP,wmape:FLOAT,features:STRING,notes:STRING

echo "[3/4] Verifying setup..."
bq --project_id=$PROJECT_ID ls $DATASET

echo ""
echo "[4/4] Dataset ready!"
echo ""
echo "Next steps:"
echo "  1. Run TRAIN_V4_MODELS.py to generate predictions"
echo "  2. Upload CSVs with: bq load --source_format=CSV --skip_leading_rows=1 $DATASET.table_name file.csv"
echo ""
