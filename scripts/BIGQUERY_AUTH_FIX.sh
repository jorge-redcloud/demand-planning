#!/bin/bash
# BigQuery Authentication Fix
# ============================
# Run these commands to fix BigQuery access

echo "=============================================="
echo "BIGQUERY AUTHENTICATION FIX"
echo "=============================================="

# Step 1: Check current auth
echo ""
echo "[1] Current authentication:"
gcloud auth list

# Step 2: Check current project
echo ""
echo "[2] Current project:"
gcloud config get-value project

# Step 3: Set the correct project
echo ""
echo "[3] Setting project to mimetic-maxim-443710-s2..."
gcloud config set project mimetic-maxim-443710-s2

# Step 4: Re-authenticate if needed
echo ""
echo "[4] If you see 'Access Denied', run this to re-authenticate:"
echo "    gcloud auth login"
echo ""
echo "    OR for application default credentials:"
echo "    gcloud auth application-default login"

# Step 5: Test access
echo ""
echo "[5] Testing BigQuery access..."
bq ls mimetic-maxim-443710-s2:redai_demand_forecast

echo ""
echo "=============================================="
echo "If you still have issues, check IAM permissions in Google Cloud Console"
echo "=============================================="
