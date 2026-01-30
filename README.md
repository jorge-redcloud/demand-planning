# ACA Demand Planning - Revenue Forecasting

## Project Overview

This project implements a demand forecasting system for ACA (African Cash & Carry) distributors across South Africa. The system predicts weekly revenue at three levels: SKU, Category, and Customer.

### Business Context
- **Client**: RedCloud / ACA Hardware distributors
- **Regions**: Cape Town, Gauteng, Polokwane, George
- **Data Period**: January 2025 - December 2025
- **Forecast Horizon**: H2 (Weeks 27-52) using H1 (Weeks 1-26) for training

### Key Metrics
- **Best Model (SKU level)**: XGBoost with ~51% WMAPE
- **Best Model (Category level)**: Exponential Smoothing with ~36% WMAPE
- **Total SKUs**: 1,366 unique products
- **Total Customers**: 605 unique buyers

---

## File Structure

```
demand_planning/
│
├── 2025/                          # RAW DATA - Excel files by month
│   ├── January 2025/
│   │   ├── ACA Cape Town Sales Data January 2025.xlsx
│   │   ├── ACA Capital Gauteng Sales Data January 2025.xlsx
│   │   ├── ACA Polokwane Sales Data January 2025.xlsx
│   │   └── Missing_Buyers_Information_V1.xlsx
│   ├── February 2025/
│   │   └── ... (same structure per region)
│   └── ... (through December 2025)
│
├── features_v2/                   # PROCESSED DATA - CSV files
│   ├── transactions_clean.csv     # Master transaction table (434K rows)
│   ├── v2_fact_lineitem.csv       # Line-level transactions
│   ├── v2_dim_products.csv        # Product dimension (SKU master)
│   ├── v2_dim_customers.csv       # Customer dimension with segments
│   ├── v2_features_weekly.csv     # SKU × Week aggregations
│   ├── v2_features_category.csv   # Category × Week aggregations
│   ├── v2_features_sku_customer.csv # SKU × Customer × Week
│   ├── v2_customer_cycles.csv     # Customer buying patterns
│   ├── v2_price_history.csv       # SKU × Week price tracking
│   │
│   ├── forecast_sku_weekly.csv        # SKU forecasting dataset (full year)
│   ├── forecast_sku_weekly_H1.csv     # Training set (W01-W26)
│   ├── forecast_sku_weekly_H2.csv     # Test set (W27-W52)
│   ├── forecast_category_weekly*.csv  # Category level datasets
│   └── forecast_customer_weekly*.csv  # Customer level datasets
│
├── model_evaluation/              # MODEL OUTPUTS (Production v4)
│   ├── bigquery_*_predictions_v4.csv  # BigQuery upload files
│   ├── *_predictions_v4.csv           # Final predictions (SKU/Category/Customer)
│   ├── *_h1_actuals_v4.csv            # Training period actuals
│   ├── sku_predictions_*.csv          # Individual model outputs:
│   │   ├── sku_predictions_XGBoost.csv
│   │   ├── sku_predictions_Naive_Last.csv
│   │   ├── sku_predictions_MA_4Week.csv
│   │   ├── sku_predictions_ExpSmooth_03.csv
│   │   └── sku_predictions_Linear_Trend.csv
│   ├── model_comparison.csv           # All models performance comparison
│   ├── sku_pattern_analysis.csv       # SKU classification (sparse/volatile/stable)
│   └── customer_data_sufficiency.csv  # Data density per customer
│
├── scripts/                       # PYTHON SCRIPTS
│   ├── STAGE1_RAW_EVAL.py         # Raw data quality assessment
│   ├── STAGE2_EXTRACTION_EVAL.py  # Data extraction from Excel
│   ├── STAGE2_5_ENRICH.py         # Price inference & enrichment
│   ├── MODEL_EVALUATION.py        # Train & evaluate all baseline models
│   ├── TRAIN_V4_MODELS.py         # Final XGBoost training pipeline
│   ├── GENERATE_DASHBOARD_DATA_ALL_VERSIONS.py  # Dashboard data generator
│   └── SYNC_ALL_VERSIONS_TO_BIGQUERY.py  # BigQuery upload
│
├── notebooks/                     # JUPYTER NOTEBOOKS (for GCP Workbench)
│   ├── shared/
│   │   ├── config.py              # GCP configuration (project, bucket, dataset)
│   │   └── experiment_tracker.py  # Vertex AI experiment tracking
│   ├── prep/                      # Run these first (in order)
│   │   ├── 00_setup_config.ipynb  # Verify GCP/local connection
│   │   ├── 01_data_extraction.ipynb   # Extract from Excel files
│   │   ├── 02_data_prep.ipynb         # Clean data, customer mapping
│   │   └── 03_feature_engineering.ipynb # Create lag features, H1/H2 split
│   ├── production/                # Model training & evaluation
│   │   ├── 04_model_training.ipynb    # Train Naive, MA, ExpSmooth, XGBoost
│   │   └── 05_model_selection.ipynb   # Evaluate on H2, analyze results
│   └── experiments/
│       └── 06_run_experiment.ipynb    # Quick hyperparameter testing
│
├── archive/                       # HISTORICAL ITERATIONS (not for production)
│   ├── old_features/              # Previous feature extraction versions
│   └── old_model_evaluation/      # Previous model versions (v1-v3)
│
├── docs/                          # DOCUMENTATION
│   ├── history/                   # Decision history & roadmaps
│   └── archive/                   # Old presentations & demos
│
├── dashboard_v8.html              # Interactive forecast dashboard
├── dashboard_data_all_versions.js # Dashboard data (V1-V4 models)
├── .gitignore                     # Git ignore rules
├── CLAUDE_GUIDE.md                # Guide for using Claude/Cowork with this project
│
└── README.md                      # This file
```

---

## Data Pipeline

### Stage 1: Raw Data Extraction

**Input**: Excel files from regional sales teams
**Script**: `scripts/STAGE2_EXTRACTION_EVAL.py`

Each Excel file contains:
- Invoice sheets (one per invoice number)
- Summary sheet with monthly totals
- Customer/debtor information

**Key transformations**:
- Extract line items from each invoice sheet
- Parse SKU codes, quantities, unit prices
- Map region from filename
- Handle multiple date formats

### Stage 2: Data Enrichment

**Script**: `scripts/STAGE2_5_ENRICH.py`

- Infer missing prices from same-SKU historical transactions
- Fill missing regions based on filename patterns
- Flag data quality issues
- Create audit trail for price inference

### Stage 3: Customer ID Mapping

**Problem**: Customer IDs were inconsistent across regions:
- Numeric IDs: `820`, `815`, `636`
- String IDs: `HB_FOU002`, `HB_INV013` (Hardware-specific codes)
- Same customer with different IDs across months

**Solution**: Created `master_customer_id` mapping:
1. Extract all unique customer_id + customer_name combinations
2. Deduplicate by customer_name (keep first occurrence)
3. Assign sequential integer IDs
4. Store mapping in `features_v2/v2_dim_customers.csv`

**Assumptions**:
- Same customer_name = same customer (even if different ID)
- Hardware codes (HB_*) are region-specific but map to real customer names
- 605 unique customers after deduplication

### Stage 4: Feature Engineering

**Script**: `scripts/extract_sku_data_v2.py`

Creates weekly aggregations with features:
- `weekly_quantity`: Sum of units sold
- `weekly_revenue`: Sum of line_total (quantity × unit_price)
- `lag1_quantity`, `lag2_quantity`, `lag4_quantity`: Previous weeks' values
- `rolling_avg_4w`: 4-week moving average
- `week_num`: Week of year (1-52)
- `is_w47`: Black Friday indicator

### Stage 5: Model Training

**Script**: `scripts/TRAIN_V4_MODELS.py`

**Models evaluated**:
| Model | Description | Best For |
|-------|-------------|----------|
| Naive_Last | Predict last known value | Baseline |
| MA_4Week | 4-week moving average | Stable SKUs |
| ExpSmooth_03 | Exponential smoothing (α=0.3) | Categories |
| XGBoost | Gradient boosting with lag features | SKUs |

**Model selection logic**:
- If H1 weeks ≥ 4: Train dedicated XGBoost model
- If H1 weeks < 4: Use global/naive fallback model

**Current issue**: ~53% of SKUs use the naive fallback model due to sparse data. These show artificially high accuracy (predicting 0 when actual is 0).

---

## Running the Pipeline

### Prerequisites

```bash
pip install pandas numpy xgboost scikit-learn google-cloud-bigquery google-cloud-storage
```

### 1. Extract & Process Data

```bash
cd "demand planning"

# Stage 1: Evaluate raw data quality
python scripts/STAGE1_RAW_EVAL.py

# Stage 2: Extract from Excel files
python scripts/STAGE2_EXTRACTION_EVAL.py

# Stage 2.5: Enrich data (price inference)
python scripts/STAGE2_5_ENRICH.py
```

### 2. Train Models

```bash
# Train all model versions (V1-V4)
python scripts/TRAIN_V4_MODELS.py

# Or use MODEL_EVALUATION.py for full comparison
python scripts/MODEL_EVALUATION.py
```

### 3. Generate Dashboard Data

```bash
python scripts/GENERATE_DASHBOARD_DATA_ALL_VERSIONS.py
```

### 4. Run Dashboard Locally

```bash
python -m http.server 8000
# Open: http://localhost:8000/dashboard_v8.html
```

---

## Jupyter Notebooks

The notebooks can run in **multiple environments** - choose based on your setup:

| Environment | Best For | Setup |
|-------------|----------|-------|
| **Local Jupyter** | Quick testing, no GCP costs | `pip install jupyterlab` then `jupyter lab` |
| **VS Code** | Development with Git integration | Install Jupyter extension |
| **Vertex AI Workbench** | Production, GCP integration, collaboration | Create instance in GCP Console |
| **Colab** | Quick sharing, free GPU | Upload notebooks to Google Drive |

All notebooks use the same code - just update the paths in `notebooks/shared/config.py`:
- **Local**: `BASE_PATH = "./"`
- **GCP Workbench**: `BASE_PATH = "/home/jupyter/demand_planning/"`
- **Colab**: Mount Google Drive and set path accordingly

### Notebook Execution Order

```
notebooks/
├── prep/                          # Run ONCE to prepare data
│   ├── 00_setup_config.ipynb      # 1. Verify GCP connection
│   ├── 01_data_extraction.ipynb   # 2. Extract from Excel → raw_extraction.csv
│   ├── 02_data_prep.ipynb         # 3. Clean data, customer mapping
│   └── 03_feature_engineering.ipynb # 4. Create features, H1/H2 split
│
├── production/                    # Run to train & evaluate models
│   ├── 04_model_training.ipynb    # 5. Train all models (Naive, MA, XGBoost)
│   └── 05_model_selection.ipynb   # 6. Evaluate on H2, analyze results
│
└── experiments/                   # Use for iterating
    └── 06_run_experiment.ipynb    # Quick hyperparameter testing
```

### Running in GCP Workbench

1. **Create Workbench Instance**:
   - Go to Vertex AI → Workbench → Create Instance
   - Region: `europe-west4` (or any region)
   - Machine: `n1-standard-4` (4 vCPU, 15GB RAM)

2. **Upload Files**:
   ```bash
   # Option A: Upload via JupyterLab UI (drag & drop)

   # Option B: Upload via GCS
   gsutil -m cp -r notebooks/ gs://demand_planning_aca/notebooks/
   gsutil -m cp -r features_v2/ gs://demand_planning_aca/features_v2/

   # Then in JupyterLab terminal:
   gsutil -m cp -r gs://demand_planning_aca/notebooks/ ~/demand_planning/
   gsutil -m cp -r gs://demand_planning_aca/features_v2/ ~/demand_planning/
   ```

3. **Install Dependencies** (first cell in any notebook):
   ```python
   !pip install xgboost scikit-learn google-cloud-bigquery pandas matplotlib
   ```

4. **Run Notebooks in Order**: 00 → 01 → 02 → 03 → 04 → 05

### Notebook Details

| Notebook | Purpose | Output |
|----------|---------|--------|
| 00_setup_config | Verify GCP/local connection | - |
| 01_data_extraction | Extract line items from Excel | `raw_extraction.csv` |
| 02_data_prep | Clean SKUs, map customers, infer prices | `transactions_clean.csv` |
| 03_feature_engineering | Create weekly aggregations, lag features | `forecast_sku_weekly_H1.csv`, `forecast_sku_weekly_H2.csv` |
| 04_model_training | Train Naive, MA, ExpSmooth, XGBoost | `model_comparison.csv`, `xgboost_model.json` |
| 05_model_selection | Evaluate predictions, identify problem areas | `predictions_h2.csv`, `sku_evaluation_metrics.csv` |

---

## GCP Integration

### BigQuery Dataset

- **Project**: `mimetic-maxim-443710-s2`
- **Dataset**: `redai_demand_forecast_eu` (europe-west6)
- **Tables**:
  - `transactions_clean` - Master transaction table
  - `forecast_sku_weekly_H1` - Training data
  - `forecast_sku_weekly_H2` - Test data
  - `predictions_sku_v4` - Model predictions

### Upload to BigQuery

```bash
cd "demand planning"
./upload_forecast_tables.sh
```

### GCS Bucket

- **Bucket**: `gs://demand_planning_aca/`
- **Paths**:
  - `raw_data/` - Original Excel files
  - `processed/` - Feature CSVs
  - `models/` - Saved model artifacts

---

## Dashboard Features (v8)

- **Version selector**: Compare V1, V2, V3, V4 model performance
- **Confidence filters**: High (<40% WMAPE), Medium (<60%), Low (≥60%)
- **Model type filters**:
  - ✓ XGBoost (trained model)
  - ⚠ Naive (fallback model) ← Filter out for demos
- **W47 Black Friday** indicator
- **H1/H2 toggle**: Show training period actuals

---

## Known Issues & Next Steps

### Current Issues

1. **Naive Model Problem**: 53% of SKUs use naive/global fallback model because they have <4 weeks of H1 data. These show misleading accuracy metrics.

2. **WMAPE Calculation**: Current WMAPE includes weeks where both actual=0 and predicted=0, inflating accuracy for sparse SKUs.

3. **No 2024 Data**: Model only trained on 2025 data, missing year-over-year seasonality patterns.

### Recommended Next Steps

1. **Remove Naive Model from Metrics**
   - Only report accuracy for SKUs with dedicated XGBoost models
   - Or implement minimum data threshold (e.g., ≥10 H1 weeks)

2. **Implement Clustering**
   - Group similar SKUs by sales pattern (stable, volatile, seasonal, sparse)
   - Train cluster-level models for sparse SKUs
   - Use category-level predictions as fallback

3. **Add 2024 Historical Data**
   - Enable year-over-year seasonality features
   - Improve Black Friday / holiday predictions
   - Better training for products with long sales history

4. **Seasonality & Promotion Labels**
   - Tag weeks with known promotions
   - Add holiday indicators (Easter, Christmas, etc.)
   - Create promotion lift features

5. **Confidence Thresholds**
   - Implement dynamic thresholds based on data density
   - Weight confidence by revenue importance
   - Add prediction intervals

---

## Contact

- **Project**: RedCloud ACA Demand Planning
- **GCP Project**: mimetic-maxim-443710-s2

