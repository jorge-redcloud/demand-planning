# Using Claude / Claude Cowork with This Project

This guide explains how to use Claude AI (via Claude.ai, Claude Cowork, or Claude Code) to work with this demand planning project.

---

## What is Claude Cowork?

Claude Cowork is an AI assistant mode that can:
- Read and analyze files in your project
- Run Python scripts and Jupyter notebooks
- Execute shell commands
- Create and modify files
- Query BigQuery and interact with GCP services

---

## Getting Started

### 1. Share Your Project Folder

When starting a Cowork session, select the `demand planning` folder as your workspace. This gives Claude access to:
- All data files (features_v2/, model_evaluation/)
- Scripts and notebooks
- Configuration files

### 2. Useful First Prompts

**Understand the project:**
```
Can you summarize the project structure and explain what each folder contains?
```

**Check data status:**
```
What data files are available in features_v2/ and model_evaluation/?
```

**Run the pipeline:**
```
Help me run the full training pipeline using the Jupyter notebooks
```

---

## Common Tasks

### Data Exploration

```
# Ask Claude to analyze the data
"What does the transactions_clean.csv contain? Show me summary statistics."

"How many unique SKUs and customers are in the dataset?"

"Show me the distribution of weekly revenue across categories."
```

### Model Training

```
# Run notebooks in sequence
"Run notebook 04_model_training.ipynb and show me the results"

# Train specific models
"Train an XGBoost model on the SKU data and evaluate on H2"

# Compare models
"Compare the WMAPE scores across all model types (Naive, MA, XGBoost)"
```

### Dashboard & Visualization

```
# Generate dashboard data
"Run GENERATE_DASHBOARD_DATA_ALL_VERSIONS.py and update the dashboard"

# Start local server
"Start a local server so I can view the dashboard"

# Analyze specific SKU
"Show me the forecast chart for SKU 'MONSTER ENERGY 500ML GREEN'"
```

### BigQuery Integration

```
# Upload to BigQuery
"Upload the forecast tables to BigQuery using upload_forecast_tables.sh"

# Query BigQuery
"Query the predictions_sku_v4 table in BigQuery and show me top 10 SKUs by accuracy"
```

---

## Example Workflows

### Workflow 1: Add New Month's Data

When you receive new Excel files for a month:

```
1. "I've added new Excel files to 2025/February 2025/. Can you extract and process them?"

2. Claude will:
   - Run data extraction (notebook 01)
   - Clean and enrich data (notebook 02)
   - Update features (notebook 03)
   - Optionally retrain models (notebook 04)

3. "Update the dashboard with the new predictions"
```

### Workflow 2: Investigate Poor Predictions

When a SKU has bad forecasts:

```
1. "SKU 'ABC123' has terrible predictions. Can you investigate why?"

2. Claude will:
   - Check data density (how many weeks of history)
   - Analyze sales pattern (sparse? volatile?)
   - Check if it's using XGBoost or Naive model
   - Suggest improvements

3. "What would improve predictions for this SKU?"
```

### Workflow 3: Export to Production

When you need to deploy predictions:

```
1. "Export the latest predictions to BigQuery for the API to consume"

2. Claude will:
   - Validate predictions
   - Run upload_forecast_tables.sh
   - Verify data in BigQuery

3. "Generate a summary report of the upload"
```

---

## Tips for Working with Claude

### Be Specific
```
❌ "Analyze the data"
✅ "Show me the top 10 SKUs by revenue in H1 and their prediction accuracy in H2"
```

### Reference Files Directly
```
❌ "Look at the predictions"
✅ "Read model_evaluation/sku_predictions_v4.csv and calculate WMAPE by category"
```

### Ask for Explanations
```
"Explain the model_type column - what's the difference between 'sku' and 'global'?"
"Why do some SKUs show 0 predictions for every week?"
```

### Chain Tasks
```
"First, check if there are any data quality issues in the latest extraction.
Then, if everything looks good, run the model training pipeline.
Finally, update the dashboard and show me the new accuracy metrics."
```

---

## Project-Specific Context

When working with Claude on this project, it helps to know:

### Key Concepts

| Concept | Explanation |
|---------|-------------|
| **H1/H2 Split** | H1 = Weeks 1-26 (training), H2 = Weeks 27-52 (testing) |
| **WMAPE** | Weighted Mean Absolute Percentage Error - lower is better |
| **Naive Model** | Fallback model that repeats last known value (used when <4 weeks of data) |
| **XGBoost Model** | Machine learning model trained per SKU (used when ≥4 weeks of data) |
| **model_type** | 'sku' = dedicated model, 'global' = naive/fallback |

### Important Files

| File | Purpose |
|------|---------|
| `features_v2/transactions_clean.csv` | Master transaction table (start here for data analysis) |
| `features_v2/forecast_sku_weekly_H1.csv` | Training data for models |
| `model_evaluation/sku_predictions_v4.csv` | Latest model predictions |
| `model_evaluation/model_comparison.csv` | Performance metrics for all models |
| `notebooks/shared/config.py` | GCP configuration (project ID, bucket, dataset) |

### Known Limitations

1. **53% of SKUs use Naive model** - They have insufficient H1 data for XGBoost
2. **No 2024 data** - Year-over-year seasonality not captured
3. **Price changes not modeled** - Prices are inferred, not predicted

---

## Troubleshooting

### "File not found" errors
```
"Can you check the current directory and list all available data files?"
```

### BigQuery authentication issues
```
"Check if I'm authenticated to GCP. If not, help me set up credentials."
```

### Notebook kernel issues
```
"Install the required dependencies: pandas, xgboost, scikit-learn, google-cloud-bigquery"
```

### Dashboard not loading
```
"Start a local HTTP server and give me the URL to view the dashboard"
```

---

## Getting Help

If you're stuck, ask Claude:

```
"I'm trying to [describe goal]. What's the best approach given this project structure?"

"What would you recommend as next steps to improve the model accuracy?"

"Can you explain how [specific component] works in this pipeline?"
```

Claude has access to all the code and documentation, so it can provide context-specific guidance.

---

## Version History

- **v4 (current)**: XGBoost with pattern-based model selection
- **v3**: Added customer-level predictions
- **v2**: Improved feature engineering
- **v1**: Basic Naive and MA models

See `docs/history/` for detailed decision logs.
