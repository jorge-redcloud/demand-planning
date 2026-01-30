# RedAI Models Integration Specification

**Project:** ZA Demand Optimization
**Version:** 1.0
**Date:** January 2026
**For:** RedAI v2 Demo App Integration

---

## 1. Overview

This document specifies how to integrate the demand forecasting models into the RedAI app. The goal is to:

1. **Visualize** the model pipeline (Raw → Prep → Features → Models → Predictions)
2. **Consume** models programmatically via BigQuery API
3. **Display** in the Models and Projects sections of the app

---

## 2. Project Structure: ZA Demand Optimization

### 2.1 Pipeline Stages

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     ZA DEMAND OPTIMIZATION PIPELINE                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌────────┐│
│  │  1.RAW   │───▶│  2.PREP  │───▶│ 3.DIMS   │───▶│4.FEATURES│───▶│5.MODELS││
│  │  DATA    │    │          │    │          │    │          │    │        ││
│  └──────────┘    └──────────┘    └──────────┘    └──────────┘    └────────┘│
│       │               │               │               │              │      │
│  Excel files    Clean CSVs      Dimension      Feature         XGBoost     │
│  12 months      436K rows       tables         engineering     3 models    │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Stage Details

| Stage | Name | Description | Status | Outputs |
|-------|------|-------------|--------|---------|
| 1 | **Raw Data** | Monthly Excel files from ACA | ✅ Complete | 12 Excel files |
| 2 | **Data Prep** | Clean, validate, standardize | ✅ Complete | `fact_lineitem.csv` (436K rows) |
| 3 | **Dimensions** | Master data tables | ✅ Complete | `dim_products`, `dim_customers` |
| 4 | **Features** | Lag features, rolling averages | ✅ Complete | `features_*_weekly.csv` |
| 5 | **Models** | XGBoost trained models | ✅ Complete | 3 BigQuery ML models |

---

## 3. BigQuery Resources

### 3.1 Project Configuration

```json
{
  "project_id": "mimetic-maxim-443710-s2",
  "dataset": "redai_demand_forecast",
  "location": "US"
}
```

### 3.2 Tables

| Table | Description | Rows | Key Columns |
|-------|-------------|------|-------------|
| `fact_lineitem` | Transaction data | 436,176 | invoice_id, sku, customer_id, quantity, year_week |
| `dim_products` | Product master | 1,575 | sku, description, category_l1, category_l2 |
| `dim_customers` | Customer master | 1,142 | customer_id, customer_name, segment |
| `features_sku_weekly` | SKU features | ~33K | sku, year_week, lag1, lag2, lag4, rolling_avg_4w |
| `features_category_weekly` | Category features | ~500 | category, year_week, lag1, lag2, lag4 |
| `features_customer_weekly` | Customer features | ~19K | customer_id, year_week, lag1, lag2, lag4 |
| `predictions_sku` | SKU predictions | ~1.1K | sku, year_week, actual, predicted, pct_error |
| `predictions_category` | Category predictions | ~234 | category, year_week, actual, predicted |
| `predictions_customer` | Customer predictions | ~962 | customer_id, year_week, actual, predicted |
| `eval_summary` | Model performance | 3 | level, model, median_mape, mae, rmse |
| `mape_by_sku` | MAPE per SKU | ~50 | sku, median_mape |
| `mape_by_category` | MAPE per category | 9 | category, median_mape |
| `mape_by_customer` | MAPE per customer | ~30 | customer_id, median_mape |

### 3.3 Models

| Model | Type | Level | Best MAPE | Features |
|-------|------|-------|-----------|----------|
| `xgb_sku_model` | BOOSTED_TREE_REGRESSOR | SKU | 15.0% | lag1, lag2, lag4, rolling_avg_4w, week_num |
| `xgb_category_model` | BOOSTED_TREE_REGRESSOR | Category | 29.0% | lag1, lag2, lag4, rolling_avg_4w, week_num |
| `xgb_customer_model` | BOOSTED_TREE_REGRESSOR | Customer | 49.0% | lag1, lag2, lag4, rolling_avg_4w, week_num |

---

## 4. API Integration

### 4.1 Authentication

```typescript
// Use Google Cloud Service Account
import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery({
  projectId: 'mimetic-maxim-443710-s2',
  keyFilename: '/path/to/service-account.json'
});
```

### 4.2 Query Functions

#### Get Model Performance Summary
```typescript
async function getModelSummary(): Promise<ModelSummary[]> {
  const query = `
    SELECT level, model, n_predictions, mae, median_mape, mean_mape, rmse
    FROM \`mimetic-maxim-443710-s2.redai_demand_forecast.eval_summary\`
  `;
  const [rows] = await bigquery.query(query);
  return rows;
}
```

#### Get SKU Predictions
```typescript
async function getSkuPredictions(sku: string): Promise<Prediction[]> {
  const query = `
    SELECT year_week, actual, predicted, pct_error
    FROM \`mimetic-maxim-443710-s2.redai_demand_forecast.predictions_sku\`
    WHERE sku = @sku
    ORDER BY year_week
  `;
  const [rows] = await bigquery.query({
    query,
    params: { sku }
  });
  return rows;
}
```

#### Get Category Predictions
```typescript
async function getCategoryPredictions(category: string): Promise<Prediction[]> {
  const query = `
    SELECT year_week, actual, predicted, pct_error
    FROM \`mimetic-maxim-443710-s2.redai_demand_forecast.predictions_category\`
    WHERE category = @category
    ORDER BY year_week
  `;
  const [rows] = await bigquery.query({
    query,
    params: { category }
  });
  return rows;
}
```

#### Make New Prediction (Real-time)
```typescript
async function predictDemand(
  level: 'sku' | 'category' | 'customer',
  params: PredictionParams
): Promise<number> {
  const model = `xgb_${level}_model`;
  const query = `
    SELECT predicted_quantity
    FROM ML.PREDICT(
      MODEL \`mimetic-maxim-443710-s2.redai_demand_forecast.${model}\`,
      (SELECT
        @lag1 as lag1,
        @lag2 as lag2,
        @lag4 as lag4,
        @rolling_avg_4w as rolling_avg_4w,
        @week_num as week_num
      )
    )
  `;
  const [rows] = await bigquery.query({
    query,
    params
  });
  return rows[0].predicted_quantity;
}
```

#### Get Best/Worst Performing Entities
```typescript
async function getPerformanceRanking(
  level: 'sku' | 'category' | 'customer',
  limit: number = 10,
  order: 'best' | 'worst' = 'best'
): Promise<RankingItem[]> {
  const table = `mape_by_${level}`;
  const direction = order === 'best' ? 'ASC' : 'DESC';
  const query = `
    SELECT *
    FROM \`mimetic-maxim-443710-s2.redai_demand_forecast.${table}\`
    ORDER BY median_mape ${direction}
    LIMIT @limit
  `;
  const [rows] = await bigquery.query({
    query,
    params: { limit }
  });
  return rows;
}
```

---

## 5. Data Types

### 5.1 TypeScript Interfaces

```typescript
// Model Summary
interface ModelSummary {
  level: 'SKU' | 'Category' | 'Customer';
  model: string;
  n_predictions: number;
  mae: number;
  median_mape: number;
  mean_mape: number;
  rmse: number;
}

// Prediction Result
interface Prediction {
  year_week: string;
  actual: number;
  predicted: number;
  pct_error: number;
}

// Prediction Parameters
interface PredictionParams {
  lag1: number;      // Last week's quantity
  lag2: number;      // 2 weeks ago quantity
  lag4: number;      // 4 weeks ago quantity
  rolling_avg_4w: number;  // 4-week rolling average
  week_num: number;  // Week of year (1-52)
}

// Ranking Item
interface RankingItem {
  entity_id: string;
  entity_name?: string;
  n_weeks: number;
  median_mape: number;
  mean_mape: number;
}

// Pipeline Stage
interface PipelineStage {
  id: number;
  name: string;
  status: 'pending' | 'running' | 'completed' | 'failed';
  description: string;
  outputs: string[];
  metrics?: Record<string, number>;
}

// Project
interface Project {
  id: string;
  name: string;
  description: string;
  stages: PipelineStage[];
  models: Model[];
  createdAt: Date;
  updatedAt: Date;
}

// Model
interface Model {
  id: string;
  name: string;
  type: 'BOOSTED_TREE_REGRESSOR' | 'ARIMA_PLUS' | 'LINEAR_REG';
  level: 'sku' | 'category' | 'customer';
  features: string[];
  metrics: {
    mape: number;
    mae: number;
    rmse: number;
  };
  bigqueryModel: string;  // Full path to BQ model
  status: 'training' | 'deployed' | 'deprecated';
}
```

---

## 6. UI Components Specification

### 6.1 Projects View

```
┌─────────────────────────────────────────────────────────────────┐
│ Projects                                                    [+] │
├─────────────────────────────────────────────────────────────────┤
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 🇿🇦 ZA Demand Optimization                                  │ │
│ │                                                             │ │
│ │ Demand forecasting for ACA Distribution (South Africa)     │ │
│ │                                                             │ │
│ │ Stages: ████████████████████░░░░ 80%                       │ │
│ │                                                             │ │
│ │ [Raw Data] → [Prep] → [Dims] → [Features] → [Models]       │ │
│ │     ✅         ✅       ✅        ✅           ✅            │ │
│ │                                                             │ │
│ │ Models: 3 deployed │ Last updated: Jan 21, 2026            │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ 🇰🇪 KE Inventory Optimization                    [Coming]   │ │
│ └─────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

### 6.2 Project Detail View (Stages)

```
┌─────────────────────────────────────────────────────────────────┐
│ ← ZA Demand Optimization                                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Pipeline Stages                                                │
│  ───────────────                                                │
│                                                                 │
│  ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐   ┌────────┐│
│  │1. RAW  │──▶│2. PREP │──▶│3. DIMS │──▶│4. FEAT │──▶│5.MODEL ││
│  │   ✅   │   │   ✅   │   │   ✅   │   │   ✅   │   │   ✅   ││
│  └────────┘   └────────┘   └────────┘   └────────┘   └────────┘│
│                                                                 │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ Stage: Features                                             ││
│  │ ─────────────────────────────────────────────────────────── ││
│  │ Status: ✅ Complete                                         ││
│  │ Last run: Jan 21, 2026 14:54 UTC                           ││
│  │                                                             ││
│  │ Outputs:                                                    ││
│  │   • features_sku_weekly (33,308 rows)                      ││
│  │   • features_category_weekly (456 rows)                    ││
│  │   • features_customer_weekly (19,274 rows)                 ││
│  │                                                             ││
│  │ Features created:                                           ││
│  │   lag1, lag2, lag4, rolling_avg_4w, week_num               ││
│  │                                                             ││
│  │ [View Data] [Re-run Stage]                                 ││
│  └─────────────────────────────────────────────────────────────┘│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.3 Models View

```
┌─────────────────────────────────────────────────────────────────┐
│ Models                                                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ xgb_sku_model                                    🟢 Deployed│ │
│ │ ─────────────────────────────────────────────────────────── │ │
│ │ Type: XGBoost (BOOSTED_TREE_REGRESSOR)                      │ │
│ │ Level: SKU                                                   │ │
│ │ Best MAPE: 15.0%  │  MAE: 1,278  │  RMSE: 2,156            │ │
│ │                                                             │ │
│ │ Features: lag1, lag2, lag4, rolling_avg_4w, week_num        │ │
│ │                                                             │ │
│ │ [Test Model] [View Predictions] [API Docs]                  │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ xgb_category_model                               🟢 Deployed│ │
│ │ ─────────────────────────────────────────────────────────── │ │
│ │ Type: XGBoost (BOOSTED_TREE_REGRESSOR)                      │ │
│ │ Level: Category                                              │ │
│ │ Best MAPE: 29.0%  │  MAE: 45,231  │  RMSE: 62,847          │ │
│ │                                                             │ │
│ │ [Test Model] [View Predictions] [API Docs]                  │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ xgb_customer_model                               🟢 Deployed│ │
│ │ ─────────────────────────────────────────────────────────── │ │
│ │ Type: XGBoost (BOOSTED_TREE_REGRESSOR)                      │ │
│ │ Level: Customer                                              │ │
│ │ Best MAPE: 49.0%  │  MAE: 8,456  │  RMSE: 12,234           │ │
│ │                                                             │ │
│ │ [Test Model] [View Predictions] [API Docs]                  │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 6.4 Model Test Interface

```
┌─────────────────────────────────────────────────────────────────┐
│ Test Model: xgb_sku_model                                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│ Input Features                        Prediction                │
│ ──────────────                        ──────────                │
│                                                                 │
│ lag1 (last week):      [1000    ]     ┌─────────────────────┐  │
│ lag2 (2 weeks ago):    [950     ]     │                     │  │
│ lag4 (4 weeks ago):    [900     ]     │   Predicted: 1,045  │  │
│ rolling_avg_4w:        [962     ]     │   units/week        │  │
│ week_num:              [5       ]     │                     │  │
│                                        │   Confidence: ±15%  │  │
│ [Predict]                              │                     │  │
│                                        └─────────────────────┘  │
│                                                                 │
│ Or select existing entity:                                      │
│ ┌─────────────────────────────────────────────────────────────┐ │
│ │ SKU: [12130 - BB CEMENT 32.5R              ▼]              │ │
│ │                                                             │ │
│ │ Historical: ████████████████████ 1,200 avg/week            │ │
│ │ Predicted:  ████████████████████ 1,150 next week           │ │
│ └─────────────────────────────────────────────────────────────┘ │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 7. Chat Integration

### 7.1 Model-Aware Queries

The RedAI chat can use these models to answer questions:

| User Query | Model Used | Response Type |
|------------|------------|---------------|
| "What's the forecast for SKU 12130?" | `xgb_sku_model` | Prediction + confidence |
| "Which categories are hardest to predict?" | `mape_by_category` | Ranking table |
| "Predict demand for next week" | `xgb_*_model` | Multiple predictions |
| "How accurate are our models?" | `eval_summary` | Performance metrics |
| "Which customers have volatile demand?" | `mape_by_customer` | High MAPE customers |

### 7.2 Example Chat Flow

```
User: What's the demand forecast for cement next week?

RedAI: Based on the XGBoost model for SKU 12130 (BB CEMENT 32.5R):

📊 **Forecast for Week 5:**
- Predicted demand: **1,045 units**
- Model accuracy (MAPE): 15.0%
- Confidence range: 889 - 1,202 units

📈 **Recent trend:**
- Week 1: 980 units
- Week 2: 1,020 units
- Week 3: 1,050 units
- Week 4: 1,000 units

This SKU is one of our best-predicted items with consistently
stable demand patterns.
```

---

## 8. Implementation Checklist

### 8.1 Backend (API)

- [ ] Add BigQuery client configuration
- [ ] Create `/api/models` endpoint (list all models)
- [ ] Create `/api/models/:id/predict` endpoint
- [ ] Create `/api/projects` endpoint (list projects)
- [ ] Create `/api/projects/:id/stages` endpoint
- [ ] Add caching for predictions (Redis/in-memory)
- [ ] Add authentication for BigQuery access

### 8.2 Frontend

- [ ] Create `Models` page component
- [ ] Create `Projects` page component
- [ ] Create `ProjectDetail` component with stages
- [ ] Create `ModelCard` component
- [ ] Create `ModelTest` component (prediction interface)
- [ ] Create `PipelineVisualization` component
- [ ] Add model queries to chat context

### 8.3 Data

- [ ] Verify BigQuery tables are accessible
- [ ] Set up service account with BigQuery permissions
- [ ] Create seed data for project metadata
- [ ] Add cron job for daily prediction refresh

---

## 9. Environment Variables

```env
# BigQuery Configuration
GOOGLE_CLOUD_PROJECT=mimetic-maxim-443710-s2
BIGQUERY_DATASET=redai_demand_forecast
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json

# Model Endpoints
BQ_SKU_MODEL=mimetic-maxim-443710-s2.redai_demand_forecast.xgb_sku_model
BQ_CATEGORY_MODEL=mimetic-maxim-443710-s2.redai_demand_forecast.xgb_category_model
BQ_CUSTOMER_MODEL=mimetic-maxim-443710-s2.redai_demand_forecast.xgb_customer_model
```

---

## 10. Quick Start

### 10.1 Test BigQuery Connection

```bash
# Install BigQuery client
npm install @google-cloud/bigquery

# Test query
bq query --project_id=mimetic-maxim-443710-s2 \
  --use_legacy_sql=false \
  "SELECT * FROM redai_demand_forecast.eval_summary"
```

### 10.2 Minimal API Implementation

```typescript
// pages/api/models/index.ts
import { BigQuery } from '@google-cloud/bigquery';

const bigquery = new BigQuery();

export default async function handler(req, res) {
  const query = `
    SELECT * FROM \`mimetic-maxim-443710-s2.redai_demand_forecast.eval_summary\`
  `;
  const [rows] = await bigquery.query(query);
  res.json({ models: rows });
}
```

### 10.3 Test Prediction

```bash
curl -X POST http://localhost:3001/api/models/sku/predict \
  -H "Content-Type: application/json" \
  -d '{"lag1": 1000, "lag2": 950, "lag4": 900, "rolling_avg_4w": 962, "week_num": 5}'
```

---

## 11. Related Documents

| Document | Location | Description |
|----------|----------|-------------|
| Model Inventory | `docs/MODEL_INVENTORY.md` | All 15 models evaluated |
| Technical Spec | `docs/MODEL_EVALUATION_TECHNICAL_SPEC.md` | Full technical details |
| BigQuery SQL | `scripts/bigquery_xgboost_all_levels.sql` | Model training SQL |
| Upload Script | `scripts/upload_to_bigquery.sh` | Deployment automation |
