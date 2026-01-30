# Gap Analysis & Model Roadmap
## ACA Demand Forecasting - Path to Production

**Date:** January 20, 2026
**Status:** v0 Complete → v1 Planning

---

## 🔍 Key Findings

### 1. Why Some Weeks Have Almost No Sales

| Week | Units | Diagnosis |
|------|-------|-----------|
| W01 | 3.66M | ✅ Full data |
| W02 | 50K | ⚠️ Partial - likely only some regions uploaded |
| W03 | 53K | ⚠️ Partial |
| W04 | 49K | ⚠️ Partial |
| W05 | 2.54M | ✅ Full data |
| W06-W08 | 0 | ❌ **MISSING** - No source files |
| W09 | 4.11M | ✅ Full data |
| W10-W13 | 0 | ❌ **MISSING** - No source files |
| W14 | 4.15M | ✅ Full data |
| W15-W17 | 70-183K | ⚠️ Partial |
| W18 | 3.87M | ✅ Full data |

**Root Cause:** The source Excel files are uploaded monthly/periodically, not weekly. The "low weeks" (W02-W04, W15-W17) appear to have only partial regional data.

**Impact on Models:** ARIMA sees this as volatility, not missing data. It's learning the wrong pattern.

---

### 2. Customer Data EXISTS But Wasn't Extracted

**Source Files Have:**
| Field | Location | Example |
|-------|----------|---------|
| `Account` (Customer ID) | Summary sheet | 300, 301, 302 |
| `Debtors Name` | Summary sheet | "VICTORY EDUCATION GROUP" |
| `ACC NO` | Debtors Masterfile | Links to Account |
| `CUSTOMER NAME` | _DT_ files | Already joined |

**Current Extraction:** ❌ Not extracting customer data from Summary sheet

**Fix Required:** Re-run extraction to join Summary → Invoice sheets to get customer ID per line item

---

### 3. Current Model Limitations (v0)

| Limitation | Impact | Fix |
|------------|--------|-----|
| No customer segmentation | Can't separate bulk vs retail | Extract customer data |
| Missing weeks treated as low demand | Wrong forecasts | Flag/impute missing data |
| No buyer-level forecasting | Can't predict by customer | Add customer dimension |
| All data weighted equally | Outliers skew results | Add data quality weights |

---

## 📊 Model Versioning Strategy

### v0 (Current) - Baseline SKU/Category Models
```
Status: ✅ DEPLOYED
Scope: SKU × Week, Category × Week
Data: 11 weeks (with gaps)
Features: Price, lags, seasonality
Limitation: No customer dimension, treats gaps as real lows
```

### v1 (Next) - Customer-Aware Models
```
Status: 🔨 TO BUILD
Scope: SKU × Customer × Week
New Features:
  - customer_id (from Summary sheet)
  - customer_type (bulk vs retail classification)
  - customer_historical_avg
  - order_frequency
Fixes:
  - Flag incomplete weeks
  - Separate bulk orders from regular
```

### v2 (Future) - Full Production Models
```
Status: 📋 PLANNED
Scope: SKU × Customer × Region × Week
New Features:
  - Customer segmentation (RFM)
  - Price elasticity by customer type
  - Promotion response curves
  - Seasonality by region
  - Lead time / reorder points
```

---

## 🛠️ What Needs to Change

### Extraction Pipeline Changes

```python
# CURRENT (v0): Only extracts from invoice sheets
for sheet in invoice_sheets:
    extract_line_items(sheet)  # No customer info

# NEEDED (v1): Join Summary → Invoice to get customer
summary_df = read_summary_sheet()  # Has Account, Debtors Name
for sheet in invoice_sheets:
    line_items = extract_line_items(sheet)
    line_items = join_customer_from_summary(line_items, summary_df)
```

### New Tables for v1

| Table | Granularity | Purpose |
|-------|-------------|---------|
| `v1_dim_customers` | Customer | Customer master with type labels |
| `v1_fact_lineitem` | Transaction | Line items WITH customer_id |
| `v1_features_sku_customer` | SKU × Customer × Week | Customer-level SKU features |
| `v1_features_customer` | Customer × Week | Customer-level aggregates |

### New Model Suite for v1

| Model | Type | Purpose |
|-------|------|---------|
| `v1_model_sku_arima` | ARIMA+ | SKU forecast (clean data only) |
| `v1_model_sku_xgboost` | XGBoost | SKU demand with customer features |
| `v1_model_customer_ltv` | XGBoost | Customer lifetime value |
| `v1_model_bulk_detector` | Classification | Identify bulk vs retail orders |

---

## 📈 Proposed Buyer Segmentation

### By Order Size (Simple)
```sql
CASE
  WHEN avg_order_units < 500 THEN 'Small Retailer'
  WHEN avg_order_units < 5000 THEN 'Medium Retailer'
  WHEN avg_order_units < 50000 THEN 'Large Retailer'
  ELSE 'Bulk/Wholesale'
END as customer_segment
```

### By Behavior (Advanced - v2)
- **Frequency**: How often do they order?
- **Recency**: When was last order?
- **Monetary**: Total spend
- **Consistency**: Variance in order size

---

## 🗓️ Implementation Roadmap

### Phase 1: Data Fix (1-2 days)
- [ ] Update extraction script to pull customer data from Summary sheet
- [ ] Re-extract all 12 months with customer_id
- [ ] Create customer dimension table
- [ ] Flag incomplete weeks in the data

### Phase 2: Customer Labeling (1 day)
- [ ] Calculate order size distribution per customer
- [ ] Apply segmentation rules (bulk vs retail)
- [ ] Create `v1_dim_customers` table

### Phase 3: v1 Features (1-2 days)
- [ ] Build customer-level features
- [ ] Create SKU × Customer weekly aggregates
- [ ] Add data completeness flags

### Phase 4: v1 Models (1-2 days)
- [ ] Train ARIMA on complete weeks only
- [ ] Train XGBoost with customer features
- [ ] Build bulk order detection model
- [ ] Deploy to BigQuery

### Phase 5: Validation & Dashboard (1 day)
- [ ] Compare v0 vs v1 forecast accuracy
- [ ] Update dashboard with customer view
- [ ] Document model differences

---

## 🎯 What You'll Be Able to Do with v1

### 1. Forecast by Customer Type
```sql
-- "What's the demand for Huggies from Bulk buyers vs Retailers?"
SELECT
  customer_segment,
  sku,
  SUM(predicted_units) as forecast
FROM ML.PREDICT(MODEL v1_model_sku_xgboost, ...)
GROUP BY customer_segment, sku
```

### 2. What-If by Segment
```sql
-- "If we cut price 10% for Large Retailers only, what's the lift?"
```

### 3. Customer-Level Forecasting
```sql
-- "What will Customer ABC order next month?"
SELECT * FROM ML.FORECAST(MODEL v1_model_customer_arima, ...)
WHERE customer_id = 'ABC'
```

### 4. Identify Bulk Order Patterns
```sql
-- "Which customers are likely to place bulk orders this week?"
SELECT * FROM ML.PREDICT(MODEL v1_model_bulk_detector, ...)
WHERE bulk_probability > 0.8
```

---

## 📋 Summary

| Aspect | v0 (Current) | v1 (Next) | v2 (Future) |
|--------|--------------|-----------|-------------|
| Customer Data | ❌ None | ✅ ID + Segment | ✅ Full RFM |
| Data Gaps | ⚠️ Treated as real | ✅ Flagged | ✅ Imputed |
| Granularity | SKU × Week | SKU × Customer × Week | SKU × Customer × Region × Week |
| Bulk Detection | ❌ No | ✅ Yes | ✅ + Prediction |
| Forecast Accuracy | Baseline | +10-20% expected | +25-40% expected |

---

## Next Step

**Do you want me to:**
1. Start building the v1 extraction pipeline to add customer data?
2. Run the deep analysis script first to see full week × region breakdown?
3. Create a visual dashboard showing the data gaps?

