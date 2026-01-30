# RedAI Demand Forecasting - Technical Specification
## South African Distributor Data Pipeline

---

# 1. RAW DATA INVENTORY

## 1.1 Source Files

| File Type | Pattern | Count | Records |
|-----------|---------|-------|---------|
| Regional Sales | `ZAF_ACA_269037_*_{Region}*.xlsx` | 60 files | ~22,693 transactions |
| Product Catalog | `DUB_PROD_Products-2025-02-03-0117.xlsx` | 1 file | 1,350 SKUs |
| Customer Master | `DUB_Customers-2025-02-03-0113.xlsx` | 1 file | 1,407 customers |
| Verification | `_DT_ACA_{Region}_{Month}_2025.xlsx` | 12 files | Line-item detail |

## 1.2 Regional Coverage

| Region Code | Region Name | Branch Code |
|-------------|-------------|-------------|
| ACWCP | Cape Town | ACWCP |
| ACWGT | Gauteng | ACWGT |
| ACWGE | George | ACWGE |
| ACWPK | Polokwane | ACWPK |
| ACWHW | Hardware | ACWHW |

---

# 2. DATA SCHEMA

## 2.1 Sales Transaction Data (Summary Sheet)

```sql
-- Source: ZAF_ACA_269037_*_Summary sheet
CREATE TABLE raw_transactions (
    transaction_type    STRING,       -- 'Account Sales', 'Credit Note'
    document_no         INT64,        -- Invoice number (e.g., 4992005142)
    invoice_date        DATE,         -- Transaction date
    account_no          STRING,       -- Customer account (e.g., '4992005031')
    customer_name       STRING,       -- Debtor name
    total_amount_incl   FLOAT64,      -- Invoice total including VAT
    branch_code         STRING        -- Regional branch (ACWCP, ACWGT, etc.)
);
```

**Sample Data:**
| transaction_type | document_no | invoice_date | account_no | customer_name | total_amount_incl | branch_code |
|-----------------|-------------|--------------|------------|---------------|-------------------|-------------|
| Account Sales | 4992005142 | 2025-01-15 | 4992005031 | SHOPRITE HOLDINGS | 234360.00 | ACWCP |
| Account Sales | 4992005143 | 2025-01-15 | 4992005032 | PICK N PAY | 458590.00 | ACWCP |

## 2.2 Line Item Data (Individual Invoice Sheets)

```sql
-- Source: ZAF_ACA_269037_* individual account sheets
CREATE TABLE raw_line_items (
    document_no         INT64,        -- FK to raw_transactions
    stock_code          STRING,       -- Product SKU (numeric, e.g., '10452')
    description         STRING,       -- Product description
    quantity            INT64,        -- Units sold
    unit_price          FLOAT64,      -- Price per unit
    discount_pct        FLOAT64,      -- Discount percentage
    line_total_incl     FLOAT64       -- Line total including VAT
);
```

## 2.3 Product Master Data

```sql
-- Source: DUB_PROD_Products-2025-02-03-0117.xlsx
-- Total columns: 152 (showing key fields)
CREATE TABLE dim_products (
    -- Identity
    sku                     STRING,       -- 'ACP-10452' format
    name                    STRING,       -- Product name
    product_type            STRING,       -- 'simple', 'configurable'

    -- Classification
    categories              STRING,       -- Category path (e.g., 'Food/Beverages/Soft Drinks')
    category_ids            STRING,       -- Numeric category IDs
    brand                   STRING,       -- Brand name
    manufacturer            STRING,       -- Manufacturer
    fmcg                    BOOLEAN,      -- FMCG flag

    -- Pricing
    price                   FLOAT64,      -- Base price
    special_price           FLOAT64,      -- Promotional price (sparse)
    special_price_from      DATE,         -- Promo start (sparse)
    special_price_to        DATE,         -- Promo end (sparse)
    tax_class_name          STRING,       -- Tax classification

    -- Physical
    weight                  FLOAT64,      -- Product weight
    color                   STRING,       -- Color attribute

    -- Inventory
    qty                     INT64,        -- Current stock quantity
    is_in_stock             BOOLEAN,      -- Stock availability
    manage_stock            BOOLEAN,      -- Inventory management flag
    notify_stock_below      INT64,        -- Reorder threshold

    -- Metadata
    created_at              TIMESTAMP,    -- Product creation date
    updated_at              TIMESTAMP,    -- Last update
    visibility              STRING,       -- 'Catalog, Search'
    seller_id               STRING        -- Seller identifier
);
```

**Column Population Analysis (152 total):**
| Category | Columns | Populated % |
|----------|---------|-------------|
| Core Identity | 12 | 100% |
| Pricing | 8 | 60-100% |
| Inventory | 21 | 100% |
| Physical Attributes | 15 | 30-70% |
| Images | 12 | 80% |
| Sparse/Optional | 84 | <10% |

## 2.4 Customer Master Data

```sql
-- Source: DUB_Customers-2025-02-03-0113.xlsx
-- Total columns: 65 (showing key fields)
CREATE TABLE dim_customers (
    -- Identity
    email                   STRING,       -- Primary key
    firstname               STRING,
    lastname                STRING,
    phone_number            STRING,       -- 70% populated

    -- Classification
    group_id                INT64,        -- Customer group
    customer_group_code     STRING,       -- Group name
    tax_class_name          STRING,       -- Tax classification
    tax_class_id            INT64,

    -- Business
    taxvat                  STRING,       -- VAT number (86% populated)
    kyc_verified            BOOLEAN,      -- KYC status (73% populated)
    category_commission     STRING,       -- Commission category (70% populated)

    -- Location
    website_id              INT64,        -- Website assignment
    store_id                INT64,        -- Store assignment

    -- Metadata
    created_at              TIMESTAMP,
    updated_at              TIMESTAMP
);
```

**Column Population Analysis (65 total):**
| Category | Columns | Populated % |
|----------|---------|-------------|
| Core Identity | 18 | 100% |
| Contact | 4 | 70% |
| Business | 6 | 70-86% |
| Sparse/Optional | 37 | <30% |

---

# 3. DATA QUALITY NOTES

## 3.1 Known Issues

1. **SKU Format Mismatch**
   - Product catalog: `ACP-10452` (prefixed)
   - Sales files: `10452` (numeric only)
   - **Resolution:** Strip 'ACP-' prefix for joins

2. **Customer Key Mismatch**
   - Customer master: keyed by `email`
   - Sales files: keyed by `account_no` (numeric)
   - **Resolution:** Need mapping table or use customer name fuzzy match

3. **Date Formats**
   - Sales files: Excel serial dates
   - Other files: ISO format strings
   - **Resolution:** Standardize to DATE type

4. **Regional File Variations**
   - January: Multiple versions (V1, V2, -corrected)
   - Feb onwards: Single file per region
   - **Resolution:** Prefer `-corrected` or latest version

## 3.2 Data Validation (from DT files)

The `_DT_ACA_*` files contain a verification column `Is Exact?` comparing calculated vs stored totals. **Result: 100% accuracy** across all verified transactions.

---

# 4. FEATURE EXTRACTION PIPELINE

## 4.1 Base Fact Table

```sql
-- Step 1: Create unified transaction fact table
CREATE TABLE fact_transactions AS
SELECT
    t.document_no,
    t.invoice_date,
    t.account_no,
    t.customer_name,
    t.total_amount_incl,
    t.branch_code,
    -- Derived region
    CASE t.branch_code
        WHEN 'ACWCP' THEN 'Cape Town'
        WHEN 'ACWGT' THEN 'Gauteng'
        WHEN 'ACWGE' THEN 'George'
        WHEN 'ACWPK' THEN 'Polokwane'
        WHEN 'ACWHW' THEN 'Hardware'
    END AS region_name,
    -- Time dimensions
    EXTRACT(YEAR FROM t.invoice_date) AS year,
    EXTRACT(MONTH FROM t.invoice_date) AS month,
    EXTRACT(WEEK FROM t.invoice_date) AS week_of_year,
    EXTRACT(DAYOFWEEK FROM t.invoice_date) AS day_of_week,
    EXTRACT(DAY FROM t.invoice_date) AS day_of_month
FROM raw_transactions t
WHERE t.transaction_type = 'Account Sales';
```

## 4.2 Feature Sets

### 4.2.1 Temporal Features

```sql
-- Feature Set: TEMPORAL
CREATE TABLE features_temporal AS
SELECT
    invoice_date,
    region_name,

    -- Aggregations
    COUNT(DISTINCT document_no) AS transaction_count,
    SUM(total_amount_incl) AS daily_revenue,
    COUNT(DISTINCT account_no) AS unique_customers,
    AVG(total_amount_incl) AS avg_transaction_value,

    -- Time features
    EXTRACT(DAYOFWEEK FROM invoice_date) AS dow,
    EXTRACT(DAY FROM invoice_date) AS dom,
    EXTRACT(WEEK FROM invoice_date) AS woy,
    EXTRACT(MONTH FROM invoice_date) AS month,
    EXTRACT(QUARTER FROM invoice_date) AS quarter,

    -- Binary flags
    CASE WHEN EXTRACT(DAYOFWEEK FROM invoice_date) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
    CASE WHEN EXTRACT(DAY FROM invoice_date) <= 7 THEN 1 ELSE 0 END AS is_month_start,
    CASE WHEN EXTRACT(DAY FROM invoice_date) >= 25 THEN 1 ELSE 0 END AS is_month_end

FROM fact_transactions
GROUP BY invoice_date, region_name;
```

### 4.2.2 Lag Features (for time series)

```sql
-- Feature Set: LAG FEATURES
CREATE TABLE features_lag AS
SELECT
    week_start,
    region_name,
    weekly_revenue,
    transaction_count,

    -- Lag features
    LAG(weekly_revenue, 1) OVER w AS revenue_lag_1w,
    LAG(weekly_revenue, 2) OVER w AS revenue_lag_2w,
    LAG(weekly_revenue, 3) OVER w AS revenue_lag_3w,
    LAG(weekly_revenue, 4) OVER w AS revenue_lag_4w,

    -- Rolling aggregations
    AVG(weekly_revenue) OVER (PARTITION BY region_name ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) AS revenue_ma_4w,
    AVG(weekly_revenue) OVER (PARTITION BY region_name ORDER BY week_start ROWS BETWEEN 8 PRECEDING AND 1 PRECEDING) AS revenue_ma_8w,

    -- Trend features
    weekly_revenue - LAG(weekly_revenue, 1) OVER w AS revenue_diff_1w,
    (weekly_revenue - LAG(weekly_revenue, 4) OVER w) / NULLIF(LAG(weekly_revenue, 4) OVER w, 0) AS revenue_pct_change_4w,

    -- Volatility
    STDDEV(weekly_revenue) OVER (PARTITION BY region_name ORDER BY week_start ROWS BETWEEN 4 PRECEDING AND 1 PRECEDING) AS revenue_std_4w

FROM weekly_aggregates
WINDOW w AS (PARTITION BY region_name ORDER BY week_start);
```

### 4.2.3 Customer Features

```sql
-- Feature Set: CUSTOMER
CREATE TABLE features_customer AS
SELECT
    account_no,
    region_name,

    -- Recency
    MAX(invoice_date) AS last_purchase_date,
    DATE_DIFF(CURRENT_DATE(), MAX(invoice_date), DAY) AS days_since_last_purchase,

    -- Frequency
    COUNT(DISTINCT document_no) AS total_transactions,
    COUNT(DISTINCT DATE_TRUNC(invoice_date, MONTH)) AS active_months,
    COUNT(DISTINCT document_no) / COUNT(DISTINCT DATE_TRUNC(invoice_date, MONTH)) AS avg_transactions_per_month,

    -- Monetary
    SUM(total_amount_incl) AS lifetime_value,
    AVG(total_amount_incl) AS avg_order_value,
    MAX(total_amount_incl) AS max_order_value,
    MIN(total_amount_incl) AS min_order_value,
    STDDEV(total_amount_incl) AS order_value_std,

    -- Trend
    SUM(CASE WHEN invoice_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) THEN total_amount_incl ELSE 0 END) AS revenue_last_30d,
    SUM(CASE WHEN invoice_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY) THEN total_amount_incl ELSE 0 END) AS revenue_last_90d

FROM fact_transactions
GROUP BY account_no, region_name;
```

### 4.2.4 Regional Features

```sql
-- Feature Set: REGIONAL
CREATE TABLE features_regional AS
SELECT
    week_start,
    region_name,

    -- Volume metrics
    weekly_revenue,
    transaction_count,
    unique_customers,

    -- Market share (within week)
    weekly_revenue / SUM(weekly_revenue) OVER (PARTITION BY week_start) AS revenue_share,
    transaction_count / SUM(transaction_count) OVER (PARTITION BY week_start) AS transaction_share,

    -- Regional rank
    RANK() OVER (PARTITION BY week_start ORDER BY weekly_revenue DESC) AS revenue_rank,

    -- Growth vs other regions
    weekly_revenue / NULLIF(LAG(weekly_revenue, 1) OVER (PARTITION BY region_name ORDER BY week_start), 0) - 1 AS wow_growth,
    AVG(weekly_revenue / NULLIF(LAG(weekly_revenue, 1) OVER (PARTITION BY region_name ORDER BY week_start), 0) - 1)
        OVER (PARTITION BY week_start) AS avg_regional_growth

FROM weekly_regional_aggregates;
```

---

# 5. MODEL SPECIFICATIONS (BigQuery ML)

## 5.1 Model 1: ARIMA+ (Time Series)

```sql
-- Model: ARIMA+ for univariate time series forecasting
CREATE OR REPLACE MODEL `project.dataset.demand_forecast_arima`
OPTIONS(
    model_type = 'ARIMA_PLUS',
    time_series_timestamp_col = 'week_start',
    time_series_data_col = 'weekly_revenue',
    time_series_id_col = 'region_name',
    auto_arima = TRUE,
    data_frequency = 'WEEKLY',
    holiday_region = 'ZA',  -- South Africa holidays
    clean_spikes_and_dips = TRUE,
    adjust_step_changes = TRUE
) AS
SELECT
    week_start,
    region_name,
    weekly_revenue
FROM weekly_regional_aggregates
WHERE week_start < '2025-12-01';  -- Training cutoff

-- Generate forecasts
SELECT *
FROM ML.FORECAST(
    MODEL `project.dataset.demand_forecast_arima`,
    STRUCT(8 AS horizon, 0.95 AS confidence_level)
);
```

## 5.2 Model 2: XGBoost Regression

```sql
-- Model: XGBoost with engineered features
CREATE OR REPLACE MODEL `project.dataset.demand_forecast_xgboost`
OPTIONS(
    model_type = 'BOOSTED_TREE_REGRESSOR',
    input_label_cols = ['weekly_revenue'],
    max_iterations = 100,
    learn_rate = 0.1,
    max_tree_depth = 6,
    subsample = 0.8,
    colsample_bytree = 0.8,
    early_stop = TRUE,
    min_split_loss = 0.1,
    data_split_method = 'SEQ',
    data_split_eval_fraction = 0.2
) AS
SELECT
    -- Target
    weekly_revenue,

    -- Temporal features
    month,
    week_of_year,
    is_month_start,
    is_month_end,

    -- Lag features
    revenue_lag_1w,
    revenue_lag_2w,
    revenue_lag_4w,
    revenue_ma_4w,
    revenue_diff_1w,
    revenue_pct_change_4w,
    revenue_std_4w,

    -- Regional features
    region_name,
    revenue_share,
    revenue_rank

FROM features_combined
WHERE week_start < '2025-12-01';
```

## 5.3 Model 3: Linear Regression (Baseline)

```sql
-- Model: Simple linear regression baseline
CREATE OR REPLACE MODEL `project.dataset.demand_forecast_linear`
OPTIONS(
    model_type = 'LINEAR_REG',
    input_label_cols = ['weekly_revenue'],
    optimize_strategy = 'NORMAL_EQUATION',
    l2_reg = 0.1
) AS
SELECT
    weekly_revenue,
    revenue_lag_1w,
    revenue_lag_4w,
    revenue_ma_4w,
    month,
    CAST(region_name AS STRING) AS region_name
FROM features_combined
WHERE week_start < '2025-12-01';
```

---

# 6. MODEL EVALUATION

```sql
-- Evaluate all models on holdout period
CREATE TABLE model_evaluation AS
WITH actuals AS (
    SELECT week_start, region_name, weekly_revenue AS actual
    FROM weekly_regional_aggregates
    WHERE week_start >= '2025-12-01'
),
arima_preds AS (
    SELECT
        forecast_timestamp AS week_start,
        time_series_id AS region_name,
        forecast_value AS predicted_arima
    FROM ML.FORECAST(MODEL `project.dataset.demand_forecast_arima`, STRUCT(8 AS horizon))
),
xgboost_preds AS (
    SELECT
        week_start,
        region_name,
        predicted_weekly_revenue AS predicted_xgboost
    FROM ML.PREDICT(MODEL `project.dataset.demand_forecast_xgboost`,
        (SELECT * FROM features_combined WHERE week_start >= '2025-12-01'))
)

SELECT
    a.week_start,
    a.region_name,
    a.actual,
    ar.predicted_arima,
    xg.predicted_xgboost,

    -- Error metrics
    ABS(a.actual - ar.predicted_arima) AS arima_abs_error,
    ABS(a.actual - xg.predicted_xgboost) AS xgboost_abs_error,
    ABS(a.actual - ar.predicted_arima) / NULLIF(a.actual, 0) AS arima_pct_error,
    ABS(a.actual - xg.predicted_xgboost) / NULLIF(a.actual, 0) AS xgboost_pct_error

FROM actuals a
LEFT JOIN arima_preds ar USING (week_start, region_name)
LEFT JOIN xgboost_preds xg USING (week_start, region_name);

-- Aggregate metrics
SELECT
    'ARIMA+' AS model,
    AVG(arima_abs_error) AS mae,
    SQRT(AVG(POW(actual - predicted_arima, 2))) AS rmse,
    AVG(arima_pct_error) * 100 AS mape,
    SUM(arima_abs_error) / SUM(actual) * 100 AS wmape
FROM model_evaluation
UNION ALL
SELECT
    'XGBoost' AS model,
    AVG(xgboost_abs_error) AS mae,
    SQRT(AVG(POW(actual - predicted_xgboost, 2))) AS rmse,
    AVG(xgboost_pct_error) * 100 AS mape,
    SUM(xgboost_abs_error) / SUM(actual) * 100 AS wmape
FROM model_evaluation;
```

---

# 7. OUTPUT SCHEMA

## 7.1 Forecast Output Table

```sql
CREATE TABLE output_forecasts (
    forecast_id             STRING,       -- UUID
    generated_at            TIMESTAMP,    -- When forecast was generated
    model_version           STRING,       -- Model identifier

    -- Dimensions
    region_name             STRING,
    week_start              DATE,

    -- Predictions
    revenue_forecast        FLOAT64,      -- Point estimate
    revenue_lower_95        FLOAT64,      -- 95% CI lower bound
    revenue_upper_95        FLOAT64,      -- 95% CI upper bound

    -- Metadata
    confidence_score        FLOAT64,      -- Model confidence (0-1)
    input_features_hash     STRING        -- Hash of input features for reproducibility
);
```

## 7.2 Model Monitoring Table

```sql
CREATE TABLE output_model_metrics (
    evaluation_date         DATE,
    model_name              STRING,
    region_name             STRING,

    -- Accuracy metrics
    mae                     FLOAT64,
    rmse                    FLOAT64,
    mape                    FLOAT64,
    wmape                   FLOAT64,

    -- Directional accuracy
    direction_accuracy      FLOAT64,      -- % of correct trend predictions

    -- Data quality
    training_rows           INT64,
    missing_values_pct      FLOAT64,

    -- Drift detection
    feature_drift_score     FLOAT64,
    prediction_drift_score  FLOAT64
);
```

---

# 8. IMPLEMENTATION NOTES

## 8.1 Data Pipeline Schedule

| Step | Frequency | Dependencies |
|------|-----------|--------------|
| Raw data ingestion | Daily | Source file availability |
| Feature extraction | Daily | Raw data loaded |
| Model retraining | Weekly | Feature tables updated |
| Forecast generation | Daily | Model available |
| Model evaluation | Weekly | Actuals available |

## 8.2 BigQuery Cost Optimization

1. **Partitioning:** Partition fact tables by `invoice_date`
2. **Clustering:** Cluster by `region_name`, `account_no`
3. **Materialized Views:** For frequently used aggregations
4. **Reservation:** Consider flat-rate for ML training

## 8.3 Next Steps

1. [ ] Create SKU-to-product mapping table
2. [ ] Build customer account-to-email mapping
3. [ ] Add product category features from catalog
4. [ ] Implement seasonal decomposition
5. [ ] Add external features (holidays, events)
