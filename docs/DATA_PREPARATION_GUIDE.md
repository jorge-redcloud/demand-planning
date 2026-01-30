# Data Preparation Guide
## ACA Demand Forecasting Project

**Version:** 2.0
**Last Updated:** January 20, 2026
**Author:** RedAI Data Team

---

## V2 Key Improvements

| Metric | V1 | V2 | Improvement |
|--------|----|----|-------------|
| **Price Coverage** | 27% | **100%** | Fixed column detection |
| **Customers** | 225 | **605** | Better parsing |
| **Transactions** | 431K | **436K** | More files processed |
| **Price History** | None | **33K records** | SKU×Week price tracking |
| **Buying Cycles** | None | **605 customers** | Weekly/Monthly/Irregular |
| **Bulk Buyers** | Not identified | **157 identified** | Classification model |

---

## 1. Source Data Overview

### 1.1 Data Location
```
demand planning/
└── 2025/
    ├── January 2025/      # ZAF_ACA_*.xlsx format
    ├── February 2025/     # ZAF_ACA_*.xlsx format
    ├── March 2025/        # ZAF_ACA_*.xlsx format
    ├── April 2025/        # ZAF_ACA_*.xlsx format
    ├── May 2025/          # ZAF_ACA_*.xlsx format
    ├── June 2025/         # ACA *.xlsx format (different naming!)
    ├── July 2025/         # ACA *.xlsx format
    ├── August 2025/       # ACA *.xlsx format
    ├── September 2025/    # ACA *.xlsx format
    ├── October 2025/      # ACA *.xlsx format
    ├── November 2025/     # ACA *.xlsx format
    └── December 2025/     # ACA *.xlsx format
```

### 1.2 File Naming Conventions

| Period | Pattern | Example |
|--------|---------|---------|
| Jan-May 2025 | `ZAF_ACA_269037_{Month}_{Year}_{Region}_V{n}.xlsx` | `ZAF_ACA_269037_January_2025_CapeTown_V2.xlsx` |
| Jun-Dec 2025 | `ACA {Region} Sales Data {Month} {Year}.xlsx` | `ACA Cape Town Sales Data June 2025.xlsx` |

### 1.3 Regions
| Region Code | Region Name | Notes |
|-------------|-------------|-------|
| CapeTown / Cape Town | Cape Town | Coastal, smaller volume |
| Gauteng | Gauteng | **Largest region (~64% volume)** |
| George | George | Coastal |
| Hardware | Hardware | Specialized products |
| Polokwane | Polokwane | Northern region |

---

## 2. Excel File Structure

### 2.1 Sheet Types

Each Excel file contains multiple sheets:

| Sheet Name | Purpose | Key Columns |
|------------|---------|-------------|
| **Debtors Masterfile** | Customer master data | `ACC NO`, `NAME`, `CONTACT PERSON` |
| **Summary** | Invoice headers with customer link | `Document No.`, `Date`, `Account`, `Debtors Name` |
| **{Invoice Number}** | Line item details (e.g., `4992005031`) | `StockCode`, `Description1`, `Quantity`, `Price`, `Total Incl.` |

### 2.2 Summary Sheet Structure

```
| Column          | Type     | Description                    |
|-----------------|----------|--------------------------------|
| Txan.Type       | string   | Transaction type               |
| Document No.    | float64  | Invoice number (links to sheet)|
| Date            | datetime | Invoice date                   |
| Account         | float64  | Customer ID (→ ACC NO)         |
| Debtors Name    | string   | Customer name at invoice time  |
| Doc.Total (Incl)| float64  | Invoice total with VAT         |
| S/Brch          | string   | Branch code                    |
```

### 2.3 Invoice Sheet Structure (Line Items)

```
| Column       | Type    | Description           |
|--------------|---------|-----------------------|
| StockCode    | float64 | SKU / Product code    |
| Description1 | string  | Product name          |
| Quantity     | float64 | Units sold            |
| Price        | float64 | Unit price            |
| Discount     | float64 | Discount applied      |
| Total Incl.  | int64   | Line total with VAT   |
```

### 2.4 Debtors Masterfile Structure

```
| Column           | Type   | Description              |
|------------------|--------|--------------------------|
| ACC NO           | int64  | Customer account number  |
| NAME             | string | Customer/Company name    |
| CONTACT PERSON   | string | Contact person           |
| TELEPHONE / FAX  | string | Phone number             |
| TELEPHONE / FAX2 | string | Secondary phone          |
```

---

## 3. Data Linkage

### 3.1 How Data Connects

```
┌─────────────────────────────────────────────────────────────────┐
│                        EXCEL FILE                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────┐         ┌──────────────────┐              │
│  │ Debtors Masterfile│         │    Summary       │              │
│  │                  │         │                  │              │
│  │  ACC NO ─────────┼────────►│  Account         │              │
│  │  NAME            │         │  Debtors Name    │              │
│  │  CONTACT PERSON  │         │  Document No. ───┼──────┐       │
│  └──────────────────┘         │  Date            │      │       │
│                               │  Doc.Total       │      │       │
│                               └──────────────────┘      │       │
│                                                         │       │
│                               ┌──────────────────┐      │       │
│                               │ Invoice Sheet    │◄─────┘       │
│                               │ (e.g., 4992005031)              │
│                               │                  │              │
│                               │  StockCode       │              │
│                               │  Quantity        │              │
│                               │  Price           │              │
│                               │  Total Incl.     │              │
│                               └──────────────────┘              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Key Relationships

1. **Invoice → Customer**: `Summary.Document No.` links to sheet name, `Summary.Account` links to customer
2. **Customer Master**: `Summary.Account` = `Debtors Masterfile.ACC NO`
3. **Line Items**: Each invoice sheet contains product-level details

---

## 4. Data Quality Issues

### 4.1 Known Issues by Region

| Region | Issue | Impact | Workaround |
|--------|-------|--------|------------|
| **Gauteng** | No customer IDs in Summary | 66% of data has no customer | Use invoice-level grouping |
| **George** | Account field has text (e.g., 'HB_GOL001') | Summary parsing fails | Skip customer extraction |
| **Cape Town** | Only Jan 2025 has full data | Limited time series | Combine with "Unknown" region |
| **Hardware** | Best customer data quality | ✅ Good | Use as reference |

### 4.2 Customer Data Coverage (Full Year)

```
Total Line Items:     431,444
WITH customer_id:     122,894 (28%)
WITHOUT customer_id:  308,550 (72%)

Coverage by Region:
- Hardware:   ~85% has customer ID
- Cape Town:  ~75% has customer ID
- Polokwane:  ~60% has customer ID
- George:     0% (parsing error - text in Account field)
- Gauteng:    0% (no Account data in source files)
```

### 4.3 Week Completeness (Full Year - 52 Weeks)

| Status | Weeks | Count |
|--------|-------|-------|
| ✅ Complete | W03-W06, W08, W10-W18, W20-W22, W24-W28, W30-W41, W43-W51 | **43 weeks** |
| ⚠️ Partial | W01, W02, W07, W09, W19, W23, W29, W42 | **8 weeks** |
| ❌ Minimal | W52 | **1 week** (holiday) |

**Notable Volume Weeks:**
- W47: 8.7M units (Black Friday)
- W48: 3.5M units (holiday season)
- W44-W46: 1.8-2.3M units (November buildup)

---

## 5. Extraction Pipeline

### 5.1 Pipeline Versions

| Version | Script | Features |
|---------|--------|----------|
| **v0** | `extract_sku_data.py` | Basic SKU extraction, no customers |
| **v1** | `extract_sku_data_v1.py` | + Customer data, + Segmentation, + Completeness flags |

### 5.2 v1 Pipeline Steps

```
1. SCAN SOURCE FILES
   └── Match ZAF_ACA_*.xlsx and ACA*.xlsx patterns

2. FOR EACH FILE:
   ├── Read Summary sheet → Extract invoice-customer mapping
   ├── Read Debtors Masterfile → Enrich customer info
   └── FOR EACH Invoice Sheet:
       └── Extract line items with customer linkage

3. POST-PROCESSING:
   ├── Flag data completeness by week
   ├── Segment customers (Small/Medium/Large/Bulk)
   └── Generate feature tables
```

### 5.3 Running the Extraction

```bash
# Navigate to project folder
cd "demand planning"

# Install dependencies
pip install pandas openpyxl

# Run v2 extraction (recommended)
python3 scripts/extract_sku_data_v2.py

# Or run v1 extraction (legacy)
python3 scripts/extract_sku_data_v1.py
```

---

## 6. Output Files

### 6.0 V2 Output Structure (RECOMMENDED)

```
features_v2/
├── v2_fact_lineitem.csv         # 436,176 rows - All transactions (100% prices)
├── v2_price_history.csv         # 33,308 rows  - SKU × Week price tracking
├── v2_customer_cycles.csv       # 605 rows     - Customer buying patterns
├── v2_features_weekly.csv       # 33,308 rows  - SKU × Week with price lags
├── v2_features_sku_customer.csv # 379,762 rows - SKU × Customer × Week
├── v2_features_category.csv     # 456 rows     - Category × Week
├── v2_dim_customers.csv         # 605 rows     - Customer dimension with buyer types
├── v2_dim_products.csv          # 2,301 rows   - Product dimension with price stats
└── v2_week_completeness.csv     # 55 rows      - Data quality flags
```

**v2_price_history.csv (NEW):**
```
sku, year_week, avg_price, min_price, max_price, price_std,
price_observations, weekly_quantity, weekly_revenue,
prev_avg_price, price_change, price_change_pct
```

**v2_customer_cycles.csv (NEW):**
```
customer_id, customer_name, primary_region,
total_orders, total_units, total_revenue, avg_order_value,
avg_days_between_orders, cycle_regularity, buyer_type,
top_skus, first_order, last_order, active_weeks, customer_segment
```

**Buying Cycle Classifications:**
- Weekly: Orders every ~7 days
- Bi-weekly: Orders every ~14 days
- Monthly: Orders every ~30 days
- Sporadic: Irregular but somewhat predictable
- Irregular: Highly variable ordering
- One-time: Single purchase

**Buyer Type Classifications:**
- Bulk Buyer: >100K units or avg order >10K units
- High-Value Buyer: Avg order >R50K
- Frequent Buyer: 40+ orders
- Regular Buyer: 10-39 orders
- Occasional Buyer: <10 orders

### 6.1 v1 Output Structure

```
features_v1/
├── v1_fact_lineitem.csv         # 431,444 rows - All transactions (full year)
├── v1_features_weekly.csv       # 33,936 rows  - SKU × Week features
├── v1_features_sku_customer.csv # 101,970 rows - SKU × Customer × Week
├── v1_features_category.csv     # 1,866 rows   - Category × Week
├── v1_dim_customers.csv         # 225 rows     - Customer dimension
├── v1_dim_products.csv          # 2,311 rows   - Product dimension
└── v1_week_completeness.csv     # 52 rows      - Data quality flags (full year)
```

### 6.2 Key Fields in Output

**v1_fact_lineitem.csv:**
```
invoice_id, order_date, customer_id, customer_name, region_name,
sku, description, quantity, unit_price, line_total,
year_week, data_completeness, customer_segment
```

**v1_dim_customers.csv:**
```
customer_id, customer_name, primary_region,
total_orders, total_units, total_revenue,
avg_order_units, avg_order_value,
customer_segment, order_frequency
```

---

## 7. Customer Segmentation Rules

### 7.1 By Order Size

```python
def segment_customer(avg_order_units):
    if avg_order_units < 500:
        return 'Small Retailer'
    elif avg_order_units < 5000:
        return 'Medium Retailer'
    elif avg_order_units < 50000:
        return 'Large Retailer'
    else:
        return 'Bulk/Wholesale'
```

### 7.2 Segment Distribution (v1 Results)

| Segment | Customers | Total Units | Total Revenue | Avg Order Size |
|---------|-----------|-------------|---------------|----------------|
| Small Retailer | 49 | 989K | R221M | <500 units |
| Medium Retailer | 167 | 4.4M | R926M | 500-5K units |
| Large Retailer | 2 | 212K | R13.8M | 5K-50K units |
| Unknown (no ID) | - | 13.2M | R2.3B | - |

---

## 8. Data Completeness Flags

### 8.1 Week Completeness Rules

```python
def completeness_flag(total_units, median_high_week):
    if total_units > median_high_week * 0.5:
        return 'complete'
    elif total_units > median_high_week * 0.1:
        return 'partial'
    else:
        return 'minimal'
```

### 8.2 Using Completeness in Models

```sql
-- Only train on complete weeks
SELECT * FROM v1_features_weekly
WHERE data_completeness = 'complete'
```

---

## 9. BigQuery Deployment

### 9.1 Table Naming Convention

```
Project: mimetic-maxim-443710-s2
Dataset: demand_forecasting

Tables:
├── v0 (Original)
│   ├── sku0_fact_lineitem
│   ├── sku0_features_weekly
│   └── cat0_features_weekly
│
└── v1 (With Customers)
    ├── v1_fact_lineitem
    ├── v1_features_weekly
    ├── v1_features_sku_customer  # NEW
    ├── v1_dim_customers          # NEW
    └── v1_features_category
```

### 9.2 Upload Command

```bash
# Upload v1 tables to BigQuery
bq load --source_format=CSV --autodetect \
  demand_forecasting.v1_fact_lineitem \
  features_v1/v1_fact_lineitem.csv
```

---

## 10. Known Limitations & Future Work

### 10.1 Current Limitations

1. **Gauteng customer data missing** - Largest region has no customer IDs
2. **George parsing errors** - Text in Account field breaks extraction
3. **Cape Town limited** - Only 1 month of data
4. **No customer contact info** - Phone/email not extracted

### 10.2 Recommended Improvements

| Priority | Improvement | Effort |
|----------|-------------|--------|
| High | Fix Gauteng source files to include Account | Data provider |
| High | Handle George text Account codes | 1 day |
| Medium | Extract full year (re-run with fixed patterns) | 1 hour |
| Low | Add customer contact info | 2 hours |

---

## 11. Appendix

### A. File Count by Month

| Month | Files | Notes |
|-------|-------|-------|
| January 2025 | 8 | Multiple versions (-V1, -V2, -corrected) |
| February 2025 | 3 | Standard |
| March 2025 | 5 | Standard |
| April 2025 | 5 | Standard |
| May 2025 | 5 | Standard |
| June 2025 | 5 | Different naming pattern |
| July 2025 | 5 | Includes .xlsm |
| August 2025 | 5 | "Hardware Red Cloud" naming |
| September 2025 | 5 | Standard |
| October 2025 | 5 | Standard |
| November 2025 | 5 | Standard |
| December 2025 | 5 | Standard |

### B. SKU Code Format

- Format: Numeric (e.g., `10024`, `11119`)
- Prefix in dimension: `ACP-{sku}` (e.g., `ACP-10024`)
- Range: 10000-12999 observed

### C. Invoice ID Format

- Format: 10-digit numeric (e.g., `4992005031`)
- Pattern: `499{sequence}`
- Unique per region per period

---

## Contact

**Project:** RedAI Demand Forecasting
**Client:** ACA Distribution (South Africa)
**Questions:** Contact RedAI Data Team
