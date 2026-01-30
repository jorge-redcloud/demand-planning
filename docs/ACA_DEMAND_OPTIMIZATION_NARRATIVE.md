# From Reactive to Proactive: ACA's Transformation with Intelligent Demand Planning

**RedAI × ACA Case Study**
**January 2026**

---

## The Challenge: A Distributor of Distributors in a Volatile Market

ACA operates as a **distributor of distributors** in the South African FMCG (Fast-Moving Consumer Goods) market, serving over 1,142 customers through 1,575+ SKUs across 9 product categories.

Before implementing intelligent demand planning, ACA faced the classic challenges of a reactive operation:

### The Reactive Cycle

```
         ┌─────────────────────────────────────────────────────┐
         │                  REACTIVE MODE                       │
         │                                                      │
         │   Unpredictable →    Frequent    →    Lost          │
         │   Demand             Stockouts        Sales          │
         │                                                      │
         │   Over-ordering →    Inventory   →   Trapped        │
         │   from Fear          Returns          Capital        │
         │                                                      │
         │   No Market    →     Late        →   Eroded         │
         │   Visibility         Decisions       Margins         │
         └─────────────────────────────────────────────────────┘
```

**Operational Impact:**
- **Stockouts** on high-rotation products during demand peaks
- **Inventory returns** from over-stocked products
- **Inefficient working capital** tied to incorrect inventory
- **Missed opportunities** for cross-selling and upselling
- **Purchasing planning** based on intuition, not data

---

## The Solution: AI-Powered Demand Planning

RedAI worked in **co-creation with ACA** to develop a forecasting system that transforms transactional data into actionable intelligence.

### Solution Architecture

```
┌────────────────────────────────────────────────────────────────────┐
│                                                                    │
│   DATA              →      AI           →     ACTIONS              │
│   TRANSACTIONAL          PREDICTIVE          COMMERCIAL            │
│                                                                    │
│   ┌──────────────┐    ┌──────────────┐    ┌──────────────────┐    │
│   │ 436K lines   │    │ XGBoost      │    │ Anticipate       │    │
│   │ historical   │───▶│ Forecasting  │───▶│ purchases        │    │
│   │              │    │              │    │                  │    │
│   │ 52 weeks of  │    │ SKU-level    │    │ Optimize         │    │
│   │ history      │    │ Category     │    │ inventory        │    │
│   │              │    │ Customer     │    │                  │    │
│   │ Purchase     │    │              │    │ Intelligent      │    │
│   │ patterns     │    │ MAPE 15-49%  │    │ upsell           │    │
│   └──────────────┘    └──────────────┘    └──────────────────┘    │
│                                                                    │
└────────────────────────────────────────────────────────────────────┘
```

### Three Levels of Prediction

| Level | Purpose | Accuracy (MAPE) | Primary Use |
|-------|---------|------------------|---------------|
| **SKU** | Individual product | 15% (best) | Automated replenishment |
| **Category** | Product group | 29% | Purchasing planning |
| **Customer** | Individual account | 49% | Commercial strategy |

---

## Results: From Stockouts to Service Level Excellence

### Model Performance by Category

| Category | MAPE | Interpretation |
|----------|------|----------------|
| **Home & Garden** | 29% | Excellent - reliable for planning |
| **Hardware** | 32% | Good - useful with safety stock |
| **Toiletries** | 35% | Good - directional guidance |
| **Brand Manufacturers** | 38% | Fair - add buffers |
| **Baby & Toddler** | 41% | Fair - directional guidance |
| **Food** | 45% | Fair - investigate patterns |
| **Beverages** | 52% | Fair - high seasonality |
| **Vehicles & Parts** | 55% | High variance - use caution |

### Key Performance Indicators

```
                    MODEL PERFORMANCE
                    ─────────────────

SKU Level           15% MAPE        ✓ Excellent
(Top 50 SKUs)       Best: BB Cement 32.5R

Category Level      29% MAPE        ✓ Good
(9 Categories)      Best: Home & Garden

Customer Level      49% MAPE        ~ Fair
(Top 30 Customers)  Higher variance expected
```

---

## The Shift: From Reactive to Proactive

### Before: Reactive Operations

```
Monday AM:   "We ran out of cement on Friday"
             → Emergency order
             → Premium shipping cost
             → Frustrated customer

End of Month: "We have 3 months of shampoo stock"
              → Negotiate return with supplier
              → Forced discount to move inventory
              → Eroded margin
```

### After: Proactive Operations

```
Monday AM:   System alert: "Cement demand +40% next 2 weeks"
             → Order scheduled in advance
             → Optimized logistics cost
             → Satisfied customer

End of Month: Dashboard shows: "Shampoo stock aligned to forecast"
              → Healthy turnover
              → No trapped capital
              → Protected margin
```

---

## Emerging Use Cases

Co-creation with ACA has revealed additional opportunities:

### 1. Intelligent Upselling

```
System Insight:
"Customer #46 will buy ~2,500 units of Hardware next week"

Commercial Action:
Representative proactively contacts with volume offer
→ Order increases to 3,200 units (+28%)
```

### 2. Anticipating Inbound Sales

```
Aggregated Forecast:
"Beverages Category: +35% demand in W31-W34 (summer)"

Purchasing Action:
Negotiate volume with suppliers 6 weeks in advance
→ Better pricing for committed volume
→ Guaranteed availability
```

### 3. Service Segmentation

```
Customer Analysis:
- Predictable demand customers (MAPE < 30%): Standard service
- Volatile customers (MAPE > 60%): Additional safety stock

Result:
Service resources allocated by predictability
→ Operational efficiency without sacrificing service
```

---

## The RedAI Philosophy: Data → AI → Actions → Results

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│                     THE VIRTUOUS CYCLE                          │
│                                                                 │
│         ┌─────────┐                        ┌─────────┐         │
│         │  DATA   │                        │ RESULTS │         │
│         │         │                        │         │         │
│         │Transact.│                        │+Revenue │         │
│         │Historic.│                        │-Costs   │         │
│         │Patterns │                        │+NPS     │         │
│         └────┬────┘                        └────▲────┘         │
│              │                                  │               │
│              ▼                                  │               │
│         ┌─────────┐                        ┌─────────┐         │
│         │   AI    │─────────────────────▶ │ ACTIONS │         │
│         │         │                        │         │         │
│         │XGBoost  │                        │Buy      │         │
│         │Forecast │                        │Sell     │         │
│         │Insights │                        │Plan     │         │
│         └─────────┘                        └─────────┘         │
│                                                                 │
│    "It's not AI for AI's sake. It's AI that generates          │
│     measurable commercial actions that impact the P&L"          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## Technical Implementation

### Models Deployed to BigQuery ML

| Model | Type | Training Period | Test Period |
|-------|------|-----------------|-------------|
| `xgb_sku_model` | BOOSTED_TREE_REGRESSOR | H1 (W01-W26) | H2 (W27-W52) |
| `xgb_category_model` | BOOSTED_TREE_REGRESSOR | H1 (W01-W26) | H2 (W27-W52) |
| `xgb_customer_model` | BOOSTED_TREE_REGRESSOR | H1 (W01-W26) | H2 (W27-W52) |

### Feature Engineering (All Levels)

- `lag1` - Previous week demand
- `lag2` - 2 weeks ago demand
- `lag4` - 4 weeks ago demand (monthly pattern)
- `rolling_avg_4w` - 4-week rolling average
- `week_num` - Week of year (seasonality)

### API Access

Models are exposed via BigQuery ML API:
```sql
SELECT * FROM ML.PREDICT(MODEL `project.dataset.xgb_category_model`,
  (SELECT * FROM features_table WHERE year_week = '2026-W05'))
```

---

## Conclusion

ACA's transformation demonstrates that **intelligent demand planning is not a technology project, it's a business project enabled by technology**.

> *"We went from putting out fires every day to anticipating what's coming.
> Our customers notice, our team appreciates it, and our numbers reflect it."*
>
> — ACA Operations Team

**The impact is clear:**
- Fewer stockouts → more captured sales
- Less over-stock → less trapped capital
- Better visibility → better decisions
- Data + AI → actions that generate value

---

*Developed in co-creation by RedAI × ACA*
*January 2026*
