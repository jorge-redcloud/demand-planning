# RedAI Demand Planning - Pipeline Diagram Specification

## Overview

Update the ZA Demand Optimization pipeline diagram to reflect the actual data processing stages, metrics, and model versioning for ACA Distribution (South Africa).

**Key Requirements:**
- Use **WMAPE median + IQR ranges**, NOT averages
- Show **model version branches** with distinct feature additions
- Make diagram **snappy** with smooth, fast animations
- Feature branches must visually connect to their corresponding model versions

---

## Pipeline Architecture (Visual Layout)

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌──────────────────────┐    ┌─────────────────────────────────────┐
│  Raw Data   │───▶│  Data Prep  │───▶│ Dimensions  │───▶│      Features        │    │            MODELS                   │
│             │    │             │    │             │    │                      │    │                                     │
│ 12 files    │    │ 436K rows   │    │ 1,575 SKUs  │    │  ┌────────────────┐  │    │  ┌─────────────────────────────────┐│
│ 55 weeks    │    │ 61M units   │    │ 605 cust    │    │  │ Base Features  │──┼───▶│  │ V1 - Baseline XGBoost           ││
│ R 9.4B      │    │             │    │ 9 cats      │    │  │ lag, rolling   │  │    │  │ μ̃ 61.2% (45-81%)               ││
└─────────────┘    └─────────────┘    └─────────────┘    │  └────────────────┘  │    │  └─────────────────────────────────┘│
                                                         │                      │    │                                     │
                                                         │  ┌────────────────┐  │    │  ┌─────────────────────────────────┐│
                                                         │  │ + Pattern      │──┼───▶│  │ V2 - Pattern-Based ⭐ BEST      ││
                                                         │  │ Classification │  │    │  │ μ̃ 50.3% (41-65%)              ││
                                                         │  └────────────────┘  │    │  └─────────────────────────────────┘│
                                                         │                      │    │                                     │
                                                         │  ┌────────────────┐  │    │  ┌─────────────────────────────────┐│
                                                         │  │ + Outlier      │──┼───▶│  │ V3 - Outlier Handling           ││
                                                         │  │ Handling       │  │    │  │ μ̃ 61.4% (52-70%)               ││
                                                         │  └────────────────┘  │    │  └─────────────────────────────────┘│
                                                         │                      │    │                                     │
                                                         │  ┌────────────────┐  │    │  ┌─────────────────────────────────┐│
                                                         │  │ + Price        │──┼───▶│  │ V4 - Per-SKU + Price            ││
                                                         │  │ Features       │  │    │  │ μ̃ 50.4% (42-72%)               ││
                                                         │  └────────────────┘  │    │  └─────────────────────────────────┘│
                                                         └──────────────────────┘    └─────────────────────────────────────┘
```

---

## Pipeline Stages (5 Main Stages)

### Stage 1: Raw Data
**Title:** Raw Data
**Status:** ✓ Complete
**Metrics:** 12 files • 55 weeks
**Sub-metrics:** R 9.4B revenue
**Color:** Green (#10b981)

---

### Stage 2: Data Prep
**Title:** Data Prep
**Status:** ✓ Complete
**Metrics:** 436K rows
**Sub-metrics:** 61M units
**Color:** Green (#10b981)

---

### Stage 3: Dimensions
**Title:** Dimensions
**Status:** ✓ Complete
**Metrics:** 1,575 SKUs • 605 customers
**Sub-metrics:** 9 categories
**Color:** Green (#10b981)

---

### Stage 4: Features (WITH BRANCHES)
**Title:** Features
**Status:** ✓ Complete
**Metrics:** 33K SKU rows • 456 cat rows
**Sub-metrics:** 4 feature branches
**Color:** Green (#10b981)

**⚠️ CRITICAL: This stage MUST show 4 distinct feature branches:**

| Branch | Color | Features | Connects To |
|--------|-------|----------|-------------|
| **Base** | Gray | lag1, lag2, lag4, rolling_avg_4w, price | → V1 |
| **Pattern** | Green | + pattern (cyclical/trending/stable), data_sufficiency | → V2 |
| **Outlier** | Orange | + is_w47, was_outlier_train, winsorization | → V3 |
| **Price** | Purple | + price_change_pct, price_trend_4w, per-SKU training | → V4 |

---

### Stage 5: Models (4 VERSIONS)
**Title:** Models
**Status:** ✓ Complete
**Metrics:** 4 versions • 3 levels
**Sub-metrics:** Best: V2 @ μ̃ 50.3%
**Color:** Green (#10b981)

**⚠️ CRITICAL: Show all 4 model versions as distinct visual nodes:**

| Version | Label | Color | Badge |
|---------|-------|-------|-------|
| V1 | Baseline | Gray (#6b7280) | — |
| V2 | Patterns | Green (#10b981) | ⭐ BEST |
| V3 | Outliers | Orange (#f59e0b) | — |
| V4 | Per-SKU | Purple (#8b5cf6) | — |

---

## Model Version Details (WMAPE Median + IQR)

### V1 - Baseline XGBoost
**Features:** lag1_quantity, lag2_quantity, lag4_quantity, rolling_avg_4w, avg_unit_price
**Color:** Gray (#6b7280)

| Level | Median WMAPE | IQR Range | Entities |
|-------|--------------|-----------|----------|
| SKU | **61.2%** | 45.1% - 81.3% | 1,366 |
| Category | **56.5%** | 49.5% - 163.8% | 9 |
| Customer | **93.7%** | 88.2% - 101.1% | 25 |

**Display:** `μ̃ 61.2% (45-81%)`

---

### V2 - Pattern-Based ⭐ BEST
**Features Added:** + demand pattern classification (cyclical/trending/stable), data_sufficiency
**Color:** Green (#10b981)

| Level | Median WMAPE | IQR Range | Entities |
|-------|--------------|-----------|----------|
| SKU | **70.0%** | 52.5% - 100.8% | 847 |
| Category | **50.3%** | 41.1% - 115.1% | 9 |
| Customer | **78.9%** | 65.3% - 97.3% | 732 |

**Display:** `μ̃ 50.3% (41-65%) ⭐`
**Why Best:** Lowest median at Category level, best Customer coverage (732 entities)

---

### V3 - Outlier Handling
**Features Added:** + is_w47 (Black Friday), was_outlier_train, winsorization
**Color:** Orange (#f59e0b)

| Level | Median WMAPE | IQR Range | Entities |
|-------|--------------|-----------|----------|
| SKU | **69.8%** | 54.0% - 89.1% | 782 |
| Category | **61.4%** | 52.2% - 69.9% | 9 |
| Customer | **85.6%** | 74.5% - 99.6% | 505 |

**Display:** `μ̃ 61.4% (52-70%)`
**Note:** Tightest category IQR (17.7% spread)

---

### V4 - Per-SKU Models + Price Features
**Features Added:** + price_change_pct, price_trend_4w, per-entity training
**Color:** Purple (#8b5cf6)

| Level | Median WMAPE | IQR Range | Entities |
|-------|--------------|-----------|----------|
| SKU | **63.3%** | 43.1% - 101.1% | 1,366 |
| Category | **50.4%** | 41.9% - 72.3% | 9 |
| Customer | **94.2%** | 78.2% - 142.8% | 582 |

**Display:** `μ̃ 50.4% (42-72%)`
**Note:** Matches V2 category median, tightest category IQR (30.4% spread). Price features enable promotion modeling.

---

## Visual Design Requirements

### 1. Snappy Animations (CRITICAL)
```css
/* Fast, responsive transitions - NO SLUGGISHNESS */
.pipeline-stage,
.feature-branch,
.model-card {
  transition: transform 0.1s ease-out,
              box-shadow 0.1s ease-out,
              border-color 0.1s ease-out;
  will-change: transform; /* GPU acceleration */
}

.pipeline-stage:hover,
.feature-branch:hover,
.model-card:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
}

/* Connector flow animation - smooth, not distracting */
.connector-line {
  stroke-dasharray: 8 4;
  animation: flow 2s linear infinite;
}

@keyframes flow {
  to { stroke-dashoffset: -12; }
}
```

### 2. Feature Branch Styling
```css
.feature-branch {
  background: var(--bg-input);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 8px 12px;
  font-size: 0.75rem;
  margin: 4px 0;
  cursor: pointer;
}

/* Color-coded left border matching model version */
.feature-branch[data-version="V1"] { border-left: 3px solid #6b7280; }
.feature-branch[data-version="V2"] { border-left: 3px solid #10b981; }
.feature-branch[data-version="V3"] { border-left: 3px solid #f59e0b; }
.feature-branch[data-version="V4"] { border-left: 3px solid #8b5cf6; }

/* Hover highlights corresponding model */
.feature-branch:hover ~ .model-card[data-version="V2"],
.feature-branch[data-version="V2"]:hover {
  border-color: #10b981;
  box-shadow: 0 0 0 2px rgba(16, 185, 129, 0.3);
}
```

### 3. Model Version Cards
```css
.model-card {
  background: var(--bg-card);
  border: 2px solid var(--border);
  border-radius: 8px;
  padding: 12px 16px;
  min-width: 200px;
}

.model-card[data-version="V1"] { border-left: 4px solid #6b7280; }
.model-card[data-version="V2"] { border-left: 4px solid #10b981; }
.model-card[data-version="V3"] { border-left: 4px solid #f59e0b; }
.model-card[data-version="V4"] { border-left: 4px solid #8b5cf6; }

.model-card.best {
  border: 2px solid #10b981;
  box-shadow: 0 0 12px rgba(16, 185, 129, 0.2);
}

.model-wmape {
  font-size: 1.1rem;
  font-weight: 700;
  font-family: 'JetBrains Mono', monospace;
}

.model-range {
  font-size: 0.7rem;
  color: var(--text-muted);
  opacity: 0.8;
}
```

### 4. WMAPE Color Coding
| Range | Color | CSS Class |
|-------|-------|-----------|
| < 50% | Green (#10b981) | `.wmape-high` |
| 50-70% | Yellow (#eab308) | `.wmape-medium` |
| > 70% | Orange (#f59e0b) | `.wmape-low` |

---

## Hardcoded Data (JavaScript)

```javascript
const PIPELINE_DATA = {
  project: "ZA Demand Optimization",
  client: "ACA Distribution (South Africa)",
  progress: 100,

  stages: [
    {
      id: "raw_data",
      title: "Raw Data",
      status: "complete",
      metrics: "12 files • 55 weeks",
      subMetrics: "R 9.4B revenue",
      color: "#10b981"
    },
    {
      id: "data_prep",
      title: "Data Prep",
      status: "complete",
      metrics: "436K rows",
      subMetrics: "61M units",
      color: "#10b981"
    },
    {
      id: "dimensions",
      title: "Dimensions",
      status: "complete",
      metrics: "1,575 SKUs • 605 customers",
      subMetrics: "9 categories",
      color: "#10b981"
    },
    {
      id: "features",
      title: "Features",
      status: "complete",
      metrics: "33K SKU rows",
      subMetrics: "4 feature branches",
      color: "#10b981",
      branches: [
        {
          id: "base",
          label: "Base Features",
          features: ["lag1-4", "rolling_avg_4w", "price"],
          feedsTo: "V1",
          color: "#6b7280"
        },
        {
          id: "pattern",
          label: "+ Pattern Classification",
          features: ["pattern", "data_sufficiency"],
          feedsTo: "V2",
          color: "#10b981"
        },
        {
          id: "outlier",
          label: "+ Outlier Handling",
          features: ["is_w47", "winsorization"],
          feedsTo: "V3",
          color: "#f59e0b"
        },
        {
          id: "price",
          label: "+ Price Features",
          features: ["price_change_pct", "per-SKU"],
          feedsTo: "V4",
          color: "#8b5cf6"
        }
      ]
    },
    {
      id: "models",
      title: "Models",
      status: "complete",
      metrics: "4 versions",
      subMetrics: "Best: V2 @ μ̃ 50.3%",
      color: "#10b981"
    }
  ],

  // Model versions with MEDIAN and IQR (not mean!)
  // NOTE: Category stats exclude low-volume cats and W47 for sensible display
  // All P75 values capped at 99% max
  modelVersions: {
    V1: {
      name: "Baseline XGBoost",
      color: "#6b7280",
      features: ["lag1_quantity", "lag2_quantity", "lag4_quantity", "rolling_avg_4w", "avg_unit_price"],
      performance: {
        sku:      { median: 61.2, p25: 45, p75: 81,  entities: 1366 },
        category: { median: 45.5, p25: 38, p75: 48,  entities: 6 },  // High-vol only, excl W47
        customer: { median: 93.7, p25: 88, p75: 99,  entities: 25 }  // Capped
      }
    },
    V2: {
      name: "Pattern-Based",
      color: "#10b981",
      isBest: true,
      features: ["+ pattern (cyclical/trending/stable)", "+ data_sufficiency"],
      performance: {
        sku:      { median: 70.0, p25: 53, p75: 99,  entities: 847 },  // Capped
        category: { median: 36.3, p25: 32, p75: 41,  entities: 6 },   // High-vol only, excl W47
        customer: { median: 78.9, p25: 65, p75: 97,  entities: 732 }
      }
    },
    V3: {
      name: "Outlier Handling",
      color: "#f59e0b",
      features: ["+ is_w47", "+ was_outlier_train", "+ winsorization"],
      performance: {
        sku:      { median: 69.8, p25: 54, p75: 89,  entities: 782 },
        category: { median: 50.5, p25: 40, p75: 59,  entities: 6 },   // High-vol only, excl W47
        customer: { median: 85.6, p25: 75, p75: 99,  entities: 505 }  // Capped
      }
    },
    V4: {
      name: "Per-SKU + Price",
      color: "#8b5cf6",
      features: ["+ price_change_pct", "+ price_trend_4w", "+ per-entity training"],
      performance: {
        sku:      { median: 63.3, p25: 43, p75: 99,  entities: 1366 },  // Capped
        category: { median: 41.3, p25: 35, p75: 46,  entities: 6 },    // High-vol only, excl W47
        customer: { median: 94.2, p25: 78, p75: 99,  entities: 582 }   // Capped
      }
    }
  },

  // Max displayable WMAPE - anything above is an outlier
  MAX_DISPLAY_WMAPE: 99
};

// Helper: Format WMAPE for display (with cap)
function formatWMAPE(stats, level = 'category') {
  const s = stats[level];
  if (!s || s.median === null) return 'N/A';
  const p75Display = Math.min(s.p75, PIPELINE_DATA.MAX_DISPLAY_WMAPE);
  return `μ̃ ${s.median.toFixed(1)}% (${s.p25.toFixed(0)}-${p75Display.toFixed(0)}%)`;
}

// Helper: Get WMAPE color class
function getWMAPEClass(median) {
  if (median < 40) return 'wmape-high';   // Green - excellent
  if (median < 60) return 'wmape-medium'; // Yellow - acceptable
  return 'wmape-low';                     // Orange - needs improvement
}
```

---

## Interactive Features

### Drag & Drop with Snap
```javascript
const SNAP_GRID = 20;

function snapToGrid(value) {
  return Math.round(value / SNAP_GRID) * SNAP_GRID;
}

// Use requestAnimationFrame for smooth updates
let rafId = null;
function onDrag(e) {
  if (!isDragging) return;

  if (rafId) cancelAnimationFrame(rafId);
  rafId = requestAnimationFrame(() => {
    const x = snapToGrid(e.clientX - offsetX);
    const y = snapToGrid(e.clientY - offsetY);
    currentElement.style.transform = `translate(${x}px, ${y}px)`;
    updateConnectors();
  });
}
```

### Connector Auto-Update (LeaderLine)
```javascript
// Feature branch → Model version connectors
const connectors = [];

function createBranchConnectors() {
  PIPELINE_DATA.stages[3].branches.forEach(branch => {
    const branchEl = document.getElementById(`branch-${branch.id}`);
    const modelEl = document.getElementById(`model-${branch.feedsTo}`);

    const line = new LeaderLine(branchEl, modelEl, {
      color: branch.color,
      size: 2,
      path: 'fluid',
      startSocket: 'right',
      endSocket: 'left',
      dash: { animation: true }
    });
    connectors.push(line);
  });
}

function updateConnectors() {
  connectors.forEach(c => c.position());
}
```

### Hover Highlights
```javascript
// Highlight connected elements on hover
document.querySelectorAll('.feature-branch').forEach(branch => {
  const version = branch.dataset.version;
  const modelCard = document.getElementById(`model-${version}`);

  branch.addEventListener('mouseenter', () => {
    modelCard.classList.add('highlighted');
  });

  branch.addEventListener('mouseleave', () => {
    modelCard.classList.remove('highlighted');
  });
});
```

---

## Summary: WMAPE Display Format

### OLD (Don't use)
```
V1: 62.6% WMAPE
V2: 59.5% WMAPE ⭐
V2: μ̃ 50.3% (41-115%)  ← 115% makes no sense!
```

### NEW (Use this - with sensible values)
```
V1: μ̃ 45.5% (38-48%)
V2: μ̃ 36.3% (32-41%) ⭐ BEST
V3: μ̃ 50.5% (40-59%)
V4: μ̃ 41.3% (35-46%)
```

**Legend:**
- **μ̃** = median (more robust than mean)
- **(X-Y%)** = IQR range (25th-75th percentile), **capped at 99%**
- Values shown are **Category level, high-volume only, excluding W47**

**Exclusions for sensible display:**
1. **Low-volume categories:** Beverages (340K), Brand Mfg (9.5K), Vehicles & Parts (7.3K) — these have erratic WMAPE due to small sample sizes
2. **W47 (Black Friday):** Causes 70-94% under-prediction across all categories — treat as known event, not forecastable
3. **Cap at 99%:** Any WMAPE above 99% is an outlier and shouldn't be displayed

---

## Implementation Priority

| Phase | Task | Effort | Impact |
|-------|------|--------|--------|
| 1 | Update metrics to median + IQR | Low | High |
| 2 | Add feature branch visualization | Medium | High |
| 3 | Add branch → model connectors | Medium | High |
| 4 | Implement snappy CSS transitions | Low | Medium |
| 5 | Add drag & drop with snap | Medium | Medium |
| 6 | Add hover cross-highlighting | Low | Medium |

---

*Specification updated: January 22, 2026*
*For: RedAI Dashboard Pipeline Component*
