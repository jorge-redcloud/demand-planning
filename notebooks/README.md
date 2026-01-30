# ACA Demand Planning - Notebooks

## Estructura de Carpetas

```
notebooks/
├── shared/                     # Módulos compartidos
│   ├── config.py              # Configuración GCP (bucket, BigQuery, thresholds)
│   ├── experiment_tracker.py  # Tracking de experimentos con Vertex AI
│   └── setup.py               # Import rápido para notebooks
│
├── prep/                       # Preparación de datos (correr 1 vez)
│   ├── 00_setup_config.ipynb  # Configuración inicial
│   ├── 01_data_extraction.ipynb
│   ├── 02_data_prep.ipynb
│   └── 03_feature_engineering.ipynb
│
├── experiments/                # Experimentación (iterar muchas veces)
│   └── 06_run_experiment.ipynb
│
└── production/                 # Modelos de producción
    ├── 04_model_training.ipynb
    └── 05_model_selection.ipynb
```

---

## Workflow

### 1️⃣ Preparación (Una vez o cuando cambien los datos)

```bash
# Orden de ejecución:
prep/00_setup_config.ipynb    → Configura GCP, crea config.py
prep/01_data_extraction.ipynb → Excel → CSV
prep/02_data_prep.ipynb       → Limpieza, dimensiones
prep/03_feature_engineering.ipynb → Features, H1/H2 split
```

**Dónde correr:** Local o GCP (ambos funcionan)

**Output:**
- GCS: `gs://demand_planning_aca/processed/`, `gs://demand_planning_aca/features/`
- BigQuery: `fact_lineitem`, `dim_products`, `features_*`

---

### 2️⃣ Experimentación (Iterar muchas veces)

```bash
# Editar parámetros y re-correr:
experiments/06_run_experiment.ipynb
```

**Parámetros a cambiar:**
```python
EXPERIMENT_NAME = "v5_no_blackfriday"
MODEL_VERSION = "V5"
EXCLUDE_WEEKS = [47]           # Excluir Black Friday
MIN_H1_WEEKS = 10
USE_PATTERN_FEATURES = True
```

**Dónde correr:** GCP recomendado (más rápido para iterar)

**Output:**
- GCS: `gs://demand_planning_aca/experiments/{nombre}/{run}/`
- Vertex AI: Métricas y comparación de experimentos

---

### 3️⃣ Producción (Cuando elijas el mejor modelo)

```bash
production/04_model_training.ipynb  → Entrenar modelo final
production/05_model_selection.ipynb → Reglas de selección por SKU
```

**Dónde correr:** GCP

**Output:**
- GCS: `gs://demand_planning_aca/models/`, `gs://demand_planning_aca/predictions/`
- BigQuery: `predictions_sku_all`, `model_selection`

---

## Cómo Usar

### En cada notebook, agregar al inicio:

```python
import sys
sys.path.insert(0, '../shared')
from setup import *

# Ahora tienes acceso a:
# - CONFIG, DIO, STORAGE_CLIENT
# - ExperimentTracker, create_experiment
# - pandas as pd, numpy as np
```

### O importar selectivamente:

```python
import sys
sys.path.insert(0, '../shared')
from config import CONFIG, DIO
from experiment_tracker import ExperimentTracker
```

---

## GCP Resources

| Recurso | Valor |
|---------|-------|
| **GCS Bucket** | `gs://demand_planning_aca/` |
| **BigQuery Project** | `mimetic-maxim-443710-s2` |
| **BigQuery Dataset** | `redai_demand_forecast` |
| **Vertex AI Region** | `us-central1` |

---

## Estructura GCS

```
gs://demand_planning_aca/
├── raw_data/           # Excel originales
├── processed/          # CSVs limpios, dimensiones
├── features/           # Features para modelos
├── models/             # Modelos de producción
├── predictions/        # Predicciones de producción
└── experiments/        # ⭐ Experimentos versionados
    ├── v5_no_blackfriday_20260125/
    │   └── sku_level/
    │       ├── predictions.csv
    │       ├── wmape_per_sku.csv
    │       ├── models/xgb_V5.joblib
    │       └── run_summary.json
    └── v6_price_features_20260126/
        └── ...
```

---

## Thresholds (en CONFIG)

| Parámetro | Valor | Descripción |
|-----------|-------|-------------|
| `MIN_H1_WEEKS` | 10 | Mínimo semanas para modelar |
| `HIGH_CONF_WEEKS` | 20 | Semanas para alta confianza |
| `WMAPE_HIGH_THRESHOLD` | 40% | WMAPE < 40% = HIGH confidence |
| `WMAPE_MEDIUM_THRESHOLD` | 70% | WMAPE < 70% = MEDIUM confidence |
| `H1_END_WEEK` | 26 | H1 = W01-W26 (training) |
| `H2_START_WEEK` | 27 | H2 = W27-W52 (validation) |

---

## Comparar Experimentos

### En Vertex AI Console:
```
https://console.cloud.google.com/vertex-ai/experiments
```

### En Python:
```python
from experiment_tracker import ExperimentTracker

# Listar todos los experimentos
experiments = ExperimentTracker.list_experiments()

# Comparar runs de un experimento
comparison = ExperimentTracker.compare_runs("v5_no_blackfriday_20260125")
print(comparison)
```

---

## Troubleshooting

### GCP no disponible
Si ves "GCP not available", los notebooks funcionan en modo local:
- Lee/escribe de `../features_v2/` en lugar de GCS
- No sube a BigQuery
- Experimentos se guardan en `../experiments/`

### Autenticar en local
```bash
gcloud auth application-default login
```

### Instalar dependencias
```bash
pip install google-cloud-storage google-cloud-bigquery google-cloud-aiplatform pandas xgboost
```
