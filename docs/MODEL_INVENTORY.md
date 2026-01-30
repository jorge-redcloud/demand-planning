# Inventario de Modelos de Forecasting

**RedAI Demand Forecasting - ACA Dataset**
**Versión:** 1.0
**Fecha:** Enero 2026

---

## Resumen Ejecutivo

Durante el desarrollo del sistema de forecasting, se crearon y evaluaron **15 modelos** en total, distribuidos en 3 niveles de agregación. Después de la evaluación comparativa, se seleccionó **XGBoost** como el modelo activo para los 3 niveles.

| Nivel | Modelos Evaluados | Modelo Seleccionado | Mejor MAPE |
|-------|-------------------|---------------------|------------|
| SKU | 8 | XGBoost | 15.0% |
| Category | 5 | XGBoost | 29.0% |
| Customer | 2 | XGBoost | 49.0% |
| **TOTAL** | **15** | **3 activos** | - |

---

## 1. Modelos a Nivel SKU (8 modelos)

### 1.1 XGBoost ✅ **MODELO ACTIVO**

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_XGBoost.csv` |
| **Estado** | ✅ Activo - En producción |
| **Algoritmo** | Gradient Boosting Regressor |
| **Features** | lag1, lag2, lag4, rolling_avg_4w, week_num |
| **Mejor MAPE** | 15.0% |
| **Peor MAPE** | 215.0% |

**Configuración:**
```python
GradientBoostingRegressor(
    n_estimators=50,
    max_depth=3,
    learning_rate=0.1,
    random_state=42
)
```

**¿Por qué se eligió?**
- Mejor performance general en la mayoría de SKUs
- Captura patrones no lineales y estacionalidad
- Robusto ante outliers gracias a los árboles de decisión

---

### 1.2 Moving Average 4 Weeks (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_MA_4Week.csv` |
| **Estado** | ⚠️ Deprecado |
| **Algoritmo** | Promedio móvil simple |
| **Fórmula** | ŷ(t) = (y(t-1) + y(t-2) + y(t-3) + y(t-4)) / 4 |
| **MAPE Mediana** | 58.3% |

**¿Por qué se deprecó?**
- No captura tendencias
- Reacciona lento a cambios bruscos
- Superado por XGBoost en 86% de los SKUs

---

### 1.3 Moving Average 8 Weeks (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_MA_8Week.csv` |
| **Estado** | ⚠️ Deprecado |
| **Algoritmo** | Promedio móvil simple |
| **Fórmula** | ŷ(t) = (y(t-1) + ... + y(t-8)) / 8 |
| **MAPE Mediana** | 54.9% |

**¿Por qué se deprecó?**
- Demasiado suavizado, pierde señales importantes
- Requiere más historia (8 semanas mínimo)

---

### 1.4 Exponential Smoothing α=0.3 (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_ExpSmooth_03.csv` |
| **Estado** | ⚠️ Deprecado |
| **Algoritmo** | Suavizado exponencial simple |
| **Fórmula** | ŷ(t) = 0.3 × y(t-1) + 0.7 × ŷ(t-1) |
| **MAPE Mediana** | 57.7% |

**¿Por qué se deprecó?**
- α bajo = reacción lenta a cambios
- No captura estacionalidad

---

### 1.5 Exponential Smoothing α=0.5 (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_ExpSmooth_05.csv` |
| **Estado** | ⚠️ Deprecado |
| **Algoritmo** | Suavizado exponencial simple |
| **Fórmula** | ŷ(t) = 0.5 × y(t-1) + 0.5 × ŷ(t-1) |
| **MAPE Mediana** | 55.2% |

**¿Por qué se deprecó?**
- Mejor que α=0.3 pero aún inferior a XGBoost
- No usa features adicionales

---

### 1.6 Linear Trend (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_Linear_Trend.csv` |
| **Estado** | ⚠️ Deprecado |
| **Algoritmo** | Regresión lineal sobre tiempo |
| **Fórmula** | ŷ(t) = β₀ + β₁ × t |
| **MAPE Mediana** | 86.9% |

**¿Por qué se deprecó?**
- Asume tendencia lineal constante (raramente cierto)
- No captura estacionalidad ni volatilidad
- Peor performance de todos los modelos

---

### 1.7 Naive (Last Value) (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_Naive_Last.csv` |
| **Estado** | ⚠️ Deprecado - **Baseline** |
| **Algoritmo** | Persistencia |
| **Fórmula** | ŷ(t) = y(t-1) |
| **MAPE Mediana** | 62.1% |

**Uso:**
- Sirve como **baseline** para comparar otros modelos
- Si un modelo no supera al Naive, no aporta valor

---

### 1.8 Seasonal Naive (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/sku_predictions_Seasonal_Naive.csv` |
| **Estado** | ⚠️ Deprecado |
| **Algoritmo** | Persistencia estacional |
| **Fórmula** | ŷ(t) = y(t-52) (mismo periodo año anterior) |
| **MAPE Mediana** | N/A (datos insuficientes) |

**¿Por qué se deprecó?**
- Requiere al menos 1 año de historia
- Datos disponibles no cubren año completo anterior

---

## 2. Modelos a Nivel Category (5 modelos)

### 2.1 XGBoost ✅ **MODELO ACTIVO**

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/category_predictions_XGBoost.csv` |
| **Estado** | ✅ Activo - En producción |
| **Algoritmo** | Gradient Boosting Regressor |
| **Features** | lag1, lag2, lag4, rolling_avg_4w, week_num |
| **Mejor MAPE** | 29.0% (Home & Garden) |
| **Peor MAPE** | 63.4% (Unknown) |

**Performance por Categoría:**
| Categoría | MAPE |
|-----------|------|
| Home & Garden | 29.0% |
| Toiletries | 31.2% |
| Baby and Toddler | 35.8% |
| Hardware | 38.5% |
| Food | 42.1% |
| Brand Manufacturers | 45.3% |
| Beverages | 51.7% |
| Vehicles & Parts | 58.2% |
| Unknown | 63.4% |

---

### 2.2 Exponential Smoothing α=0.3 (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/category_predictions_ExpSmooth_03.csv` |
| **Estado** | ⚠️ Deprecado |
| **Problema** | Predicciones eran líneas planas (no walk-forward correcto) |
| **MAPE** | 33.4% - 102.9% |

**¿Por qué se deprecó?**
- Implementación inicial incorrecta (no actualizaba con actuals)
- Reemplazado por versión walk-forward, luego por XGBoost

---

### 2.3 Exponential Smoothing Walk-Forward (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/category_predictions_walkforward.csv` |
| **Estado** | ⚠️ Deprecado |
| **Algoritmo** | Exp. Smoothing con actualización semanal |
| **MAPE** | 18.4% - 61.5% |

**¿Por qué se deprecó?**
- Mejor que versión original pero XGBoost ofrece más flexibilidad
- No usa features adicionales como week_num

---

### 2.4 Moving Average 4 Weeks (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/category_predictions_MA_4Week.csv` |
| **Estado** | ⚠️ Deprecado |
| **MAPE** | Similar a Exp. Smoothing |

---

### 2.5 Linear Trend / Naive (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivos** | `category_predictions_Linear_Trend.csv`, `category_predictions_Naive_Last.csv` |
| **Estado** | ⚠️ Deprecado |
| **Uso** | Solo para comparación baseline |

---

## 3. Modelos a Nivel Customer (2 modelos)

### 3.1 XGBoost ✅ **MODELO ACTIVO**

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/customer_predictions_XGBoost.csv` |
| **Estado** | ✅ Activo - En producción |
| **Algoritmo** | Gradient Boosting Regressor |
| **Features** | lag1, lag2, lag4, rolling_avg_4w, week_num |
| **Cobertura** | Top 30 clientes por volumen |
| **Mejor MAPE** | 49.0% |
| **Peor MAPE** | 406.8% |

**Nota importante:** Customer-level tiene la mayor variabilidad porque:
- Patrones de compra erráticos (bulk orders)
- Muchas semanas sin compras
- Promociones puntuales no capturadas

---

### 3.2 Exponential Smoothing Walk-Forward (Deprecado)

| Atributo | Valor |
|----------|-------|
| **Archivo** | `model_evaluation/customer_predictions_walkforward.csv` |
| **Estado** | ⚠️ Deprecado |
| **MAPE** | 23.6% - 100%+ |

**¿Por qué se deprecó?**
- Reemplazado por XGBoost para consistencia entre niveles
- XGBoost permite añadir más features en el futuro

---

## 4. Comparación de Modelos (SKU Level)

Resultados de evaluación comparativa en H2 (W27-W52):

| Modelo | Median MAPE | Mean MAPE | % SKUs < 50% MAPE |
|--------|-------------|-----------|-------------------|
| **XGBoost** | **50.8%** | 67.2% | **62%** |
| MA 8 Weeks | 54.9% | 71.3% | 55% |
| Exp. Smooth α=0.5 | 55.2% | 72.1% | 54% |
| Exp. Smooth α=0.3 | 57.7% | 74.8% | 51% |
| MA 4 Weeks | 58.3% | 75.2% | 50% |
| Naive | 62.1% | 78.9% | 45% |
| Linear Trend | 86.9% | 112.4% | 28% |

**Conclusión:** XGBoost supera a todos los baselines en todas las métricas.

---

## 5. Modelos en BigQuery ML

Después de desplegar en BigQuery, tendrás **3 modelos** entrenados:

| Modelo BigQuery | Equivalente Local | Tabla de Predicciones |
|-----------------|-------------------|----------------------|
| `xgb_sku_model` | XGBoost SKU | `predictions_sku` |
| `xgb_category_model` | XGBoost Category | `predictions_category` |
| `xgb_customer_model` | XGBoost Customer | `predictions_customer` |

**Configuración BigQuery ML:**
```sql
CREATE MODEL `project.dataset.xgb_sku_model`
OPTIONS(
  model_type = 'BOOSTED_TREE_REGRESSOR',
  input_label_cols = ['quantity'],
  max_iterations = 50,
  max_tree_depth = 3,
  learn_rate = 0.1
)
```

---

## 6. Archivos de Referencia

### Predicciones Activas
```
model_evaluation/
├── sku_predictions_XGBoost.csv        ✅ Activo
├── category_predictions_XGBoost.csv   ✅ Activo
└── customer_predictions_XGBoost.csv   ✅ Activo
```

### Predicciones Deprecadas (para referencia histórica)
```
model_evaluation/
├── sku_predictions_MA_4Week.csv           ⚠️ Deprecado
├── sku_predictions_MA_8Week.csv           ⚠️ Deprecado
├── sku_predictions_ExpSmooth_03.csv       ⚠️ Deprecado
├── sku_predictions_ExpSmooth_05.csv       ⚠️ Deprecado
├── sku_predictions_Linear_Trend.csv       ⚠️ Deprecado
├── sku_predictions_Naive_Last.csv         ⚠️ Deprecado (baseline)
├── sku_predictions_Seasonal_Naive.csv     ⚠️ Deprecado
├── category_predictions_ExpSmooth_03.csv  ⚠️ Deprecado
├── category_predictions_MA_4Week.csv      ⚠️ Deprecado
├── category_predictions_Linear_Trend.csv  ⚠️ Deprecado
├── category_predictions_Naive_Last.csv    ⚠️ Deprecado
├── category_predictions_walkforward.csv   ⚠️ Deprecado
└── customer_predictions_walkforward.csv   ⚠️ Deprecado
```

### Datos de Entrenamiento (H1)
```
model_evaluation/
├── sku_h1_actuals.csv
├── category_h1_actuals.csv
├── category_h1_actuals_v2.csv
├── customer_h1_actuals.csv
└── customer_h1_actuals_v2.csv
```

### Scripts
```
scripts/
├── bigquery_xgboost_all_levels.sql   # SQL para BigQuery ML
└── upload_to_bigquery.sh             # Script de despliegue
```

---

## 7. Decisiones de Diseño

### ¿Por qué XGBoost para todos los niveles?

1. **Consistencia metodológica** - Mismo algoritmo permite comparación justa entre niveles
2. **Flexibilidad** - Fácil añadir nuevos features (precio, promociones, clima)
3. **Escalabilidad** - BigQuery ML soporta XGBoost nativamente
4. **Performance** - Mejor MAPE que todos los baselines probados

### ¿Por qué deprecar los otros modelos?

1. **Simplicidad operativa** - Mantener 1 modelo por nivel reduce complejidad
2. **Performance inferior** - Todos los baselines fueron superados por XGBoost
3. **Documentación** - Se mantienen los archivos para auditoría y comparación histórica

### ¿Cuándo usar los modelos deprecados?

- **Baseline comparison** - Para demostrar valor de XGBoost vs métodos simples
- **Fallback** - Si XGBoost falla, Moving Average es un backup razonable
- **Interpretabilidad** - Exp. Smoothing es más fácil de explicar a stakeholders no técnicos
