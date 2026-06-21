# Track 3 — MLOps 🧪

El ciclo de vida de ML en Databricks: **feature engineering → AutoML → entrenamiento → registro en Unity Catalog (Champion/Challenger) → validación → batch inference**, todo con MLflow y gobernado por Unity Catalog.

**Fuentes que adapta:** `ML_workshop` (pipeline de churn con MLflow + UC) y `lakehouse-iot-plataform_full_spanish` (ML/GenAI de turbinas: AutoML API, inferencia).

> ➡️ Este track es la **introducción** al ciclo ML. Para el flujo **end-to-end con Model Serving (endpoints) + un Job de orquestación automática**, ve al workshop ampliado en [`../../CP/MLOps/`](../../CP/MLOps/).

## Módulos

| # | Módulo | Tiempo | Qué haces | Enfoque UI vs Code |
|---|---|---|---|---|
| 00 | **Bienvenida y Agenda** | 5 min | Objetivos, agenda, pre-check | — |
| 01 | **Product Tour (MLOps en Databricks)** | 20 min | El ciclo ML y el rol de MLflow + UC | Conceptual |
| 02 | **LAB — Del notebook a Unity Catalog** | 70 min | Feature eng → AutoML → train → registro UC → Champion/Challenger → batch | Mixto (ver abajo) |
| 05 | **Cierre y Workshop Preview** | 10 min | Recap + puente a CP/MLOps | — |

## Carpeta `labs/ml_en_databricks/`

El pipeline ML completo (reutilizado del repo), que el módulo 02 enmarca:

- `00_presentacion_ml` · `01_feature_engineering` · `02_autoML` · `03_train_lightGBM`
- `04_models_in_uc` · `05_challenger_validation` · `06_batch_inference`
- `_resources/00-setup` (cargado por `%run`) · `_opcional/` (LightGBM, Kubernetes)

## 🧭 Decisiones UI vs Code de este track (resumen)

El módulo 02 recorre el pipeline mezclando ambos planos, deliberadamente por sub-tarea:

- **Feature engineering — Code.** Transformaciones en PySpark/pandas; lo natural es código.
- **AutoML — Lado a lado.** Lanzas AutoML en la **UI (glass-box)** *y* con la **API** (`databricks.automl.classify`). Misma capacidad, dos puertas: la UI para explorar, la API para reproducir.
- **Registro en UC — Code → UI.** Registras el modelo con `mlflow.register_model` y asignas alias por **código**; luego lo ves/gobiernas en **Models in Unity Catalog (UI)** (versiones, alias, lineage, permisos).
- **Challenger validation — Code.** Lógica de promoción (comparar F1, métricas de negocio) en código, pero los resultados se inspeccionan en la **UI del modelo** (tags, descripciones).
- **Batch inference — Code, inspección UI.** `spark_udf`/pandas para puntuar; la tabla resultante se explora en Catalog Explorer.

> El **Model Serving** (endpoints, UI + API) y la **orquestación con Jobs** se cubren a fondo en `../../CP/MLOps/` — este track deja el modelo listo en UC para ese siguiente paso.

## Prerrequisitos

- Corre `../00_Setup/00_verify_environment`.
- Cluster clásico **`ml_workshop_databricks`** (no Serverless) para los notebooks de ML.
- Catálogo `ardemo_classic_dnubtw_catalog`, schema `ws_<usuario>`.
