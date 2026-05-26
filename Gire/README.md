<img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png">

# GIRE x Databricks — Workshop Hands-On

Entrenamiento práctico de la **Databricks Data Intelligence Platform**.

El flujo va de menor a mayor complejidad técnica: empezamos con **Genie** (lenguaje natural para usuarios de negocio), seguimos con **AI Functions** (SQL), llegamos a **Agent Bricks**, y terminamos con el track de **Machine Learning** completo.

## Catálogo y schema (obligatorio leer antes de empezar)

Todos los notebooks usan el mismo patrón:

- **Catálogo compartido**: `ardemo_classic_dnubtw_catalog`
- **Schema personal por usuario**: `ws_<usuario>` (ej. `ws_juan_perez`)

La primera celda **Setup del lab** de cada notebook valida acceso y crea tu schema si no existe — no necesitas configurar nada manualmente.

## Agenda

| # | Lab | Tiempo | Tema |
| -- | -- | -- | -- |
| 00 | **Setup** | 5 min | Verificar entorno y permisos |
| 01 | **Genie y Apps** | 45 min | NLQ sobre datos + Databricks Apps |
| 02 | **AI Functions** | 30 min | `ai_query`, `ai_classify`, `ai_extract` en SQL |
| 03 | **Agent Bricks** | 45 min | Knowledge Assistant con Vector Search |
| 04 | **ML en Databricks** | 90 min | Feature Engineering → AutoML → UC → Batch Inference |

## Estructura del repositorio

```
Gire/
├── 00_Setup/
│   └── 00_verify_environment            ← EMPIEZA AQUI
├── Lab_01_Genie_y_Apps/
│   ├── 01_Introduccion_Apps_y_Genie
│   ├── 02_Crear_Genie_y_Preguntas
│   └── 03_App_Streamlit_Actualizar_Inventario
├── Lab_02_AI_Functions/
│   └── 01_ai_functions_sql
├── Lab_03_Agent_Bricks/
│   └── 01_knowledge_assistant
└── Lab_04_ML_en_Databricks/
    ├── _resources/
    │   └── 00-setup                     ← cargado por cada notebook ML via %run
    ├── 00_presentacion_ml
    ├── 01_feature_engineering
    ├── 02_autoML
    ├── 03_models_in_uc
    ├── 04_challenger_validation
    ├── 05_batch_inference
    └── _opcional/
        ├── train_lightGBM
        └── deploy_kubernetes
```

## Prerrequisitos

- Acceso al workspace de Databricks (Gire)
- Catálogo `ardemo_classic_dnubtw_catalog` con permisos `CREATE SCHEMA` y `CREATE TABLE`
- Cluster Serverless Environment v2 (Labs 01–03)
- Cluster clásico `ml_workshop_databricks` para los notebooks de ML (Lab 04)

> Ejecuta primero `00_Setup/00_verify_environment` para validar todo lo anterior automáticamente.

## Material de referencia

- [Genie](https://docs.databricks.com/aws/en/genie/)
- [AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions)
- [Agent Bricks](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/)
- [MLflow](https://docs.databricks.com/aws/en/mlflow/)
- [AutoML](https://docs.databricks.com/aws/en/machine-learning/automl/)
