<img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png">

# CP — Workshop MLOps end-to-end 🧪🚀

Workshop **hands-on en español** que recorre el ciclo de vida completo de Machine Learning en la Databricks Data Intelligence Platform — **del feature engineering al endpoint en producción, orquestado automáticamente**.

Está basado en `ML_workshop`, **mejorado y limpiado**, y **añade lo que faltaba**:

- ✅ **Model Serving** end-to-end — endpoints de Databricks Model Serving (flujo **UI** + flujo **API/código**). *(El `ML_workshop` original no lo cubría.)*
- ✅ **Job de orquestación** — un Lakeflow Job que corre el pipeline (feature → train → register → validate → **deploy/serve** → batch) **en schedule**, con reintentos y alertas (flujo **Jobs UI** + **Databricks Asset Bundle / JSON**).

Caso de uso: **predicción de churn (abandono) de clientes** de telecom.

## Módulos (didácticos)

| # | Módulo | Qué haces | Enfoque UI vs Code |
|---|---|---|---|
| 00 | **Bienvenida y Agenda** | Objetivos, agenda, pre-check | — |
| 01 | **Product Tour (MLOps end-to-end)** | El ciclo completo y dónde encaja serving + orquestación | Conceptual |
| 02 | **Feature Engineering y Gobernanza** | Features + gobernanza de la tabla en UC | **Code (+ inspección UI)** |
| 03 | **AutoML, Entrenamiento y Tracking** | AutoML + LightGBM + MLflow | **Lado a lado (UI ↔ API)** |
| 04 | **Registro en UC y Champion/Challenger** | `register_model`, alias, validación, promoción | **Code → UI** |
| 05 | **Model Serving + AI Gateway (UI + API)** ⭐ | Crear endpoint REST + gobernanza con AI Gateway | **Secuencial (UI → Code)** |
| 06 | **Batch Inference** | Scoring masivo con `spark_udf` y `ai_query` | **Code (+ inspección UI)** |
| 07 | **Orquestación — Job del pipeline ML** ⭐ | Job multi-tarea con schedule, reintentos, alertas | **Secuencial (UI → Code)** |
| 07b | **Auditoría y Trazabilidad (system tables)** | Queries a `system.access.audit_logs` + lineage | **Lado a lado (Catalog UI ↔ SQL)** |
| 08 | **Cierre y Recap** | Recap + monitoreo + qué sigue | — |

⭐ = lo nuevo respecto a `ML_workshop`.

## Carpetas

- `_resources/00-setup` — setup compartido (catálogo/schema + descarga del dataset IBM Telco Churn). Lo carga cada módulo con `%run`.
- `pipeline/` — **notebooks de tarea** diseñados para automatización (los encadena el Job del módulo 07): feature → train+register → validate+promote → deploy serving → batch scoring.
- `bundle/` — el Job como **código**: `databricks.yml` (Databricks Asset Bundle) y `job.json` (Jobs API 2.1).

## 🧭 Patrón UI vs Code (decisiones, resumen)

Este workshop hace vivir **las dos caras** de Databricks, y cada módulo declara su elección:

- **02 Feature eng — Code (+ UI):** las transformaciones son código; la tabla resultante se inspecciona/gobierna en Catalog Explorer.
- **03 AutoML — Lado a lado:** AutoML en la UI glass-box *y* por API; LightGBM en código con MLflow, comparado en Experiments UI.
- **04 Registro — Code → UI:** registras y asignas alias por API; gobiernas en *Models in Unity Catalog*.
- **07b Auditoría — Lado a lado (Catalog UI ↔ SQL):** descubres las system tables en Catalog Explorer y respondes "quién hizo qué" en SQL sobre `system.access.audit_logs` + lineage.
- **05 Model Serving — UI → Code:** primero creas y pruebas el endpoint en la **Serving UI** (intuición: estado, scale-to-zero, query panel), luego lo creas/consultas/actualizas por **API** para automatizar.
- **06 Batch — Code (+ UI):** `spark_udf`/`ai_query` para puntuar; tabla en Catalog Explorer.
- **07 Orquestación — UI → Code:** armas el Job multi-tarea en la **Jobs UI** (grafo, schedule, reintentos), luego lo defines como **Asset Bundle / JSON** para CI/CD. El Job reutiliza los notebooks de `pipeline/`.

## Prerrequisitos

- **Serverless** o un cluster con **ML Runtime**. El setup instala automáticamente las librerías necesarias vía `%pip install`.
- Catálogo `ardemo_classic_dnubtw_catalog`, schema personal `ws_<usuario>` (se crea solo).
- Permiso para crear endpoints de Model Serving y Jobs.

## Mejoras respecto a `ML_workshop`

1. Estructura unificada estilo workshop (Bienvenida → Tour → LABs → Cierre) y narrativa en español consistente.
2. Setup self-contained con el dataset **IBM Telco Churn** (descarga automática).
3. **Modelo servido** en un endpoint REST (faltaba).
4. **Pipeline orquestado** como Job + Asset Bundle (faltaba).
5. Nota explícita **UI vs Code** por módulo.
6. Módulos de **gobernanza/trazabilidad** (`04b` no-training, `07b` auditoría con system tables) que cierran gaps de la scorecard de proveedores de IA.
