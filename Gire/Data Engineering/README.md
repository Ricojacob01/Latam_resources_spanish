# Track 1 — Data Engineering 🛠️

De datos crudos a tablas listas para BI/ML usando la **Databricks Data Intelligence Platform**: ingesta con **Auto Loader**, arquitectura **medallion** (bronze → silver → gold), **Spark Declarative Pipelines** (Lakeflow) con **calidad de datos** y **CDC**, y **orquestación con Lakeflow Jobs**.

**Fuentes que adapta:** `SDP Workshop` (pipelines declarativos, expectations, AUTO CDC, dashboard BI) y `lakehouse-iot-plataform_full_spanish` (ingesta incremental + orquestación de turbinas eólicas).

## Módulos

| # | Módulo | Tiempo | Qué haces | Enfoque UI vs Code |
|---|---|---|---|---|
| 00 | **Bienvenida y Agenda** | 5 min | Objetivos, agenda, pre-check | — |
| 01 | **Product Tour (Lakehouse & Lakeflow)** | 20 min | El "por qué" de Lakeflow, Delta, medallion | Conceptual |
| 02 | **LAB Express — Ingesta y Medallion** | 25 min | Auto Loader + bronze/silver/gold en notebook, luego inspección en Catalog Explorer | **Secuencial (Code → UI)** |
| 03 | **LAB Spark Declarative Pipeline (Calidad + CDC)** | 35 min | Defines el pipeline como código SQL y lo creas/ejecutas/monitoreas en la UI de Pipelines | **Code + UI (la definición es código; creación, ejecución y monitoreo de calidad en la UI)** |
| 04 | **LAB Orquestación con Jobs** | 25 min | Construyes el Job en la UI de Jobs, luego lo defines como código (Asset Bundle / JSON / SDK) | **Secuencial (UI → Code)** |
| 05 | **Cierre y Workshop Preview** | 10 min | Recap + qué sigue | — |

## Carpeta `pipelines/`

Contiene las definiciones SQL del Spark Declarative Pipeline que se usan en el módulo 03:

- `orders_pipeline.sql` — medallion de pedidos con expectations (calidad de datos).
- `customers_pipeline.sql` — CDC con `AUTO CDC INTO` (SCD Tipo 1).

## 🧭 Decisiones UI vs Code de este track (resumen)

- **Módulo 02 — Code → UI.** La ingesta incremental se *entiende* mejor escribiendo `read_files(... STREAM ...)` y viendo cómo solo se procesan archivos nuevos; luego abrimos **Catalog Explorer** para ver tablas, esquemas y **lineage** generados — la UI confirma lo que el código produjo.
- **Módulo 03 — Code + UI a la vez.** Un Spark Declarative Pipeline *es* código declarativo (SQL), pero su valor pedagógico está en la **UI de Pipelines**: el grafo de dependencias, el panel de **expectations** (filas válidas/descartadas) y la ejecución incremental. Damos el código y lo operamos en la UI.
- **Módulo 04 — UI → Code.** Un Job se *entiende* visualmente (tareas, dependencias, schedule, reintentos) en la **Jobs UI**; una vez claro, lo volvemos **código** (Databricks Asset Bundle / JSON / SDK) para CI/CD y reproducibilidad.

## Prerrequisitos

- Corre `../00_Setup/00_verify_environment` primero.
- Catálogo `ardemo_classic_dnubtw_catalog`, schema personal `ws_<usuario>`.
- Permiso para crear pipelines y jobs (Lakeflow). Serverless v2.
