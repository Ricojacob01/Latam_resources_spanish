# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour 📊 · Lakehouse & Lakeflow
# MAGIC
# MAGIC ~20 min. Arco narrativo:
# MAGIC
# MAGIC > **El problema** (data engineering tradicional) → **Lakehouse + Delta** → **Medallion** → **Spark Declarative Pipelines** → **Calidad + CDC declarativos** → **Orquestación con Jobs**

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 1 — El problema
# MAGIC
# MAGIC El data engineering tradicional sufre de:
# MAGIC
# MAGIC - **Pipelines frágiles**: código imperativo donde tú gestionas el orden, los checkpoints, los reintentos.
# MAGIC - **Calidad reactiva**: los problemas de datos se descubren *después*, en el dashboard o en el modelo.
# MAGIC - **Dos stacks**: uno para batch, otro para streaming; uno para ETL, otro para orquestación.
# MAGIC - **CDC manual**: lógica de MERGE artesanal para aplicar INSERT/UPDATE/DELETE.
# MAGIC
# MAGIC > La plataforma resuelve esto con **un solo lenguaje declarativo** sobre **Delta Lake**, donde describes *qué* tabla quieres y Databricks gestiona el *cómo*.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Es difícil construir y operar pipelines confiables](../_assets/slides/data_engineering/deckA_problema_pipelines.png)
# MAGIC
# MAGIC *Construir y operar pipelines confiables a mano implica gestionar dependencias, checkpoints, reintentos, backfills, calidad y gobierno — todo código frágil que tú mantienes.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 2 — Lakehouse + Delta Lake 🏞️
# MAGIC
# MAGIC **Delta Lake** es el formato de tabla abierto que da: transacciones ACID, time travel, schema enforcement/evolution, y `MERGE`/CDC sobre data lake barato (object storage).
# MAGIC
# MAGIC El **Lakehouse** = la confiabilidad del warehouse + la flexibilidad y costo del data lake, todo gobernado por **Unity Catalog**.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Plataforma de Inteligencia de Datos](../_assets/slides/data_engineering/deckB_data_intelligence_platform.png)
# MAGIC
# MAGIC *La Databricks Data Intelligence Platform unifica ETL, streaming, warehousing y AI sobre una sola arquitectura Lakehouse.*

# COMMAND ----------

# MAGIC %md
# MAGIC ![Fundación abierta y serverless](../_assets/slides/data_engineering/deckA_open_foundation.png)
# MAGIC
# MAGIC *Construido sobre una base abierta: cómputo serverless gestionado, gobierno unificado (Unity Catalog) y almacenamiento confiable en formato abierto (Delta Lake).*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 3 — Arquitectura Medallion 🥉🥈🥇
# MAGIC
# MAGIC ```
# MAGIC   📥 Archivos raw (JSON/CSV/Parquet en un Volume UC)
# MAGIC            │  read_files() + STREAM   (Auto Loader, incremental)
# MAGIC            ▼
# MAGIC   🥉 BRONZE  ── datos crudos, append-only, con metadata (_metadata.file_name)
# MAGIC            │  limpieza + tipado + EXPECTATIONS (calidad)
# MAGIC            ▼
# MAGIC   🥈 SILVER  ── datos limpios y validados, listos para negocio
# MAGIC            │  agregaciones / joins (MATERIALIZED VIEW)
# MAGIC            ▼
# MAGIC   🥇 GOLD    ── KPIs y tablas analíticas para BI / Genie / ML
# MAGIC ```
# MAGIC
# MAGIC - **Bronze:** `STREAMING TABLE` que ingiere archivos nuevos incrementalmente.
# MAGIC - **Silver:** `STREAMING TABLE` con *expectations* que validan cada fila.
# MAGIC - **Gold:** `MATERIALIZED VIEW` que recalcula agregaciones de forma incremental.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Data Engineering en Databricks](../_assets/slides/data_engineering/deckA_de_architecture.png)
# MAGIC
# MAGIC *Arquitectura de Data Engineering: ingesta → transformación → orquestación, sobre cómputo serverless, gobierno unificado y almacenamiento confiable.*

# COMMAND ----------

# MAGIC %md
# MAGIC ![Pipelines de ETL en producción](../_assets/slides/data_engineering/deckB_production_etl_medallion.png)
# MAGIC
# MAGIC *El flujo bronze → silver → gold se construye como un pipeline de producción con calidad, observabilidad, CI/CD y orquestación integradas.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 4 — Spark Declarative Pipelines (Lakeflow) 🧱
# MAGIC
# MAGIC Antes llamado *Delta Live Tables (DLT)*. Defines tus tablas en SQL o Python y el pipeline:
# MAGIC
# MAGIC - infiere el **grafo de dependencias** (bronze → silver → gold) por las referencias entre tablas;
# MAGIC - gestiona **checkpoints, orden, reintentos y procesamiento incremental**;
# MAGIC - expone un **panel de calidad** (cuántas filas pasaron/fallaron cada expectation);
# MAGIC - corre el mismo código en **batch o streaming**.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC AS SELECT *, _metadata.file_name AS source_file
# MAGIC FROM STREAM read_files("${source}/orders", format => 'json');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ![DLT en resumen](../_assets/slides/data_engineering/deckB_dlt_summarized.png)
# MAGIC
# MAGIC *Delta Live Tables (Lakeflow Declarative Pipelines) es un framework de ETL declarativo: defines las transformaciones y la plataforma gestiona el resto.*

# COMMAND ----------

# MAGIC %md
# MAGIC ![Flujo de DLT](../_assets/slides/data_engineering/deckB_dlt_flow.png)
# MAGIC
# MAGIC *El pipeline infiere el grafo de dependencias entre tablas y procesa los datos de forma incremental, en batch o streaming, con el mismo código.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 5 — Calidad de datos (Expectations) ✅
# MAGIC
# MAGIC La calidad deja de ser reactiva: la declaras junto a la tabla.
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders_clean (
# MAGIC   CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL)        ON VIOLATION FAIL UPDATE,
# MAGIC   CONSTRAINT valid_customer  EXPECT (customer_id IS NOT NULL)     ON VIOLATION DROP ROW,
# MAGIC   CONSTRAINT valid_timestamp EXPECT (order_timestamp > '2020-01-01')
# MAGIC ) AS SELECT ... FROM STREAM bronze_orders;
# MAGIC ```
# MAGIC
# MAGIC | Acción | Qué hace |
# MAGIC |---|---|
# MAGIC | `FAIL UPDATE` | Detiene el pipeline si se viola (datos críticos) |
# MAGIC | `DROP ROW` | Descarta la fila que viola (la cuenta en el panel) |
# MAGIC | *(sin acción)* | Carga la fila pero registra la violación como métrica |

# COMMAND ----------

# MAGIC %md
# MAGIC ![Validación de calidad de datos](../_assets/slides/data_engineering/deckB_data_quality_validation.png)
# MAGIC
# MAGIC *Define controles de calidad e integridad dentro del pipeline con expectations, y resuelve errores con políticas flexibles: fail, drop o alert.*

# COMMAND ----------

# MAGIC %md
# MAGIC ![Expectations - Acciones](../_assets/slides/data_engineering/deckB_expectations_actions.png)
# MAGIC
# MAGIC *Cada expectation puede retener, descartar o fallar las filas que la violan, y todas las métricas quedan registradas en el panel de calidad del pipeline.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 6 — CDC declarativo con `AUTO CDC INTO` 🔄
# MAGIC
# MAGIC Aplica INSERT/UPDATE/DELETE a una tabla destino **sin escribir MERGE a mano**:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FLOW customers_cdc_flow AS
# MAGIC AUTO CDC INTO silver_customers
# MAGIC FROM STREAM bronze_customers_clean
# MAGIC   KEYS (customer_id)
# MAGIC   APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC   SEQUENCE BY timestamp_datetime          -- ordena eventos out-of-order
# MAGIC   COLUMNS * EXCEPT (operation, ...)
# MAGIC   STORED AS SCD TYPE 1;                   -- solo estado actual (TYPE 2 = histórico)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ![CDC declarativo con APPLY CHANGES INTO](../_assets/slides/data_engineering/deckB_cdc_apply_changes.png)
# MAGIC
# MAGIC *La API declarativa de CDC procesa inserts/updates/deletes, maneja eventos fuera de orden, evolución de esquema y SCD tipo 1 o 2 — sin MERGE manual.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 7 — Orquestación con Lakeflow Jobs 🗓️
# MAGIC
# MAGIC Un **Job** encadena tareas (pipeline → query/dashboard → notebook), con **schedule**, **dependencias**, **reintentos** y **alertas** — sin orquestador externo.
# MAGIC
# MAGIC ```
# MAGIC   [Tarea 1: Pipeline SDP]  →  [Tarea 2: Refrescar dashboard]  →  [Tarea 3: Notebook KPIs]
# MAGIC          (cada hora, con reintentos y notificación por email/Slack si falla)
# MAGIC ```
# MAGIC
# MAGIC Lo construyes visual en la **Jobs UI** y luego lo defines como **código** (Databricks Asset Bundle / JSON) para llevarlo a CI/CD.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Workflows: orquestación unificada](../_assets/slides/data_engineering/deckB_workflows_orchestration.png)
# MAGIC
# MAGIC *Databricks Workflows orquesta pipelines, queries, dashboards y notebooks sobre toda la plataforma Lakehouse, sin necesidad de un orquestador externo.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧩 Recap del stack
# MAGIC
# MAGIC ```
# MAGIC Volume UC (raw)
# MAGIC     → [Spark Declarative Pipeline]  bronze → silver(+expectations) → gold(MV)
# MAGIC                                          ↑ AUTO CDC INTO (SCD1)
# MAGIC     → [Lakeflow Job]  orquesta: pipeline + dashboard + KPIs, con schedule y alertas
# MAGIC     → Unity Catalog gobierna todo (permisos, lineage, calidad)
# MAGIC ```
# MAGIC
# MAGIC ## ¿Listo para el hands-on? → `02 - LAB Express - Ingesta y Medallion`
