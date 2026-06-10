# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — FinOps con System Tables
# MAGIC
# MAGIC **Reemplaza**: `observability/FinOpsAnalyzer` (costos DBU custom)
# MAGIC
# MAGIC ## ¿Qué hace su código actual?
# MAGIC
# MAGIC En `comfama-ai-core/observability/finops.py`:
# MAGIC - `FinOpsAnalyzer` que estima costos DBU desde tracking interno
# MAGIC - Anotación "se modifica solo en primer step" (asume tracking incompleto)
# MAGIC - `get_solution_cost_summary()` con cálculos manuales
# MAGIC - Tabla custom para histórico de costos
# MAGIC
# MAGIC ## ¿Qué hace Databricks?
# MAGIC
# MAGIC **System Tables** — tablas nativas con todo el consumo:
# MAGIC - `system.billing.usage` — cada DBU consumida, por workspace, por SKU, por cluster, por user
# MAGIC - `system.compute.clusters` — config y costo de cada cluster
# MAGIC - `system.query.history` — cada query SQL ejecutada
# MAGIC - `system.lakeflow.jobs` — runs de jobs y su consumo
# MAGIC - `system.serving.endpoint_usage` — costo de Model Serving / FM APIs
# MAGIC
# MAGIC **Budget Policies API** — límites y alertas automáticas sobre consumo.
# MAGIC
# MAGIC **Tiempo estimado:** 8 min

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar acceso a System Tables

# COMMAND ----------

display(spark.sql("""
SELECT
  table_catalog,
  table_schema,
  table_name
FROM system.information_schema.tables
WHERE table_catalog = 'system'
  AND table_schema IN ('billing', 'compute', 'query', 'lakeflow', 'serving', 'access')
ORDER BY table_schema, table_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consumo total del workspace en últimos 7 días

# COMMAND ----------

display(spark.sql("""
SELECT
  date(usage_start_time) AS dia,
  SUM(usage_quantity) AS dbus_total,
  ROUND(SUM(usage_quantity) * 0.55, 2) AS costo_usd_aprox  -- precio aproximado (varía)
FROM system.billing.usage
WHERE workspace_id = (SELECT current_metastore())
  AND usage_start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY dia
ORDER BY dia DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Consumo por SKU (qué producto Databricks gasta más)

# COMMAND ----------

display(spark.sql("""
SELECT
  sku_name,
  SUM(usage_quantity) AS dbus_total,
  ROUND(SUM(usage_quantity) * 100.0 /
        SUM(SUM(usage_quantity)) OVER (), 2) AS porcentaje
FROM system.billing.usage
WHERE usage_start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY sku_name
HAVING dbus_total > 0
ORDER BY dbus_total DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Consumo por usuario (top spenders)

# COMMAND ----------

display(spark.sql("""
SELECT
  identity_metadata.run_as AS run_as_user,
  SUM(usage_quantity) AS dbus,
  COUNT(DISTINCT date(usage_start_time)) AS dias_activos,
  COUNT(*) AS records
FROM system.billing.usage
WHERE usage_start_time >= current_date() - INTERVAL 7 DAYS
  AND identity_metadata.run_as IS NOT NULL
GROUP BY identity_metadata.run_as
ORDER BY dbus DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Costos del Model Serving específicamente (donde corre el agente)

# COMMAND ----------

# Costo de endpoints de Model Serving en última semana
display(spark.sql("""
SELECT
  date(usage_start_time) AS dia,
  custom_tags AS tags,
  SUM(usage_quantity) AS dbus,
  COUNT(*) AS records
FROM system.billing.usage
WHERE sku_name LIKE '%SERVING%'
  AND usage_start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY dia, custom_tags
ORDER BY dia DESC, dbus DESC
LIMIT 30
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Endpoint usage detail (Model Serving + FM APIs)

# COMMAND ----------

# Detalle de uso por endpoint
try:
    display(spark.sql("""
    SELECT
      date(usage_date) AS dia,
      endpoint_name,
      served_entity_name,
      SUM(request_count) AS requests,
      SUM(usage_quantity) AS dbus
    FROM system.serving.endpoint_usage
    WHERE usage_date >= current_date() - INTERVAL 7 DAYS
    GROUP BY dia, endpoint_name, served_entity_name
    ORDER BY dia DESC, requests DESC
    LIMIT 30
    """))
except Exception as e:
    print(f"system.serving.endpoint_usage no disponible aún en este workspace: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Query History — qué SQL queries gastan más

# COMMAND ----------

display(spark.sql("""
SELECT
  user_identity.email AS user_email,
  warehouse_id,
  COUNT(*) AS total_queries,
  ROUND(AVG(total_duration_ms), 0) AS avg_duration_ms,
  SUM(read_bytes) / 1e9 AS gb_read,
  SUM(written_bytes) / 1e9 AS gb_written
FROM system.query.history
WHERE start_time >= current_date() - INTERVAL 1 DAYS
GROUP BY user_identity.email, warehouse_id
ORDER BY total_queries DESC
LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Forecasting — Cost forecast con built-in `ai_forecast`
# MAGIC
# MAGIC Databricks expone una función `ai_forecast` para predecir consumo futuro.

# COMMAND ----------

try:
    display(spark.sql("""
    WITH daily AS (
      SELECT date(usage_start_time) AS ds, SUM(usage_quantity) AS y
      FROM system.billing.usage
      WHERE usage_start_time >= current_date() - INTERVAL 90 DAYS
      GROUP BY ds
    )
    SELECT * FROM ai_forecast(TABLE(daily), 'ds', 'y', 14)  -- forecast 14 días
    """))
except Exception as e:
    print(f"ai_forecast no disponible en este workspace: {e}")
    print("Alternativa: usar Prophet / ARIMA en una notebook")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Budget Policies (asignar tags + límites)
# MAGIC
# MAGIC La API de Budget permite definir presupuestos y disparar alertas/bloqueos automáticos.

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Listar budget policies existentes
try:
    policies = list(w.budget_policy.list())
    print(f"Budget policies existentes: {len(policies)}")
    for p in policies[:5]:
        print(f"  {p.policy_id}: {p.policy_name}")
except Exception as e:
    print(f"Budget policies (puede requerir admin): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Crear una budget policy para el agente Comfama (ejemplo)
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk.service.billing import BudgetPolicy, CustomPolicyTag
# MAGIC
# MAGIC w.budget_policy.create(
# MAGIC     policy=BudgetPolicy(
# MAGIC         policy_name="comfama-agente-prod",
# MAGIC         custom_tags=[
# MAGIC             CustomPolicyTag(key="proyecto", value="comfama_agente"),
# MAGIC             CustomPolicyTag(key="entorno", value="prod"),
# MAGIC         ],
# MAGIC     ),
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC Luego, en `app.yaml` o en cluster config, agregar los tags:
# MAGIC ```yaml
# MAGIC custom_tags:
# MAGIC   proyecto: comfama_agente
# MAGIC   entorno: prod
# MAGIC ```
# MAGIC
# MAGIC Todos los costos asociados se agruparan automáticamente bajo esta policy.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Dashboard de FinOps (ejemplo de query para Lakeview)
# MAGIC
# MAGIC Esta query es el insumo para un Lakeview dashboard "FinOps Comfama":

# COMMAND ----------

dashboard_query = """
SELECT
  date(usage_start_time) AS dia,
  sku_name,
  custom_tags.proyecto AS proyecto,
  custom_tags.entorno AS entorno,
  SUM(usage_quantity) AS dbus,
  ROUND(SUM(usage_quantity) * 0.55, 2) AS costo_usd_aprox
FROM system.billing.usage
WHERE usage_start_time >= current_date() - INTERVAL 30 DAYS
GROUP BY dia, sku_name, proyecto, entorno
ORDER BY dia DESC
"""
print("Pegar en SQL Editor + crear Lakeview Dashboard:")
print(dashboard_query)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Comparación side-by-side
# MAGIC
# MAGIC | Capacidad | Su código actual | Databricks System Tables |
# MAGIC |---|---|---|
# MAGIC | Tracking de DBU | Estimación manual + tabla custom | `system.billing.usage` (oficial) |
# MAGIC | Granularidad | Lo que decidan instrumentar | Por workspace, sku, cluster, user, tag, hora |
# MAGIC | Costo Model Serving | No tienen | `system.serving.endpoint_usage` |
# MAGIC | Query cost | No tienen | `system.query.history` con bytes leídos |
# MAGIC | Job cost | No tienen | `system.lakeflow.jobs` |
# MAGIC | Forecast | Implementar (Prophet) | `ai_forecast()` SQL function |
# MAGIC | Budget enforcement | (no tienen) | Budget Policies API + tags |
# MAGIC | Dashboard | Construir en PowerBI | Lakeview sobre las mismas tablas |
# MAGIC | **Setup** | Tabla custom + cron + estimaciones | **Tablas managed, cero setup** |
# MAGIC | **Source of truth** | Estimación interna | **Datos oficiales de billing** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¡Fin del demo!
# MAGIC
# MAGIC ## Recap de lo que reemplazamos
# MAGIC
# MAGIC | Notebook | Capa Comfama reemplazada | LOC eliminadas (aprox.) |
# MAGIC |---|---|---|
# MAGIC | `02 - Observability` | `TelemetryManager` + OTLP + Prometheus + `@instrument` | ~600 |
# MAGIC | `03 - Governance` | `AuthManager` + `SecretProvider` + audit manual | ~400 |
# MAGIC | `04 - Monitoring` | Monitoreo de drift custom | ~250 |
# MAGIC | `05 - SQL Alerts` | `AlertEvaluator` + `AlertProtocols` | ~300 |
# MAGIC | `06 - FinOps` | `FinOpsAnalyzer` | ~150 |
# MAGIC | **Total** | **Capa transversal** | **~1,700 LOC** |
# MAGIC
# MAGIC Más todo lo que está en el notebook `00 - Architecture Overview` (Agent Framework, Vector Search, Lakebase, AI Gateway, Apps, MCP) que adicionalmente reemplazan ~2,000 LOC más + 6 servicios Azure.
# MAGIC
# MAGIC ## Próximos pasos para Comfama
# MAGIC
# MAGIC 1. **Workshop hands-on** con su equipo de plataforma (1-2h)
# MAGIC 2. **PoC con 1 agente real** migrado a Mosaic AI Agent Framework
# MAGIC 3. **Validación de Lakebase** vs Cosmos para estado conversacional
# MAGIC 4. **Definir DABs** como pipeline de promoción dev → stg → prod
# MAGIC 5. **Activar AI Gateway** sobre los modelos para guardrails + observability uniformes

