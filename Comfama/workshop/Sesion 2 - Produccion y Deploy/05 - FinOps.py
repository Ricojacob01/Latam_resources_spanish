# Databricks notebook source
# MAGIC %md
# MAGIC # 💰 Sesión 2 · 05 — FinOps (costos del agente)
# MAGIC
# MAGIC **Meta:** entender y controlar el **costo** del agente con **System Tables** (DBUs de serving y Lakebase), el
# MAGIC **usage tracking del AI Gateway** (tokens) y la **Budget API**.
# MAGIC
# MAGIC > **Equivale a: `FinOpsAnalyzer`.** En vez de un analizador de costos custom, las System Tables ya tienen el dato
# MAGIC > gobernado y consultable con SQL.
# MAGIC
# MAGIC Módulo **dual-mode**: explorar costos **🖱️ en System Tables / Account console** o **⌨️ por SQL/API**.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Costo por SKU (System Tables) — `system.billing.usage`
# MAGIC Filtramos el consumo de **Model Serving** y **Lakebase** de los últimos 14 días.

# COMMAND ----------

display(spark.sql("""
  SELECT usage_date,
         sku_name,
         ROUND(SUM(usage_quantity), 2) AS dbus
  FROM system.billing.usage
  WHERE usage_date >= current_date() - INTERVAL 14 DAYS
    AND (sku_name ILIKE '%SERVING%' OR sku_name ILIKE '%MODEL%'
         OR sku_name ILIKE '%LAKEBASE%' OR sku_name ILIKE '%POSTGRES%'
         OR sku_name ILIKE '%VECTOR%')
  GROUP BY usage_date, sku_name
  ORDER BY usage_date DESC, dbus DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Costo en $ — unir con `system.billing.list_prices`

# COMMAND ----------

display(spark.sql("""
  WITH u AS (
    SELECT sku_name, SUM(usage_quantity) AS dbus
    FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 30 DAYS
      AND (sku_name ILIKE '%SERVING%' OR sku_name ILIKE '%LAKEBASE%'
           OR sku_name ILIKE '%POSTGRES%' OR sku_name ILIKE '%VECTOR%')
    GROUP BY sku_name
  )
  SELECT u.sku_name, ROUND(u.dbus,2) AS dbus,
         ROUND(u.dbus * p.price_usd, 2) AS usd_estimado
  FROM u
  LEFT JOIN (
     SELECT sku_name, MAX(CAST(pricing.default AS DOUBLE)) AS price_usd
     FROM system.billing.list_prices GROUP BY sku_name
  ) p ON p.sku_name = u.sku_name
  ORDER BY usd_estimado DESC NULLS LAST
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Tokens del agente — usage tracking del AI Gateway
# MAGIC El AI Gateway (módulo 06 de S1) escribe **inference tables** con tokens por request. Así medimos costo por token
# MAGIC y volumen real de uso del agente.

# COMMAND ----------

# Buscar la inference table del gateway (prefijo 'gw_agente' del módulo 06)
tablas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()]
gw = [t for t in tablas if t.startswith("gw_agente")]
print("Inference tables del gateway:", gw or "(aún no generadas — requiere tráfico tras configurar el Gateway)")
if gw:
    display(spark.sql(f"""
      SELECT date(request_time) AS dia, count(*) AS requests
      FROM {CATALOG}.{SCHEMA}.{gw[0]}
      GROUP BY 1 ORDER BY 1 DESC LIMIT 14
    """))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — Budgets
# MAGIC 1. **Account console** (admin) → **Usage** → **Budgets** → **Create budget**.
# MAGIC 2. Filtra por tag/SKU del agente, define monto mensual y destinatarios de alerta de presupuesto.
# MAGIC
# MAGIC > Etiqueta los recursos del agente (endpoint, app, Lakebase) con un **tag** común (p.ej. `proyecto=agente-comfama`)
# MAGIC > para que el budget y las System Tables los agrupen automáticamente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — Budget API (ejemplo)
# MAGIC ```python
# MAGIC from databricks.sdk import AccountClient
# MAGIC a = AccountClient()
# MAGIC a.budgets.create(budget={
# MAGIC   "name": "agente-comfama-mensual",
# MAGIC   "filter": {"tags": [{"key": "proyecto", "value": {"operator":"IN","values":["agente-comfama"]}}]},
# MAGIC   "period": "MONTH", "amount": "500",
# MAGIC   "alerts": [{"email_notifications": ["finops@comfama.com"], "percent_threshold": 80}]
# MAGIC })
# MAGIC ```
# MAGIC > Requiere credenciales de **account** (no workspace). Se incluye como patrón para CI/CD (módulo 06).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC Costo del agente visible y controlable: DBUs por SKU, $ estimado, tokens del Gateway y presupuestos con alerta —
# MAGIC con el dato gobernado en System Tables, sin analizador de costos custom.
# MAGIC
# MAGIC ### ▶️ Siguiente: `06 - Deploy-as-Code para su framework`

