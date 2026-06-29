# Databricks notebook source
# MAGIC %md
# MAGIC # 💰 Sesión 2 · 05 — FinOps (costos del agente)
# MAGIC
# MAGIC **Meta:** entender, **visualizar** y controlar el **costo** del agente con **System Tables** (DBUs de serving y
# MAGIC Lakebase), el **usage tracking del AI Gateway** (tokens), un **dashboard AI/BI + Genie Space** (creados por *prompt*)
# MAGIC y la **Budget API**.
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
# MAGIC ## 📊 Visualizar: dashboard AI/BI + Genie Space (por *prompt*)
# MAGIC Persistimos los resultados como **vistas** estables y luego generamos un **prompt** (con **las pestañas que tú
# MAGIC defines**) para crear el **dashboard AI/BI** y un **Genie Space** sin escribir SQL a mano.

# COMMAND ----------

# Vistas FinOps estables — las consumen el dashboard y Genie
FILTRO = ("sku_name ILIKE '%SERVING%' OR sku_name ILIKE '%MODEL%' OR sku_name ILIKE '%LAKEBASE%' "
          "OR sku_name ILIKE '%POSTGRES%' OR sku_name ILIKE '%VECTOR%'")

spark.sql(f"""CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.finops_dbus_por_sku AS
  SELECT sku_name, ROUND(SUM(usage_quantity),2) AS dbus
  FROM system.billing.usage
  WHERE usage_date >= current_date() - INTERVAL 30 DAYS AND ({FILTRO})
  GROUP BY sku_name""")

spark.sql(f"""CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.finops_dbus_diario AS
  SELECT usage_date, ROUND(SUM(usage_quantity),2) AS dbus
  FROM system.billing.usage
  WHERE usage_date >= current_date() - INTERVAL 30 DAYS AND ({FILTRO})
  GROUP BY usage_date""")

spark.sql(f"""CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.finops_costo_usd AS
  WITH u AS (
    SELECT sku_name, SUM(usage_quantity) AS dbus FROM system.billing.usage
    WHERE usage_date >= current_date() - INTERVAL 30 DAYS AND ({FILTRO}) GROUP BY sku_name),
  p AS (SELECT sku_name, MAX(CAST(pricing.default AS DOUBLE)) AS price_usd
        FROM system.billing.list_prices GROUP BY sku_name)
  SELECT u.sku_name, ROUND(u.dbus,2) AS dbus, ROUND(u.dbus*p.price_usd,2) AS usd_estimado
  FROM u LEFT JOIN p ON p.sku_name=u.sku_name""")

print(f"✅ Vistas creadas en {CATALOG}.{SCHEMA}: finops_dbus_por_sku, finops_dbus_diario, finops_costo_usd")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define las pestañas → genera el prompt del dashboard
# MAGIC Edita `DASHBOARD_TABS` a tu gusto; el prompt se arma **solo** con esas pestañas (eso es "pasar las pestañas").

# COMMAND ----------

DASHBOARD_TABS = [
    {"pestaña": "Resumen",          "contenido": "tarjetas (counters) con el total de USD estimado y el total de DBUs de los últimos 30 días"},
    {"pestaña": "Costo por SKU",    "contenido": "gráfico de barras de usd_estimado por sku_name, ordenado de mayor a menor"},
    {"pestaña": "Tendencia diaria", "contenido": "gráfico de línea de dbus por usage_date"},
    {"pestaña": "Uso del agente",   "contenido": "requests por día del agente desde su inference table (creada por agents.deploy)"},
]

TABLAS = (f"{CATALOG}.{SCHEMA}.finops_costo_usd, "
          f"{CATALOG}.{SCHEMA}.finops_dbus_diario, "
          f"{CATALOG}.{SCHEMA}.finops_dbus_por_sku")

prompt_dashboard = (
    "Crea un dashboard AI/BI de FinOps para el 'Agente de Servicios al Afiliado Comfama'.\n"
    f"Usa como fuentes estas vistas: {TABLAS}.\n"
    "Crea una pestaña por cada ítem de esta lista (título — contenido):\n"
    + "\n".join(f"  {i+1}. {t['pestaña']} — {t['contenido']}" for i, t in enumerate(DASHBOARD_TABS))
    + "\nFormatea los importes como moneda USD, ordena de mayor a menor costo y usa títulos en español."
)
print(prompt_dashboard)

# COMMAND ----------

# MAGIC %md
# MAGIC **🖱️ Cómo usarlo (dashboard):** menú **Dashboards** → **Create dashboard** → abre el **Assistant** (✨) en el
# MAGIC editor → **pega el prompt** de arriba → genera. Ajusta las visualizaciones y **Publish**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prompt para un Genie Space (preguntas de costo en lenguaje natural)

# COMMAND ----------

prompt_genie = (
    "Crea un Genie Space de FinOps del agente Comfama sobre estas vistas: "
    f"{TABLAS}.\n"
    "Instrucciones: responde en español. 'costo' = columna usd_estimado; 'consumo' = dbus. "
    "Trata serving, Lakebase y Vector Search como componentes del agente.\n"
    "Preguntas de ejemplo: ¿cuál es el SKU más caro del último mes? · "
    "¿cómo evolucionó el consumo diario de DBUs? · ¿cuánto cuesta el serving del agente?"
)
print(prompt_genie)

# COMMAND ----------

# MAGIC %md
# MAGIC **🖱️ Cómo usarlo (Genie):** menú **Genie** → **New** → selecciona las vistas `finops_*` de tu schema → pega las
# MAGIC **instrucciones** de arriba en *Instructions* → guarda. Ya puedes preguntarle en español.
# MAGIC
# MAGIC > ⌨️ *Alternativa por código:* también se puede generar el dashboard programáticamente con la **Lakeview API**
# MAGIC > (widgets desde las vistas `finops_*`). Avísame si lo quieres como celda ejecutable.

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
# MAGIC Costo del agente visible y controlable: DBUs por SKU, $ estimado, tokens del Gateway, un **dashboard AI/BI** y un
# MAGIC **Genie Space** (creados por prompt desde las vistas `finops_*`) y presupuestos con alerta — con el dato gobernado
# MAGIC en System Tables, sin analizador de costos custom.
# MAGIC
# MAGIC ### ▶️ Siguiente: `06 - Deploy-as-Code para su framework`

