# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea de Job — Refrescar KPIs 🗓️
# MAGIC
# MAGIC Este notebook es la **segunda tarea** del Job del módulo `04`. Corre *después* del pipeline
# MAGIC y materializa una tabla de KPIs de negocio sobre las tablas gold que produjo el pipeline.

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"Refrescando KPIs en {CATALOG}.{SCHEMA}")

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE gold_kpis_diarios AS
SELECT
  current_timestamp()                AS refreshed_at,
  count(*)                           AS dias_con_pedidos,
  sum(total_daily_orders)            AS pedidos_totales,
  round(avg(total_daily_orders), 1)  AS promedio_pedidos_dia,
  round(sum(daily_revenue), 2)       AS ingresos_totales
FROM gold_order_summary
""")

display(spark.table("gold_kpis_diarios"))
print("✅ KPIs refrescados")
