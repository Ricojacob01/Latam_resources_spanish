# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bienvenida 👋
# MAGIC
# MAGIC ## Sesión Express: Databricks Apps
# MAGIC
# MAGIC **1 hora** · Surface-level tour. Workshop deep-dive unificado (Apps + Agents + Lakebase) a fin de mes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué van a salir sabiendo
# MAGIC
# MAGIC 1. **Qué resuelve** Databricks Apps (vs el Container Apps de su stack actual)
# MAGIC 2. **Cómo se ve** un app desplegado (vamos a tocar el del agente Comfama)
# MAGIC 3. **Cómo Apps integra** con Model Serving, SQL Warehouses, Unity Catalog, Secrets
# MAGIC 4. **Pricing + best practices** generales

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda (60 min)
# MAGIC
# MAGIC | Tiempo | Actividad |
# MAGIC |---|---|
# MAGIC | 0–5 | Bienvenida + pre-check |
# MAGIC | 5–25 | **Product Tour** — 17 slides del Apps Product Deck |
# MAGIC | 25–50 | **LAB Express** — Visitar `comfama-agente-app`, inspeccionar config, ver logs |
# MAGIC | 50–60 | Cierre + Workshop preview |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

print("Usuario:", spark.sql("SELECT current_user() AS u").collect()[0]["u"])
print()
print("Apps en el workspace:")
response = w.api_client.do("GET", "/api/2.0/apps")
for a in response.get("apps", []):
    state = a.get("compute_status", {}).get("state", "?")
    print(f"  {a['name']:35s}  {state}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuar → `01 - Product Tour (Slides)`
