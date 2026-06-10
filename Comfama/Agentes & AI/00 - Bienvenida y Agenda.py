# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bienvenida 👋
# MAGIC
# MAGIC ## Sesión Express: Playground + AI Gateway
# MAGIC
# MAGIC **Duración:** 1 hora · **Tipo:** Tour express
# MAGIC
# MAGIC ⚠️ Esto es un **surface-level tour**. El workshop deep-dive con hands-on completo es al final del mes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Qué van a salir sabiendo?
# MAGIC
# MAGIC 1. **Qué es** el Playground y para qué sirve (lo van a usar en vivo)
# MAGIC 2. **Qué problema resuelve** AI Gateway (governance + observability + routing de modelos)
# MAGIC 3. **Cómo se ve** Agent Bricks (la forma declarativa de construir agentes en Databricks)
# MAGIC 4. **Vista general** de Foundation Models, Vector Search, Inference Tables — no entramos en detalle, pero saben de su existencia y cuándo usarlos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC | Tiempo | Actividad |
# MAGIC |---|---|
# MAGIC | 0–5 | **Bienvenida** (este notebook) |
# MAGIC | 5–25 | **Product Tour** — slides oficiales (notebook `01`) |
# MAGIC | 25–50 | **LAB Express** — Playground en vivo + notebook con llamadas reales (notebook `02`) |
# MAGIC | 50–60 | **Cierre** — preview workshop del fin de mes + Q&A (notebook `03`) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check rápido

# COMMAND ----------

print("Usuario:", spark.sql("SELECT current_user() AS u").collect()[0]["u"])

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Verifica que los Foundation Models clave estén READY
KEY = ["databricks-meta-llama-3-3-70b-instruct",
       "databricks-claude-sonnet-4-5",
       "databricks-claude-haiku-4-5",
       "databricks-gte-large-en"]

available = {e.name: e for e in w.serving_endpoints.list() if e.name in KEY}
print("\n¿Modelos clave disponibles?")
for m in KEY:
    icon = "✅" if m in available else "❌"
    print(f"  {icon}  {m}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Listo? → continuar con `01 - Product Tour (Slides)`
