# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bienvenida 👋
# MAGIC
# MAGIC ## Sesión Express: Lakebase
# MAGIC
# MAGIC **1 hora** · Surface-level tour. Workshop deep-dive unificado a fin de mes (Apps + Agents + Lakebase).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qué van a salir sabiendo
# MAGIC
# MAGIC 1. **Qué es** Lakebase y por qué Databricks lanzó un Postgres gestionado
# MAGIC 2. **Cómo se ve** desde la consola — crear/listar instancias
# MAGIC 3. **Branching, snapshots, scale-to-zero** — los 3 superpoderes principales
# MAGIC 4. **Cuándo usarlo vs Delta** (OLTP vs OLAP)
# MAGIC 5. **Cómo conectar** desde un notebook con psycopg estándar

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda (60 min)
# MAGIC
# MAGIC | Tiempo | Actividad |
# MAGIC |---|---|
# MAGIC | 0–5 | Bienvenida + pre-check |
# MAGIC | 5–25 | **Product Tour** — 16 slides del Lakebase Deck (en español) |
# MAGIC | 25–50 | **LAB Express** — crear/conectar instancia, queries SQL, ver branching |
# MAGIC | 50–60 | Cierre + Workshop preview unificado |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

print("Usuario:", spark.sql("SELECT current_user() AS u").collect()[0]["u"])
print()
print("Lakebase instances en el workspace:")
try:
    instances = list(w.database.list_database_instances())
    if not instances:
        print("  (ninguna todavía — la creamos en el LAB)")
    for inst in instances:
        print(f"  {inst.name:35s}  state={inst.state}  capacity={inst.capacity}")
except Exception as e:
    print(f"  (API requires latest SDK: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuar → `01 - Product Tour (Slides)`

