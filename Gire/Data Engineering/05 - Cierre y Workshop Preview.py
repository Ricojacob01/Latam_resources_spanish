# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Cierre 🎬 · Track Data Engineering
# MAGIC
# MAGIC **10 min.** Recap + qué sigue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo que construiste hoy
# MAGIC
# MAGIC | Módulo | Logro | UI vs Code |
# MAGIC |---|---|---|
# MAGIC | 02 | Ingesta incremental (Auto Loader) + medallion | Code → UI |
# MAGIC | 03 | Spark Declarative Pipeline con expectations + CDC | Code + UI |
# MAGIC | 04 | Orquestación con Jobs (UI → Asset Bundle) | UI → Code |
# MAGIC
# MAGIC **Patrón del track:** la **UI** construye intuición y gobierna; el **código** reproduce e industrializa. Cada tarea la viviste de las dos formas, a propósito.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo conecta con los otros tracks
# MAGIC
# MAGIC - 🥇 Las tablas **gold** que produjiste son la entrada de **Agents and Governance** (Genie/AI Functions) y de **MLOps** (features).
# MAGIC - El **dashboard + Genie** sobre `gold_order_summary` es la puerta al track de Agents.
# MAGIC - El patrón de **Jobs** se reutiliza en MLOps para orquestar entrenamiento → registro → serving (ver `../../CP/MLOps/`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para profundizar (deep-dive)
# MAGIC
# MAGIC - Lakeflow Connect (ingesta gestionada desde SaaS/DBs), Auto Loader avanzado (schema evolution, rescued data).
# MAGIC - SCD **Tipo 2** (histórico), expectations cuarentena, *flows* múltiples sobre una tabla.
# MAGIC - DABs en CI/CD (GitHub Actions), tests de calidad como gate del pipeline.
# MAGIC - Lakehouse Monitoring sobre las tablas gold.
# MAGIC
# MAGIC ## Cleanup (opcional)
# MAGIC ```python
# MAGIC # spark.sql(f"DROP TABLE IF EXISTS gold_order_summary")
# MAGIC # ... y borra el pipeline + job desde la UI cuando termines.
# MAGIC ```
# MAGIC
# MAGIC ## ¡Gracias! 🎉 — sigue con el track **Agents and Governance** o **MLOps**.
