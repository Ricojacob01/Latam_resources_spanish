# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Cierre y Recap 🎬 · Workshop MLOps end-to-end
# MAGIC
# MAGIC **10 min.** Lo que construiste y qué sigue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## El ciclo completo que recorriste
# MAGIC
# MAGIC | Módulo | Logro | UI vs Code |
# MAGIC |---|---|---|
# MAGIC | 02 | Feature engineering + gobernanza UC | Code (+ UI) |
# MAGIC | 03 | AutoML + LightGBM + MLflow | Lado a lado |
# MAGIC | 04 | Registro UC + Champion/Challenger | Code → UI |
# MAGIC | 05 | ⭐ **Model Serving** (endpoint REST) | UI → Code |
# MAGIC | 06 | Batch inference | Code (+ UI) |
# MAGIC | 07 | ⭐ **Orquestación** (Job + Asset Bundle) | UI → Code |
# MAGIC
# MAGIC ⭐ = lo que agregamos respecto al `ML_workshop` original.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo nuevo, en una frase
# MAGIC
# MAGIC - **Model Serving:** el Champion ya no vive solo en UC — está **servido** como endpoint REST con scale-to-zero, consultable desde apps, SQL (`ai_query`) o `curl`.
# MAGIC - **Orquestación:** el pipeline corre **solo** (feature → train → register → validate → deploy → batch), con un **gate de validación** que impide publicar un modelo malo, agendado y como **código** (DAB) para CI/CD.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoreo (siguiente paso natural)
# MAGIC
# MAGIC - **Inference Tables** del endpoint → cada request loggeado en Delta.
# MAGIC - **Lakehouse Monitoring** sobre esa tabla → detectar **drift** de datos/predicciones.
# MAGIC - Si hay drift → el **Job** del módulo 07 reentrena y revalida → cierra el ciclo MLOps.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (opcional, tras el workshop)

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# Descomenta para limpiar tu endpoint y tablas (NO borres durante el lab).
# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# try:
#     w.serving_endpoints.delete(SERVING_ENDPOINT)
#     print("Endpoint borrado:", SERVING_ENDPOINT)
# except Exception as e:
#     print(e)
# spark.sql("DROP TABLE IF EXISTS mlops_churn_predictions")
print("Cleanup disponible arriba (comentado). ¡Gracias! 🎉")
