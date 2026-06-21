# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Cierre 🎬 · Track MLOps
# MAGIC
# MAGIC **10 min.** Recap + puente al workshop ampliado.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo que construiste hoy
# MAGIC
# MAGIC ✅ Feature engineering reproducible
# MAGIC ✅ AutoML (UI glass-box + API)
# MAGIC ✅ MLflow tracking (autolog, signature, dataset lineage)
# MAGIC ✅ Registro en Unity Catalog + alias **Champion/Challenger**
# MAGIC ✅ Validación del Challenger y **batch inference**
# MAGIC
# MAGIC **Patrón del track:** las transformaciones y la lógica viven en **código**; la UI sirve para **explorar (AutoML, Experiments)** y **gobernar (Models in UC)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚀 El siguiente paso: `../../CP/MLOps/`
# MAGIC
# MAGIC Este track dejó el modelo **Champion** listo en UC. El workshop ampliado lo lleva a producción end-to-end, agregando lo que aquí **no** vimos:
# MAGIC
# MAGIC - **Model Serving** — un endpoint REST en tiempo real, con UI y con API (`mlflow.deployments` / Serving UI).
# MAGIC - **Job de orquestación** — train → register → validate → deploy/serve → batch, en schedule, con reintentos y alertas (Jobs UI + Asset Bundle).
# MAGIC
# MAGIC ## Para profundizar
# MAGIC
# MAGIC - Feature Store / Feature Engineering in UC, online features.
# MAGIC - Model monitoring (drift) con Lakehouse Monitoring + Inference Tables.
# MAGIC - A/B testing y canary en Model Serving.
# MAGIC
# MAGIC ## ¡Gracias! 🎉 — continúa en **CP/MLOps** para el end-to-end con serving + orquestación.
