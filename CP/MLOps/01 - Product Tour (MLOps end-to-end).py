# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour 📊 · MLOps end-to-end
# MAGIC
# MAGIC ~10 min. El ciclo completo, con foco en lo que agregamos: **Serving** y **Orquestación**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## El ciclo MLOps en Databricks
# MAGIC
# MAGIC ```
# MAGIC  ┌──────────────┐   ┌──────────────┐   ┌────────────────────┐   ┌────────────────────┐
# MAGIC  │ 02 Feature   │──►│ 03 AutoML +  │──►│ 04 Registro UC      │──►│ 04 Validar &       │
# MAGIC  │    eng (UC)  │   │   train+MLflow│   │  @Challenger        │   │  promover @Champion│
# MAGIC  └──────────────┘   └──────────────┘   └────────────────────┘   └─────────┬──────────┘
# MAGIC                                                                            │
# MAGIC                          ┌─────────────────────────────────────────────────┴────────────┐
# MAGIC                          ▼                                                                ▼
# MAGIC                 ┌──────────────────┐                                          ┌────────────────────┐
# MAGIC                 │ 05 ⭐ Model       │   tiempo real (REST)                     │ 06 Batch inference │
# MAGIC                 │   Serving endpoint│◄──── apps / servicios                    │  (spark_udf/ai_query)│
# MAGIC                 └──────────────────┘                                          └────────────────────┘
# MAGIC                          ▲
# MAGIC                          │  todo encadenado y agendado por:
# MAGIC                 ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC                 │ 07 ⭐ Lakeflow Job: feature → train → register → validate → deploy →     │
# MAGIC                 │       serve → batch  ·  schedule + reintentos + alertas  ·  como DAB    │
# MAGIC                 └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acto 1 — Por qué falta serving + orquestación
# MAGIC
# MAGIC Un modelo en UC con alias **@Champion** ya es valioso, pero no genera valor hasta que:
# MAGIC
# MAGIC - **Se sirve** (predicciones bajo demanda, baja latencia) → **Model Serving**.
# MAGIC - **Se re-ejecuta solo** (datos nuevos → reentrenar → revalidar → redeploy) → **Jobs**.
# MAGIC
# MAGIC Esto es exactamente lo que agrega este workshop sobre el `ML_workshop` original.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acto 2 — Model Serving 🌐
# MAGIC
# MAGIC Databricks **Model Serving** toma un modelo de UC y lo expone como **endpoint REST** gestionado:
# MAGIC
# MAGIC - **Scale-to-zero** (paga solo cuando hay tráfico), autoscaling.
# MAGIC - Versiones servidas con **traffic split** (A/B, canary).
# MAGIC - **Inference Tables** (loggea cada request para monitoreo).
# MAGIC - Se crea en la **UI** o por **API** (`mlflow.deployments` / `WorkspaceClient`).
# MAGIC
# MAGIC ```python
# MAGIC client = get_deploy_client("databricks")
# MAGIC client.create_endpoint(name=ENDPOINT, config={"served_entities":[{
# MAGIC     "entity_name": MODEL, "entity_version": v,
# MAGIC     "workload_size": "Small", "scale_to_zero_enabled": True}]})
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Acto 3 — Orquestación con Lakeflow Jobs 🗓️
# MAGIC
# MAGIC El pipeline completo como un **Job multi-tarea** con dependencias:
# MAGIC
# MAGIC ```
# MAGIC 01_feature_engineering → 02_train_and_register → 03_validate_and_promote
# MAGIC                                                        ├─► 04_deploy_serving
# MAGIC                                                        └─► 05_batch_scoring
# MAGIC ```
# MAGIC
# MAGIC Con **schedule** (p.ej. semanal), **reintentos**, **alertas** y todo definido como **código** (Databricks Asset Bundle) para CI/CD. Lo construyes en la **Jobs UI** y lo industrializas como DAB en el módulo 07.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Listo? → `02 - Feature Engineering y Gobernanza`
