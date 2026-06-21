# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour 📊 · MLOps en Databricks
# MAGIC
# MAGIC ~20 min. Arco narrativo:
# MAGIC
# MAGIC > **El problema** (MLOps tradicional) → **MLflow** (tracking) → **AutoML** → **Modelos en Unity Catalog** (Champion/Challenger) → **Validación + Batch** → **(luego) Serving + Jobs**

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 1 — El problema del MLOps
# MAGIC
# MAGIC - Features que no se comparten ni versionan; "funciona en mi notebook".
# MAGIC - Experimentos sin trazabilidad: ¿qué datos, qué params, qué métrica?
# MAGIC - Modelos sin gobernanza: ¿cuál está en prod?, ¿quién lo aprobó?
# MAGIC - Promoción manual y frágil; sin validación automática.
# MAGIC
# MAGIC > Databricks unifica esto: **MLflow** para tracking/registry, **Unity Catalog** para gobernar modelos como cualquier dato, y AutoML/serving/jobs nativos.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 2 — MLflow Tracking 📒
# MAGIC
# MAGIC Cada entrenamiento queda registrado: params, métricas, artefactos, **signature**, y el **dataset** usado (lineage). `mlflow.autolog()` captura casi todo solo.
# MAGIC
# MAGIC ```python
# MAGIC with mlflow.start_run(run_name="lgbm"):
# MAGIC     mlflow.sklearn.autolog()
# MAGIC     model.fit(X_train, y_train)
# MAGIC     mlflow.log_input(dataset, context="training-input")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 3 — AutoML 🤖
# MAGIC
# MAGIC Genera modelos de referencia (baseline) y un **notebook glass-box** del mejor modelo — no es una caja negra. Lo lanzas en la **UI** o por **API**:
# MAGIC
# MAGIC ```python
# MAGIC from databricks import automl
# MAGIC run = automl.classify(dataset=df, target_col="churn", timeout_minutes=10)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 4 — Modelos en Unity Catalog 🏛️
# MAGIC
# MAGIC El modelo se registra como `catalog.schema.modelo` y se gobierna con permisos + lineage. Los **alias** marcan el rol:
# MAGIC
# MAGIC ```python
# MAGIC mlflow.set_registry_uri("databricks-uc")
# MAGIC d = mlflow.register_model(f"runs:/{run_id}/model", f"{catalog}.{schema}.mlops_churn")
# MAGIC client.set_registered_model_alias(name, alias="Challenger", version=d.version)
# MAGIC ```
# MAGIC
# MAGIC - **@Challenger** — candidato a validar.
# MAGIC - **@Champion** — el que sirve producción.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 5 — Validación + Batch inference ✅
# MAGIC
# MAGIC Antes de promover Challenger → Champion, validamos: ¿tiene descripción?, ¿su F1 ≥ Champion?, ¿mejora el impacto de negocio? Si pasa, se promueve.
# MAGIC
# MAGIC ```python
# MAGIC udf = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{schema}.mlops_churn@Champion")
# MAGIC scored = df.withColumn("pred", udf(*udf.metadata.get_input_schema().input_names()))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 6 — Lo que sigue: Serving + Jobs (en CP/MLOps) 🚀
# MAGIC
# MAGIC Este track deja el **Champion listo en UC**. El workshop ampliado `../../CP/MLOps/` lo lleva a producción:
# MAGIC
# MAGIC - **Model Serving**: un endpoint REST en tiempo real (UI + API).
# MAGIC - **Job de orquestación**: train → register → validate → deploy/serve → batch, en schedule, con reintentos.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧩 Recap
# MAGIC
# MAGIC ```
# MAGIC features → AutoML/train (MLflow tracking) → register en UC (@Challenger)
# MAGIC          → validar → promover (@Champion) → batch inference
# MAGIC          → [CP/MLOps] Model Serving endpoint + Job de orquestación
# MAGIC ```
# MAGIC
# MAGIC ## ¿Listo? → `02 - LAB - Del notebook a Unity Catalog`
