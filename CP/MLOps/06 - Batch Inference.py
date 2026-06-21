# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Batch Inference 📦
# MAGIC
# MAGIC Puntuamos un volumen de clientes con el modelo **@Champion**, sin endpoint, directamente en Spark.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Code (+ inspección UI)**
# MAGIC El scoring masivo es **código** (`spark_udf` o `ai_query`); la tabla resultante se explora/gobierna en **Catalog Explorer**.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción A — `spark_udf` (modelo Python distribuido)

# COMMAND ----------

import mlflow
udf = mlflow.pyfunc.spark_udf(spark, f"models:/{MODEL_NAME}@Champion", env_manager="virtualenv")
cols = udf.metadata.get_input_schema().input_names()

inference_df = spark.table("mlops_churn_training").filter("split = 'test'")
scored = inference_df.withColumn("churn_prediction", udf(*[c for c in cols]))
scored.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_predictions")

print("✓ mlops_churn_predictions:", spark.table("mlops_churn_predictions").count(), "filas")
display(spark.table("mlops_churn_predictions").select("customer_id", "churn", "churn_prediction").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción B — `ai_query` contra el endpoint de Serving (SQL)
# MAGIC
# MAGIC Si el endpoint del módulo 05 está **Ready**, puedes puntuar desde SQL llamando al endpoint. Útil cuando el consumidor es un analista en SQL.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC SELECT *,
# MAGIC   ai_query(
# MAGIC     'mlops_churn_<tu_slug>',                 -- nombre del endpoint (ver SERVING_ENDPOINT)
# MAGIC     named_struct(
# MAGIC       'gender', gender, 'senior_citizen', senior_citizen, 'tenure', tenure,
# MAGIC       'contract', contract, 'monthly_charges', monthly_charges,
# MAGIC       'total_charges', total_charges, 'num_optional_services', num_optional_services
# MAGIC       /* ... resto de columnas ... */
# MAGIC     )
# MAGIC   ) AS prediction
# MAGIC FROM mlops_churn_training WHERE split = 'test'
# MAGIC ```
# MAGIC (Ajusta el nombre del endpoint y las columnas a la signature del modelo.)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspección en la UI (🖱️)
# MAGIC
# MAGIC Abre `mlops_churn_predictions` en **Catalog Explorer** → **Sample Data** y **Lineage** (verás el modelo como origen del scoring).
# MAGIC
# MAGIC ## Continuar → `07 - Orquestacion - Job del pipeline ML` ⭐
