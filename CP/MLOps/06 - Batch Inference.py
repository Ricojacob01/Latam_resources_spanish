# Databricks notebook source
# DBTITLE 1,Intro with images
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC
# MAGIC # 06 — Batch Inference 📦
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-5-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC Puntuamos un volumen de clientes con el modelo **@Champion**, sin endpoint, directamente en Spark.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Code (+ inspección UI)**
# MAGIC El scoring masivo es **código** (`spark_udf` o `ai_query`); la tabla resultante se explora/gobierna en **Catalog Explorer**.

# COMMAND ----------

# DBTITLE 1,Setup with inference data
# MAGIC %run ./_resources/00-setup $setup_inference_data=true

# COMMAND ----------

# DBTITLE 1,Opcion A header
# MAGIC %md
# MAGIC ## Opción A — Inferencia con `pyfunc` (compatible con Serverless)
# MAGIC
# MAGIC Cargamos el modelo **@Champion** directamente como pyfunc y predecimos en pandas. Este enfoque funciona en Serverless y clusters clásicos.

# COMMAND ----------

# DBTITLE 1,Batch inference with pyfunc
import subprocess
subprocess.check_call(["pip", "install", "lightgbm", "-q"])

import mlflow

# Load customer features to be scored
inference_df = spark.read.table("mlops_churn_inference")

# Load champion model directly (pandas-based prediction for serverless compatibility)
champion_model = mlflow.pyfunc.load_model(model_uri=f"models:/{catalog}.{db}.mlops_churn@Champion")

# Get input column names from model schema
input_cols = champion_model.metadata.get_input_schema().input_names()

# Batch score using pandas
inference_pd = inference_df.toPandas()
inference_pd['predictions'] = champion_model.predict(inference_pd[input_cols])
preds_df = spark.createDataFrame(inference_pd)

# Save predictions table
preds_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_predictions")
print("✓ mlops_churn_predictions:", spark.table("mlops_churn_predictions").count(), "filas")
display(preds_df)

# COMMAND ----------

# DBTITLE 1,Opcion B header
# MAGIC %md
# MAGIC ## Opción B — `spark_udf` (distribuido en cluster clásico)
# MAGIC
# MAGIC Si estás en un cluster clásico (no Serverless), puedes usar `spark_udf` para distribuir la inferencia:
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC udf = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{db}.mlops_churn@Champion", env_manager="virtualenv")
# MAGIC cols = udf.metadata.get_input_schema().input_names()
# MAGIC scored = inference_df.withColumn("churn_prediction", udf(*[c for c in cols]))
# MAGIC ```
# MAGIC
# MAGIC Y también `ai_query` contra el endpoint de Serving (si el endpoint del módulo 05 está **Ready**).

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

# DBTITLE 1,Conclusion
# MAGIC %md
# MAGIC ## Inspección en la UI (🖱️)
# MAGIC
# MAGIC Abre `mlops_churn_predictions` en **Catalog Explorer** → **Sample Data** y **Lineage** (verás el modelo como origen del scoring).
# MAGIC
# MAGIC ¡Eso es todo! Ahora los datos pueden ser reutilizados por el equipo de Análisis de Datos / Marketing para tomar acciones especiales y reducir el riesgo de Churn. ¡Tus datos también estarán disponibles en Genie para responder cualquier pregunta relacionada con churn!
# MAGIC
# MAGIC ## Continuar → `07 - Orquestacion - Job del pipeline ML` ⭐
