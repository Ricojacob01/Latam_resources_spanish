# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/05_batch_scoring — Tarea del Job
# MAGIC Puntúa el conjunto de inferencia con @Champion y guarda `mlops_churn_predictions`.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Batch scoring (tested)
import subprocess
subprocess.check_call(["pip", "install", "lightgbm", "-q"])

import mlflow

model_name = f"{catalog}.{db}.mlops_churn"

# Load inference data (test split from training table)
inference_df = spark.table("mlops_churn_training").filter("split = 'test'")

# Load Champion model
champion_model = mlflow.pyfunc.load_model(model_uri=f"models:/{model_name}@Champion")
input_cols = champion_model.metadata.get_input_schema().input_names()

# Batch score using pandas
inference_pd = inference_df.toPandas()
inference_pd['churn_prediction'] = champion_model.predict(inference_pd[input_cols])
preds_df = spark.createDataFrame(inference_pd)

preds_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_predictions")
print("✓ batch scoring OK:", spark.table("mlops_churn_predictions").count(), "filas")
