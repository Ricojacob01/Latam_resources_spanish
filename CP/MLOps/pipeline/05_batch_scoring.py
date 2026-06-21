# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/05_batch_scoring — Tarea del Job
# MAGIC Puntúa el conjunto de inferencia con @Champion y guarda `mlops_churn_predictions`.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

import mlflow
udf = mlflow.pyfunc.spark_udf(spark, f"models:/{MODEL_NAME}@Champion", env_manager="virtualenv")
cols = udf.metadata.get_input_schema().input_names()

(spark.table("mlops_churn_training").filter("split = 'test'")
   .withColumn("churn_prediction", udf(*[c for c in cols]))
   .write.mode("overwrite").option("overwriteSchema", "true")
   .saveAsTable("mlops_churn_predictions"))

print("✓ batch scoring OK:", spark.table("mlops_churn_predictions").count(), "filas")
