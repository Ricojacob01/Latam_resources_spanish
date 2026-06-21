# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/01_feature_engineering — Tarea del Job
# MAGIC Versión automatizable del módulo 02. Deja `mlops_churn_training`.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def clean_churn_features(dataDF: DataFrame) -> DataFrame:
    p = dataDF.pandas_api()
    p = p.astype({"senior_citizen": "string"})
    p["senior_citizen"] = p["senior_citizen"].map({"1": "Yes", "0": "No"})
    p["total_charges"] = p["total_charges"].apply(lambda x: float(x) if str(x).strip() else 0.0)
    p = p.fillna({"tenure": 0.0, "monthly_charges": 0.0, "total_charges": 0.0})
    opt = ["online_security", "online_backup", "device_protection",
           "tech_support", "streaming_tv", "streaming_movies"]
    p["num_optional_services"] = sum((p[c] == "Yes").astype("int") for c in opt)
    return p.to_spark()

feats = (clean_churn_features(spark.table("mlops_churn_bronze_customers"))
         .withColumn("split", F.when(F.abs(F.hash("customer_id")) % 10 < 8, F.lit("train"))
                                .otherwise(F.lit("test"))))
feats.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_training")
spark.sql("COMMENT ON TABLE mlops_churn_training IS 'Features de churn (pipeline CP/MLOps).'")
print("✓ feature engineering OK:", spark.table("mlops_churn_training").count(), "filas")
