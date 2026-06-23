# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/01_feature_engineering — Tarea del Job
# MAGIC Versión automatizable del módulo 02. Deja `mlops_churn_training`.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Feature engineering (tested)
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_churn_features(dataDF: DataFrame) -> DataFrame:
  """
  Simple cleaning function leveraging pandas API
  """
  data_psdf = dataDF.pandas_api()
  data_psdf = data_psdf.astype({"senior_citizen": "string"})
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({"1" : "Yes", "0" : "No"})
  data_psdf["total_charges"] = data_psdf["total_charges"].apply(lambda x: float(x) if x.strip() else 0)
  data_psdf = data_psdf.fillna({"tenure": 0.0})
  data_psdf = data_psdf.fillna({"monthly_charges": 0.0})
  data_psdf = data_psdf.fillna({"total_charges": 0.0})

  def sum_optional_services(df):
      cols = ["online_security", "online_backup", "device_protection", "tech_support",
              "streaming_tv", "streaming_movies"]
      return sum(map(lambda c: (df[c] == "Yes"), cols))

  data_psdf["num_optional_services"] = sum_optional_services(data_psdf)
  return data_psdf.to_spark()


# Apply features and split
train_ratio = 0.8
churn_features = clean_churn_features(spark.table("mlops_churn_bronze_customers"))
churn_features = (churn_features.withColumn("random", F.rand(seed=42))
                                .withColumn("split",
                                            F.when(F.col("random") < train_ratio, "train")
                                            .otherwise("test"))
                                .drop("random"))

churn_features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_training")
spark.sql(f"""COMMENT ON TABLE {catalog}.{db}.mlops_churn_training IS 'Features de churn derivadas de mlops_churn_bronze_customers. Pipeline CP/MLOps.'""")
print("✓ feature engineering OK:", spark.table("mlops_churn_training").count(), "filas")
