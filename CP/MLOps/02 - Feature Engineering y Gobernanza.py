# Databricks notebook source
# DBTITLE 1,Intro
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC
# MAGIC # 02 — Feature Engineering y Gobernanza 🧹🛡️
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-1-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC Limpiamos y construimos features de churn, dejamos la tabla `mlops_churn_training` y la **gobernamos** en Unity Catalog.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Code (+ inspección UI)**
# MAGIC Las transformaciones son **código** (PySpark/pandas-on-Spark). Al final **inspeccionamos y gobernamos** la tabla en **Catalog Explorer** (comentarios, lineage, permisos): el código *produce*, la UI *gobierna*.
# MAGIC
# MAGIC ### Usando la API de Pandas en Spark
# MAGIC Usamos la [API de pandas en spark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) para escalar el código de `pandas`. Las instrucciones de Pandas se convertirán en el motor de spark internamente y se distribuirán a escala.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Limpieza + features (código)

# COMMAND ----------

# DBTITLE 1,Feature engineering - clean and write
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_churn_features(dataDF: DataFrame) -> DataFrame:
  """
  Simple cleaning function leveraging pandas API
  """
  # Convert to pandas on spark dataframe
  data_psdf = dataDF.pandas_api()
  # Convert some columns
  data_psdf = data_psdf.astype({"senior_citizen": "string"})
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({"1" : "Yes", "0" : "No"})

  data_psdf["total_charges"] = data_psdf["total_charges"].apply(lambda x: float(x) if x.strip() else 0)

  # Fill some missing numerical values with 0
  data_psdf = data_psdf.fillna({"tenure": 0.0})
  data_psdf = data_psdf.fillna({"monthly_charges": 0.0})
  data_psdf = data_psdf.fillna({"total_charges": 0.0})

  def sum_optional_services(df):
      """Count number of optional services enabled, like streaming TV"""
      cols = ["online_security", "online_backup", "device_protection", "tech_support",
              "streaming_tv", "streaming_movies"]
      return sum(map(lambda c: (df[c] == "Yes"), cols))

  data_psdf["num_optional_services"] = sum_optional_services(data_psdf)

  # Return the cleaned Spark dataframe
  return data_psdf.to_spark()


# Leer tabla bronze y aplicar features
telcoDF = spark.table("mlops_churn_bronze_customers")
churn_features = clean_churn_features(telcoDF)

# Specify train-test split
train_ratio, test_ratio = 0.8, 0.2
churn_features = (churn_features.withColumn("random", F.rand(seed=42))
                                .withColumn("split",
                                            F.when(F.col("random") < train_ratio, "train")
                                            .otherwise("test"))
                                .drop("random"))

# Write table for training
(churn_features.write.mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable("mlops_churn_training"))

print("✓ mlops_churn_training:", spark.table("mlops_churn_training").count(), "filas")
display(spark.table("mlops_churn_training").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Gobernanza de la tabla (código → UI)
# MAGIC
# MAGIC Documentamos la tabla y la feature derivada, y mostramos el patrón de `GRANT`.

# COMMAND ----------

# DBTITLE 1,Governance - comments and grants
# Add comment to the table
spark.sql(f"""COMMENT ON TABLE {catalog}.{db}.mlops_churn_training IS 'The features in this table are derived from the mlops_churn_bronze_customers table in the lakehouse. We created service features and cleaned up their names. No aggregations were performed.'""")
spark.sql("ALTER TABLE mlops_churn_training ALTER COLUMN num_optional_services COMMENT 'Cantidad de servicios opcionales contratados (feature derivada)'")
spark.sql("ALTER TABLE mlops_churn_training ALTER COLUMN churn COMMENT 'Etiqueta objetivo: Yes/No'")

# Ejemplo de GRANT (descomenta y ajusta el grupo):
# spark.sql("GRANT SELECT ON TABLE mlops_churn_training TO `analysts`")
print("✓ Comentarios aplicados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Inspección en la UI (🖱️)
# MAGIC
# MAGIC 1. **Catalog → `ardemo_classic_dnubtw_catalog` → tu schema → `mlops_churn_training`**.
# MAGIC 2. **Overview**: comentarios de tabla y columnas que acabas de poner.
# MAGIC 3. **Lineage**: verás que proviene de `mlops_churn_bronze_customers`.
# MAGIC 4. **Permissions**: aquí gestionarías el `GRANT` con clicks.
# MAGIC
# MAGIC ## Continuar → `03 - AutoML, Entrenamiento y Tracking`
