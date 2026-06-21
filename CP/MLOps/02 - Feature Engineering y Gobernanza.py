# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Feature Engineering y Gobernanza 🧹🛡️
# MAGIC
# MAGIC Limpiamos y construimos features de churn, dejamos la tabla `mlops_churn_training` y la **gobernamos** en Unity Catalog.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Code (+ inspección UI)**
# MAGIC Las transformaciones son **código** (PySpark/pandas-on-Spark). Al final **inspeccionamos y gobernamos** la tabla en **Catalog Explorer** (comentarios, lineage, permisos): el código *produce*, la UI *gobierna*.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Limpieza + features (código)

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

bronze = spark.table("mlops_churn_bronze_customers")
features = clean_churn_features(bronze)

# split reproducible train/test
features = features.withColumn("split",
    F.when(F.abs(F.hash("customer_id")) % 10 < 8, F.lit("train")).otherwise(F.lit("test")))

features.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_training")
print("✓ mlops_churn_training:", spark.table("mlops_churn_training").count(), "filas")
display(spark.table("mlops_churn_training").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Gobernanza de la tabla (código → UI)
# MAGIC
# MAGIC Documentamos la tabla y la feature derivada, y mostramos el patrón de `GRANT`.

# COMMAND ----------

spark.sql("COMMENT ON TABLE mlops_churn_training IS 'Features de churn listas para entrenamiento (split train/test). Generada por el workshop CP/MLOps.'")
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
