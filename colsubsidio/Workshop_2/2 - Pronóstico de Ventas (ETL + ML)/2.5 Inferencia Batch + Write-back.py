# Databricks notebook source
# MAGIC %md
# MAGIC # 2.5 · Inferencia batch + write-back a SAP HANA
# MAGIC
# MAGIC Generamos el pronóstico con el modelo **`@Champion`** usando **`fe.score_batch`** — que recupera
# MAGIC automáticamente las features del Feature Store por sus claves — y escribimos el resultado **de vuelta a
# MAGIC SAP HANA** para alimentar SAP Analytics Cloud.
# MAGIC
# MAGIC > **Cierre del principio del taller:** los datos (histórico y pronóstico) viven en **SAP HANA**. En
# MAGIC > Databricks quedan únicamente los **activos de ML**: la feature table y el modelo registrado.

# COMMAND ----------

# DBTITLE 1,Dependencias
# MAGIC %pip install --quiet databricks-feature-engineering mlflow scikit-learn --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parámetros
import mlflow
dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")
CATALOGO = dbutils.widgets.get("catalogo").strip()
usuario  = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO   = usuario.split("@")[0].replace(".", "_").replace("-", "_")
FQN      = f"{CATALOGO}.ws2_{SUFIJO}"
FEATURE_TABLE = f"{FQN}.ft_ventas_features"
MODELO        = f"{FQN}.modelo_pronostico_ventas"

mlflow.set_registry_uri("databricks-uc")
print(f"Modelo @Champion: {MODELO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Preparar el spine de scoring
# MAGIC
# MAGIC Para el pronóstico batch necesitamos las **claves** `(producto_familia, fecha)` a puntuar. En este taller
# MAGIC puntuamos los últimos 30 días como demostración; `fe.score_batch` recupera las features automáticamente.

# COMMAND ----------

# DBTITLE 1,Spine de las fechas a pronosticar
from pyspark.sql import functions as F

corte = spark.sql(f"SELECT date_sub(max(fecha), 30) FROM {FEATURE_TABLE}").collect()[0][0]
spine = spark.table(FEATURE_TABLE).where(F.col("fecha") > F.lit(corte)) \
             .select("producto_familia", "fecha", F.col("label_unidades").alias("real"))
print(f"✔ Filas a pronosticar: {spine.count():,}")

# COMMAND ----------

# DBTITLE 1,Puntuar con el modelo @Champion (Feature Store)
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

pronostico = fe.score_batch(
    model_uri=f"models:/{MODELO}@Champion",
    df=spine,
)

pronostico = (
    pronostico
    .withColumnRenamed("prediction", "pronostico")
    .withColumn("pronostico", F.round("pronostico", 0).cast("int"))
    .withColumn("error_abs", F.abs(F.col("real") - F.col("pronostico")))
    .withColumn("generado_por", F.lit("DATABRICKS"))
    .withColumn("modelo", F.lit(MODELO))
    .select("producto_familia", "fecha", "real", "pronostico", "error_abs", "generado_por", "modelo")
)
display(pronostico.orderBy("producto_familia", "fecha").limit(20))

# COMMAND ----------

# DBTITLE 1,Precisión por familia
display(
    pronostico.groupBy("producto_familia").agg(
        F.round(F.avg("error_abs"), 1).alias("error_abs_prom"),
        F.round((1 - F.avg(F.col("error_abs") / F.col("real"))) * 100, 2).alias("precision_pct"),
    ).orderBy(F.col("precision_pct").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Write-back del pronóstico a SAP HANA
# MAGIC
# MAGIC El resultado se escribe de vuelta a SAP HANA (`PRONOSTICO_VENTAS`) con la Databricks Connection — mismo
# MAGIC patrón que los módulos ETL. **El modelo y las features quedan en Databricks; el pronóstico va a HANA.**

# COMMAND ----------

# DBTITLE 1,Write-back JDBC (gated)
ESCRIBIR_A_HANA = False
CONNECTION_NAME = "sap_bw_workshop"
SAP_SCHEMA      = "WORKSHOP"

if ESCRIBIR_A_HANA:
    (pronostico.write.format("jdbc")
        .option("connectionName", CONNECTION_NAME)
        .option("dbtable", f'"{SAP_SCHEMA}"."PRONOSTICO_VENTAS"')
        .option("batchsize", 10000).option("numPartitions", 4)
        .mode("overwrite").save())
    print(f"✔ Escrito a SAP HANA → {SAP_SCHEMA}.PRONOSTICO_VENTAS ({pronostico.count():,} filas)")
else:
    print(f"[SIMULADO] Se escribirían {pronostico.count():,} filas a SAP HANA.PRONOSTICO_VENTAS. "
          f"Pon ESCRIBIR_A_HANA=True para el write-back real.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cierre — hoja de ruta
# MAGIC
# MAGIC | Fase | Alcance |
# MAGIC |------|---------|
# MAGIC | **Fase 1** | Integración as-is: Databricks procesa y escribe de vuelta a SAP HANA |
# MAGIC | **Fase 2** | Feature Store + MLflow + Model Registry + inferencia batch (este módulo) |
# MAGIC | **Fase 3** | **Genie y agentes de Databricks** — preguntas en lenguaje natural sobre el pronóstico |
# MAGIC
# MAGIC **Activos persistidos en Databricks:** la **feature table** y el **modelo registrado** (con alias
# MAGIC `@Champion`). Todos los datos de negocio — histórico y pronóstico — viven en **SAP HANA**.

