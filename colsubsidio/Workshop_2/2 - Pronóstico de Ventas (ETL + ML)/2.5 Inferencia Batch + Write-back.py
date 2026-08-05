# Databricks notebook source
# MAGIC %md
# MAGIC # 2.5 · Inferencia batch + write-back a SAP HANA
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-4-v2.png?raw=true" width="1000">
# MAGIC
# MAGIC Último paso del ciclo: **poner el modelo a trabajar**. Generamos el pronóstico con el modelo
# MAGIC **`@Champion`** usando **`fe.score_batch`** y entregamos el resultado a los consumidores de negocio.
# MAGIC
# MAGIC ## 📘 Por qué `fe.score_batch` en lugar de `model.predict`
# MAGIC
# MAGIC `fe.score_batch` es la pieza que cierra el círculo del Feature Store:
# MAGIC
# MAGIC 1. Le pasamos **solo las claves** `(producto_familia, fecha)` a puntuar — **no** las features.
# MAGIC 2. El Feature Store **recupera automáticamente** las features de la tabla `ft_ventas_features`, usando
# MAGIC    exactamente la **misma definición** con la que se entrenó (recuerda: el modelo se registró con su
# MAGIC    `training_set` en 2.3).
# MAGIC 3. Aplica el modelo `@Champion` y devuelve las predicciones.
# MAGIC
# MAGIC Esto **elimina el training-serving skew** por diseño: es imposible que la inferencia use una lógica de
# MAGIC features distinta a la del entrenamiento, porque ambas leen de la misma feature table. Además, al
# MAGIC referenciar `@Champion` (no un número de versión), este notebook usa siempre el modelo en producción sin
# MAGIC modificaciones.
# MAGIC
# MAGIC > **Principio del taller:** los **datos de negocio** (histórico y pronóstico) pertenecen a **SAP HANA**.
# MAGIC > En Databricks solo persisten los **activos de ML** (la feature table y el modelo). En el laboratorio,
# MAGIC > como no hay un HANA conectado, escribimos el pronóstico a una tabla del catálogo para que puedas ver
# MAGIC > el resultado; el bloque final muestra el **write-back real a SAP HANA** que se usaría en producción.

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
# MAGIC ## 2. Entregar el pronóstico
# MAGIC
# MAGIC Dos rutas, según el entorno:
# MAGIC
# MAGIC * **Laboratorio (celda A):** escribimos a una tabla del catálogo para poder inspeccionar el resultado,
# MAGIC   ya que no hay un SAP HANA conectado.
# MAGIC * **Producción (celda B, gated):** el write-back **real a SAP HANA** vía JDBC — la misma Databricks
# MAGIC   Connection de los módulos ETL. Aquí es donde el pronóstico "regresa" a HANA para alimentar SAP
# MAGIC   Analytics Cloud, respetando el principio de no dejar datos de negocio en Databricks.

# COMMAND ----------

# DBTITLE 1,A) Write-back al catálogo (laboratorio)
# En el laboratorio escribimos a UC para poder ver el resultado (no hay HANA conectado).
TABLA_PRONOSTICO = f"{FQN}.pronostico_ventas_lab"

(pronostico.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLA_PRONOSTICO))

print(f"✔ [LAB] Escrito a Unity Catalog → {TABLA_PRONOSTICO} ({pronostico.count():,} filas)")

# COMMAND ----------

# DBTITLE 1,B) Write-back real a SAP HANA (producción — gated)
# En producción, el pronóstico se escribe DE VUELTA a SAP HANA (no se queda en Databricks).
ESCRIBIR_A_HANA = False               # ← True cuando exista la conexión SAP HANA
CONNECTION_NAME = "sap_bw_workshop"
SAP_SCHEMA      = "WORKSHOP"

if ESCRIBIR_A_HANA:
    (pronostico.write.format("jdbc")
        .option("connectionName", CONNECTION_NAME)
        .option("dbtable", f'"{SAP_SCHEMA}"."PRONOSTICO_VENTAS"')
        .option("batchsize", 10000).option("numPartitions", 4)
        .mode("overwrite").save())
    print(f"✔ [PROD] Escrito a SAP HANA → {SAP_SCHEMA}.PRONOSTICO_VENTAS ({pronostico.count():,} filas)")
else:
    print(f"[SIMULADO] Se escribirían {pronostico.count():,} filas a SAP HANA.PRONOSTICO_VENTAS. "
          f"Pon ESCRIBIR_A_HANA=True cuando exista la conexión.")

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
# MAGIC
# MAGIC ## 🔁 Cómo aplicar este marco a otros modelos
# MAGIC
# MAGIC La inferencia batch con `fe.score_batch` es idéntica para cualquier modelo registrado con Feature Store.
# MAGIC Para productivizar otro modelo:
# MAGIC
# MAGIC 1. Prepara el **spine** con las claves a puntuar (los afiliados del mes, las empresas activas, etc.).
# MAGIC 2. Llama `fe.score_batch(model_uri="models:/<modelo>@Champion", df=spine)`.
# MAGIC 3. Escribe el resultado **de vuelta a SAP HANA** con el mismo patrón JDBC.
# MAGIC 4. **Automatízalo** en un Databricks Job programado (diario, mensual…) que encadene:
# MAGIC    features → (reentrenamiento opcional) → validación/promoción → inferencia → write-back.
# MAGIC
# MAGIC Ese Job es la versión productiva de los notebooks 2.1–2.5. La **misma plantilla** sirve para todos los
# MAGIC modelos de Colsubsidio — solo cambian el spine, el nombre del modelo y la tabla destino en HANA.
