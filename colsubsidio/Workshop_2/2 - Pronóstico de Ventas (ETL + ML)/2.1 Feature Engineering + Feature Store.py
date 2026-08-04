# Databricks notebook source
# MAGIC %md
# MAGIC # 2.1 · Ingeniería de Características + Feature Store
# MAGIC ### Pronóstico de ventas — Retail Farma (Colsubsidio)
# MAGIC
# MAGIC Primer paso del flujo de ML: construir las **features** y guardarlas en el **Databricks Feature Store**
# MAGIC (Feature Engineering in Unity Catalog). Seguimos el flujo de MLOps de referencia:
# MAGIC
# MAGIC **2.1 Features (este notebook)** → 2.2 AutoML → 2.3 Entrenamiento → 2.4 Registro en UC → 2.5 Inferencia batch
# MAGIC
# MAGIC ### Sobre el principio "sin almacenamiento en Databricks"
# MAGIC
# MAGIC > Los **datos de los casos de uso siguen en SAP HANA**. Lo que sí vive en Databricks son los **activos del
# MAGIC > ciclo de vida de ML**: la **tabla de features** (gobernada en Unity Catalog, con linaje) y, más adelante,
# MAGIC > el **modelo registrado**. Es exactamente la excepción acordada: *Databricks persiste solo lo de ML*.

# COMMAND ----------

# DBTITLE 1,Dependencias
# MAGIC %pip install --quiet databricks-feature-engineering mlflow --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parámetros — catálogo y esquema del usuario
dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")

CATALOGO = dbutils.widgets.get("catalogo").strip()
usuario  = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO   = usuario.split("@")[0].replace(".", "_").replace("-", "_")
ESQUEMA  = f"ws2_{SUFIJO}"
FQN      = f"{CATALOGO}.{ESQUEMA}"

FEATURE_TABLE = f"{FQN}.ft_ventas_features"   # tabla de features en UC

spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")
print(f"Origen (stand-in SAP HANA): {FQN}")
print(f"Feature table (UC)        : {FEATURE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Leer datos de origen desde SAP HANA
# MAGIC
# MAGIC Igual que en los módulos ETL: en producción se lee desde SAP HANA vía la Databricks Connection; en el
# MAGIC laboratorio leemos las tablas sintéticas equivalentes.

# COMMAND ----------

# DBTITLE 1,Lectura ventas + datos externos
LEER_DESDE_HANA = False
CONNECTION_NAME = "sap_bw_workshop"
SAP_SCHEMA      = "WORKSHOP"

def leer_base(tabla_hana, tabla_lab):
    if LEER_DESDE_HANA:
        df = (spark.read.format("jdbc")
              .option("connectionName", CONNECTION_NAME)
              .option("dbtable", f'"{SAP_SCHEMA}"."{tabla_hana}"')
              .load())
        return df.toDF(*[c.lower() for c in df.columns])
    return spark.table(f"{FQN}.{tabla_lab}")

ventas   = leer_base("VENTAS_HISTORICO", "sap_ventas_historico")
externos = leer_base("DATOS_EXTERNOS",   "sap_datos_externos")
print(f"✔ ventas   : {ventas.count():,} filas")
print(f"✔ externos : {externos.count():,} filas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lógica de ingeniería de características
# MAGIC
# MAGIC Encapsulamos la creación de features en una **función** (buena práctica: reutilizable en entrenamiento
# MAGIC e inferencia y fácil de testear). Señales:
# MAGIC
# MAGIC * **Lags** — ventas de 1, 7, 14 y 28 días atrás por familia
# MAGIC * **Medias móviles** — 7 y 28 días
# MAGIC * **Calendario** — día de la semana, mes, día del año, fin de semana, temporada decembrina
# MAGIC * **Macro** — inflación, TRM, tasa de interés, ICC (join por periodo)
# MAGIC
# MAGIC La clave primaria de la tabla de features será `(producto_familia, fecha)`.

# COMMAND ----------

# DBTITLE 1,Función de featurización
from pyspark.sql import functions as F, DataFrame
from pyspark.sql.window import Window

def construir_features(ventas: DataFrame, externos: DataFrame) -> DataFrame:
    """Construye features de pronóstico. PK: (producto_familia, fecha)."""
    w = Window.partitionBy("producto_familia").orderBy("fecha")
    return (
        ventas
        .join(externos, on="periodo", how="left")
        .withColumn("lag_1",  F.lag("unidades", 1).over(w))
        .withColumn("lag_7",  F.lag("unidades", 7).over(w))
        .withColumn("lag_14", F.lag("unidades", 14).over(w))
        .withColumn("lag_28", F.lag("unidades", 28).over(w))
        .withColumn("ma_7",  F.avg("unidades").over(w.rowsBetween(-7, -1)))
        .withColumn("ma_28", F.avg("unidades").over(w.rowsBetween(-28, -1)))
        .withColumn("dow",   F.dayofweek("fecha"))
        .withColumn("mes",   F.month("fecha"))
        .withColumn("doy",   F.dayofyear("fecha"))
        .withColumn("es_finde",      F.when(F.dayofweek("fecha").isin(1, 7), 1).otherwise(0))
        .withColumn("es_decembrina", F.when(F.dayofyear("fecha").between(330, 360), 1).otherwise(0))
        .filter(F.col("lag_28").isNotNull() & F.col("ma_28").isNotNull())
        .select(
            "producto_familia", "fecha", "periodo",
            "lag_1", "lag_7", "lag_14", "lag_28", "ma_7", "ma_28",
            "dow", "mes", "doy", "es_finde", "es_decembrina",
            "ipc_inflacion_anual", "trm", "tasa_interes", "icc_confianza_consumidor",
            F.col("unidades").alias("label_unidades"),
        )
    )

features = construir_features(ventas, externos)
print(f"✔ Filas con features completas: {features.count():,}")
display(features.orderBy("producto_familia", "fecha").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Guardar en el Feature Store (Unity Catalog)
# MAGIC
# MAGIC Usamos el **`FeatureEngineeringClient`** para crear una tabla de features gobernada en Unity Catalog.
# MAGIC Ventajas frente a un simple `saveAsTable`:
# MAGIC
# MAGIC * **Clave primaria declarada** → habilita `FeatureLookup` en entrenamiento e inferencia
# MAGIC * **Linaje** entre features, modelos y datos de origen (visible en Catalog Explorer)
# MAGIC * **Reutilización** de features entre equipos y casos de uso
# MAGIC * Base para **feature serving** online (batch y tiempo real)

# COMMAND ----------

# DBTITLE 1,Crear/actualizar la Feature Table
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Idempotente: si existe, la sobreescribimos con merge; si no, la creamos.
existe = spark.catalog.tableExists(FEATURE_TABLE)
if not existe:
    fe.create_table(
        name=FEATURE_TABLE,
        primary_keys=["producto_familia", "fecha"],
        df=features,
        description="Features de pronóstico de ventas por familia de producto y fecha. "
                    "Derivadas de sap_ventas_historico + sap_datos_externos (origen SAP HANA).",
    )
    print(f"✔ Feature table creada: {FEATURE_TABLE}")
else:
    fe.write_table(name=FEATURE_TABLE, df=features, mode="merge")
    print(f"✔ Feature table actualizada (merge): {FEATURE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Verificar la tabla de features
display(spark.sql(f"SELECT COUNT(*) AS filas, COUNT(DISTINCT producto_familia) AS familias FROM {FEATURE_TABLE}"))
display(spark.table(FEATURE_TABLE).orderBy("producto_familia", "fecha").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧪 Ejercicio — Añade una feature de calendario
# MAGIC
# MAGIC Agrega a `construir_features` una columna `inicio_mes` (1 si el día es de los primeros 5 del mes, 0 si no)
# MAGIC — útil porque muchas compras institucionales se concentran a comienzo de mes. Vuelve a ejecutar y
# MAGIC actualiza la feature table.
# MAGIC
# MAGIC <details><summary>💡 Pista</summary>
# MAGIC
# MAGIC ```python
# MAGIC .withColumn("inicio_mes", F.when(F.dayofmonth("fecha") <= 5, 1).otherwise(0))
# MAGIC # recuerda añadirla al .select(...) también
# MAGIC ```
# MAGIC </details>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Ya tenemos la **feature table** gobernada en Unity Catalog. Continúa con:
# MAGIC
# MAGIC * **2.2 AutoML** — genera un modelo baseline automáticamente
# MAGIC * **2.3 Entrenamiento** — entrena usando la feature table con `FeatureLookup` y MLflow

