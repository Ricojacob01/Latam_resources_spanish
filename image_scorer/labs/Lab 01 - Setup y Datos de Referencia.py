# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On LAB 01 - Setup y Datos de Referencia
# MAGIC
# MAGIC Entrenamiento Hands-on en la plataforma de Databricks con foco en **Image Scoring con Vector Search**.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos del Ejercicio
# MAGIC
# MAGIC El objetivo de este laboratorio es:
# MAGIC - Configurar el ambiente de trabajo (catalogo, esquema, volumen)
# MAGIC - Crear la tabla Delta de referencia con displays etiquetados
# MAGIC - Entender la arquitectura del sistema de puntuacion
# MAGIC
# MAGIC ### Arquitectura
# MAGIC
# MAGIC El patron que implementaremos es: **Model Serving + Delta + Vector Search (Standard)**
# MAGIC
# MAGIC 1. **Tabla Delta** con imagenes de referencia etiquetadas (displays conocidos con puntuaciones)
# MAGIC 2. **Model Serving** para generar embeddings de imagenes (CLIP)
# MAGIC 3. **Vector Search** con Delta Sync + self-managed embeddings para busqueda por similitud
# MAGIC 4. **Puntuacion** basada en los vecinos mas cercanos recuperados
# MAGIC
# MAGIC Este es un patron de **recuperacion en tiempo real**, ideal para Vector Search.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion
# MAGIC
# MAGIC Para ejecutar los ejercicios, necesitamos conectar este notebook a un cluster/computo.
# MAGIC
# MAGIC Simplemente siga los pasos a continuacion:
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el cluster: **Serverless**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01 - Configuracion del ambiente

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Importaciones y parametros

# COMMAND ----------

import json
import requests
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType, IntegerType, ArrayType, DoubleType
)

# COMMAND ----------

# DBTITLE 1,Parametros del workshop
# -- Catalogo y esquema --
CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "image_score"
VOLUME = "archivos"

# -- Rutas --
PATH_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
PATH_TABLE = f"{CATALOG}.{SCHEMA}"

# -- Vector Search --
VS_ENDPOINT_NAME = "image-scorer-vs-endpoint"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.displays_index"

# -- Model Serving --
EMBEDDING_ENDPOINT = "image-embedding-endpoint"
EMBEDDING_DIM = 512

# -- Parametros de puntuacion --
TOP_K = 5
SCORE_WEIGHT_NEIGHBORS = 0.7
SCORE_WEIGHT_MODEL = 0.3

print(f"Catalogo: {CATALOG}")
print(f"Esquema: {SCHEMA}")
print(f"Indice VS: {VS_INDEX_NAME}")
print(f"TOP_K: {TOP_K}")
print(f"Peso vecinos: {SCORE_WEIGHT_NEIGHBORS} | Peso modelo: {SCORE_WEIGHT_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Creacion del esquema y volumen

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"Schema '{CATALOG}.{SCHEMA}' y volumen '{VOLUME}' creados/verificados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Crear la tabla Delta de referencia
# MAGIC
# MAGIC Nuestra tabla de referencia contiene displays de productos **ya etiquetados** por expertos. Cada registro incluye:
# MAGIC
# MAGIC | Campo | Descripcion |
# MAGIC |-------|-------------|
# MAGIC | `display_id` | Identificador unico del display |
# MAGIC | `image_url` | URL o ruta de la imagen del display |
# MAGIC | `brand` | Marca del producto exhibido |
# MAGIC | `store_type` | Tipo de tienda (supermercado, conveniencia, etc.) |
# MAGIC | `region` | Region geografica |
# MAGIC | `ideal_score` | Puntuacion ideal del display (0-100) |
# MAGIC | `compliance_score` | Puntuacion de cumplimiento (0-100) |
# MAGIC | `quality_label` | Etiqueta de calidad (excelente, bueno, regular, deficiente) |
# MAGIC
# MAGIC **Nota importante:** Para imagenes, almacenamos la URL/ruta de la imagen, **no** el binario crudo. Los embeddings se calcularan en el siguiente laboratorio.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Definir el esquema de la tabla

# COMMAND ----------

display_schema = StructType([
    StructField("display_id", IntegerType(), False),
    StructField("image_url", StringType(), False),
    StructField("brand", StringType(), True),
    StructField("store_type", StringType(), True),
    StructField("region", StringType(), True),
    StructField("ideal_score", FloatType(), True),
    StructField("compliance_score", FloatType(), True),
    StructField("quality_label", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Cargar datos de referencia
# MAGIC
# MAGIC Vamos a crear un conjunto de datos de ejemplo con displays de productos etiquetados.
# MAGIC En un escenario real, estos datos vendrian de auditorias de campo con imagenes reales.

# COMMAND ----------

# DBTITLE 1,Datos de referencia de displays etiquetados
reference_data = [
    # -- Displays excelentes --
    (1, f"{PATH_VOLUME}/displays/display_001.jpg", "Coca-Cola", "supermercado", "CDMX", 95.0, 92.0, "excelente"),
    (2, f"{PATH_VOLUME}/displays/display_002.jpg", "Coca-Cola", "conveniencia", "Monterrey", 90.0, 88.0, "excelente"),
    (3, f"{PATH_VOLUME}/displays/display_003.jpg", "PepsiCo", "supermercado", "Guadalajara", 93.0, 90.0, "excelente"),
    (4, f"{PATH_VOLUME}/displays/display_004.jpg", "Nestle", "supermercado", "CDMX", 91.0, 89.0, "excelente"),
    (5, f"{PATH_VOLUME}/displays/display_005.jpg", "Bimbo", "conveniencia", "Puebla", 88.0, 91.0, "excelente"),
    # -- Displays buenos --
    (6, f"{PATH_VOLUME}/displays/display_006.jpg", "Coca-Cola", "supermercado", "Monterrey", 78.0, 75.0, "bueno"),
    (7, f"{PATH_VOLUME}/displays/display_007.jpg", "PepsiCo", "conveniencia", "CDMX", 80.0, 77.0, "bueno"),
    (8, f"{PATH_VOLUME}/displays/display_008.jpg", "Nestle", "supermercado", "Guadalajara", 76.0, 80.0, "bueno"),
    (9, f"{PATH_VOLUME}/displays/display_009.jpg", "Bimbo", "supermercado", "Monterrey", 82.0, 78.0, "bueno"),
    (10, f"{PATH_VOLUME}/displays/display_010.jpg", "Coca-Cola", "conveniencia", "Puebla", 74.0, 72.0, "bueno"),
    # -- Displays regulares --
    (11, f"{PATH_VOLUME}/displays/display_011.jpg", "PepsiCo", "supermercado", "CDMX", 60.0, 55.0, "regular"),
    (12, f"{PATH_VOLUME}/displays/display_012.jpg", "Nestle", "conveniencia", "Monterrey", 58.0, 62.0, "regular"),
    (13, f"{PATH_VOLUME}/displays/display_013.jpg", "Coca-Cola", "supermercado", "Guadalajara", 55.0, 50.0, "regular"),
    (14, f"{PATH_VOLUME}/displays/display_014.jpg", "Bimbo", "conveniencia", "Puebla", 62.0, 58.0, "regular"),
    (15, f"{PATH_VOLUME}/displays/display_015.jpg", "PepsiCo", "supermercado", "CDMX", 57.0, 53.0, "regular"),
    # -- Displays deficientes --
    (16, f"{PATH_VOLUME}/displays/display_016.jpg", "Coca-Cola", "conveniencia", "Monterrey", 35.0, 30.0, "deficiente"),
    (17, f"{PATH_VOLUME}/displays/display_017.jpg", "Nestle", "supermercado", "Guadalajara", 28.0, 25.0, "deficiente"),
    (18, f"{PATH_VOLUME}/displays/display_018.jpg", "PepsiCo", "conveniencia", "CDMX", 32.0, 35.0, "deficiente"),
    (19, f"{PATH_VOLUME}/displays/display_019.jpg", "Bimbo", "supermercado", "Puebla", 40.0, 38.0, "deficiente"),
    (20, f"{PATH_VOLUME}/displays/display_020.jpg", "Coca-Cola", "supermercado", "Monterrey", 25.0, 22.0, "deficiente"),
]

df_reference = spark.createDataFrame(reference_data, schema=display_schema)
display(df_reference)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Guardar como tabla Delta
# MAGIC
# MAGIC **Importante:** Para que Vector Search con Delta Sync funcione en endpoints Standard, la tabla debe tener **Change Data Feed (CDF)** habilitado.

# COMMAND ----------

# DBTITLE 1,Crear tabla Delta con CDF habilitado
df_reference.write.mode("overwrite").saveAsTable(f"{PATH_TABLE}.displays_referencia")

spark.sql(f"""
    ALTER TABLE {PATH_TABLE}.displays_referencia
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print(f"Tabla creada: {PATH_TABLE}.displays_referencia")
print(f"CDF habilitado: Si")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Verificar los datos

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   quality_label,
# MAGIC   COUNT(*) as cantidad,
# MAGIC   ROUND(AVG(ideal_score), 1) as promedio_ideal,
# MAGIC   ROUND(AVG(compliance_score), 1) as promedio_compliance
# MAGIC FROM academia.image_scorer.displays_referencia
# MAGIC GROUP BY quality_label
# MAGIC ORDER BY promedio_ideal DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ya tenemos nuestros datos de referencia listos! Ahora vamos a generar los embeddings de las imagenes.
# MAGIC
# MAGIC [Lab 02 - Generacion de Embeddings de Imagenes]($./Lab 02 - Generacion de Embeddings de Imagenes)
