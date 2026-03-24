# Databricks notebook source
# MAGIC %md
# MAGIC # Configuracion del Workshop - Image Scorer

# COMMAND ----------

# Catalogo y esquema
CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "image_score"
VOLUME = "archivos"

# Rutas
PATH_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
PATH_TABLE = f"{CATALOG}.{SCHEMA}"

# Vector Search
VS_ENDPOINT_NAME = "image-scorer-vs-endpoint"
VS_INDEX_NAME = f"{CATALOG}.{SCHEMA}.displays_index"

# Model Serving - Embedding endpoint
EMBEDDING_ENDPOINT = "image-embedding-endpoint"
EMBEDDING_DIM = 512

# Parametros del workshop
TOP_K = 5
SCORE_WEIGHT_NEIGHBORS = 0.7
SCORE_WEIGHT_MODEL = 0.3
