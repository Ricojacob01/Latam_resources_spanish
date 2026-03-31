# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On LAB 03 - Endpoint e Indice de Vector Search
# MAGIC
# MAGIC En este laboratorio crearemos la infraestructura de Vector Search para busqueda por similitud de imagenes.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos del Ejercicio
# MAGIC
# MAGIC - Crear un endpoint de Vector Search tipo **Standard**
# MAGIC - Crear un indice **Delta Sync** con self-managed embeddings
# MAGIC - Entender las opciones de configuracion y sus implicaciones
# MAGIC
# MAGIC ### Por que Standard?
# MAGIC
# MAGIC | Tipo | Latencia | Costo minimo | Recomendado para |
# MAGIC |------|----------|-------------|------------------|
# MAGIC | **Standard** | 20-50ms | Menor | Workshops, POCs, datasets pequenos/medianos |
# MAGIC | Storage Optimized | 50-100ms | Mayor | Datasets grandes en produccion |
# MAGIC
# MAGIC Standard es la recomendacion por defecto para POCs y workshops.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion
# MAGIC
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el cluster: **Serverless**

# COMMAND ----------

# MAGIC %run "../config"

# COMMAND ----------

import time
from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient()

# COMMAND ----------

# DBTITLE 1,Instalar dependencias
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01 - Crear el Vector Search Endpoint
# MAGIC
# MAGIC El endpoint es el recurso computacional que aloja y sirve los indices de Vector Search.

# COMMAND ----------

# DBTITLE 1,Crear endpoint Standard
def create_vs_endpoint(endpoint_name: str):
    """Crea un endpoint de Vector Search tipo Standard."""
    try:
        endpoint = vsc.create_endpoint(
            name=endpoint_name,
            endpoint_type="STANDARD"
        )
        print(f"Endpoint '{endpoint_name}' creado. Esperando a que este listo...")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Endpoint '{endpoint_name}' ya existe. Continuando...")
        else:
            raise e

create_vs_endpoint(VS_ENDPOINT_NAME)

# COMMAND ----------

# DBTITLE 1,Esperar a que el endpoint este listo
def wait_for_endpoint(endpoint_name: str, timeout_minutes: int = 15):
    """Espera a que el endpoint este en estado ONLINE."""
    start = time.time()
    timeout = timeout_minutes * 60

    while time.time() - start < timeout:
        try:
            ep = vsc.get_endpoint(endpoint_name)
            status = ep.get("endpoint_status", {}).get("state", "UNKNOWN")
            print(f"  Estado del endpoint: {status}")

            if status == "ONLINE":
                print(f"Endpoint '{endpoint_name}' esta ONLINE!")
                return True
        except Exception as e:
            print(f"  Verificando... ({e})")

        time.sleep(30)

    print(f"Timeout esperando endpoint. Verifique manualmente en Compute > Vector Search.")
    return False

wait_for_endpoint(VS_ENDPOINT_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Crear el indice Delta Sync
# MAGIC
# MAGIC Usaremos el modo **Delta Sync + self-managed embeddings**:
# MAGIC
# MAGIC - **Delta Sync**: El indice se sincroniza automaticamente cuando la tabla Delta cambia
# MAGIC - **Self-managed embeddings**: Nosotros pre-calculamos los embeddings (necesario para imagenes)
# MAGIC
# MAGIC ### Requisitos:
# MAGIC - La tabla fuente debe tener **Change Data Feed (CDF)** habilitado (ya lo hicimos en Lab 01)
# MAGIC - Debe existir una columna con el vector de embeddings
# MAGIC - Se necesita una columna de clave primaria

# COMMAND ----------

# DBTITLE 1,Crear indice Delta Sync con self-managed embeddings
SOURCE_TABLE = f"{PATH_TABLE}.displays_referencia"

def create_vs_index(endpoint_name: str, index_name: str, source_table: str):
    """Crea un indice de Vector Search con Delta Sync y self-managed embeddings."""
    try:
        index = vsc.create_delta_sync_index(
            endpoint_name=endpoint_name,
            index_name=index_name,
            source_table_name=source_table,
            pipeline_type="TRIGGERED",  # Se sincroniza cuando lo solicitamos
            primary_key="display_id",
            embedding_dimension=EMBEDDING_DIM,
            embedding_vector_column="embedding",
            columns_to_sync=[
                "display_id",
                "image_url",
                "brand",
                "store_type",
                "region",
                "ideal_score",
                "compliance_score",
                "quality_label"
            ]
        )
        print(f"Indice '{index_name}' creado exitosamente!")
        print(f"  Tabla fuente: {source_table}")
        print(f"  Dimension embedding: {EMBEDDING_DIM}")
        print(f"  Columnas sincronizadas: todas las de metadatos")
    except Exception as e:
        if "already exists" in str(e).lower():
            print(f"Indice '{index_name}' ya existe. Continuando...")
        else:
            raise e

create_vs_index(VS_ENDPOINT_NAME, VS_INDEX_NAME, SOURCE_TABLE)

# COMMAND ----------

# DBTITLE 1,Esperar a que el indice este listo
def wait_for_index(index_name: str, timeout_minutes: int = 20):
    """Espera a que el indice este sincronizado y listo."""
    start = time.time()
    timeout = timeout_minutes * 60

    while time.time() - start < timeout:
        try:
            idx = vsc.get_index(
                endpoint_name=VS_ENDPOINT_NAME,
                index_name=index_name
            )
            status = idx.describe().get("status", {})
            ready = status.get("ready", False)
            state = status.get("detailed_state", "UNKNOWN")

            print(f"  Estado del indice: {state} | Listo: {ready}")

            if ready:
                print(f"Indice '{index_name}' esta listo para consultas!")
                return True
        except Exception as e:
            print(f"  Verificando... ({e})")

        time.sleep(30)

    print(f"Timeout esperando indice. Verifique en Catalog > indice.")
    return False

wait_for_index(VS_INDEX_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 03 - Verificar el indice

# COMMAND ----------

# DBTITLE 1,Consultar estado del indice
import json

index = vsc.get_index(
    endpoint_name=VS_ENDPOINT_NAME,
    index_name=VS_INDEX_NAME
)

index_info = index.describe()
print(json.dumps(index_info, indent=2, default=str))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Prueba rapida de consulta
# MAGIC
# MAGIC Vamos a hacer una consulta de prueba usando uno de los embeddings existentes para verificar que todo funciona.

# COMMAND ----------

# DBTITLE 1,Consulta de prueba con un embedding existente
import json

# Tomar el embedding del primer display como consulta de prueba
test_row = spark.table(f"{PATH_TABLE}.displays_referencia").filter("display_id = 1").first()
test_embedding = test_row.embedding

# Buscar los 5 mas similares
results = index.similarity_search(
    query_vector=test_embedding,
    columns=["display_id", "brand", "quality_label", "ideal_score", "compliance_score"],
    num_results=TOP_K
)

print(f"Consulta de prueba - Display ID 1 ({test_row.brand}, {test_row.quality_label})")
print(f"Top {TOP_K} resultados mas similares:")
print("-" * 70)

result_df = results.get("result", {}).get("data_array", [])
for i, row in enumerate(result_df):
    print(f"  {i+1}. Display {row[0]} | {row[1]} | {row[2]} | "
          f"ideal={row[3]} | compliance={row[4]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### El indice esta funcionando! Ahora vamos a construir el flujo completo de consulta y puntuacion.
# MAGIC
# MAGIC [Lab 04 - Consulta Puntuacion y Explicabilidad]($./Lab 04 - Consulta Puntuacion y Explicabilidad)
