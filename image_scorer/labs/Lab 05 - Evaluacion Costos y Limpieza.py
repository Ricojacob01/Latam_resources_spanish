# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On LAB 05 - Evaluacion, Costos y Limpieza
# MAGIC
# MAGIC En este laboratorio evaluaremos la calidad de la recuperacion, entenderemos los costos
# MAGIC y limpiaremos los recursos creados durante el workshop.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos del Ejercicio
# MAGIC
# MAGIC - Evaluar la calidad de la recuperacion con precision@k y recall@k
# MAGIC - Entender el modelo de costos de Vector Search
# MAGIC - Limpiar los recursos creados durante el workshop

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion
# MAGIC
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el cluster: **Serverless**

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "../config"

# COMMAND ----------

VS_INDEX_NAME='ardemo_classic_dnubtw_catalog.image_score.displays_index_1'

# COMMAND ----------

import numpy as np
import json
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import functions as F

vsc = VectorSearchClient()
index = vsc.get_index(
    endpoint_name=VS_ENDPOINT_NAME,
    index_name=VS_INDEX_NAME
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01 - Evaluar la calidad de recuperacion
# MAGIC
# MAGIC Antes de poner un sistema de recuperacion en produccion, es fundamental medir su calidad.
# MAGIC La guia de POC de Vector Search recomienda:
# MAGIC
# MAGIC 1. **Acordar consultas reales** representativas del caso de uso
# MAGIC 2. **Elegir metricas** que reflejen la experiencia del usuario
# MAGIC 3. **Medir** precision@k y recall@k como punto de partida
# MAGIC
# MAGIC ### Metricas clave:
# MAGIC
# MAGIC | Metrica | Descripcion |
# MAGIC |---------|-------------|
# MAGIC | **Precision@K** | De los K resultados devueltos, que fraccion es relevante? |
# MAGIC | **Recall@K** | De todos los resultados relevantes, que fraccion recuperamos en el top K? |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Definir conjunto de evaluacion
# MAGIC
# MAGIC Creamos un pequeno eval set con consultas conocidas y sus resultados esperados.

# COMMAND ----------

# DBTITLE 1,Conjunto de evaluacion
eval_set = [
    {
        "name": "Display excelente Coca-Cola",
        "display_id_query": 1,  # Usamos un display conocido como consulta
        "expected_quality": "excelente",
        "expected_brand": "Coca-Cola",
        "relevant_ids": [1, 2, 6, 10, 16],  # IDs de displays Coca-Cola
    },
    {
        "name": "Display bueno PepsiCo",
        "display_id_query": 7,
        "expected_quality": "bueno",
        "expected_brand": "PepsiCo",
        "relevant_ids": [3, 7, 11, 15, 18],  # IDs de displays PepsiCo
    },
    {
        "name": "Display regular Nestle",
        "display_id_query": 12,
        "expected_quality": "regular",
        "expected_brand": "Nestle",
        "relevant_ids": [4, 8, 12, 17],  # IDs de displays Nestle
    },
]

print(f"Conjunto de evaluacion: {len(eval_set)} consultas")
for e in eval_set:
    print(f"  - {e['name']} (display {e['display_id_query']}, esperado: {e['expected_quality']})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Ejecutar evaluacion

# COMMAND ----------

# DBTITLE 1,Evaluar precision@K y recall@K
def evaluate_retrieval(eval_set: list, k: int = TOP_K) -> dict:
    """
    Evalua la calidad de recuperacion del indice de Vector Search.

    Returns:
        dict con metricas agregadas y por consulta
    """
    results_detail = []

    for eval_item in eval_set:
        # Obtener embedding de la consulta
        query_row = spark.table(f"{PATH_TABLE}.displays_referencia").filter(
            f"display_id = {eval_item['display_id_query']}"
        ).first()

        if not query_row or not query_row.embedding:
            print(f"  SKIP: No se encontro embedding para display {eval_item['display_id_query']}")
            continue

        # Buscar
        search_results = index.similarity_search(
            query_vector=query_row.embedding,
            columns=["display_id", "brand", "quality_label"],
            num_results=k
        )

        data_array = search_results.get("result", {}).get("data_array", [])
        retrieved_ids = [row[0] for row in data_array]

        # Calcular metricas
        relevant_ids = set(eval_item["relevant_ids"])
        retrieved_set = set(retrieved_ids)

        relevant_retrieved = relevant_ids.intersection(retrieved_set)

        precision_at_k = len(relevant_retrieved) / k if k > 0 else 0
        recall_at_k = len(relevant_retrieved) / len(relevant_ids) if relevant_ids else 0

        # Verificar que la calidad dominante coincide
        retrieved_qualities = [row[2] for row in data_array]
        quality_match = eval_item["expected_quality"] in retrieved_qualities

        result = {
            "name": eval_item["name"],
            "precision_at_k": round(precision_at_k, 3),
            "recall_at_k": round(recall_at_k, 3),
            "quality_match": quality_match,
            "retrieved_ids": retrieved_ids,
            "relevant_retrieved": list(relevant_retrieved),
        }
        results_detail.append(result)

    # Metricas agregadas
    avg_precision = np.mean([r["precision_at_k"] for r in results_detail])
    avg_recall = np.mean([r["recall_at_k"] for r in results_detail])

    return {
        "avg_precision_at_k": round(avg_precision, 3),
        "avg_recall_at_k": round(avg_recall, 3),
        "k": k,
        "num_queries": len(results_detail),
        "details": results_detail
    }

# COMMAND ----------

eval_results = evaluate_retrieval(eval_set)

print("=" * 70)
print(f"RESULTADOS DE EVALUACION (K={eval_results['k']})")
print("=" * 70)
print(f"  Precision@{eval_results['k']} promedio: {eval_results['avg_precision_at_k']}")
print(f"  Recall@{eval_results['k']} promedio:    {eval_results['avg_recall_at_k']}")
print(f"  Consultas evaluadas: {eval_results['num_queries']}")
print()

for detail in eval_results["details"]:
    status = "OK" if detail["quality_match"] else "!!"
    print(f"  [{status}] {detail['name']}")
    print(f"       Precision@K: {detail['precision_at_k']} | Recall@K: {detail['recall_at_k']}")
    print(f"       Recuperados: {detail['retrieved_ids']}")
    print(f"       Relevantes encontrados: {detail['relevant_retrieved']}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Interpretar resultados
# MAGIC
# MAGIC **Guia de interpretacion:**
# MAGIC
# MAGIC | Metrica | Bueno | Aceptable | Necesita mejora |
# MAGIC |---------|-------|-----------|-----------------|
# MAGIC | Precision@5 | > 0.7 | 0.5 - 0.7 | < 0.5 |
# MAGIC | Recall@5 | > 0.6 | 0.4 - 0.6 | < 0.4 |
# MAGIC
# MAGIC **Si los resultados no son satisfactorios:**
# MAGIC - Verificar la calidad de los embeddings
# MAGIC - Aumentar el numero de ejemplos de referencia
# MAGIC - Ajustar el modelo de embedding (probar CLIP ViT-L/14 en lugar de ViT-B/32)
# MAGIC - Considerar fine-tuning del modelo de embedding para el dominio especifico

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Costos y operaciones
# MAGIC
# MAGIC Es importante entender el modelo de costos de Vector Search para planificar la produccion.
# MAGIC
# MAGIC ### Modelo de precios de Vector Search:
# MAGIC
# MAGIC | Componente | Costo |
# MAGIC |------------|-------|
# MAGIC | **Endpoint runtime** | Principal driver de costo - se cobra por tiempo de ejecucion |
# MAGIC | **Almacenamiento del indice** | Basado en el tamano del corpus |
# MAGIC | **Consultas** | Incluidas en el costo del endpoint |
# MAGIC
# MAGIC ### Consideraciones importantes:
# MAGIC
# MAGIC - **Scale-to-zero NO es soportado** para endpoints de Vector Search en produccion
# MAGIC - Los endpoints activos con indices incurren costo continuo
# MAGIC - Para estimaciones de costo, use el **GenAI Calculator**
# MAGIC - Endpoints vacios (sin indices) no generan costo significativo

# COMMAND ----------

# DBTITLE 1,Informacion del endpoint y estimacion de costos
print("=" * 70)
print("INFORMACION DE COSTOS Y OPERACIONES")
print("=" * 70)
print()

# Informacion del endpoint
try:
    ep = vsc.get_endpoint(VS_ENDPOINT_NAME)
    print(f"  Endpoint: {VS_ENDPOINT_NAME}")
    print(f"  Tipo: Standard")
    print(f"  Estado: {ep.get('endpoint_status', {}).get('state', 'N/A')}")
except Exception as e:
    print(f"  Error obteniendo info del endpoint: {e}")

print()

# Estimacion del corpus
num_rows = spark.table(f"{PATH_TABLE}.displays_referencia").count()
embedding_size_bytes = EMBEDDING_DIM * 4  # float32 = 4 bytes
total_index_size_mb = (num_rows * embedding_size_bytes) / (1024 * 1024)

print(f"  CORPUS:")
print(f"    Numero de registros: {num_rows}")
print(f"    Dimension del embedding: {EMBEDDING_DIM}")
print(f"    Tamano estimado del indice: {total_index_size_mb:.2f} MB")
print()

print(f"  REFERENCIAS DE PRECIOS:")
print(f"    - Pricing: https://www.databricks.com/product/pricing")
print(f"    - GenAI Calculator: consulte a su equipo de Databricks")
print()

print(f"  NOTA: Para workshops y POCs, recuerde limpiar los recursos")
print(f"  al finalizar para evitar costos innecesarios.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Consultar uso via system tables (opcional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Descomentar para ver el uso de endpoints de serving
# MAGIC -- SELECT
# MAGIC --   usage_date,
# MAGIC --   sku_name,
# MAGIC --   usage_quantity,
# MAGIC --   usage_unit
# MAGIC -- FROM system.billing.usage
# MAGIC -- WHERE sku_name LIKE '%VECTOR_SEARCH%'
# MAGIC -- ORDER BY usage_date DESC
# MAGIC -- LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 03 - Limpieza de recursos
# MAGIC
# MAGIC **Importante:** Los endpoints de Vector Search con indices activos generan costo continuo.
# MAGIC Limpie los recursos al finalizar el workshop.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Eliminar el indice

# COMMAND ----------

# DBTITLE 1,Eliminar indice de Vector Search
# ATENCION: Descomente las siguientes lineas solo cuando desee limpiar los recursos

# try:
#     vsc.delete_index(
#         endpoint_name=VS_ENDPOINT_NAME,
#         index_name=VS_INDEX_NAME
#     )
#     print(f"Indice '{VS_INDEX_NAME}' eliminado.")
# except Exception as e:
#     print(f"Error eliminando indice: {e}")

print(">> Descomente el codigo anterior para eliminar el indice <<")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Eliminar el endpoint de Vector Search

# COMMAND ----------

# DBTITLE 1,Eliminar endpoint de Vector Search
# ATENCION: Descomente las siguientes lineas solo cuando desee limpiar los recursos

# try:
#     vsc.delete_endpoint(VS_ENDPOINT_NAME)
#     print(f"Endpoint '{VS_ENDPOINT_NAME}' eliminado.")
# except Exception as e:
#     print(f"Error eliminando endpoint: {e}")

print(">> Descomente el codigo anterior para eliminar el endpoint <<")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Eliminar el endpoint de Model Serving

# COMMAND ----------

# DBTITLE 1,Eliminar endpoint de embedding
# ATENCION: Descomente las siguientes lineas solo cuando desee limpiar los recursos

# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# try:
#     w.serving_endpoints.delete(EMBEDDING_ENDPOINT)
#     print(f"Endpoint '{EMBEDDING_ENDPOINT}' eliminado.")
# except Exception as e:
#     print(f"Error eliminando endpoint: {e}")

print(">> Descomente el codigo anterior para eliminar el endpoint de embedding <<")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Eliminar tablas temporales

# COMMAND ----------

# DBTITLE 1,Eliminar tablas y esquema
# ATENCION: Descomente las siguientes lineas solo cuando desee limpiar los recursos

# spark.sql(f"DROP TABLE IF EXISTS {PATH_TABLE}.displays_referencia")
# spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
# spark.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{SCHEMA}")
# print("Tablas, volumen y esquema eliminados.")

print(">> Descomente el codigo anterior para eliminar tablas y esquema <<")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Felicidades!
# MAGIC
# MAGIC Has completado el workshop de **Image Scorer con Vector Search**!
# MAGIC
# MAGIC ### Resumen de lo aprendido:
# MAGIC
# MAGIC | Lab | Tema |
# MAGIC |-----|------|
# MAGIC | 01 | Setup, arquitectura y datos de referencia en Delta |
# MAGIC | 02 | Generacion de embeddings de imagenes con CLIP y Model Serving |
# MAGIC | 03 | Creacion de endpoint e indice de Vector Search (Standard + Delta Sync) |
# MAGIC | 04 | Consulta por similitud, filtros, puntuacion y explicabilidad |
# MAGIC | 05 | Evaluacion de calidad de recuperacion, costos y limpieza |
# MAGIC
# MAGIC ### Patron implementado: Model Serving + Delta + Vector Search (Standard)
# MAGIC
# MAGIC - **Self-managed embeddings** para contenido visual (imagenes)
# MAGIC - **Delta Sync** para sincronizacion automatica del indice
# MAGIC - **Puntuacion transparente** basada en vecinos mas cercanos etiquetados
# MAGIC - **Explicabilidad** integrada para adopcion por usuarios de negocio
# MAGIC
# MAGIC ### Proximos pasos para produccion:
# MAGIC
# MAGIC 1. Reemplazar embeddings simulados con el endpoint CLIP real
# MAGIC 2. Aumentar el corpus de referencia (>100 displays etiquetados por categoria)
# MAGIC 3. Fine-tune del modelo de embedding para el dominio especifico
# MAGIC 4. Integrar con una aplicacion web/movil para auditores de campo
# MAGIC 5. Monitorear la calidad de recuperacion con evaluaciones periodicas
