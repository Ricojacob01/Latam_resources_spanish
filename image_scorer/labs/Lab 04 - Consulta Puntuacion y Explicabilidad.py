# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On LAB 04 - Consulta, Puntuacion y Explicabilidad
# MAGIC
# MAGIC En este laboratorio implementaremos el flujo completo:
# MAGIC subir una imagen nueva, buscar displays similares, calcular la puntuacion y mostrar la explicabilidad.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos del Ejercicio
# MAGIC
# MAGIC - Consultar el indice con una imagen nueva
# MAGIC - Aplicar filtros de metadatos (marca, tipo de tienda)
# MAGIC - Calcular la puntuacion final basada en vecinos mas cercanos
# MAGIC - Mostrar la explicabilidad del resultado
# MAGIC
# MAGIC ### Formula de puntuacion
# MAGIC
# MAGIC ```
# MAGIC final_score = 0.7 * nn_score + 0.3 * model_score
# MAGIC ```
# MAGIC
# MAGIC Donde:
# MAGIC - `nn_score`: promedio ponderado de las puntuaciones de los vecinos mas cercanos
# MAGIC - `model_score`: puntuacion derivada de un rubric del modelo (opcional)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion
# MAGIC
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el cluster: **Serverless**

# COMMAND ----------

# MAGIC %run "../config"

# COMMAND ----------

import numpy as np
import json
import requests
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql import functions as F

vsc = VectorSearchClient()
index = vsc.get_index(
    endpoint_name=VS_ENDPOINT_NAME,
    index_name=VS_INDEX_NAME
)

# COMMAND ----------

# DBTITLE 1,Instalar dependencias
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01 - Consultar con una imagen nueva
# MAGIC
# MAGIC Simulamos el escenario real: un auditor de campo sube una foto de un display y el sistema
# MAGIC encuentra los displays de referencia mas similares para calcular una puntuacion.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Generar embedding de la imagen nueva

# COMMAND ----------

# DBTITLE 1,Funcion para procesar imagen nueva
def query_new_image(image_url: str, top_k: int = TOP_K, filters: dict = None):
    """
    Procesa una imagen nueva y busca los displays mas similares.

    Args:
        image_url: URL o ruta de la imagen a evaluar
        top_k: Numero de vecinos a recuperar
        filters: Filtros opcionales (ej: {"brand": "Coca-Cola"})

    Returns:
        dict con resultados de la busqueda
    """
    # -- Paso 1: Generar embedding de la imagen nueva --
    # En produccion: embedding = get_image_embedding(image_url)

    # Para el workshop, simulamos un embedding de una imagen "buena"
    np.random.seed(hash(image_url) % 2**32)
    query_embedding = np.random.randn(EMBEDDING_DIM).astype(np.float32)
    query_embedding[:10] += 0.65  # Simular un display de calidad "buena"
    query_embedding = (query_embedding / np.linalg.norm(query_embedding)).tolist()

    # -- Paso 2: Buscar en Vector Search --
    search_params = {
        "query_vector": query_embedding,
        "columns": [
            "display_id", "image_url", "brand", "store_type",
            "region", "ideal_score", "compliance_score", "quality_label"
        ],
        "num_results": top_k
    }

    # Aplicar filtros si se proporcionan (formato dict para standard endpoints)
    if filters:
        search_params["filters"] = filters

    results = index.similarity_search(**search_params)

    return {
        "query_embedding": query_embedding,
        "results": results,
        "image_url": image_url
    }

# COMMAND ----------

# DBTITLE 1,Consultar con una imagen nueva (sin filtros)
# Simular la subida de una nueva imagen de display
new_image_url = f"{PATH_VOLUME}/displays/testing/display_test_good.jpg"

print("=" * 70)
print("CONSULTA: Imagen nueva de display")
print(f"  Imagen: {new_image_url}")
print(f"  Top K: {TOP_K}")
print(f"  Filtros: Ninguno")
print("=" * 70)

query_result = query_new_image(new_image_url)

data_array = query_result["results"].get("result", {}).get("data_array", [])

print(f"\nTop {TOP_K} displays mas similares:")
print("-" * 70)
print(f"{'#':>3} | {'ID':>4} | {'Marca':<12} | {'Tienda':<14} | {'Calidad':<12} | {'Ideal':>6} | {'Compliance':>10}")
print("-" * 70)

for i, row in enumerate(data_array):
    print(f"{i+1:>3} | {row[0]:>4} | {row[2]:<12} | {row[3]:<14} | {row[7]:<12} | {row[5]:>6.1f} | {row[6]:>10.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Filtros de metadatos
# MAGIC
# MAGIC Vector Search soporta filtros como capacidad nativa.
# MAGIC Esto es util para restringir la busqueda a un subconjunto relevante
# MAGIC (por ejemplo, solo displays de una marca o tipo de tienda especifico).
# MAGIC
# MAGIC **Recomendacion:** Valide el comportamiento de los filtros durante el workshop.
# MAGIC Es importante probar temprano que los filtros funcionan como se espera.

# COMMAND ----------

# DBTITLE 1,Consulta filtrada por marca
print("=" * 70)
print("CONSULTA FILTRADA: Solo displays de Coca-Cola")
print("=" * 70)

filtered_result = query_new_image(
    new_image_url,
    filters={"brand": "Coca-Cola"}
)

data_filtered = filtered_result["results"].get("result", {}).get("data_array", [])

print(f"\nTop displays Coca-Cola mas similares:")
print("-" * 70)
for i, row in enumerate(data_filtered):
    print(f"  {i+1}. Display {row[0]} | {row[2]} | {row[3]} | {row[7]} | "
          f"ideal={row[5]:.1f} | compliance={row[6]:.1f}")

# COMMAND ----------

# DBTITLE 1,Consulta filtrada por tipo de tienda
print("=" * 70)
print("CONSULTA FILTRADA: Solo displays en supermercados")
print("=" * 70)

filtered_result_store = query_new_image(
    new_image_url,
    filters={"store_type": "supermercado"}
)

data_store = filtered_result_store["results"].get("result", {}).get("data_array", [])

print(f"\nTop displays en supermercados:")
print("-" * 70)
for i, row in enumerate(data_store):
    print(f"  {i+1}. Display {row[0]} | {row[2]} | {row[3]} | {row[7]} | "
          f"ideal={row[5]:.1f} | compliance={row[6]:.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Comparacion: Con filtros vs Sin filtros
# MAGIC
# MAGIC Observe como los resultados filtrados son mas relevantes para el contexto especifico.
# MAGIC En un escenario real, filtrar por marca asegura que la puntuacion se calcule
# MAGIC comparando contra displays de la misma marca.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 03 - Calcular la puntuacion final del display
# MAGIC
# MAGIC La puntuacion se calcula de forma **transparente**:
# MAGIC usamos los ejemplos etiquetados mas similares para derivar una puntuacion ponderada.
# MAGIC
# MAGIC ```
# MAGIC nn_score = promedio ponderado de ideal_score de los vecinos
# MAGIC model_score = puntuacion del rubric del modelo (opcional)
# MAGIC final_score = 0.7 * nn_score + 0.3 * model_score
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Funcion de puntuacion basada en vecinos
def compute_display_score(search_results: dict,
                          weight_neighbors: float = SCORE_WEIGHT_NEIGHBORS,
                          weight_model: float = SCORE_WEIGHT_MODEL) -> dict:
    """
    Calcula la puntuacion final de un display basada en los vecinos mas cercanos.

    La puntuacion es transparente: se basa en ejemplos reales etiquetados.

    Args:
        search_results: Resultados de similarity_search
        weight_neighbors: Peso para la puntuacion de vecinos (default: 0.7)
        weight_model: Peso para la puntuacion del modelo (default: 0.3)

    Returns:
        dict con puntuaciones detalladas
    """
    data_array = search_results.get("result", {}).get("data_array", [])

    if not data_array:
        return {"error": "No se encontraron vecinos similares"}

    # Extraer puntuaciones de los vecinos
    ideal_scores = [row[5] for row in data_array if row[5] is not None]
    compliance_scores = [row[6] for row in data_array if row[6] is not None]
    quality_labels = [row[7] for row in data_array if row[7] is not None]

    # Promedio de puntuaciones de vecinos
    nn_ideal = np.mean(ideal_scores) if ideal_scores else 0
    nn_compliance = np.mean(compliance_scores) if compliance_scores else 0
    nn_score = (nn_ideal + nn_compliance) / 2

    # Puntuacion del modelo (rubric simplificado)
    # En produccion, esto podria ser una llamada a un LLM multimodal
    quality_map = {"excelente": 95, "bueno": 75, "regular": 55, "deficiente": 30}
    if quality_labels:
        from collections import Counter
        most_common = Counter(quality_labels).most_common(1)[0][0]
        model_score = quality_map.get(most_common, 50)
    else:
        model_score = 50

    # Puntuacion final
    final_score = weight_neighbors * nn_score + weight_model * model_score

    # Clasificacion
    if final_score >= 85:
        classification = "EXCELENTE"
    elif final_score >= 70:
        classification = "BUENO"
    elif final_score >= 50:
        classification = "REGULAR"
    else:
        classification = "DEFICIENTE"

    return {
        "nn_ideal_score": round(nn_ideal, 2),
        "nn_compliance_score": round(nn_compliance, 2),
        "nn_combined_score": round(nn_score, 2),
        "model_score": round(model_score, 2),
        "final_score": round(final_score, 2),
        "classification": classification,
        "neighbors_count": len(data_array),
        "dominant_quality": most_common if quality_labels else "N/A",
        "formula": f"{weight_neighbors} * {round(nn_score,2)} + {weight_model} * {model_score} = {round(final_score,2)}"
    }

# COMMAND ----------

# DBTITLE 1,Calcular puntuacion del display
score = compute_display_score(query_result["results"])

print("=" * 70)
print("RESULTADO DE PUNTUACION")
print("=" * 70)
print(f"  Puntuacion ideal (vecinos):      {score['nn_ideal_score']}")
print(f"  Puntuacion compliance (vecinos):  {score['nn_compliance_score']}")
print(f"  Puntuacion combinada (vecinos):   {score['nn_combined_score']}")
print(f"  Puntuacion modelo (rubric):       {score['model_score']}")
print(f"  ")
print(f"  Formula: {score['formula']}")
print(f"  ")
print(f"  >>> PUNTUACION FINAL: {score['final_score']} - {score['classification']} <<<")
print(f"  ")
print(f"  Vecinos usados: {score['neighbors_count']}")
print(f"  Calidad dominante: {score['dominant_quality']}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 04 - Explicabilidad
# MAGIC
# MAGIC Este es el valor de negocio: **"Puntuamos este display comparandolo con tus mejores ejemplos conocidos."**
# MAGIC
# MAGIC La explicabilidad es clave para la adopcion del sistema. Los auditores de campo
# MAGIC necesitan entender *por que* el sistema asigno una puntuacion determinada.

# COMMAND ----------

# DBTITLE 1,Panel de explicabilidad completo
def show_explainability(image_url: str, search_results: dict, score: dict):
    """Muestra el panel de explicabilidad con imagen consultada, vecinos y puntuacion."""

    data_array = search_results.get("result", {}).get("data_array", [])

    print("=" * 80)
    print("                    PANEL DE EXPLICABILIDAD - IMAGE SCORER")
    print("=" * 80)
    print()
    print(f"  IMAGEN EVALUADA: {image_url}")
    print()

    # Tabla de vecinos
    print("  TOP DISPLAYS SIMILARES (referencia):")
    print("  " + "-" * 76)
    print(f"  {'#':>3} | {'ID':>4} | {'Marca':<12} | {'Tienda':<14} | {'Calidad':<12} | {'Ideal':>6} | {'Compl.':>6}")
    print("  " + "-" * 76)

    for i, row in enumerate(data_array):
        emoji_map = {"excelente": "[++]", "bueno": "[+ ]", "regular": "[~ ]", "deficiente": "[--]"}
        indicator = emoji_map.get(row[7], "[??]")
        print(f"  {i+1:>3} | {row[0]:>4} | {row[2]:<12} | {row[3]:<14} | {indicator} {row[7]:<8} | {row[5]:>6.1f} | {row[6]:>6.1f}")

    print("  " + "-" * 76)
    print()

    # Calculo de puntuacion
    print("  CALCULO DE PUNTUACION:")
    print(f"    Promedio ideal de vecinos:      {score['nn_ideal_score']:>6.2f}")
    print(f"    Promedio compliance de vecinos:  {score['nn_compliance_score']:>6.2f}")
    print(f"    Score combinado vecinos:         {score['nn_combined_score']:>6.2f}")
    print(f"    Score del modelo (rubric):       {score['model_score']:>6.2f}")
    print()
    print(f"    Formula: final = {SCORE_WEIGHT_NEIGHBORS} x vecinos + {SCORE_WEIGHT_MODEL} x modelo")
    print(f"             final = {score['formula']}")
    print()

    # Resultado final
    class_bar = {
        "EXCELENTE":  "[==================] ",
        "BUENO":      "[=============     ] ",
        "REGULAR":    "[========          ] ",
        "DEFICIENTE": "[====              ] ",
    }
    bar = class_bar.get(score['classification'], "[??????????????????] ")

    print(f"    {bar} {score['final_score']:.1f}/100 - {score['classification']}")
    print()

    # Razonamiento
    print("  RAZONAMIENTO:")
    print(f"    Este display fue comparado con {score['neighbors_count']} displays de referencia.")
    print(f"    La mayoria de los vecinos similares fueron clasificados como '{score['dominant_quality']}'.")
    print(f"    Basado en la similitud visual con estos ejemplos conocidos,")
    print(f"    el sistema asigna una puntuacion de {score['final_score']:.1f}/100.")
    print()
    print("=" * 80)

# COMMAND ----------

show_explainability(new_image_url, query_result["results"], score)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 05 - Flujo completo en una sola funcion
# MAGIC
# MAGIC Ahora encapsulamos todo el flujo en una sola funcion reutilizable.

# COMMAND ----------

# DBTITLE 1,Funcion end-to-end: imagen -> puntuacion
def score_display(image_url: str,
                  brand_filter: str = None,
                  store_filter: str = None,
                  show_details: bool = True) -> dict:
    """
    Flujo completo: sube imagen, busca similares, calcula puntuacion.

    Args:
        image_url: URL o ruta de la imagen del display
        brand_filter: Filtrar por marca (opcional)
        store_filter: Filtrar por tipo de tienda (opcional)
        show_details: Mostrar panel de explicabilidad

    Returns:
        dict con la puntuacion y detalles
    """
    # Construir filtros
    filters = {}
    if brand_filter:
        filters["brand"] = brand_filter
    if store_filter:
        filters["store_type"] = store_filter

    # Consultar
    query_result = query_new_image(image_url, filters=filters if filters else None)

    # Puntuar
    score = compute_display_score(query_result["results"])

    # Mostrar
    if show_details:
        filter_info = f" | Filtros: {filters}" if filters else ""
        show_explainability(image_url, query_result["results"], score)

    return score

# COMMAND ----------

# DBTITLE 1,Ejemplo 1: Evaluar display sin filtros
resultado_1 = score_display(f"{PATH_VOLUME}/displays/testing/display_test_good.jpg")

# COMMAND ----------

# DBTITLE 1,Ejemplo 2: Evaluar display filtrado por marca
resultado_2 = score_display(
    f"{PATH_VOLUME}/displays/display_campo_002.jpg",
    brand_filter="Coca-Cola"
)

# COMMAND ----------

# DBTITLE 1,Ejemplo 3: Evaluar display filtrado por tienda
resultado_3 = score_display(
    f"{PATH_VOLUME}/displays/display_campo_003.jpg",
    store_filter="supermercado"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Excelente! El sistema de puntuacion esta completo. Ahora vamos a evaluar la calidad y entender los costos.
# MAGIC
# MAGIC [Lab 05 - Evaluacion Costos y Limpieza]($./Lab 05 - Evaluacion Costos y Limpieza)
