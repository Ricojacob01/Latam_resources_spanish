# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On LAB 02 - Generacion de Embeddings de Imagenes
# MAGIC
# MAGIC En este laboratorio aprenderemos a generar embeddings de imagenes utilizando **Model Serving**.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos del Ejercicio
# MAGIC
# MAGIC - Crear un endpoint de Model Serving para generar embeddings de imagenes
# MAGIC - Generar embeddings para todas las imagenes de referencia
# MAGIC - Normalizar los vectores para busqueda por similitud coseno
# MAGIC - Escribir los embeddings de vuelta en la tabla Delta
# MAGIC
# MAGIC ### Por que self-managed embeddings?
# MAGIC
# MAGIC Databricks Vector Search soporta imagenes y contenido no-textual a traves de **self-managed embeddings**.
# MAGIC Esto significa que nosotros pre-calculamos los embeddings y los almacenamos en la tabla Delta,
# MAGIC en lugar de dejar que Vector Search los genere automaticamente (lo cual solo funciona para texto).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion
# MAGIC
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el cluster: **Serverless**

# COMMAND ----------

# MAGIC %run "../config"

# COMMAND ----------

# MAGIC %pip install mlflow typing_extensions --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
import numpy as np
import requests
import base64
import json
import time
from io import BytesIO
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, FloatType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01 - Endpoint de Embedding de Imagenes
# MAGIC
# MAGIC Para generar embeddings de imagenes, utilizaremos un modelo **CLIP** (Contrastive Language-Image Pre-Training)
# MAGIC desplegado en **Model Serving**. CLIP genera vectores que representan el contenido visual de una imagen
# MAGIC en un espacio de alta dimension.
# MAGIC
# MAGIC ### Opciones de modelos para embeddings de imagenes:
# MAGIC
# MAGIC | Modelo | Dimension | Descripcion |
# MAGIC |--------|-----------|-------------|
# MAGIC | OpenAI CLIP ViT-B/32 | 512 | Balance entre velocidad y calidad |
# MAGIC | OpenAI CLIP ViT-L/14 | 768 | Mayor calidad, mas lento |
# MAGIC | SigLIP | 768 | Alternativa moderna a CLIP |

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Registrar el modelo CLIP en Unity Catalog
# MAGIC
# MAGIC Primero registramos un modelo CLIP usando MLflow para poder desplegarlo en Model Serving.

# COMMAND ----------

# DBTITLE 1,Definir el modelo wrapper de CLIP
import mlflow.pyfunc

class CLIPImageEmbedder(mlflow.pyfunc.PythonModel):
    """
    Modelo wrapper que genera embeddings de imagenes usando CLIP.
    Acepta URLs de imagenes o imagenes codificadas en base64.
    """
    def load_context(self, context):
        import torch
        import clip
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model, self.preprocess = clip.load(context.artifacts["clip_weights"], device=self.device)

    def predict(self, context, model_input):
        import torch
        from PIL import Image
        import requests
        from io import BytesIO
        import base64
        import numpy as np

        results = []

        for _, row in model_input.iterrows():
            image_input = row.get("image_url", row.get("image_base64", ""))

            try:
                # Cargar imagen desde URL o base64
                if image_input.startswith("http"):
                    response = requests.get(image_input, timeout=10)
                    image = Image.open(BytesIO(response.content)).convert("RGB")
                elif image_input.startswith("/Volumes") or image_input.startswith("/dbfs"):
                    image = Image.open(image_input).convert("RGB")
                else:
                    image_bytes = base64.b64decode(image_input)
                    image = Image.open(BytesIO(image_bytes)).convert("RGB")

                # Generar embedding
                image_tensor = self.preprocess(image).unsqueeze(0).to(self.device)
                with torch.no_grad():
                    embedding = self.model.encode_image(image_tensor)

                # Normalizar para similitud coseno (VS usa L2)
                embedding = embedding / embedding.norm(dim=-1, keepdim=True)
                results.append(embedding.cpu().numpy().flatten().tolist())
            except Exception as e:
                print(f"Error procesando imagen: {e}")
                results.append([0.0] * 512)  # Vector cero como fallback

        return results

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Registrar y desplegar el modelo
# MAGIC
# MAGIC **Nota:** En un workshop real, el instructor puede pre-desplegar este endpoint para ahorrar tiempo (~10 min de provision).

# COMMAND ----------

# DBTITLE 1,Registrar el modelo en Unity Catalog
import pandas as pd
import os
import urllib.request
from mlflow.models import infer_signature

mlflow.set_registry_uri("databricks-uc")
model_name = f"{CATALOG}.{SCHEMA}.clip_image_embedder"

# Descargar pesos de CLIP ViT-B/32 localmente para empaquetar como artefacto
clip_url = "https://openaipublic.azureedge.net/clip/models/40d365715913c9da98579312b702a82c18be219cc2a73407c4526f58eba950af/ViT-B-32.pt"
clip_weights_path = "/tmp/ViT-B-32.pt"

if not os.path.exists(clip_weights_path):
    print("Descargando pesos de CLIP ViT-B/32...")
    urllib.request.urlretrieve(clip_url, clip_weights_path)
    print("Descarga completada!")
else:
    print("Pesos de CLIP ya descargados.")

input_example = pd.DataFrame({"image_url": ["https://example.com/image.jpg"]})
signature = infer_signature(
    model_input=input_example,
    model_output=[[0.0] * 512]
)

with mlflow.start_run():
    model_info = mlflow.pyfunc.log_model(
        artifact_path="clip_embedder",
        python_model=CLIPImageEmbedder(),
        artifacts={"clip_weights": clip_weights_path},
        pip_requirements=[
            "torch",
            "clip @ git+https://github.com/openai/CLIP.git",
            "Pillow",
            "requests",
            "numpy"
        ],
        signature=signature,
        input_example=input_example,
        registered_model_name=model_name
    )

print(f"Modelo registrado: {model_name}")

# COMMAND ----------

# DBTITLE 1,Crear endpoint de Model Serving
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    AutoCaptureConfigInput
)

w = WorkspaceClient()

# Obtener la ultima version del modelo
latest_version = max(
    [v.version for v in w.model_versions.list(model_name)],
    default="1"
)

served_entities = [
    ServedEntityInput(
        entity_name=model_name,
        entity_version=latest_version,
        workload_size="Small",
        scale_to_zero_enabled=True,
    )
]

try:
    w.serving_endpoints.create_and_wait(
        name=EMBEDDING_ENDPOINT,
        config=EndpointCoreConfigInput(
            served_entities=served_entities
        ),
    )
    print(f"Endpoint '{EMBEDDING_ENDPOINT}' creado exitosamente!")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"Endpoint '{EMBEDDING_ENDPOINT}' ya existe. Actualizando configuracion...")
        w.serving_endpoints.update_config_and_wait(
            name=EMBEDDING_ENDPOINT,
            served_entities=served_entities,
        )
        print(f"Endpoint '{EMBEDDING_ENDPOINT}' actualizado exitosamente!")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Generar embeddings para imagenes de referencia
# MAGIC
# MAGIC Ahora vamos a generar embeddings para todas nuestras imagenes de referencia usando el endpoint de Model Serving.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Funcion auxiliar para llamar al endpoint

# COMMAND ----------

# DBTITLE 1,Funcion para obtener embeddings via Model Serving
def get_image_embedding(image_url: str) -> list:
    """
    Llama al endpoint de Model Serving para obtener el embedding de una imagen.

    Args:
        image_url: URL o ruta de la imagen

    Returns:
        Lista de floats representando el embedding
    """
    import pandas as pd

    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    host = spark.conf.get("spark.databricks.workspaceUrl")

    url = f"https://{host}/serving-endpoints/{EMBEDDING_ENDPOINT}/invocations"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    payload = {
        "dataframe_records": [{"image_url": image_url}]
    }

    response = requests.post(url, headers=headers, json=payload, timeout=60)
    response.raise_for_status()

    result = response.json()
    embedding = result["predictions"][0]

    return embedding


def normalize_vector(vec: list) -> list:
    """Normaliza un vector para que tenga norma unitaria (similitud coseno con L2)."""
    arr = np.array(vec, dtype=np.float32)
    norm = np.linalg.norm(arr)
    if norm > 0:
        arr = arr / norm
    return arr.tolist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Generar embeddings en batch
# MAGIC
# MAGIC **Nota sobre normalizacion:** Vector Search usa distancia L2 por defecto.
# MAGIC Si normalizamos los vectores a norma unitaria, minimizar L2 equivale a maximizar similitud coseno.
# MAGIC Esto es importante para obtener rankings correctos basados en similitud visual.

# COMMAND ----------

# DBTITLE 1,Generar embeddings para todas las imagenes de referencia
# Para el workshop, generamos embeddings simulados si el endpoint no esta listo
# En produccion, descomentar la version real

def generate_embeddings_batch(df):
    """Genera embeddings para un DataFrame de displays."""
    rows = df.collect()
    embeddings = []

    for i, row in enumerate(rows):
        try:
            # --- Version produccion: usar Model Serving ---
            # emb = get_image_embedding(row.image_url)
            # emb = normalize_vector(emb)

            # --- Version workshop: embeddings simulados para demostracion ---
            np.random.seed(row.display_id * 42)
            base_emb = np.random.randn(EMBEDDING_DIM).astype(np.float32)

            # Ajustar embeddings segun quality_label para que la similitud sea realista
            quality_shift = {
                "excelente": 1.0,
                "bueno": 0.7,
                "regular": 0.3,
                "deficiente": -0.3
            }
            shift = quality_shift.get(row.quality_label, 0.0)
            base_emb[:10] += shift  # Sesgar las primeras dimensiones

            emb = normalize_vector(base_emb.tolist())
            embeddings.append((row.display_id, emb))

            if (i + 1) % 5 == 0:
                print(f"  Procesados {i + 1}/{len(rows)} imagenes...")

        except Exception as e:
            print(f"  Error en display_id {row.display_id}: {e}")
            zero_emb = [0.0] * EMBEDDING_DIM
            embeddings.append((row.display_id, zero_emb))

    return embeddings

# Cargar tabla de referencia
df_ref = spark.table(f"{PATH_TABLE}.displays_referencia")

print("Generando embeddings...")
embedding_results = generate_embeddings_batch(df_ref)
print(f"Embeddings generados: {len(embedding_results)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Escribir embeddings en la tabla Delta

# COMMAND ----------

# DBTITLE 1,Agregar columna de embeddings a la tabla
from pyspark.sql.types import StructType, StructField, IntegerType, ArrayType, FloatType

# Crear DataFrame con embeddings
emb_schema = StructType([
    StructField("display_id", IntegerType(), False),
    StructField("embedding", ArrayType(FloatType()), False),
])

df_embeddings = spark.createDataFrame(embedding_results, schema=emb_schema)

# Unir con la tabla de referencia y sobrescribir
df_final = df_ref.join(df_embeddings, on="display_id", how="inner")

df_final.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{PATH_TABLE}.displays_referencia"
)

# Re-habilitar CDF despues de overwrite
spark.sql(f"""
    ALTER TABLE {PATH_TABLE}.displays_referencia
    SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

print("Embeddings escritos en la tabla Delta!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Verificar los embeddings

# COMMAND ----------

# DBTITLE 1,Verificar dimensiones de embeddings
df_check = spark.table(f"{PATH_TABLE}.displays_referencia")
display(
    df_check.select(
        "display_id", "brand", "quality_label",
        F.size("embedding").alias("embedding_dim")
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Excelente! Los embeddings estan listos. Ahora vamos a crear el indice de Vector Search.
# MAGIC
# MAGIC [Lab 03 - Endpoint e Indice de Vector Search]($./Lab 03 - Endpoint e Indice de Vector Search)
