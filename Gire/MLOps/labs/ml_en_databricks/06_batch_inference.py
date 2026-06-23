# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `ardemo_classic_dnubtw_catalog`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta celda valida acceso y crea tu schema si no existe.

# COMMAND ----------

# MAGIC %md
# MAGIC # Inferencia del Modelo de Churn
# MAGIC
# MAGIC ## Inferencia con el modelo Campeón
# MAGIC
# MAGIC Con los Modelos en Unity Catalog, pueden ser cargados para su uso en pipelines de inferencia por lotes. Las predicciones generadas pueden utilizarse para crear estrategias de retención de clientes o para análisis. El modelo en uso es el modelo __Campeón__, y vamos a cargarlo para usarlo en nuestro pipeline.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-5-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Recopilar datos de uso (visualización). Elimine para deshabilitar la recopilación o desactive el rastreador durante la instalación. Consulte el README para más detalles.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F01-mlops-quickstart%2F05_batch_inference&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F05_batch_inference&version=1&user_hash=f7ea13a45c991650d8df810431c3e0e2b12887e9ed7e206ee8fb6209bdb2ae82">

# COMMAND ----------

# MAGIC %run ./_resources/00-setup 
# MAGIC $setup_inference_data=true

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## Desplegando el modelo para batch inference
# MAGIC
# MAGIC <!--img style="float: right; margin-left: 20px" width="600" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn_batch_inference.gif" /-->
# MAGIC
# MAGIC Ahora que nuestro modelo está disponible en el Unity Catalog Model Registry, podemos cargarlo para calcular nuestras inferencias y guardarlas en una tabla para comenzar a construir dashboards.
# MAGIC
# MAGIC Usaremos la función de MLflow para cargar un UDF de pyspark y distribuir nuestra inferencia en todo el clúster. Podemos cargar el modelo con Python puro y usar un DataFrame de Pandas si los datos son pequeños.
# MAGIC
# MAGIC Si no sabes cómo empezar, puedes obtener un código de ejemplo desde la página de __"Artifacts"__ del experimento del modelo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejecutar inferencias

# COMMAND ----------

# MAGIC %md
# MAGIC ### Primero, reinstalemos los requisitos del modelo

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository

requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.mlops_churn@Champion").download_artifacts(artifact_path="requirements.txt") # download model from remote registry

# COMMAND ----------

# MAGIC %pip install --quiet -r $requirements_path
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Batch Inference en el modelo Champion
# MAGIC
# MAGIC Estamos listos para ejecutar la inferencia en el modelo Champion. Cargaremos el modelo como un UDF de Spark y generaremos predicciones para nuestros registros de clientes.
# MAGIC
# MAGIC Para simplificar, asumimos que las características ya han sido extraídas para los nuevos registros de clientes y almacenadas en la tabla de características. Normalmente, esto se realiza mediante pipelines de ingeniería de características separados.

# COMMAND ----------

# DBTITLE 1,In a Python notebook
import subprocess
subprocess.check_call(["pip", "install", "lightgbm", "-q"])

import mlflow


# Load customer features to be scored
inference_df = spark.read.table(f"mlops_churn_inference")
# Load champion model directly (pandas-based prediction for serverless compatibility)
champion_model = mlflow.pyfunc.load_model(model_uri=f"models:/{catalog}.{db}.mlops_churn@Champion")

# Get input column names from model schema
input_cols = champion_model.metadata.get_input_schema().input_names()

# Batch score using pandas
inference_pd = inference_df.toPandas()
inference_pd['predictions'] = champion_model.predict(inference_pd[input_cols])
preds_df = spark.createDataFrame(inference_pd)

display(preds_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ¡Eso es todo! Ahora nuestros datos pueden ser guardados como una tabla y reutilizados por el equipo de Análisis de Datos / Marketing para tomar acciones especiales y reducir el riesgo de Churn en estos clientes.
# MAGIC
# MAGIC ¡Tus datos también estarán disponibles en Genie para responder cualquier pregunta relacionada con churn usando texto en inglés sencillo!

# COMMAND ----------

# MAGIC %md
# MAGIC ### Conclusión
# MAGIC
# MAGIC ¡Esto es todo para la demostración rápida! Hemos revisado los conceptos básicos de MLOps y cómo Databricks te ayuda a lograrlos. Estos incluyen:
# MAGIC
# MAGIC - Ingeniería de características y almacenamiento de tablas de características con etiquetas en Databricks
# MAGIC - AutoML, entrenamiento de modelos y seguimiento de experimentos en MLflow
# MAGIC - Registro de modelos como Modelos en Unity Catalog para uso gobernado
# MAGIC - Validación de modelos, pruebas Champion-Challenger y promoción de modelos
# MAGIC - Batch Inference cargando el modelo como un UDF de PySpark
# MAGIC
# MAGIC Esperamos que hayas disfrutado esta demostración. Como siguiente paso, busca nuestra demostración avanzada de MLOps de extremo a extremo, que incluirá recorridos más detallados sobre los siguientes aspectos de MLOps: https://www.databricks.com/resources/demos/tutorials/data-science-and-ai/mlops-end-to-end-pipeline?itm_data=demo_center 
# MAGIC
# MAGIC - Servir características y Feature Store
# MAGIC - Monitoreo de datos y modelos
# MAGIC - Despliegue para inferencia en tiempo real
