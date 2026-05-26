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

CATALOG = catalog = CATALOGO = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = db = schema = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
try:
    spark.conf.set("c.catalog", CATALOG)
    spark.conf.set("c.schema", SCHEMA)
except Exception:
    pass  # Not available on Serverless

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"User:    {_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Validación del modelo Challenger
# MAGIC
# MAGIC Este notebook realiza tareas de validación en el modelo candidato __Challenger__.
# MAGIC
# MAGIC Pasa por algunos pasos para validar el modelo antes de etiquetarlo (asignándole el alias) como `Challenger`.
# MAGIC
# MAGIC Cuando las organizaciones comienzan a implementar procesos de MLOps, deberían considerar tener un "humano en el circuito" para realizar análisis visuales y validar los modelos antes de promoverlos. A medida que se familiarizan con el proceso, pueden considerar automatizar los pasos en un __Workflow__. El beneficio de la automatización es asegurar que estas comprobaciones de validación se realicen sistemáticamente antes de que los nuevos modelos se integren en los pipelines de inferencia o se desplieguen para el servicio en tiempo real. Por supuesto, las organizaciones pueden optar por mantener un "humano en el circuito" en cualquier parte del proceso y establecer el grado de automatización que se adapte a sus necesidades empresariales.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-4-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC *Nota: en una configuración típica de MLOps, esto se ejecutaría como parte de un trabajo automatizado para validar un nuevo modelo. Ejecutaremos esta sencilla demostración como un notebook interactivo.*
# MAGIC
# MAGIC <!-- Recopilar datos de uso (vista). Elimínelo para desactivar la recopilación o desactive el rastreador durante la instalación. Consulte el README para más detalles.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F01-mlops-quickstart%2F04_challenger_validation&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F04_challenger_validation&version=1&user_hash=f7ea13a45c991650d8df810431c3e0e2b12887e9ed7e206ee8fb6209bdb2ae82">

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Comprobaciones Generales de Validación
# MAGIC
# MAGIC <!--img style="float: right" src="https://github.com/QuentinAmbard/databricks-demo/raw/main/retail/resources/images/churn-mlflow-webhook-1.png" width=600 -->
# MAGIC
# MAGIC En el contexto de MLOps, existen más pruebas que simplemente la precisión de un modelo. Para garantizar la estabilidad de nuestro sistema de ML y el cumplimiento de cualquier requisito normativo, someteremos cada modelo añadido al registro a una serie de comprobaciones de validación. Estas incluyen, pero no se limitan a:
# MAGIC <br>
# MAGIC * __Documentación del modelo__
# MAGIC * __Inferencia sobre datos de producción__
# MAGIC * __Pruebas Champion-Challenger para asegurar que los KPIs de negocio sean aceptables__
# MAGIC
# MAGIC En este notebook, exploramos algunos enfoques para realizar estas pruebas y cómo podemos añadir metadatos a nuestros modelos etiquetando si han pasado una prueba determinada.
# MAGIC
# MAGIC Esta parte suele ser específica para tu línea de negocio y requisitos de calidad.
# MAGIC
# MAGIC Para cada prueba, agregaremos información usando etiquetas para saber qué se ha validado en el modelo. También podemos añadir comentarios a un modelo si es necesario.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

from mlflow.store.artifact.models_artifact_repo import ModelsArtifactRepository


requirements_path = ModelsArtifactRepository(f"models:/{catalog}.{db}.mlops_churn@Challenger").download_artifacts(artifact_path="requirements.txt") # download model from remote registry

# COMMAND ----------

# MAGIC %md
# MAGIC ## Obtener información del modelo
# MAGIC
# MAGIC Obtendremos la información del modelo __Challenger__ desde Unity Catalog.

# COMMAND ----------

# We are interested in validating the Challenger model
model_alias = "Challenger"
model_name = f"{catalog}.{db}.mlops_churn"

client = MlflowClient()
model_details = client.get_model_version_by_alias(model_name, model_alias)
model_version = int(model_details.version)

print(f"Validating {model_alias} model for {model_name} on model version {model_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobaciones del modelo

# COMMAND ----------

# MAGIC %md
# MAGIC #### Verificación de la descripción
# MAGIC
# MAGIC ¿El científico de datos proporcionó una descripción del modelo que está siendo enviado?

# COMMAND ----------

# If there's no description or an insufficient number of characters, tag accordingly
if not model_details.description:
  has_description = False
  print("Please add model description")
elif not len(model_details.description) > 20:
  has_description = False
  print("Please add detailed model description (40 char min).")
else:
  has_description = True

print(f'Model {model_name} version {model_details.version} has description: {has_description}')
client.set_model_version_tag(name=model_name, version=str(model_details.version), key="has_description", value=has_description)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Métrica de desempeño del modelo
# MAGIC
# MAGIC Queremos validar la métrica de desempeño del modelo. Normalmente, queremos comparar esta métrica obtenida para el modelo Challenger contra la del modelo Champion. Como aún no hemos registrado un modelo Champion, solo recuperaremos la métrica para el modelo Challenger sin hacer una comparación.
# MAGIC
# MAGIC El modelo registrado captura información sobre la ejecución del experimento MLflow, donde las métricas del modelo se registraron durante el entrenamiento. Esto le brinda trazabilidad desde el modelo implementado hasta las ejecuciones de entrenamiento iniciales.
# MAGIC
# MAGIC Aquí, usaremos el puntaje F1 para el conjunto de datos de prueba reservado durante el entrenamiento.

# COMMAND ----------

model_run_id = model_details.run_id
f1_score = mlflow.get_run(model_run_id).data.metrics['val_f1_score']

try:
    #Compare the challenger f1 score to the existing champion if it exists
    champion_model = client.get_model_version_by_alias(model_name, "Champion")
    champion_f1 = mlflow.get_run(champion_model.run_id).data.metrics['val_f1_score']
    print(f'Champion f1 score: {champion_f1}. Challenger f1 score: {f1_score}.')
    metric_f1_passed = f1_score >= champion_f1
except:
    print(f"No Champion found. Accept the model as it's the first one.")
    metric_f1_passed = True

print(f'Model {model_name} version {model_details.version} metric_f1_passed: {metric_f1_passed}')
# Tag that F1 metric check has passed
client.set_model_version_tag(name=model_name, version=model_details.version, key="metric_f1_passed", value=metric_f1_passed)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Métricas de referencia o de negocio en el conjunto de datos de evaluación
# MAGIC
# MAGIC Vamos a usar nuestro conjunto de datos de validación para verificar el impacto potencial del nuevo modelo.
# MAGIC
# MAGIC ***Nota: Esto es solo para evaluar nuestros modelos, no debe confundirse con la prueba A/B**. La prueba A/B se realiza en línea, dividiendo el tráfico entre 2 modelos. Requiere un ciclo de retroalimentación para evaluar el efecto de la predicción (por ejemplo, después de una predicción, ¿el descuento que ofrecimos al cliente evitó el churn?). Cubriremos la prueba A/B en la parte avanzada.*

# COMMAND ----------

# DBTITLE 1,Cell 17
import pyspark.sql.functions as F
import mlflow

# Get the eval dataset and convert to pandas once
eval_df = spark.table('mlops_churn_training').filter("split='test'")
eval_pdf = eval_df.toPandas()

print(f"Loaded evaluation dataset with {len(eval_pdf)} rows")

# COMMAND ----------

# DBTITLE 1,Install lightgbm dependency
# MAGIC %pip install lightgbm --quiet

# COMMAND ----------

# DBTITLE 1,Cell 18
import subprocess, sys
subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'lightgbm', '-q'])

import pandas as pd
import plotly.express as px
from sklearn.metrics import confusion_matrix
import mlflow

# Note: this is over-simplified and depends on your use-case, but the idea is to evaluate our model against business metrics
cost_of_customer_churn = 2000  # in dollar
cost_of_discount = 500  # in dollar

cost_true_negative = 0  # did not churn, we did not give him the discount
cost_false_negative = cost_of_customer_churn  # did churn, we lost the customer
cost_true_positive = cost_of_customer_churn - cost_of_discount  # We avoided churn with the discount
cost_false_positive = -cost_of_discount  # doesn't churn, we gave the discount for free

# Get input columns (exclude target and split columns)
exclude_cols = ['churn', 'split']
input_cols = [col for col in eval_pdf.columns if col not in exclude_cols]

# Check if Champion model exists
is_champ_model_exist = True
try:
    client.get_model_version_by_alias(f"{catalog}.{db}.mlops_churn", "Champion")
    print("Model already registered as Champion")
except Exception as error:
    print("An error occurred:", type(error).__name__, "- It means no champion model yet exists")
    is_champ_model_exist = False

# Calculate revenue gains
if is_champ_model_exist:
    # Load Champion model and predict
    champion_model = mlflow.pyfunc.load_model(f"models:/{catalog}.{db}.mlops_churn@Champion")
    champion_predictions = champion_model.predict(eval_pdf[input_cols])
    tn, fp, fn, tp = confusion_matrix(eval_pdf['churn'], champion_predictions).ravel()
    champion_potential_revenue_gain = tn * cost_true_negative + fp * cost_false_positive + fn * cost_false_negative + tp * cost_true_positive
    
    # Load Challenger model and predict
    challenger_model = mlflow.pyfunc.load_model(f"models:/{catalog}.{db}.mlops_churn@Challenger")
    challenger_predictions = challenger_model.predict(eval_pdf[input_cols])
    tn, fp, fn, tp = confusion_matrix(eval_pdf['churn'], challenger_predictions).ravel()
    challenger_potential_revenue_gain = tn * cost_true_negative + fp * cost_false_positive + fn * cost_false_negative + tp * cost_true_positive
else:
    print(f"No Champion found. Accept the model as it's the first one.")
    champion_potential_revenue_gain = 0
    
    # Load Challenger model and predict
    challenger_model = mlflow.pyfunc.load_model(f"models:/{catalog}.{db}.mlops_churn@Challenger")
    challenger_predictions = challenger_model.predict(eval_pdf[input_cols])
    tn, fp, fn, tp = confusion_matrix(eval_pdf['churn'], challenger_predictions).ravel()
    challenger_potential_revenue_gain = tn * cost_true_negative + fp * cost_false_positive + fn * cost_false_negative + tp * cost_true_positive

data = {'Model Alias': ['Challenger', 'Champion'],
        'Potential Revenue Gain': [challenger_potential_revenue_gain, champion_potential_revenue_gain]}

# Create a bar plot using plotly express
px.bar(data, x='Model Alias', y='Potential Revenue Gain', color='Model Alias',
    labels={'Potential Revenue Gain': 'Revenue Impacted'},
    title='Business Metrics - Revenue Impacted')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train a Second Model
# MAGIC
# MAGIC Run the cell below to execute the training notebook `02_3_train_lightGBM`. This will train a new LightGBM model and register it to Unity Catalog, creating a new model version.

# COMMAND ----------

# Pass hyperparameters as arguments to create a model with different F1 score
# Modify these values to experiment with different model performance:
training_params = {
    "learning_rate": "0.200",
    "max_bin": "400",      # Lower learning rate (default: 0.0678)
    "max_depth": "15",            # Shallower trees (default: 8)
    "num_leaves": "180",           # Fewer leaves (default: 100)
    "n_estimators": "500"         # Fewer trees (default: 250)
}

# Alternative parameter sets to try (uncomment one):
# Option 1: More aggressive model (potentially higher F1)
# training_params = {"learning_rate": "0.1", "max_depth": "10", "num_leaves": "120", "n_estimators": "300"}

# Option 2: More conservative model (potentially lower F1)
# training_params = {"learning_rate": "0.03", "max_depth": "5", "num_leaves": "50", "n_estimators": "150"}


# COMMAND ----------

# DBTITLE 1,Run training and registration
# Train a new model with different hyperparameters
print("Training new model with custom hyperparameters...")

dbutils.notebook.run("./03_train_lightGBM", timeout_seconds=600, arguments=training_params)

# Register the new model to Unity Catalog (creates new version)
print("\nRegistering model to Unity Catalog...")
dbutils.notebook.run("./02_4_from_notebook_to_models_in_uc", timeout_seconds=300)

print("\n✅ New model trained and registered successfully!")

# COMMAND ----------

# DBTITLE 1,Set new model as Challenger
# Get the latest model version and set it as Challenger
latest_version = client.search_model_versions(f"name='{catalog}.{db}.mlops_churn'")[0]

client.set_registered_model_alias(
    name=f"{catalog}.{db}.mlops_churn",
    alias="Challenger",
    version=latest_version.version
)

print(f"✅ Model version {latest_version.version} set as Challenger")
print(f"\nYou can now re-run the validation cells above (cells 17-18) to compare Champion vs Challenger")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resultados de validación
# MAGIC
# MAGIC ¡Eso es todo! Hemos demostrado algunas comprobaciones simples en el modelo. Veamos los resultados de la validación.

# COMMAND ----------

results = client.get_model_version(model_name, model_version)
results.tags

# COMMAND ----------

# MAGIC %md
# MAGIC ## Promoviendo el Challenger a Champion
# MAGIC
# MAGIC Cuando estemos satisfechos con los resultados del modelo __Challenger__, podemos promoverlo a Champion. Esto se hace estableciendo su alias como `@Champion`. Las canalizaciones de inferencia que cargan el modelo usando el alias `@Champion` cargarán entonces este nuevo modelo. El alias en el modelo Champion anterior, si existe, se eliminará automáticamente. El modelo mantiene su alias `@Challenger` hasta que se implemente un nuevo modelo Challenger con el alias para reemplazarlo.

# COMMAND ----------

if results.tags["has_description"] == "True" and results.tags["metric_f1_passed"] == "True":
  print('register model as Champion!')
  client.set_registered_model_alias(
    name=model_name,
    alias="Champion",
    version=model_version
  )
else:
  raise Exception("Model not ready for promotion")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ¡Felicidades! Nuestro modelo ha sido validado y promovido correctamente
# MAGIC
# MAGIC Ahora tenemos la certeza de que nuestro modelo está listo para ser utilizado en canalizaciones de inferencia y endpoints de servicio en tiempo real, ya que cumple con nuestros estándares de validación.
# MAGIC
# MAGIC Siguiente: [Ejecutar inferencia por lotes desde nuestro nuevo modelo Champion promovido]($./06_batch_inference)
