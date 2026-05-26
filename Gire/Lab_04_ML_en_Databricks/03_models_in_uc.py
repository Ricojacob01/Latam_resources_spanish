# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>

# COMMAND ----------

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
spark.sql(f"USE SCHEMA {CATALOG}.{SCHEMA}")
spark.conf.set("c.catalog", CATALOG)
spark.conf.set("c.schema", SCHEMA)

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"User:    {_user}")


# MAGIC %md
# MAGIC # Gestión del ciclo de vida del modelo en Unity Catalog
# MAGIC
# MAGIC Uno de los principales desafíos entre los científicos de datos e ingenieros de ML es la ausencia de un repositorio central para modelos, sus versiones y los medios para gestionarlos a lo largo de su ciclo de vida.
# MAGIC
# MAGIC [Modelos en Unity Catalog](https://docs.databricks.com/en/mlflow/models-in-uc.html) aborda este desafío y permite a los miembros del equipo de datos:
# MAGIC <br>
# MAGIC * **Descubrir** modelos registrados, alias actuales en el desarrollo de modelos, ejecuciones de experimentos y código asociado con un modelo registrado
# MAGIC * **Promocionar** modelos a diferentes fases de su ciclo de vida mediante el uso de alias de modelo
# MAGIC * **Etiquetar** modelos para capturar metadatos específicos de tu proceso de MLOps
# MAGIC * **Desplegar** diferentes versiones de un modelo registrado, ofreciendo a los ingenieros de MLOps la capacidad de desplegar y realizar pruebas de diferentes versiones de modelos
# MAGIC * **Probar** modelos de forma automatizada
# MAGIC * **Documentar** modelos a lo largo de su ciclo de vida
# MAGIC * **Asegurar** el acceso y los permisos para el registro, ejecución o modificaciones de modelos
# MAGIC
# MAGIC Veremos cómo probamos y promocionamos un nuevo modelo __Challenger__ como candidato para reemplazar un modelo __Champion__ existente.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-3-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Recopilar datos de uso (vista). Elimínalo para deshabilitar la recopilación o desactiva el rastreador durante la instalación. Consulta el README para más detalles.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F01-mlops-quickstart%2F03_from_notebook_to_models_in_uc&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F03_from_notebook_to_models_in_uc&version=1&user_hash=f7ea13a45c991650d8df810431c3e0e2b12887e9ed7e206ee8fb6209bdb2ae82">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo usar Modelos en Unity Catalog
# MAGIC Típicamente, los científicos de datos que usan MLflow realizarán muchos experimentos, cada uno con varias ejecuciones que rastrean y registran métricas y parámetros. Durante el ciclo de desarrollo, seleccionarán la mejor ejecución dentro de un experimento y registrarán su modelo en Unity Catalog. Piensa en esto como **hacer commit** del modelo en Unity Catalog, de la misma manera que harías commit de código en un sistema de control de versiones.
# MAGIC
# MAGIC Unity Catalog propone alias de modelo en texto libre, por ejemplo, `Baseline`, `Challenger`, `Champion`, junto con el uso de etiquetas.
# MAGIC
# MAGIC Los usuarios con los permisos apropiados pueden crear modelos, modificar alias y etiquetas, usar modelos, etc.

# COMMAND ----------

# DBTITLE 1,Install MLflow version for model lineage in UC [for MLR < 15.2]
# MAGIC %pip install --quiet mlflow --upgrade
# MAGIC
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Encontrar programáticamente la mejor ejecución y registrar el modelo en Unity Catalog para validación
# MAGIC
# MAGIC Hemos completado las ejecuciones de entrenamiento para encontrar un modelo candidato __Challenger__. Seleccionaremos programáticamente el mejor modelo de nuestro último experimento de ML y lo registraremos en Unity Catalog. Podemos hacerlo fácilmente usando la API `search_runs` de MLflow:

# COMMAND ----------

model_name = f"{catalog}.{db}.mlops_churn"

# COMMAND ----------

print(f"Finding best run from {xp_name} and pushing new model version to {model_name}")
mlflow.set_experiment(f"{xp_path}/{xp_name}")

# COMMAND ----------

import mlflow

xp_name = "dbdemos_mlops_churn_demo_experiment"
print(f"Finding best run from {xp_name}_* and pushing new model version to {model_name}")

experiment_name = f"{xp_path}/{xp_name}"

mlflow.set_experiment(experiment_name)

experiment_id = mlflow.search_experiments(filter_string=f"name LIKE '{xp_path}/{xp_name}%'", order_by=["last_update_time DESC"])[0].experiment_id
print(experiment_id)

# COMMAND ----------

# Let's get our best ml run
best_model = mlflow.search_runs(
  experiment_ids=experiment_id,
  order_by=["metrics.val_f1_score DESC"],
  max_results=1,
  filter_string="status = 'FINISHED' and run_name='light_gbm_baseline'" #filter on mlops_best_run to always use the notebook 02 to have a more predictable demo
)
# Optional: Load MLflow Experiment as a spark df and see all runs
#df = spark.read.format("mlflow-experiment").load(experiment_id)
#df.display()
best_model

# COMMAND ----------

# MAGIC %md
# MAGIC Una vez que tengamos nuestro mejor modelo, podemos registrarlo en el Unity Catalog Model Registry usando su run ID.

# COMMAND ----------

print(f"Registering model to {model_name}")  # {model_name} is defined in the setup script

# Get the run id from the best model
run_id = best_model.iloc[0]['run_id']

# Register the best model from experiments run to MLflow model registry
model_details = mlflow.register_model(f"runs:/{run_id}/sklearn_model", model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC En este punto, el modelo aún no tiene alias ni descripciones que indiquen su ciclo de vida y meta-datos/información. Vamos a actualizar esta información.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dar una descripción al modelo registrado
# MAGIC
# MAGIC Haremos esto de forma general para el modelo registrado.

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient()

# The main model description is typically done once.
client.update_registered_model(
  name=model_details.name,
  description="This model predicts whether a customer will churn using the features in the mlops_churn_training table. It is used to power the Telco Churn Dashboard in DB SQL.",
)

# COMMAND ----------

# MAGIC %md
# MAGIC Y agrega más detalles sobre la nueva versión que acabamos de registrar

# COMMAND ----------

# Provide more details on this specific model version
best_score = best_model['metrics.val_f1_score'].values[0]
run_name = best_model['tags.mlflow.runName'].values[0]
version_desc = f"This model version has an F1 validation metric of {round(best_score,4)*100}%. Follow the link to its training run for more details."

client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description=version_desc
)

# We can also tag the model version with the F1 score for visibility
client.set_model_version_tag(
  name=model_details.name,
  version=model_details.version,
  key="f1_score",
  value=f"{round(best_score,4)}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Establecer la última versión del modelo como modelo Baseline/Challenger
# MAGIC
# MAGIC Vamos a establecer esta nueva versión registrada del modelo como el modelo __Challenger__ _(o __Baseline__)_. Los modelos Challenger son candidatos para reemplazar el modelo Champion, que es el modelo actualmente en uso.
# MAGIC
# MAGIC Usaremos el alias del modelo para indicar la etapa en la que se encuentra en su ciclo de vida.

# COMMAND ----------

# Set this version as the Challenger model, using its model alias
client.set_registered_model_alias(
  name=model_name,
  alias="Challenger", # Baseline
  version=model_details.version
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Ahora, inspecciona visualmente las versiones del modelo en Unity Catalog Explorer. Deberías ver la descripción de la versión y el alias `Challenger` aplicado a la versión.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo: Validación del modelo Challenger
# MAGIC
# MAGIC En este punto, con el modelo __Challenger__ registrado, queremos validar el modelo. Los pasos de validación se implementan en un notebook para que el proceso de validación pueda ser automatizado como parte de un trabajo de Databricks Workflow.
# MAGIC
# MAGIC Si el modelo pasa todas las pruebas, será promovido a `Champion`.
# MAGIC
# MAGIC Próximo: Descubre cómo se está probando el modelo antes de ser promovido a `Champion` [usando el notebook de validación de modelos]($./02_5_challenger_validation)
