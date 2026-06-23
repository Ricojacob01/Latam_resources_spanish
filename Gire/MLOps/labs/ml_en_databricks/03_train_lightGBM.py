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
# MAGIC # Entrena un modelo de `LightGBM`
# MAGIC
# MAGIC Para esta demostración rápida, vamos a entrenar un modelo base de `LightGBM`.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-2-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Recopilar datos de uso (vista). Elimínelo para deshabilitar la recopilación o desactive el rastreador durante la instalación. Consulte el README para más detalles. -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F01-mlops-quickstart%2F02_train_lightGBM&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F02_train_lightGBM&version=1&user_hash=f7ea13a45c991650d8df810431c3e0e2b12887e9ed7e206ee8fb6209bdb2ae82">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilice el cluster «mlops_workshop_databricks» para ejecutar este notebook
# MAGIC Para ejecutar esta demostración, simplemente selecciona el cluster `mlops_workshop_databricks` en el menú desplegable.
# MAGIC

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC # LightGBM Classifier training

# COMMAND ----------

# DBTITLE 1,Set MLflow experiment
import mlflow

# xp_name is defined in _resources/00-setup
experiment_name = f"{xp_path}/{xp_name}"

try:
  experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id

except Exception as e:
  print(f"Creating experiment: {experiment_name}")
  experiment_id = mlflow.create_experiment(name=experiment_name, tags={"dbdemos":"quickstart"})

print(f"Experiment ID: {experiment_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Linaje de datos
# MAGIC Capturar el linaje de los datos de origen para un modelo permite que los equipos de ciencia de datos realicen análisis de causa raíz cuando se observan problemas en las predicciones de los modelos. El grafo de linaje puede examinarse a través de Unity Catalog y consultarse desde las System Tables.
# MAGIC
# MAGIC MLflow proporciona APIs para capturar este linaje. La captura del linaje implica los siguientes pasos:
# MAGIC
# MAGIC - Cargar el objeto que representa el conjunto de datos de entrenamiento desde Unity Catalog
# MAGIC
# MAGIC   `src_dataset = mlflow.data.load_delta(table_name=f'{catalog}.{db}.mlops_churn_training', version=latest)`
# MAGIC
# MAGIC - Registrar el objeto dataset como parte de la ejecución de entrenamiento
# MAGIC
# MAGIC   <br>
# MAGIC
# MAGIC   
# MAGIC    mlflow.start_run():
# MAGIC      ...
# MAGIC      mlflow.log_input(src_dataset, context="training-input")

# COMMAND ----------

# Load the dataset object from Unity Catalog
latest_table_version = max(
    spark.sql(f"describe history {catalog}.{db}.mlops_churn_training").toPandas()["version"]
)

src_dataset = mlflow.data.load_delta(table_name=f"{catalog}.{db}.mlops_churn_training", version=str(latest_table_version))

# COMMAND ----------

# DBTITLE 1,Load training dataset
df_loaded = src_dataset.df.filter("split = 'train'").drop("customer_id", "split")

# Preview data
display(df_loaded.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preprocessors

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columnas booleanas
# MAGIC Para cada columna, imputa los valores faltantes y luego conviértelos en unos y ceros.

# COMMAND ----------

from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder


bool_imputers = []

bool_pipeline = Pipeline(steps=[
    ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
    ("imputers", ColumnTransformer(bool_imputers, remainder="passthrough")),
    ("onehot", SklearnOneHotEncoder(handle_unknown="ignore", drop="first")),
])

bool_transformers = [("boolean", bool_pipeline, ["gender", "phone_service", "dependents", "senior_citizen", "paperless_billing", "partner"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columnas numéricas
# MAGIC
# MAGIC Los valores faltantes en las columnas numéricas se imputan con la media por defecto.

# COMMAND ----------

from sklearn.preprocessing import StandardScaler


num_imputers = []
num_imputers.append(("impute_mean", SimpleImputer(), ["monthly_charges", "num_optional_services", "tenure", "total_charges"]))

numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(pd.to_numeric, errors='coerce'))),
    ("imputers", ColumnTransformer(num_imputers)),
    ("standardizer", StandardScaler()),
])

numerical_transformers = [("numerical", numerical_pipeline, ["monthly_charges", "total_charges", "tenure", "num_optional_services"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Columnas categóricas

# COMMAND ----------

# MAGIC %md
# MAGIC #### Categóricas de baja cardinalidad
# MAGIC Convierte cada columna categórica de baja cardinalidad en múltiples columnas binarias mediante One Hot Encoder.
# MAGIC Para cada columna categórica de entrada (cadena o numérica), el número de columnas de salida es igual al número de valores únicos en la columna de entrada.

# COMMAND ----------

from sklearn.preprocessing import OneHotEncoder


one_hot_imputers = []
one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", OneHotEncoder(handle_unknown="ignore")),
])

categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["contract", "device_protection", "internet_service", "multiple_lines", "online_backup", "online_security", "payment_method", "streaming_movies", "streaming_tv", "tech_support"])]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Agrupar en un único pipeline

# COMMAND ----------

transformers = bool_transformers + numerical_transformers + categorical_one_hot_transformers
preprocessor = ColumnTransformer(transformers, remainder="drop", sparse_threshold=0)

# COMMAND ----------

# MAGIC %md
# MAGIC ## División de Entrenamiento - Validación - Prueba
# MAGIC - Entrenamiento (80% del conjunto de datos utilizado para entrenar el modelo)
# MAGIC - Validación (20% del conjunto de datos utilizado para ajustar los hiperparámetros del modelo)
# MAGIC
# MAGIC
# MAGIC Usamos esta columna para dividir el conjunto de datos en los tres conjuntos anteriores.
# MAGIC La columna no debe usarse para el entrenamiento, por lo que se elimina después de realizar la división.

# COMMAND ----------

from sklearn.model_selection import train_test_split


label_col = "churn"
X = df_loaded.toPandas()
X_train, X_val, Y_train, Y_val = train_test_split(X.drop(label_col, axis=1), X[label_col], test_size=0.2, random_state=42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Entrenar modelo de clasificación
# MAGIC - Registra métricas relevantes en MLflow para rastrear ejecuciones
# MAGIC - Todas las ejecuciones se registran bajo un experimento accesible desde la vista "Experimento" en el panel derecho de tu espacio de trabajo
# MAGIC - Cambia los parámetros del modelo y vuelve a ejecutar la celda de entrenamiento para registrar una prueba diferente en el experimento de MLflow
# MAGIC - Para ver la lista completa de hiperparámetros ajustables, consulta la salida de la celda siguiente

# COMMAND ----------

!pip install lightgbm

from lightgbm import LGBMClassifier


help(LGBMClassifier)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definir la función de entrenamiento

# COMMAND ----------

# DBTITLE 1,Cell 26
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
import sklearn
from sklearn import set_config
from sklearn.pipeline import Pipeline


def train_fn(params):
  with mlflow.start_run(experiment_id=experiment_id, run_name=params["run_name"]) as mlflow_run:
    lgbmc_classifier = LGBMClassifier(**params)

    model = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", lgbmc_classifier),
    ])

    # Enable automatic logging of input samples, metrics, parameters, and models
    mlflow.sklearn.autolog(log_models=False, silent=True)

    model.fit(X_train, Y_train)
    signature = infer_signature(X_train, Y_train)
    mlflow.sklearn.log_model(
        model, "sklearn_model",
        input_example=X_train.iloc[0].to_dict(),
        signature=signature,
        serialization_format="cloudpickle",
    )

    # Log training dataset object to capture upstream data lineage
    mlflow.log_input(src_dataset, context="training-input")

    # Log metrics for the training set
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(label_col):Y_train}),
        targets=label_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "training_" , "pos_label": "Yes" }
    )
    sklr_training_metrics = training_eval_result.metrics

    # Log metrics for the validation set
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(label_col):Y_val}),
        targets=label_col,
        model_type="classifier",
        evaluator_config = {"log_model_explainability": False,
                            "metric_prefix": "val_" , "pos_label": "Yes" }
    )
    sklr_val_metrics = val_eval_result.metrics

    loss = -sklr_val_metrics["val_f1_score"]

    # Truncate metric key names so they can be displayed together
    sklr_val_metrics = {k.replace("val_", ""): v for k, v in sklr_val_metrics.items()}

    return {
      "loss": loss,
      "val_metrics": sklr_val_metrics,
      "model": model,
      "run": mlflow_run,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configurar hiperparámetros del modelo
# MAGIC
# MAGIC Para documentación sobre los parámetros utilizados por el modelo en uso, consulta:
# MAGIC https://lightgbm.readthedocs.io/en/stable/pythonapi/lightgbm.LGBMClassifier.html
# MAGIC
# MAGIC NOTA: La URL anterior apunta a una versión estable de la documentación correspondiente a la última
# MAGIC versión publicada del paquete. La documentación puede diferir ligeramente de la versión del paquete
# MAGIC utilizada en este notebook.

# COMMAND ----------

params = {
  "run_name": "light_gbm_baseline",
  "colsample_bytree": 0.4120544919020157, 
  "lambda_l1": 2.6616074270114995,
  "lambda_l2": 514.9224373768443,
  "learning_rate": 0.0678497372371143,
  "max_bin": 229,
  "max_depth": 8,
  "min_child_samples": 66,
  "n_estimators": 250,
  "num_leaves": 100,
  "path_smooth": 61.06596877554017,
  "subsample": 0.6965257092078714,
  "random_state": 42,
}

# COMMAND ----------

# DBTITLE 1,Cell 29
training_results = train_fn(params)

# COMMAND ----------

loss = training_results["loss"]
model = training_results["model"]
print(f"Model loss: {loss}")

# COMMAND ----------

model

# COMMAND ----------

# MAGIC %md
# MAGIC ## Matriz de confusión, curva ROC y curva de Precisión-Recall para los datos de validación
# MAGIC
# MAGIC Mostramos la matriz de confusión, la curva ROC y la curva de Precisión-Recall del modelo en los datos de validación.
# MAGIC
# MAGIC Para ver los gráficos evaluados en los datos de entrenamiento y prueba, consulta los artefactos en la página de ejecución de MLflow.

# COMMAND ----------

mlflow_run = training_results["run"]

# COMMAND ----------

# Click the link to see the MLflow run page
displayHTML(f"<a href=#mlflow/experiments/{mlflow_run.info.experiment_id}/runs/{ mlflow_run.info.run_id }/artifactPath/model> Link to model run page </a>")

# COMMAND ----------

import os
import uuid
from IPython.display import Image


# Create temp directory to download MLflow model artifact
eval_temp_dir = os.path.join(os.environ["SPARK_LOCAL_DIRS"], "tmp", str(uuid.uuid4())[:8])
os.makedirs(eval_temp_dir, exist_ok=True)

# Download the artifact
eval_path = mlflow.artifacts.download_artifacts(run_id=mlflow_run.info.run_id, dst_path=eval_temp_dir)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Matriz de confusión para el conjunto de validación

# COMMAND ----------

eval_confusion_matrix_path = os.path.join(eval_path, "val_confusion_matrix.png")
display(Image(filename=eval_confusion_matrix_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Curva ROC para el conjunto de validación

# COMMAND ----------

eval_roc_curve_path = os.path.join(eval_path, "val_roc_curve_plot.png")
display(Image(filename=eval_roc_curve_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Curva de Precision-Recall para el conjunto de validación

# COMMAND ----------

eval_pr_curve_path = os.path.join(eval_path, "val_precision_recall_curve_plot.png")
display(Image(filename=eval_pr_curve_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Automatizar la validación de promoción de modelos
# MAGIC
# MAGIC Próximo paso: [Buscar ejecuciones y activar la validación de promoción de modelos]($./04_models_in_uc)
