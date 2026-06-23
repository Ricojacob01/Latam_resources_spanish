# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — AutoML, Entrenamiento y Tracking 🤖📒
# MAGIC
# MAGIC Generamos un baseline con **AutoML** y entrenamos un **LightGBM** con **MLflow tracking**.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Lado a lado (UI ↔ API)**
# MAGIC AutoML es la **misma capacidad** desde la **UI glass-box** y desde la **API**. Lanza ambas y compara: la UI para explorar e iterar, la API para reproducir en CI/CD. Luego entrenamos un modelo propio en código con MLflow y lo comparamos en la **Experiments UI**.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — AutoML lado a lado
# MAGIC
# MAGIC **UI (🖱️):** Experiments → **Create AutoML Experiment** → *Classification* → dataset `mlops_churn_training`, target `churn`, *timeout* 10 min. AutoML genera trials + el **notebook glass-box** del mejor modelo.
# MAGIC
# MAGIC **API (código):** lo de abajo hace lo mismo de forma reproducible. (Descomenta para correrlo; tarda ~10 min.)

# COMMAND ----------

# DBTITLE 1,AutoML API (optional)
# from databricks import automl
# summary = automl.classify(
#     dataset=spark.table("mlops_churn_training").drop("split", "customer_id"),
#     target_col="churn",
#     timeout_minutes=10,
# )
# print("Mejor run AutoML:", summary.best_trial.mlflow_run_id)
print("AutoML (API) disponible arriba. Para el lab seguimos con el LightGBM de abajo (más rápido).")
print(f"Dataset: mlops_churn_training en {catalog}.{db}")

# COMMAND ----------

# DBTITLE 1,Paso 2 header
# MAGIC %md
# MAGIC ## Paso 2 — Entrenar LightGBM con MLflow tracking (código)
# MAGIC
# MAGIC Capturamos el linaje de datos, definimos preprocessors (bool, numérico, categórico), y entrenamos un LightGBM con evaluación completa en MLflow.

# COMMAND ----------

# DBTITLE 1,LightGBM training pipeline
import mlflow
from mlflow.models import Model, infer_signature, ModelSignature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
import sklearn
from sklearn import set_config
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier

# --- 1. Capturar linaje del dataset ---
latest_table_version = max(
    spark.sql(f"describe history {catalog}.{db}.mlops_churn_training").toPandas()["version"]
)
src_dataset = mlflow.data.load_delta(table_name=f"{catalog}.{db}.mlops_churn_training", version=str(latest_table_version))

# --- 2. Cargar datos de entrenamiento ---
label_col = "churn"
df_loaded = src_dataset.df.filter("split = 'train'").drop("customer_id", "split")
X = df_loaded.toPandas()
X_train, X_val, Y_train, Y_val = train_test_split(X.drop(label_col, axis=1), X[label_col], test_size=0.2, random_state=42)

# --- 3. Preprocessors ---
# Boolean columns
bool_imputers = []
bool_pipeline = Pipeline(steps=[
    ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
    ("imputers", ColumnTransformer(bool_imputers, remainder="passthrough")),
    ("cast_bool", FunctionTransformer(lambda df: df == "Yes")),
])
bool_transformers = [("boolean", bool_pipeline, ["senior_citizen", "partner", "dependents", "phone_service", "paperless_billing"])]

# Numeric columns
num_imputers = [("impute_mean", SimpleImputer(), ["monthly_charges", "num_optional_services", "tenure", "total_charges"])]
numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(lambda c: c.astype(float), axis=0))),
    ("imputers", ColumnTransformer(num_imputers, remainder="passthrough")),
    ("standardizer", StandardScaler()),
])
numerical_transformers = [("numerical", numerical_pipeline, ["monthly_charges", "num_optional_services", "tenure", "total_charges"])]

# Categorical columns (one-hot)
one_hot_imputers = []
one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer(one_hot_imputers, remainder="passthrough")),
    ("one_hot_encoder", SklearnOneHotEncoder(handle_unknown="ignore")),
])
categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["contract", "device_protection", "internet_service", "multiple_lines", "online_backup", "online_security", "payment_method", "streaming_movies", "streaming_tv", "tech_support"])]

transformers = bool_transformers + numerical_transformers + categorical_one_hot_transformers
preprocessor = ColumnTransformer(transformers, remainder="drop", sparse_threshold=0)

# --- 4. Configurar experimento MLflow ---
experiment_name = f"{xp_path}/{xp_name}"
try:
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
except Exception:
    print(f"Creating experiment: {experiment_name}")
    experiment_id = mlflow.create_experiment(name=experiment_name, tags={"dbdemos": "quickstart"})

# --- 5. Función de entrenamiento ---
def train_fn(params):
  with mlflow.start_run(experiment_id=experiment_id, run_name=params["run_name"]) as mlflow_run:
    lgbmc_classifier = LGBMClassifier(**params)
    model = Pipeline([
        ("preprocessor", preprocessor),
        ("classifier", lgbmc_classifier),
    ])
    mlflow.sklearn.autolog(log_models=False, silent=True)
    model.fit(X_train, Y_train)
    signature = infer_signature(X_train, Y_train)
    mlflow.sklearn.log_model(
        model, "sklearn_model",
        input_example=X_train.iloc[0].to_dict(),
        signature=signature,
        serialization_format="cloudpickle",
    )
    # Log training dataset lineage
    mlflow.log_input(src_dataset, context="training-input")
    # Evaluate on training
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    training_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_train.assign(**{str(label_col): Y_train}),
        targets=label_col,
        model_type="classifier",
        evaluator_config={"log_model_explainability": False, "metric_prefix": "training_", "pos_label": "Yes"}
    )
    # Evaluate on validation
    val_eval_result = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(label_col): Y_val}),
        targets=label_col,
        model_type="classifier",
        evaluator_config={"log_model_explainability": False, "metric_prefix": "val_", "pos_label": "Yes"}
    )
    return {
      "loss": -val_eval_result.metrics["val_f1_score"],
      "val_metrics": val_eval_result.metrics,
      "model": model,
      "run": mlflow_run,
    }

# --- 6. Hiperparámetros y entrenamiento ---
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

training_results = train_fn(params)
print(f"✓ val_f1_score = {-training_results['loss']:.4f}")
print(f"  Experiment: {experiment_name}")

# COMMAND ----------

# DBTITLE 1,Paso 3 - compare
# MAGIC %md
# MAGIC ## Paso 3 — Comparar en la Experiments UI (🖱️)
# MAGIC
# MAGIC 1. **Sidebar → Experiments → `mlops_experiment_<tu_usuario>`**.
# MAGIC 2. Ordena los runs por `val_f1_score`. Abre el run `light_gbm_baseline`: params, métricas, **artefacto del modelo**, `input_example` y **signature**.
# MAGIC 3. Revisa el **Dataset lineage** — verás `mlops_churn_training` vinculado al run.
# MAGIC 4. (Si corriste AutoML) compara el F1 de tu LightGBM contra el mejor trial de AutoML.
# MAGIC
# MAGIC ## Continuar → `04 - Registro en UC y Champion-Challenger`
