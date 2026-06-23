# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/02_train_and_register — Tarea del Job
# MAGIC Entrena LightGBM, lo trackea en MLflow y lo registra en UC como **@Challenger**.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Train LightGBM and register (tested)
import subprocess
subprocess.check_call(["pip", "install", "lightgbm", "-q"])

import mlflow
from mlflow.models import Model, infer_signature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
from mlflow import MlflowClient
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier

# --- Load data with lineage ---
latest_table_version = max(
    spark.sql(f"describe history {catalog}.{db}.mlops_churn_training").toPandas()["version"]
)
src_dataset = mlflow.data.load_delta(table_name=f"{catalog}.{db}.mlops_churn_training", version=str(latest_table_version))

label_col = "churn"
df_loaded = src_dataset.df.filter("split = 'train'").drop("customer_id", "split")
X = df_loaded.toPandas()
X_train, X_val, Y_train, Y_val = train_test_split(X.drop(label_col, axis=1), X[label_col], test_size=0.2, random_state=42)

# --- Preprocessors ---
bool_pipeline = Pipeline(steps=[
    ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
    ("imputers", ColumnTransformer([], remainder="passthrough")),
    ("cast_bool", FunctionTransformer(lambda df: df == "Yes")),
])
bool_transformers = [("boolean", bool_pipeline, ["senior_citizen", "partner", "dependents", "phone_service", "paperless_billing"])]

num_imputers = [("impute_mean", SimpleImputer(), ["monthly_charges", "num_optional_services", "tenure", "total_charges"])]
numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(lambda c: c.astype(float), axis=0))),
    ("imputers", ColumnTransformer(num_imputers, remainder="passthrough")),
    ("standardizer", StandardScaler()),
])
numerical_transformers = [("numerical", numerical_pipeline, ["monthly_charges", "num_optional_services", "tenure", "total_charges"])]

one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer([], remainder="passthrough")),
    ("one_hot_encoder", SklearnOneHotEncoder(handle_unknown="ignore")),
])
categorical_one_hot_transformers = [("onehot", one_hot_pipeline, ["contract", "device_protection", "internet_service", "multiple_lines", "online_backup", "online_security", "payment_method", "streaming_movies", "streaming_tv", "tech_support"])]

preprocessor = ColumnTransformer(bool_transformers + numerical_transformers + categorical_one_hot_transformers, remainder="drop", sparse_threshold=0)

# --- Train ---
experiment_name = f"{xp_path}/{xp_name}"
try:
    experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id
except Exception:
    experiment_id = mlflow.create_experiment(name=experiment_name)

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

run_name = params.pop("run_name", "light_gbm_baseline")
with mlflow.start_run(experiment_id=experiment_id, run_name=run_name) as mlflow_run:
    lgbmc_classifier = LGBMClassifier(**params)
    model = Pipeline([("preprocessor", preprocessor), ("classifier", lgbmc_classifier)])
    mlflow.sklearn.autolog(log_models=False, silent=True)
    model.fit(X_train, Y_train)
    signature = infer_signature(X_train, Y_train)
    mlflow.sklearn.log_model(model, "sklearn_model",
        input_example=X_train.iloc[0].to_dict(), signature=signature, serialization_format="cloudpickle")
    mlflow.log_input(src_dataset, context="training-input")
    # Evaluate
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    val_eval = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(label_col): Y_val}),
        targets=label_col, model_type="classifier",
        evaluator_config={"log_model_explainability": False, "metric_prefix": "val_", "pos_label": "Yes"}
    )
    val_f1 = val_eval.metrics["val_f1_score"]
    run_id = mlflow_run.info.run_id

# --- Register as Challenger ---
client = MlflowClient()
model_name = f"{catalog}.{db}.mlops_churn"
d = mlflow.register_model(f"runs:/{run_id}/sklearn_model", model_name)
client.update_registered_model(model_name, description="Predicts customer churn. Pipeline CP/MLOps.")
client.update_model_version(model_name, d.version, description=f"LightGBM. F1={round(val_f1*100,2)}%.")
client.set_model_version_tag(model_name, d.version, "f1_score", f"{round(val_f1,4)}")
client.set_registered_model_alias(model_name, "Challenger", d.version)
print(f"✓ registrado v{d.version} como @Challenger (F1={val_f1:.4f})")
