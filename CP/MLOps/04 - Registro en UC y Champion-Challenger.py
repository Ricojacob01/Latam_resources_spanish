# Databricks notebook source
# DBTITLE 1,Intro with banner
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC
# MAGIC # 04 — Registro en UC y Champion/Challenger 🏛️
# MAGIC
# MAGIC Registramos el mejor modelo en Unity Catalog, le ponemos alias **@Challenger**, lo **validamos** y lo promovemos a **@Champion**.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Code → UI**
# MAGIC Registras y asignas alias por **API** (reproducible, automatizable en el Job); luego **gobiernas en *Models in Unity Catalog* (UI)**: versiones, alias, lineage, permisos, tags.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Registrar el mejor run como @Challenger (código)

# COMMAND ----------

# DBTITLE 1,Register best run as Challenger
import mlflow
from mlflow import MlflowClient
client = MlflowClient()

model_name = f"{catalog}.{db}.mlops_churn"
print(f"Finding best run from {xp_name} and pushing new model version to {model_name}")

# Set experiment and find the best run
experiment_name = f"{xp_path}/{xp_name}"
mlflow.set_experiment(experiment_name)
experiment_id = mlflow.search_experiments(filter_string=f"name LIKE '{xp_path}/{xp_name}%'", order_by=["last_update_time DESC"])[0].experiment_id

# Get best model by val_f1_score
best_model = mlflow.search_runs(
  experiment_ids=experiment_id,
  order_by=["metrics.val_f1_score DESC"],
  max_results=1,
  filter_string="status = 'FINISHED' and run_name='light_gbm_baseline'"
)

# Register the best model
run_id = best_model.iloc[0]['run_id']
print(f"Registering model to {model_name}")
model_details = mlflow.register_model(f"runs:/{run_id}/sklearn_model", model_name)

# Add description to the registered model
client.update_registered_model(
  name=model_details.name,
  description="This model predicts whether a customer will churn using the features in the mlops_churn_training table. It is used to power the Telco Churn Dashboard in DB SQL.",
)

# Add details to the version
best_score = best_model['metrics.val_f1_score'].values[0]
version_desc = f"This model version has an F1 validation metric of {round(best_score,4)*100}%. Follow the link to its training run for more details."
client.update_model_version(
  name=model_details.name,
  version=model_details.version,
  description=version_desc
)
client.set_model_version_tag(
  name=model_details.name,
  version=model_details.version,
  key="f1_score",
  value=f"{round(best_score,4)}"
)

# Set as Challenger
client.set_registered_model_alias(
  name=model_name,
  alias="Challenger",
  version=model_details.version
)
print(f"✓ Registrado {model_name} v{model_details.version} como @Challenger (F1={best_score:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Validar el Challenger (código)
# MAGIC
# MAGIC Checks: (1) descripción mínima, (2) F1 ≥ Champion (o no hay Champion aún).

# COMMAND ----------

# DBTITLE 1,Validate Challenger
# --- Validación del Challenger ---
model_alias = "Challenger"
challenger_details = client.get_model_version_by_alias(model_name, model_alias)
model_version = int(challenger_details.version)
print(f"Validating {model_alias} model for {model_name} on model version {model_version}")

# Check 1: Description
if not challenger_details.description:
  has_description = False
  print("Please add model description")
elif not len(challenger_details.description) > 20:
  has_description = False
  print("Please add detailed model description (40 char min).")
else:
  has_description = True
  print(f"Model {model_name} version {model_version} has description: {has_description}")
client.set_model_version_tag(model_name, str(model_version), "has_description", str(has_description))

# Check 2: F1 metric comparison
model_run_id = challenger_details.run_id
f1_score = mlflow.get_run(model_run_id).data.metrics['val_f1_score']

try:
    champion_model = client.get_model_version_by_alias(model_name, "Champion")
    champion_f1 = mlflow.get_run(champion_model.run_id).data.metrics['val_f1_score']
    metric_f1_passed = f1_score >= champion_f1
    print(f"Champion f1 score: {champion_f1}. Challenger f1 score: {f1_score}.")
except Exception:
    metric_f1_passed = True
    print(f"No hay Champion previo → Challenger F1={f1_score:.4f} pasa por default")

client.set_model_version_tag(model_name, str(model_version), "metric_f1_passed", str(metric_f1_passed))
print(f"\nChecks → has_description={has_description} · metric_f1_passed={metric_f1_passed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Promover a @Champion si pasa (código)

# COMMAND ----------

# DBTITLE 1,Promote to Champion
if has_description and metric_f1_passed:
    client.set_registered_model_alias(model_name, "Champion", challenger_details.version)
    print(f"🏆 Promovido a @Champion: {model_name} v{challenger_details.version}")
else:
    print("❌ No promovido — revisa los checks.")
    if not has_description:
        print("  → Falta descripción del modelo")
    if not metric_f1_passed:
        print("  → F1 score no supera al Champion actual")

# COMMAND ----------

# DBTITLE 1,Paso 4 - UI
# MAGIC %md
# MAGIC ## Paso 4 — Gobernar en la UI (🖱️)
# MAGIC
# MAGIC **Catalog → Models → `mlops_churn`**:
# MAGIC - Versiones y aliases **@Champion / @Challenger**.
# MAGIC - Tags `f1_score`, `has_description`, `metric_f1_passed` (los dejó la validación).
# MAGIC - **Lineage**: del run y el dataset de entrenamiento al modelo.
# MAGIC - **Permissions**: quién puede usar/desplegar el modelo.
# MAGIC
# MAGIC Deberías ver la descripción de la versión y el alias `Champion` aplicado a la versión que acaba de pasar la validación.
# MAGIC
# MAGIC ## Continuar → `05 - Model Serving (UI + API)` ⭐

# COMMAND ----------

# DBTITLE 1,Iterative Champion-Challenger header
# MAGIC %md
# MAGIC ## Paso 5 — Iteración: Nuevo modelo → @Challenger vs @Champion 🔄
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-4-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC Ahora simulamos la **iteración real** del ciclo MLOps:
# MAGIC 1. Entrenamos un **nuevo modelo** con hiperparámetros distintos
# MAGIC 2. Lo registramos como **@Challenger**
# MAGIC 3. Comparamos métricas contra el **@Champion** actual
# MAGIC 4. Si supera al Champion → se promueve automáticamente
# MAGIC
# MAGIC Esto es exactamente lo que ocurre en producción cuando un data scientist propone una mejora.

# COMMAND ----------

# DBTITLE 1,Train new model version (different hyperparams)
import subprocess
subprocess.check_call(["pip", "install", "lightgbm", "-q"])

import mlflow
from mlflow.models import Model, infer_signature
from mlflow.pyfunc import PyFuncModel
from mlflow import pyfunc
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler
from sklearn.preprocessing import OneHotEncoder as SklearnOneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from lightgbm import LGBMClassifier

# --- Cargar datos de entrenamiento ---
label_col = "churn"
df_loaded = spark.table(f"{catalog}.{db}.mlops_churn_training").filter("split = 'train'").drop("customer_id", "split")
X = df_loaded.toPandas()
X_train, X_val, Y_train, Y_val = train_test_split(X.drop(label_col, axis=1), X[label_col], test_size=0.2, random_state=42)

# --- Preprocessors (misma estructura que en 03) ---
bool_pipeline = Pipeline(steps=[
    ("cast_type", FunctionTransformer(lambda df: df.astype(object))),
    ("imputers", ColumnTransformer([], remainder="passthrough")),
    ("cast_bool", FunctionTransformer(lambda df: df == "Yes")),
])
numerical_pipeline = Pipeline(steps=[
    ("converter", FunctionTransformer(lambda df: df.apply(lambda c: c.astype(float), axis=0))),
    ("imputers", ColumnTransformer([("impute_mean", SimpleImputer(), ["monthly_charges", "num_optional_services", "tenure", "total_charges"])], remainder="passthrough")),
    ("standardizer", StandardScaler()),
])
one_hot_pipeline = Pipeline(steps=[
    ("imputers", ColumnTransformer([], remainder="passthrough")),
    ("one_hot_encoder", SklearnOneHotEncoder(handle_unknown="ignore")),
])

transformers = [
    ("boolean", bool_pipeline, ["senior_citizen", "partner", "dependents", "phone_service", "paperless_billing"]),
    ("numerical", numerical_pipeline, ["monthly_charges", "num_optional_services", "tenure", "total_charges"]),
    ("onehot", one_hot_pipeline, ["contract", "device_protection", "internet_service", "multiple_lines", "online_backup", "online_security", "payment_method", "streaming_movies", "streaming_tv", "tech_support"]),
]
preprocessor = ColumnTransformer(transformers, remainder="drop", sparse_threshold=0)

# --- Nuevos hiperparámetros (más agresivos) ---
new_params = {
    "colsample_bytree": 0.55,
    "lambda_l1": 1.5,
    "lambda_l2": 200.0,
    "learning_rate": 0.05,
    "max_bin": 300,
    "max_depth": 10,
    "min_child_samples": 40,
    "n_estimators": 400,
    "num_leaves": 150,
    "path_smooth": 40.0,
    "subsample": 0.75,
    "random_state": 42,
}

experiment_name = f"{xp_path}/{xp_name}"
mlflow.set_experiment(experiment_name)
experiment_id = mlflow.get_experiment_by_name(experiment_name).experiment_id

with mlflow.start_run(experiment_id=experiment_id, run_name="light_gbm_v2_tuned") as run:
    lgbm = LGBMClassifier(**new_params)
    model = Pipeline([("preprocessor", preprocessor), ("classifier", lgbm)])
    mlflow.sklearn.autolog(log_models=False, silent=True)
    model.fit(X_train, Y_train)
    signature = infer_signature(X_train, Y_train)
    mlflow.sklearn.log_model(
        model, "sklearn_model",
        input_example=X_train.iloc[0].to_dict(),
        signature=signature,
        serialization_format="cloudpickle",
    )
    # Evaluate
    mlflow_model = Model()
    pyfunc.add_to_model(mlflow_model, loader_module="mlflow.sklearn")
    pyfunc_model = PyFuncModel(model_meta=mlflow_model, model_impl=model)
    val_eval = mlflow.evaluate(
        model=pyfunc_model,
        data=X_val.assign(**{str(label_col): Y_val}),
        targets=label_col,
        model_type="classifier",
        evaluator_config={"log_model_explainability": False, "metric_prefix": "val_", "pos_label": "Yes"}
    )
    new_f1 = val_eval.metrics["val_f1_score"]
    new_run_id = run.info.run_id

print(f"✓ Nuevo modelo (v2_tuned) entrenado — val_f1_score = {new_f1:.4f}")
print(f"  Run ID: {new_run_id}")

# COMMAND ----------

# DBTITLE 1,Register new version as Challenger and compare
# --- Registrar nueva versión como @Challenger ---
print(f"Registrando nueva versión en {model_name}...")
new_model_details = mlflow.register_model(f"runs:/{new_run_id}/sklearn_model", model_name)

client.update_model_version(
    name=new_model_details.name,
    version=new_model_details.version,
    description=f"LightGBM v2 tuned — F1={new_f1:.4f}. Más estimadores, más hojas, regularización reducida."
)
client.set_model_version_tag(model_name, new_model_details.version, "f1_score", f"{new_f1:.4f}")

# Asignar alias @Challenger
client.set_registered_model_alias(model_name, "Challenger", new_model_details.version)
print(f"✓ Registrado {model_name} v{new_model_details.version} como @Challenger (F1={new_f1:.4f})")

# COMMAND ----------

# DBTITLE 1,Compare Champion vs Challenger
# --- Comparar @Champion vs @Challenger ---
champion = client.get_model_version_by_alias(model_name, "Champion")
challenger = client.get_model_version_by_alias(model_name, "Challenger")

champion_f1 = mlflow.get_run(champion.run_id).data.metrics['val_f1_score']
challenger_f1 = mlflow.get_run(challenger.run_id).data.metrics['val_f1_score']

print("="*60)
print(f"  📊 COMPARACIÓN: Champion vs Challenger")
print("="*60)
print(f"  @Champion  (v{champion.version}):  F1 = {champion_f1:.4f}")
print(f"  @Challenger (v{challenger.version}):  F1 = {challenger_f1:.4f}")
print(f"  Diferencia:            ΔF1 = {(challenger_f1 - champion_f1):+.4f}")
print("="*60)

# --- Promover si supera al Champion ---
if challenger_f1 >= champion_f1:
    client.set_registered_model_alias(model_name, "Champion", challenger.version)
    print(f"\n🏆 ¡Nuevo Champion! v{challenger.version} reemplaza a v{champion.version}")
else:
    print(f"\n⏸️  Challenger no supera al Champion — se mantiene v{champion.version} como @Champion")
    print(f"  El Challenger v{challenger.version} queda disponible para análisis en la UI.")

# COMMAND ----------

# DBTITLE 1,Summary and next steps
# MAGIC %md
# MAGIC ## ✅ Resumen del ciclo iterativo
# MAGIC
# MAGIC Acabas de completar el **loop completo** de Champion/Challenger:
# MAGIC
# MAGIC | Paso | Acción | Herramienta |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Entrenar modelo baseline | Código + MLflow |
# MAGIC | 2 | Registrar como @Challenger | API (`register_model`) |
# MAGIC | 3 | Validar y promover a @Champion | API (`set_registered_model_alias`) |
# MAGIC | 4 | Entrenar nuevo modelo (v2) | Código + MLflow |
# MAGIC | 5 | Registrar v2 como @Challenger | API |
# MAGIC | 6 | Comparar métricas y promover | API (automático) |
# MAGIC
# MAGIC **En la UI** → ve a **Catalog → Models → `mlops_churn`** para ver ambas versiones, sus tags, y el historial de alias.
# MAGIC
# MAGIC ## Continuar → `05 - Model Serving (UI + API)` ⭐
