# Databricks notebook source
# MAGIC %md
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
