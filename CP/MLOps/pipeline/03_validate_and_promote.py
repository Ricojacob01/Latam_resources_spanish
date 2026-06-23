# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/03_validate_and_promote — Tarea del Job
# MAGIC Valida el @Challenger y lo promueve a @Champion. Si NO pasa, falla la tarea
# MAGIC (así el Job no despliega un modelo malo — las tareas siguientes no corren).

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Validate and promote (tested)
import mlflow
from mlflow import MlflowClient
client = MlflowClient()

model_name = f"{catalog}.{db}.mlops_churn"
model_alias = "Challenger"

# Get Challenger info
challenger_details = client.get_model_version_by_alias(model_name, model_alias)
model_version = int(challenger_details.version)
print(f"Validating {model_alias} v{model_version} for {model_name}")

# Check 1: Description
if not challenger_details.description:
  has_description = False
elif not len(challenger_details.description) > 20:
  has_description = False
else:
  has_description = True
client.set_model_version_tag(model_name, str(model_version), "has_description", str(has_description))

# Check 2: F1 metric comparison
f1_score = mlflow.get_run(challenger_details.run_id).data.metrics['val_f1_score']
try:
    champion_model = client.get_model_version_by_alias(model_name, "Champion")
    champion_f1 = mlflow.get_run(champion_model.run_id).data.metrics['val_f1_score']
    metric_f1_passed = f1_score >= champion_f1
    print(f"Champion F1={champion_f1:.4f} vs Challenger F1={f1_score:.4f}")
except Exception:
    metric_f1_passed = True
    print(f"No Champion exists yet. Challenger F1={f1_score:.4f} passes by default.")

client.set_model_version_tag(model_name, str(model_version), "metric_f1_passed", str(metric_f1_passed))
print(f"Checks: has_description={has_description}, metric_f1_passed={metric_f1_passed}")

# Promote or fail
if has_description and metric_f1_passed:
    client.set_registered_model_alias(model_name, "Champion", challenger_details.version)
    print(f"🏆 Promoted v{model_version} to @Champion (F1={f1_score:.4f})")
else:
    raise Exception(f"Validation failed: has_description={has_description}, metric_f1_passed={metric_f1_passed}")
