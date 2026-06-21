# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/03_validate_and_promote — Tarea del Job
# MAGIC Valida el @Challenger y lo promueve a @Champion. Si NO pasa, falla la tarea
# MAGIC (así el Job no despliega un modelo malo — las tareas siguientes no corren).

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
client = MlflowClient()

ch = client.get_model_version_by_alias(MODEL_NAME, "Challenger")
has_desc = bool(ch.description and len(ch.description) > 20)
ch_f1 = mlflow.get_run(ch.run_id).data.metrics["val_f1_score"]
try:
    champ = client.get_model_version_by_alias(MODEL_NAME, "Champion")
    f1_passed = ch_f1 >= mlflow.get_run(champ.run_id).data.metrics["val_f1_score"]
except Exception:
    f1_passed = True

client.set_model_version_tag(MODEL_NAME, ch.version, "has_description", str(has_desc))
client.set_model_version_tag(MODEL_NAME, ch.version, "metric_f1_passed", str(f1_passed))

if has_desc and f1_passed:
    client.set_registered_model_alias(MODEL_NAME, "Champion", ch.version)
    print(f"🏆 promovido v{ch.version} a @Champion (F1={ch_f1:.4f})")
else:
    raise Exception(f"Validación fallida: has_description={has_desc}, metric_f1_passed={f1_passed}")
