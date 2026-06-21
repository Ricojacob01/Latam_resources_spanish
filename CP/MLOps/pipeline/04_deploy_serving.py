# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/04_deploy_serving — Tarea del Job
# MAGIC Crea o actualiza el endpoint de Model Serving con la versión **@Champion** (idempotente).

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import EndpointCoreConfigInput, ServedEntityInput
from mlflow import MlflowClient

champ = MlflowClient().get_model_version_by_alias(MODEL_NAME, "Champion")
w = WorkspaceClient()
served = ServedEntityInput(entity_name=MODEL_NAME, entity_version=champ.version,
                           workload_size="Small", scale_to_zero_enabled=True)

if SERVING_ENDPOINT in {e.name for e in w.serving_endpoints.list()}:
    w.serving_endpoints.update_config(name=SERVING_ENDPOINT, served_entities=[served])
    print(f"↻ endpoint {SERVING_ENDPOINT} actualizado a v{champ.version}")
else:
    w.serving_endpoints.create(name=SERVING_ENDPOINT,
        config=EndpointCoreConfigInput(served_entities=[served]))
    print(f"＋ endpoint {SERVING_ENDPOINT} creado con v{champ.version}")
