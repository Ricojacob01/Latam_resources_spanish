# Databricks notebook source
# DBTITLE 1,Intro with banner
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC
# MAGIC # 05 — ⭐ Model Serving (UI + API) 🌐
# MAGIC
# MAGIC Tomamos el modelo **@Champion** y lo exponemos como **endpoint REST en tiempo real** con Databricks Model Serving. **Módulo avanzado que completa el ciclo MLOps.**
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Secuencial (UI → Code)**
# MAGIC Primero creas y pruebas el endpoint en la **Serving UI** para construir intuición (estado, scale-to-zero, panel de query, latencia). Luego haces **lo mismo por API** (`mlflow.deployments` / `WorkspaceClient`) — porque en producción el endpoint lo crea/actualiza un **Job** (módulo 07), no una persona con clicks.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
champ = MlflowClient().get_model_version_by_alias(MODEL_NAME, "Champion")
print(f"Modelo a servir: {MODEL_NAME} v{champ.version} (@Champion)")
print(f"Nombre del endpoint: {SERVING_ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,Parte A - UI instructions
# MAGIC %md
# MAGIC ## Parte A — Crear el endpoint en la UI (🖱️) — construir intuición
# MAGIC
# MAGIC 1. **Sidebar → Serving → Create serving endpoint.**
# MAGIC 2. **Name:** pega el valor de `SERVING_ENDPOINT` (lo imprimió la celda de arriba).
# MAGIC 3. **Entity:** **Unity Catalog model** → `mlops_churn` → versión con alias **Champion**.
# MAGIC 4. **Compute:** *Small* · **Scale to zero**: ✅ (paga solo cuando hay tráfico).
# MAGIC 5. *(Opcional pero recomendado)* **Inference tables**: ✅ — loggea cada request a una tabla Delta para monitoreo.
# MAGIC 6. **Create.** El estado pasa **Not Ready → Updating → Ready** (3–10 min la primera vez).
# MAGIC 7. Cuando esté **Ready**, abre la tab **Query endpoint** y pega este JSON para probar desde la UI:
# MAGIC    ```json
# MAGIC    {"dataframe_records": [
# MAGIC      {"gender":"Female","senior_citizen":"No","partner":"Yes","dependents":"No",
# MAGIC       "tenure":2,"phone_service":"Yes","multiple_lines":"No",
# MAGIC       "internet_service":"DSL","online_security":"No","online_backup":"No",
# MAGIC       "device_protection":"No","tech_support":"No","streaming_tv":"Yes",
# MAGIC       "streaming_movies":"Yes","contract":"Month-to-month",
# MAGIC       "paperless_billing":"Yes","payment_method":"Electronic check",
# MAGIC       "monthly_charges":89.9,"total_charges":180.2,"num_optional_services":2}
# MAGIC    ]}
# MAGIC    ```
# MAGIC
# MAGIC > Observa: estado, **scale-to-zero**, latencia, y (si activaste) la **inference table**. Esa es la intuición que la API por sí sola no da.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Lo mismo por API/código (para automatizar)
# MAGIC
# MAGIC En el Job (módulo 07) el endpoint lo crea/actualiza código, no clicks. Esta celda es **idempotente**: crea el endpoint si no existe, o actualiza la versión servida si ya existe.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput, ServedEntityInput)

w = WorkspaceClient()
served = ServedEntityInput(
    entity_name=MODEL_NAME,
    entity_version=champ.version,
    workload_size="Small",
    scale_to_zero_enabled=True,
)

existing = {e.name for e in w.serving_endpoints.list()}
if SERVING_ENDPOINT in existing:
    print(f"Endpoint existe → actualizando a v{champ.version} ...")
    w.serving_endpoints.update_config(name=SERVING_ENDPOINT, served_entities=[served])
else:
    print(f"Creando endpoint {SERVING_ENDPOINT} ...")
    w.serving_endpoints.create(
        name=SERVING_ENDPOINT,
        config=EndpointCoreConfigInput(served_entities=[served]),
    )
print("Solicitud enviada. (Puede tardar varios minutos en quedar Ready.)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — Esperar a que esté Ready (código)

# COMMAND ----------

import time
for _ in range(60):  # hasta ~10 min
    st = w.serving_endpoints.get(SERVING_ENDPOINT).state
    ready = getattr(st, "ready", None)
    upd = getattr(st, "config_update", None)
    print(f"ready={ready} · config_update={upd}")
    if str(ready) and "READY" in str(ready).upper():
        break
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Consultar el endpoint por código
# MAGIC
# MAGIC La **misma** llamada que harías desde una app o servicio externo (REST). Aquí vía el cliente de MLflow Deployments.

# COMMAND ----------

# DBTITLE 1,Query endpoint by code
from mlflow.deployments import get_deploy_client
client = get_deploy_client("databricks")

ejemplo = {"dataframe_records": [{
    "gender": "Female", "senior_citizen": "No", "partner": "Yes", "dependents": "No",
    "tenure": 2, "phone_service": "Yes", "multiple_lines": "No",
    "internet_service": "DSL", "online_security": "No", "online_backup": "No",
    "device_protection": "No", "tech_support": "No", "streaming_tv": "Yes",
    "streaming_movies": "Yes", "contract": "Month-to-month",
    "paperless_billing": "Yes", "payment_method": "Electronic check",
    "monthly_charges": 89.9, "total_charges": 180.2, "num_optional_services": 2,
}]}

try:
    resp = client.predict(endpoint=SERVING_ENDPOINT, inputs=ejemplo)
    print("Predicción del endpoint:", resp)
except Exception as e:
    print("Si falla, espera a que el endpoint esté Ready (Parte C). Detalle:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte E — Llamada REST cruda (curl) — para integraciones externas
# MAGIC
# MAGIC Cualquier servicio puede consumir el endpoint con un token. La URL es:
# MAGIC `https://<workspace-host>/serving-endpoints/<endpoint>/invocations`

# COMMAND ----------

# DBTITLE 1,Curl example
host = spark.conf.get("spark.databricks.workspaceUrl")
print("Ejemplo de llamada (sustituye $DATABRICKS_TOKEN):\n")
print(f"""curl -s -X POST \\
  https://{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations \\
  -H "Authorization: Bearer $DATABRICKS_TOKEN" \\
  -H "Content-Type: application/json" \\
  -d '{{"dataframe_records":[{{"gender":"Female","senior_citizen":"No","partner":"Yes","dependents":"No","tenure":2,"phone_service":"Yes","multiple_lines":"No","internet_service":"DSL","online_security":"No","online_backup":"No","device_protection":"No","tech_support":"No","streaming_tv":"Yes","streaming_movies":"Yes","contract":"Month-to-month","paperless_billing":"Yes","payment_method":"Electronic check","monthly_charges":89.9,"total_charges":180.2,"num_optional_services":2}}]}}'""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Creaste un endpoint de **Model Serving** en la UI (con scale-to-zero e inference tables)
# MAGIC ✅ Lo recreaste/actualizaste por **API** (idempotente, listo para el Job)
# MAGIC ✅ Lo consultaste por SDK y viste la llamada **REST** cruda
# MAGIC ✅ Patrón **UI → Code**: la UI enseña el endpoint, el código lo automatiza
# MAGIC
# MAGIC > El módulo 07 deja la **creación/actualización del endpoint dentro del Job** (`pipeline/04_deploy_serving`).
# MAGIC
# MAGIC ## Continuar → `06 - Batch Inference`
