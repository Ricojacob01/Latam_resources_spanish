# Databricks notebook source
# DBTITLE 1,Intro with banner
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC
# MAGIC # 05 — ⭐ Model Serving (UI + API) 🌐
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-5-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC Tomamos el modelo **@Champion** y lo exponemos como **endpoint REST en tiempo real** con Databricks Model Serving. **Módulo avanzado que completa el ciclo MLOps.**
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Secuencial (UI → Code)**
# MAGIC Primero creas y pruebas el endpoint en la **Serving UI** para construir intuición (estado, scale-to-zero, panel de query, latencia). Luego haces **lo mismo por API** (`mlflow.deployments` / `WorkspaceClient`) — porque en producción el endpoint lo crea/actualiza un **Job** (módulo 06), no una persona con clicks.

# COMMAND ----------

# DBTITLE 1,Setup with inference data
# MAGIC %run ./_resources/00-setup $setup_inference_data=true

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
# MAGIC En el Job (módulo 06) el endpoint lo crea/actualiza código, no clicks. Esta celda es **idempotente**: crea el endpoint si no existe, o actualiza la versión servida si ya existe.

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

# DBTITLE 1,Serving summary
# MAGIC %md
# MAGIC ## Resumen — Model Serving
# MAGIC
# MAGIC ✅ Creaste un endpoint de **Model Serving** en la UI (con scale-to-zero e inference tables)
# MAGIC ✅ Lo recreaste/actualizaste por **API** (idempotente, listo para el Job)
# MAGIC ✅ Lo consultaste por SDK y viste la llamada **REST** cruda
# MAGIC ✅ Patrón **UI → Code**: la UI enseña el endpoint, el código lo automatiza
# MAGIC
# MAGIC > El módulo 06 deja la **creación/actualización del endpoint dentro del Job** (`pipeline/04_deploy_serving`).
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Batch Inference header
# MAGIC %md
# MAGIC ## Parte F — Batch Inference 📦
# MAGIC
# MAGIC No todo scoring requiere un endpoint en tiempo real. Para puntuar un **volumen masivo** de clientes (ej: campaña mensual anti-churn), usamos el modelo **@Champion** directamente en Spark/pandas — sin endpoint.
# MAGIC
# MAGIC ### Opción A — Inferencia con `pyfunc` (compatible con Serverless)
# MAGIC
# MAGIC Cargamos el modelo directamente como pyfunc y predecimos en pandas. Funciona en Serverless y clusters clásicos.

# COMMAND ----------

# DBTITLE 1,Batch inference with pyfunc
import subprocess
subprocess.check_call(["pip", "install", "lightgbm", "-q"])

import mlflow

# Load customer features to be scored
inference_df = spark.read.table("mlops_churn_inference")

# Load champion model directly (pandas-based prediction for serverless compatibility)
champion_model = mlflow.pyfunc.load_model(model_uri=f"models:/{catalog}.{db}.mlops_churn@Champion")

# Get input column names from model schema
input_cols = champion_model.metadata.get_input_schema().input_names()

# Batch score using pandas
inference_pd = inference_df.toPandas()
inference_pd['predictions'] = champion_model.predict(inference_pd[input_cols])
preds_df = spark.createDataFrame(inference_pd)

# Save predictions table
preds_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("mlops_churn_predictions")
print("✓ mlops_churn_predictions:", spark.table("mlops_churn_predictions").count(), "filas")
display(preds_df)

# COMMAND ----------

# DBTITLE 1,Batch inference alternatives
# MAGIC %md
# MAGIC ### Opción B — `spark_udf` (distribuido en cluster clásico)
# MAGIC
# MAGIC Si estás en un cluster clásico (no Serverless), puedes usar `spark_udf` para distribuir la inferencia:
# MAGIC
# MAGIC ```python
# MAGIC import mlflow
# MAGIC udf = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{db}.mlops_churn@Champion", env_manager="virtualenv")
# MAGIC cols = udf.metadata.get_input_schema().input_names()
# MAGIC scored = inference_df.withColumn("churn_prediction", udf(*[c for c in cols]))
# MAGIC ```
# MAGIC
# MAGIC ### Opción C — `ai_query` contra el endpoint de Serving
# MAGIC
# MAGIC Si el endpoint del Parte A-E está **Ready**, puedes usarlo para batch scoring desde SQL:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *,
# MAGIC   ai_query(
# MAGIC     'mlops_churn_<tu_slug>',
# MAGIC     named_struct(
# MAGIC       'gender', gender, 'senior_citizen', senior_citizen, 'tenure', tenure,
# MAGIC       'contract', contract, 'monthly_charges', monthly_charges,
# MAGIC       'total_charges', total_charges, 'num_optional_services', num_optional_services
# MAGIC     )
# MAGIC   ) AS prediction
# MAGIC FROM mlops_churn_training WHERE split = 'test'
# MAGIC ```
# MAGIC (Ajusta el nombre del endpoint y las columnas a la signature del modelo.)

# COMMAND ----------

# DBTITLE 1,Final conclusion
# MAGIC %md
# MAGIC ## Inspección en la UI (🖱️)
# MAGIC
# MAGIC Abre `mlops_churn_predictions` en **Catalog Explorer** → **Sample Data** y **Lineage** (verás el modelo como origen del scoring).
# MAGIC
# MAGIC ¡Eso es todo! Ahora los datos pueden ser reutilizados por el equipo de Análisis de Datos / Marketing para tomar acciones especiales y reducir el riesgo de Churn. ¡Tus datos también estarán disponibles en Genie para responder cualquier pregunta relacionada con churn!
# MAGIC
# MAGIC ## ✅ Resumen completo — Módulo 05
# MAGIC
# MAGIC | Modo | Herramienta | Caso de uso |
# MAGIC | --- | --- | --- |
# MAGIC | Real-time (UI) | Serving UI | Explorar, probar, construir intuición |
# MAGIC | Real-time (API) | WorkspaceClient / mlflow.deployments | Automatizar en Jobs, CI/CD |
# MAGIC | Real-time (ext) | curl / REST | Apps externas, microservicios |
# MAGIC | Batch (pyfunc) | mlflow.pyfunc.load_model | Serverless, scoring masivo |
# MAGIC | Batch (spark_udf) | mlflow.pyfunc.spark_udf | Cluster clásico, distribuido |
# MAGIC | Batch (SQL) | ai_query() | SQL nativo contra endpoint |
# MAGIC
# MAGIC ## Continuar → `06 - Orquestacion - Job del pipeline ML` ⭐
