# Databricks notebook source
# MAGIC %md
# MAGIC # 🚀 Sesión 1 · 05 — Servir el Agente
# MAGIC
# MAGIC **Meta:** desplegar el agente registrado (módulo 04) como un **endpoint REST de Model Serving**, listo para que
# MAGIC lo consuma la App (Sesión 2) o cualquier sistema de Comfama.
# MAGIC
# MAGIC Módulo **dual-mode**: lo creamos **🖱️ por la Serving UI** o **⌨️ con `databricks-agents`**.
# MAGIC
# MAGIC > El despliegue de agentes con `agents.deploy` provisiona, además del endpoint: **inference tables**
# MAGIC > (registro de cada request/response) y la **Review App** de evaluación — base para Observabilidad, AI Gateway y FinOps.

# COMMAND ----------

# MAGIC %pip install -U databricks-agents mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — desde el modelo registrado
# MAGIC 1. **Catalog** → tu schema `ws_<usuario>` → **Models** → `agente_afiliados` → abre la **última versión**.
# MAGIC 2. Botón **Serve this model** (o **Serving** → **Create serving endpoint**).
# MAGIC 3. Configura:
# MAGIC    - **Endpoint name**: el valor de `AGENT_ENDPOINT` (abajo).
# MAGIC    - **Compute scale-to-zero**: activado (ahorra cuando no hay tráfico).
# MAGIC    - **Entity**: el modelo `agente_afiliados`, versión más reciente.
# MAGIC 4. **Create**. El endpoint pasa a **Ready** en unos minutos.
# MAGIC
# MAGIC > 💡 Para *agentes* es preferible `agents.deploy` (celda siguiente): además del endpoint, configura las
# MAGIC > **inference tables** y la **Review App** automáticamente.

# COMMAND ----------

print(f"Modelo:   {AGENT_MODEL_NAME}")
print(f"Endpoint: {AGENT_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — `agents.deploy`

# COMMAND ----------

from databricks import agents
from mlflow import MlflowClient

mc = MlflowClient(registry_uri="databricks-uc")
# En UC, latest_versions no se popula; obtenemos la última versión con search_model_versions
version = max(int(v.version) for v in mc.search_model_versions(f"name='{AGENT_MODEL_NAME}'"))
print(f"Desplegando {AGENT_MODEL_NAME} v{version} ...")

deployment = agents.deploy(
    model_name=AGENT_MODEL_NAME,
    model_version=version,
    scale_to_zero=True,
    endpoint_name=AGENT_ENDPOINT,
)
print("✅ Endpoint:", deployment.endpoint_name)
print("   Query URL:", deployment.query_endpoint)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⏳ Esperar a que el endpoint esté listo

# COMMAND ----------

from databricks.sdk import WorkspaceClient
import time
w = WorkspaceClient()
for _ in range(60):
    ep = w.serving_endpoints.get(AGENT_ENDPOINT)
    state = ep.state.ready.value if ep.state and ep.state.ready else "…"
    print("estado:", state)
    if state == "READY":
        break
    time.sleep(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Probar el endpoint servido

# COMMAND ----------

from mlflow.deployments import get_deploy_client
client = get_deploy_client("databricks")

for pregunta in [
    "¿Qué requisitos tiene el subsidio de vivienda?",
    "Soy el afiliado 1001, ¿qué tengo inscrito y qué cursos con cupo me recomiendas?",
]:
    r = client.predict(endpoint=AGENT_ENDPOINT,
                       inputs={"messages":[{"role":"user","content":pregunta}]})
    print("👤", pregunta)
    print("🤖", r["messages"][-1]["content"], "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC El agente ya es un **servicio REST** con scale-to-zero e **inference tables** activas. En el módulo 06 lo
# MAGIC **gobernamos con AI Gateway** (límites, guardrails, tracking) y en la Sesión 2 le ponemos una **App** encima.
# MAGIC
# MAGIC ### ▶️ Siguiente: `06 - AI Gateway`

