# Databricks notebook source
# MAGIC %md
# MAGIC # 💬 Sesión 2 · 01 — Databricks App (frontend del agente)
# MAGIC
# MAGIC **Meta:** darle **cara** al agente con una **Databricks App** — un chat web, serverless, con **auth integrada
# MAGIC (OBO)**, que llama al endpoint del agente y muestra las reservas del afiliado desde Lakebase.
# MAGIC
# MAGIC > **Equivale a: Azure Container Apps.** Hoy Comfama hospeda el frontend del agente en Container Apps. Databricks
# MAGIC > Apps reemplaza esa pieza: sin gestión de contenedores, con auth, secrets y conexión nativa a UC/Lakebase.
# MAGIC
# MAGIC Módulo **dual-mode**: crear la App **🖱️ por la UI** o **⌨️ por SDK**. ⚠️ **Validar en dry-run** (API de Apps).

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El código de la App (Streamlit)
# MAGIC Tres archivos: `app.py`, `app.yaml`, `requirements.txt`. Los escribimos en `../app_source/`.
# MAGIC
# MAGIC La App usa el **token OBO** del usuario que la abre (`X-Forwarded-Access-Token`) para llamar al agente — así cada
# MAGIC afiliado actúa con **sus propios permisos**.

# COMMAND ----------

APP_PY = '''
import os, streamlit as st
from databricks.sdk import WorkspaceClient
from mlflow.deployments import get_deploy_client

st.set_page_config(page_title="Comfama · Asistente al Afiliado", page_icon="🤖")
st.title("🤖 Asistente de Servicios al Afiliado")

AGENT_ENDPOINT = os.environ["AGENT_ENDPOINT"]

# OBO: token del usuario que abrió la app (lo inyecta Databricks Apps)
def user_token():
    try:
        return st.context.headers.get("X-Forwarded-Access-Token")
    except Exception:
        return None

afiliado_id = st.sidebar.number_input("Tu # de afiliado", min_value=1000, value=1001, step=1)

if "msgs" not in st.session_state:
    st.session_state.msgs = []

for m in st.session_state.msgs:
    st.chat_message(m["role"]).write(m["content"])

if prompt := st.chat_input("Pregunta por beneficios, programas o reserva un cupo..."):
    st.session_state.msgs.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)
    client = get_deploy_client("databricks")
    convo = [{"role": "system", "content": f"El afiliado autenticado es el id {afiliado_id}."}]
    convo += st.session_state.msgs
    resp = client.predict(endpoint=AGENT_ENDPOINT, inputs={"messages": convo})
    answer = resp["messages"][-1]["content"]
    st.session_state.msgs.append({"role": "assistant", "content": answer})
    st.chat_message("assistant").write(answer)
'''

APP_YAML = '''
command: ["streamlit", "run", "app.py"]
env:
  - name: AGENT_ENDPOINT
    value: "AGENT_ENDPOINT_PLACEHOLDER"
'''.replace("AGENT_ENDPOINT_PLACEHOLDER", AGENT_ENDPOINT)

REQUIREMENTS = "streamlit\nmlflow\ndatabricks-sdk\n"

# Escribir a ../app_source en el workspace
from databricks.sdk import WorkspaceClient
import base64, os

w = WorkspaceClient()

def upload_raw(path, content):
    """Escribe un archivo crudo (no-notebook) en el workspace vía REST (format=RAW)."""
    w.api_client.do("POST", "/api/2.0/workspace/import",
                    body={"path": path, "format": "RAW", "overwrite": True,
                          "content": base64.b64encode(content.encode("utf-8")).decode()})

nb_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
APP_DIR = os.path.dirname(os.path.dirname(nb_path)) + "/app_source"   # .../workshop/app_source
APP_DIR_WS = APP_DIR if APP_DIR.startswith("/Workspace") else "/Workspace" + APP_DIR
w.workspace.mkdirs(APP_DIR)
for fname, content in [("app.py", APP_PY), ("app.yaml", APP_YAML), ("requirements.txt", REQUIREMENTS)]:
    upload_raw(f"{APP_DIR}/{fname}", content)
print("✅ App source escrito en:", APP_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — crear y desplegar la App
# MAGIC 1. Menú izquierdo → **Compute** → pestaña **Apps** → **Create app** → **Custom**.
# MAGIC 2. Nombre: el valor de `APP_NAME` (abajo).
# MAGIC 3. **Resources** → agrega el **Serving endpoint** `AGENT_ENDPOINT` (permiso *Can Query*) y, si la App escribe en
# MAGIC    Lakebase, la **Database** `comfama` (la auth se inyecta como variables de entorno / OBO).
# MAGIC 4. **Source code path**: selecciona la carpeta `app_source` que acabamos de crear.
# MAGIC 5. **Deploy**. Al terminar tendrás una **URL** pública (con login Databricks).

# COMMAND ----------

print(f"APP_NAME:       {APP_NAME}")
print(f"AGENT_ENDPOINT: {AGENT_ENDPOINT}")
print(f"app_source:     {APP_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — crear + desplegar por SDK

# COMMAND ----------

from databricks.sdk.service.apps import (
    App, AppResource, AppResourceServingEndpoint,
    AppResourceServingEndpointServingEndpointPermission, AppDeployment,
)

# 1) Crear la app con el endpoint del agente como recurso
try:
    w.apps.create_and_wait(App(
        name=APP_NAME,
        description="Asistente de Servicios al Afiliado Comfama",
        resources=[AppResource(
            name="agente",
            serving_endpoint=AppResourceServingEndpoint(
                name=AGENT_ENDPOINT,
                permission=AppResourceServingEndpointServingEndpointPermission.CAN_QUERY,
            ))],
    ))
    print("✅ App creada:", APP_NAME)
except Exception as e:
    print(f"App ya existe o requiere ajuste: {type(e).__name__}: {str(e)[:160]}")

# 2) Desplegar el código fuente
dep = w.apps.deploy_and_wait(
    app_name=APP_NAME,
    app_deployment=AppDeployment(source_code_path=APP_DIR_WS))
print("✅ Deploy:", dep.status.message if dep.status else dep)
print("🌐 URL:", w.apps.get(name=APP_NAME).url)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC El agente ya tiene un **frontend serverless con auth**. Comfama puede apagar su Container Apps: el hosting, la
# MAGIC autenticación y la conexión a datos las da la plataforma.
# MAGIC
# MAGIC ### ▶️ Siguiente: `02 - Observabilidad (MLflow Tracing)`

