# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Deploy Databricks App (frontend del agente)
# MAGIC
# MAGIC Despliega un Databricks App con Streamlit que consume el endpoint del agente desplegado en el notebook 07.
# MAGIC
# MAGIC **Reemplaza:** Container Apps de Azure (frontend + backend FastAPI).
# MAGIC
# MAGIC **Tiempo estimado:** 3-5 min

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk>=0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
APP_NAME = "comfama-agente-app"
SERVING_ENDPOINT = "agente_comfama"

CURRENT_USER = spark.sql("SELECT current_user() as u").collect()[0]["u"]
APP_FOLDER = f"/Workspace/Users/{CURRENT_USER}/Latam_resources_spanish/Comfama_framework/app_source"

print(f"App name:       {APP_NAME}")
print(f"App folder:     {APP_FOLDER}")
print(f"Serving endpoint: {SERVING_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Crear los archivos del app (Streamlit)

# COMMAND ----------

import os
os.makedirs("/tmp/comfama_app", exist_ok=True)

# app.yaml — config
APP_YAML = """command:
  - "streamlit"
  - "run"
  - "app.py"
"""

# requirements.txt
REQUIREMENTS = """streamlit==1.39.0
requests==2.32.3
databricks-sdk>=0.30.0
"""

# app.py — Streamlit frontend
APP_PY = '''
import streamlit as st
import requests
import os
import time
from databricks.sdk import WorkspaceClient

# Setup
st.set_page_config(page_title="Asistente Comfama", page_icon="🤝", layout="centered")

# Get auth from Databricks Apps runtime
def get_databricks_auth():
    """Databricks Apps inject DATABRICKS_HOST and DATABRICKS_CLIENT_ID/SECRET as env vars
    for service principal auth via on-behalf-of."""
    host = os.environ.get("DATABRICKS_HOST", "")
    # For on-behalf-of user token (preferred for apps), use the user_token header
    user_token = st.context.headers.get("X-Forwarded-Access-Token") if hasattr(st, "context") else None
    if user_token:
        return host, user_token
    # Fallback: service principal token
    w = WorkspaceClient()
    return host, w.config.token

SERVING_ENDPOINT = "agente_comfama"

# Sidebar
with st.sidebar:
    st.title("🤝 Comfama")
    st.markdown("**Asistente Virtual**")
    st.markdown("---")
    st.markdown("Arquitectura:")
    st.markdown("- 🧠 Mosaic AI Agent Framework")
    st.markdown("- 🔍 Vector Search")
    st.markdown("- 🛡️ Unity Catalog")
    st.markdown("- 📊 Inference Tables")
    st.markdown("- 🚪 AI Gateway-ready")
    st.markdown("---")
    if st.button("🗑️ Limpiar conversación"):
        st.session_state.messages = []

# Title
st.title("Asistente Virtual Comfama")
st.caption("Pregunta sobre subsidios, servicios de salud, créditos y más")

# Init chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display history
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        if msg.get("sources"):
            with st.expander("📎 Fuentes consultadas"):
                for src in msg["sources"]:
                    st.markdown(f"- **{src['titulo']}** (score: {src.get('score',0):.2f})")

# Input
if prompt := st.chat_input("¿En qué te puedo ayudar?"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Consultando..."):
            try:
                host, token = get_databricks_auth()
                url = f"{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations"
                resp = requests.post(
                    url,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"dataframe_records": [{"query": prompt}]},
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    pred = data.get("predictions", [{}])[0]
                    answer = pred.get("answer", "Sin respuesta")
                    sources = pred.get("sources", [])
                    st.markdown(answer)
                    if sources:
                        with st.expander("📎 Fuentes consultadas"):
                            for src in sources:
                                st.markdown(f"- **{src['titulo']}** (score: {src.get('score',0):.2f})")
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": answer,
                        "sources": sources,
                    })
                else:
                    error_msg = f"❌ Error del endpoint: HTTP {resp.status_code}\\n```\\n{resp.text[:300]}\\n```"
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})
            except Exception as e:
                st.error(f"❌ Error: {e}")
                st.session_state.messages.append({"role": "assistant", "content": f"Error: {e}"})
'''

with open("/tmp/comfama_app/app.yaml", "w") as f:
    f.write(APP_YAML)
with open("/tmp/comfama_app/requirements.txt", "w") as f:
    f.write(REQUIREMENTS)
with open("/tmp/comfama_app/app.py", "w") as f:
    f.write(APP_PY)

print("✓ Archivos del app creados en /tmp/comfama_app/")
print("  - app.yaml")
print("  - requirements.txt")
print("  - app.py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Subir los archivos al workspace

# COMMAND ----------

# Crear folder en workspace
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

w = WorkspaceClient()

WORKSPACE_FOLDER = f"/Users/{CURRENT_USER}/Latam_resources_spanish/Comfama_framework/app_source"

try:
    w.workspace.mkdirs(WORKSPACE_FOLDER)
except Exception:
    pass

# Subir cada archivo
for fname in ["app.yaml", "requirements.txt", "app.py"]:
    local_path = f"/tmp/comfama_app/{fname}"
    ws_path = f"{WORKSPACE_FOLDER}/{fname}"
    with open(local_path, "rb") as f:
        content = f.read()
    try:
        w.workspace.upload(
            path=ws_path,
            content=content,
            format=ImportFormat.AUTO,
            overwrite=True,
        )
        print(f"  ✓ subido {fname}")
    except Exception as e:
        print(f"  ⚠ {fname}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear/actualizar la app

# COMMAND ----------

from databricks.sdk.service.apps import App, AppResource, AppResourceServingEndpoint, AppResourceServingEndpointServingEndpointPermission

# Recursos: damos permiso al app para llamar el serving endpoint
serving_resource = AppResource(
    name="agente_comfama_endpoint",
    description="Endpoint del agente que el app va a consumir",
    serving_endpoint=AppResourceServingEndpoint(
        name=SERVING_ENDPOINT,
        permission=AppResourceServingEndpointServingEndpointPermission.CAN_QUERY,
    ),
)

# Crear o actualizar
try:
    existing = w.apps.get(name=APP_NAME)
    print(f"App {APP_NAME} ya existe — actualizando")
    app = w.apps.update(
        name=APP_NAME,
        app=App(
            name=APP_NAME,
            description="Asistente virtual Comfama — demo del framework Databricks",
            resources=[serving_resource],
        ),
    )
except Exception:
    print(f"Creando app {APP_NAME}")
    app = w.apps.create(
        app=App(
            name=APP_NAME,
            description="Asistente virtual Comfama — demo del framework Databricks",
            resources=[serving_resource],
        ),
    ).result()

print(f"✓ App: {app.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Desplegar el código del app

# COMMAND ----------

from databricks.sdk.service.apps import AppDeployment

# Apps API requires the /Workspace/ prefix
deployment = w.apps.deploy(
    app_name=APP_NAME,
    app_deployment=AppDeployment(
        source_code_path=f"/Workspace{WORKSPACE_FOLDER}",
    ),
).result()

print(f"✓ Deployment {deployment.deployment_id}")
print(f"  Status: {deployment.status.state if deployment.status else '?'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. URL del app + monitoreo

# COMMAND ----------

import time

# Esperar a que esté activa
for attempt in range(20):
    try:
        a = w.apps.get(name=APP_NAME)
        compute_state = a.compute_status.state if a.compute_status else "?"
        deploy_state = a.app_status.state if a.app_status else "?"
        print(f"  [{attempt+1:02d}] compute={compute_state}  app={deploy_state}")
        if str(compute_state).upper() == "ACTIVE" and str(deploy_state).upper() in ("SUCCEEDED", "DEPLOYED"):
            break
    except Exception as e:
        print(f"  err: {e}")
    time.sleep(20)

a = w.apps.get(name=APP_NAME)
print()
print(f"🎉 App URL: {a.url}")
print(f"   Compute: {a.compute_status.state if a.compute_status else '?'}")
print(f"   Service principal: {a.service_principal_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC | Componente | Detalles |
# MAGIC |---|---|
# MAGIC | App name | `comfama-agente-app` |
# MAGIC | Tipo | Streamlit |
# MAGIC | Backend | `agente_comfama` serving endpoint |
# MAGIC | Auth | On-behalf-of (token de usuario forward) |
# MAGIC | Source | `/Users/.../Comfama_framework/app_source/` |
# MAGIC
# MAGIC **Continuar:** Notebook `09 - Lakeview Dashboard`

