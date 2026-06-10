# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour + Deploy tu propio chatbot 📊 🚀
# MAGIC
# MAGIC ~25 min. Primero slides oficiales del Apps Product Deck. Al final, **cada uno va a desplegar su propio chatbot** — que luego exploramos en notebook 02.

# COMMAND ----------

import os, base64
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLIDES_PATH = f"/Workspace/Users/{CURRENT_USER}/Latam_resources_spanish/Comfama/Apps/imagenes"

def show_slide(filename, width=1100, caption=""):
    full_path = f"{SLIDES_PATH}/{filename}"
    try:
        with open(full_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        html = f'<div style="margin:8px 0"><img src="data:image/png;base64,{b64}" style="max-width:{width}px;width:100%;border:1px solid #ddd;border-radius:6px"/>'
        if caption:
            html += f'<div style="font-size:13px;color:#666;font-style:italic;margin-top:6px">{caption}</div>'
        html += "</div>"
        displayHTML(html)
    except FileNotFoundError:
        displayHTML(f'<div style="padding:20px;background:#fee;border:1px solid #fcc">Slide no encontrado: {full_path}</div>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 — El problema

# COMMAND ----------

show_slide("01_apps_cover.png", caption="Databricks Apps — construir aplicaciones data-driven con seguridad")

# COMMAND ----------

show_slide("02_apps_problem.png", caption="Las apps eran difíciles de construir y desplegar — bottleneck de DevOps + infra dedicada")

# COMMAND ----------

show_slide("03_apps_intro.png", caption="Apps simplifica el dev en frameworks que los devs ya conocen, con production-readiness")

# COMMAND ----------

show_slide("04_apps_customers.png", caption="2500+ customers desde Nov 2024 — adopción rápida")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 — Cuándo usar Apps

# COMMAND ----------

show_slide("05_apps_usecases.png", caption="Use cases: interactive data apps, predictive analytics, dashboards, chatbots, internal tools")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 — Stack técnico

# COMMAND ----------

show_slide("06_apps_simple_frameworks.png", caption="Frameworks soportados: Dash, Gradio, Streamlit, Flask, Shiny, Node.js. Familiares para devs")

# COMMAND ----------

show_slide("07_apps_production_ready.png", caption="Production-ready: serverless compute auto-provisioned, no manejas infra")

# COMMAND ----------

show_slide("11_apps_architecture.png", caption="Arquitectura: Serverless Compute corre en tu cloud, accesible vía workspace")

# COMMAND ----------

show_slide("12_apps_integration.png", caption="Apps se integran con SQL Warehouse, Model Serving, Vector Search, Volumes, Secrets")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4 — Auth + sharing + governance

# COMMAND ----------

show_slide("08_apps_sharing.png", caption="Sharing: con users específicos del workspace o externos — no necesitan acceso al workspace")

# COMMAND ----------

show_slide("09_apps_auth.png", caption="Apps Authentication: el browser del user habla con el app vía OAuth")

# COMMAND ----------

show_slide("10_apps_obo_sp.png",
           caption="Dos identidades: On-Behalf-Of (user pass-through) + Dedicated Service Principal (para acceso a recursos)")

# COMMAND ----------

show_slide("14_apps_resources.png",
           caption="Resources: declaras qué endpoints/warehouses/secrets necesita el app, Databricks auto-grants permisos al SP")

# COMMAND ----------

show_slide("15_apps_audit.png", caption="Audit log: logins, permission changes — todo en system.access.audit")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 5 — Recursos para devs

# COMMAND ----------

show_slide("13_apps_cookbook.png",
           caption="Apps Cookbook: 10+ recipes para casos comunes. apps-cookbook.dev")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 6 — Pricing + best practices

# COMMAND ----------

show_slide("16_apps_pricing.png", caption="Pricing: por hora basado en capacity. Standard = up to 2 CPUs/app, 0.5 DBU/h")

# COMMAND ----------

show_slide("17_apps_best_practices.png", caption="Best practices: CI/CD para promote entre envs, restringe 'Can Manage', sigue SDLC")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 Hands-on: cada uno deploy su propio chatbot
# MAGIC
# MAGIC Suficiente teoría. Ahora cada uno va a:
# MAGIC
# MAGIC 1. Crear los 3 archivos del app (yaml + requirements + py)
# MAGIC 2. Subirlos a su carpeta personal en el workspace
# MAGIC 3. Crear y deploy el app vía API
# MAGIC 4. Abrirlo en otra pestaña y probarlo
# MAGIC
# MAGIC Al final cada uno tendrá su propio chatbot funcional llamando a Llama 3.3 70B. En el siguiente notebook lo exploramos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Definir tu app
# MAGIC
# MAGIC Tu app va a ser único — usa tu username como sufijo para que no colisione con tus compañeros.

# COMMAND ----------

import re
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
# user-friendly slug from email
SLUG = re.sub(r"[^a-z0-9]+", "-", CURRENT_USER.split("@")[0].lower()).strip("-")[:30]

APP_NAME = f"chatbot-{SLUG}"
APP_FOLDER = f"/Users/{CURRENT_USER}/chatbot-app-source"
APP_FOLDER_WORKSPACE = f"/Workspace{APP_FOLDER}"
SERVING_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"  # Foundation Model

print(f"App name:        {APP_NAME}")
print(f"Source folder:   {APP_FOLDER}")
print(f"Model endpoint:  {SERVING_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generar los 3 archivos del app

# COMMAND ----------

# app.py — el código Streamlit
APP_PY = '''import streamlit as st
import os
import requests
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Chatbot Express", page_icon="💬", layout="centered")

SERVING_ENDPOINT = os.environ.get("SERVING_ENDPOINT", "databricks-meta-llama-3-3-70b-instruct")

def get_auth():
    """SP token (auto-mint via Apps runtime DATABRICKS_CLIENT_ID/SECRET)."""
    w = WorkspaceClient()
    host = w.config.host or os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    headers = w.config.authenticate()
    auth_val = headers.get("Authorization", "")
    token = auth_val.split(" ", 1)[1] if auth_val.startswith("Bearer ") else (w.config.token or "")
    return host, token

with st.sidebar:
    st.title("💬 Chatbot Express")
    st.markdown(f"**Modelo:** `{SERVING_ENDPOINT}`")
    st.markdown("---")
    if st.button("🗑️ Limpiar conversación"):
        st.session_state.messages = []
    st.caption("Powered by Databricks Foundation Models")

st.title("Tu Chatbot")
st.caption("Pregunta lo que quieras — habla con Llama 3.3 70B")

if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

if prompt := st.chat_input("Escribe tu mensaje..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Pensando..."):
            try:
                host, token = get_auth()
                url = f"{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations"
                # Convert chat history to OpenAI-style messages
                api_messages = [
                    {"role": "system", "content": "Eres un asistente útil y conciso. Responde en español."}
                ] + [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages]
                resp = requests.post(
                    url,
                    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                    json={"messages": api_messages, "max_tokens": 500, "temperature": 0.7},
                    timeout=60,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    answer = data["choices"][0]["message"]["content"]
                    st.markdown(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})
                else:
                    err = f"❌ Error {resp.status_code}: {resp.text[:200]}"
                    st.error(err)
                    st.session_state.messages.append({"role": "assistant", "content": err})
            except Exception as e:
                st.error(f"❌ {e}")
                st.session_state.messages.append({"role": "assistant", "content": str(e)})
'''

# app.yaml — entry point
APP_YAML = '''command:
  - "streamlit"
  - "run"
  - "app.py"

env:
  - name: SERVING_ENDPOINT
    value: "databricks-meta-llama-3-3-70b-instruct"
'''

# requirements.txt
REQUIREMENTS = '''streamlit==1.39.0
requests==2.32.3
databricks-sdk>=0.30.0
'''

print("✓ Archivos definidos en memoria")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Subir los archivos al workspace

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
import base64

w = WorkspaceClient()

# Crear folder
try:
    w.workspace.mkdirs(APP_FOLDER)
    print(f"✓ Folder creado: {APP_FOLDER}")
except Exception:
    print(f"✓ Folder ya existe: {APP_FOLDER}")

# Subir cada archivo
files = {"app.py": APP_PY, "app.yaml": APP_YAML, "requirements.txt": REQUIREMENTS}
for fname, content in files.items():
    w.workspace.upload(
        path=f"{APP_FOLDER}/{fname}",
        content=content.encode(),
        format=ImportFormat.AUTO,
        overwrite=True,
    )
    print(f"  ✓ subido {fname}")

# Verificar
print("\nContenido del folder:")
for item in w.workspace.list(APP_FOLDER):
    print(f"  {item.object_type:10s} {item.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Crear el app

# COMMAND ----------

# Verificar si ya existe
try:
    existing = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
    print(f"App {APP_NAME} ya existe — saltamos creación")
    print(f"  status: {existing.get('app_status', {}).get('state')}")
    print(f"  url:    {existing.get('url')}")
    app_existed = True
except Exception:
    app_existed = False

if not app_existed:
    create_body = {
        "name": APP_NAME,
        "description": f"Chatbot personal de {CURRENT_USER} — llama Llama 3.3 70B",
        "resources": [
            {
                "name": "llm-endpoint",
                "description": "Foundation Model endpoint que el app va a llamar",
                "serving_endpoint": {
                    "name": SERVING_ENDPOINT,
                    "permission": "CAN_QUERY",
                },
            },
        ],
    }
    created = w.api_client.do("POST", "/api/2.0/apps", body=create_body)
    print(f"✓ App creado: {created.get('name')}")
    print(f"  Esperando que el compute esté activo (1-2 min)...")

# COMMAND ----------

# Esperar compute ACTIVE
import time
for i in range(20):
    app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
    compute_state = app.get("compute_status", {}).get("state", "?")
    print(f"  [{i+1:02d}] compute={compute_state}")
    if compute_state == "ACTIVE":
        print("✓ Compute activo")
        break
    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Deploy el código

# COMMAND ----------

deployment_body = {
    "source_code_path": APP_FOLDER_WORKSPACE,
}
deployment = w.api_client.do("POST", f"/api/2.0/apps/{APP_NAME}/deployments", body=deployment_body)
deployment_id = deployment.get("deployment_id")
print(f"✓ Deployment iniciado: {deployment_id}")

# Esperar a que esté SUCCEEDED
for i in range(20):
    d = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}/deployments/{deployment_id}")
    state = d.get("status", {}).get("state", "?")
    msg = d.get("status", {}).get("message", "")
    print(f"  [{i+1:02d}] state={state}  {msg[:60]}")
    if state == "SUCCEEDED":
        print("✅ Deploy completo")
        break
    if state == "FAILED":
        print("❌ Deploy falló")
        break
    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: 🎉 Abre tu app y prueba

# COMMAND ----------

app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
displayHTML(f'''
<div style="padding:20px;background:#E8F2F4;border:2px solid #1B5161;border-radius:10px;text-align:center">
  <h2 style="margin:0 0 12px 0;color:#1B3037">🎉 Tu app está corriendo</h2>
  <div style="font-size:14px;color:#618793;margin-bottom:16px">{APP_NAME}</div>
  <a href="{app['url']}" target="_blank"
     style="display:inline-block;padding:12px 24px;background:#FF3620;color:white;
            text-decoration:none;border-radius:6px;font-weight:600;font-size:16px">
    🚀 Abrir mi chatbot →
  </a>
  <div style="font-size:12px;color:#618793;margin-top:12px">
    Probar: "¿Qué es Lakebase?" o "Hazme un haiku sobre datos"
  </div>
</div>
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tu turno (5 min)
# MAGIC
# MAGIC Click el botón de arriba. Manda **3 preguntas** de prueba:
# MAGIC
# MAGIC 1. *"¿Qué es Databricks AI Gateway?"*
# MAGIC 2. *"Dame 3 ideas para automatizar tareas de service desk con IA"*
# MAGIC 3. *(libre — algo de tu dominio)*
# MAGIC
# MAGIC Mientras lo pruebas, observa:
# MAGIC - **Latencia** del primer mensaje (cold start)
# MAGIC - **Streaming** vs respuesta completa
# MAGIC - **Tone** de Llama 3.3 70B
# MAGIC
# MAGIC Cuando todos hayan probado el suyo → continúa con `02 - LAB Express` donde inspeccionamos por dentro lo que acabas de crear.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variable global para el siguiente notebook
# MAGIC
# MAGIC Guardamos el nombre de tu app para que el notebook 02 sepa cuál mirar:

# COMMAND ----------

dbutils.jobs.taskValues.set(key="app_name", value=APP_NAME)
print(f"App name guardado para notebook 02: {APP_NAME}")
print(f"App URL: {app['url']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuar → `02 - LAB Express` (inspecciona TU app)
