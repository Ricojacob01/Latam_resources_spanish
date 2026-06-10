# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — LAB: Conectar tu chatbot al Agente 🔗
# MAGIC
# MAGIC **20 min.** En la sesión de **Agentes & AI** crearon un **Agente de Facturas** (Knowledge Assistant) con Agent Bricks.
# MAGIC
# MAGIC En la sesión de **Apps** (notebook 02) crearon un **chatbot** que habla con Llama 3.3 70B directo.
# MAGIC
# MAGIC **Hoy los conectamos:** el mismo chatbot, pero ahora con el **cerebro del agente** (con conocimiento de las facturas).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diferencia conceptual
# MAGIC
# MAGIC ```
# MAGIC ANTES (notebook 02):
# MAGIC   User → 📱 chatbot-{tu_slug} → 🧠 databricks-meta-llama-3-3-70b-instruct
# MAGIC                                  (modelo "puro", sin contexto)
# MAGIC
# MAGIC AHORA (este notebook):
# MAGIC   User → 📱 chatbot-{tu_slug} → 🤖 agente_facturas_{tu_slug}
# MAGIC                                  (modelo + retrieval sobre tus 12 PDFs)
# MAGIC ```
# MAGIC
# MAGIC El mismo frontend, swap del backend. **Patrón Mosaic AI:** los endpoints son intercambiables porque hablan la misma API.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Verifica que tienes ambos assets

# COMMAND ----------

import re
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]

# Slug consistente con los demás notebooks
SLUG_APP = re.sub(r"[^a-z0-9]+", "-", CURRENT_USER.split("@")[0].lower()).strip("-")[:30]
SLUG_AGENT = re.sub(r"[^a-z0-9]+", "_", CURRENT_USER.split("@")[0].lower()).strip("_")[:25]

APP_NAME = f"chatbot-{SLUG_APP}"
AGENT_NAME = f"agente_facturas_{SLUG_AGENT}"

print(f"App name:   {APP_NAME}")
print(f"Agent name: {AGENT_NAME}")

# COMMAND ----------

# Verificar app
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

print("=== Verificando app ===")
try:
    app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
    print(f"✓ App existe: {app.get('name')}")
    print(f"  URL: {app.get('url')}")
    print(f"  Status: {app.get('app_status', {}).get('state')}")
    print(f"  Compute: {app.get('compute_status', {}).get('state')}")
except Exception as e:
    print(f"❌ App no encontrada: {e}")
    print(f"   Asegúrate de haber completado notebook 01 de la sesión de Apps")

# COMMAND ----------

# Verificar agent endpoint
print("=== Verificando agente ===")
try:
    agent_ep = w.api_client.do("GET", f"/api/2.0/serving-endpoints/{AGENT_NAME}")
    print(f"✓ Agent endpoint existe: {agent_ep.get('name')}")
    print(f"  Ready: {agent_ep.get('state', {}).get('ready')}")
    print(f"  Config: {agent_ep.get('state', {}).get('config_update')}")
except Exception as e:
    print(f"❌ Agent no encontrado: {e}")
    print(f"   Asegúrate de haber completado notebook 03 de la sesión de Agentes & AI")
    print(f"   Si tu agente tiene otro nombre, ajústalo abajo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Verificar el formato de respuesta del agente
# MAGIC
# MAGIC Antes de re-conectar el app, llamemos al agente directo para confirmar:
# MAGIC - Que acepta la misma estructura `messages` que el chatbot usa hoy
# MAGIC - Qué formato tiene la respuesta (puede incluir citations)

# COMMAND ----------

import requests, json

host = w.config.host
if host and not host.startswith("http"):
    host = f"https://{host}"
headers = w.config.authenticate()
token = headers.get("Authorization", "").split(" ", 1)[1]

url = f"{host}/serving-endpoints/{AGENT_NAME}/invocations"
payload = {
    "messages": [
        {"role": "user", "content": "¿Cuántas facturas hay en mi base de conocimiento?"}
    ]
}

resp = requests.post(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                     json=payload, timeout=60)

print(f"HTTP {resp.status_code}\n")
if resp.status_code == 200:
    data = resp.json()
    print("=== Estructura de la respuesta ===")
    print(json.dumps(data, indent=2)[:2000])
else:
    print(resp.text[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Análisis
# MAGIC
# MAGIC Agent Bricks expone su Knowledge Assistant con formato chat estándar. La respuesta puede venir como:
# MAGIC
# MAGIC - `choices[0].message.content` (formato OpenAI compatible) — el chatbot ya lo maneja ✅
# MAGIC - O `messages[-1].content` (formato Agent Framework)
# MAGIC - Las **citations** (PDFs source) vienen en metadata adicional
# MAGIC
# MAGIC Si la respuesta arriba siguió el formato OpenAI, **podemos simplemente cambiar el resource del app** sin tocar código.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Cambiar el resource binding del app
# MAGIC
# MAGIC El app `chatbot-{tu_slug}` tiene declarado un resource llamado `llm-endpoint` apuntando a `databricks-meta-llama-3-3-70b-instruct`. Lo vamos a cambiar para que apunte a tu agente.

# COMMAND ----------

# Actual resources del app
app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
print("Resources actuales:")
for r in app.get("resources", []):
    print(f"  📌 {r.get('name')}")
    if r.get("serving_endpoint"):
        print(f"     → {r['serving_endpoint'].get('name')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Update — apuntar el resource al agente

# COMMAND ----------

# Construir el body del UPDATE (PATCH-like vía PUT)
new_resources = []
for r in app.get("resources", []):
    if r.get("name") == "llm-endpoint":
        # Reemplazar el endpoint
        new_resources.append({
            "name": "llm-endpoint",
            "description": f"Agent Bricks endpoint para facturas — {AGENT_NAME}",
            "serving_endpoint": {
                "name": AGENT_NAME,
                "permission": "CAN_QUERY",
            },
        })
    else:
        new_resources.append(r)

# PUT al app con los nuevos resources
update_body = {
    "name": APP_NAME,
    "description": app.get("description"),
    "resources": new_resources,
}

updated = w.api_client.do("PATCH", f"/api/2.0/apps/{APP_NAME}", body=update_body)
print(f"✓ App actualizado")
print(f"\nNuevos resources:")
for r in updated.get("resources", []):
    print(f"  📌 {r.get('name')}")
    if r.get("serving_endpoint"):
        print(f"     → {r['serving_endpoint'].get('name')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Actualizar `app.py` para apuntar al nuevo endpoint
# MAGIC
# MAGIC En el código del app, `SERVING_ENDPOINT` venía del env var en `app.yaml`. Ahora actualizamos `app.yaml` para que apunte al agente.

# COMMAND ----------

APP_FOLDER = f"/Users/{CURRENT_USER}/chatbot-app-source"

# Leer app.yaml actual
import base64
yaml_path = f"{APP_FOLDER}/app.yaml"
exported = w.workspace.export(path=yaml_path, format="AUTO")
current_yaml = base64.b64decode(exported.content).decode()
print("=== app.yaml actual ===")
print(current_yaml)

# COMMAND ----------

# Generar nuevo yaml con el agente
new_yaml = f'''command:
  - "streamlit"
  - "run"
  - "app.py"

env:
  - name: SERVING_ENDPOINT
    value: "{AGENT_NAME}"
'''

print("=== app.yaml nuevo ===")
print(new_yaml)

# COMMAND ----------

# Subir el yaml actualizado
from databricks.sdk.service.workspace import ImportFormat

w.workspace.upload(
    path=yaml_path,
    content=new_yaml.encode(),
    format=ImportFormat.AUTO,
    overwrite=True,
)
print(f"✓ app.yaml actualizado")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Re-deploy el app

# COMMAND ----------

deploy_body = {
    "source_code_path": f"/Workspace{APP_FOLDER}",
}
deployment = w.api_client.do("POST", f"/api/2.0/apps/{APP_NAME}/deployments", body=deploy_body)
deployment_id = deployment.get("deployment_id")
print(f"✓ Deployment iniciado: {deployment_id}")

# Esperar SUCCEEDED
import time
for i in range(15):
    d = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}/deployments/{deployment_id}")
    state = d.get("status", {}).get("state", "?")
    print(f"  [{i+1:02d}] {state}")
    if state == "SUCCEEDED":
        print("✅ Deploy completo — el chatbot ahora habla con el agente")
        break
    if state == "FAILED":
        print(f"❌ Falló: {d.get('status', {}).get('message')}")
        break
    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: 🎉 Probar el chatbot conectado al agente

# COMMAND ----------

app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")
displayHTML(f'''
<div style="padding:20px;background:#E8F2F4;border:2px solid #1B5161;border-radius:10px;text-align:center">
  <h2 style="margin:0 0 12px 0;color:#1B3037">🎉 Tu chatbot ahora es un experto en facturas</h2>
  <div style="font-size:14px;color:#618793;margin-bottom:16px">
    Mismo app que ayer · Nuevo cerebro
  </div>
  <a href="{app['url']}" target="_blank"
     style="display:inline-block;padding:12px 24px;background:#FF3620;color:white;
            text-decoration:none;border-radius:6px;font-weight:600;font-size:16px">
    🚀 Abrir mi chatbot →
  </a>
  <div style="font-size:12px;color:#618793;margin-top:14px">
    Probar: "¿Cuánto compró Aaron Bergman?" — ahora debe responder con las facturas
  </div>
</div>
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tu turno
# MAGIC
# MAGIC Limpia la conversación previa (botón 🗑️) y prueba 3 preguntas:
# MAGIC
# MAGIC ```
# MAGIC 1. ¿Quién es Aaron Bergman y qué compró?
# MAGIC 2. Compara las facturas de Alyssa Tate y Brendan Murry
# MAGIC 3. ¿Cuál fue la factura más grande?
# MAGIC ```
# MAGIC
# MAGIC Y luego una pregunta **fuera del dominio**:
# MAGIC
# MAGIC ```
# MAGIC 4. ¿Qué es la teoría de la relatividad?
# MAGIC ```
# MAGIC
# MAGIC Observa: con un modelo "puro" responde con conocimiento general. Con el agente, debería decir "esa info no está en mis fuentes" (porque el system prompt del agente lo guía a hablar solo de los PDFs).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lección clave
# MAGIC
# MAGIC ```
# MAGIC App = frontend reusable
# MAGIC Backend (LLM o Agent) = swap declarativo
# MAGIC ```
# MAGIC
# MAGIC Esto es lo que les venden de **AI Gateway** y del patrón **Unified Query Interface**:
# MAGIC
# MAGIC - Cambiaste el "modelo" del chatbot **sin tocar Streamlit**
# MAGIC - Mismo payload `messages: [...]`
# MAGIC - Mismo `POST /serving-endpoints/{name}/invocations`
# MAGIC - El "modelo" puede ser una FM API, un Agent Bricks, un modelo custom, o un external proxy via AI Gateway

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Verificaron que su agente Knowledge Assistant está corriendo
# MAGIC ✅ Cambiaron el resource binding del app vía API
# MAGIC ✅ Actualizaron `app.yaml` para apuntar al nuevo endpoint
# MAGIC ✅ Re-deployaron el app (mismo código, distinto cerebro)
# MAGIC ✅ Probaron que el chatbot ahora habla con conocimiento de las facturas
# MAGIC
# MAGIC ## Lo que **no** hicieron (workshop deep-dive)
# MAGIC
# MAGIC - Mostrar citaciones (Sources) en la UI del chatbot
# MAGIC - Agregar `tools` al agente para que pueda buscar/calcular
# MAGIC - Configurar AI Gateway al frente del endpoint para guardrails
# MAGIC - A/B test entre Llama puro vs agente
# MAGIC - Persistir conversaciones en Lakebase
# MAGIC
# MAGIC ## Continuar → `04 - Cierre y Workshop Preview`

