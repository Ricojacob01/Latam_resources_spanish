# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — LAB Express: inspecciona TU app 🧪
# MAGIC
# MAGIC **20 min.** Vamos a inspeccionar el chatbot que acabas de desplegar — su config, resources, auth, logs y audit.
# MAGIC
# MAGIC ⚠️ Necesitas haber completado `01 - Product Tour (Slides)` que crea tu app.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recuperar el nombre de tu app

# COMMAND ----------

import re
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLUG = re.sub(r"[^a-z0-9]+", "-", CURRENT_USER.split("@")[0].lower()).strip("-")[:30]
APP_NAME = f"chatbot-{SLUG}"

print(f"Inspeccionando: {APP_NAME}")
print(f"User: {CURRENT_USER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — Abrir el app en la UI (3 min)
# MAGIC
# MAGIC ### Pasos
# MAGIC
# MAGIC 1. **Sidebar izquierdo** → **Compute** → tab **Apps**
# MAGIC 2. Click en tu app (`chatbot-<tu-username>`)
# MAGIC 3. Observa las 4 tabs principales:
# MAGIC    - **Overview** — URL, status, service principal asignado
# MAGIC    - **Resources** — el endpoint Llama que declaramos en notebook 01
# MAGIC    - **Deployments** — historial (deberías tener 1 deploy SUCCEEDED)
# MAGIC    - **Logs** — stdout/stderr del Streamlit en vivo
# MAGIC 4. Click **Open app** arriba — abre tu chatbot
# MAGIC 5. Manda 1-2 mensajes nuevos (los vamos a buscar en los logs y audit)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Config del app via SDK (8 min)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

app = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}")

print(f"Nombre:       {app.get('name')}")
print(f"URL:          {app.get('url')}")
print(f"Estado app:   {app.get('app_status', {}).get('state')}")
print(f"Estado compute: {app.get('compute_status', {}).get('state')}")
print(f"Service Principal: {app.get('service_principal_id')}")
print(f"Creator:      {app.get('creator')}")
print(f"Created at:   {app.get('create_time')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resources declarados
# MAGIC
# MAGIC En notebook 01 declaramos un solo resource: el endpoint de Llama. Databricks auto-granteó CAN_QUERY al service principal.

# COMMAND ----------

resources = app.get("resources", [])
if resources:
    for r in resources:
        print(f"📌 {r.get('name')}")
        print(f"   description: {r.get('description')}")
        if r.get("serving_endpoint"):
            se = r["serving_endpoint"]
            print(f"   serving endpoint: {se.get('name')} ({se.get('permission')})")
        if r.get("sql_warehouse"):
            sw = r["sql_warehouse"]
            print(f"   sql warehouse: {sw.get('id')} ({sw.get('permission')})")
        if r.get("secret"):
            s = r["secret"]
            print(f"   secret: {s.get('scope')}/{s.get('key')} ({s.get('permission')})")
        print()
else:
    print("Sin resources declarados")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Active deployment

# COMMAND ----------

ad = app.get("active_deployment", {})
if ad:
    print(f"Deployment ID:  {ad.get('deployment_id')}")
    print(f"Status:         {ad.get('status', {}).get('state')} - {ad.get('status', {}).get('message')}")
    print(f"Source path:    {ad.get('source_code_path')}")
    print(f"Created at:     {ad.get('create_time')}")
    print(f"Mode:           {ad.get('mode')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — Code source del app (3 min)
# MAGIC
# MAGIC El app vive como 3 archivos en tu carpeta personal. Vamos a verlos.

# COMMAND ----------

APP_FOLDER = f"/Users/{CURRENT_USER}/chatbot-app-source"
print(f"Source folder: {APP_FOLDER}\n")

for item in w.workspace.list(APP_FOLDER):
    print(f"  {item.object_type:10s}  {item.path.split('/')[-1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ver el código del app
# MAGIC
# MAGIC En el sidebar → **Workspace** → tu user → `chatbot-app-source/` → click `app.py`
# MAGIC
# MAGIC Verás Streamlit puro. Es **~60 líneas** y hace lo siguiente:
# MAGIC
# MAGIC ```
# MAGIC 1. Recupera el SP token del runtime (no necesita PAT)
# MAGIC 2. Mantiene historial de chat en st.session_state
# MAGIC 3. POST al endpoint /serving-endpoints/{name}/invocations
# MAGIC 4. Renderiza la respuesta con st.chat_message
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Logs en vivo (3 min)

# COMMAND ----------

# Logs en vivo: no hay REST endpoint — se ven en UI o CLI.
# Aquí mostramos el estado del deployment más reciente via API.

deploy_id = app.get("active_deployment", {}).get("deployment_id")
if deploy_id:
    dep = w.api_client.do("GET", f"/api/2.0/apps/{APP_NAME}/deployments/{deploy_id}")
    print(f"Deployment: {dep['deployment_id']}")
    print(f"Estado:     {dep.get('status', {}).get('state')} — {dep.get('status', {}).get('message')}")
    print(f"Source:     {dep.get('source_code_path')}")
    print(f"Creado:     {dep.get('create_time')}")
    print(f"Modo:       {dep.get('mode')}")
else:
    print("No hay deployment activo.")

print(f"\n📋 Para logs en streaming (stdout/stderr del Streamlit):")
print(f"   UI → Compute → Apps → {APP_NAME} → tab Logs")
print(f"   CLI: databricks apps logs {APP_NAME} --follow")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte E — Audit + costos (3 min)

# COMMAND ----------

# Audit: quién accedió al app
display(spark.sql(f"""
SELECT
  event_time,
  user_identity.email,
  action_name,
  request_params
FROM system.access.audit
WHERE event_date >= current_date() - INTERVAL 1 DAYS
  AND service_name = 'apps'
  AND request_params.app_name = '{APP_NAME}'
ORDER BY event_time DESC
LIMIT 10
"""))

# COMMAND ----------

# Endpoint usage del modelo que está usando tu app
display(spark.sql(f"""
SELECT
  date(u.request_time) AS dia,
  e.endpoint_name,
  COUNT(*) AS requests,
  SUM(u.input_token_count + u.output_token_count) AS total_tokens
FROM system.serving.endpoint_usage u
JOIN system.serving.served_entities e
  ON u.served_entity_id = e.served_entity_id
WHERE u.request_time >= current_date() - INTERVAL 7 DAYS
  AND e.endpoint_name = 'databricks-meta-llama-3-3-70b-instruct'
GROUP BY dia, e.endpoint_name
ORDER BY dia DESC
"""))

# COMMAND ----------

# Cost del app específicamente (Apps se factura bajo ALL_PURPOSE_SERVERLESS)
display(spark.sql(f"""
SELECT
  date(usage_start_time) AS dia,
  usage_metadata.app_name,
  sku_name,
  ROUND(SUM(usage_quantity), 4) AS dbus
FROM system.billing.usage
WHERE usage_metadata.app_name = '{APP_NAME}'
  AND usage_start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY dia, usage_metadata.app_name, sku_name
ORDER BY dia DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Desplegaste tu propio chatbot funcional (notebook 01)
# MAGIC ✅ Probaste el chatbot en vivo
# MAGIC ✅ Inspeccionaste su config, resources, service principal
# MAGIC ✅ Viste el código fuente (3 archivos, ~60 líneas)
# MAGIC ✅ Revisaste logs + audit + cost tracking en System Tables
# MAGIC
# MAGIC ## Lo que **no** hicimos (workshop deep-dive del fin de mes)
# MAGIC
# MAGIC - Conectar el app a Vector Search para RAG
# MAGIC - Auth OBO (que el chatbot use la identidad del user, no del SP)
# MAGIC - Persistir conversaciones en Lakebase
# MAGIC - CI/CD del app con DABs
# MAGIC - Custom domains + share con users externos
# MAGIC - Múltiples deployments + rollback
# MAGIC
# MAGIC ## Cleanup (opcional, después de la sesión)
# MAGIC
# MAGIC Si **no** vas a usar tu app después de la sesión, lo puedes detener (no eliminar) para no acumular costo:
# MAGIC
# MAGIC ```python
# MAGIC # Detener compute (puedes restartear cuando quieras)
# MAGIC w.api_client.do("POST", f"/api/2.0/apps/{APP_NAME}/stop")
# MAGIC
# MAGIC # O eliminar completamente
# MAGIC # w.api_client.do("DELETE", f"/api/2.0/apps/{APP_NAME}")
# MAGIC ```
# MAGIC
# MAGIC ## Continuar → `03 - Cierre y Workshop Preview`
