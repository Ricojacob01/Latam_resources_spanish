# Databricks notebook source
# MAGIC %md
# MAGIC # ⌨️ Sesión 2 · 06 — Deploy-as-Code para su framework ⭐
# MAGIC
# MAGIC **Meta (cierre del workshop):** mostrar que **todo lo que hicimos a mano (UI/celdas) se puede desplegar como
# MAGIC código** — para integrarlo al **framework de agentes de Comfama** y a su **CI/CD**.
# MAGIC
# MAGIC Tres sabores del **mismo despliegue**:
# MAGIC 1. **Databricks Asset Bundle** (`databricks.yml`) — declarativo, ideal para Git/CI-CD.
# MAGIC 2. **CLI / Jobs API** — imperativo, para scripts.
# MAGIC 3. **SDK (Python)** — para integrarlo dentro del código del framework de Comfama.
# MAGIC
# MAGIC > Este es el **único módulo donde el código es el protagonista**. Los demás se podían hacer por UI; aquí el valor
# MAGIC > está justamente en **automatizar**.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. El bundle — `databricks.yml`
# MAGIC Un Asset Bundle versiona en Git **el agente, su endpoint, la app y un job** que reconstruye todo. Lo escribimos
# MAGIC en `../bundle/`.

# COMMAND ----------

DATABRICKS_YML = f"""
bundle:
  name: agente-afiliados-comfama

variables:
  catalog:   {{default: {CATALOG}}}
  schema:    {{default: {SCHEMA}}}

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://{spark.conf.get("spark.databricks.workspaceUrl")}
  prod:
    mode: production
    workspace:
      host: https://{spark.conf.get("spark.databricks.workspaceUrl")}

resources:

  jobs:
    construir_agente:
      name: "[Comfama] Construir y desplegar agente"
      tasks:
        - task_key: setup_kb
          notebook_task: {{notebook_path: ../Sesion 1 - Construir y Servir el Agente/02 - Setup & Knowledge Base}}
        - task_key: lakebase
          depends_on: [{{task_key: setup_kb}}]
          notebook_task: {{notebook_path: ../Sesion 1 - Construir y Servir el Agente/03 - Lakebase (datos del afiliado)}}
        - task_key: construir
          depends_on: [{{task_key: lakebase}}]
          notebook_task: {{notebook_path: ../Sesion 1 - Construir y Servir el Agente/04 - Construir el Agente}}
        - task_key: servir
          depends_on: [{{task_key: construir}}]
          notebook_task: {{notebook_path: ../Sesion 1 - Construir y Servir el Agente/05 - Servir el Agente}}
      schedule:
        quartz_cron_expression: "0 0 6 * * ?"   # reconstrucción diaria 6am
        timezone_id: "America/Bogota"

  apps:
    asistente_afiliados:
      name: {APP_NAME}
      source_code_path: ../app_source
      resources:
        - name: agente
          serving_endpoint:
            name: {AGENT_ENDPOINT}
            permission: CAN_QUERY
"""

from databricks.sdk import WorkspaceClient
import os, base64
w = WorkspaceClient()

def upload_raw(path, content):
    """Escribe un archivo crudo (no-notebook) en el workspace vía REST (format=RAW)."""
    w.api_client.do("POST", "/api/2.0/workspace/import",
                    body={"path": path, "format": "RAW", "overwrite": True,
                          "content": base64.b64encode(content.encode("utf-8")).decode()})

nb_dir = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
WORKSHOP = os.path.dirname(nb_dir)   # .../workshop
BUNDLE_DIR = WORKSHOP + "/bundle"
w.workspace.mkdirs(BUNDLE_DIR)
upload_raw(f"{BUNDLE_DIR}/databricks.yml", DATABRICKS_YML)
print("✅ Bundle escrito en", BUNDLE_DIR)
print(DATABRICKS_YML)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Desplegar el bundle (CLI)
# MAGIC Desde la carpeta `bundle/` (en local o en CI):
# MAGIC ```bash
# MAGIC databricks bundle validate          # valida el YAML
# MAGIC databricks bundle deploy -t dev     # crea/actualiza job + app + recursos
# MAGIC databricks bundle run construir_agente   # ejecuta el pipeline del agente
# MAGIC databricks bundle deploy -t prod    # promoción a producción
# MAGIC ```
# MAGIC En **CI/CD** (GitHub Actions / Azure DevOps) basta con `databricks bundle deploy -t prod` en el pipeline de Comfama.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. El mismo job, por **Jobs API / SDK**
# MAGIC Si el framework de Comfama prefiere llamar APIs desde su propio código (Python), este es el equivalente:

# COMMAND ----------

# Ejemplo (no se ejecuta aquí): crear el job del pipeline por SDK
job_json = {
  "name": "[Comfama] Construir y desplegar agente",
  "tasks": [
    {"task_key": "construir", "notebook_task": {
        "notebook_path": f"{WORKSHOP}/Sesion 1 - Construir y Servir el Agente/04 - Construir el Agente"}},
    {"task_key": "servir", "depends_on": [{"task_key": "construir"}], "notebook_task": {
        "notebook_path": f"{WORKSHOP}/Sesion 1 - Construir y Servir el Agente/05 - Servir el Agente"}},
  ],
}
print("Job JSON (para w.jobs.create / POST /api/2.1/jobs/create):")
import json; print(json.dumps(job_json, indent=2, ensure_ascii=False))
# w.jobs.create(**job_json)   # <- descomentar para crearlo por SDK

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Cómo encaja en el **framework de agentes de Comfama**
# MAGIC
# MAGIC | Pieza del framework Comfama | Reemplazo Databricks | Cómo se despliega como código |
# MAGIC |---|---|---|
# MAGIC | Definición del agente (`TemplateAgentes`) | Modelo en UC + `agents.deploy` | tarea `construir`/`servir` del bundle |
# MAGIC | Config de LLM (`LLMConfig`) | AI Gateway (`put_ai_gateway`) | celda/tarea idempotente en el pipeline |
# MAGIC | Estado (Cosmos) | Lakebase | `databricks postgres`/synced tables en el bundle |
# MAGIC | Frontend (Container Apps) | Databricks App | recurso `apps:` del bundle |
# MAGIC | Despliegue custom | **`databricks bundle deploy`** | un solo comando en su CI/CD |
# MAGIC
# MAGIC > **Mensaje de cierre:** Comfama puede mantener su repo y su CI/CD; solo cambia *qué* despliega: en vez de
# MAGIC > contenedores + Cosmos + telemetría custom, despliega **un bundle** que para Databricks-native.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ▶️ Siguiente: `07 - Cierre y Recap`

