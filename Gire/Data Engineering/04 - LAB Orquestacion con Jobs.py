# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — LAB 🗓️ · Orquestación con Lakeflow Jobs
# MAGIC
# MAGIC **25 min.** Encadenamos el pipeline del módulo `03` con una tarea de KPIs en un **Job** con schedule, dependencias, reintentos y alertas. Lo construimos en la **UI** y luego lo definimos como **código** (Databricks Asset Bundle / JSON).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Secuencial (UI → Code)**
# MAGIC
# MAGIC Un Job se **entiende mejor visualmente** (tareas, dependencias, schedule, reintentos, alertas) en la **Jobs UI**. Una vez claro el concepto, lo volvemos **código** (Asset Bundle / JSON / SDK) para llevarlo a **CI/CD** y versionarlo en Git.
# MAGIC
# MAGIC > Es el patrón inverso al módulo `02` (Code → UI): aquí la UI **enseña** y el código **industrializa**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — Construir el Job en la UI (🖱️)
# MAGIC
# MAGIC 1. **Sidebar → Jobs & Pipelines → Create → Job**. Nombre: `gire_de_job_<tu_usuario>`.
# MAGIC 2. **Tarea 1 — Pipeline:**
# MAGIC    - Task name: `01_pipeline`
# MAGIC    - Type: **Pipeline**
# MAGIC    - Pipeline: selecciona `gire_de_<tu_usuario>` (el del módulo 03).
# MAGIC 3. **Tarea 2 — KPIs (depende de la 1):**
# MAGIC    - **+ Add task** → Type: **Notebook**
# MAGIC    - Notebook: este folder → `tasks/refrescar_kpis`
# MAGIC    - **!!!IMPORTANTE!!!!** : Edita el archivo `tasks/refrescar_kpis` para usar to catalago
# MAGIC    - **Depends on:** `01_pipeline`
# MAGIC 4. **Schedule & Triggers:** **Add trigger** → Scheduled → cada 1 hora (cron `0 0 * * * ?`). Para el lab puedes dejarlo **Paused**.
# MAGIC 5. **Reintentos:** en la tarea, **Retries** = 2, con backoff.
# MAGIC 6. **Alertas:** **Notifications** → on failure → tu email (o un canal de Slack vía webhook).
# MAGIC 7. **Run now** y observa el **grafo de tareas** y la línea de tiempo de la corrida.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Ver el Job como CÓDIGO desde la propia UI
# MAGIC
# MAGIC En la página del Job, esquina superior derecha → **kebab (⋮) → View JSON** (o **Switch to code version** / **YAML**).
# MAGIC
# MAGIC > 💡 Insight clave: **todo lo que armaste con clicks es serializable**. Ese JSON/YAML es lo que versionas en Git. Lo replicamos abajo como **Databricks Asset Bundle (DAB)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — El Job como CÓDIGO: Databricks Asset Bundle
# MAGIC
# MAGIC En la carpeta `jobs/` de este track dejamos un bundle listo:
# MAGIC
# MAGIC - `jobs/databricks.yml` — define el Job (2 tareas, schedule, reintentos, notificaciones) de forma declarativa.
# MAGIC - `jobs/job.json` — el mismo Job en formato JSON de la **Jobs API 2.1** (`POST /api/2.1/jobs/create`).
# MAGIC
# MAGIC Desde tu máquina (no en el notebook):
# MAGIC ```bash
# MAGIC cd "Gire/Data Engineering/jobs"
# MAGIC databricks bundle validate
# MAGIC databricks bundle deploy -t dev      # crea/actualiza el Job
# MAGIC databricks bundle run gire_de_pipeline_job -t dev
# MAGIC ```
# MAGIC
# MAGIC Esto es **infraestructura como código**: el mismo Job, reproducible, en cualquier workspace (dev/prod) y revisable en un PR.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Alternativa: crear el Job por API/SDK (código)
# MAGIC
# MAGIC Si prefieres el SDK de Python (mismo resultado que la UI):

# COMMAND ----------

# Demostrativo — descomenta para crear el Job vía SDK.
# Requiere que exista el pipeline del módulo 03 (pasa su pipeline_id).
#
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service import jobs
# w = WorkspaceClient()
# _user = spark.sql("SELECT current_user()").collect()[0][0]
# nb = f"/Workspace/Users/{_user}/Latam_resources_spanish/Gire/Data Engineering/tasks/refrescar_kpis"
# PIPELINE_ID = "PEGA_AQUI_EL_PIPELINE_ID"   # cópialo de la UI del pipeline (Settings)
#
# created = w.jobs.create(
#     name=f"gire_de_job_{_user.split('@')[0]}",
#     tasks=[
#         jobs.Task(task_key="01_pipeline",
#                   pipeline_task=jobs.PipelineTask(pipeline_id=PIPELINE_ID)),
#         jobs.Task(task_key="02_kpis",
#                   depends_on=[jobs.TaskDependency(task_key="01_pipeline")],
#                   notebook_task=jobs.NotebookTask(notebook_path=nb),
#                   max_retries=2),
#     ],
#     schedule=jobs.CronSchedule(quartz_cron_expression="0 0 * * * ?",
#                                timezone_id="America/Bogota",
#                                pause_status=jobs.PauseStatus.PAUSED),
# )
# print("Job creado:", created.job_id)

print("Celda demostrativa — revisa jobs/databricks.yml y jobs/job.json para la versión declarativa.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Construiste un Job multi-tarea en la **UI** (pipeline → KPIs) con schedule, reintentos y alertas
# MAGIC ✅ Viste que la UI **serializa** a JSON/YAML
# MAGIC ✅ Definiste el mismo Job como **código**: Asset Bundle, JSON de la API y SDK
# MAGIC ✅ Entendiste el patrón **UI → Code** para industrializar (CI/CD)
# MAGIC
# MAGIC ## Continuar → `05 - Cierre y Workshop Preview`
