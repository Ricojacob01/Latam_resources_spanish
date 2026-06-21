# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — ⭐ Orquestación · Job del pipeline ML 🗓️
# MAGIC
# MAGIC Encadenamos todo el pipeline en un **Lakeflow Job** que corre solo, en schedule, con reintentos y alertas. **Esto es lo que faltaba en `ML_workshop`.**
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Secuencial (UI → Code)**
# MAGIC Primero armas el Job **en la Jobs UI** (ves el grafo de tareas, el schedule, los reintentos, las alertas — intuición). Luego lo defines como **código** (Databricks Asset Bundle / JSON) para versionarlo en Git y llevarlo a CI/CD. El Job reutiliza los notebooks de `pipeline/`.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## El grafo del pipeline
# MAGIC
# MAGIC ```
# MAGIC   feature_engineering ─► train_and_register ─► validate_and_promote ─┬─► deploy_serving
# MAGIC                                                                      └─► batch_scoring
# MAGIC ```
# MAGIC
# MAGIC - Si `validate_and_promote` **falla** (el Challenger no pasa los checks), `deploy_serving` y `batch_scoring` **no corren** — no se publica un modelo malo.
# MAGIC - `deploy_serving` y `batch_scoring` corren **en paralelo** tras la validación.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — Construir el Job en la UI (🖱️)
# MAGIC
# MAGIC 1. **Sidebar → Jobs & Pipelines → Create → Job.** Nombre: `cp_mlops_churn_<tu_usuario>`.
# MAGIC 2. **Tarea 1** `feature_engineering` → Type **Notebook** → `pipeline/01_feature_engineering`.
# MAGIC    - **Cluster:** crea un *Job cluster* con **ML Runtime** (p.ej. `15.4.x-cpu-ml-scala2.12`).
# MAGIC 3. **+ Add task** `train_and_register` → `pipeline/02_train_and_register` → **Depends on:** `feature_engineering`.
# MAGIC 4. **+ Add task** `validate_and_promote` → `pipeline/03_validate_and_promote` → depende de `train_and_register`. **Retries: 1**.
# MAGIC 5. **+ Add task** `deploy_serving` → `pipeline/04_deploy_serving` → depende de `validate_and_promote`.
# MAGIC 6. **+ Add task** `batch_scoring` → `pipeline/05_batch_scoring` → depende de `validate_and_promote`.
# MAGIC 7. **Schedule:** **Add trigger** → Scheduled → cron `0 0 6 ? * MON` (lunes 6am). Déjalo **Paused** para el lab.
# MAGIC 8. **Notifications:** on failure → tu email / Slack webhook.
# MAGIC 9. **Run now** y observa el **grafo** ejecutarse y la línea de tiempo de cada tarea.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Verlo como código desde la UI
# MAGIC
# MAGIC En la página del Job: **⋮ → View JSON / Edit as YAML**. Todo lo que armaste con clicks es **serializable** — eso es lo que versionamos. Lo replicamos como bundle abajo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — El Job como código: Databricks Asset Bundle
# MAGIC
# MAGIC En `bundle/` dejamos el Job completo:
# MAGIC - `bundle/databricks.yml` — DAB con las 5 tareas, el job cluster ML, schedule, reintentos y notificaciones.
# MAGIC - `bundle/job.json` — el mismo Job en JSON de la Jobs API 2.1.
# MAGIC
# MAGIC Desde tu máquina:
# MAGIC ```bash
# MAGIC cd CP/MLOps/bundle
# MAGIC databricks bundle validate
# MAGIC databricks bundle deploy -t dev
# MAGIC databricks bundle run mlops_churn_pipeline -t dev
# MAGIC ```
# MAGIC
# MAGIC O por API directa:
# MAGIC ```bash
# MAGIC databricks jobs create --json @bundle/job.json
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — (Opcional) Crear el Job por SDK desde aquí
# MAGIC
# MAGIC Demostrativo: crea el Job multi-tarea programáticamente (mismo resultado que la UI/DAB).

# COMMAND ----------

# Descomenta para crear el Job vía SDK.
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service import jobs, compute
# w = WorkspaceClient()
# base = f"/Workspace/Users/{_user}/Latam_resources_spanish/CP/MLOps/pipeline"
# cluster = jobs.JobCluster(job_cluster_key="ml_cluster",
#     new_cluster=compute.ClusterSpec(spark_version="15.4.x-cpu-ml-scala2.12",
#                                     node_type_id="i3.xlarge", num_workers=1))
# def nb(key, path, deps=None, retries=0):
#     return jobs.Task(task_key=key, job_cluster_key="ml_cluster",
#         notebook_task=jobs.NotebookTask(notebook_path=f"{base}/{path}"),
#         depends_on=[jobs.TaskDependency(task_key=d) for d in (deps or [])],
#         max_retries=retries)
# created = w.jobs.create(
#     name=f"cp_mlops_churn_{_user.split('@')[0]}",
#     job_clusters=[cluster],
#     tasks=[
#         nb("feature_engineering", "01_feature_engineering"),
#         nb("train_and_register", "02_train_and_register", ["feature_engineering"]),
#         nb("validate_and_promote", "03_validate_and_promote", ["train_and_register"], retries=1),
#         nb("deploy_serving", "04_deploy_serving", ["validate_and_promote"]),
#         nb("batch_scoring", "05_batch_scoring", ["validate_and_promote"]),
#     ],
#     schedule=jobs.CronSchedule(quartz_cron_expression="0 0 6 ? * MON",
#         timezone_id="America/Bogota", pause_status=jobs.PauseStatus.PAUSED))
# print("Job creado:", created.job_id)
print("Celda demostrativa — usa bundle/databricks.yml para la versión declarativa (recomendado).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Armaste el pipeline ML como **Job multi-tarea** en la UI (con gate de validación)
# MAGIC ✅ Schedule + reintentos + alertas
# MAGIC ✅ El mismo Job como **código**: Asset Bundle, JSON y SDK
# MAGIC ✅ El Job **despliega el endpoint** y corre **batch scoring** automáticamente
# MAGIC ✅ Patrón **UI → Code** para industrializar (CI/CD)
# MAGIC
# MAGIC ## Continuar → `08 - Cierre y Recap`
