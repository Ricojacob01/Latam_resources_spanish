# Databricks notebook source
# DBTITLE 1,Header + intro
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC
# MAGIC # 01 — Product Tour 📊 · MLOps end-to-end en Databricks
# MAGIC
# MAGIC ~10 min. Visión general del ciclo completo de MLOps: desde la preparación de datos hasta **Model Serving**, **AI Gateway** y **orquestación automatizada**.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-0-v2.png?raw=true" width="1200">

# COMMAND ----------

# DBTITLE 1,MLOps cycle overview
# MAGIC %md
# MAGIC ## ¿Qué es MLOps?
# MAGIC
# MAGIC MLOps es un conjunto de estándares, herramientas y procesos que optimizan el ciclo de vida de proyectos de Machine Learning — desde la experimentación hasta la producción. Sin MLOps, los modelos se quedan en notebooks y nunca generan valor de negocio.
# MAGIC
# MAGIC **Desafíos comunes:**
# MAGIC - ¿Cómo actualizar datos y re-entrenar modelos automáticamente?
# MAGIC - ¿Cómo asegurar que un nuevo modelo no rompa la producción?
# MAGIC - ¿Cómo servir predicciones en tiempo real con gobernanza?
# MAGIC - ¿Cómo monitorear, auditar y controlar el acceso a modelos y LLMs?
# MAGIC
# MAGIC Databricks resuelve todo esto en una sola plataforma.
# MAGIC
# MAGIC ## El ciclo MLOps en este workshop
# MAGIC
# MAGIC ```
# MAGIC  ┌──────────────┐   ┌──────────────┐   ┌────────────────────┐   ┌────────────────────┐
# MAGIC  │ 02 Feature   │──►│ 03 AutoML +  │──►│ 04 Registro UC      │──►│ 04 Validar &       │
# MAGIC  │    eng (UC)  │   │   train+MLflow│   │  @Challenger        │   │  promover @Champion│
# MAGIC  └──────────────┘   └──────────────┘   └────────────────────┘   └─────────┬──────────┘
# MAGIC                                                                            │
# MAGIC                          ┌─────────────────────────────────────────────────┴────────────┐
# MAGIC                          ▼                                                                ▼
# MAGIC                 ┌──────────────────┐                                          ┌────────────────────┐
# MAGIC                 │ 05 Model Serving  │   tiempo real (REST)                     │ 06 Batch inference │
# MAGIC                 │ + ⭐ AI Gateway   │◄──── apps / LLMs / servicios             │  (pyfunc/ai_query) │
# MAGIC                 └──────────────────┘                                          └────────────────────┘
# MAGIC                          ▲
# MAGIC                          │  todo encadenado y agendado por:
# MAGIC                 ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC                 │ 07 ⭐ Lakeflow Job: feature → train → register → validate → deploy →     │
# MAGIC                 │       serve → batch  ·  schedule + reintentos + alertas  ·  como DAB    │
# MAGIC                 └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# DBTITLE 1,Act 1 - why serving + orchestration
# MAGIC %md
# MAGIC ## Acto 1 — El desafío de llevar ML a producción
# MAGIC
# MAGIC Tener un modelo funcionando en un notebook **no es suficiente**. Para generar valor de negocio necesitamos cubrir:
# MAGIC
# MAGIC | Desafío | Solución Databricks |
# MAGIC |---|---|
# MAGIC | Preparar y gobernar features | **Unity Catalog** + tablas Delta gobernadas |
# MAGIC | Entrenar y trackear experimentos | **MLflow** + AutoML |
# MAGIC | Gestionar versiones de modelos | **Models in Unity Catalog** (aliases Champion/Challenger) |
# MAGIC | Servir predicciones en tiempo real | **Model Serving** (endpoints REST, scale-to-zero) |
# MAGIC | Gobernar acceso a modelos y LLMs | ⭐ **AI Gateway** (rate limits, guardrails, logging) |
# MAGIC | Automatizar el pipeline completo | **Lakeflow Jobs** (schedule, reintentos, alertas) |
# MAGIC | Auditar quién hizo qué | **System Tables** (audit logs, lineage) |
# MAGIC
# MAGIC Este workshop recorre **todo el ciclo**, desde feature engineering hasta Model Serving con AI Gateway y orquestación automatizada.

# COMMAND ----------

# DBTITLE 1,Act 2 - Model Serving + AI Gateway
# MAGIC %md
# MAGIC ## Acto 2 — Model Serving + AI Gateway 🌐
# MAGIC
# MAGIC ### Model Serving
# MAGIC Databricks **Model Serving** toma un modelo registrado en Unity Catalog y lo expone como **endpoint REST** gestionado:
# MAGIC
# MAGIC - **Scale-to-zero** — paga solo cuando hay tráfico, autoscaling automático.
# MAGIC - **Traffic split** — A/B testing y canary deployments entre versiones.
# MAGIC - **Inference Tables** — loggea cada request/response a Delta para monitoreo y debugging.
# MAGIC - Se crea en la **Serving UI** o por **API** (`mlflow.deployments` / `WorkspaceClient`).
# MAGIC
# MAGIC ### ⭐ AI Gateway
# MAGIC El **AI Gateway** agrega una capa de **gobernanza y control** sobre los endpoints de serving (tanto modelos propios como LLMs externos):
# MAGIC
# MAGIC - **Rate limiting** — controla cuántas requests por usuario/minuto (protege costos y recursos).
# MAGIC - **Guardrails** — filtros de seguridad en input/output (PII detection, contenido inapropiado).
# MAGIC - **Fallback** — si un proveedor falla, redirige automáticamente a otro.
# MAGIC - **Usage tracking** — métricas centralizadas de uso por endpoint, usuario y equipo.
# MAGIC - **Interfaz unificada** — misma API para modelos propios, Llama, Claude, GPT (vía Databricks).
# MAGIC
# MAGIC ```
# MAGIC                         ┌─────────────────┐
# MAGIC   App / Usuario ───►    │  AI Gateway     │    ───►  Endpoint de Serving (tu modelo churn)
# MAGIC                         │  (rate limits,  │    ───►  Foundation Model (Llama, DBRX)
# MAGIC                         │   guardrails,   │    ───►  External Model (Claude, GPT)
# MAGIC                         │   logging)      │
# MAGIC                         └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC > En el módulo 05, primero creamos el endpoint de Model Serving para nuestro modelo de churn, y luego lo gobernamos con AI Gateway (rate limits, usage tracking, guardrails).

# COMMAND ----------

# DBTITLE 1,Act 3 - Orchestration
# MAGIC %md
# MAGIC ## Acto 3 — Orquestación con Lakeflow Jobs 🗓️
# MAGIC
# MAGIC Un modelo que no se re-entrena con datos nuevos se **degrada**. Necesitamos automatizar todo el ciclo como un **Job multi-tarea** con dependencias:
# MAGIC
# MAGIC ```
# MAGIC 01_feature_engineering → 02_train_and_register → 03_validate_and_promote
# MAGIC                                                        ├─► 04_deploy_serving
# MAGIC                                                        └─► 05_batch_scoring
# MAGIC ```
# MAGIC
# MAGIC **Lakeflow Jobs** orquesta esto con:
# MAGIC - **Schedule** (p.ej. semanal) — reentrena automáticamente.
# MAGIC - **Gate de validación** — si el Challenger no supera al Champion, el deploy **no ocurre**.
# MAGIC - **Reintentos + alertas** — robustez ante fallos transitorios.
# MAGIC - **Definido como código** (Declarative Automation Bundle) — versionado en Git para CI/CD.
# MAGIC
# MAGIC Lo construyes en la **Jobs UI** (intuición) y lo industrializas como código en el módulo 07.

# COMMAND ----------

# DBTITLE 1,Summary and next steps
# MAGIC %md
# MAGIC ## Resumen del recorrido
# MAGIC
# MAGIC | Módulo | Qué harás |
# MAGIC |---|---|
# MAGIC | 02 | Limpiar datos y crear features gobernadas en Unity Catalog |
# MAGIC | 03 | Entrenar un modelo con MLflow tracking y AutoML |
# MAGIC | 04 | Registrar en UC, validar y promover con el patrón Champion/Challenger |
# MAGIC | 05 | Desplegar como endpoint REST + configurar AI Gateway (rate limits, guardrails) |
# MAGIC | 06 | Scoring masivo en batch |
# MAGIC | 07 | Automatizar todo como un Job con schedule |
# MAGIC
# MAGIC ## ¿Listo? → `02 - Feature Engineering y Gobernanza`
