# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour 📊 · MLOps en Databricks
# MAGIC
# MAGIC ~20 min. Arco narrativo:
# MAGIC
# MAGIC > **El problema** (MLOps tradicional) → **MLflow** (tracking) → **AutoML** → **Modelos en Unity Catalog** (Champion/Challenger) → **Validación + Batch** → **(luego) Serving + Jobs**

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 1 — El problema del MLOps
# MAGIC
# MAGIC - Features que no se comparten ni versionan; "funciona en mi notebook".
# MAGIC - Experimentos sin trazabilidad: ¿qué datos, qué params, qué métrica?
# MAGIC - Modelos sin gobernanza: ¿cuál está en prod?, ¿quién lo aprobó?
# MAGIC - Promoción manual y frágil; sin validación automática.
# MAGIC
# MAGIC > Databricks unifica esto: **MLflow** para tracking/registry, **Unity Catalog** para gobernar modelos como cualquier dato, y AutoML/serving/jobs nativos.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Qué es MLOps](../_assets/slides/mlops/deckB_what_is_mlops.png)
# MAGIC
# MAGIC *MLOps = DataOps + DevOps + ModelOps: procesos y automatización para gestionar datos, código y modelos.*
# MAGIC
# MAGIC ![Mosaic AI](../_assets/slides/mlops/deckA_mosaic_ai_overview.png)
# MAGIC
# MAGIC *Mosaic AI: soporte integral para todo el ciclo de ML y GenAI (MLflow, AutoML, Model Serving, y más).*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 2 — MLflow Tracking 📒
# MAGIC
# MAGIC Cada entrenamiento queda registrado: params, métricas, artefactos, **signature**, y el **dataset** usado (lineage). `mlflow.autolog()` captura casi todo solo.
# MAGIC
# MAGIC ```python
# MAGIC with mlflow.start_run(run_name="lgbm"):
# MAGIC     mlflow.sklearn.autolog()
# MAGIC     model.fit(X_train, y_train)
# MAGIC     mlflow.log_input(dataset, context="training-input")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ![MLflow](../_assets/slides/mlops/deckA_mlflow.png)
# MAGIC
# MAGIC *MLflow: plataforma open-source para el ciclo de vida de ML, con seguimiento y reproducibilidad automáticos.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 3 — AutoML 🤖
# MAGIC
# MAGIC Genera modelos de referencia (baseline) y un **notebook glass-box** del mejor modelo — no es una caja negra. Lo lanzas en la **UI** o por **API**:
# MAGIC
# MAGIC ```python
# MAGIC from databricks import automl
# MAGIC run = automl.classify(dataset=df, target_col="churn", timeout_minutes=10)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ![AutoML](../_assets/slides/mlops/deckA_automl.png)
# MAGIC
# MAGIC *AutoML: del dato a un modelo listo para producción — regresión, forecasting y clasificación, integrado con Unity Catalog.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 4 — Modelos en Unity Catalog 🏛️
# MAGIC
# MAGIC El modelo se registra como `catalog.schema.modelo` y se gobierna con permisos + lineage. Los **alias** marcan el rol:
# MAGIC
# MAGIC ```python
# MAGIC mlflow.set_registry_uri("databricks-uc")
# MAGIC d = mlflow.register_model(f"runs:/{run_id}/model", f"{catalog}.{schema}.mlops_churn")
# MAGIC client.set_registered_model_alias(name, alias="Challenger", version=d.version)
# MAGIC ```
# MAGIC
# MAGIC - **@Challenger** — candidato a validar.
# MAGIC - **@Champion** — el que sirve producción.

# COMMAND ----------
# MAGIC %md
# MAGIC ![Modelos en Unity Catalog](../_assets/slides/mlops/deckA_models_unity_catalog.png)
# MAGIC
# MAGIC *El Model Registry vive en Unity Catalog: control de acceso centralizado, auditoría y linaje del modelo, y modelos compartibles entre workspaces.*
# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 5 — Validación + Batch inference ✅
# MAGIC
# MAGIC Antes de promover Challenger → Champion, validamos: ¿tiene descripción?, ¿su F1 ≥ Champion?, ¿mejora el impacto de negocio? Si pasa, se promueve.
# MAGIC
# MAGIC ```python
# MAGIC udf = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{schema}.mlops_churn@Champion")
# MAGIC scored = df.withColumn("pred", udf(*udf.metadata.get_input_schema().input_names()))
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ![Lakehouse Monitoring](../_assets/slides/mlops/deckB_lakehouse_monitoring.png)
# MAGIC
# MAGIC *Lakehouse Monitoring: calidad de datos y modelos, métricas de drift y dashboards automáticos para validar calidad.*
# MAGIC
# MAGIC ![Batch Inference con AI Functions](../_assets/slides/mlops/deckA_batch_inference_ai_functions.png)
# MAGIC
# MAGIC *Batch inference con `ai_query()` / AI Functions: aplica modelos a gran escala desde SQL, Notebooks y Jobs.*

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 6 — Lo que sigue: Serving + Jobs (en CP/MLOps) 🚀
# MAGIC
# MAGIC Este track deja el **Champion listo en UC**. El workshop ampliado `../../CP/MLOps/` lo lleva a producción:
# MAGIC
# MAGIC - **Model Serving**: un endpoint REST en tiempo real (UI + API).
# MAGIC - **Job de orquestación**: train → register → validate → deploy/serve → batch, en schedule, con reintentos.

# COMMAND ----------

# MAGIC %md
# MAGIC ![Model Serving](../_assets/slides/mlops/deckB_model_serving.png)
# MAGIC
# MAGIC *Model Serving: despliegue serverless en tiempo real — el modelo como API lista para producción.*
# MAGIC
# MAGIC ![Producción (dev/staging/prod)](../_assets/slides/mlops/deckB_production_dev_staging_prod.png)
# MAGIC
# MAGIC *Vista multi-entorno dev → staging → producción: cómo el modelo Champion llega a producción de forma gobernada.*

# COMMAND ----------

# DBTITLE 1,Acto 7 — AI Gateway
# MAGIC %md
# MAGIC # 🎬 Acto 7 — AI Gateway: gobernar el modelo servido 🚦
# MAGIC
# MAGIC Una vez el modelo está en un endpoint (serving), necesitas **gobernar quién lo consume y cómo**. **AI Gateway** es esa capa:
# MAGIC
# MAGIC - **Rate limits** — cuotas por usuario/app/key para evitar abuso o sobrecosto.
# MAGIC - **Guardrails** — filtrado de seguridad en entrada/salida (contenido inseguro, toxicidad).
# MAGIC - **PII detection** — detecta y enmascara datos personales antes de llegar al modelo.
# MAGIC - **Routing & fallback** — envía tráfico a distintos modelos (A/B testing, costo vs calidad) con fallback si uno falla.
# MAGIC - **Spend controls** — presupuesto máximo por endpoint/periodo.
# MAGIC - **Usage tracking** — Inference Tables registran cada request (quién, cuándo, tokens, latencia).
# MAGIC
# MAGIC > En el ciclo MLOps, AI Gateway cierra el loop: entrenas → registras → validas → sirves → **gobiernas el consumo**. Sin él, el modelo está en producción pero sin control de acceso ni visibilidad de costos.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧩 Recap
# MAGIC
# MAGIC ```
# MAGIC features → AutoML/train (MLflow tracking) → register en UC (@Challenger)
# MAGIC          → validar → promover (@Champion) → batch inference
# MAGIC          → [CP/MLOps] Model Serving endpoint + Job de orquestación
# MAGIC          → AI Gateway (rate limits · guardrails · PII · routing · spend controls)
# MAGIC ```
# MAGIC
# MAGIC ## ¿Listo? → `02 - LAB - Del notebook a Unity Catalog`
