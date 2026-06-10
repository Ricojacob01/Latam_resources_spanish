# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — Architecture & Monitoring Summary
# MAGIC
# MAGIC **Notebook final del demo**. Acá conectamos cada asset desplegado con:
# MAGIC - 🛡️ **Governance** — quién es dueño, quién puede leer/escribir, lineage
# MAGIC - 📊 **Observability** — dónde ver trazas, métricas, logs
# MAGIC - 💰 **FinOps** — qué tabla de sistema reporta su costo
# MAGIC - 🚨 **Alerting** — qué SQL Alert vigila este asset
# MAGIC - 🔗 **Links directos** a la UI del workspace
# MAGIC
# MAGIC Pasa fila por fila: este es el documento de hand-off al equipo de plataforma de Comfama.

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk>=0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

w = WorkspaceClient()
HOST = w.config.host
print(f"Workspace: {HOST}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Vista global de la arquitectura desplegada
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────────┐
# MAGIC │                      Usuario (browser SSO Entra ID)                      │
# MAGIC │                                  │                                       │
# MAGIC │                                  ▼                                       │
# MAGIC │            ┌──────────────────────────────────────────┐                  │
# MAGIC │            │     Databricks App: comfama-agente-app   │                  │
# MAGIC │            │     (Streamlit, on-behalf-of user auth)  │                  │
# MAGIC │            └──────────────────┬───────────────────────┘                  │
# MAGIC │                               │ HTTPS                                    │
# MAGIC │                               ▼                                          │
# MAGIC │            ┌──────────────────────────────────────────┐                  │
# MAGIC │            │  Model Serving: agente_comfama           │                  │
# MAGIC │            │  ← Inference Table (auto-capture)        │                  │
# MAGIC │            │  PyFunc Model (registered in UC)         │                  │
# MAGIC │            └────────┬───────────────┬────────────────┬┘                  │
# MAGIC │                     │               │                │                   │
# MAGIC │                     ▼               ▼                ▼                   │
# MAGIC │         ┌──────────────────┐  ┌──────────┐  ┌─────────────────┐          │
# MAGIC │         │ Vector Search    │  │ FM API:  │  │  MLflow Tracing │          │
# MAGIC │         │ (documentos_idx) │  │ Llama70B │  │ (auto traces)   │          │
# MAGIC │         └────────┬─────────┘  └──────────┘  └─────────────────┘          │
# MAGIC │                  │                                                       │
# MAGIC │                  ▼                                                       │
# MAGIC │         ┌──────────────────┐                                             │
# MAGIC │         │ Delta: documentos │                                            │
# MAGIC │         │ _subsidios (CDF)  │                                            │
# MAGIC │         └──────────────────┘                                             │
# MAGIC │                                                                          │
# MAGIC │ ─────────────── Plano de control / observability ───────────────         │
# MAGIC │                                                                          │
# MAGIC │  Unity Catalog ───┐                                                      │
# MAGIC │                   │                                                      │
# MAGIC │                   ├─► system.access.audit  (cada query / acceso)         │
# MAGIC │                   ├─► system.access.table_lineage (data lineage)         │
# MAGIC │                   └─► system.billing.usage (costo DBU)                   │
# MAGIC │                                                                          │
# MAGIC │  Lakehouse Monitor (sobre metricas_agente_gold)                          │
# MAGIC │       └─► profile_metrics + drift_metrics tables                         │
# MAGIC │       └─► Dashboard auto-generado                                        │
# MAGIC │                                                                          │
# MAGIC │  Lakeview Dashboard: "Comfama Agente — Observability & FinOps"           │
# MAGIC │       └─► consume las tablas Gold + Inference + System                   │
# MAGIC │                                                                          │
# MAGIC │  SQL Alert: comfama_latencia_p95_alert                                   │
# MAGIC │       └─► dispara si latencia P95 > 1800ms en última hora                │
# MAGIC └──────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recorrido por componente
# MAGIC
# MAGIC Para cada asset, mostramos: **Governance | Observability | FinOps | Alerting | Link directo**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🗄️ 1. Tabla Delta `documentos_subsidios`
# MAGIC
# MAGIC **Función**: Base de conocimiento del agente (8 documentos sobre servicios de Comfama).
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | UC permissions (`GRANT SELECT ON ...`); Tags `categoria`; lineage automático en `system.access.table_lineage` |
# MAGIC | **Observability** | `system.access.audit` filtrado por `full_name_arg LIKE '%documentos_subsidios%'` |
# MAGIC | **FinOps** | Storage cost en `system.billing.usage` (sku `STORAGE_*`) |
# MAGIC | **Alerting** | (opcional) alerta si `MAX(fecha_actualizacion)` > 30 días |

# COMMAND ----------

display(spark.sql(f"""
SELECT
  source_table_full_name AS upstream,
  target_table_full_name AS downstream,
  source_type,
  MAX(event_time) AS last_access
FROM system.access.table_lineage
WHERE source_table_full_name = '{FULL_SCHEMA}.documentos_subsidios'
   OR target_table_full_name = '{FULL_SCHEMA}.documentos_subsidios'
GROUP BY upstream, downstream, source_type
ORDER BY last_access DESC
LIMIT 10
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🔍 2. Vector Search index `documentos_index`
# MAGIC
# MAGIC **Función**: Índice vectorial delta-sync sobre la tabla de documentos. Powered by `databricks-gte-large-en` embedding.
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | UC permissions sobre el índice mismo + sobre la tabla source |
# MAGIC | **Observability** | UI: Catalog → comfama → documentos_index → tab "Status" (sync history, latency) |
# MAGIC | **FinOps** | `system.billing.usage` filtrado por sku VECTOR_SEARCH_* |
# MAGIC | **Alerting** | (opcional) alerta si último sync > N horas |

# COMMAND ----------

# Verificar estado del índice y costos
print("📍 Link al índice en UI:")
print(f"   {HOST}/explore/data/{CATALOG}/{SCHEMA}/documentos_index")
print()

try:
    display(spark.sql("""
    SELECT
      sku_name,
      ROUND(SUM(usage_quantity), 2) AS dbus_7d
    FROM system.billing.usage
    WHERE sku_name LIKE '%VECTOR_SEARCH%'
      AND usage_start_time >= current_date() - INTERVAL 7 DAYS
    GROUP BY sku_name
    """))
except Exception as e:
    print(f"VS billing: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 3. Registered model `agente_comfama` (Unity Catalog)
# MAGIC
# MAGIC **Función**: PyFunc model con retrieval + generation; alias `production`.
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | UC permissions (CAN_QUERY, CAN_MANAGE); aliases versionados |
# MAGIC | **Observability** | MLflow experiment `comfama_agente_real` + cada deploy crea nuevo Run |
# MAGIC | **FinOps** | Costos del experiment incluidos en cluster cost |
# MAGIC | **Alerting** | (opcional) alerta cuando nueva versión sin tests pasa a production |

# COMMAND ----------

print("📍 Link al modelo:")
print(f"   {HOST}/explore/data/models/{CATALOG}/{SCHEMA}/agente_comfama")
print()

# Mostrar versiones del modelo
try:
    from mlflow import MlflowClient
    client = MlflowClient(registry_uri="databricks-uc")
    versions = client.search_model_versions(f"name='{CATALOG}.{SCHEMA}.agente_comfama'")
    print(f"Versiones del modelo: {len(versions)}")
    for v in versions[:5]:
        aliases = client.get_model_version(name=f"{CATALOG}.{SCHEMA}.agente_comfama", version=v.version)
        print(f"  v{v.version}  status={v.status}  aliases={aliases.aliases if hasattr(aliases, 'aliases') else '?'}")
except Exception as e:
    print(f"Modelo: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚀 4. Model Serving endpoint `agente_comfama`
# MAGIC
# MAGIC **Función**: Endpoint REST que sirve el agente.
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | Permisos: CAN_QUERY al app + a usuarios; service principal del app puede llamarlo |
# MAGIC | **Observability** | **Inference Table** (`agente_inference_payload`) — cada request con request/response/latency; UI "Logs" del endpoint |
# MAGIC | **FinOps** | `system.serving.endpoint_usage` por endpoint, por hora |
# MAGIC | **Alerting** | `comfama_latencia_p95_alert` (notebook 05) sobre la inference table |

# COMMAND ----------

print("📍 Link al endpoint:")
print(f"   {HOST}/ml/endpoints/agente_comfama")
print()

# Inference table data (si ya hay datos)
inference_table = f"{FULL_SCHEMA}.agente_inference_payload"
try:
    n = spark.sql(f"SELECT COUNT(*) as n FROM {inference_table}").collect()[0]["n"]
    print(f"📊 Inference table — {n} requests capturados")
    if n > 0:
        display(spark.sql(f"""
        SELECT
          date_format(timestamp_ms / 1000, 'yyyy-MM-dd HH:mm') AS timestamp,
          status_code,
          execution_duration_ms,
          substring(request, 1, 100) AS request_preview
        FROM {inference_table}
        ORDER BY timestamp_ms DESC
        LIMIT 5
        """))
except Exception as e:
    print(f"Inference table aún no materializada: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📱 5. Databricks App `comfama-agente-app`
# MAGIC
# MAGIC **Función**: Frontend Streamlit que consume el endpoint del agente.
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | Service principal del app con permiso CAN_QUERY al endpoint; on-behalf-of token para passing user identity |
# MAGIC | **Observability** | App logs (UI → Logs tab); deployments history |
# MAGIC | **FinOps** | `system.billing.usage` sku `APPS_*` |
# MAGIC | **Alerting** | (opcional) alerta si error rate > X% en N min |

# COMMAND ----------

try:
    app = w.apps.get(name="comfama-agente-app")
    print(f"📱 App: {app.name}")
    print(f"   URL: {app.url}")
    print(f"   Compute: {app.compute_status.state if app.compute_status else '?'}")
    print(f"   Service Principal: {app.service_principal_id}")
except Exception as e:
    print(f"App: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📈 6. Lakeview Dashboard
# MAGIC
# MAGIC **Función**: Vista consolidada de latencia, volumen, costo, intents, feedback.
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | Permisos por usuario/grupo; share read-only |
# MAGIC | **Observability** | El dashboard mismo ES el observability sheet |
# MAGIC | **FinOps** | Una sección del dashboard cubre System Tables |
# MAGIC | **Alerting** | (opcional) alertas sobre queries del dashboard |

# COMMAND ----------

# Buscar el dashboard
try:
    dashboards = w.lakeview.list(view="ALL")
    for d in dashboards:
        if "Comfama" in (d.display_name or ""):
            print(f"📊 Dashboard: {d.display_name}")
            print(f"   ID:  {d.dashboard_id}")
            print(f"   URL: {HOST}/dashboardsv3/{d.dashboard_id}")
            break
except Exception as e:
    print(f"Dashboards: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🛡️ 7. Lakehouse Monitor sobre `metricas_agente_gold`
# MAGIC
# MAGIC **Función**: Profile + drift sobre las métricas agregadas del agente.
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | El monitor produce 2 tablas gobernadas en UC: `_profile_metrics`, `_drift_metrics` |
# MAGIC | **Observability** | Dashboard auto-generado; tablas son queryables |
# MAGIC | **FinOps** | Costo de refresh aparece en `system.billing.usage` |
# MAGIC | **Alerting** | SQL Alert puede correr sobre las tablas de drift |

# COMMAND ----------

try:
    monitor = w.quality_monitors.get(table_name=f"{FULL_SCHEMA}.metricas_agente_gold")
    print(f"🛡️ Monitor sobre {FULL_SCHEMA}.metricas_agente_gold")
    print(f"   Status:    {monitor.status}")
    print(f"   Dashboard: {HOST}/dashboardsv3/{monitor.dashboard_id}")
    print(f"   Profile:   {monitor.profile_metrics_table_name}")
    print(f"   Drift:     {monitor.drift_metrics_table_name}")
except Exception as e:
    print(f"Monitor: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🚨 8. SQL Alert
# MAGIC
# MAGIC **Función**: Vigilancia automática sobre KPIs del agente.
# MAGIC
# MAGIC | Dimensión | Cómo se gobierna / monitorea |
# MAGIC |---|---|
# MAGIC | **Governance** | Owner del alert; permission grants para edit/run |
# MAGIC | **Observability** | Historia de evaluaciones en UI; cada trigger queda en audit |
# MAGIC | **FinOps** | Cada eval consume warehouse — registrado en `system.query.history` |
# MAGIC | **Alerting** | _meta_: ¿quién monitorea que el alert no esté broken? |

# COMMAND ----------

print("🚨 Query base del alert:")
print(f"   {HOST}/sql/editor")
print()
print("Para activar el alert (UI):")
print("   1. SQL → Alerts → New alert")
print("   2. Query: comfama_latencia_p95_alert")
print("   3. Threshold: p95_latency_ms > 1800")
print("   4. Schedule: every 5 min")
print("   5. Destinos: email + Slack webhook")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapa de monitoreo end-to-end
# MAGIC
# MAGIC La siguiente tabla resume **dónde encontrar info sobre cada cosa** que un SRE/Platform engineer normalmente busca:
# MAGIC
# MAGIC | Pregunta operativa | Dónde mirar |
# MAGIC |---|---|
# MAGIC | "¿El agente respondió bien las últimas N preguntas?" | `agente_inference_payload` (request/response/latency) |
# MAGIC | "¿Cómo cambió la latencia en el tiempo?" | Lakeview dashboard "Comfama Agente" |
# MAGIC | "¿Cuánto me costó el agente este mes?" | `system.billing.usage` filtrado por tag `proyecto=comfama_agente` |
# MAGIC | "¿Quién consultó el dataset de documentos hoy?" | `system.access.audit` con full_name LIKE '%documentos_subsidios%' |
# MAGIC | "¿De dónde viene el dato en metricas_agente_gold?" | `system.access.table_lineage` |
# MAGIC | "¿La calidad de las predicciones está derivando?" | Lakehouse Monitor drift metrics |
# MAGIC | "¿Cuáles son los intents top?" | Lakeview dashboard widget "Top intents" |
# MAGIC | "¿Alguien atacó el endpoint con prompts maliciosos?" | Inference Table + AI Gateway logs (cuando se active) |
# MAGIC | "¿Cuándo se actualizó el modelo por última vez?" | UC Model registry (versions + aliases) |
# MAGIC | "¿El App está respondiendo?" | Apps UI → status; Lakeview con queries sobre app logs |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cheat sheet para hand-off
# MAGIC
# MAGIC ### Onboarding de un nuevo miembro del equipo Comfama
# MAGIC
# MAGIC ```
# MAGIC Día 1:
# MAGIC 1. Acceder al workspace https://fevm-ardemo-classic-dnubtw.cloud.databricks.com
# MAGIC 2. Sidebar → Catalog → ardemo_classic_dnubtw_catalog → comfama
# MAGIC    → Ver las 5 tablas + sus tags + lineage
# MAGIC 3. Sidebar → Experiments → comfama_agente_real
# MAGIC    → Ver las trazas de los últimos runs del agente
# MAGIC 4. Sidebar → Serving → agente_comfama
# MAGIC    → Ver QPS, latencia, errores
# MAGIC 5. Sidebar → Dashboards → Comfama Agente
# MAGIC    → Vista ejecutiva de métricas
# MAGIC
# MAGIC Día 2 (para SREs):
# MAGIC 6. SQL Editor → query system.access.audit + system.billing.usage
# MAGIC 7. Catalog → metricas_agente_gold → tab Quality → ver monitor + drift
# MAGIC 8. SQL → Alerts → ver los alerts configurados
# MAGIC 9. Apps → comfama-agente-app → tab Logs
# MAGIC ```
# MAGIC
# MAGIC ### Para el equipo de seguridad / DPO
# MAGIC
# MAGIC ```
# MAGIC 1. system.access.audit - quién accedió a qué tabla cuándo
# MAGIC 2. system.access.column_lineage - qué columnas se propagan a qué tablas
# MAGIC 3. UC tags - qué columnas están etiquetadas como sensitive
# MAGIC 4. Inference table - qué prompts entran al modelo (PII detection)
# MAGIC ```
# MAGIC
# MAGIC ### Para FinOps
# MAGIC
# MAGIC ```
# MAGIC 1. system.billing.usage - DBU por workspace/sku/usuario/tag
# MAGIC 2. system.serving.endpoint_usage - costo del agente específicamente
# MAGIC 3. system.query.history - costo de queries SQL
# MAGIC 4. ai_forecast() - predicción de gasto
# MAGIC 5. Budget Policies API - límites automáticos
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🎯 Conclusión: el equivalente Databricks del framework Comfama
# MAGIC
# MAGIC | Construido a mano por Comfama | Lo que ahora tienen managed en Databricks |
# MAGIC |---|---|
# MAGIC | `TemplateAgentes` (FastAPI + LangGraph) | `agente_comfama` Model Serving endpoint |
# MAGIC | `TemplateMCP` (FastMCP) | Managed MCP Servers (referenciado, no en este demo) |
# MAGIC | Container Apps frontend + backend | `comfama-agente-app` Databricks App |
# MAGIC | `LLMConfig` + `TokenProvider` | Foundation Model APIs vía `databricks-meta-llama-3-3-70b-instruct` |
# MAGIC | Azure Search vector store | `documentos_index` Vector Search delta-sync |
# MAGIC | Cosmos DB (estado conversacional) | Lakebase (referenciado, no en este demo) |
# MAGIC | `TelemetryManager` custom | MLflow tracing automático + Inference Tables |
# MAGIC | `AuthManager` + audit custom | Unity Catalog + `system.access.audit` |
# MAGIC | `AlertEvaluator` + `AlertProtocols` | SQL Alerts (`comfama_latencia_p95_alert`) |
# MAGIC | `FinOpsAnalyzer` (estimado DBU) | `system.billing.usage` (oficial) |
# MAGIC | Dashboard custom Grafana | Lakeview "Comfama Agente — Observability & FinOps" |
# MAGIC | Monitor de drift custom | Lakehouse Monitoring sobre `metricas_agente_gold` |
# MAGIC
# MAGIC **Resultado neto**: el repo de Comfama-AI puede reducirse en ~50% (toda la capa de plataforma) sin perder ninguna capacidad operacional — y ganando lineage automático, audit nativo, governance unificado, y forecast de costos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Listo
# MAGIC
# MAGIC Tienes un demo end-to-end funcionando que puede mostrarse a Comfama en cualquier conversación técnica. Cada notebook es independientemente runnable y cada asset tiene su mapa de governance + observability + finops.

