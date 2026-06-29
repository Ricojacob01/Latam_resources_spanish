# Databricks notebook source
# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png">
# MAGIC
# MAGIC # 🤖 Comfama — Workshop Agentes en Producción
# MAGIC ## Sesión 2 · 00 — Bienvenida y Recap
# MAGIC
# MAGIC En la **Sesión 1** construimos y servimos el **Agente de Servicios al Afiliado**. Hoy lo llevamos a **producción**:
# MAGIC le ponemos una **App**, lo **observamos / gobernamos / monitoreamos**, controlamos su **costo**, y mostramos cómo
# MAGIC **desplegarlo como código** para integrarlo al framework de agentes de Comfama.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Recap Sesión 1 — lo que quedó andando
# MAGIC | Componente | Producto | Equivale a |
# MAGIC |---|---|---|
# MAGIC | Base de conocimiento (RAG) | Vector Search | — |
# MAGIC | Capa operacional (cupos, reservas, memoria) | **Lakebase** | Cosmos DB |
# MAGIC | Agente (RAG + 3 tools) | Mosaic AI Agent Framework | `TemplateAgentes` |
# MAGIC | Endpoint REST del agente | Model Serving | — |
# MAGIC | Gobierno del modelo (límites, PII, tracking) | AI Gateway | `LLMConfig + TokenProvider` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🗺️ Agenda Sesión 2
# MAGIC | # | Módulo | Equivale a |
# MAGIC |---|---|---|
# MAGIC | 01 | Databricks App (frontend de chat, OBO) | Azure Container Apps |
# MAGIC | 02 | Observabilidad (MLflow Tracing) | `TelemetryManager` |
# MAGIC | 03 | Gobernanza (Unity Catalog) | `security/` |
# MAGIC | 04 | Monitoreo + Alertas | `AlertEvaluator` |
# MAGIC | 05 | FinOps | `FinOpsAnalyzer` |
# MAGIC | 06 | **Deploy-as-Code para su framework** | — |
# MAGIC | 07 | Cierre y Recap | — |

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Pre-check Sesión 2 — confirmamos que los assets de S1 siguen vivos

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
print("Verificando assets de la Sesión 1...\n")

# Endpoint del agente
try:
    ep = w.serving_endpoints.get(AGENT_ENDPOINT)
    estado = ep.state.ready.value if ep.state and ep.state.ready else "?"
    print(f"Agente servido ({AGENT_ENDPOINT}): {estado}")
except Exception as e:
    print(f"⚠️ Agente {AGENT_ENDPOINT} no encontrado — corre primero la Sesión 1 ({type(e).__name__})")

# Modelo registrado
try:
    from mlflow import MlflowClient
    mc = MlflowClient(registry_uri="databricks-uc")
    v = max(int(x.version) for x in mc.search_model_versions(f"name='{AGENT_MODEL_NAME}'"))
    print(f"Modelo en UC ({AGENT_MODEL_NAME}): v{v} ✅")
except Exception as e:
    print(f"⚠️ Modelo {AGENT_MODEL_NAME} no encontrado ({type(e).__name__})")

# Tablas semilla
tablas = [r.tableName for r in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()]
print("Tablas base:", "✅" if {"programas","afiliados","kb_documentos"} <= set(tablas) else "⚠️ faltan")

print("\nSi todo está ✅, continúa con el módulo 01.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### ▶️ Siguiente: `01 - Databricks App`

