# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bienvenida 👋 · Track Agents and Governance 🤖
# MAGIC
# MAGIC **Duración:** ~2.5 horas · **Tipo:** Hands-on
# MAGIC
# MAGIC Datos **gobernados** → consultados en **lenguaje natural** → convertidos en **agentes** confiables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Qué van a salir sabiendo?
# MAGIC
# MAGIC 1. **Gobernanza con Unity Catalog**: comentarios, tags y *column masking* — manuales en la UI y **automatizados con IA** (`ai_gen`, `ai_query`).
# MAGIC 2. **AI Functions** en SQL: `ai_query`, `ai_classify`, `ai_extract`, `ai_analyze_sentiment`, `ai_summarize`… y batch inference a escala.
# MAGIC 3. **Genie**: preguntar a tus datos en español; y una **App** que lo consume.
# MAGIC 4. **Agent Bricks**: un Knowledge Assistant (RAG) sobre un PDF, sin construir el retriever a mano.
# MAGIC 5. **AI Gateway**: gobernar el acceso a modelos (rate limits, guardrails, routing, monitoreo) y evaluar agentes con jueces LLM.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este track
# MAGIC
# MAGIC Vives **las dos caras** de Databricks. Cada módulo abre con su nota; el resumen:
# MAGIC
# MAGIC | Módulo | Patrón | Por qué |
# MAGIC |---|---|---|
# MAGIC | 02 Gobernanza | **UI → Code** | Entiendes el control con clicks (tag/mask), luego lo automatizas con IA sobre todo el esquema. |
# MAGIC | 03 AI Functions | **Lado a lado** | Playground UI y SQL son la *misma* capacidad; las usas en paralelo. |
# MAGIC | 04 Genie y Apps | **UI → Code** | Genie se crea en UI; una App (código) lo consume vía SDK. |
# MAGIC | 05 Agent Bricks | **Code → UI** | El código prepara datos; el agente RAG se ensambla sin código en la UI. |
# MAGIC | 05b AI Gateway | **UI → Code → UI** | Configuras rate limits/guardrails en UI, consumes por código, monitoreas en dashboard. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC | Tiempo | Actividad | Notebook |
# MAGIC |---|---|---|
# MAGIC | 0–5 | **Bienvenida** | `00` (este) |
# MAGIC | 5–25 | **Product Tour** | `01` |
# MAGIC | 25–55 | **LAB Gobernanza UC** | `02` |
# MAGIC | 55–80 | **LAB AI Functions** | `03` |
# MAGIC | 80–115 | **LAB Genie y Apps** | `04` |
# MAGIC | 115–135 | **LAB Agent Bricks** (intro) | `05` |
# MAGIC | 135–175 | **LAB AI Gateway y Evaluación** | `05b` |
# MAGIC | 175–185 | **Cierre** | `06` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check rápido

# COMMAND ----------

from databricks.sdk import WorkspaceClient

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"Usuario: {_user}\nCatalog: {CATALOG}\nSchema:  {SCHEMA}")

w = WorkspaceClient()
KEY = ["databricks-meta-llama-3-3-70b-instruct", "databricks-gte-large-en"]
avail = {e.name for e in w.serving_endpoints.list()}
print("\n¿Foundation Models clave disponibles?")
for m in KEY:
    print(f"  {'✅' if m in avail else '❌'}  {m}")

print("\n✅ Continúa con `01 - Product Tour (UC + Genie + Agent Bricks)`")
