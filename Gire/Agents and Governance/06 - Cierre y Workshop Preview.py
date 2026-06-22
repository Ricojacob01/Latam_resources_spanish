# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Cierre 🎬 · Track Agents and Governance
# MAGIC
# MAGIC **10 min.** Recap + qué sigue.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo que construiste hoy
# MAGIC
# MAGIC | Módulo | Logro | UI vs Code |
# MAGIC |---|---|---|
# MAGIC | 02 | Gobernanza UC asistida por IA (comentarios, tags, masks) | UI → Code |
# MAGIC | 03 | AI Functions en SQL + batch inference | Lado a lado |
# MAGIC | 04 | Genie space + App que lo consume | UI → Code |
# MAGIC | 05 | Knowledge Assistant (Agent Bricks, RAG) | Code → UI |
# MAGIC | 05b | AI Gateway (rate limits, guardrails, routing) + evaluación con jueces LLM + dashboard de monitoreo | UI → Code → UI |
# MAGIC
# MAGIC **Hilo conductor:** datos **gobernados y descritos** → razonamiento confiable de Genie y agentes → **consumo gobernado y monitoreado** con AI Gateway. La gobernanza no es un freno, es lo que hace *posibles* los agentes empresariales.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cómo conecta con los otros tracks
# MAGIC
# MAGIC > ⚠️ Cada track es **independiente** — no necesitas haber corrido Data Engineering ni MLOps para completar este. Cada lab crea sus propios datos de ejemplo.
# MAGIC
# MAGIC - En producción real, las tablas **gold** del track **Data Engineering** alimentarían Genie y AI Functions — los **patrones** son los mismos, solo cambia la fuente.
# MAGIC - El endpoint del **Knowledge Assistant** es un caso de **Model Serving** — que el track **MLOps** (`../../CP/MLOps/`) lleva a producción con orquestación.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Para profundizar
# MAGIC
# MAGIC - Agentes con **tools** (UC Functions, Vector Search, Lakebase) y **Multi-agent Supervisor**.
# MAGIC - **AI Gateway** — la capa de gobernanza sobre el consumo de modelos:
# MAGIC   - *Rate limits* por usuario/app/key (requests y tokens).
# MAGIC   - *Guardrails* de entrada/salida (contenido inseguro, toxicidad).
# MAGIC   - *PII detection* — enmascara datos personales antes de llegar al LLM.
# MAGIC   - *Routing & fallback* — dirige tráfico entre modelos (costo/calidad, A/B) con fallback automático.
# MAGIC   - *Spend controls* — presupuesto máximo por endpoint.
# MAGIC   - *Usage tracking* + Inference Tables — auditoría completa de quién llamó qué.
# MAGIC   - Se configura en **Sidebar → Serving → AI Gateway** (UI) o vía API/SDK.
# MAGIC - **Lakehouse Monitoring** + Inference Tables sobre el agente.
# MAGIC - Row-level security, SCD masking dinámico, *attribute-based access control*.
# MAGIC
# MAGIC ## ¡Gracias! 🎉 — sigue con **MLOps** o revisa **Data Engineering**.
