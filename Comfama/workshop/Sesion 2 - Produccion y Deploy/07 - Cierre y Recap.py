# Databricks notebook source
# MAGIC %md
# MAGIC # 🏁 Sesión 2 · 07 — Cierre y Recap
# MAGIC
# MAGIC ¡Lo lograron! En dos sesiones construyeron, sirvieron, gobernaron y automatizaron un **agente de IA en
# MAGIC producción** sobre Databricks — reemplazando, pieza por pieza, el framework custom de Comfama.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 Arquitectura final
# MAGIC
# MAGIC ```
# MAGIC                  ┌──────────────────────────────────────────────┐
# MAGIC                  │            Databricks App (chat UI)            │  ✅ S2·01  (Container Apps)
# MAGIC                  │                  OBO auth                      │
# MAGIC                  └───────────────┬───────────────┬──────────────┘
# MAGIC                                  │               │
# MAGIC                  ┌───────────────▼──────┐   ┌────▼─────────────────────┐
# MAGIC                  │   Agente servido      │   │      Lakebase (OLTP)     │  ✅ S1·03  (Cosmos DB)
# MAGIC                  │ (Model Serving)       │   │  afiliados · programas   │
# MAGIC                  │   + AI Gateway        │   │  reservas · cupos        │  ✅ S1·06  (LLMConfig/TokenProvider)
# MAGIC                  └───────┬───────┬──────┘   └────┬─────────────────────┘
# MAGIC                          │       │               │ sync ⇄
# MAGIC               ┌──────────▼─┐  ┌──▼───────────┐   │
# MAGIC               │  FM (LLM)  │  │ Vector Search│   │
# MAGIC               └────────────┘  └──────┬───────┘   │
# MAGIC                  ┌───────────────────▼───────────▼──────────────┐
# MAGIC                  │          Unity Catalog (Delta · gobierno)     │  ✅ S2·03  (security/)
# MAGIC                  └───────────────────────────────────────────────┘
# MAGIC   Observabilidad ✅ S2·02 (TelemetryManager) · Monitoreo+Alertas ✅ S2·04 (AlertEvaluator) · FinOps ✅ S2·05 (FinOpsAnalyzer)
# MAGIC   Deploy-as-Code ✅ S2·06 → framework de agentes de Comfama
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 Scorecard completo: framework Comfama → Databricks
# MAGIC
# MAGIC | Pieza Comfama (Azure, custom) | Databricks (managed) | Módulo |
# MAGIC |---|---|---|
# MAGIC | `TemplateAgentes` | Mosaic AI Agent Framework | S1·04 |
# MAGIC | `LLMConfig + TokenProvider` | AI Gateway + FM APIs | S1·06 |
# MAGIC | **Cosmos DB** | **Lakebase** | S1·03 |
# MAGIC | Retrieval/embeddings | Vector Search | S1·02 |
# MAGIC | Azure Container Apps | Databricks Apps | S2·01 |
# MAGIC | `TelemetryManager` | MLflow Tracing | S2·02 |
# MAGIC | `security/` | Unity Catalog (ABAC, lineage, audit) | S2·03 |
# MAGIC | Monitoreo + `AlertEvaluator` | Lakehouse Monitoring + SQL Alerts | S2·04 |
# MAGIC | `FinOpsAnalyzer` | System Tables + Budget API | S2·05 |
# MAGIC | Despliegue custom | Asset Bundle · API · SDK | S2·06 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 💡 Qué se llevan
# MAGIC - Un **agente de afiliados** real: RAG + 3 tools transaccionales sobre **Lakebase**, servido y con **App**.
# MAGIC - Gobierno, observabilidad, monitoreo, alertas y FinOps **incluidos en la plataforma** (no como código a mantener).
# MAGIC - El patrón **deploy-as-code** para integrarlo a su framework y CI/CD.
# MAGIC
# MAGIC ## 🚀 Próximos pasos sugeridos
# MAGIC 1. Conectar el agente a datos reales de Comfama (sus catálogos de programas y afiliados) vía **synced tables**.
# MAGIC 2. Endurecer guardrails (topics/keywords) y evaluación de calidad con la **Review App**.
# MAGIC 3. Migrar el repo del agente a un **Asset Bundle** en su Git y conectar el pipeline de CI/CD.
# MAGIC 4. Definir presupuestos y SLOs (latencia/costo) con System Tables + Budgets.
# MAGIC
# MAGIC ## 🧹 Limpieza (opcional)
# MAGIC Scale-to-zero ya minimiza costo. Para borrar todo: el endpoint del agente, la App, el monitor, el índice de VS y
# MAGIC la instancia Lakebase (o solo tu **branch**).
# MAGIC
# MAGIC ¡Gracias por participar! 🙌

