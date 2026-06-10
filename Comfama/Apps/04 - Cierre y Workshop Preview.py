# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Cierre + Workshop Deep-Dive Unificado 🎬
# MAGIC
# MAGIC **10 minutos.** Recap de esta sesión + preview del workshop **unificado** del fin de mes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo que cubrimos hoy
# MAGIC
# MAGIC Esta sesión fue una de **3 sesiones express** que vimos esta semana:
# MAGIC
# MAGIC | Sesión | Productos | Folder |
# MAGIC |---|---|---|
# MAGIC | **Agentes & AI** | Playground, AI Gateway, Agent Bricks, Foundation Models | `Comfama/Agentes & AI/` |
# MAGIC | **Apps** | Databricks Apps + integración con Model Serving / UC / Secrets | `Comfama/Apps/` |
# MAGIC | **Lakebase** | Postgres serverless + branching + sync nativo | `Comfama/Lakebase/` |
# MAGIC
# MAGIC Cada una fue surface-level (~1 hora). **El workshop deep-dive es donde todo se conecta.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Workshop Deep-Dive — fin de mes (4 horas, unificado)
# MAGIC
# MAGIC En lugar de 3 workshops separados, vamos a hacer **uno solo end-to-end** donde construimos un agente productivo real para Comfama, usando los 3 productos integrados.
# MAGIC
# MAGIC ### Caso de uso del workshop
# MAGIC
# MAGIC > **Asistente conversacional sobre servicios de Comfama** — subsidios, salud, créditos, citas. Con memoria conversacional persistente y deployable desde día 1.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda del workshop (4 horas)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │  Bloque 1 (1h):  DATOS + GOVERNANCE                              │
# MAGIC │  ──────────────────────────────────────                          │
# MAGIC │  · Ingestar docs reales de Comfama a UC                          │
# MAGIC │  · Crear Vector Search index                                     │
# MAGIC │  · Tags, lineage, ABAC                                           │
# MAGIC │  · Aplicar permisos UC                                           │
# MAGIC │                                                                  │
# MAGIC │  Productos: UC + Vector Search + Auto Loader                     │
# MAGIC │                                                                  │
# MAGIC │  Bloque 2 (1h):  AGENTE + AI GATEWAY                             │
# MAGIC │  ──────────────────────────────────                              │
# MAGIC │  · Prototipo en Playground                                       │
# MAGIC │  · Export → notebook → Agent Bricks                              │
# MAGIC │  · Deploy a Model Serving                                        │
# MAGIC │  · Configurar AI Gateway: guardrails, rate limits, fallback      │
# MAGIC │                                                                  │
# MAGIC │  Productos: AI Playground + Agent Bricks + AI Gateway            │
# MAGIC │                                                                  │
# MAGIC │  Bloque 3 (1h):  ESTADO PERSISTENTE + APP                        │
# MAGIC │  ──────────────────────────────────────                          │
# MAGIC │  · Crear instancia Lakebase para conversaciones                  │
# MAGIC │  · Schema: sesiones, mensajes, feedback                          │
# MAGIC │  · Construir Streamlit app integrando el agente                  │
# MAGIC │  · Deploy a Databricks Apps                                      │
# MAGIC │  · Auth OBO + Service Principal                                  │
# MAGIC │                                                                  │
# MAGIC │  Productos: Lakebase + Databricks Apps                           │
# MAGIC │                                                                  │
# MAGIC │  Bloque 4 (1h):  MONITORING + ITERATION                          │
# MAGIC │  ────────────────────────────────                                │
# MAGIC │  · Inference Tables capturando todo                              │
# MAGIC │  · Lakehouse Monitoring sobre métricas Gold                      │
# MAGIC │  · Lakeview dashboard con KPIs end-to-end                        │
# MAGIC │  · SQL Alerts sobre degradación                                  │
# MAGIC │  · System Tables para FinOps                                     │
# MAGIC │                                                                  │
# MAGIC │  Productos: Inference Tables + Lakehouse Monitor + SQL Alerts    │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagrama del stack que vamos a construir
# MAGIC
# MAGIC ```
# MAGIC                       ┌─────────────────────────┐
# MAGIC                       │  📱 Databricks App      │ ← Streamlit, OBO auth
# MAGIC                       └─────────┬───────────────┘
# MAGIC                                 │
# MAGIC          ┌──────────────────────┼─────────────────────────┐
# MAGIC          ↓                      ↓                          ↓
# MAGIC  ┌──────────────┐    ┌──────────────────┐      ┌──────────────────┐
# MAGIC  │ 🚀 Model     │    │ 🐘 Lakebase      │      │ 🔍 Vector Search │
# MAGIC  │   Serving    │    │   (sesiones+     │      │   (docs idx)     │
# MAGIC  │  + Gateway   │    │    historial)    │      │                  │
# MAGIC  └──────┬───────┘    └──────────────────┘      └──────────────────┘
# MAGIC         │
# MAGIC         ↓
# MAGIC  ┌──────────────────────────────────────────────────────────┐
# MAGIC  │ 📊 Inference Table + 📈 Monitor + 🚨 Alerts + 💰 Sys.tables │
# MAGIC  └──────────────────────────────────────────────────────────┘
# MAGIC                          ↓
# MAGIC                  Lakeview Dashboard
# MAGIC ```
# MAGIC
# MAGIC Todo gobernado por Unity Catalog. Single source of truth.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-trabajo del cliente (1 semana antes del workshop)
# MAGIC
# MAGIC Para que el workshop sea sobre **su caso real** y no un demo genérico:
# MAGIC
# MAGIC ### Datos
# MAGIC - [ ] Recopilar 5-10 documentos reales (PDF/Word) de servicios Comfama — subsidios, FAQs, políticas
# MAGIC - [ ] Subirlos a un Volume UC antes del workshop
# MAGIC
# MAGIC ### Caso de uso
# MAGIC - [ ] Definir 1 caso de uso específico (ej. "asistente para preguntas sobre subsidio escolar")
# MAGIC - [ ] Listar 10-15 preguntas reales que afiliados harían
# MAGIC - [ ] Métricas de éxito (latencia objetivo, accuracy, costo aceptable)
# MAGIC
# MAGIC ### Pre-requisitos técnicos
# MAGIC - [ ] Cada participante: acceso al workspace + permisos básicos (USE CATALOG, CREATE TABLE)
# MAGIC - [ ] Workspace tiene Foundation Models habilitados ✅ (ya validado)
# MAGIC - [ ] Vector Search endpoint disponible ✅ (ya hay uno)
# MAGIC - [ ] Lakebase habilitado en el workspace ✅ (es FEVM, debe estar)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo que cada participante va a salir teniendo
# MAGIC
# MAGIC Al final de las 4 horas, cada uno tendrá en su workspace:
# MAGIC
# MAGIC 1. ✅ Su propio Vector Search index con docs de Comfama
# MAGIC 2. ✅ Su propio agente desplegado en Model Serving con AI Gateway
# MAGIC 3. ✅ Su propia instancia Lakebase para sesiones
# MAGIC 4. ✅ Su propia Databricks App corriendo
# MAGIC 5. ✅ Su propio dashboard de monitoring
# MAGIC 6. ✅ Su propio SQL alert configurado
# MAGIC
# MAGIC Es un demo que pueden **mostrar internamente a su jefe o equipo** sin ediciones.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximos pasos individuales (esta semana, antes del workshop)
# MAGIC
# MAGIC Para llegar preparados:
# MAGIC
# MAGIC | Si te interesa más... | Haz esto |
# MAGIC |---|---|
# MAGIC | **Playground** | Crea una "session" en Playground con un tool de retrieval |
# MAGIC | **AI Gateway** | Lee el [Product Guide](https://docs.databricks.com/en/ai-gateway/index.html) — 15 min |
# MAGIC | **Apps** | Revisa los [recipes del Cookbook](https://apps-cookbook.dev/) que apliquen a su stack |
# MAGIC | **Lakebase** | Lee el [post de GA](https://www.databricks.com/blog/announcing-lakebase-public-preview) — 10 min |
# MAGIC | **Su framework actual vs Databricks** | Identifica las 3 features que más temen perder al migrar, las trabajamos en el workshop |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recursos
# MAGIC
# MAGIC ### Docs oficiales
# MAGIC
# MAGIC - AI Gateway: https://docs.databricks.com/en/ai-gateway/index.html
# MAGIC - Playground: https://docs.databricks.com/en/large-language-models/ai-playground.html
# MAGIC - Agent Framework: https://docs.databricks.com/en/generative-ai/agent-framework/index.html
# MAGIC - Databricks Apps: https://docs.databricks.com/en/dev-tools/databricks-apps/index.html
# MAGIC - Lakebase: https://docs.databricks.com/en/database/index.html
# MAGIC - Apps Cookbook: https://apps-cookbook.dev/
# MAGIC
# MAGIC ### En este repo (workspace)
# MAGIC
# MAGIC - `Comfama/Framework/` — el demo completo del agente Comfama (Vector Search + Agent + App + Dashboard) — referencia funcional
# MAGIC - `Comfama/Agentes & AI/` — sesión express de Playground + AI Gateway
# MAGIC - `Comfama/Apps/` — sesión express de Databricks Apps
# MAGIC - `Comfama/Lakebase/` — sesión express de Lakebase
# MAGIC
# MAGIC ### Para reagendar / dudas
# MAGIC
# MAGIC Slack: `#fe-latam` o ping directo a Rico Martinez.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q&A
# MAGIC
# MAGIC Tiempo para preguntas. Las que no alcancemos a contestar las recogemos como tema para el workshop unificado.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Nos vemos a fin de mes para construir todo end-to-end 🚀**
