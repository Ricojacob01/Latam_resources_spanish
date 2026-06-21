# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour 📊 · Governance, Genie & Agent Bricks
# MAGIC
# MAGIC ~20 min. Arco narrativo:
# MAGIC
# MAGIC > **El problema** (datos sin gobernar, agentes frágiles) → **Unity Catalog** (gobernanza) → **AI Functions** (IA en SQL) → **Genie** (NLQ) → **Agent Bricks** (agentes declarativos) → **Cómo se conecta todo**

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 1 — El problema
# MAGIC
# MAGIC - **Gobernanza manual no escala:** clasificar y documentar miles de columnas a mano es inviable; sin esto, los agentes razonan sobre datos que no entienden.
# MAGIC - **Agentes sin contexto empresarial** alucinan: necesitan datos *descritos*, *gobernados* y *seguros*.
# MAGIC - **Fragmentación:** un stack para BI, otro para LLMs, otro para permisos.
# MAGIC
# MAGIC > La plataforma resuelve esto con **Unity Catalog** como base de gobernanza + **IA nativa** (AI Functions, Genie, Agent Bricks) que *reutiliza* esa gobernanza.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 2 — Unity Catalog: la base de gobernanza 🛡️
# MAGIC
# MAGIC Un solo plano de control para **datos + IA**: permisos (`GRANT`), *lineage*, *tags*, comentarios, **row filters** y **column masks**, y auditoría.
# MAGIC
# MAGIC La novedad: la **gobernanza asistida por IA** — `ai_gen` genera comentarios de columnas, `ai_query` clasifica datos sensibles (SENSIBLE/CONFIDENCIAL/PÚBLICO), y aplicas *tags* + *masks* en bucle sobre todo el esquema.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 3 — AI Functions: IA en SQL 🧮
# MAGIC
# MAGIC Funciones SQL que llaman un LLM gobernado, sin salir del data warehouse:
# MAGIC
# MAGIC | Función | Para qué |
# MAGIC |---|---|
# MAGIC | `ai_query` | Inferencia general contra cualquier modelo (con `responseFormat` JSON) |
# MAGIC | `ai_classify` | Clasificar texto en categorías |
# MAGIC | `ai_extract` | Extraer entidades/campos |
# MAGIC | `ai_analyze_sentiment` | Sentimiento |
# MAGIC | `ai_summarize` / `ai_translate` / `ai_gen` | Resumir / traducir / generar |
# MAGIC
# MAGIC Se usan igual en una fila o sobre **millones** (batch inference).

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 4 — Genie: lenguaje natural sobre tus datos 💬
# MAGIC
# MAGIC Un **Genie space** deja a usuarios de negocio preguntar en español y obtener SQL + tablas + gráficos. Tú lo guías con *instructions*, ejemplos de preguntas y relaciones (JOINs). Se consume desde la UI **o** desde una **App** vía el SDK (`w.genie.start_conversation_and_wait(...)`).

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 5 — Agent Bricks: agentes declarativos 🧱
# MAGIC
# MAGIC La forma de **construir y desplegar agentes** sin pelear con frameworks:
# MAGIC
# MAGIC - **Knowledge Assistant** (RAG sobre documentos) — el que harás en el módulo 05.
# MAGIC - **Information Extraction**, **Multi-agent Supervisor**, **Chat Bot**.
# MAGIC
# MAGIC Databricks hace el parsing, chunking, embeddings, Vector Search index, el endpoint y la evaluación **por ti**. Tú aportas datos gobernados + instrucciones.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧩 Recap — cómo se conecta
# MAGIC
# MAGIC ```
# MAGIC   Unity Catalog (permisos · tags · masks · lineage · comentarios con IA)
# MAGIC        │  datos descritos, seguros, gobernados
# MAGIC        ├─► AI Functions (SQL)  ── enriquecer/clasificar a escala
# MAGIC        ├─► Genie                ── NLQ para negocio  ──► App (SDK)
# MAGIC        └─► Agent Bricks         ── agentes RAG/tools ──► Model Serving endpoint
# MAGIC ```
# MAGIC
# MAGIC ## ¿Listo? → `02 - LAB Gobernanza con Unity Catalog`
