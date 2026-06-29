# Databricks notebook source
# MAGIC %md
# MAGIC # 🧭 Sesión 1 · 01 — Product Tour: el Agente end-to-end
# MAGIC
# MAGIC Antes de construir, veamos el **mapa completo**: qué productos de Databricks tocamos, en qué orden, y a qué
# MAGIC pieza del **framework actual de Comfama** equivale cada uno.
# MAGIC
# MAGIC > Módulo **conceptual** — no hay nada que ejecutar. Tómate 15–20 min para ubicar el bosque antes de los árboles.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 Arquitectura objetivo
# MAGIC
# MAGIC ```
# MAGIC                  ┌──────────────────────────────────────────────┐
# MAGIC                  │            Databricks App (chat UI)            │  ← equivale a Azure Container Apps  [S2]
# MAGIC                  │                  OBO auth                      │
# MAGIC                  └───────────────┬───────────────┬──────────────┘
# MAGIC                                  │               │
# MAGIC                  ┌───────────────▼──────┐   ┌────▼─────────────────────┐
# MAGIC                  │   Agente servido      │   │      Lakebase (OLTP)     │  ← equivale a Cosmos DB     [S1]
# MAGIC                  │ (Model Serving) [S1]  │   │  afiliados · programas   │
# MAGIC                  │   + AI Gateway  [S1]  │   │  reservas · cupos        │
# MAGIC                  │  guardrails · límites │   │  conversaciones/mensajes │
# MAGIC                  └───────┬───────┬──────┘   └────┬─────────────────────┘
# MAGIC                          │       │               │ sync ⇅
# MAGIC               ┌──────────▼─┐  ┌──▼───────────┐   │
# MAGIC               │  FM (LLM)  │  │ Vector Search│   │
# MAGIC               │ vía Gateway│  │  (RAG KB)    │   │
# MAGIC               └────────────┘  └──────┬───────┘   │
# MAGIC                                      │           │
# MAGIC                  ┌───────────────────▼───────────▼──────────────┐
# MAGIC                  │          Unity Catalog (Delta · gobierno)     │  ← equivale a security/         [S2]
# MAGIC                  │   KB · tablas analíticas · lineage · audit    │
# MAGIC                  └───────────────────────────────────────────────┘
# MAGIC        Transversales [S2]:  MLflow Tracing · Lakehouse Monitoring · SQL Alerts · System Tables/Budget
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 Equivalencias: framework de Comfama → producto Databricks
# MAGIC
# MAGIC | Pieza del framework Comfama (hoy, Azure) | Producto Databricks (este workshop) | Sesión |
# MAGIC |---|---|---|
# MAGIC | `TemplateAgentes` (orquestación de agentes) | **Mosaic AI Agent Framework / Agent Bricks** | S1 · 04 |
# MAGIC | `LLMConfig + TokenProvider` (config y llaves del LLM) | **AI Gateway + Foundation Model APIs** | S1 · 06 |
# MAGIC | **Cosmos DB** (estado conversacional + datos operacionales) | **Lakebase** (Postgres serverless OLTP) | S1 · 03 |
# MAGIC | **Azure Container Apps** (frontend del agente) | **Databricks Apps** | S2 · 01 |
# MAGIC | `TelemetryManager` (OTLP + Prometheus) | **MLflow 3 Tracing** | S2 · 02 |
# MAGIC | `security/` (gobierno y permisos) | **Unity Catalog** (lineage, ABAC, audit) | S2 · 03 |
# MAGIC | Monitoreo de datos/modelos custom | **Lakehouse Monitoring** | S2 · 04 |
# MAGIC | `AlertEvaluator` (alertas) | **Databricks SQL Alerts** | S2 · 04 |
# MAGIC | `FinOpsAnalyzer` (costos DBU) | **System Tables + Budget API** | S2 · 05 |
# MAGIC | Despliegue custom (scripts) | **Asset Bundle · Jobs API · SDK** | S2 · 06 |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧩 Los 3 productos protagonistas hoy (Sesión 1)
# MAGIC
# MAGIC **1. Vector Search (RAG)** — convierte la base de conocimiento de Comfama (`kb_documentos`) en un índice
# MAGIC semántico. El agente lo consulta para responder preguntas con información de la fuente, sin alucinaciones.
# MAGIC
# MAGIC **2. Lakebase (OLTP)** — Postgres serverless gestionado. Es el **sistema de registro operacional** del agente:
# MAGIC afiliados, programas con cupos, reservas y memoria conversacional. Aquí ocurre la **transacción** `crear_reserva`.
# MAGIC *Equivale a Cosmos DB, pero integrado al lakehouse (sync con Delta, gobierno en UC, branching, scale-to-zero).*
# MAGIC
# MAGIC **3. Agent Framework + Model Serving + AI Gateway** — construimos el agente (retriever RAG + 3 tools), lo
# MAGIC **servimos** como endpoint REST, y lo **gobernamos** con AI Gateway (rate limits, guardrails de PII, tracking
# MAGIC de uso que luego alimenta FinOps). *Equivale a `TemplateAgentes` + `LLMConfig/TokenProvider`.*

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🛠️ Las 3 herramientas (tools) del agente
# MAGIC
# MAGIC | Tool | Qué hace | Lee/Escribe en |
# MAGIC |---|---|---|
# MAGIC | `consultar_beneficios(afiliado_id)` | Devuelve inscripciones y estado del afiliado | **lee** Lakebase |
# MAGIC | `consultar_disponibilidad(programa)` | Cupos disponibles de un programa | **lee** Lakebase |
# MAGIC | `crear_reserva(afiliado_id, programa_id)` | Reserva un cupo (transacción atómica) | **escribe** Lakebase |
# MAGIC | *(retriever)* | Busca en la base de conocimiento | **lee** Vector Search |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 📊 Slides oficiales (complementarias)
# MAGIC
# MAGIC > _Placeholder para el paso de reutilización (hybrid):_ aquí incrustamos como imágenes las slides clave de los
# MAGIC > decks oficiales de **AI Gateway**, **Agent Bricks** y **Lakebase** (ya disponibles en las carpetas
# MAGIC > `Agentes & AI/imagenes`, `Lakebase/imagenes` de Comfama). Se agregan en la carpeta `imagenes/` de esta sesión
# MAGIC > y se referencian aquí, **solo como complemento** del flujo hands-on.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ▶️ Siguiente: `02 - Setup & Knowledge Base` — creamos el índice de Vector Search.

