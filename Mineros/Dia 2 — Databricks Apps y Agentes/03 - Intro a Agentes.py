# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2 · Lección 3: Introducción a los Agentes en Databricks
# MAGIC
# MAGIC Genie responde preguntas sobre **una** fuente de datos. Un **agente** va más allá:
# MAGIC razona, decide **qué herramientas usar** y las combina para resolver tareas complejas.
# MAGIC
# MAGIC ## De Genie a Agentes
# MAGIC | | Genie | Agente |
# MAGIC |---|---|---|
# MAGIC | Entrada | Pregunta en lenguaje natural | Objetivo / tarea |
# MAGIC | Alcance | Consulta tablas configuradas | Usa **varias herramientas** (SQL, funciones, APIs, retrieval) |
# MAGIC | Razonamiento | SQL de una pasada | Multi-paso, decide y encadena acciones |
# MAGIC | Ejemplo | "¿Cuántos pedidos hubo?" | "Revisa el pedido del cliente X, verifica stock y redacta una respuesta" |
# MAGIC
# MAGIC ## Bloques de construcción en Databricks
# MAGIC - **Foundation Model APIs** — LLMs servidos y listos (pay-per-token o provisioned).
# MAGIC - **AI Playground** — probar prompts y modelos sin escribir código.
# MAGIC - **Herramientas (tools)** — funciones UC, consultas a Lakehouse, Lakebase, retrieval de docs.
# MAGIC - **Agent Bricks** — construir agentes (p. ej. *Knowledge Assistant*) desde la UI.
# MAGIC - **Mosaic AI Agent Framework + MLflow** — para producción (tracing, evaluación, serving).
# MAGIC
# MAGIC ## Arquitectura típica de un agente
# MAGIC ```
# MAGIC   Usuario ──▶ Agente (LLM + razonamiento)
# MAGIC                 ├──▶ Herramienta: consulta SQL / función UC
# MAGIC                 ├──▶ Herramienta: retrieval de documentos (Vector Search)
# MAGIC                 └──▶ Herramienta: datos operacionales (Lakebase)
# MAGIC ```
# MAGIC
# MAGIC ## Lo que haremos (UI-first)
# MAGIC 1. `04 - Lab Crear un Agente`: usar Foundation Models, el Playground y **definir herramientas**
# MAGIC    (funciones, datos estructurados del Lakehouse, datos no estructurados).
# MAGIC 2. `05 - Lab Agent Bricks`: crear un **Knowledge Assistant** desde la UI.
# MAGIC
# MAGIC > 🎯 Mantendremos el enfoque **UI-first**: la meta es entender el modelo mental y crear un
# MAGIC > agente funcional, no ingeniería de producción (eso es un taller aparte).
# MAGIC
# MAGIC ➡️ **Siguiente:** `04 - Lab Crear un Agente`.
