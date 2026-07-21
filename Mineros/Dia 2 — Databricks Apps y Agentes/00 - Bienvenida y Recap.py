# Databricks notebook source
# MAGIC %md
# MAGIC # 👋 Bienvenida — Día 2: Databricks Apps y Agentes
# MAGIC
# MAGIC ## Recap del Día 1
# MAGIC Construimos un pipeline (Bronze→Silver→Gold), lo gobernamos con Unity Catalog y
# MAGIC creamos un **Genie** sobre las tablas Gold de pedidos y clientes.
# MAGIC
# MAGIC ## Objetivo del día
# MAGIC Llevar esos datos a manos del negocio: primero con una **App que integra Genie**, y luego
# MAGIC dando el salto a **agentes** que razonan y usan herramientas.
# MAGIC
# MAGIC ## Al terminar el Día 2, sabrás:
# MAGIC - Qué son las **Databricks Apps** y cuándo usarlas.
# MAGIC - Construir y **desplegar una App Streamlit** que lee tus datos, **escribe notas** (write-back) e integra **chat de Genie**.
# MAGIC - Los fundamentos de **agentes** en Databricks (Foundation Models, herramientas, AI Playground).
# MAGIC - Crear un agente y un **Knowledge Assistant con Agent Bricks** (UI-first).
# MAGIC
# MAGIC ## Agenda (~3:30 h)
# MAGIC | Tiempo | Lección | Notebook |
# MAGIC |---|---|---|
# MAGIC | 0:00–0:15 | Bienvenida y recap | `00 - Bienvenida y Recap` |
# MAGIC | 0:15–0:35 | Intro a Apps y Genie | `01 - Intro a Apps y Genie` |
# MAGIC | 0:35–1:35 | Lab: App Streamlit + Genie | `02 - Lab App Streamlit + Genie` |
# MAGIC | 1:35–1:45 | ☕ Break | |
# MAGIC | 1:45–2:05 | Intro a Agentes | `03 - Intro a Agentes` |
# MAGIC | 2:05–2:55 | Lab: Crear un Agente | `04 - Lab Crear un Agente` |
# MAGIC | 2:55–3:05 | ☕ Break | |
# MAGIC | 3:05–3:25 | Lab: Agent Bricks | `05 - Lab Agent Bricks` |
# MAGIC | 3:25–3:30 | Cierre y próximos pasos | `06 - Cierre y Próximos Pasos` |
# MAGIC
# MAGIC ## Prerrequisitos (del Día 1)
# MAGIC - Tu esquema `academia.<tu_apellido>` con las tablas Gold.
# MAGIC - Tu **Genie Space ID** y el **HTTP Path** de tu SQL Warehouse.
# MAGIC
# MAGIC > ℹ️ Los labs de agentes usan un dataset propio (clientes/productos/opiniones) que se
# MAGIC > carga en `04 - Lab Crear un Agente`. Los datos están en `../_recursos/datos_agentes`.
# MAGIC
# MAGIC ➡️ **Empieza con** `01 - Intro a Apps y Genie`.

