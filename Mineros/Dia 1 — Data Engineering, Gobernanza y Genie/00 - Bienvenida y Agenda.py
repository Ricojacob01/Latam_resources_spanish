# Databricks notebook source
# MAGIC %md
# MAGIC # 👋 Bienvenida — Día 1: Data Engineering, Gobernanza y Genie
# MAGIC
# MAGIC ## Objetivo del día
# MAGIC Construir un pipeline de datos confiable de punta a punta y ponerle una capa de
# MAGIC lenguaje natural (Genie) encima — la base sobre la que el **Día 2** construiremos una App y agentes.
# MAGIC
# MAGIC ## Al terminar el Día 1, sabrás:
# MAGIC - Construir un **Lakeflow Spark Declarative Pipeline** (Bronze → Silver → Gold).
# MAGIC - Aplicar **expectativas de calidad de datos** y **Auto Loader**.
# MAGIC - Implementar **Change Data Capture (AUTO CDC)** con SCD Tipo 1.
# MAGIC - Gobernar los datos con **Unity Catalog** (permisos, linaje).
# MAGIC - Crear un panel **BI** y un espacio **Genie** en español sobre tus tablas.
# MAGIC
# MAGIC ## Agenda (~3:30 h)
# MAGIC | Tiempo | Lección | Notebook |
# MAGIC |---|---|---|
# MAGIC | 0:00–0:15 | Bienvenida y contexto | `00 - Bienvenida y Agenda` |
# MAGIC | 0:15–0:30 | Setup del entorno | `01 - Setup` |
# MAGIC | 0:30–1:15 | Pipeline con calidad de datos | `02 - Lab Pipeline con Calidad de Datos` |
# MAGIC | 1:15–1:25 | ☕ Break | |
# MAGIC | 1:25–2:15 | CDC y producción | `03 - Lab CDC y Producción` |
# MAGIC | 2:15–2:45 | Gobernanza (Unity Catalog) | `04 - Gobernanza (Unity Catalog)` |
# MAGIC | 2:45–2:55 | ☕ Break | |
# MAGIC | 2:55–3:15 | BI Dashboard | `05 - BI Dashboard` |
# MAGIC | 3:15–3:25 | Crear un Genie | `06 - Crear un Genie` |
# MAGIC | 3:25–3:30 | Cierre | `07 - Cierre Día 1` |
# MAGIC
# MAGIC ## Prerrequisitos
# MAGIC - Workspace con **Unity Catalog** y **Serverless** habilitados.
# MAGIC - Permisos **CREATE CATALOG**.
# MAGIC - Un **SQL Warehouse** disponible.
# MAGIC
# MAGIC ## Caso de negocio (hilo conductor)
# MAGIC Somos un retailer: ingerimos **pedidos** y **clientes**, los limpiamos y agregamos,
# MAGIC y queremos responder preguntas del negocio sin escribir SQL. Ese mismo dataset nos
# MAGIC acompañará el Día 2 en la App y el agente.
# MAGIC
# MAGIC ➡️ **Empieza con** `01 - Setup`.
