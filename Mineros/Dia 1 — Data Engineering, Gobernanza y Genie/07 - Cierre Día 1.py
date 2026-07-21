# Databricks notebook source
# MAGIC %md
# MAGIC # ✅ Cierre — Día 1
# MAGIC
# MAGIC ## Lo que construiste hoy
# MAGIC - Un pipeline **Bronze → Silver → Gold** con Auto Loader y expectativas de calidad.
# MAGIC - **AUTO CDC** para clientes (SCD Tipo 1) — INSERT/UPDATE/DELETE declarativos.
# MAGIC - **Gobernanza** con Unity Catalog (permisos y linaje).
# MAGIC - Un **dashboard BI** y un **espacio Genie** en español sobre tus tablas Gold.
# MAGIC
# MAGIC ## Activos que quedan en tu catálogo `sdp_workshop_<usuario>`
# MAGIC | Capa | Tabla | Descripción |
# MAGIC |---|---|---|
# MAGIC | silver | `orders_clean` | Pedidos validados |
# MAGIC | silver | `customers` | Estado actual de clientes (SCD1) |
# MAGIC | gold | `order_summary` | Pedidos agregados por día |
# MAGIC | gold | `customer_summary` | Resumen de clientes |
# MAGIC
# MAGIC ## 📌 Anota para mañana
# MAGIC - Tu **catálogo**: `sdp_workshop_<usuario>`
# MAGIC - Tu **Genie Space ID** (de la Lección 6)
# MAGIC - El **HTTP Path** de tu SQL Warehouse
# MAGIC
# MAGIC ## Mañana (Día 2)
# MAGIC Convertiremos este Genie en una **Databricks App** y luego daremos el salto a **agentes**
# MAGIC que razonan y usan herramientas.
# MAGIC
# MAGIC ¡Buen trabajo! 🎉
