# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2 · Lección 1: Introducción a Databricks Apps y Genie
# MAGIC
# MAGIC Ayer (Día 1) construimos un pipeline de datos, lo gobernamos con Unity Catalog y
# MAGIC creamos un **espacio Genie** sobre las tablas Gold. Hoy convertimos eso en producto:
# MAGIC una **Databricks App** que la gente del negocio puede usar.
# MAGIC
# MAGIC ## ¿Qué es una Databricks App?
# MAGIC - Aplicaciones web (Streamlit, Dash, Flask, Node…) que corren **dentro** de Databricks.
# MAGIC - **Serverless**: sin gestionar contenedores ni infraestructura.
# MAGIC - **Auth integrada**: cada app tiene un *Service Principal*; respeta Unity Catalog.
# MAGIC - Ideal para: paneles interactivos, formularios de captura, y **frontends de agentes/Genie**.
# MAGIC
# MAGIC ## ¿Qué es Genie?
# MAGIC - Interfaz de **lenguaje natural** sobre tus tablas: preguntas en español → SQL + respuesta.
# MAGIC - Se configura con *instructions*, consultas de referencia y relaciones entre tablas.
# MAGIC - Se puede **incrustar** en una App vía el SDK (`WorkspaceClient().genie`).
# MAGIC
# MAGIC ## Apps + Genie juntos (lo que haremos hoy)
# MAGIC ```
# MAGIC   Usuario ──▶ Databricks App (Streamlit) ──▶ Genie Space ──▶ Unity Catalog (tablas Gold Día 1)
# MAGIC                     │
# MAGIC                     └──▶ SQL Warehouse (panel de analítica, solo lectura)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación rápida de prerrequisitos
# MAGIC Confirmamos que las tablas del Día 1 existen y que sabes tu catálogo.

# COMMAND ----------

import re

current_user = spark.sql("SELECT current_user()").collect()[0][0]
clean_username = re.sub(r'[^a-z0-9]', '_', current_user.split("@")[0].lower())
CATALOGO = f"sdp_workshop_{clean_username}"

try:
    spark.sql(f"USE CATALOG `{CATALOGO}`")
    tablas = [r["tableName"] for r in spark.sql("SHOW TABLES IN silver").collect()]
    print(f"✓ Catálogo: {CATALOGO}")
    print(f"✓ Tablas en silver: {tablas}")
    print("\nListo para la Lección 2 (construir la App).")
except Exception as e:
    print(f"⚠️  No encuentro tu catálogo del Día 1 ({CATALOGO}).")
    print("   Ejecuta primero el Día 1 (Setup + pipeline). Detalle:", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Product Tour (slides)
# MAGIC Si tienes las slides del *Apps & Genie Product Deck*, muéstralas aquí (5–10 min) antes
# MAGIC de pasar al laboratorio. Puntos a cubrir:
# MAGIC - Casos de uso reales de Apps en clientes.
# MAGIC - Modelo de permisos (Service Principal, On-Behalf-Of, Secrets).
# MAGIC - Apps Cookbook y plantillas.
# MAGIC
# MAGIC ➡️ **Siguiente:** `02 - Lab App Streamlit + Genie`.
