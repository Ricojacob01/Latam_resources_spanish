# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2 · Lección 2: Databricks App (Streamlit) que integra Genie
# MAGIC
# MAGIC Vamos a construir y desplegar una **Databricks App** en Streamlit que:
# MAGIC 1. Lee y visualiza las tablas Gold/Silver que construimos el **Día 1** (pedidos y clientes).
# MAGIC 2. Integra el **espacio Genie** del Día 1 como un **chatbot** dentro de la app.
# MAGIC
# MAGIC **Referencia:** databricks-apps-cookbook — https://github.com/databricks-solutions/databricks-apps-cookbook
# MAGIC
# MAGIC **Prerrequisitos:**
# MAGIC - Día 1 completo (tablas `silver.orders_clean`, `silver.customers`, `gold.order_summary`).
# MAGIC - Un espacio Genie creado (Día 1, Lección 6) — necesitas su **Genie Space ID**.
# MAGIC - Un **SQL Warehouse** (anota su *HTTP Path*).
# MAGIC
# MAGIC > 📁 El código de la app listo para desplegar está en la carpeta **`app_source/`** junto a este notebook
# MAGIC > (`app.py`, `app.yaml`, `requirements.txt`). Este notebook explica cada parte y cómo desplegarla.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0: Contexto (mismo catálogo del Día 1)

# COMMAND ----------

import re

current_user = spark.sql("SELECT current_user()").collect()[0][0]
clean_username = re.sub(r'[^a-z0-9]', '_', current_user.split("@")[0].lower())

CATALOGO = f"sdp_workshop_{clean_username}"
spark.sql(f"USE CATALOG `{CATALOGO}`")
print(f"Catálogo: {CATALOGO}")
print(f"Tablas: {CATALOGO}.silver.orders_clean · {CATALOGO}.silver.customers · {CATALOGO}.gold.order_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1: Probar la lógica de lectura (modo notebook)
# MAGIC Antes de desplegar como App, validamos las consultas que usará el frontend.

# COMMAND ----------

# Vista de pedidos por día (para la gráfica de tendencia)
display(spark.sql("SELECT order_date, total_daily_orders, unique_customers FROM gold.order_summary ORDER BY order_date"))

# COMMAND ----------

# Pedidos por ciudad (JOIN pedidos ↔ clientes) — para la tabla y el gráfico de barras
display(spark.sql("""
  SELECT c.city, COUNT(o.order_id) AS total_pedidos, COUNT(DISTINCT o.customer_id) AS clientes_unicos
  FROM silver.orders_clean o
  JOIN silver.customers c ON o.customer_id = c.customer_id
  GROUP BY c.city
  ORDER BY total_pedidos DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2: Anatomía de la app (`app_source/app.py`)
# MAGIC La app tiene tres bloques:
# MAGIC
# MAGIC 1. **Conexión** a un SQL Warehouse vía `databricks-sql-connector` (auth con el
# MAGIC    Service Principal de la App — sin tokens hardcodeados).
# MAGIC 2. **Panel de analítica** (solo lectura): KPIs de pedidos, tendencia diaria y
# MAGIC    pedidos por ciudad. *No modificamos las tablas del pipeline* — son gestionadas
# MAGIC    por Lakeflow (streaming tables / materialized views), así que la app las **consume**.
# MAGIC 3. **Chatbot Genie**: usa `WorkspaceClient().genie` para conversar sobre los mismos datos.
# MAGIC
# MAGIC > 🔎 **Por qué solo lectura:** una materialized view / streaming table no admite `UPDATE`
# MAGIC > desde fuera del pipeline. Si quieres una app que *escriba* datos, ve al **Apéndice**
# MAGIC > al final (tabla propia de la app en un esquema `app`, patrón que luego escala a Lakebase).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3: Configurar y desplegar la App
# MAGIC
# MAGIC 1. **Edita `app_source/app.yaml`** y define tus variables (o pásalas como *App resources*):
# MAGIC    - `GENIE_SPACE_ID` — el ID del espacio Genie del Día 1.
# MAGIC    - `SQL_HTTP_PATH` — el HTTP Path de tu SQL Warehouse.
# MAGIC    - `CATALOG` — tu `sdp_workshop_<usuario>`.
# MAGIC 2. **Crea la App** (UI): *Compute → Apps → Create App → Custom*, o por CLI:
# MAGIC    ```bash
# MAGIC    databricks apps create pedidos-genie-<apellido>
# MAGIC    databricks sync ./app_source /Workspace/Users/<tu_usuario>/pedidos-genie/app_source
# MAGIC    databricks apps deploy pedidos-genie-<apellido> \
# MAGIC        --source-code-path /Workspace/Users/<tu_usuario>/pedidos-genie/app_source
# MAGIC    ```
# MAGIC 3. **Permisos:** dale al *Service Principal* de la App acceso `CAN USE` al SQL Warehouse,
# MAGIC    `CAN RUN` al espacio Genie, y `SELECT` sobre el catálogo `sdp_workshop_<usuario>`.
# MAGIC 4. Abre la URL de la App y prueba el panel + el chat de Genie.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 (opcional): Vista previa de la app dentro del notebook
# MAGIC Puedes pegar el contenido de `app_source/app.py` en una celda `%python` con Streamlit
# MAGIC en modo notebook para iterar rápido antes de desplegar. Para producción, usa siempre
# MAGIC la Databricks App (auth integrada, serverless, sin gestión de contenedores).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Personalización (logo, colores, textos)
# MAGIC - **Logo:** crea `app_source/assets/logo.png` y en `app.py` agrega
# MAGIC   `st.image("assets/logo.png", width=180)` después de `st.set_page_config()`.
# MAGIC - **Colores:** inyecta CSS con `st.markdown("<style>…</style>", unsafe_allow_html=True)`.
# MAGIC - **Texto de bienvenida:** un `st.markdown` con el objetivo de la app y el caso de uso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## APÉNDICE — App que *escribe* datos (patrón write-back)
# MAGIC Si tu caso de uso necesita actualizar registros desde la app (p. ej. un catálogo
# MAGIC editable), **no** escribas sobre tablas del pipeline. En su lugar:
# MAGIC 1. Crea una tabla propia de la app: `CREATE SCHEMA IF NOT EXISTS app;`
# MAGIC    y `CREATE TABLE app.editable_items (...)`.
# MAGIC 2. La app hace `INSERT/UPDATE` sobre `app.editable_items` (tabla Delta gestionada por la app).
# MAGIC 3. Para baja latencia transaccional real, este patrón escala a **Lakebase** (Postgres gestionado).
# MAGIC
# MAGIC Así mantenemos el pipeline como *source of truth* de solo lectura y separamos el estado editable.

# COMMAND ----------

# MAGIC %md
# MAGIC ### ✅ Cierre de la Lección 2
# MAGIC Tienes una Databricks App desplegada que **integra Genie** sobre los datos del Día 1.
# MAGIC En la siguiente lección pasamos de "chat sobre datos" a **agentes** que razonan y usan herramientas.

