# Databricks notebook source
# MAGIC %md
# MAGIC # Día 2 · Lección 2: Databricks App (Streamlit) que integra Genie
# MAGIC
# MAGIC Vamos a construir y desplegar una **Databricks App** en Streamlit que:
# MAGIC 1. **Lee y visualiza** las tablas Gold/Silver que construimos el **Día 1** (pedidos y clientes).
# MAGIC 2. **Escribe datos** (write-back): un rep de atención puede registrar **notas de seguimiento** de un cliente.
# MAGIC 3. Integra el **espacio Genie** del Día 1 como un **chatbot** dentro de la app.
# MAGIC
# MAGIC **Referencia:** databricks-apps-cookbook — https://github.com/databricks-solutions/databricks-apps-cookbook
# MAGIC
# MAGIC **Prerrequisitos:**
# MAGIC - Día 1 completo (tablas `orders_silver`, `customers_silver`, `order_summary_gold` en `academia.<tu_apellido>`).
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

CATALOGO = "academia"           # catálogo compartido
ESQUEMA = clean_username         # tu esquema (Día 1)
spark.sql(f"USE CATALOG {CATALOGO}")
spark.sql(f"USE SCHEMA {ESQUEMA}")
print(f"Catálogo/Esquema: {CATALOGO}.{ESQUEMA}")
print(f"Tablas de lectura: orders_silver · customers_silver · order_summary_gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1: Crear la tabla de escritura (write-back)
# MAGIC
# MAGIC ⚠️ **Importante:** las tablas del Día 1 (`orders_silver`, `order_summary_gold`, …) las
# MAGIC gestiona **Lakeflow** (streaming tables / materialized views). **No se pueden `UPDATE`/`INSERT`**
# MAGIC desde fuera del pipeline. Por eso la app escribe en una **tabla propia** que creamos aquí:
# MAGIC `app_notas_clientes`. Así separamos el *source of truth* (solo lectura) del estado editable.
# MAGIC
# MAGIC Caso de uso: un agente de atención registra una **nota de seguimiento** sobre un cliente
# MAGIC (p. ej. "cliente pidió cambio", "contactar la próxima semana").

# COMMAND ----------

spark.sql("""
CREATE TABLE IF NOT EXISTS app_notas_clientes (
  customer_id   STRING,
  nota          STRING,
  prioridad     STRING,
  autor         STRING,
  actualizado   TIMESTAMP
) COMMENT 'Notas de seguimiento escritas por la App (tabla propia de la app, editable)'
""")
print("✓ Tabla de write-back lista: app_notas_clientes")
display(spark.sql("SELECT * FROM app_notas_clientes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2: Probar la lógica de lectura (modo notebook)
# MAGIC Antes de desplegar como App, validamos las consultas que usará el frontend.

# COMMAND ----------

# Vista de pedidos por día (para la gráfica de tendencia)
display(spark.sql("SELECT order_date, total_daily_orders, unique_customers FROM order_summary_gold ORDER BY order_date"))

# COMMAND ----------

# Pedidos por ciudad (JOIN pedidos ↔ clientes) — para la tabla y el gráfico de barras
display(spark.sql("""
  SELECT c.city, COUNT(o.order_id) AS total_pedidos, COUNT(DISTINCT o.customer_id) AS clientes_unicos
  FROM orders_silver o
  JOIN customers_silver c ON o.customer_id = c.customer_id
  GROUP BY c.city
  ORDER BY total_pedidos DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3: Probar la lógica de escritura (modo notebook)
# MAGIC Un `MERGE` idempotente: inserta la nota si el cliente no tiene una, o la actualiza si ya existe
# MAGIC (SCD Tipo 1 sobre `customer_id`). Es la misma sentencia que ejecutará la app.

# COMMAND ----------

spark.sql("""
MERGE INTO app_notas_clientes t
USING (SELECT
         'CUST0001' AS customer_id,
         'Cliente pidió cambio de producto; contactar la próxima semana.' AS nota,
         'alta' AS prioridad,
         current_user() AS autor,
         current_timestamp() AS actualizado) s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
""")
display(spark.sql("SELECT * FROM app_notas_clientes ORDER BY actualizado DESC"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4: Anatomía de la app (`app_source/app.py`)
# MAGIC La app tiene cuatro bloques:
# MAGIC
# MAGIC 1. **Conexión** a un SQL Warehouse vía `databricks-sql-connector` (auth con el
# MAGIC    Service Principal de la App — sin tokens hardcodeados).
# MAGIC 2. **Panel de analítica** (solo lectura): KPIs de pedidos, tendencia diaria y pedidos por ciudad.
# MAGIC    Consume las tablas del pipeline (Lakeflow), no las modifica.
# MAGIC 3. **Notas de seguimiento** (write-back): un formulario que hace `MERGE` sobre
# MAGIC    `app_notas_clientes` — la tabla propia de la app que creamos en el Paso 1.
# MAGIC 4. **Chatbot Genie**: usa `WorkspaceClient().genie` para conversar sobre los mismos datos.
# MAGIC
# MAGIC > 🔎 **Separación de responsabilidades:** el pipeline es el *source of truth* de solo lectura;
# MAGIC > el estado editable vive en una tabla aparte de la app. Para baja latencia transaccional real
# MAGIC > (muchas escrituras concurrentes), este patrón escala a **Lakebase** (Postgres gestionado).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5: Configurar y desplegar la App
# MAGIC
# MAGIC 1. **Edita `app_source/app.yaml`** y define tus variables (o pásalas como *App resources*):
# MAGIC    - `GENIE_SPACE_ID` — el ID del espacio Genie del Día 1.
# MAGIC    - `SQL_HTTP_PATH` — el HTTP Path de tu SQL Warehouse.
# MAGIC    - `CATALOG` — `academia` (compartido).
# MAGIC    - `SCHEMA` — tu esquema `<tu_apellido>`.
# MAGIC 2. **Crea la App** (UI): *Compute → Apps → Create App → Custom*, o por CLI:
# MAGIC    ```bash
# MAGIC    databricks apps create pedidos-genie-<apellido>
# MAGIC    databricks sync ./app_source /Workspace/Users/<tu_usuario>/pedidos-genie/app_source
# MAGIC    databricks apps deploy pedidos-genie-<apellido> \
# MAGIC        --source-code-path /Workspace/Users/<tu_usuario>/pedidos-genie/app_source
# MAGIC    ```
# MAGIC 3. **Permisos** para el *Service Principal* de la App:
# MAGIC    - `CAN USE` en el SQL Warehouse.
# MAGIC    - `CAN RUN` en el espacio Genie.
# MAGIC    - `SELECT` sobre las tablas del Día 1 (`orders_silver`, `customers_silver`, `order_summary_gold`).
# MAGIC    - `MODIFY` (INSERT/UPDATE) sobre la tabla de escritura `app_notas_clientes`.
# MAGIC      > 💡 En Unity Catalog: `GRANT SELECT, MODIFY ON TABLE academia.<tu_apellido>.app_notas_clientes TO `<app_service_principal>`;`
# MAGIC 4. Abre la URL de la App y prueba el panel + el formulario de notas + el chat de Genie.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 (opcional): Vista previa de la app dentro del notebook
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
# MAGIC ### ✅ Cierre de la Lección 2
# MAGIC Tienes una Databricks App desplegada que **lee** el panel de analítica, **escribe** notas de
# MAGIC seguimiento (write-back sobre una tabla propia) e **integra Genie** — todo sobre los datos del Día 1.
# MAGIC En la siguiente lección pasamos de "chat sobre datos" a **agentes** que razonan y usan herramientas.

