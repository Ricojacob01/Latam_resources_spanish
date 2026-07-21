# Databricks notebook source
# DBTITLE 1,Título y objetivos
# MAGIC %md
# MAGIC # Lección 4: Gobernanza y Seguridad con Unity Catalog
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC Al finalizar esta lección, podrás:
# MAGIC - Explorar el catálogo y las tablas creadas por el pipeline
# MAGIC - Aplicar permisos granulares con `GRANT`
# MAGIC - Proteger datos sensibles con **column masking**
# MAGIC - Restringir el acceso con **row-level filtering**
# MAGIC - Entender el linaje automático de Unity Catalog
# MAGIC
# MAGIC ## Duración: ~30 minutos
# MAGIC
# MAGIC ## Prerrequisitos
# MAGIC - Lecciones 1–3 completadas (pipeline con `orders` y `customers`)
# MAGIC - Tablas `silver.customers` y `silver.orders_clean` disponibles
# MAGIC
# MAGIC > 💡 Unity Catalog se configura una sola vez y aplica automáticamente a todos los activos: tablas, notebooks, modelos, dashboards y volúmenes.

# COMMAND ----------

# DBTITLE 1,¿Qué es Unity Catalog?
# MAGIC %md
# MAGIC ## ¿Qué es Unity Catalog?
# MAGIC
# MAGIC La gobernanza de datos es difícil cuando se trata de una plataforma completa. Con **Unity Catalog**, Databricks centraliza el control de acceso en un único lugar.
# MAGIC
# MAGIC ### Los tres pilares de UC:
# MAGIC
# MAGIC | Pilar | Qué hace |
# MAGIC |---|---|
# MAGIC | **Control de acceso** | GRANTs granulares a nivel de catálogo, schema, tabla, columna y fila |
# MAGIC | **Auditoría** | Registra quién accede a qué datos y cuándo |
# MAGIC | **Linaje** | Traza automáticamente el origen y uso de cada tabla |
# MAGIC
# MAGIC ### La jerarquía: Catálogo → Schema → Tabla
# MAGIC
# MAGIC ```
# MAGIC academia                     ← Catálogo compartido (todos los participantes)
# MAGIC   └── <tu_apellido>          ← Tu esquema personal
# MAGIC         ├── orders_bronze
# MAGIC         ├── orders_silver
# MAGIC         ├── order_summary_gold
# MAGIC         ├── customers          ← Pipeline CDC
# MAGIC         └── customers_demo     ← Tabla para la demo de hoy
# MAGIC ```
# MAGIC
# MAGIC UC cubre **todos** los activos: Tablas, Archivos, Modelos de ML, Dashboards, Notebooks.

# COMMAND ----------

# DBTITLE 1,Setup: conectar al catálogo
# MAGIC %md
# MAGIC ## Setup: Conectar a nuestro catálogo
# MAGIC
# MAGIC Establezcamos el contexto de trabajo. La siguiente celda usa el mismo patrón que el Setup y el pipeline.

# COMMAND ----------

# DBTITLE 1,Establecer catálogo y schema
import re
current_user = spark.sql("SELECT current_user()").collect()[0][0]
clean_username = re.sub(r'[^a-z0-9]', '_', current_user.split("@")[0].lower())

spark.sql("USE CATALOG academia")
spark.sql(f"USE SCHEMA {clean_username}")
print(f"✓ Catálogo: academia")
print(f"✓ Schema:   {clean_username}")
print(f"✓ Usuario:  {current_user}")

# COMMAND ----------

# DBTITLE 1,Verificar contexto
# MAGIC %sql
# MAGIC -- Verificar el contexto actual
# MAGIC SELECT
# MAGIC   CURRENT_CATALOG() AS catalogo,
# MAGIC   CURRENT_SCHEMA()  AS esquema,
# MAGIC   CURRENT_USER()    AS usuario;

# COMMAND ----------

# DBTITLE 1,A. Explorar tablas con Unity Catalog
# MAGIC %md
# MAGIC ## A. Explorar nuestras tablas con Unity Catalog
# MAGIC
# MAGIC Unity Catalog proporciona un **Explorador de Datos** completo desde el menú lateral izquierdo (**Catalog**).
# MAGIC Puedes buscar tablas, ver su schema, estadísticas, linaje y quién las usa — sin escribir SQL.
# MAGIC
# MAGIC Explorémoslas también desde código:

# COMMAND ----------

# DBTITLE 1,SHOW TABLES
# MAGIC %sql
# MAGIC -- Ver todas las tablas en tu schema
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 1,Explorar datos de clientes
# MAGIC %sql
# MAGIC -- Muestra de clientes del pipeline CDC (Lección 2)
# MAGIC -- Estos campos son datos PII que querremos proteger
# MAGIC SELECT customer_id, name, email, address, city, state
# MAGIC FROM silver.customers
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,Crear tabla de demo
# MAGIC %md
# MAGIC ### Crear tabla de demo para gobernanza
# MAGIC
# MAGIC Las streaming tables del pipeline son gestionadas por Lakeflow. Para demostrar column masking y row filters sin interferir con el pipeline, crearemos una copia de trabajo independiente:

# COMMAND ----------

# DBTITLE 1,Crear customers_demo
# MAGIC %sql
# MAGIC -- Tabla Delta normal para la demo de gobernanza
# MAGIC -- (separada de las streaming tables del pipeline)
# MAGIC CREATE OR REPLACE TABLE customers_demo AS
# MAGIC SELECT customer_id, name, email, address, city, state, zip_code
# MAGIC FROM silver.customers;
# MAGIC
# MAGIC SELECT COUNT(*) AS total_clientes FROM customers_demo;

# COMMAND ----------

# DBTITLE 1,B. Control de acceso (GRANT)
# MAGIC %md
# MAGIC ## B. Control de acceso granular (GRANT)
# MAGIC
# MAGIC Unity Catalog permite otorgar permisos en cualquier nivel de la jerarquía:
# MAGIC
# MAGIC ```sql
# MAGIC -- A nivel de catálogo
# MAGIC GRANT USE CATALOG ON CATALOG academia TO `equipo@empresa.com`;
# MAGIC
# MAGIC -- A nivel de schema
# MAGIC GRANT SELECT ON SCHEMA mi_schema TO `analistas@empresa.com`;
# MAGIC
# MAGIC -- A nivel de tabla
# MAGIC GRANT SELECT ON TABLE customers_demo TO `reportes@empresa.com`;
# MAGIC ```
# MAGIC
# MAGIC > 💡 En producción usarías grupos de cuenta en lugar de emails individuales. El administrador crea los grupos y asigna miembros desde la consola de la cuenta de Databricks.

# COMMAND ----------

# DBTITLE 1,SHOW GRANTS sobre la tabla
# MAGIC %sql
# MAGIC -- Ver permisos actuales sobre la tabla de demo
# MAGIC SHOW GRANTS ON TABLE customers_demo;

# COMMAND ----------

# DBTITLE 1,C. Enmascaramiento de columnas PII
# MAGIC %md
# MAGIC ## C. Enmascaramiento de columnas PII (Column Masking)
# MAGIC
# MAGIC `customers_demo` tiene columnas sensibles: `email` y `address`. Con **column masking**:
# MAGIC - Los **admins** ven el valor real
# MAGIC - El resto de usuarios ven `****` automáticamente
# MAGIC - La máscara es **transparente** — los usuarios no saben que existe más información
# MAGIC - No requiere cambios en las aplicaciones que consultan la tabla

# COMMAND ----------

# DBTITLE 1,Crear y aplicar máscara PII
# MAGIC %sql
# MAGIC -- Función de enmascaramiento:
# MAGIC -- admins ven el valor real · todos los demás ven '****'
# MAGIC CREATE OR REPLACE FUNCTION mask_pii(column_value STRING)
# MAGIC RETURN IF(is_account_group_member('admins'), column_value, '****');
# MAGIC
# MAGIC -- Aplicar la máscara a las columnas PII
# MAGIC ALTER TABLE customers_demo ALTER COLUMN email   SET MASK mask_pii;
# MAGIC ALTER TABLE customers_demo ALTER COLUMN address SET MASK mask_pii;
# MAGIC
# MAGIC SELECT 'Máscaras aplicadas a email y address ✓' AS resultado;

# COMMAND ----------

# DBTITLE 1,Consultar tabla con máscara aplicada
# MAGIC %sql
# MAGIC -- Consultar la tabla — los no-admins verán '****' en email y address
# MAGIC SELECT customer_id, name, email, address, city, state
# MAGIC FROM customers_demo
# MAGIC LIMIT 5;

# COMMAND ----------

# DBTITLE 1,D. Filtrado a nivel de fila
# MAGIC %md
# MAGIC ## D. Filtrado a nivel de fila (Row-Level Security)
# MAGIC
# MAGIC Además de columnas, podemos restringir **qué filas** ve cada usuario:
# MAGIC - Los **admins** ven todos los registros
# MAGIC - Los demás usuarios solo ven un subconjunto (p.ej. su región asignada)
# MAGIC
# MAGIC Este patrón es clave para **multi-tenancy** y cumplimiento regulatorio (GDPR, HIPAA).

# COMMAND ----------

# DBTITLE 1,Crear y aplicar filtro de filas
# MAGIC %sql
# MAGIC -- Filtro de filas:
# MAGIC -- admins ven todos los estados · otros usuarios solo ven clientes de California (CA)
# MAGIC CREATE OR REPLACE FUNCTION customers_state_filter(customer_state STRING)
# MAGIC RETURN is_account_group_member('admins') OR customer_state = 'CA';
# MAGIC
# MAGIC -- Aplicar el filtro a la tabla
# MAGIC ALTER TABLE customers_demo SET ROW FILTER customers_state_filter ON (state);
# MAGIC
# MAGIC SELECT 'Filtro de filas aplicado ✓' AS resultado;

# COMMAND ----------

# DBTITLE 1,Verificar efecto del filtro de filas
# MAGIC %sql
# MAGIC -- Consultar — usuarios no-admin solo verán clientes del estado 'CA'
# MAGIC SELECT state, COUNT(*) AS total_clientes
# MAGIC FROM customers_demo
# MAGIC GROUP BY state
# MAGIC ORDER BY total_clientes DESC;

# COMMAND ----------

# DBTITLE 1,E. Linaje, Auditoría y Cierre
# MAGIC %md
# MAGIC ## E. Linaje y Auditoría
# MAGIC
# MAGIC ### Linaje automático
# MAGIC
# MAGIC Unity Catalog rastrea automáticamente las dependencias entre tablas. Para verlo:
# MAGIC 1. En el menú lateral, ve a **Catalog** → busca `customers_demo`
# MAGIC 2. Haz clic en la pestaña **Lineage**
# MAGIC 3. Verás la cadena: `raw/customers (JSON)` → `bronze.customers_raw` → `bronze.customers_clean` → `silver.customers` → `customers_demo`
# MAGIC
# MAGIC ### Registro de auditoría
# MAGIC
# MAGIC Cada acceso queda registrado automáticamente. Desde los logs de UC puedes ver quién consultó qué tabla, qué queries se ejecutaron y los intentos de acceso denegados.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Resumen
# MAGIC
# MAGIC | Herramienta | Protección | Cómo |
# MAGIC |---|---|---|
# MAGIC | `GRANT` | Acceso a nivel de objeto | Permite/niega lectura y escritura |
# MAGIC | Column Mask | Columnas PII | Oculta valores sensibles automáticamente |
# MAGIC | Row Filter | Subconjuntos de datos | Restringe filas por condición |
# MAGIC | Lineage | Trazabilidad | Rastrea el origen y uso de los datos |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ✅ **Cierre de la Lección 4** — tus datos están gobernados con Unity Catalog.  
# MAGIC ➡️ **Siguiente:** `05 - BI Dashboard` — construir un panel de analítica sobre las tablas Gold.
