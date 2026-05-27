# Databricks notebook source
# MAGIC %md
# MAGIC # Lección 2: Change Data Capture y Pipelines de Producción
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC Al finalizar esta lección, podrás:
# MAGIC - Implementar Change Data Capture (CDC) usando AUTO CDC INTO
# MAGIC - Entender SCD Tipo 1 (slowly changing dimensions)
# MAGIC - Manejar operaciones INSERT, UPDATE y DELETE automáticamente
# MAGIC - Agregar nuevas fuentes de datos a un pipeline existente
# MAGIC - Programar pipelines para producción
# MAGIC - Aplicar mejores prácticas de producción
# MAGIC
# MAGIC ## Duración: ~60 minutos
# MAGIC
# MAGIC ## Prerrequisitos
# MAGIC - Completar la Lección 1: Building Pipelines with Data Quality
# MAGIC - Tener listo tu pipeline de la Lección 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Qué es Change Data Capture (CDC)?
# MAGIC
# MAGIC **Change Data Capture** es un patrón para rastrear cambios en los datos a lo largo del tiempo. En lugar de reemplazar tablas completas, CDC captura cambios individuales (INSERT, UPDATE, DELETE) y los aplica incrementalmente.
# MAGIC
# MAGIC ### Casos de uso reales:
# MAGIC
# MAGIC - **Datos maestros de clientes**: Cambios de dirección, email, estado
# MAGIC - **Catálogo de productos**: Detalles, precios, disponibilidad
# MAGIC - **Registros de empleados**: Mantener datos de RR.HH. actualizados
# MAGIC - **Inventario**: Niveles de stock en tiempo real con cambios
# MAGIC
# MAGIC ### Enfoque tradicional (MERGE manual):
# MAGIC
# MAGIC ```sql
# MAGIC MERGE INTO target_table
# MAGIC USING source_stream
# MAGIC ON target_table.id = source_stream.id
# MAGIC WHEN MATCHED AND source_stream.operation = 'UPDATE' 
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND source_stream.operation = 'DELETE' 
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND source_stream.operation = 'INSERT' 
# MAGIC   THEN INSERT *
# MAGIC ```
# MAGIC
# MAGIC **Problemas:**
# MAGIC - SQL complejo
# MAGIC - Manejo manual de eventos fuera de orden
# MAGIC - Necesidad de desduplicación
# MAGIC - Requiere tuning de rendimiento
# MAGIC
# MAGIC ### Enfoque Lakeflow AUTO CDC:
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FLOW customers_cdc AS 
# MAGIC AUTO CDC INTO target_table
# MAGIC FROM STREAM source_table
# MAGIC   KEYS (customer_id)
# MAGIC   SEQUENCE BY timestamp
# MAGIC   STORED AS SCD TYPE 1
# MAGIC ```
# MAGIC
# MAGIC **Beneficios:**
# MAGIC - ✅ Declarativo: solo especifica lo que quieres
# MAGIC - ✅ Ordenamiento automático de eventos
# MAGIC - ✅ Desduplicación integrada
# MAGIC - ✅ Rendimiento optimizado
# MAGIC - ✅ Maneja datos que llegan tarde

# COMMAND ----------

# MAGIC %md
# MAGIC ## A. SCD Tipo 1 vs Tipo 2
# MAGIC
# MAGIC ### SCD Tipo 1: Solo estado actual
# MAGIC
# MAGIC Sobrescribe valores antiguos con nuevos. Solo se mantiene el estado actual.
# MAGIC
# MAGIC **Ejemplo:**
# MAGIC ```
# MAGIC Inserción inicial:
# MAGIC   customer_id='C001', name='John Doe', email='john@example.com'
# MAGIC
# MAGIC Tras actualización:
# MAGIC   customer_id='C001', name='John Smith', email='john.smith@example.com'
# MAGIC   (se pierden valores antiguos)
# MAGIC
# MAGIC Tras eliminación:
# MAGIC   (se elimina la fila)
# MAGIC ```
# MAGIC
# MAGIC **Usar cuando:**
# MAGIC - El histórico no importa
# MAGIC - El almacenamiento/rendimiento es crítico
# MAGIC - Solo se necesita el estado actual para analítica
# MAGIC - Ejemplo: Catálogo de productos, inventario actual
# MAGIC
# MAGIC ### SCD Tipo 2: Seguimiento histórico
# MAGIC
# MAGIC Conserva el histórico creando nuevas filas por cada cambio con timestamps inicio/fin.
# MAGIC
# MAGIC **Ejemplo:**
# MAGIC ```
# MAGIC Inserción inicial:
# MAGIC   customer_id='C001', name='John Doe', email='john@example.com'
# MAGIC   __START_AT='2024-01-01', __END_AT=NULL, __CURRENT=true
# MAGIC
# MAGIC Tras actualización:
# MAGIC   Fila 1 (vieja): __END_AT='2024-02-01', __CURRENT=false
# MAGIC   Fila 2 (nueva): name='John Smith', email='john.smith@example.com'
# MAGIC                  __START_AT='2024-02-01', __END_AT=NULL, __CURRENT=true
# MAGIC ```
# MAGIC
# MAGIC **En esta lección**: Usaremos SCD Tipo 1 por simplicidad

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. Revisar el código del pipeline de clientes (CDC)
# MAGIC
# MAGIC Antes de añadirlo a tu pipeline, entendamos `customers_pipeline.sql`.
# MAGIC
# MAGIC ### Paso 1: Abrir el archivo
# MAGIC
# MAGIC 1. En tu workspace, navega a **2 - CDC and Production**
# MAGIC 2. Abre **customers_pipeline.sql**
# MAGIC 3. Revisa la estructura
# MAGIC
# MAGIC ### Componentes clave:
# MAGIC
# MAGIC #### 1. Capa Bronze - Eventos CDC raw
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze.customers_raw
# MAGIC AS 
# MAGIC SELECT *, current_timestamp() AS processing_time
# MAGIC FROM STREAM read_files("${source}/customers", format => 'json');
# MAGIC ```
# MAGIC
# MAGIC - Ingiera eventos CDC desde archivos JSON
# MAGIC - Cada evento incluye: campos + `operation` + `timestamp`
# MAGIC - Operaciones: INSERT, UPDATE, DELETE
# MAGIC
# MAGIC #### 2. Capa Bronze - Validación de calidad
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze.customers_clean
# MAGIC   (
# MAGIC     CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC     CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC     CONSTRAINT valid_email EXPECT (rlike(email, '...') OR operation = 'DELETE')
# MAGIC   )
# MAGIC AS SELECT *, CAST(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
# MAGIC FROM STREAM bronze.customers_raw;
# MAGIC ```
# MAGIC
# MAGIC **Crítico:** ¡Siempre valida los datos CDC ANTES de aplicarlos!
# MAGIC
# MAGIC #### 3. Capa Silver - Tabla destino
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver.customers;
# MAGIC ```
# MAGIC
# MAGIC **Definición simple**: ¡AUTO CDC gestionará su contenido!
# MAGIC
# MAGIC #### 4. CREATE FLOW - La magia de CDC
# MAGIC
# MAGIC ```sql
# MAGIC CREATE FLOW customers_cdc AS 
# MAGIC AUTO CDC INTO silver.customers
# MAGIC FROM STREAM bronze.customers_clean
# MAGIC   KEYS (customer_id)                    -- Primary key
# MAGIC   APPLY AS DELETE WHEN operation = 'DELETE'
# MAGIC   SEQUENCE BY timestamp_datetime
# MAGIC   COLUMNS * EXCEPT (timestamp, operation, ...) 
# MAGIC   STORED AS SCD TYPE 1;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. Entender las cláusulas de AUTO CDC
# MAGIC
# MAGIC - **KEYS (customer_id)**: Define la clave primaria para hacer match  
# MAGIC - **SEQUENCE BY**: Orden en que se aplican los cambios  
# MAGIC - **APPLY AS DELETE WHEN**: Qué registros provocan eliminación  
# MAGIC - **COLUMNS * EXCEPT**: Columnas incluidas en la tabla destino  
# MAGIC - **STORED AS SCD TYPE 1**: Solo estado actual (sin histórico)

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Agregar el pipeline de clientes a tu pipeline existente
# MAGIC
# MAGIC ### Paso 1: Mover el archivo
# MAGIC
# MAGIC 1. En el explorador, ubica **Exercise_2/customers_pipeline.sql**
# MAGIC 2. **Arrastra y suelta** (o corta/pega) a **transformations/**
# MAGIC 3. Verifica que esté en la carpeta transformations
# MAGIC
# MAGIC ### Paso 2: Abrir tu pipeline
# MAGIC
# MAGIC 1. Regresa a tu pipeline de la Lección 1
# MAGIC 2. En la pestaña **Pipeline** a la izquierda
# MAGIC 3. Debes ver **dos archivos**:
# MAGIC    - orders_pipeline.sql
# MAGIC    - customers_pipeline.sql

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Ejecutar el pipeline con CDC
# MAGIC
# MAGIC ¡Veamos AUTO CDC en acción!
# MAGIC
# MAGIC ### Paso 1: Iniciar
# MAGIC
# MAGIC 1. Haz clic en **Run pipeline** en la barra
# MAGIC 2. Observa el gráfico mientras ejecuta
# MAGIC 3. Se procesarán pedidos y clientes
# MAGIC
# MAGIC ### Paso 2: Observar CDC
# MAGIC
# MAGIC - **bronze.customers_raw**: 27 registros (20 INSERT + 5 UPDATE + 2 DELETE)
# MAGIC - **bronze.customers_clean**: 27 registros (validados)
# MAGIC - **silver.customers**: **18 registros** (estado actual)
# MAGIC   - 20 inserts iniciales
# MAGIC   - 5 actualizaciones (mismo conteo, valores cambiados)
# MAGIC   - 2 eliminados
# MAGIC   - Final: 20 - 2 = 18 clientes
# MAGIC
# MAGIC ### Paso 3: Verificar resultados
# MAGIC
# MAGIC - Cliente 1: Email y dirección **actualizados**
# MAGIC - Cliente 3: **Eliminado**
# MAGIC - Cliente 10: Información **actualizada**
# MAGIC
# MAGIC ### Paso 4: Métricas de calidad de datos
# MAGIC
# MAGIC - En **bronze.customers_clean** > pestaña **Table metrics**
# MAGIC - Verifica que todos los constraints pasaron:
# MAGIC   - valid_id: 27
# MAGIC   - valid_operation: 27
# MAGIC   - valid_email: 25 (DELETE no requiere email)

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Consultar resultados de CDC
# MAGIC
# MAGIC Verifiquemos que las operaciones CDC funcionaron correctamente.

# COMMAND ----------

# DBTITLE 1,Establecer catálogo y esquema
# MAGIC %py
# MAGIC # Establecer catálogo para las consultas SQL en este notebook
# MAGIC import re
# MAGIC current_user = spark.sql("SELECT current_user()").collect()[0][0]
# MAGIC username = current_user.split("@")[0]
# MAGIC clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())
# MAGIC catalog_name = spark.sql("SELECT current_catalog()").collect()[0][0]
# MAGIC
# MAGIC # Usar como catálogo por defecto
# MAGIC spark.sql(f"USE CATALOG {catalog_name}")
# MAGIC print(f"✓ Usando catálogo: {catalog_name}")
# MAGIC print("  Todas las consultas SQL usarán este catálogo automáticamente")

# COMMAND ----------

# Create widget with the catalog name
dbutils.widgets.text("bronze", f"sdp_workshop_{clean_username}_bronze")
dbutils.widgets.text("silver", f"sdp_workshop_{clean_username}_silver")
dbutils.widgets.text("gold", f"sdp_workshop_{clean_username}_gold")

# COMMAND ----------

# Usar como catálogo por defecto
spark.sql(f"USE CATALOG {catalog_name}")
print(f"✓ Usando catálogo: {catalog_name}")
print("  Todas las consultas SQL usarán este catálogo automáticamente")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Ver estado actual final
# MAGIC SELECT * FROM $silver.customers ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar conteos
# MAGIC SELECT 
# MAGIC   'customers_raw' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM $bronze.customers_raw
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'customers_clean' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM $bronze.customers_clean
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'customers (current)' AS table_name, COUNT(*) AS row_count 
# MAGIC FROM $silver.customers;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clientes actualizados (con ciudad SF)
# MAGIC SELECT customer_id, name, email, city, state
# MAGIC FROM $silver.customers
# MAGIC WHERE city = 'San Francisco'
# MAGIC ORDER BY customer_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verificar que eliminados no existan
# MAGIC SELECT customer_id 
# MAGIC FROM $bronze.customers_raw
# MAGIC WHERE operation = 'DELETE'
# MAGIC EXCEPT
# MAGIC SELECT customer_id
# MAGIC FROM $silver.customers;
# MAGIC -- Debe devolver IDs eliminados (CUST0003, CUST0007)

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Programar el pipeline para producción
# MAGIC
# MAGIC Ahora que el pipeline maneja pedidos y CDC de clientes, ¡programémoslo para producción!
# MAGIC
# MAGIC ### Paso 1: Programar con Lakeflow Jobs
# MAGIC
# MAGIC 1. Selecciona el **icono Schedule** (🗓️) en la parte superior izquierda de la pestaña **Pipeline** o arriba a la derecha (entre **Settings** y **Share**)
# MAGIC 2. Selecciona **Add Schedule**
# MAGIC 3. Job Name: `Colsubsidio Workshop - {user_name}`
# MAGIC 4. Selecciona **Simple**
# MAGIC 5. Programa cada `1` `Day`
# MAGIC 5. Mantén `Performance Optimized` activado
# MAGIC 6. Selecciona **Create**
# MAGIC 7. Abre el enlace a tu nuevo job
# MAGIC
# MAGIC Notarás que estás en el canvas de Lakeflow Jobs. Todos los pipelines SDP se orquestan con Lakeflow Jobs y pueden combinarse con otros tipos de tareas, incluyendo notificaciones, reintentos y umbrales de métricas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## K. Puntos clave - Lección 2
# MAGIC
# MAGIC ✅ **AUTO CDC INTO** simplifica CDC con sintaxis declarativa  
# MAGIC ✅ **SCD Tipo 1** mantiene estado actual; Tipo 2 preserva histórico  
# MAGIC ✅ **KEYS** define la clave primaria para hacer match  
# MAGIC ✅ **SEQUENCE BY** asegura el orden correcto de eventos  
# MAGIC ✅ **APPLY AS DELETE WHEN** define la condición de borrado  
# MAGIC ✅ **Pipelines multi‑archivo** descubren/orquestan dependencias  
# MAGIC ✅ **Programación en producción** habilita procesamiento fiable  
# MAGIC ✅ **Expectativas de calidad** deben validarse ANTES de aplicar CDC  
# MAGIC ✅ **Monitoreo y alertas** son críticos en producción
# MAGIC
# MAGIC ## ¡Has construido un pipeline de producción!
# MAGIC
# MAGIC Ahora tienes un pipeline programado que:
# MAGIC - Ingierde pedidos incrementalmente con Auto Loader
# MAGIC - Aplica CDC para actualizaciones de clientes
# MAGIC - Hace cumplir calidad de datos en todas las capas
# MAGIC - Corre automáticamente según agenda
# MAGIC - Sigue mejores prácticas de producción

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fin del taller
# MAGIC
# MAGIC ¡Felicitaciones! Has completado el taller de Lakeflow Spark Declarative Pipelines.
# MAGIC
# MAGIC ### Lo que has aprendido:
# MAGIC
# MAGIC 1. ✅ Construir pipelines declarativos con arquitectura medallion
# MAGIC 2. ✅ Implementar calidad de datos con expectativas
# MAGIC 3. ✅ Usar Auto Loader para ingesta incremental
# MAGIC 4. ✅ Aplicar CDC con AUTO CDC INTO
# MAGIC 5. ✅ Gestionar proyectos multi‑archivo
# MAGIC 6. ✅ Programar pipelines para producción
# MAGIC 7. ✅ Mejores prácticas de producción
# MAGIC
# MAGIC ¡Gracias por participar! 🎉

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Opcional) Limpiar recursos del taller
# MAGIC
# MAGIC **IMPORTANTE:** Ejecuta esto solo si deseas eliminar TODOS los recursos del taller.
# MAGIC
# MAGIC Esto eliminará:
# MAGIC - Tu catálogo (sdp_workshop_<username>)
# MAGIC - Todos los esquemas (bronze, silver, gold)
# MAGIC - Todas las tablas y datos
# MAGIC - El volumen raw y archivos fuente
# MAGIC - Funciones UC (add_orders, add_status)
# MAGIC
# MAGIC **¡Esta acción no se puede deshacer!**

# COMMAND ----------

# Descomenta las líneas siguientes para limpiar todos los recursos del taller

# import re
# current_user = spark.sql("SELECT current_user()").collect()[0][0]
# username = current_user.split("@")[0]
# clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())
# catalog_name = f"sdp_workshop_{clean_username}"
#
# print(f"ADVERTENCIA: Se eliminará el catálogo: {catalog_name}")
# print("¡Esto removerá TODOS los datos, tablas y volúmenes del taller!")
# print("\nDescomenta la línea DROP CATALOG para continuar...")
#
# # Descomenta para eliminar realmente:
# # spark.sql(f"DROP CATALOG IF EXISTS {catalog_name} CASCADE")
# # print(f"✓ Catálogo eliminado: {catalog_name}")
# # print("✓ Limpieza del taller completada")


