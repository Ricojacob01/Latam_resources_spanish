# Databricks notebook source
# DBTITLE 1,Introducción de la lección
# MAGIC %md
# MAGIC # Lección 1: Creación de Pipelines de Datos con Calidad de Datos
# MAGIC
# MAGIC ## Objetivos de aprendizaje
# MAGIC Al finalizar esta lección, podrás:
# MAGIC * Crear un Lakeflow Spark Declarative Pipeline usando el editor multi‑archivo
# MAGIC * Implementar arquitectura medallion (Bronze → Silver → Gold)
# MAGIC * Aplicar expectativas de calidad de datos con manejo correcto de violaciones
# MAGIC * Procesar datos incrementalmente con Auto Loader
# MAGIC * Monitorear la ejecución del pipeline y ver resultados
# MAGIC
# MAGIC ## Duración: ~50 minutos
# MAGIC
# MAGIC ## Prerrequisitos
# MAGIC * Haber ejecutado el cuaderno **01 - Setup**
# MAGIC
# MAGIC ## Modelo de nombres (importante)
# MAGIC * Catálogo compartido **`academia`** · esquema propio **`<tu_apellido>`**.
# MAGIC * La capa medallion se distingue por el **sufijo** de la tabla:
# MAGIC   `orders_bronze` → `orders_silver` → `order_summary_gold`.

# COMMAND ----------

# DBTITLE 1,Qué es Lakeflow Spark Declarative Pipelines
# MAGIC %md
# MAGIC ## ¿Qué es Lakeflow Spark Declarative Pipelines?
# MAGIC
# MAGIC **Lakeflow Spark Declarative Pipelines** (anteriormente Delta Live Tables) es un marco declarativo para construir pipelines de datos confiables, mantenibles y comprobables.
# MAGIC
# MAGIC ### Funcionalidades clave:
# MAGIC
# MAGIC * **Declarativo**: Define el qué, no el cómo
# MAGIC * **Gestión automática de dependencias**: El sistema determina el orden de ejecución
# MAGIC * **Calidad de datos integrada**: Las expectativas hacen cumplir la calidad en la ingesta
# MAGIC * **Procesamiento incremental**: Solo procesa automáticamente los datos nuevos
# MAGIC * **Organización multi‑archivo**: Organiza el código lógicamente en varios archivos
# MAGIC * **IDE integrado**: Entorno completo con DAG, vistas previas y monitoreo
# MAGIC
# MAGIC ### El Editor de Lakeflow Pipelines:
# MAGIC
# MAGIC A diferencia de los notebooks tradicionales, el nuevo **Lakeflow Pipelines Editor** ofrece:
# MAGIC * Edición multi‑archivo con pestañas
# MAGIC * Gráfico del pipeline (DAG) en vivo
# MAGIC * Vistas previas de datos en línea
# MAGIC * Monitoreo de rendimiento integrado
# MAGIC * Ejecución selectiva (ejecutar archivo, tabla o pipeline)

# COMMAND ----------

# DBTITLE 1,Arquitectura del pipeline
# MAGIC %md
# MAGIC ## A. Comprender la arquitectura del pipeline
# MAGIC
# MAGIC En esta lección construiremos un pipeline sencillo siguiendo la **arquitectura medallion**:
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
# MAGIC │   BRONZE    │     │   SILVER    │     │    GOLD     │
# MAGIC │             │────▶│             │────▶│             │
# MAGIC │   Raw Data  │     │ Clean Data  │     │ Aggregated  │
# MAGIC └─────────────┘     └─────────────┘     └─────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Nuestro pipeline:
# MAGIC
# MAGIC 1. **Bronze**: `orders_bronze`
# MAGIC    * Ingesta archivos JSON sin procesar desde el almacenamiento
# MAGIC    * Conserva todos los datos fuente
# MAGIC    * Agrega metadatos (hora de procesamiento, archivo de origen)
# MAGIC
# MAGIC 2. **Silver**: `orders_silver`
# MAGIC    * Analiza y valida tipos de datos
# MAGIC    * Aplica expectativas de calidad de datos
# MAGIC    * Selecciona columnas relevantes
# MAGIC
# MAGIC 3. **Gold**: `order_summary_gold`
# MAGIC    * Agregaciones de negocio
# MAGIC    * Resúmenes diarios de pedidos
# MAGIC    * Listo para analítica/reportes

# COMMAND ----------

# DBTITLE 1,Crear primer pipeline
# MAGIC %md
# MAGIC ## B. Crea tu primer pipeline
# MAGIC
# MAGIC Vamos a crear un pipeline usando el Lakeflow Pipelines Editor.
# MAGIC
# MAGIC ### Paso 1: Abrir el creador de pipelines
# MAGIC
# MAGIC 1. En la **barra lateral izquierda**, haz clic en **New** (botón azul arriba)
# MAGIC 2. Selecciona **ETL pipeline**
# MAGIC
# MAGIC ### Paso 2: Configurar ajustes básicos
# MAGIC
# MAGIC 1. **Nombre del pipeline**: `SDP Workshop - [tu-usuario]`
# MAGIC    * Ejemplo: `SDP Workshop - john_doe`
# MAGIC
# MAGIC 2. **Catálogo predeterminado (Default catalog)**: `academia` (compartido)
# MAGIC
# MAGIC 3. **Esquema predeterminado (Default schema)**: tu esquema `<tu_apellido>`
# MAGIC    * Fue creado por el cuaderno de setup (p. ej. `john_doe`)
# MAGIC
# MAGIC 4. **Opción de creación**: Selecciona **"Add existing assets"**
# MAGIC    * Pipeline root folder: selecciona la carpeta que importaste **Build Data Pipelines with Lakeflow Spark Declarative Pipeline**
# MAGIC    * Source code paths: selecciona **transformations**
# MAGIC
# MAGIC 5. Haz clic en **Add**
# MAGIC
# MAGIC Ahora deberías ver el **Lakeflow Pipelines Editor** con:
# MAGIC * Explorador de assets del pipeline a la izquierda
# MAGIC * Editor de código en el centro
# MAGIC * Gráfico del pipeline vacío a la derecha

# COMMAND ----------

# DBTITLE 1,Explorar interfaz del editor
# MAGIC %md
# MAGIC ## C. Explora la interfaz del editor de pipelines
# MAGIC
# MAGIC Antes de agregar código, entendamos la interfaz:
# MAGIC
# MAGIC ### Panel izquierdo: Explorador de assets
# MAGIC
# MAGIC * **Pestaña Pipeline**: Muestra archivos del proyecto
# MAGIC * **Pestaña All files**: Acceso a todo tu workspace
# MAGIC * **Icono Settings**: Configuración del pipeline
# MAGIC * **Icono Schedule**: Programar ejecuciones
# MAGIC * **Icono Share**: Gestionar permisos
# MAGIC
# MAGIC ### Panel central: Editor de código
# MAGIC
# MAGIC * Interfaz con pestañas para múltiples archivos
# MAGIC * Resaltado de sintaxis para SQL y Python
# MAGIC * Controles de ejecución en la barra de herramientas
# MAGIC
# MAGIC ### Panel derecho: Gráfico del pipeline (DAG)
# MAGIC
# MAGIC * Representación visual del flujo de datos
# MAGIC * Se actualiza en tiempo real durante la ejecución
# MAGIC * Haz clic en nodos para ver vista previa de datos
# MAGIC
# MAGIC ### Panel inferior: Detalles y monitoreo
# MAGIC
# MAGIC * **Tables**: Lista de tablas con métricas
# MAGIC * **Performance**: Perfiles de ejecución
# MAGIC * **Issues**: Errores y advertencias
# MAGIC * **Event Log**: Eventos de ejecución detallados

# COMMAND ----------

# DBTITLE 1,Configurar el pipeline
# MAGIC %md
# MAGIC ## D. Configurar el pipeline
# MAGIC
# MAGIC Antes de agregar código, configuremos el pipeline:
# MAGIC
# MAGIC ### Paso 1: Abrir Settings
# MAGIC
# MAGIC 1. Haz clic en el icono **Settings** en la barra lateral izquierda
# MAGIC 2. El panel de configuración se abre a la derecha
# MAGIC
# MAGIC ### Paso 2: Revisar ajustes generales
# MAGIC
# MAGIC * **Nombre del pipeline**: Debe coincidir con lo ingresado
# MAGIC * **Modo del pipeline**: **Triggered** (predeterminado)
# MAGIC * **Run as**: Tu usuario (no cambia sin permisos)
# MAGIC
# MAGIC ### Paso 3: Verificar assets de código
# MAGIC
# MAGIC * **Root folder**: Carpeta del proyecto del pipeline
# MAGIC * **Source code**: Debe estar vacío o mostrar el archivo por defecto
# MAGIC
# MAGIC ### Paso 4: Confirmar ubicación predeterminada
# MAGIC
# MAGIC * **Default catalog**: `academia`
# MAGIC * **Default schema**: tu esquema `<tu_apellido>`
# MAGIC * Determinan dónde se crean tablas si no se especifica
# MAGIC
# MAGIC ### Paso 5: Configurar cómputo
# MAGIC
# MAGIC 1. Ve a la sección **Compute**
# MAGIC 2. Asegúrate que **Serverless** esté seleccionado (recomendado)
# MAGIC 3. Si no está disponible, classic compute funciona pero tarda más en iniciar
# MAGIC
# MAGIC ### Paso 6: Agregar variable de configuración
# MAGIC
# MAGIC Esto es crítico: el SQL referencia la variable `${source}`:
# MAGIC
# MAGIC 1. En **Configuration**, haz clic en **Add configuration**
# MAGIC 2. **Key**: `source`
# MAGIC 3. **Value**: Tu ruta de volumen del setup
# MAGIC    * Formato: `/Volumes/academia/{tu_apellido}/raw`
# MAGIC    * Ejemplo: `/Volumes/academia/john_doe/raw`
# MAGIC    * Si no recuerdas: Revisa la salida de `01 - Setup`
# MAGIC 4. Haz clic en **Save**
# MAGIC
# MAGIC ### Paso 7: Guardar ajustes
# MAGIC
# MAGIC * Cierra el panel de configuración haciendo clic en el editor

# COMMAND ----------

# DBTITLE 1,Entender código del pipeline
# MAGIC %md
# MAGIC ## E. Entender el código del pipeline de pedidos
# MAGIC
# MAGIC Ve a la pestaña pipeline > transformations y abre `orders_pipeline.sql`
# MAGIC
# MAGIC Desglosemos lo que hace el código:
# MAGIC
# MAGIC ### Capa Bronze - Ingesta Raw
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_bronze
# MAGIC   COMMENT "Datos de pedidos sin procesar ingeridos desde archivos JSON"
# MAGIC   TBLPROPERTIES ("pipelines.reset.allowed" = false)
# MAGIC AS 
# MAGIC SELECT *, current_timestamp() AS processing_time, ...
# MAGIC FROM STREAM read_files("${source}/orders", format => 'json');
# MAGIC ```
# MAGIC
# MAGIC **Conceptos clave**:
# MAGIC * `CREATE OR REFRESH STREAMING TABLE`: Define una tabla actualizada incrementalmente
# MAGIC * `STREAM read_files()`: Auto Loader - procesa archivos nuevos incrementalmente
# MAGIC * `${source}`: Sustitución de variable desde la configuración
# MAGIC * `pipelines.reset.allowed = false`: Previene refrescos completos accidentales
# MAGIC * El checkpoint se gestiona automáticamente
# MAGIC * Hereda catálogo y esquema por defecto o escribe con nombre totalmente calificado
# MAGIC
# MAGIC ### Capa Silver - Datos limpios
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE orders_silver
# MAGIC   (
# MAGIC     CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC     CONSTRAINT valid_timestamp EXPECT (order_timestamp > "2020-01-01")
# MAGIC   )
# MAGIC AS SELECT ... FROM STREAM orders_bronze;
# MAGIC ```
# MAGIC
# MAGIC **Conceptos clave**:
# MAGIC * **Expectations**: Reglas de calidad de datos
# MAGIC * `ON VIOLATION FAIL UPDATE`: Detiene el pipeline si falla
# MAGIC * `FROM STREAM`: Lee incrementalmente desde bronze
# MAGIC
# MAGIC ### Capa Gold - Lógica de negocio
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW order_summary_gold
# MAGIC AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
# MAGIC FROM orders_silver
# MAGIC GROUP BY date(order_timestamp);
# MAGIC ```
# MAGIC
# MAGIC **Conceptos clave**:
# MAGIC * `MATERIALIZED VIEW`: Resultados persistidos
# MAGIC * Sin `STREAM`: Lee todos los datos de la fuente
# MAGIC * Optimiza refresco incremental cuando es posible
# MAGIC * Ideal para agregaciones y analítica

# COMMAND ----------

# DBTITLE 1,Validar con dry run
# MAGIC %md
# MAGIC ## F. Validar el pipeline (Dry Run)
# MAGIC
# MAGIC Antes de ejecutar el pipeline, validémoslo:
# MAGIC
# MAGIC ### Paso 1: Ejecutar un Dry Run
# MAGIC
# MAGIC 1. En la **barra de herramientas** superior, haz clic en **Dry run**
# MAGIC 2. Espera 1‑2 minutos para completar la validación
# MAGIC 3. Observa el panel izquierdo; mostrará modo "DRY RUN"
# MAGIC
# MAGIC ### Paso 2: Revisar resultados
# MAGIC
# MAGIC Tras completar:
# MAGIC
# MAGIC 1. **Pipeline Graph** (panel derecho):
# MAGIC    * Debe mostrar 3 nodos: `orders_bronze` → `orders_silver` → `order_summary_gold`
# MAGIC    * Flechas muestran el flujo
# MAGIC    * No deben aparecer errores
# MAGIC
# MAGIC 2. **Tables** (panel inferior):
# MAGIC    * Lista las 3 tablas
# MAGIC    * Muestra catálogo, esquema y tipo
# MAGIC    * Estado "Validated"
# MAGIC
# MAGIC 3. **Issues** (panel inferior):
# MAGIC    * Debe estar vacío
# MAGIC    * Si hay errores, revisa tu configuración
# MAGIC
# MAGIC ### Solución de problemas (Dry Run)
# MAGIC
# MAGIC **Error: "Variable 'source' not found"**
# MAGIC * Vuelve a Settings → Configuration
# MAGIC * Verifica que `source` esté configurado correctamente
# MAGIC
# MAGIC **Error: "Schema not found"**
# MAGIC * Asegúrate de que 0-SETUP.py se ejecutó correctamente
# MAGIC * Verifica Default Catalog en Settings
# MAGIC
# MAGIC **Error: "Permission denied"**
# MAGIC * Verifica privilegios CREATE en el catálogo
# MAGIC * Contacta al admin si es necesario

# COMMAND ----------

# DBTITLE 1,Ejecutar una tabla
# MAGIC %md
# MAGIC ## G. Ejecuta una tabla a la vez
# MAGIC
# MAGIC El nuevo IDE facilita construir y probar iterativamente. En lugar de ejecutar todo el pipeline, puedes ejecutar solo una tabla.
# MAGIC
# MAGIC 1. Haz clic en `dataset actions` (▶️) arriba de `CREATE OR REFRESH STREAMING TABLE orders_bronze`
# MAGIC 2. Selecciona **Run table** `academia.{tu_apellido}.orders_bronze`
# MAGIC 3. En el Pipeline Graph verás solo la tabla de pedidos ejecutada
# MAGIC
# MAGIC Tras completar, deberías ver:
# MAGIC
# MAGIC * **orders_bronze**: 174 filas
# MAGIC   * Se procesó un archivo JSON (00.json)
# MAGIC   * Todos los datos raw preservados

# COMMAND ----------

# DBTITLE 1,Ejecutar el pipeline completo
# MAGIC %md
# MAGIC ## H. Ejecutar el pipeline
# MAGIC
# MAGIC ¡Ahora ejecutemos el pipeline completo!
# MAGIC
# MAGIC ### Paso 1: Iniciar el pipeline
# MAGIC
# MAGIC 1. En la **barra de herramientas**, haz clic en **Run pipeline**
# MAGIC 2. El pipeline comenzará a ejecutarse
# MAGIC 3. Verás el estado cambiar en el panel izquierdo
# MAGIC
# MAGIC ### Paso 2: Observar la ejecución (2‑3 minutos)
# MAGIC
# MAGIC Observa el **Pipeline Graph** mientras corre:
# MAGIC
# MAGIC 1. **Starting**: Nodos grises/azules
# MAGIC 2. **Running**: Nodos amarillos con spinner
# MAGIC 3. **Complete**: Nodos verdes con ✔
# MAGIC 4. **Row counts**: Números en los bordes
# MAGIC
# MAGIC ### Paso 3: Resultados esperados (primer run)
# MAGIC
# MAGIC * **orders_silver**: 174 filas
# MAGIC   * Mismo conteo (validación pasó)
# MAGIC   * Se cumplieron los constraints
# MAGIC
# MAGIC * **order_summary_gold**: ~30 filas
# MAGIC   * Agregado por fecha
# MAGIC   * ~30 fechas únicas en los datos

# COMMAND ----------

# DBTITLE 1,Explorar resultados del pipeline
# MAGIC %md
# MAGIC ## I. Explora los resultados del pipeline
# MAGIC
# MAGIC Veamos qué creó el pipeline:
# MAGIC
# MAGIC ### Paso 1: Calidad de datos
# MAGIC
# MAGIC 1. Clic en **orders_silver**
# MAGIC 2. Busca la sección **Expectations**
# MAGIC 3. Deberías ver:
# MAGIC    * **valid_order_id**: 174 cumplidas (0 violaciones)
# MAGIC    * **valid_timestamp**: 174 cumplidas (0 violaciones)
# MAGIC    * **valid_customer_id**: 174 cumplidas (0 violaciones)
# MAGIC
# MAGIC ### Paso 2: Ver datos agregados
# MAGIC
# MAGIC 1. Clic en **order_summary_gold**
# MAGIC 2. En la pestaña **Data** verás:
# MAGIC    * `order_date`: La fecha
# MAGIC    * `total_daily_orders`: Conteo por fecha
# MAGIC    * `unique_customers`: Clientes únicos por fecha
# MAGIC
# MAGIC ### Paso 3: Revisar rendimiento
# MAGIC
# MAGIC 1. Clic en **Performance** en el panel inferior
# MAGIC 2. Revisa métricas:
# MAGIC    * Duración total: ~2‑3 minutos (primer run)
# MAGIC    * Tiempo por tabla
# MAGIC    * Uso de recursos
# MAGIC 3. Clic en cualquier tabla para ver profile

# COMMAND ----------

# DBTITLE 1,Procesamiento incremental
# MAGIC %md
# MAGIC ## J. Procesamiento incremental
# MAGIC
# MAGIC Uno de los principales beneficios es el **procesamiento incremental automático**.
# MAGIC
# MAGIC ### Ejecutar de nuevo
# MAGIC
# MAGIC 1. Haz clic en **Run pipeline** nuevamente
# MAGIC 2. Espera (será rápido)
# MAGIC 3. Observa:
# MAGIC    * **0 filas nuevas** en todas las tablas
# MAGIC    * Ejecución más rápida
# MAGIC
# MAGIC **¿Por qué?** Auto Loader rastrea qué archivos ya fueron procesados.
# MAGIC
# MAGIC ### Agregar datos nuevos
# MAGIC
# MAGIC Agreguemos un archivo nuevo y veamos el procesamiento incremental:

# COMMAND ----------

# DBTITLE 1,Agregar archivo de pedidos
import sys, os, re

# Agregar la carpeta _recursos al path para importar utils
recursos_path = '/Workspace/Users/rico.martinez@databricks.com/Workshop/Mineros/_recursos'
sys.path.insert(0, recursos_path)

# Importar helper
from utils import add_orders_file

# Info de usuario actual
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0]

# Limpiar username para nombres (remover caracteres especiales)
clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())

working_dir = f'/Volumes/classic_stable_paco_catalog/{clean_username}/raw'

result = add_orders_file(spark, working_dir, file_number=1, num_orders=25)
print(result)

# COMMAND ----------

# DBTITLE 1,Ejecutar con datos nuevos
# MAGIC %md
# MAGIC ### Ejecutar pipeline con datos nuevos
# MAGIC
# MAGIC 1. Vuelve a la pestaña del **pipeline editor**
# MAGIC 2. Haz clic en **Run pipeline** de nuevo
# MAGIC 3. Observa el gráfico
# MAGIC 4. Observa:
# MAGIC    * **+25 filas** en bronze y silver
# MAGIC    * La capa gold se recomputa eficientemente
# MAGIC    * Solo se leyó el archivo NUEVO (01.json)
# MAGIC
# MAGIC **¡Esto es procesamiento incremental!** El pipeline:
# MAGIC * Detecta nuevos archivos automáticamente
# MAGIC * Procesa solo datos nuevos
# MAGIC * Actualiza tablas downstream
# MAGIC * Sin intervención manual

# COMMAND ----------

# DBTITLE 1,Consultar tablas del pipeline
# MAGIC %md
# MAGIC ## K. Consulta las tablas de tu pipeline
# MAGIC
# MAGIC Las tablas creadas están en Unity Catalog. ¡Puedes consultarlas como cualquier otra!

# COMMAND ----------

# DBTITLE 1,Establecer catálogo y esquema
import re

# Establecer catálogo para consultas SQL en este notebook
current_user = spark.sql("SELECT current_user()").collect()[0][0]
username = current_user.split("@")[0]
clean_username = re.sub(r'[^a-z0-9]', '_', username.lower())

# Catálogo compartido + tu esquema como contexto por defecto
spark.sql("USE CATALOG academia")
spark.sql(f"USE SCHEMA {clean_username}")
print(f"✓ Contexto: academia.{clean_username}")
print("  Todas las consultas SQL sin prefijo usarán este esquema automáticamente")

# COMMAND ----------

# DBTITLE 1,Consultar tabla bronze
# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Consultar tabla silver
# MAGIC %sql
# MAGIC SELECT order_id, order_timestamp, customer_id
# MAGIC FROM orders_silver
# MAGIC ORDER BY order_timestamp DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# DBTITLE 1,Consultar agregación gold
# MAGIC %sql
# MAGIC SELECT * FROM order_summary_gold
# MAGIC ORDER BY order_date;

# COMMAND ----------

# DBTITLE 1,Verificar conteos totales
# MAGIC %sql
# MAGIC SELECT
# MAGIC   'orders_bronze' AS table_name, COUNT(*) AS row_count FROM orders_bronze
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'orders_silver' AS table_name, COUNT(*) AS row_count FROM orders_silver
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   'order_summary_gold' AS table_name, COUNT(*) AS row_count FROM order_summary_gold;

# COMMAND ----------

# DBTITLE 1,Puntos clave de la lección
# MAGIC %md
# MAGIC ## L. Puntos clave - Lección 1
# MAGIC
# MAGIC * **Lakeflow Spark Declarative Pipelines** provee un marco declarativo para ETL
# MAGIC * **Editor multi‑archivo** ofrece experiencia tipo IDE con monitoreo integrado
# MAGIC * **Streaming tables** manejan procesamiento incremental automáticamente
# MAGIC * **Materialized views** proveen agregaciones eficientes
# MAGIC * **Expectativas de calidad** hacen cumplir estándares en la ingesta
# MAGIC * **Auto Loader** (`read_files`) simplifica la ingesta con checkpoints
# MAGIC * **Arquitectura medallion** organiza datos en Bronze → Silver → Gold
# MAGIC
# MAGIC ## ¿Qué sigue?
# MAGIC
# MAGIC En la **Lección 2**, ampliaremos estos conceptos para crear pipelines de producción con:
# MAGIC * Múltiples archivos de código
# MAGIC * Dependencias entre archivos
# MAGIC * Uniones entre tablas en streaming
# MAGIC * Programación y monitoreo
# MAGIC * Change Data Capture (CDC)
