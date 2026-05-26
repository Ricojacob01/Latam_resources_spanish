# Databricks notebook source
# MAGIC %md
# MAGIC # Taller: Crear un espacio Genie y probar consultas (Parte 2)
# MAGIC
# MAGIC Objetivos:
# MAGIC - Crear un espacio Genie desde la UI (recomendado) y alternativa por código (opcional).
# MAGIC - Conectar Genie a la tabla de inventario creada en la Parte 1.
# MAGIC - Definir instrucciones (indicaciones) para que Genie responda en español y con contexto.
# MAGIC - Probar preguntas y solicitar visualizaciones.
# MAGIC
# MAGIC Prerrequisito: Haber creado el catálogo/esquema/tabla `inventario_insumos_oficina` en la Parte 1.
# MAGIC
# MAGIC

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `workshop_databricks`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta celda valida acceso y crea tu schema si no existe.

# COMMAND ----------

CATALOG = catalog = CATALOGO = "workshop_databricks"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = db = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {CATALOG}.{SCHEMA}")
spark.conf.set("c.catalog", CATALOG)
spark.conf.set("c.schema", SCHEMA)

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"User:    {_user}")


# Configuración rápida (usa el mismo catálogo/esquema/tabla de la Parte 1)

# (replaced by setup cell)
# (replaced by setup cell)
TABLA = "inventario_insumos_oficina"

spark.sql(f"USE CATALOG `{CATALOGO}`")
spark.sql(f"USE `{CATALOGO}`.`{ESQUEMA}`")
print(f"Contexto: {CATALOGO}.{ESQUEMA}.{TABLA}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## A. Crear Genie desde la UI (recomendado)
# MAGIC 1. Ve a la vista SQL (DBSQL) en el menú superior.
# MAGIC 2. Abre “Genie” (o Assistant) en la barra lateral.
# MAGIC 3. Crea un nuevo espacio Genie y asígnale un nombre (ej.: “Genie Inventario Oficina - Apellido).
# MAGIC 4. Fuente de datos: selecciona tu catálogo/esquema y elige la tabla `inventario_insumos_oficina`.
# MAGIC 5. Instrucciones (System prompt) sugeridas:
# MAGIC    - “Responde en español.”
# MAGIC    - “Cuando cites datos, usa los campos y sus descripciones tal como están en la tabla.”
# MAGIC    - “Si la pregunta es ambigua, solicita aclaraciones y sugiere filtros como fecha o categoría.”
# MAGIC    - “Si corresponde, sugiere visualizaciones (barras/series) y limita resultados.
# MAGIC 6. Guarda y prueba el asistente.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## B. (Opcional) Crear/actualizar Genie por código (REST API)
# MAGIC Nota: La API puede variar según la versión. Este ejemplo ilustra el patrón general.
# MAGIC
# MAGIC 1) Crea un token PAT y ten a mano tu `host` del workspace.
# MAGIC 2) Usa la API de Genie/Assistant (cuando esté disponible) o los endpoints de DBSQL para espacios.
# MAGIC
# MAGIC Ejemplo (boceto):
# MAGIC ```python
# MAGIC import requests, json, os
# MAGIC
# MAGIC host = os.environ.get("DATABRICKS_HOST", "https://<tu-workspace>")
# MAGIC token = os.environ.get("DATABRICKS_TOKEN", "<PAT>")
# MAGIC
# MAGIC headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
# MAGIC
# MAGIC payload = {
# MAGIC   "name": "Genie Inventario Oficina",
# MAGIC   "instructions": "Responde en español, usa la tabla inventario_insumos_oficina.",
# MAGIC   "data_sources": [{
# MAGIC     "catalog": "databricks_workshop_apellido",
# MAGIC     "schema": "ws_<usuario>",
# MAGIC     "tables": ["inventario_insumos_oficina"]
# MAGIC   }]
# MAGIC }
# MAGIC
# MAGIC # Ejemplo de endpoint (ilustrativo; valida en tu workspace):
# MAGIC # resp = requests.post(f"{host}/api/2.0/genie/spaces", headers=headers, data=json.dumps(payload))
# MAGIC # print(resp.status_code, resp.text)
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## C. 5 preguntas de prueba para Genie
# MAGIC 1. ¿Cuál es el total de ítems disponibles en el inventario?
# MAGIC 2. ¿Cuáles son las 5 subcategorías con mayor stock actual? Muestra un gráfico de barras.
# MAGIC 3. ¿Cuál es el promedio de días de rotación por categoría? Muestra una tabla ordenada descendentemente.
# MAGIC 4. ¿Qué insumos están por debajo del stock mínimo? Devuélveme los 10 más críticos.
# MAGIC 5. ¿Puedes mostrar una serie temporal de las fechas de última compra (conteo por mes) y explicarme tendencias?
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## D. Explicación de JOINs con ejemplos
# MAGIC
# MAGIC Para enriquecer el inventario, crearemos una tabla pequeña de proveedores y mostraremos distintos JOINs:
# MAGIC - INNER JOIN: Solo filas que hacen match en ambas tablas.
# MAGIC - LEFT JOIN: Todas las filas de la izquierda (inventario) y las que empatan de proveedores (si existen).
# MAGIC - RIGHT/FULL JOIN: Complementan el LEFT para cubrir todos los casos.
# MAGIC - SEMI/ANTI JOIN: Filas que sí tienen match (SEMI) o que no tienen match (ANTI) sin duplicar columnas.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Crear tabla de proveedores de ejemplo
# MAGIC CREATE TABLE IF NOT EXISTS `proveedores_info` (
# MAGIC   proveedor STRING,
# MAGIC   pais STRING,
# MAGIC   sla_dias INT,
# MAGIC   rating DOUBLE
# MAGIC ) COMMENT 'Tabla de referencia de proveedores para ejemplos de JOIN';
# MAGIC
# MAGIC -- Poblar algunos registros (idempotente usando MERGE)
# MAGIC MERGE INTO `proveedores_info` AS target
# MAGIC USING (
# MAGIC   SELECT * FROM VALUES
# MAGIC     ('OfiMax','MX',7,4.5),
# MAGIC     ('Papelería Centro','AR',10,4.0),
# MAGIC     ('TechPlus','US',5,4.7),
# MAGIC     ('Distribuidora Sur','CL',9,4.1)
# MAGIC   AS t(proveedor, pais, sla_dias, rating)
# MAGIC ) AS source
# MAGIC ON target.proveedor = source.proveedor
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (proveedor, pais, sla_dias, rating)
# MAGIC   VALUES (source.proveedor, source.pais, source.sla_dias, source.rating);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Nota importante sobre JOINs en Genie
# MAGIC
# MAGIC Para que Genie pueda generar respuestas precisas que involucren combinaciones de tablas (JOINs), es fundamental que las instrucciones del espacio Genie incluyan explícitamente las relaciones entre tablas. Por ejemplo, se debe indicar que la tabla `inventario_insumos_oficina` se puede unir con `proveedores_info` usando la columna `proveedor`. Esto permite que Genie entienda cómo construir consultas SQL correctas y devuelva resultados enriquecidos.
# MAGIC
# MAGIC **Recomendación:**  
# MAGIC Edita las instrucciones del espacio Genie y añade algo como:
# MAGIC
# MAGIC - "La tabla `inventario_insumos_oficina` se puede unir con `proveedores_info` usando la columna `proveedor`."
# MAGIC - "Cuando se requiera información de proveedores, realiza un JOIN entre ambas tablas usando `proveedor` como clave."
# MAGIC
# MAGIC Esto mejora la calidad de las respuestas y habilita consultas más complejas en el asistente.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INNER JOIN: solo coincidencias
# MAGIC SELECT i.item_id, i.nombre, i.proveedor, p.pais, p.sla_dias, p.rating
# MAGIC FROM `inventario_insumos_oficina` i
# MAGIC INNER JOIN `proveedores_info` p
# MAGIC   ON i.proveedor = p.proveedor
# MAGIC LIMIT 20;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## E. Expresiones y consultas SQL útiles
# MAGIC Agrega esta query a la seccion SQL queries & fuctions bajo Instructions esto mejorara el resultado de tu genie: 
# MAGIC
# MAGIC - CASE WHEN para clasificar: p. ej., "stock bajo", "stock medio", "stock alto".
# MAGIC - Funciones de fecha: date_trunc, month, year.
# MAGIC - Ventanas (WINDOW): rank/row_number por categoría/subcategoría.
# MAGIC - Agregaciones: sum, avg, count.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --What question does this anwer ?
# MAGIC --¿Cuál es el nivel de stock (bajo/medio/alto) de cada artículo del inventario y cuál es su posición (ranking) dentro de su categoría según su stock disponible? 
# MAGIC --Usage Guidence
# MAGIC -- Clasificar nivel de stock y ranking dentro de categoría
# MAGIC
# MAGIC WITH base AS (
# MAGIC   SELECT 
# MAGIC     categoria,
# MAGIC     subcategoria,
# MAGIC     item_id,
# MAGIC     nombre,
# MAGIC     stock_actual,
# MAGIC     stock_minimo,
# MAGIC     CASE 
# MAGIC       WHEN stock_actual < stock_minimo THEN 'bajo'
# MAGIC       WHEN stock_actual <= stock_minimo * 1.5 THEN 'medio'
# MAGIC       ELSE 'alto'
# MAGIC     END AS nivel_stock,
# MAGIC     date_trunc('month', fecha_ultima_compra) AS mes_compra
# MAGIC   FROM `inventario_insumos_oficina`
# MAGIC )
# MAGIC SELECT 
# MAGIC   categoria,
# MAGIC   subcategoria,
# MAGIC   item_id,
# MAGIC   nombre,
# MAGIC   nivel_stock,
# MAGIC   stock_actual,
# MAGIC   stock_minimo,
# MAGIC   ROW_NUMBER() OVER (PARTITION BY categoria ORDER BY stock_actual DESC) AS rn_cat,
# MAGIC   COUNT(*) OVER (PARTITION BY categoria, subcategoria) AS items_subcat,
# MAGIC   mes_compra
# MAGIC FROM base
# MAGIC ORDER BY categoria, rn_cat
# MAGIC LIMIT 50;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## F. Benchmarks para probar Genie
# MAGIC Usa las siguientes preguntas de validación. Para cada una, verifica:
# MAGIC - Columnas devueltas y tipos razonables
# MAGIC - Filtros aplicados correctamente
# MAGIC - Ordenación/agrupación coherente
# MAGIC - Si pide gráfico, que lo sugiera/retorne
# MAGIC
# MAGIC Preguntas y criterios:
# MAGIC 1) "Total de ítems en inventario y promedio de stock por categoría".
# MAGIC    - Ground Truth SQL:
# MAGIC
# MAGIC    SELECT
# MAGIC    categoria,
# MAGIC    COUNT(item_id) AS total_items,
# MAGIC    AVG(stock_actual) AS promedio_stock_actual
# MAGIC    FROM
# MAGIC    workshop_databricks.ws_<usuario>.inventario_insumos_oficina
# MAGIC    WHERE
# MAGIC    categoria IS NOT NULL
# MAGIC    AND stock_actual IS NOT NULL
# MAGIC    GROUP BY
# MAGIC    categoria
# MAGIC    ORDER BY
# MAGIC    categoria;
# MAGIC
# MAGIC 2) "Top 10 ítems con stock por debajo del mínimo ordenados ascendente".
# MAGIC    - Esperado: [item_id, nombre, stock_actual, stock_minimo] orden ASC, LIMIT 10.
# MAGIC 3) "Stock total por proveedor con rating del proveedor, mostrar gráfico de barras".
# MAGIC    - Esperado: JOIN con proveedores_info; [proveedor, stock_total, rating].
# MAGIC 4) "Tendencia mensual de compras (conteo por mes) en el último año".
# MAGIC    - Esperado: date_trunc('month', fecha_ultima_compra), COUNT(*).
# MAGIC 5) "Subcategorías con mayor rotación (promedio días_rotacion)".
# MAGIC    - Esperado: [subcategoria, avg_dias], orden DESC.
# MAGIC 6) "Proveedores sin coincidencia en inventario (anti join)".
# MAGIC    - Esperado: proveedores presentes solo en proveedores_info.
# MAGIC 7) "Ítems por nivel de stock (bajo/medio/alto)".
# MAGIC    - Esperado: usa CASE WHEN similar al ejemplo anterior.
# MAGIC 8) "Ítems y ubicación (almacén/pasillo/estante/nivel) filtrados por categoría = 'Escritura'".
# MAGIC    - Esperado: aplica filtro y retorna columnas de ubicación.
# MAGIC
# MAGIC Sugerencia: Ejecuta cada pregunta y valida la respuesta; si difiere, pide a Genie que corrija filtros/agrupación/orden.
# MAGIC
# MAGIC Sugerencia: Escoge una pregunta con hard truth y una prejunta sin hard truth, al momento de provar el benchbanch deja el hardtruth en blanco y compara el resultado manualmente
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## G. Añadir descripción del espacio Genie usando la UI
# MAGIC 1. En la vista Genie, abre tu espacio creado.
# MAGIC 2. Edita la “Descripción”/“Instructions” para incluir:
# MAGIC    - Contexto: "Este asistente responde sobre el inventario de insumos de oficina."
# MAGIC    - Idioma: "Responde en español."
# MAGIC    - Estilo: "Da respuestas claras y concisas con tablas/resúmenes y sugiere gráficos cuando corresponda."
# MAGIC    - Alcance: "No inventes datos fuera de las tablas configuradas."
# MAGIC 3. Guarda los cambios y realiza una pregunta de prueba para validar que la descripción influye en la respuesta.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## H. Calificar respuestas y ver monitoreo
# MAGIC 1. En el panel de conversación de Genie, usa los controles de calificación (👍/👎) y añade comentarios sobre exactitud/claridad.
# MAGIC 2. Repite con varias preguntas (usa los Benchmarks) para generar señales de calidad.
# MAGIC 3. Ve a la sección de monitoreo/telemetría de DBSQL o del Assistant para revisar:
# MAGIC    - Tasa de éxito, tiempos de respuesta, errores.
# MAGIC    - Preguntas más frecuentes y calidad promedio.
# MAGIC 4. Opcional: define etiquetas (tags) o categorías en tus pruebas para comparar iteraciones.
# MAGIC
# MAGIC
