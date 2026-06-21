# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `ardemo_classic_dnubtw_catalog`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta celda valida acceso y crea tu schema si no existe.

# COMMAND ----------

CATALOG = catalog = CATALOGO = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = db = schema = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE SCHEMA {SCHEMA}")
try:
    spark.conf.set("c.catalog", CATALOG)
    spark.conf.set("c.schema", SCHEMA)
except Exception:
    pass

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"User:    {_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Agent Bricks: Creando un Knowledge Assistant
# MAGIC
# MAGIC En este lab vas a crear un **Knowledge Assistant** — un agente que responde preguntas en lenguaje natural sobre un documento (un informe económico en PDF).
# MAGIC
# MAGIC ## Flujo del lab
# MAGIC
# MAGIC | Paso | Qué haces | Dónde |
# MAGIC | -- | -- | -- |
# MAGIC | 1 | Preparar datos: descargar PDF, parsearlo con `ai_parse_document`, escribir tabla Delta | Notebook (este) |
# MAGIC | 2 | Crear un **Vector Search Index** sobre la tabla | UI — Catalog Explorer |
# MAGIC | 3 | Crear el **Knowledge Assistant** con Agent Bricks | UI — Agents |
# MAGIC | 4 | Probar el agente en el **Playground** | UI — Playground |
# MAGIC
# MAGIC Tiempo estimado: 30–45 minutos.

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 1 — Preparar los datos
# MAGIC
# MAGIC Descargamos el informe **"Actualización de Perspectivas de la Economía Mundial"** (PDF), lo parseamos con `ai_parse_document`, y dejamos cada elemento del documento como una fila en una tabla Delta. Esa tabla será la fuente del Vector Search Index del Paso 2.

# COMMAND ----------

# Crear volumen para guardar el PDF
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.archivos")
print(f"Volume listo: /Volumes/{CATALOG}/{SCHEMA}/archivos")

# COMMAND ----------

# Descargar el PDF al volumen
volume = "archivos"
file_name = "economia_mundial.pdf"
table_name = "economia_mundial_pdf"

download_url = "https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/data/economia_mundial.pdf"
path_volume = f"/Volumes/{CATALOG}/{SCHEMA}/{volume}"
full_file_path = f"{path_volume}/{file_name}"

dbutils.fs.cp(download_url, full_file_path)
print(f"PDF descargado en: {full_file_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## # Parsear el PDF con ai_parse_document — extrae elementos estructurados (texto, tablas, etc.)
# MAGIC

# COMMAND ----------

# Parsear el PDF con ai_parse_document — extrae elementos estructurados (texto, tablas, etc.)
from pyspark.sql.functions import expr

df = (spark.read.format("binaryFile").load(full_file_path)
      .withColumn("parsed", expr("CAST(ai_parse_document(content, MAP('version', '2.0')) AS STRING)")))

display(df)

# COMMAND ----------

# Extraer el JSON `document.elements` (una lista de bloques con el contenido del PDF)
from pyspark.sql.functions import col, get_json_object

df_text = df.select(
    col("path"),
    get_json_object(col("parsed"), "$.document.elements").alias("elements"))

display(df_text)

# COMMAND ----------

# Explotar la lista en múltiples filas — una por elemento del documento
from pyspark.sql.functions import explode
from pyspark.sql.types import ArrayType, StringType

# Convertir VARIANT a array de strings
df_text2 = df_text.withColumn(
    "elements_array",
    from_json(col("elements").cast("string"), ArrayType(StringType()))
)

# Explotar
df_text3 = df_text2.select("path", explode(col("elements_array")).alias("element"))
display(df_text3)

# COMMAND ----------

# Añadir id (clave primaria requerida para el Vector Search Index)
from pyspark.sql.functions import monotonically_increasing_id

df_final = df_text3.withColumn("id", monotonically_increasing_id())
display(df_final)

# COMMAND ----------

# Escribir como tabla Delta en TU schema personal
# IMPORTANTE: la tabla debe tener Change Data Feed (CDF) habilitado para que
# Vector Search pueda sincronizarla. Lo hacemos con TBLPROPERTIES.
full_table_name = f"{CATALOG}.{SCHEMA}.{table_name}"

(df_final.write
   .mode("overwrite")
   .option("overwriteSchema", "true")
   .option("delta.enableChangeDataFeed", "true")
   .saveAsTable(full_table_name))

print(f"Tabla creada: {full_table_name}")
print(f"Filas: {spark.table(full_table_name).count()}")

# COMMAND ----------

# Link directo a tu tabla en Catalog Explorer — útil para el Paso 2
workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
table_link = f"https://{workspace_url}/explore/data/{CATALOG}/{SCHEMA}/{table_name}"
displayHTML(f"<a href='{table_link}' target='_blank'>Abrir tu tabla en Catalog Explorer →</a>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternativa: Parsear el PDF desde la UI (Agent Bricks → Information Extraction)
# MAGIC
# MAGIC Las celdas de arriba parsean el PDF en Python. Si prefieres no escribir código, **Agent Bricks → Information Extraction** te permite definir visualmente qué quieres extraer y deja que el agente lo haga sobre tu documento.
# MAGIC
# MAGIC Information Extraction está construido encima de la función SQL `ai_extract` (v2) con una UI que te ayuda a definir y optimizar el schema. A diferencia de `ai_parse_document` (que devuelve el PDF parseado tal cual), aquí defines **el shape exacto** de la tabla de salida.
# MAGIC
# MAGIC ### Paso A — Crear el agente y seleccionar la fuente
# MAGIC
# MAGIC 1. **Abre Agent Bricks**: sidebar izquierdo → **Agents**.
# MAGIC
# MAGIC 2. Click **Create Agent** → selecciona la tarjeta **Information Extraction**.
# MAGIC
# MAGIC 3. **Selecciona la fuente de datos** — tienes tres opciones:
# MAGIC    - **Upload files** — subir archivos directamente desde tu máquina
# MAGIC    - **Unity Catalog volume** — apuntar a un volumen con archivos soportados ← **úsalo para este lab**
# MAGIC    - **Table** — una tabla que ya contiene texto
# MAGIC
# MAGIC    Para este lab elige **Unity Catalog volume** y apunta a tu volumen:
# MAGIC    `/Volumes/ardemo_classic_dnubtw_catalog/ws_<tu_usuario>/archivos/`
# MAGIC
# MAGIC 4. Click **Create Agent**. Agent Bricks parsea el(los) documento(s) y te lleva a la pantalla de definición de schema.
# MAGIC
# MAGIC ### Paso B — Definir el schema
# MAGIC
# MAGIC En la pantalla del agente tienes **tres formas** de definir el schema. La más rápida es la primera:
# MAGIC
# MAGIC | Método | Cómo se usa |
# MAGIC | -- | -- |
# MAGIC | **Auto-generate** | Escribes en lenguaje natural qué quieres extraer y click **Generate Schema** — el agente propone los campos |
# MAGIC | **Add field** | Manual: click **Add field** y añades nombre + tipo + descripción de cada uno |
# MAGIC | **JSON editor** | Click **JSON** y pegas/editas el schema completo en JSON |
# MAGIC
# MAGIC Para este lab puedes usar **Auto-generate** con un prompt como:
# MAGIC > *"Extrae por cada sección del informe: el país o región mencionado, el tema (PIB, inflación, política monetaria, etc.), el año al que se refiere, la métrica con su valor numérico, los riesgos o políticas mencionadas, y la cita literal del párrafo."*
# MAGIC
# MAGIC O usa el schema sugerido de la siguiente celda con el método **Add field**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Paso C — Iterar sobre la calidad de la extracción
# MAGIC
# MAGIC La UI muestra a la **izquierda los documentos parseados** y a la **derecha los resultados de la extracción** sobre cada fila. Aquí es donde mejoras la calidad:
# MAGIC
# MAGIC 1. Revisa las extracciones — donde veas un campo mal extraído o vacío:
# MAGIC    - **Feedback en lenguaje natural** sobre una o varias filas, ej: *"Cuando el país no aparece explícito pero la sección habla de la zona euro, pon 'Zona Euro' en `pais`."*
# MAGIC    - O **edita manualmente la descripción** del campo en el schema
# MAGIC
# MAGIC 2. Click **Save and run extraction** — el agente auto-ajusta las descripciones según tu feedback y re-procesa los documentos.
# MAGIC
# MAGIC 3. Repite hasta que estés satisfecho. Cada iteración queda guardada en el menú **Versions** (puedes **Compare** o **Restore** una versión anterior).
# MAGIC
# MAGIC ### Paso D — Desplegar (botón "Use Agent")
# MAGIC
# MAGIC Cuando la calidad sea aceptable, click **Use Agent** (arriba a la derecha). **Aquí es donde se crea la tabla** — el agente por sí solo *no* persiste resultados, sólo los muestra en la UI de iteración. Hay dos formas de desplegar:
# MAGIC
# MAGIC | Opción | Qué hace | Cuándo usarla |
# MAGIC | -- | -- | -- |
# MAGIC | **Run in SQL** | Abre el SQL Editor con un query pre-armado usando `ai_extract` con tu schema. Tú decides dónde guardar el resultado (`CREATE TABLE ... AS SELECT ...`) | Ejecución única / one-shot. Lo más rápido para el lab |
# MAGIC | **Create a Spark Declarative Pipeline** | Genera un pipeline de Lakeflow que escribe en una **streaming table**. Puedes configurar un schedule para que procese nuevos documentos automáticamente | Cuando vas a recibir documentos nuevos continuamente (producción) |
# MAGIC
# MAGIC Para este lab usa **Run in SQL**:
# MAGIC
# MAGIC 1. Click **Use Agent** → **Run in SQL**.
# MAGIC 2. Se abre el SQL Editor con el query generado.
# MAGIC 3. Envuelve el query con `CREATE OR REPLACE TABLE economia_mundial_extraido AS ...` para persistir el resultado en tu schema personal.
# MAGIC 4. Click **Run** y verifica con `SELECT * FROM economia_mundial_extraido LIMIT 10`.
# MAGIC
# MAGIC ### Schema sugerido (si usas Add field)
# MAGIC
# MAGIC | Field name | Type | Description |
# MAGIC | -- | -- | -- |
# MAGIC | `pais` | string | País o región sobre la que habla esta sección (ej: "Brasil", "América Latina", "Zona Euro") |
# MAGIC | `tema` | string | Tema principal del párrafo (ej: "Proyección PIB", "Inflación", "Política monetaria", "Riesgo geopolítico") |
# MAGIC | `año` | integer | Año al que se refiere el dato o proyección (ej: 2026, 2027). Null si no aplica |
# MAGIC | `metrica` | string | Métrica mencionada con su unidad (ej: "Crecimiento PIB 2.3%", "Inflación 4.1%") |
# MAGIC | `riesgo_o_politica` | string | Riesgo, recomendación o política mencionada para ese país/tema |
# MAGIC | `cita_original` | string | Texto literal del informe del que se extrajo esta información (para referencia/auditoría) |
# MAGIC
# MAGIC ### Después de desplegar
# MAGIC
# MAGIC Una vez tienes la tabla `economia_mundial_extraido` en tu schema personal puedes:
# MAGIC
# MAGIC - **Inspeccionarla** desde Catalog Explorer o con `SELECT * FROM economia_mundial_extraido`
# MAGIC - **Usarla como fuente del Vector Search Index** del Paso 2 (alternativa más estructurada que `economia_mundial_pdf`)
# MAGIC - **Añadirla como segunda fuente de conocimiento** al Knowledge Assistant del Paso 3 — el agente conversacional puede combinar el PDF crudo + los metadatos por país
# MAGIC
# MAGIC > **Tip de optimización**: en la configuración del agente puedes elegir **Optimize for Scale** (default, throughput alto) u **Optimize for Complexity** (mejor para documentos largos como informes financieros — recomendado para este PDF). Documentación: [Information Extraction](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/info-extraction).

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 2 — Crear el Vector Search Index (desde la UI)
# MAGIC
# MAGIC Un **Vector Search Index** convierte cada fila de la tabla en un *embedding* (vector numérico) para poder hacer búsquedas semánticas. El Knowledge Assistant va a usar este índice para encontrar los pasajes relevantes del PDF cuando le hagas una pregunta.
# MAGIC
# MAGIC ## Pasos en la UI
# MAGIC
# MAGIC 1. **Abre la tabla** que acabas de crear: usa el link generado en la celda anterior, o navega manualmente en el sidebar:
# MAGIC    `Catalog → ardemo_classic_dnubtw_catalog → ws_<tu_usuario> → economia_mundial_pdf`
# MAGIC
# MAGIC 2. En la página de la tabla, arriba a la derecha, haz click en **Create → Vector search index**.
# MAGIC
# MAGIC 3. Configura el índice así:
# MAGIC
# MAGIC    | Campo | Valor |
# MAGIC    | -- | -- |
# MAGIC    | **Name** | `economia_mundial_pdf_idx` |
# MAGIC    | **Primary key** | `id` |
# MAGIC    | **Endpoint** | Selecciona uno existente (`shared-endpoint`) o crea uno nuevo tipo *Standard* |
# MAGIC    | **Sync computed embeddings** | ✓ activado |
# MAGIC    | **Embedding source** | columna `element` |
# MAGIC    | **Embedding model** | `databricks-gte-large-en` (o `databricks-bge-large-en`) |
# MAGIC    | **Sync mode** | **Triggered** (manual — suficiente para el lab) |
# MAGIC
# MAGIC 4. Click **Create**. La provisión tarda 1–3 minutos. Cuando el estado muestre **Ready**, el índice está listo.
# MAGIC
# MAGIC > **Tip**: el endpoint compartido tiene cuota; si no aparece como opción, créalo con tu nombre (`vsi-<tu-apellido>`).
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/vsi01.png" width="600">
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/vsi02.png" width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 3 — Crear el Knowledge Assistant (Agent Bricks, desde la UI)
# MAGIC
# MAGIC Agent Bricks es el constructor de agentes de Databricks. Hay cuatro tipos; nosotros usamos **Knowledge Assistant** porque queremos un agente que responda preguntas sobre documentos.
# MAGIC
# MAGIC ## Pasos en la UI
# MAGIC
# MAGIC 1. En el sidebar de Databricks, click en **Agents** (sección AI/ML).
# MAGIC    Si no lo ves, navega directamente a: `https://<tu-workspace>/agents`
# MAGIC
# MAGIC 2. Click **Create agent** y selecciona la tarjeta **Knowledge Assistant**.
# MAGIC
# MAGIC    <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/agents.png" width="600">
# MAGIC
# MAGIC 3. **Configura el agente**:
# MAGIC
# MAGIC    | Campo | Valor sugerido |
# MAGIC    | -- | -- |
# MAGIC    | **Name** | `economia_mundial_<tu_apellido>` |
# MAGIC    | **Description** | `Agente especializado en el informe Actualización de Perspectivas de la Economía Mundial. Proporciona análisis y datos clave sobre la resiliencia de la economía y la incertidumbre persistente.` |
# MAGIC
# MAGIC 4. **Knowledge sources** — click **Add knowledge source**:
# MAGIC
# MAGIC    - Tipo: **Vector search index**
# MAGIC    - Selecciona el índice: `ardemo_classic_dnubtw_catalog.ws_<tu_usuario>.economia_mundial_pdf_idx`
# MAGIC    - **Content description** (importante — el agente lo usa para decidir cuándo consultar esta fuente):
# MAGIC      `Informe del FMI con los últimos datos de crecimiento, proyecciones de inflación, y riesgos económicos por país y región.`
# MAGIC
# MAGIC    <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/KA.png" width="700">
# MAGIC
# MAGIC 5. **Instructions** (opcional pero recomendado): pega esto en el campo de instrucciones del agente:
# MAGIC    ```
# MAGIC    Responde en español. Sé conciso. Cuando uses datos del informe, menciona el país o la sección de la que provienen.
# MAGIC    Si la pregunta no se puede responder con el informe, dilo claramente en lugar de inventar.
# MAGIC    ```
# MAGIC
# MAGIC 6. Click **Create**. El agente entra en estado *Provisioning* — tarda 2–5 minutos.
# MAGIC
# MAGIC    <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/agentbricks.png" width="900">

# COMMAND ----------

# MAGIC %md
# MAGIC # Paso 4 — Probar el agente en el Playground
# MAGIC
# MAGIC El Playground es un chat para probar el agente antes de exponerlo a usuarios reales (vía API, Apps o Slack).
# MAGIC
# MAGIC ## Pasos en la UI
# MAGIC
# MAGIC 1. Desde la página del agente que acabas de crear, click **Open in Playground** (esquina superior derecha).
# MAGIC
# MAGIC    <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/KA2.png" width="900">
# MAGIC
# MAGIC 2. Prueba estas preguntas en orden:
# MAGIC
# MAGIC    1. *¿Cuáles son las proyecciones económicas para Brasil en 2026?*
# MAGIC    2. *¿Y para México?* — observa que el agente mantiene el contexto de la pregunta anterior
# MAGIC    3. *¿Cuáles son las políticas para restablecer la confianza y garantizar la sostenibilidad?*
# MAGIC    4. *¿Cuál es la receta de paella valenciana?* — pregunta fuera de tema, el agente debería rechazarla educadamente
# MAGIC
# MAGIC 3. Para cada respuesta, expande las **citations** (fuentes) que muestra el agente — verás de qué fragmento del PDF sacó la información.
# MAGIC
# MAGIC ## Iterando sobre el agente
# MAGIC
# MAGIC Si una respuesta no es buena:
# MAGIC
# MAGIC - Usa los botones **👍 / 👎** en el Playground para registrar feedback
# MAGIC - Edita las **Instructions** del agente para corregir comportamiento
# MAGIC - Ajusta la **Content description** del knowledge source si el agente consulta el índice incorrectamente
# MAGIC - Para mejoras más estructuradas, click en **Improve** → permite añadir ejemplos de preguntas/respuestas ideales (*Agent Learning from Human Feedback*)

# COMMAND ----------

# MAGIC %md
# MAGIC # Próximos pasos
# MAGIC
# MAGIC Tu agente ya está desplegado como un endpoint de Model Serving. Eso significa que puedes:
# MAGIC
# MAGIC - **Llamarlo por API** desde cualquier aplicación — el endpoint URL aparece en la página del agente
# MAGIC - **Conectarlo a Slack / Teams** — el botón **Use this agent** muestra las opciones de integración
# MAGIC - **Embeberlo en una Databricks App** — ver el código de ejemplo en `Lab_01_Genie_y_Apps/03_App_Streamlit_Actualizar_Inventario`
# MAGIC - **Combinarlo con otros agentes** usando el **Multi-Agent Supervisor** (otra tarjeta de Agent Bricks)
# MAGIC
# MAGIC ## ¡Felicidades!
# MAGIC
# MAGIC Has creado un Knowledge Assistant end-to-end:
# MAGIC datos → Vector Search → Agent Bricks → Playground.
