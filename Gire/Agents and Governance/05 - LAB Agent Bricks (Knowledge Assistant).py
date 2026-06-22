# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # 05 — LAB 🧱 · Agent Bricks (Knowledge Assistant)
# MAGIC
# MAGIC **30 min.** Construyes un agente RAG sobre un PDF (informe económico). Primero te mostramos el camino **100% UI**; luego el código opcional para quienes quieran más control sobre el parseo y el índice.

# COMMAND ----------

# DBTITLE 1,Cell 2
# MAGIC %md
# MAGIC ## 🧭 Enfoque de este módulo — **UI First, Code opcional**
# MAGIC
# MAGIC El Knowledge Assistant de Agent Bricks puede encargarse de **todo el pipeline** (parseo, chunking, embeddings, Vector Search index, retriever y endpoint) si le das los archivos en un **Volume**. No necesitas escribir código.
# MAGIC
# MAGIC | Paso | Acción | Herramienta |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | Guardar el PDF en un Volume | UI (drag & drop) **o** código |
# MAGIC | 2 | Crear el Knowledge Assistant | UI (Agent Bricks) |
# MAGIC | 3 | Probar en el Playground | UI |
# MAGIC
# MAGIC > 💡 **¿Quieres control total?** Al final del notebook hay un camino alternativo con `ai_parse_document` + Delta + Vector Search index manual.

# COMMAND ----------

# DBTITLE 1,Cell 3
# MAGIC %md
# MAGIC ## Paso 1 — Guardar el PDF en un Volume
# MAGIC
# MAGIC ### Opción A — UI (drag & drop)
# MAGIC 1. **Catalog Explorer → tu catálogo → tu schema → Volumes → `archivos`** (créalo si no existe).
# MAGIC 2. Haz clic en **Upload** y sube el archivo `economia_mundial.pdf` desde tu máquina local.
# MAGIC    - [📥 Descargar el PDF aquí](https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/data/economia_mundial.pdf) para tenerlo en tu máquina.
# MAGIC
# MAGIC ### Opción B — Código
# MAGIC Si prefieres automatizar la descarga, ejecuta la celda de abajo ⬇️

# COMMAND ----------

# DBTITLE 1,Cell 4
# === Opción B: Guardar el PDF en el Volume con código ===
CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.archivos")

volume, file_name = "archivos", "economia_mundial.pdf"
path = f"/Volumes/{CATALOG}/{SCHEMA}/{volume}/{file_name}"
url = "https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/data/economia_mundial.pdf"
dbutils.fs.cp(url, path)

print(f"✅ PDF guardado en: {path}")

# COMMAND ----------

# DBTITLE 1,Cell 5
# MAGIC %md
# MAGIC ## Paso 2 — Crear el Knowledge Assistant en la UI (🖱️)
# MAGIC
# MAGIC Con el PDF en el Volume, Agent Bricks hace **todo lo demás** (parseo, chunking, embeddings, índice, retriever, endpoint).
# MAGIC
# MAGIC 1. **Sidebar → Agents → Create agent → Knowledge Assistant.**
# MAGIC 2. **Name:** `ka_economia_<tu_usuario>`.
# MAGIC 3. **Description:** *"Agente sobre el informe de Perspectivas de la Economía Mundial: crecimiento, inflación y riesgos por país."*
# MAGIC 4. **Knowledge source → Unstructured data (files)** → selecciona tu Volume: `<catalog>.<schema>.archivos`.
# MAGIC    - **Content description** (clave para el razonamiento):
# MAGIC    *"Informe con datos de crecimiento, proyecciones de inflación y riesgos económicos por país y región."*
# MAGIC 5. **Instructions:** *"Responde en español, conciso. Cita el país/sección. Si el informe no lo cubre, dilo en vez de inventar."*
# MAGIC 6. **Create agent.** Databricks se encarga de chunking + embeddings + Vector Search + retriever + endpoint + evals.
# MAGIC
# MAGIC > ⏱️ Espera a que el estado sea **Ready** antes de continuar.

# COMMAND ----------

# DBTITLE 1,Cell 6
# MAGIC %md
# MAGIC ## Paso 3 — Probar en el Playground (🖱️)
# MAGIC
# MAGIC Cuando el agente esté **Ready**, abre su tab **Chat/Playground** y prueba:
# MAGIC
# MAGIC ```
# MAGIC 1. ¿Cuáles son las proyecciones económicas para Argentina?
# MAGIC 2. ¿Y para México?  (mantiene contexto)
# MAGIC 3. ¿Qué políticas recomienda para restablecer la confianza?
# MAGIC 4. ¿Cuál es la receta de la paella?  (fuera de alcance → debe declinar)
# MAGIC ```
# MAGIC
# MAGIC Revisa las **citas (Sources)** en cada respuesta — de qué parte del PDF salió. Explora la tab **Evaluation** (judges automáticos).
# MAGIC
# MAGIC > 💡 También puedes **consumir el agente por código** (su endpoint de Model Serving) con `mlflow.deployments.get_deploy_client("databricks").predict(endpoint=..., inputs=...)`.

# COMMAND ----------

# DBTITLE 1,Cell 7
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## (Opcional) Camino avanzado — Code + Vector Search manual
# MAGIC
# MAGIC Si necesitas **control total** sobre cómo se parsea el PDF, la estructura de la tabla y la configuración del índice, usa el camino de código:
# MAGIC
# MAGIC 1. **Parsear** el PDF con `ai_parse_document` → tabla Delta con Change Data Feed.
# MAGIC 2. **Crear Vector Search index** en la UI apuntando a esa tabla.
# MAGIC 3. **Crear Knowledge Assistant** apuntando al index (en vez del Volume).
# MAGIC
# MAGIC Ejecuta la celda de abajo para el paso 1 del camino avanzado ⬇️

# COMMAND ----------

# DBTITLE 1,Opcional - Parseo avanzado con ai_parse_document
# === (Opcional) Parseo avanzado con ai_parse_document ===
from pyspark.sql.functions import expr, col, get_json_object, explode, from_json, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`"); spark.sql(f"USE SCHEMA `{SCHEMA}`")

volume, file_name, table_name = "archivos", "economia_mundial.pdf", "economia_mundial_pdf"
path = f"/Volumes/{CATALOG}/{SCHEMA}/{volume}/{file_name}"

# Parsear el PDF con ai_parse_document
df = (spark.read.format("binaryFile").load(path)
      .withColumn("parsed", expr("CAST(ai_parse_document(content, MAP('version','2.0')) AS STRING)")))

# Explotar los elementos del documento
df_el = (df.select(col("path"), get_json_object(col("parsed"), "$.document.elements").alias("elements"))
         .withColumn("elements_array", from_json(col("elements").cast("string"), ArrayType(StringType())))
         .select("path", explode(col("elements_array")).alias("element"))
         .withColumn("id", monotonically_increasing_id()))

# Guardar como tabla Delta con Change Data Feed
(df_el.write.mode("overwrite").option("overwriteSchema", "true")
   .option("delta.enableChangeDataFeed", "true")
   .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}"))

print(f"✅ Tabla lista: {CATALOG}.{SCHEMA}.{table_name}  ({spark.table(table_name).count()} elementos)")

# COMMAND ----------

# DBTITLE 1,Vector Search index (camino avanzado)
# MAGIC %md
# MAGIC ### Vector Search index (si usaste el camino avanzado)
# MAGIC
# MAGIC 1. **Catalog → tu tabla `economia_mundial_pdf` → Create → Vector search index.**
# MAGIC 2. Config:
# MAGIC    - **Primary key:** `id`
# MAGIC    - **Embedding source column:** `element`
# MAGIC    - **Embedding model:** `databricks-gte-large-en`
# MAGIC    - **Endpoint:** existente o crea uno **Standard**
# MAGIC    - **Sync mode:** Triggered
# MAGIC 3. Espera a que el index quede **Online**.
# MAGIC 4. Luego, al crear el Knowledge Assistant en **Paso 2**, elige **Vector search index** como knowledge source (en vez de files en Volume).

# COMMAND ----------

# DBTITLE 1,Cell 8
# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Guardaste el PDF en un Volume (UI o código)
# MAGIC ✅ Creaste un **Knowledge Assistant** en la UI — Agent Bricks se encargó de parseo, chunking, embeddings, Vector Search y endpoint
# MAGIC ✅ Probaste el agente con citas y evals
# MAGIC ✅ (Opcional) Exploraste el camino avanzado con `ai_parse_document` + Vector Search manual
# MAGIC
# MAGIC **Patrón clave:** Agent Bricks permite ir de archivo → agente productivo sin escribir retriever ni pipeline de embeddings.
# MAGIC
# MAGIC ## Continuar → `06 - Cierre y Workshop Preview`
