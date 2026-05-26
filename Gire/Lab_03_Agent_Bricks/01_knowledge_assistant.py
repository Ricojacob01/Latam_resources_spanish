# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>

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


# MAGIC %md
# MAGIC ## Agent Bricks: Creando un Knowledge Assistant
# MAGIC
# MAGIC Agent Bricks en Databricks te permite construir asistentes inteligentes que acceden y analizan información relevante de tus datos empresariales. Con Agent Bricks, puedes crear Knowledge Assistants capaces de responder preguntas, extraer insights y proporcionar análisis contextualizados de manera automática.
# MAGIC
# MAGIC En este notebook aprenderás a:
# MAGIC * Configurar un agente especializado utilizando Agent Bricks en Databricks;
# MAGIC * Integrar documentos para enriquecer el conocimiento del asistente;
# MAGIC * Personalizar el comportamiento del agente para responder consultas específicas y generar análisis.
# MAGIC
# MAGIC Esta funcionalidad te ayuda a transformar la información en conocimiento accionable, facilitando la toma de decisiones y mejorando la eficiencia en el acceso a datos clave.

# COMMAND ----------

# MAGIC %md
# MAGIC En el siguiente ejemplo, _crea_ un esquema con tu nombre para no tener problema de sobreescritura de datos:

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.archivos")
print(f"Volume listo: {CATALOG}.{SCHEMA}.archivos")

# COMMAND ----------

# (replaced by setup cell) catalog override removed
# (replaced by setup cell) schema override removed
volume = "archivos"

file_name = "economia_mundial.pdf"
table_name = "economia_mundial_pdf"
download_url = "https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/data/economia_mundial.pdf"
path_volume = f"/Volumes/{catalog}/{schema}/{volume}"
path_table = f"{catalog}.{schema}"

print(path_table)   # Show the complete path
print(path_volume)  # Show the complete path

# Copy the CSV file from the URL to the volume
dbutils.fs.cp(download_url, f"{path_volume}/{file_name}")

# COMMAND ----------

from pyspark.sql.functions import expr

# Full path for standard Python libraries (like PyPDF)
full_file_path = f"/Volumes/{catalog}/{schema}/{volume}/{file_name}"

# Path for Spark APIs
path_volume = f"/Volumes/{catalog}/{schema}/{volume}"

# Path for table creation
path_table = f"{catalog}.{schema}"

df = spark.read.format("binaryFile").load(full_file_path).withColumn(
    "parsed",
    expr("ai_parse_document(content)"))

display(df)

# COMMAND ----------

# Extract all columns from dataframe
from pyspark.sql.functions import col, parse_json

df_text = df.withColumn(
   "parsed_json",
   parse_json(col("parsed").cast("string"))) \
 .selectExpr(
   "path",
   "parsed_json:document:elements")

display(df_text)

# COMMAND ----------

#Explode into multiple rows with content from each page
from pyspark.sql.functions import explode, from_json
from pyspark.sql.types import ArrayType, StringType

# Define the expected array type for your data
array_schema = ArrayType(StringType())

# Convert 'elements' (VARIANT) to array by parsing as JSON string
df_text2 = df_text.withColumn("elements_array", from_json(col("elements").cast("string"), array_schema))

# Explode the new array column
df_text3 = df_text2.select("path", explode(col("elements_array")).alias("element"))
display(df_text3)

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

# Add id to the dataframe
df_text4 = df_text3.withColumn("id", monotonically_increasing_id())
display(df_text4)

# COMMAND ----------

# Write dataframe to Delta Table
df_text4.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workshop_databricks.demo_agentbricks.economia_mundial_pdf")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Vector Search Index
# MAGIC
# MAGIC Un Vector Search Index es una herramienta que permite buscar información usando "vectores", que son representaciones numéricas de textos, imágenes o datos. Así, puedes encontrar rápidamente documentos o respuestas similares a lo que preguntas, incluso si usas palabras diferentes.

# COMMAND ----------

catalog = "workshop_databricks"
schema = "demo_agentbricks"
table_name = "economia_mundial_pdf"

workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
table_link = f"{workspace_url}/explore/data/{catalog}/{schema}/{table_name}"

displayHTML(f"Access your table here: {table_link}")

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/vsi01.png" width="350">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/vsi02.png" width="500">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Crear Agent Bricks

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/agents.png" width="300">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/KA.png" width="500">

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/agentbricks.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC **Description**
# MAGIC
# MAGIC Agente especializado en el informe 'Actualización de Perspectivas de la Economía Mundial'. Proporciona análisis y datos clave sobre la resiliencia de la economía y la incertidumbre persistente.

# COMMAND ----------

# MAGIC %md
# MAGIC **Describe the content**
# MAGIC
# MAGIC Informe sobre los últimos datos de crecimiento, proyecciones de inflación o riesgos económicos.

# COMMAND ----------

# MAGIC %md
# MAGIC **Clica en "Create Agent"**

# COMMAND ----------

# MAGIC %md
# MAGIC ### Playground
# MAGIC
# MAGIC Playground es un espacio interactivo donde puedes probar y explorar el Knowledge Assistant que creamos. Aquí puedes hacer preguntas sobre el informe de economía mundial y recibir respuestas automáticas basadas en los datos y análisis disponibles. Solo tienes que escribir tu consulta y el asistente te ayudará con información relevante de manera sencilla y rápida.

# COMMAND ----------

# MAGIC %md
# MAGIC **Clica en "Open in Playground"**

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/KA2.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC #### Preguntas
# MAGIC
# MAGIC * Cuáles son las proyecciones económicas para Brasil en 2026?
# MAGIC * Y para México?
# MAGIC * Cuáles son las políticas para restablecer la confianza y garantizar la sostenibilidad?

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¡Felicidades! ¡Has completado tu primer laboratorio de ML, AI functions y Agent bricks!
# MAGIC
# MAGIC ¡Ahora ya sabes cómo utilizar Unity Catalog, entrenar y registrar modelos con MLflow, usar las AI functions y crear un agente utilizando Agent Bricks!
