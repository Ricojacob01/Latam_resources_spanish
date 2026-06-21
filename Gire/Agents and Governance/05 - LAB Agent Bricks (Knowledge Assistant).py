# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — LAB 🧱 · Agent Bricks (Knowledge Assistant)
# MAGIC
# MAGIC **30 min.** Construyes un agente RAG sobre un PDF (informe económico). El código prepara los datos; **Agent Bricks ensambla el agente sin que escribas el retriever**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Secuencial (Code → UI)**
# MAGIC
# MAGIC Aquí el orden natural es **Code → UI**. El **código** hace el trabajo de datos: descarga el PDF, lo parsea con `ai_parse_document`, explota los elementos y escribe una tabla Delta con **Change Data Feed** (para que el índice se sincronice). Luego, **en la UI de Agent Bricks**, sin escribir una línea más, creas el **Vector Search index** y el **Knowledge Assistant** (chunking + embeddings + retriever + endpoint + evals los hace Databricks). El código **alimenta**; la UI **ensambla y despliega** el agente.
# MAGIC
# MAGIC 📓 Código de preparación completo: `labs/agent_bricks/01_knowledge_assistant.py`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Preparar los datos (código)
# MAGIC
# MAGIC Resumen del notebook de labs (descarga + parseo + tabla Delta con CDF):

# COMMAND ----------

from pyspark.sql.functions import expr, col, get_json_object, explode, from_json, monotonically_increasing_id
from pyspark.sql.types import ArrayType, StringType

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`"); spark.sql(f"USE SCHEMA `{SCHEMA}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`.archivos")

volume, file_name, table_name = "archivos", "economia_mundial.pdf", "economia_mundial_pdf"
path = f"/Volumes/{CATALOG}/{SCHEMA}/{volume}/{file_name}"
url = "https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/data/economia_mundial.pdf"
dbutils.fs.cp(url, path)

df = (spark.read.format("binaryFile").load(path)
      .withColumn("parsed", expr("CAST(ai_parse_document(content, MAP('version','2.0')) AS STRING)")))
df_el = (df.select(col("path"), get_json_object(col("parsed"), "$.document.elements").alias("elements"))
         .withColumn("elements_array", from_json(col("elements").cast("string"), ArrayType(StringType())))
         .select("path", explode(col("elements_array")).alias("element"))
         .withColumn("id", monotonically_increasing_id()))

(df_el.write.mode("overwrite").option("overwriteSchema", "true")
   .option("delta.enableChangeDataFeed", "true")
   .saveAsTable(f"{CATALOG}.{SCHEMA}.{table_name}"))

print(f"✅ Tabla lista: {CATALOG}.{SCHEMA}.{table_name}  ({spark.table(table_name).count()} elementos)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Vector Search index en la UI (🖱️)
# MAGIC
# MAGIC 1. **Catalog → tu tabla `economia_mundial_pdf` → Create → Vector search index.**
# MAGIC 2. Config:
# MAGIC    - **Primary key:** `id`
# MAGIC    - **Embedding source column:** `element`
# MAGIC    - **Embedding model:** `databricks-gte-large-en`
# MAGIC    - **Endpoint:** existente o crea uno **Standard**
# MAGIC    - **Sync mode:** Triggered
# MAGIC 3. Espera a que el index quede **Online**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Crear el Knowledge Assistant en la UI (🖱️)
# MAGIC
# MAGIC 1. **Sidebar → Agents → Create agent → Knowledge Assistant.**
# MAGIC 2. **Name:** `ka_economia_<tu_usuario>`.
# MAGIC 3. **Description:** *"Agente sobre el informe de Perspectivas de la Economía Mundial: crecimiento, inflación y riesgos por país."*
# MAGIC 4. **Knowledge source → Vector search index** → selecciona tu index. **Content description** (clave para el razonamiento):
# MAGIC    *"Informe con datos de crecimiento, proyecciones de inflación y riesgos económicos por país y región."*
# MAGIC 5. **Instructions:** *"Responde en español, conciso. Cita el país/sección. Si el informe no lo cubre, dilo en vez de inventar."*
# MAGIC 6. **Create agent.** Databricks hace chunking + embeddings + retriever + endpoint de Model Serving + evals.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Probar en el Playground (🖱️)
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
# MAGIC > 💡 También puedes **consumir el agente por código** (su endpoint de Model Serving) con `mlflow.deployments.get_deploy_client("databricks").predict(endpoint=..., inputs=...)` — el patrón se profundiza en el track MLOps (`05 - Model Serving`).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Preparaste datos con código (`ai_parse_document` + Delta + CDF)
# MAGIC ✅ Creaste un Vector Search index y un **Knowledge Assistant** en la UI (sin escribir el retriever)
# MAGIC ✅ Probaste el agente con citas y evals
# MAGIC ✅ Patrón **Code → UI**: el código alimenta, la UI ensambla y despliega
# MAGIC
# MAGIC ## Continuar → `06 - Cierre y Workshop Preview`
