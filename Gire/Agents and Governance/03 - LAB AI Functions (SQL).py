# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — LAB 🧮 · AI Functions en SQL
# MAGIC
# MAGIC **25 min.** Llamas LLMs gobernados directamente desde SQL: clasificar, extraer, analizar sentimiento, resumir — en una fila o sobre millones (batch inference).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — **Lado a lado (Playground UI ↔ SQL)**
# MAGIC
# MAGIC El **AI Playground** (UI) y las **AI Functions** (SQL) son la **misma capacidad** vista de dos formas. La estrategia de este módulo: pruebas un prompt en el **Playground** para iterar rápido y *sentir* el modelo, y **a la vez** ejecutas el `ai_query` equivalente en SQL para operacionalizarlo. Lo presentamos **lado a lado** porque son intercambiables y queremos que elijas tu flujo según la tarea (exploración → Playground; producción/escala → SQL).
# MAGIC
# MAGIC 📓 **Catálogo completo** de AI Functions (10 funciones + batch inference con `responseFormat`): `labs/ai_functions/01_ai_functions_sql.sql`. Este módulo corre una muestra representativa.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — Playground (🖱️, 5 min)
# MAGIC
# MAGIC 1. **Sidebar → Playground**.
# MAGIC 2. Modelo: **Llama 3.3 70B**. Prompt: *"Clasifica el sentimiento de esta opinión y di por qué: 'El producto llegó tarde y la batería dura poquísimo'"*.
# MAGIC 3. Click **View code** → verás el snippet que reproduce la llamada. **Ese mismo contrato es el que usa `ai_query` abajo.**
# MAGIC
# MAGIC Mantén el Playground abierto: cada celda SQL de abajo tiene su gemelo conversacional en la UI.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Datos de ejemplo (opiniones de clientes)

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`"); spark.sql(f"USE SCHEMA `{SCHEMA}`")

spark.sql("""
CREATE OR REPLACE TABLE opiniones AS
SELECT * FROM VALUES
  (1, 'Compré la aspiradora Electrolux y quedé decepcionada: hace mucho ruido y la succión es débil.'),
  (2, 'Excelente servicio, el pedido llegó antes de lo esperado y el producto es de gran calidad.'),
  (3, 'La tablet DEF se descarga muy rápido, esperaba más por el precio.'),
  (4, 'Todo perfecto, volveré a comprar sin duda.')
AS t(id, opinion)
""")
display(spark.table("opiniones"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — AI Functions en SQL (gemelas del Playground)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Sentimiento
# MAGIC SELECT id, opinion, ai_analyze_sentiment(opinion) AS sentimiento
# MAGIC FROM opiniones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Clasificación en categorías propias
# MAGIC SELECT id,
# MAGIC        ai_classify(opinion, ARRAY('producto defectuoso', 'logística/envío', 'elogio general')) AS categoria
# MAGIC FROM opiniones;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extracción de entidades
# MAGIC SELECT id,
# MAGIC        ai_extract(opinion, ARRAY('producto', 'motivo_insatisfaccion')) AS extraido
# MAGIC FROM opiniones;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Batch inference con salida estructurada (`responseFormat`)
# MAGIC
# MAGIC Una sola pasada que devuelve JSON validado y lo desempaca a columnas — el patrón para procesar grandes volúmenes.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE opiniones_estructuradas AS
# MAGIC SELECT id, opinion,
# MAGIC   ai_query(
# MAGIC     'databricks-meta-llama-3-3-70b-instruct',
# MAGIC     CONCAT('Extrae del review: producto, sentimiento (positivo/negativo/neutral), motivo. Review: ', opinion),
# MAGIC     responseFormat => '{
# MAGIC       "type": "json_schema",
# MAGIC       "json_schema": {"name": "rev", "schema": {"type": "object", "properties": {
# MAGIC         "producto": {"type": "string"}, "sentimiento": {"type": "string"}, "motivo": {"type": "string"}
# MAGIC       }}, "strict": true}
# MAGIC     }'
# MAGIC   ) AS j
# MAGIC FROM opiniones;
# MAGIC
# MAGIC SELECT id, opinion,
# MAGIC        parse_json(j):producto::string    AS producto,
# MAGIC        parse_json(j):sentimiento::string AS sentimiento,
# MAGIC        parse_json(j):motivo::string      AS motivo
# MAGIC FROM opiniones_estructuradas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tu turno
# MAGIC
# MAGIC - Abre `labs/ai_functions/01_ai_functions_sql.sql` y prueba `ai_summarize`, `ai_translate`, `ai_similarity`, `ai_mask`, `ai_fix_grammar`.
# MAGIC - Para cada una, **valida en el Playground** el mismo prompt y compara — confirma que son la misma capacidad.
# MAGIC
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Usaste AI Functions en SQL (sentimiento, clasificación, extracción)
# MAGIC ✅ Batch inference con `responseFormat` JSON
# MAGIC ✅ Viste la equivalencia **Playground (UI) ↔ `ai_query` (SQL)**, lado a lado
# MAGIC
# MAGIC ## Continuar → `04 - LAB Genie y Apps`
