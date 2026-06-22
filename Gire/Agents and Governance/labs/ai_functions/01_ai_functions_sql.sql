-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Setup del lab
-- MAGIC
-- MAGIC Catálogo compartido: `ardemo_classic_dnubtw_catalog`. Schema personal por usuario: `ws_<usuario>`.
-- MAGIC Esta celda valida acceso y crea tu schema si no existe.

-- COMMAND ----------

-- MAGIC %python
-- MAGIC CATALOG = catalog = CATALOGO = "ardemo_classic_dnubtw_catalog"
-- MAGIC _user = spark.sql("SELECT current_user()").collect()[0][0]
-- MAGIC SCHEMA = db = schema = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
-- MAGIC spark.sql(f"USE CATALOG `{CATALOG}`")
-- MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
-- MAGIC spark.sql(f"USE SCHEMA `{SCHEMA}`")
-- MAGIC try:
-- MAGIC     spark.conf.set("c.catalog", CATALOG)
-- MAGIC     spark.conf.set("c.schema", SCHEMA)
-- MAGIC except Exception:
-- MAGIC     pass  # Not available on Serverless
-- MAGIC print(f"Catalog: {CATALOG}\nSchema:  {SCHEMA}\nUser:    {_user}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Utilice un cluster Serverless Environment 2 para ejecutar este notebook
-- MAGIC Para ejecutar esta demostración, simplemente selecciona el cluster `Serverless` en el menú desplegable.
-- MAGIC Comprueba que la versión del cluster serverless es la número 2 <br />
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ![](https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/version2-serverless.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC <img src="https://raw.githubusercontent.com/Databricks-BR/genai_hackathon/main/images/head_genai_sql.png" width="1200px">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Una forma de aplicar los modelos de IA Generativa es utilizar las **[Funciones SQL de IA de Databricks](https://docs.databricks.com/en/large-language-models/ai-functions.html)**.
-- MAGIC
-- MAGIC Estas permiten el uso de SQL, un lenguaje ampliamente utilizado por analistas de datos y de negocios, para ejecutar un LLM sobre nuestras bases de datos corporativas. Con esto, también podemos crear nuevas tablas con la información extraída para ser utilizada en nuestros análisis más fácilmente.
-- MAGIC
-- MAGIC Existen funciones nativas para ejecutar tareas predefinidas o enviar cualquier instrucción deseada para ser ejecutada. A continuación se muestran las descripciones:

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC  Gen AI SQL Function | Descripción |
-- MAGIC  -- | -- |
-- MAGIC  [ai_analyze_sentiment](https://docs.databricks.com/pt/sql/language-manual/functions/ai_analyze_sentiment.html) | Análisis de Sentimiento |
-- MAGIC  [ai_classify](https://docs.databricks.com/pt/sql/language-manual/functions/ai_classify.html) | Clasificación de Texto |
-- MAGIC  [ai_extract](https://docs.databricks.com/pt/sql/language-manual/functions/ai_extract.html) | Extracción de Términos |
-- MAGIC  [ai_fix_grammar](https://docs.databricks.com/pt/sql/language-manual/functions/ai_fix_grammar.html) | Corrección Gramatical |
-- MAGIC  [ai_gen](https://docs.databricks.com/pt/sql/language-manual/functions/ai_gen.html) | Generación de Textos | 
-- MAGIC  [ai_mask](https://docs.databricks.com/pt/sql/language-manual/functions/ai_mask.html) | Enmascaramiento de datos sensibles |
-- MAGIC  [ai_query](https://docs.databricks.com/pt/sql/language-manual/functions/ai_query.html) | Consultas Gen AI |
-- MAGIC  [ai_similarity](https://docs.databricks.com/pt/sql/language-manual/functions/ai_similarity.html) | Análisis de Similitud |
-- MAGIC  [ai_summarize](https://docs.databricks.com/pt/sql/language-manual/functions/ai_summarize.html) | Resumen de Textos |
-- MAGIC  [ai_translate](https://docs.databricks.com/pt/sql/language-manual/functions/ai_translate.html) | Traducción de Textos |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ejemplos de implementación

-- COMMAND ----------

-- DBTITLE 1,Crear Informe Médico
SELECT ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    'Cree un informe médico de un paciente llamado JUAN SILVA (30 años) con síntomas de COVID que está en tratamiento hace 10 días, incluyendo todos los medicamentos utilizados para minimizar los efectos inflamatorios de una crisis pulmonar.'
  ) AS summary

-- COMMAND ----------

-- DBTITLE 1,Crear lista fake de clientes
SELECT ai_query(
    'databricks-meta-llama-3-3-70b-instruct',
    'Genere un conjunto de datos de muestra de 10 clientes que contenga las siguientes columnas: 
      "customer_id" (entero largo de 1 a 10), "firstname", "lastname" y order_count (número positivo aleatorio, menor que 200).

      Dame solo JSON. Ningún texto fuera del JSON. Sin explicaciones ni notas.
      [{"customer_id":<long>, "firstname":<string>, "lastname":<string>, "order_count":<int>}] 
      "array<struct<customer_id:long, firstname:string, lastname:string, order_count:int>>" '
      ) AS sample_data

-- COMMAND ----------

-- DBTITLE 1,Traducción
SELECT ai_translate('Hello, how are you?', 'es') as traduccion;

-- COMMAND ----------

-- DBTITLE 1,Resumen de un texto juridico
SELECT ai_summarize(
"""
Haz el resumen en español:

DE LA PUBLICIDAD: Todas las declaraciones, anuncios públicos y divulgaciones deberán ser previamente comunicadas por la parte receptora, siguiendo los principios de buena fe y transparencia contractual. Asimismo, es necesaria la aprobación de la otra parte sobre la revelación de contenidos, requiriendo el consentimiento previo y expreso.

DE LA INFORMACIÓN CONSIDERADA CONFIDENCIAL: La información confidencial es aquella que la parte reveladora no desea que sea divulgada a terceros fuera del acuerdo. Por lo tanto, debe redactarse una lista con los datos que serán considerados confidenciales, como: metodologías y herramientas de desarrollo de productos y servicios, valores e información financiera, documentos de marketing y estrategia.

DE LAS PENALIDADES Y DEL INCUMPLIMIENTO DE LA CONFIDENCIALIDAD: Las partes deberán acordar las penalidades en caso de incumplimiento de las cláusulas contractuales, tales como: pago de multa, indemnización material y/o moral, y/o reembolso de todas las pérdidas, daños causados, lucro cesante, daños directos e indirectos, derechos de autor, entre otros perjuicios patrimoniales o morales que surjan como consecuencia del incumplimiento, además de la responsabilidad civil y penal de forma judicial.
""",
    10
  ) as resumen

-- COMMAND ----------

-- DBTITLE 1,Classificar texto juridico
SELECT ai_classify(
  """
      En caso de incumplimiento del contrato por parte del proveedor de servicios, el cliente tendrá derecho a aplicar una multa contractual, cuyo valor será equivalente al 20% del valor total del contrato, sin perjuicio de otras reclamaciones o acciones legales que el cliente pueda tener. Esta multa será inmediatamente exigible y pagadera por el proveedor de servicios en la fecha de ocurrencia del incumplimiento del contrato.
  """
, ARRAY(  "cláusula de rescisión", "cláusula de garantía", "cláusula de multa"))
as avalia_contrato;

-- COMMAND ----------

-- DBTITLE 1,Similitud de textos
-- Realiza una comparación de SIMILITUD ENTRE DOS TEXTOS

-- Por ejemplo, para ver si alguna cláusula contractual fue modificada (en un contexto jurídico)

SELECT ai_similarity(
  """
        En caso de violación de contrato por parte del proveedor de servicios, el cliente tendrá derecho a aplicar una multa contractual, cuyo valor será equivalente al 10% del valor total del contrato, sin perjuicio de otras reclamaciones o acciones legales que el cliente pueda tener. Esta multa será inmediatamente exigible y pagadera por el proveedor de servicios en la fecha de ocurrencia de la violación del contrato.

  """,
  """
        En caso de violación de contrato por parte del proveedor de servicios, el cliente tendrá derecho a aplicar una multa contractual, cuyo valor será equivalente al 20% del valor total del contrato, sin perjuicio de otras reclamaciones o acciones legales que el cliente pueda tener. Esta multa será inmediatamente exigible y pagadera por el proveedor de servicios en la fecha de ocurrencia de la violación del contrato.

  """) as similitud

-- COMMAND ----------

-- DBTITLE 1,Extracción de términos
SELECT ai_extract(
    """
        El cliente Juan Pérez, con número de cuenta 123456789, realizó una transferencia bancaria el 15/09/2025 por un monto de 2,500 euros al beneficiario María López. La operación fue registrada bajo el código de transacción TRX987654.
    """,
    array('nombre', 'fecha', 'numero_de_cuenta', 'beneficiario', 'monto', 'codigo_de_transaccion')
  ) as extraccion_de_terminos;

-- COMMAND ----------

-- DBTITLE 1,Extracción de términos
SELECT extraccion.nombre,
       extraccion.fecha,      
       extraccion.numero_de_cuenta,
       extraccion.beneficiario,
       extraccion.monto,
       extraccion.codigo_de_transaccion
FROM (
  SELECT ai_extract(
    """
                El cliente Juan Pérez, con número de cuenta 123456789, realizó una transferencia bancaria el 15/09/2025 por un monto de 2,500 euros al beneficiario María López. La operación fue registrada bajo el código de transacción TRX987654.
    """,
    array('nombre', 'fecha', 'numero_de_cuenta', 'beneficiario', 'monto', 'codigo_de_transaccion')
  ) as extraccion
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC En el siguiente ejemplo, _crea_ un esquema con tu nombre para no tener problema de sobreescritura de datos:

-- COMMAND ----------


-- (replaced by setup cell)

CREATE VOLUME IF NOT EXISTS archivos;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # catalog ya definido en setup cell
-- MAGIC # (replaced by setup cell) schema override removed
-- MAGIC volume = "archivos"
-- MAGIC
-- MAGIC download_url = "https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/data/reviews.csv"
-- MAGIC file_name = "reviews.csv"
-- MAGIC table_name = "reviews"
-- MAGIC path_volume = "/Volumes/" + catalog + "/" + schema + "/" + volume
-- MAGIC path_table = catalog + "." + schema
-- MAGIC print(path_table) # Show the complete path
-- MAGIC print(path_volume) # Show the complete path
-- MAGIC
-- MAGIC dbutils.fs.cp(f"{download_url}", f"{path_volume}" + "/" + f"{file_name}")
-- MAGIC
-- MAGIC df = spark.read.csv(f"{path_volume}/{file_name}",
-- MAGIC   header=True,
-- MAGIC   inferSchema=True,
-- MAGIC   sep=",",
-- MAGIC   encoding="UTF-8")
-- MAGIC
-- MAGIC df.write.mode("overwrite").saveAsTable(f"{path_table}.{table_name}")
-- MAGIC
-- MAGIC display(df)

-- COMMAND ----------

CREATE OR REPLACE TABLE reviews_structured AS
SELECT
  ai_query(
    "databricks-meta-llama-3-3-70b-instruct",
    CONCAT(
      'Extract the following information from the review: ',
      'Location (city name, street name, whatever may be mentioned in the review), ',
      'Service Score (1–5), ',
      'Product Score (1–5), ',
      'Product Name (if mentioned) (comma separated if multiple), ',
      'Atmosphere Score [cleanliness, accessibility, location, etc] (1–5), ',
      'Sentiment (positive, negative, neutral) [Analyze the sentiment of the review and classify it as positive, negative, or neutral]. ',
      'If the review doesn’t contain an element, leave it blank or set it to zero. For instance, if the review does not mention service, then set service_score = 0. Urgency should always have a value. All scores should be 1–5 (if they are not null), with 1 being the worst and 5 being the best.',
      ' Review: ', review_es
    ),
    responseFormat => '{
                        "type": "json_schema",
                        "json_schema": {
                          "name": "review_extraction",
                          "schema": {
                            "type": "object",
                            "properties": {
                              "location": { "type": "string" },
                              "service_score": { "type": "integer" },
                              "product_score": { "type": "integer" },
                              "product_name": { "type": "string" },
                              "atmosphere_score": { "type": "integer" },
                              "sentiment": { "type": "string" }
                            }
                          }
                        }
                      }'
   ) AS structured_review, *
   FROM reviews;                   


-- COMMAND ----------

SELECT * FROM reviews_structured;

-- COMMAND ----------

 -- Now that we've got results, lets unpack the JSON so we can view as a table
CREATE OR REPLACE TABLE reviews_structured_gold AS
SELECT
  parse_json(structured_review):location::string AS location,
  parse_json(structured_review):service_score::int AS service_score,
  parse_json(structured_review):product_score::int AS product_score,
  parse_json(structured_review):atmosphere_score::int AS atmosphere_score,
  parse_json(structured_review):sentiment::string AS sentiment,
  parse_json(structured_review):product_name::string AS product_name,
  *
FROM reviews_structured;

-- COMMAND ----------

SELECT * FROM reviews_structured_gold;

-- COMMAND ----------

-- With our AI augmented data ready, we can use it to narrow down just the reviews that we should be taking action on
select product_name, location, sentiment, review_es, franchiseID, review_date
from reviews_structured_gold
where sentiment = 'negative';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### ¡Felicidades! Ya hemos aprendido como usar alguna de las AI functions en Databricks 
-- MAGIC
-- MAGIC Os invitamos a seguir investigando y aprendiendo más, agilizarán mucho vuestro trabajo del dia a dia.
-- MAGIC
-- MAGIC Ahora vamos a continuar con [Agent Bricks]($./04_agent_bricks)
