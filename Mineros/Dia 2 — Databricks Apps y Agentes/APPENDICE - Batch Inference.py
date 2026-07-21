-- Databricks notebook source
-- MAGIC %md
-- MAGIC ![](../_recursos/imagenes_agentes/header-genai.png)
-- MAGIC
-- MAGIC # Hands-On LAB 03 - Usando Batch Inference
-- MAGIC ## Análisis de sentimiento, extracción de información y generación de texto
-- MAGIC
-- MAGIC Vamos a aprender como usar funcionalidades de IA Generativa de Databricks

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Objetivos del Ejercicio
-- MAGIC
-- MAGIC El objetivo de este laboratorio es implementar el siguiente caso de uso:
-- MAGIC
-- MAGIC ### Aumentar la satisfacción del cliente con análisis automático de valoraciones
-- MAGIC
-- MAGIC En este laboratorio, construiremos un pipeline de datos que toma las valoraciones de los clientes en formato de texto libre y las enriquece con información extraída mediante preguntas en lenguaje natural a los modelos de IA Generativa disponibles en Databricks.
-- MAGIC También proporcionaremos recomendaciones de next best action para nuestro equipo de atención al cliente — es decir, si un cliente requiere seguimiento y un borrador de mensaje de respuesta.
-- MAGIC
-- MAGIC Para cada valoración:
-- MAGIC
-- MAGIC - Identificamos el sentimiento del cliente y extraemos los productos mencionados
-- MAGIC - Generamos una respuesta personalizada para el cliente
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review.png" width="100%">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Preparación
-- MAGIC
-- MAGIC Para ejecutar los ejercicios, necesitamos conectar este notebook a un clúster/cómputo.
-- MAGIC
-- MAGIC Simplemente siga los pasos a continuación:
-- MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
-- MAGIC 2. Seleccione el clúster: **Serverless**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ![](../_recursos/imagenes_agentes/serverless.png)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## En este laboratorio utilizaremos dos tablas:
-- MAGIC - **Opiniones**: datos no estructurados con el contenido de las valoraciones
-- MAGIC - **Clientes**: datos estructurados, como el registro y el consumo de los clientes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Configuración del ambiente
-- MAGIC
-- MAGIC Vamos a comenzar seleccionando el catálogo y esquema donde se encuentran nuestros datos
-- MAGIC

-- COMMAND ----------

USE academia.ia

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ejercicio 02.01 - Analizando el sentimiento y extrayendo información
-- MAGIC
-- MAGIC Nuestro objetivo es permitir el análisis rápido de grandes volúmenes de valoraciones de forma ágil y eficiente. Para ello, necesitamos extraer la siguiente información:
-- MAGIC
-- MAGIC - Productos mencionados
-- MAGIC - Sentimiento del cliente
-- MAGIC - En caso de que sea negativo, el motivo de la insatisfacción
-- MAGIC
-- MAGIC Vamos a ver cómo podemos aplicar IA Generativa para acelerar nuestro trabajo.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Usando AI Functions
-- MAGIC
-- MAGIC Para que podamos escalar el uso de los modelos de IA Generativa, podemos utilizar las **[AI Functions](https://docs.databricks.com/en/large-language-models/ai-functions.html)**.
-- MAGIC
-- MAGIC Estas funciones permiten ejecutar modelos de IA Generativa sobre nuestras bases de datos corporativas directamente desde consultas SQL, un lenguaje ampliamente utilizado por analistas de datos y de negocio. Con ello, también podemos crear nuevas tablas con la información extraída para utilizarlas en nuestros análisis.
-- MAGIC
-- MAGIC Existen funciones nativas para ejecutar tareas predefinidas o para enviar cualquier instrucción que deseemos que sea ejecutada. A continuación, se muestran las descripciones:
-- MAGIC
-- MAGIC | Gen AI SQL Function | Descripción |
-- MAGIC | -- | -- |
-- MAGIC | [ai_analyze_sentiment](https://docs.databricks.com/pt/sql/language-manual/functions/ai_analyze_sentiment.html) | Análisis de Sentimento |
-- MAGIC | [ai_classify](https://docs.databricks.com/pt/sql/language-manual/functions/ai_classify.html) | Clasifica el texto de acuerdo a las categorías definidas |
-- MAGIC | [ai_extract](https://docs.databricks.com/pt/sql/language-manual/functions/ai_extract.html) | Extrae las informaciones deseadas |
-- MAGIC | [ai_fix_grammar](https://docs.databricks.com/pt/sql/language-manual/functions/ai_fix_grammar.html) | Corrige la gramática del texto proporcionado |
-- MAGIC | [ai_gen](https://docs.databricks.com/pt/sql/language-manual/functions/ai_gen.html) | Genera un nuevo texto según la instrucción | 
-- MAGIC | [ai_mask](https://docs.databricks.com/pt/sql/language-manual/functions/ai_mask.html) | Enmascara datos sensibles |
-- MAGIC | [ai_query](https://docs.databricks.com/pt/sql/language-manual/functions/ai_query.html) | Envía instrucciones al modelo deseado |
-- MAGIC | [ai_similarity](https://docs.databricks.com/pt/sql/language-manual/functions/ai_similarity.html) | Calcula la similitud entre dos expresiones |
-- MAGIC | [ai_summarize](https://docs.databricks.com/pt/sql/language-manual/functions/ai_summarize.html) | Resume el texto proporcionado |
-- MAGIC | [ai_translate](https://docs.databricks.com/pt/sql/language-manual/functions/ai_translate.html) | Traduce el texto proporcionado |
-- MAGIC
-- MAGIC Para extraer la información que necesitamos, ¡vamos a utilizar algunas de estas funciones a continuación!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### A. Análisis de sentimiento

-- COMMAND ----------

SELECT
  opinion,
  ai_analyze_sentiment(opinion) as sentimiento
FROM
  opiniones
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### B. Extracción de los produtos mencionados

-- COMMAND ----------

SELECT
  opinion,
  ai_extract(
    opinion,
    array('producto')
  ) AS productos_mencionados
FROM opiniones
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C. Extracción de motivo de la insatisfacción
-- MAGIC

-- COMMAND ----------

SELECT
  opinion,
  ai_extract(
    opinion,
    array('motivo de la insatisfacción del cliente')
  ) AS motivo
FROM opiniones
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Ejercicio 02.02 - Análisis de sentimiento y extracción de entidades a escala
-- MAGIC
-- MAGIC Tener que especificar las instrucciones varias veces acaba siendo trabajoso, especialmente para los analistas de datos, que deberían centrarse en analizar los resultados de esa extracción.
-- MAGIC
-- MAGIC Para simplificar el acceso a esta inteligencia, crearemos una función SQL para encapsular este proceso y poder indicar simplemente en qué columna de nuestro conjunto de datos queremos aplicarla.
-- MAGIC
-- MAGIC Aquí vamos a aprovechar para enviar una única consulta a nuestro modelo para extraer toda la información de una sola vez.
-- MAGIC
-- MAGIC <img src="https://raw.githubusercontent.com/databricks-demos/dbdemos-resources/main/images/product/sql-ai-functions/sql-ai-query-function-review-wrapper.png" width="1200px">

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### A. Creando una función para extraer toda la información
-- MAGIC *Vamos a usar la función AI_QUERY() para proporcionar un prompt personalizado*

-- COMMAND ----------

CREATE OR REPLACE FUNCTION REVISAR_OPINION(opinion STRING)
RETURNS STRUCT<
  producto_nombre: STRING,
  producto_categoria: STRING,
  sentimiento: STRING,
  motivo: STRING
>
RETURN FROM_JSON(
  REGEXP_REPLACE(
    AI_QUERY(
      'databricks-meta-llama-3-3-70b-instruct',
      CONCAT(
        'Analiza la siguiente valoración y devuelve un JSON **válido y compacto**, sin texto adicional. ',
        'Campos obligatorios: producto_nombre, producto_categoria, sentimiento, motivo. ',
        'Formato exacto: {"producto_nombre":"","producto_categoria":"","sentimiento":"","motivo":""} ',
        'Opinión: ', opinion
      )
    ),
    '\\n', ''  -- eliminar saltos de línea
  ),
  'STRUCT<
    producto_nombre: STRING,
    producto_categoria: STRING,
    sentimiento: STRING,
    motivo: STRING
  >'
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### B. Probando el análisis de las opiniones

-- COMMAND ----------

SELECT 
  REVISAR_OPINION(
    'Compré una aspiradora de la marca Electrolux y quedé muy decepcionada.
    El producto hace mucho ruido y la succión es débil.
    Esperaba más por el rango de precio.
    Me gustaría que la empresa se pusiera en contacto para resolver el problema.'
  ) AS resultado;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### C. Analizando todas las opiniones

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Ahora, todos nuestros usuarios pueden aprovechar nuestra función, que fue cuidadosamente preparada para analizar nuestras valoraciones de productos.
-- MAGIC
-- MAGIC Y podemos escalar este proceso fácilmente aplicando dicha función sobre todo nuestro conjunto de datos.

-- COMMAND ----------

CREATE OR REPLACE TABLE opiniones_revisadas AS
SELECT *, resultado.*
FROM (
  SELECT 
    opinion,
    REVISAR_OPINION(opinion) AS resultado
  FROM opiniones
  LIMIT 10
);

SELECT * FROM opiniones_revisadas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Ejercicio 02.03 - Generando una sugerencia de respuesta
-- MAGIC
-- MAGIC Con toda la información extraída, podemos aprovecharla para generar sugerencias de respuestas personalizadas que aceleren el trabajo de nuestros equipos de atención.
-- MAGIC
-- MAGIC Otro punto interesante es que, en este proceso, podemos utilizar otras **informaciones estructuradas** que ya tengamos en nuestro entorno, como datos del historial de compras, para personalizar aún más nuestras respuestas.
-- MAGIC
-- MAGIC ¡Vamos a ver cómo hacerlo!

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### A. Creando una función para generar un ejemplo de respuesta

-- COMMAND ----------

CREATE OR REPLACE FUNCTION GENERAR_RESPUESTA(
    nombre STRING,
    apellido STRING,
    num_pedidos INT,
    producto STRING,
    motivo STRING
)
RETURNS TABLE(resposta STRING)
COMMENT 'Si el cliente muestra insatisfacción con algún producto, use esta función para generar una respuesta personalizada'
RETURN 
SELECT AI_QUERY(
    'databricks-meta-llama-3-3-70b-instruct',
    CONCAT(
        'Eres un asistente virtual responsable de generar una respuesta amigable y profesional para un cliente que está insatisfecho. ',
        'Tu objetivoes tranquilizar al cliente, reconocer el problema y, si corresponde, sugerir una solución o un cambio dentro de la política de la empresa. ',
        'Evita generar información falsa o inventar detalles sobre el cliente o sus pedidos. ',
        'Controla el tamaño del mensaje para que sea claro y objetivo (máx. 150 palabras) ',
        'Usa un tono cordial, empático y profesional. ',
        'Datos del cliente: Nombre: ', generar_respuesta.nombre, ' ', generar_respuesta.apellido, 
        ', Número de pedidos anteriores: ', CAST(generar_respuesta.num_pedidos AS STRING), '. ',
        'Producto: ', generar_respuesta.producto, '. ',
        'Motivo de la insatisfacción: ', generar_respuesta.motivo, '. ',
        'Genere una respuesta personalizada considerando esta información.'
    )
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### B. Generando respuestas automatizadas para todas las opiniones negativas

-- COMMAND ----------

CREATE OR REPLACE TABLE opiniones_revisadas AS
SELECT 
  a.id_opinion,
  a.id_cliente,
  c.nombre,
  c.apellido,
  c.num_pedidos,
  a.opinion,
  REVISAR_OPINION(a.opinion) AS resultado
FROM opiniones a
JOIN clientes c ON a.id_cliente = c.id_cliente;

SELECT * FROM opiniones_revisadas

-- COMMAND ----------

CREATE OR REPLACE TABLE respuestas AS
SELECT 
  r.id_opinion,
  r.id_cliente,
  r.nombre,
  r.apellido,
  r.num_pedidos,
  r.resultado.producto_nombre AS producto_nombre,
  r.resultado.producto_categoria AS producto_categoria,
  r.resultado.sentimiento AS sentimiento,
  r.resultado.motivo AS motivo,
  CASE
    WHEN LOWER(r.resultado.sentimiento) = 'negativo' THEN (
      SELECT * FROM GENERAR_RESPUESTA(
        r.nombre,
        r.apellido,
        r.num_pedidos,
        r.resultado.producto_nombre,
        r.resultado.motivo
      )
    )
    ELSE NULL
  END AS borrador
FROM opiniones_revisadas r;

SELECT * FROM respuestas

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ¡Felicidades!
-- MAGIC
-- MAGIC ¡Has completado el laboratorio de **Extracción de información y generación de texto**!
-- MAGIC
-- MAGIC Ahora ya sabes cómo utilizar Foundation Models, Playground y AI Functions para analizar el sentimiento, identificar motivos y generar respuestas personalizadas para nuestros clientes de una manera sencilla y escalable.
-- MAGIC
-- MAGIC Vamos a pasar al último laboratorio del día de hoy: [Lab 04 - Agent Bricks]($./Lab 04 - Agent Bricks)