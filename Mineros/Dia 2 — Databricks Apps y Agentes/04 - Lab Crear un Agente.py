# Databricks notebook source
# MAGIC %md
# MAGIC ![](../_recursos/imagenes_agentes/header-genai.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Lab 02 - Creando un Agente
# MAGIC
# MAGIC Los LLMs son excelentes para responder preguntas generales. Sin embargo, por sí solos no bastan para ofrecer valor a tus clientes.
# MAGIC
# MAGIC Para que sean capaces de responder a preguntas más complejas, se necesita información adicional y específica del usuario, como su ID de contrato, el último correo que envió a tu soporte o su informe de compras más reciente.
# MAGIC
# MAGIC Los agentes están diseñados para superar este desafío. Son implementaciones de IA más avanzadas, compuestas por múltiples entidades (herramientas) especializadas en distintas acciones (por ejemplo, recuperar información o interactuar con sistemas externos).
# MAGIC
# MAGIC En términos generales, tú construyes y expones un conjunto de funciones personalizadas para la IA. La LLM puede entonces razonar sobre qué información necesita reunir y qué herramientas utilizar para responder a las instrucciones recibidas.
# MAGIC
# MAGIC <br><img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-functions-flow.png?raw=true" width="100%">

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparación
# MAGIC
# MAGIC Para ejecutar los ejercicios, necesitamos conectar este notebook a un clúster/cómputo.
# MAGIC
# MAGIC Simplemente siga los pasos a continuación:
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el clúster: **Serverless**

# COMMAND ----------

# MAGIC %md
# MAGIC ![](../_recursos/imagenes_agentes/serverless.png)

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuración del ambiente
# MAGIC
# MAGIC Vamos a comenzar seleccionando el catálogo y esquema donde se encuentran nuestros datos

# COMMAND ----------

# MAGIC %sql USE academia.ia

# COMMAND ----------

# MAGIC %md
# MAGIC # Ejercicio 01 - Usando LLMs

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## a. Usando Databricks Foundation Models
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/serving-endpoints-list.png" style="float: right; padding-left: 10px; padding-top: 15px" width=600>
# MAGIC
# MAGIC Necesitamos un modelo capaz de interpretar el texto de las **opiniones** y extraer la información deseada. Para ello, vamos a utilizar **[Foundation Models](https://docs.databricks.com/en/machine-learning/foundation-models/index.html#pay-per-token-foundation-model-apis)**, que son grandes modelos de lenguaje (LLMs) ofrecidos por Databricks, y que pueden ser consultados bajo demanda, sin necesidad de implementación ni gestión de esos recursos.
# MAGIC
# MAGIC Algunos modelos disponibles son:
# MAGIC
# MAGIC - Meta Llama
# MAGIC - OpenAI GPT e GPT OSS
# MAGIC - Anthropic Claude Sonnet e Opus
# MAGIC - Google Gemini e Gemma
# MAGIC - GTE Large
# MAGIC - BGE Large
# MAGIC
# MAGIC Ahora, vamos a verlos e funcionamiento!
# MAGIC
# MAGIC 1. En el **menú principal** a la izquierda, haz clic em **`Serving`**
# MAGIC 2. En el modelo **Llama 4 Maverick**, haz clic en **`Use`**
# MAGIC 3. Añada la siguiente instrucción:
# MAGIC     ```
# MAGIC     Clasifica el sentimiento de la siguiente opinión:
# MAGIC     Compré una tablet y estoy muy insatisfecho con la calidad de la batería. Dura muy poco tiempo y tarda mucho en cargarse.
# MAGIC     ```
# MAGIC     <br>
# MAGIC 4. Haz clic en el icono **enviar**
# MAGIC
# MAGIC Con esto, ¡ya podemos comenzar rápidamente a prototipar nuestros nuevos productos de datos!
# MAGIC
# MAGIC **NOTA:** Aquí se accede a la lista completa de modelos: https://docs.databricks.com/aws/en/machine-learning/foundation-model-apis/supported-models

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## b. Probando modelos en el AI Playground
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/ai-playground.gif" style="float: right; padding-left: 10px" width=600>
# MAGIC
# MAGIC Para decidir cuál es el mejor modelo e instrucción para nuestro caso de uso, podemos utilizar el **[AI Playground](https://docs.databricks.com/en/large-language-models/ai-playground.html)**.
# MAGIC
# MAGIC De esta forma, podemos probar rápidamente diversas combinaciones de modelos e instrucciones a través de una interfaz intuitiva y elegir la mejor opción para utilizar en nuestro proyecto.
# MAGIC
# MAGIC Vamos a realizar la siguiente prueba:
# MAGIC
# MAGIC 1. En el **menu principal** a la izquierda, haz clic en **`Playgroud`**
# MAGIC 2. Haz clic en el **seletor de modelos** y selecciona el modelo **`Llama 4 Maverick`**
# MAGIC 3. Haz clic en el icono **`Add endpoint`**
# MAGIC 4. Haz clic en el **seletor de modelos** y selecciona el modelo **`GPT OSS 20B`**
# MAGIC 5. Haz clic en el icono **`Add endpoint`**
# MAGIC 6. Haz clic en el **seletor de modelos** y selecciona el modelo **`Gemma 3 12B`**
# MAGIC 7. Añade la siguiente instrucción:
# MAGIC     ```
# MAGIC     Clasifique el sentimiento de la siguiente opinión:
# MAGIC     Compré una tablet y estoy muy insatisfecho con la calidad de la batería. Dura muy poco tiempo y tarda mucho en cargarse.
# MAGIC     ```
# MAGIC     <br>
# MAGIC 8. Haz clic en el icono **enviar**
# MAGIC
# MAGIC ¡Ahora podemos comparar las respuestas, el tiempo y el costo de cada modelo para elegir el que mejor se adapte a las necesidades de nuestro proyecto!

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC # Ejercicio 02 - Definiendo las herramientas
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/llm-tools-functions/llm-tools-functions-playground.gif?raw=true" style="float: right; padding-left: 10px" width=600>
# MAGIC
# MAGIC Las herramientas de los agentes de IA permiten que los agentes realicen tareas más allá de la generación de lenguaje, como recuperar datos estructurados o no estructurados y ejecutar código personalizado.
# MAGIC
# MAGIC Para crear una herramienta con **Mosaic AI Agent Framework**, puedes usar cualquier combinación de los siguientes métodos:
# MAGIC
# MAGIC |Método| Descripción|
# MAGIC |---|---|
# MAGIC |**Funciones de Unity Catalog**| - Definidas y gestionadas en Unity Catalog con recursos de seguridad y cumplimiento integrados <br> - Crea un registro central de herramientas que pueden ser gobernadas como otros objetos de Unity Catalog <br> - Ofrecen mayor facilidad de descubrimiento y reutilización <br> - Ideales para aplicar transformaciones y agregaciones en grandes conjuntos de datos|
# MAGIC |**Herramientas de código del agente**| - Definidas directamente en el código del agente de IA <br> - Útiles para invocar APIs REST, ejecutar código arbitrario o utilizar herramientas de baja latencia <br> - No cuentan con gobernanza integrada ni con facilidad de descubrimiento de funciones|

# COMMAND ----------

# MAGIC %md
# MAGIC ## a. Ejecución de procesamientos arbitrarios
# MAGIC
# MAGIC Las herramientas pueden ser muy útiles para definir las tareas que un agente puede ejecutar. Muchas veces, esas tareas son específicas de nuestro negocio y necesitamos definir cómo el agente las llevará a cabo. En esos casos, podemos utilizar una lógica arbitraria para desarrollar estas rutinas de forma más flexible.
# MAGIC
# MAGIC Algunos casos de uso son:
# MAGIC * Cálculos matemáticos
# MAGIC * Tratamiento de texto
# MAGIC * Aplicación de reglas de negocio
# MAGIC * Validaciones

# COMMAND ----------

# MAGIC %sql CREATE OR REPLACE FUNCTION valida_id(
# MAGIC   cpf STRING COMMENT 'Número identificador'
# MAGIC )
# MAGIC RETURNS BIGINT
# MAGIC LANGUAGE PYTHON
# MAGIC COMMENT 'Utiliza esta función para validar el número identificador y convertirlo en número. Devuelve -1 si el identificador no es válido.'
# MAGIC AS
# MAGIC $$
# MAGIC   cpf = cpf.replace(".", "").replace("-", "")
# MAGIC   if len(cpf) != 11:
# MAGIC     return False
# MAGIC   elif not cpf.isdigit():
# MAGIC     return False
# MAGIC   else:
# MAGIC     d1 = (int(cpf[0])*1 + int(cpf[1])*2 + int(cpf[2])*3 + int(cpf[3])*4 + int(cpf[4])*5 + int(cpf[5])*6 + int(cpf[6])*7 + int(cpf[7])*8 + int(cpf[8])*9) % 11 % 10
# MAGIC     d2 = (int(cpf[0])*0 + int(cpf[1])*1 + int(cpf[2])*2 + int(cpf[3])*3 + int(cpf[4])*4 + int(cpf[5])*5 + int(cpf[6])*6 + int(cpf[7])*7 + int(cpf[8])*8 + d1*9) % 11 % 10
# MAGIC     if d1 == int(cpf[9]) and d2 == int(cpf[10]):
# MAGIC       return int(cpf)
# MAGIC     else:
# MAGIC       return -1
# MAGIC $$

# COMMAND ----------

# MAGIC %sql SELECT valida_id("111.111.111-11")

# COMMAND ----------

# MAGIC %md
# MAGIC ## b. Consultando datos estruturados
# MAGIC
# MAGIC Para extraer el máximo valor de nuestros agentes, necesitamos que puedan acceder a nuestros datos corporativos. Solo con esta combinación podremos crear aplicaciones capaces de impactar al negocio.
# MAGIC
# MAGIC ### Acceso a datos del Lakehouse
# MAGIC
# MAGIC Veamos cómo acceder a los datos de nuestras tablas Delta.

# COMMAND ----------

# MAGIC %sql CREATE OR REPLACE FUNCTION consultar_cliente(id BIGINT)
# MAGIC RETURNS TABLE (id_cliente BIGINT, nombre STRING, apellido STRING, num_pedidos INT)
# MAGIC COMMENT 'Use esta función para consultar los datos de un cliente'
# MAGIC RETURN SELECT id_cliente, nombre, apellido, num_pedidos FROM academia.ia.clientes c WHERE c.id_cliente = consultar_cliente.id

# COMMAND ----------

# MAGIC %sql SELECT * FROM consultar_cliente(11111111111)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC <img src="https://docs.databricks.com/aws/en/assets/images/create-online-table-473e834357fdf2f576706b4a9100850f.png" style="float: right; width: 800px; margin-top: 70px; margin-left: 10px">
# MAGIC
# MAGIC ### Lakebase
# MAGIC
# MAGIC Para escenarios en los que necesitamos menores latencias, también podemos utilizar **[Databricks Lakebase](https://docs.databricks.com/aws/en/oltp/)**, que consiste en una copia de solo lectura de una tabla Delta, que está almacenada en un formato orientado a filas, optimizado para el acceso **online**.
# MAGIC
# MAGIC Lakebase es totalmente **serverless** y escala automaticamente la capacidad de procesamiento según la carga de solicitudes, proporcionando baja latencia y alto rendimiento en el acceso a los datos de cualquier escala.
# MAGIC
# MAGIC Además, Lakebase ofrece **integración** con Mosaic AI Model Serving, Feature Serving y aplicaciones de generación aumentada por recuperación (RAG), donde se utiliza para realizar consultas rápidas de datos.
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## c. Consultando datos no-estruturados
# MAGIC
# MAGIC <img src="https://www.databricks.com/sites/default/files/2024-01/db-vector-search-image-01_0.png?v=1705100714" style="float: right; width: 800px; margin-left: 10px">
# MAGIC
# MAGIC Sin embargo, muchas veces los datos a los que necesitamos acceder no son necesariamente estructurados o no buscamos una coincidencia exacta.
# MAGIC
# MAGIC **[Databricks Vector Search](https://docs.databricks.com/aws/en/generative-ai/vector-search)** es una base de datos vectorial serverless, **integrada** de forma transparente en la Data Intelligence Platform.
# MAGIC
# MAGIC A diferencia de otras bases de datos, Databricks Vector Search soporta la **sincronización automática** de los datos desde la fuente hacia el índice, eliminando el mantenimiento complejo y costoso de los pipelines.
# MAGIC
# MAGIC Databricks Vector Search aprovecha las mismas herramientas de **seguridad y gobernanza** de datos que las organizaciones ya han implementado, ofreciendo mayor tranquilidad.
# MAGIC
# MAGIC Gracias a su diseño serverless, Databricks Vector Search **escala** fácilmente para soportar miles de millones de embeddings y miles de consultas en tiempo real por segundo.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### i. Creando un Vector Search endpoint
# MAGIC
# MAGIC Para crear el endpoint, siga los siguientes pasos:
# MAGIC
# MAGIC 1. En el **menú principal** a la izquierda, haz clic en **Compute**
# MAGIC 1. En la parte superior, haz clic en la pestaña **Vector Search**
# MAGIC 1. En la esquina superior derecha, haz clic en **Create endpoint**
# MAGIC 1. Escriba el nombre: `academia-vs-endpoint`
# MAGIC 1. Haz clic en **Confirm**

# COMMAND ----------

# MAGIC %md
# MAGIC ### ii. Consultando productos similares
# MAGIC
# MAGIC Otro escenario interesante es el de las búsquedas por similitud.  
# MAGIC
# MAGIC Por ejemplo, un usuario puede estar buscando alguna característica específica de un producto que no esté categorizada dentro de los filtros existentes de nuestro sitio web.
# MAGIC
# MAGIC De esta manera, podemos buscar dentro de las descripciones de los productos y encontrar aquellos que sean más relevantes para el cliente, facilitando el descubrimiento de productos y aumentando las posibilidades de conversión.

# COMMAND ----------

# MAGIC %md
# MAGIC Para crear el índice en Vector Search, siga los pasos a continuación:
# MAGIC
# MAGIC 1. En el **menu principal** de la izquierda, haz clic en **Catalog**
# MAGIC 1. En la esquina superior izquierda, busque la tabla `productos`
# MAGIC 1. En la esquina superior derecha, haz clic en  **Create** > **Vector search index**
# MAGIC 1. Complete la siguiente información:
# MAGIC     - **Name:** productos_index
# MAGIC     - **Primary key:** id
# MAGIC     - **Embedding source column:** producto
# MAGIC     - **Embedding model:** databricks-gte-large-en
# MAGIC     - **Vector search endpoint:** academia-vs-endpoint
# MAGIC 1. Haz clic en **Create**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION buscar_prod_sim(descripcion STRING)
# MAGIC RETURNS TABLE (id LONG, producto STRING, descripcion STRING, search_score DOUBLE)
# MAGIC COMMENT 'Esta función recibe la descripción de un producto, que es utilizada para buscar productos similares'
# MAGIC RETURN
# MAGIC SELECT id, producto, descripcion, search_score
# MAGIC FROM vector_search(
# MAGIC   index => 'academia.ia.productos_index',
# MAGIC   query_text => buscar_prod_sim.descripcion,
# MAGIC   query_type => 'HYBRID',
# MAGIC   num_results => 10
# MAGIC )
# MAGIC ORDER BY search_score DESC
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %sql SELECT * FROM buscar_prod_sim("El auricular DEF es un dispositivo de audio diseñado para ofrecer una experiencia de sonido inmersiva y de alta calidad. Con controladores de alta fidelidad y tecnología de cancelación de ruido, permite disfrutar de la música o los detalles de un pódcast sin distracciones. Además, su diseño ergonómico garantiza comodidad durante el uso prolongado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## d. Usando prompt engineering
# MAGIC
# MAGIC Para personalizar el comportamiento de nuestros modelos de IA Generativa, podemos utilizar prompts curados por especialistas. De esta forma, conseguimos:
# MAGIC
# MAGIC * Aprovechar mejor el conocimiento de expertos en IA o en un dominio de negocio específico para aumentar la **eficiencia** de los agentes 
# MAGIC * Promover la **reutilización** de estos activos entre proyectos
# MAGIC * **Democratizar** el acceso a la IA para usuarios menos avanzados
# MAGIC
# MAGIC ### Personalización de respuestas
# MAGIC
# MAGIC Con todas las informaciones extraídas, podemos utilizarlas para generar sugerencias de respuestas personalizadas que aceleren el trabajo de nuestros equipos de atención.
# MAGIC
# MAGIC Otro punto interesante es que, en este proceso, podemos aprovechar otras **informaciones estructuradas** que ya tengamos en nuestro entorno, como datos demográficos, psicográficos y el historial de compras, para personalizar aún más nuestras respuestas!
# MAGIC
# MAGIC ¡Vamos a ver cómo hacerlo!

# COMMAND ----------

# MAGIC %sql CREATE OR REPLACE FUNCTION generar_respuesta(nombre STRING, apellido STRING, num_pedidos INT, producto STRING, motivo STRING)
# MAGIC RETURNS TABLE(respuesta STRING)
# MAGIC COMMENT 'Si el cliente muestra insatisfacción con algún producto, use esta función para generar una respuesta personalizada'
# MAGIC RETURN SELECT AI_QUERY(
# MAGIC     'databricks-gpt-oss-120b',
# MAGIC     CONCAT(
# MAGIC         "Eres un asistente virtual de un e-commerce. Nuestro cliente, ", generar_respuesta.nombre, " ", generar_respuesta.apellido, " que compró ", generar_respuesta.num_pedidos, " productos este año estaba insatisfecho con el producto ", generar_respuesta.producto, 
# MAGIC         ", porque ", generar_respuesta.motivo, ". Proporcione un breve mensaje empático para el cliente, incluyendo la oferta de cambio del producto si está en conformidad con nuestra política de devoluciones. El cambio puede realizarse directamente por este asistente. ",
# MAGIC         "Quiero recuperar su confianza y evitar que deje de ser nuestro cliente.",
# MAGIC         "Escribe un mensaje con pocas frases.",
# MAGIC         "No agregues ningún texto además del mensaje.",
# MAGIC         "No añadas ninguna firma."
# MAGIC     )
# MAGIC )

# COMMAND ----------

# MAGIC %sql SELECT * FROM generar_respuesta("Juan", "Silva", 23, "tablet DEF", "duración de la bateria")

# COMMAND ----------

# MAGIC %md
# MAGIC # Ejercicio 03 - Creando el agente
# MAGIC
# MAGIC Para decidir cuál es el mejor modelo e instrucción para nuestro caso de uso, podemos utilizar el **[AI Playground](https://docs.databricks.com/en/large-language-models/ai-playground.html)**.
# MAGIC
# MAGIC De esta forma, podemos probar rápidamente diversas combinaciones de modelos e instrucciones a través de una interfaz intuitiva y elegir la mejor opción para utilizar en nuestro proyecto.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ## a. Configurando el Agente
# MAGIC
# MAGIC <img src="https://docs.databricks.com/en/_images/ai-playground.gif" style="float: right; margin-top:20px; margin-bottom:50px; padding-left: 10px" width=600>
# MAGIC
# MAGIC Siga los pasos a continuación:
# MAGIC
# MAGIC 1. En el **menu principal** de la izquierda, haz clic en **Playground**
# MAGIC 1. Haz clic en el **seletor de modelos** y seleccione el modelo **GPT OSS 120B**
# MAGIC 1. Haz clic en el icono **Add endpoint**
# MAGIC 1. Añada la siguiente instrucción en **Add system prompt**:<br>
# MAGIC     `Eres un asistente virtual de un e-commerce. Para responder a las preguntas, es necesario que el cliente proporcione un identificador válido. Si aún no tienes esa información, solicita el identificador educadamente. Tras validar el identificador, recuerda consultar los datos del cliente para personalizar sus respuestas. Si el identificador del cliente no existe en nuestra base, pide educadamente un nuevo identificador. Puedes responder preguntas sobre entrega, devolución de productos, estado de pedidos, entre otros. Si no sabes cómo responder la pregunta, di que no lo sabes. No inventes ni especules sobre nada. Siempre que se te pregunte sobre procedimientos, consulta nuestra base de conocimiento.`
# MAGIC     <br>
# MAGIC 1. Haz clic en **Tools** > **Add tool**
# MAGIC 1. Añada las rutas a sus **herramientas**: 
# MAGIC     - `academia.ia.valida_id`
# MAGIC     - `academia.ia.consultar_cliente`
# MAGIC     - `academia.ia.buscar_prod_sim`
# MAGIC     - `academia.ia.generar_respuesta`
# MAGIC 1. Haz clic en el icono **Save** 

# COMMAND ----------

# MAGIC %md
# MAGIC ## b. Probando el Agente
# MAGIC
# MAGIC Ahora podemos evaluar las respuestas, el tiempo y el costo de nuestro agente para entender si cumple con las necesidades de nuestro proyecto.
# MAGIC
# MAGIC Envía los siguientes mensajes a tu agente:
# MAGIC 1. Hola!
# MAGIC 1. Mi identificador es 111.111.111-11
# MAGIC 1. Compré una tablet DEF, pero la duración de la batería es muy corta
# MAGIC 1. ¿Podrías sugerirme un producto mejor?

# COMMAND ----------

# MAGIC %md
# MAGIC # Ejercicio 04 - Evaluando e Implementando Agentes
# MAGIC
# MAGIC Después de crear el prototipo de nuestro agente, podemos avanzar a las etapas de evaluación e implementación.

# COMMAND ----------

# MAGIC %md
# MAGIC ## a. Evaluación del agente
# MAGIC
# MAGIC Antes de poner cualquier agente en producción, igual que con cualquier otro tipo de software, es extremadamente importante evaluar su calidad. Para automatizar este proceso, podemos usar los **[built-in AI Judges](https://docs.databricks.com/aws/en/mlflow3/genai/eval-monitor/concepts/judges/pre-built-judges-scorers)**. De esta forma, tenemos acceso a evaluadores desarrollados por el equipo de investigación de Databricks para tareas comunes de validación de calidad.
# MAGIC
# MAGIC Siga los pasos a continuación para evaluar el agente:
# MAGIC 1. En la parte superior central del Playground, haga clic en **Get code** > **Create agent notebook**
# MAGIC 1. En el notebook creado, ejecute las celdas de las siguientes secciones para crear el agente y observar sus respuestas:
# MAGIC     - Prerequisites
# MAGIC     - Define the agent in code
# MAGIC     - Test the agent
# MAGIC 1. En la sección **Log the agent as an MLflow model**, agregue el índice de Vector Search y la tabla de clientes a los recursos del agente:
# MAGIC ```
# MAGIC from mlflow.models.resources import DatabricksTable, DatabricksVectorSearchIndex
# MAGIC resources.append(DatabricksTable('academia.ia.clientes'))
# MAGIC resources.append(DatabricksVectorSearchIndex('academia.ia.productos_index'))
# MAGIC ```
# MAGIC 4. Ejecute las celdas de la sección **Evaluate the agent with Agent Evaluation** para evaluar el rendimiento del agente utilizando los AI Judges.
# MAGIC 1. Haz clic en **View evaluation results** para analizar el resultado de la evaluación.
# MAGIC 1. Ejecute las celdas de la sección **Perform pre-deployment validation of the agent** para validar la ejecución del agente desde MLflow.

# COMMAND ----------

# MAGIC %md
# MAGIC ## b. Implementando el agente como una API REST
# MAGIC
# MAGIC Ahora podemos llevar nuestro agente a producción. Para permitir una fácil integración con diversos sistemas y la ejecución del agente en tiempo real, lo implementaremos como una API REST.
# MAGIC
# MAGIC Para facilitar este proceso, utilizaremos:
# MAGIC   - **Unity Catalog** para versionar y controlar el acceso a los artefactos del agente
# MAGIC   - **[Model Serving](https://docs.databricks.com/aws/en/machine-learning/model-serving/)** para crear un endpoint serverless para el agente
# MAGIC
# MAGIC Siga los pasos a continuación para implementar el agente:
# MAGIC 1. En la sección **Register the model to Unity Catalog**:
# MAGIC     - Complete las siguientes variables:
# MAGIC         - **catalog:** academia
# MAGIC         - **schema:** ia
# MAGIC         - **model_name:** agente_atencion_cliente
# MAGIC     - Ejecute las celdas para registrar el agente en Unity Catalog.
# MAGIC 1. En la sección **Deploy the agent**, habilite la opción de scale to zero, como se muestra a continuación, y ejecute las celdas para implementar el agente como una API REST:
# MAGIC ```
# MAGIC from databricks import agents
# MAGIC agents.deploy(UC_MODEL_NAME, uc_registered_model_info.version, scale_to_zero=True, tags = {"endpointSource": "playground"})
# MAGIC ```
# MAGIC 3. Acceda al enlace generado junto a **View status** para visualizar el endpoint creado.
# MAGIC 1. Después de que el estado del endpoint cambie a **Ready**, haz clic en el botón **Use** para hablar con su agente.

# COMMAND ----------

# MAGIC %md
# MAGIC # ¡Felicidades!
# MAGIC
# MAGIC ¡Has completado el laboratorio de **Creando un agente**!
# MAGIC
# MAGIC Ahora pasemos al siguiente laboratorio: [Lab 03 - Usando Batch Inference]($./Lab 03 - Usando Batch Inference)