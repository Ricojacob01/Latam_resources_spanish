# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `ardemo_classic_dnubtw_catalog`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta celda valida acceso y crea tu schema si no existe.
# MAGIC
# MAGIC IMPORTANTE: Configurar Notebook de "00-setup"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC %md
# MAGIC # Demostración de MLOps end-to-end con MLFlow, AutoML y Modelos en Unity Catalog
# MAGIC
# MAGIC ## Desafíos al llevar un proyecto de ML a producción
# MAGIC
# MAGIC Mover un proyecto de ML de un notebook independiente a una canalización de datos de nivel de producción es complejo y requiere múltiples competencias.
# MAGIC
# MAGIC Tener un modelo funcionando en un notebook no es suficiente. Necesitamos cubrir todo el ciclo de vida del proyecto de ML y resolver los siguientes desafíos:
# MAGIC
# MAGIC * Actualizar los datos con el tiempo (canalización de ingestión de nivel de producción)
# MAGIC * Cómo guardar, compartir y reutilizar características de ML en la organización
# MAGIC * Cómo asegurar que una nueva versión del modelo respete los estándares de calidad y no rompa la canalización
# MAGIC * Gobernanza del modelo: ¿qué está desplegado, cómo se entrenó, por quién y con qué datos?
# MAGIC * Cómo monitorear y reentrenar el modelo...
# MAGIC
# MAGIC Además, estos proyectos suelen involucrar a múltiples equipos, creando fricción y posibles silos
# MAGIC
# MAGIC * Ingenieros de datos encargados de ingerir, preparar y exponer los datos
# MAGIC * Científicos de datos, expertos en análisis de datos y construcción de modelos de ML
# MAGIC * Ingenieros de ML, que configuran las canalizaciones de infraestructura de ML (similar a DevOps)
# MAGIC
# MAGIC Esto tiene un impacto real en el negocio, ralentizando los proyectos y evitando que se desplieguen en producción y generen ROI.
# MAGIC
# MAGIC ## ¿Qué es MLOps?
# MAGIC
# MAGIC MLOps es un conjunto de estándares, herramientas, procesos y metodologías que buscan optimizar el tiempo, la eficiencia y la calidad, asegurando la gobernanza en los proyectos de ML.
# MAGIC
# MAGIC MLOps orquesta el ciclo de vida de un proyecto entre los equipos para implementar estas canalizaciones de ML de manera fluida.
# MAGIC
# MAGIC Databricks está especialmente posicionado para resolver este desafío con el patrón Lakehouse. No solo reunimos a Ingenieros de Datos, Científicos de Datos e Ingenieros de ML en una plataforma única, sino que también proporcionamos herramientas para orquestar proyectos de ML y acelerar la puesta en producción.
# MAGIC
# MAGIC ## Recorrido por el proceso de MLOps
# MAGIC
# MAGIC En esta demostración, repasaremos algunos pasos comunes en el proceso de MLOps. El resultado de este proceso es un modelo utilizado para alimentar un tablero para los interesados del negocio, que incluye:
# MAGIC * preparación de características
# MAGIC * entrenamiento de un modelo para su despliegue
# MAGIC * registro del modelo para que su uso esté gobernado
# MAGIC * validación del modelo en un análisis champion-challenger
# MAGIC * invocación de un modelo de ML entrenado como un UDF de PySpark
# MAGIC
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-0-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Recopilar datos de uso (vista). Elimínelo para deshabilitar la recopilación o desactive el rastreador durante la instalación. Consulte el README para más detalles.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F01-mlops-quickstart%2F00_mlops_end2end_quickstart_presentation&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F00_mlops_end2end_quickstart_presentation&version=1&user_hash=f7ea13a45c991650d8df810431c3e0e2b12887e9ed7e206ee8fb6209bdb2ae82">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilice un cluster Serverless Environment 2 para ejecutar este notebook
# MAGIC Para ejecutar esta demostración, simplemente selecciona el cluster `Serverless` en el menú desplegable.
# MAGIC Comprueba que la versión del cluster serverless es la número 2 <br />
# MAGIC

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Detección de abandono de clientes
# MAGIC
# MAGIC Para explorar MLOps, implementaremos un modelo de churn de clientes.
# MAGIC
# MAGIC Nuestro equipo de marketing nos pidió crear un tablero que rastree la evolución del riesgo de abandono. Además, necesitamos proporcionar a nuestro equipo de renovaciones una lista diaria de clientes en riesgo de churn para aumentar nuestros ingresos finales.
# MAGIC
# MAGIC Nuestro equipo de ingenieros de datos nos proporcionó un conjunto de datos que recopila información sobre nuestra base de clientes, incluyendo información de abandono. Ahí es donde comienza nuestra implementación.
# MAGIC
# MAGIC Veamos cómo podemos implementar dicho modelo y proporcionar a nuestros equipos de marketing y renovaciones tableros para rastrear y analizar nuestra predicción de churn.

# COMMAND ----------

# DBTITLE 1,Telco customer dataset exploration
telcoDF = spark.table("mlops_churn_bronze_customers")
display(telcoDF)
