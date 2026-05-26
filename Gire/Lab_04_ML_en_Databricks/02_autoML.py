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
# MAGIC ## AutoML para acelerar el desarrollo
# MAGIC
# MAGIC AutoML en Databricks permite automatizar el proceso de entrenamiento y selección de modelos, como de clasificación, lo que agiliza significativamente el desarrollo de soluciones de machine learning. Con AutoML, puedes explorar rápidamente diferentes algoritmos y configuraciones sin necesidad de ajustar manualmente los hiperparámetros, ahorrando tiempo y recursos.
# MAGIC
# MAGIC En este notebook aprenderás a:
# MAGIC * Iniciar un experimento de clasificación con AutoML en Databricks;
# MAGIC * Analizar los resultados y seleccionar el mejor modelo automáticamente;
# MAGIC * Registrar el modelo en MLflow para facilitar su gestión y despliegue.
# MAGIC
# MAGIC Esta automatización te ayuda a enfocarte en la solución del problema y en la interpretación de resultados, acelerando el ciclo de desarrollo y reduciendo errores manuales.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ve a la pestaña Experiments

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/automl.png" width="350">

# COMMAND ----------

# MAGIC %md
# MAGIC **Clica en "Classification"**

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/automl2.png" width="1500">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rellena los valores como en el ejemplo abajo

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/automl3.png" width="1500">

# COMMAND ----------

# MAGIC %md
# MAGIC **Clica en "Start AutoML"**

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/automl4.png" width="1500">

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Cuando finaliza un experimento de AutoML, se generan **múltiples pruebas con diferentes algoritmos y configuraciones**. Puedes analizar el código de cada prueba accediendo a los notebooks generados para cada ejecución. Además, el sistema selecciona automáticamente el modelo campeón, que es el que obtuvo el mejor desempeño según la métrica definida. También se crea un **notebook de exploración de datos**, donde puedes revisar el análisis inicial realizado sobre el dataset antes del entrenamiento. Esto te permite entender mejor los resultados y el proceso de selección del modelo.
