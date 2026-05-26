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
    pass  # Not available on Serverless

print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print(f"User:    {_user}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Churn Prediction Feature Engineering
# MAGIC Nuestro primer paso es analizar los datos y construir las features que usaremos para entrenar nuestro modelo. Veamos cómo se puede hacer.
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-1-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC <!-- Recopilar datos de uso (vista). Elimínelo para deshabilitar la recopilación o desactive el rastreador durante la instalación. Consulte el README para más detalles.  -->
# MAGIC <img width="1px" src="https://ppxrzfxige.execute-api.us-west-2.amazonaws.com/v1/analytics?category=data-science&org_id=1444828305810485&notebook=%2F01-mlops-quickstart%2F01_feature_engineering&demo_name=mlops-end2end&event=VIEW&path=%2F_dbdemos%2Fdata-science%2Fmlops-end2end%2F01-mlops-quickstart%2F01_feature_engineering&version=1&user_hash=f7ea13a45c991650d8df810431c3e0e2b12887e9ed7e206ee8fb6209bdb2ae82">

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilice un cluster Serverless Environment 2 para ejecutar este notebook
# MAGIC Para ejecutar esta demostración, simplemente selecciona el cluster `Serverless` en el menú desplegable.
# MAGIC Comprueba que la versión del cluster serverless es la número 2 <br />
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ![](https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/version2-serverless.png)

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Análisis exploratorio de datos
# MAGIC Para familiarizarse con los datos, identificar qué necesita limpieza, preprocesamiento, etc.
# MAGIC - **Utiliza las herramientas nativas de visualización de Databricks**
# MAGIC   - Después de ejecutar una consulta SQL en una celda del notebook, usa la pestaña `+` para agregar gráficos y visualizar los resultados.
# MAGIC - Trae tu propia librería de visualización preferida (por ejemplo, seaborn, plotly)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM mlops_churn_bronze_customers

# COMMAND ----------

telco_df = spark.read.table("mlops_churn_bronze_customers").pandas_api()
telco_df["internet_service"].value_counts().plot.pie()

# COMMAND ----------

# DBTITLE 1,Read in Bronze Delta table using Spark
# Leerlo con Spark
telcoDF = spark.read.table("mlops_churn_bronze_customers")
display(telcoDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Definir lógica de limpieza y creación de features
# MAGIC
# MAGIC Vamos a definir una función para limpiar los datos e implementar la lógica de creación de features. Para ello vamos a:
# MAGIC
# MAGIC 1. Calcular el número de servicios opcionales
# MAGIC 2. Proporcionar etiquetas significativas
# MAGIC 3. Imputar valores nulos
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Usando la API de Pandas en Spark
# MAGIC
# MAGIC Como nuestro equipo de científicos de datos está familiarizado con Pandas, utilizaremos la [API de pandas en spark](https://spark.apache.org/docs/latest/api/python/reference/pyspark.pandas/index.html) para escalar el código de `pandas`. Las instrucciones de Pandas se convertirán en el motor de spark internamente y se distribuirán a escala.
# MAGIC
# MAGIC *Nota: La API de Pandas en Spark antes se llamaba Koalas. A partir de `spark 3.2`, Koalas está incorporado y podemos obtener un DataFrame de Pandas usando `pandas_api()` [Detalles](https://spark.apache.org/docs/latest/api/python/migration_guide/koalas_to_pyspark.html).*

# COMMAND ----------

# DBTITLE 1,Define featurization function
import pyspark.sql.functions as F
from pyspark.sql import DataFrame


def clean_churn_features(dataDF: DataFrame) -> DataFrame:
  """
  Simple cleaning function leveraging pandas API
  """

  # Convert to pandas on spark dataframe
  data_psdf = dataDF.pandas_api()
  # Convert some columns
  data_psdf = data_psdf.astype({"senior_citizen": "string"})
  data_psdf["senior_citizen"] = data_psdf["senior_citizen"].map({"1" : "Yes", "0" : "No"})

  data_psdf["total_charges"] = data_psdf["total_charges"].apply(lambda x: float(x) if x.strip() else 0)


  # Fill some missing numerical values with 0
  data_psdf = data_psdf.fillna({"tenure": 0.0})
  data_psdf = data_psdf.fillna({"monthly_charges": 0.0})
  data_psdf = data_psdf.fillna({"total_charges": 0.0})

  def sum_optional_services(df):
      """Count number of optional services enabled, like streaming TV"""
      cols = ["online_security", "online_backup", "device_protection", "tech_support",
              "streaming_tv", "streaming_movies"]
      return sum(map(lambda c: (df[c] == "Yes"), cols))

  data_psdf["num_optional_services"] = sum_optional_services(data_psdf)

  # Return the cleaned Spark dataframe
  return data_psdf.to_spark()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC
# MAGIC ## Calcular features y guardar tabla con features y etiquetas
# MAGIC
# MAGIC Una vez que nuestras features estén listas, las guardaremos junto con las etiquetas como una tabla Delta Lake. Luego, esta tabla podrá ser recuperada para el entrenamiento del modelo.
# MAGIC
# MAGIC En esta demostración rápida, veremos cómo entrenar un modelo usando este conjunto de datos etiquetado guardado como una tabla Delta Lake y cómo capturar la trazabilidad entre la tabla y el modelo. La trazabilidad del modelo aporta control y gobernanza a nuestro despliegue, permitiéndonos saber qué modelo depende de qué conjunto de tablas de features.
# MAGIC
# MAGIC Databricks tiene una capacidad de Feature Store (almacén de features) totalmente integrada en la plataforma. Cualquier tabla Delta Lake con una clave primaria puede usarse como tabla de features para el entrenamiento de modelos y para el servicio batch y en línea. Veremos un ejemplo de cómo usar el Feature Store para realizar búsquedas de features en una demostración más avanzada.

# COMMAND ----------

# DBTITLE 1,Compute Churn Features and append a timestamp
churn_features = clean_churn_features(telcoDF)
display(churn_features)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Escribir la tabla para entrenamiento
# MAGIC
# MAGIC Escribe los datos etiquetados que tienen las features preparadas y las etiquetas como una tabla Delta. Luego usaremos esta tabla para entrenar el modelo para predecir la deserción.

# COMMAND ----------

# Specify train-test split
train_ratio, test_ratio = 0.8, 0.2
churn_features = (churn_features.withColumn("random", F.rand(seed=42))
                                .withColumn("split",
                                            F.when(F.col("random") < train_ratio, "train")
                                            .otherwise("test"))
                                .drop("random"))

# Write table for training
(churn_features.write.mode("overwrite")
               .option("overwriteSchema", "true")
               .saveAsTable("mlops_churn_training"))

# Add comment to the table
spark.sql(f"""COMMENT ON TABLE {catalog}.{db}.mlops_churn_training IS \'The features in this table are derived from the mlops_churn_bronze_customers table in the lakehouse. 
              We created service features and cleaned up their names.  No aggregations were performed.'""")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ¡Eso es todo! Las features etiquetadas ya están listas para ser utilizadas en el entrenamiento.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Entrenar un modelo de machine learning
# MAGIC
# MAGIC Próximo paso: [Entrenar un modelo lightGBM]($./02_3_train_lightGBM)
