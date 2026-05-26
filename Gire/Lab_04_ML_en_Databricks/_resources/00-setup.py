# Databricks notebook source
# MAGIC %md
# MAGIC # Setup compartido para el Lab 04 (ML en Databricks)
# MAGIC
# MAGIC Este notebook es invocado por cada notebook del Lab 04 vía `%run ./_resources/00-setup`.
# MAGIC
# MAGIC Define `catalog`, `db`, carga el dataset Telco Churn si no existe y configura MLflow.

# COMMAND ----------

dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.dropdown("setup_inference_data", "false", ["true", "false"], "Setup inference data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
setup_inference_data = dbutils.widgets.get("setup_inference_data") == "true"

# COMMAND ----------

# Catálogo y schema estandarizados — coherente con todos los demás labs
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
reformat_current_user = current_user.split("@")[0].lower().replace(".", "_").replace("-", "_")

catalog = "workshop_databricks"
dbName = db = f"ws_{reformat_current_user}"

print(f"User:    {current_user}")
print(f"Catalog: {catalog}")
print(f"Schema:  {db}")

# COMMAND ----------

# Crear schema si no existe y usarlo
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")
spark.sql(f"USE SCHEMA {catalog}.{db}")

# COMMAND ----------

import mlflow
import pandas as pd
import re
import warnings
import logging
from mlflow import MlflowClient

warnings.filterwarnings("ignore")
logging.getLogger("mlflow").setLevel(logging.ERROR)

# Unity Catalog como registro de modelos
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# Experiment del lab — se crea bajo el usuario
xp_path = f"/Users/{current_user}/experiments"
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    w.workspace.mkdirs(path=xp_path)
except Exception as e:
    print(f"AVISO: no se pudo crear la carpeta de experiments en {xp_path}: {e}")

# COMMAND ----------

# Cargar el dataset Telco Churn si no existe (o si reset_all_data=true)
bronze_table_name = "mlops_churn_bronze_customers"

if reset_all_data or not spark.catalog.tableExists(bronze_table_name):
    import requests
    from io import StringIO
    # Apache-licensed dataset: https://github.com/IBM/telco-customer-churn-on-icp4d
    print("Descargando dataset Telco Churn...")
    csv = requests.get(
        "https://raw.githubusercontent.com/IBM/telco-customer-churn-on-icp4d/master/data/Telco-Customer-Churn.csv"
    ).text
    df = pd.read_csv(StringIO(csv), sep=",")

    # Normalizar nombres de columnas a snake_case
    df.columns = [re.sub(r"(?<!^)(?=[A-Z])", "_", n).lower().replace("__", "_") for n in df.columns]
    df.columns = [re.sub(r"[\(\)]", "", n).lower() for n in df.columns]
    df.columns = [re.sub(r"[ -]", "_", n).lower() for n in df.columns]
    df = df.rename(columns={"streaming_t_v": "streaming_tv", "customer_i_d": "customer_id"})

    # Convertir TotalCharges (que tiene strings vacíos) a numérico
    df["total_charges"] = pd.to_numeric(df["total_charges"], errors="coerce")

    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)
    print(f"OK — tabla {catalog}.{db}.{bronze_table_name} creada con {len(df)} filas")
else:
    print(f"Tabla {catalog}.{db}.{bronze_table_name} ya existe, omitiendo carga")

# COMMAND ----------

# Setup específico para batch_inference (último notebook del lab):
# crea una copia del training table sin la columna 'churn' para inferencia
quickstart_training_table_name = "mlops_churn_training"
quickstart_unlabelled_table_name = "mlops_churn_inference"

if setup_inference_data:
    if spark.catalog.tableExists(f"{catalog}.{db}.{quickstart_training_table_name}"):
        if not spark.catalog.tableExists(f"{catalog}.{db}.{quickstart_unlabelled_table_name}"):
            print("Creando tabla sin labels para inferencia...")
            (spark.read.table(quickstart_training_table_name)
                .drop("churn")
                .write.mode("overwrite").option("overwriteSchema", "true")
                .saveAsTable(quickstart_unlabelled_table_name))
    else:
        print(f"Tabla {quickstart_training_table_name} no existe. Ejecuta primero 01_feature_engineering.")

# COMMAND ----------

def delete_feature_store_table(catalog, db, feature_table_name):
    """Helper para limpiar tablas del Feature Store en re-ejecuciones."""
    from databricks.feature_engineering import FeatureEngineeringClient
    fe = FeatureEngineeringClient()
    try:
        fe.drop_table(name=f"{catalog}.{db}.{feature_table_name}")
        spark.sql(f"DROP TABLE IF EXISTS {catalog}.{db}.{feature_table_name}")
        print(f"Drop Feature Table {catalog}.{db}.{feature_table_name}")
    except ValueError:
        print(f"Feature Table {catalog}.{db}.{feature_table_name} no existe")

