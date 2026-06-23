# Databricks notebook source
# DBTITLE 1,Setup header
# MAGIC %md
# MAGIC # _resources/00-setup — Setup compartido (CP/MLOps)
# MAGIC
# MAGIC Lo carga cada módulo y cada tarea del pipeline con `%run ../_resources/00-setup`.
# MAGIC Define catálogo/schema/nombres y descarga el dataset **IBM Telco Churn** si no existe.
# MAGIC
# MAGIC Variables expuestas: `CATALOG`/`catalog`, `SCHEMA`/`db`, `MODEL_NAME`, `SERVING_ENDPOINT`, `xp_path`, `xp_name`, helpers.

# COMMAND ----------

# DBTITLE 1,Widgets and variables
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.dropdown("setup_inference_data", "false", ["true", "false"], "Setup inference data")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
setup_inference_data = dbutils.widgets.get("setup_inference_data") == "true"

# --- Variables principales (ambos naming conventions) ---
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
reformat_current_user = current_user.split("@")[0].lower().replace(".", "_").replace("-", "_")

# Gire-style names (used by lab notebooks)
catalog = "ardemo_classic_dnubtw_catalog"
dbName = db = f"ws_{reformat_current_user}"

# CP-style names (used by CP-only notebooks like 05, 07, 07b)
CATALOG = catalog
SCHEMA = db
_user = current_user
_slug = reformat_current_user
MODEL_NAME = f"{catalog}.{db}.mlops_churn"
SERVING_ENDPOINT = f"mlops_churn_{_slug}"

spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")
spark.sql(f"USE SCHEMA {db}")

print(f"Usuario:           {current_user}")
print(f"Catalog.Schema:    {catalog}.{db}")
print(f"Modelo (UC):       {MODEL_NAME}")
print(f"Serving endpoint:  {SERVING_ENDPOINT}")

# COMMAND ----------

# DBTITLE 1,Pip install section
# MAGIC %pip install mlflow typing_extensions --upgrade -q

# COMMAND ----------

# DBTITLE 1,Imports, MLflow config, and data loading
# Only restart if mlflow couldn't be loaded (i.e., pip install was needed)
try:
    import mlflow
except ImportError:
    dbutils.library.restartPython()

import mlflow
import pandas as pd
import re
import warnings
import logging
from mlflow import MlflowClient

warnings.filterwarnings("ignore")
logging.getLogger("mlflow").setLevel(logging.ERROR)

# Re-define variables if lost after restartPython
try:
    current_user
except NameError:
    current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    reformat_current_user = current_user.split("@")[0].lower().replace(".", "_").replace("-", "_")
    catalog = "ardemo_classic_dnubtw_catalog"
    dbName = db = f"ws_{reformat_current_user}"
    CATALOG = catalog
    SCHEMA = db
    _user = current_user
    _slug = reformat_current_user
    MODEL_NAME = f"{catalog}.{db}.mlops_churn"
    SERVING_ENDPOINT = f"mlops_churn_{_slug}"
    reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
    setup_inference_data = dbutils.widgets.get("setup_inference_data") == "true"
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{db}")
    spark.sql(f"USE SCHEMA {db}")

# Unity Catalog como registro de modelos
mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()

# Experiment del lab — se crea bajo el usuario
xp_path = f"/Users/{current_user}/experiments"
xp_name = f"mlops_experiment_{current_user}"
try:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    w.workspace.mkdirs(path=xp_path)
except Exception as e:
    print(f"AVISO: no se pudo crear la carpeta de experiments en {xp_path}: {e}")

# --- Cargar dataset IBM Telco Churn si no existe ---
bronze_table_name = "mlops_churn_bronze_customers"

# Auto-detect: si la bronze existe pero total_charges no es string, fuerza reload.
needs_reload = reset_all_data or not spark.catalog.tableExists(bronze_table_name)
if not needs_reload:
    existing_dtypes = dict(spark.table(bronze_table_name).dtypes)
    if existing_dtypes.get("total_charges") != "string":
        print(f"Detectado bronze con total_charges={existing_dtypes.get('total_charges')} — forzando recarga.")
        needs_reload = True

if needs_reload:
    import requests
    from io import StringIO
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

    spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(bronze_table_name)
    print(f"OK — tabla {catalog}.{db}.{bronze_table_name} creada con {len(df)} filas")
else:
    print(f"Tabla {catalog}.{db}.{bronze_table_name} ya existe, omitiendo carga")

# COMMAND ----------

# DBTITLE 1,Inference data setup and helpers
# --- Setup de datos de inferencia (si se solicita) ---
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

# --- Helpers ---
def get_latest_model_version(model_name=MODEL_NAME):
    """Última versión registrada del modelo (útil para serving/jobs)."""
    versions = [int(v.version) for v in MlflowClient().search_model_versions(f"name='{model_name}'")]
    return max(versions) if versions else None

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

print("Setup listo ✓")
