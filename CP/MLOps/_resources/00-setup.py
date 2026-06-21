# Databricks notebook source
# MAGIC %md
# MAGIC # _resources/00-setup — Setup compartido (CP/MLOps)
# MAGIC
# MAGIC Lo carga cada módulo y cada tarea del pipeline con `%run ../_resources/00-setup`.
# MAGIC Define catálogo/schema/nombres y genera **datos sintéticos de churn** (workshop self-contained).
# MAGIC
# MAGIC Variables expuestas: `CATALOG`, `SCHEMA`, `MODEL_NAME`, `SERVING_ENDPOINT`, helpers.

# COMMAND ----------

import mlflow
from mlflow import MlflowClient

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
_slug = _user.split("@")[0].replace(".", "_").replace("-", "_")
SCHEMA = "ws_" + _slug

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

MODEL_NAME = f"{CATALOG}.{SCHEMA}.mlops_churn"
SERVING_ENDPOINT = f"mlops_churn_{_slug}"     # nombre del endpoint de Model Serving

mlflow.set_registry_uri("databricks-uc")

print(f"Usuario:           {_user}")
print(f"Catalog.Schema:    {CATALOG}.{SCHEMA}")
print(f"Modelo (UC):       {MODEL_NAME}")
print(f"Serving endpoint:  {SERVING_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generar datos sintéticos de churn (idempotente)

# COMMAND ----------

def _generate_churn_data():
    """Crea mlops_churn_bronze_customers si no existe (datos sintéticos de telecom)."""
    if spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.mlops_churn_bronze_customers"):
        print("✓ mlops_churn_bronze_customers ya existe — se omite la generación")
        return

    import numpy as np, pandas as pd
    rng = np.random.default_rng(42)
    n = 5000

    yn = lambda p=0.5: rng.choice(["Yes", "No"], n, p=[p, 1 - p])
    tenure = rng.integers(0, 73, n)
    monthly = np.round(rng.uniform(18, 120, n), 2)
    contract = rng.choice(["Month-to-month", "One year", "Two year"], n, p=[0.55, 0.25, 0.20])
    senior = rng.choice([0, 1], n, p=[0.84, 0.16])

    df = pd.DataFrame({
        "customer_id": [f"CUST{i:06d}" for i in range(n)],
        "gender": rng.choice(["Male", "Female"], n),
        "senior_citizen": senior,
        "partner": yn(0.48),
        "dependents": yn(0.30),
        "tenure": tenure,
        "phone_service": yn(0.90),
        "online_security": yn(0.35),
        "online_backup": yn(0.35),
        "device_protection": yn(0.35),
        "tech_support": yn(0.30),
        "streaming_tv": yn(0.40),
        "streaming_movies": yn(0.40),
        "contract": contract,
        "monthly_charges": monthly,
        "total_charges": np.round(monthly * np.maximum(tenure, 1) * rng.uniform(0.9, 1.1, n), 2),
    })

    # churn correlacionado con tenure bajo, contrato mes-a-mes y cargo alto
    risk = (0.45 * (df.tenure < 12) + 0.30 * (df.contract == "Month-to-month")
            + 0.15 * (df.monthly_charges > 80) + 0.10 * (df.senior_citizen == 1))
    df["churn"] = np.where(rng.uniform(0, 1, n) < risk.clip(0, 0.9), "Yes", "No")

    (spark.createDataFrame(df)
        .write.mode("overwrite").option("overwriteSchema", "true")
        .saveAsTable(f"{CATALOG}.{SCHEMA}.mlops_churn_bronze_customers"))
    print(f"✓ Generados {n} clientes sintéticos en mlops_churn_bronze_customers")

_generate_churn_data()

# COMMAND ----------

def get_latest_model_version(model_name=MODEL_NAME):
    """Última versión registrada del modelo (útil para serving/jobs)."""
    versions = [int(v.version) for v in MlflowClient().search_model_versions(f"name='{model_name}'")]
    return max(versions) if versions else None

print("Setup listo. Helpers: get_latest_model_version()")
