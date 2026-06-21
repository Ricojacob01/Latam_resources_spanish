# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — LAB 🧪 · Del notebook a Unity Catalog
# MAGIC
# MAGIC **70 min.** Recorres el pipeline ML completo (predicción de churn) y dejas un modelo **Champion** gobernado en Unity Catalog, listo para serving.
# MAGIC
# MAGIC Este módulo es la **guía** que enmarca los notebooks hands-on de `labs/ml_en_databricks/`. Ábrelos en orden; aquí está el mapa, las decisiones UI vs Code y los puntos clave.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este módulo — Mixto por sub-tarea (deliberado)
# MAGIC
# MAGIC | Notebook (en `labs/ml_en_databricks/`) | Sub-tarea | Patrón | Por qué |
# MAGIC |---|---|---|---|
# MAGIC | `01_feature_engineering` | Features | **Code** | Transformaciones → código natural. |
# MAGIC | `02_autoML` | AutoML | **Lado a lado** | UI glass-box *y* API `automl.classify`. |
# MAGIC | `03_train_lightGBM` | Entrenamiento + tracking | **Code (+ MLflow UI)** | Entrenas en código; comparas runs en la Experiments UI. |
# MAGIC | `04_models_in_uc` | Registro | **Code → UI** | Registras por API; gobiernas en Models UI. |
# MAGIC | `05_challenger_validation` | Promoción | **Code (+ UI)** | Lógica en código; tags/desc en la UI del modelo. |
# MAGIC | `06_batch_inference` | Scoring | **Code (+ UI)** | `spark_udf`/pandas; tabla en Catalog Explorer. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Feature engineering (`01_feature_engineering`)  · **Code**
# MAGIC
# MAGIC Limpieza + creación de features (ej. `num_optional_services`) sobre datos de churn, dejando `mlops_churn_training`. Patrón clave (pandas-on-Spark):
# MAGIC ```python
# MAGIC def clean_churn_features(df):
# MAGIC     p = df.pandas_api()
# MAGIC     p["senior_citizen"] = p["senior_citizen"].astype("string").map({"1":"Yes","0":"No"})
# MAGIC     p = p.fillna({"tenure":0.0,"monthly_charges":0.0,"total_charges":0.0})
# MAGIC     ...
# MAGIC     return p.to_spark()
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — AutoML (`02_autoML`)  · **Lado a lado (UI ↔ API)**
# MAGIC
# MAGIC **UI (glass-box):** Experiments → **Create AutoML Experiment** → Classification → dataset `mlops_churn_training`, target `churn`. AutoML prueba algoritmos y genera el **notebook del mejor modelo** (editable, no caja negra).
# MAGIC
# MAGIC **API (reproducible):**
# MAGIC ```python
# MAGIC from databricks import automl
# MAGIC run = automl.classify(dataset=spark.table("mlops_churn_training"),
# MAGIC                       target_col="churn", timeout_minutes=10)
# MAGIC print(run.best_trial.mlflow_run_id)
# MAGIC ```
# MAGIC Haz **las dos** y compara: misma capacidad, una para explorar, otra para CI/CD.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Entrenamiento + tracking (`03_train_lightGBM`)  · **Code + MLflow UI**
# MAGIC
# MAGIC Entrenas un LightGBM con `mlflow.sklearn.autolog()`, registras dataset (`log_input`) y métricas. Luego compara runs en la **Experiments UI** (orden por `val_f1_score`, gráficos).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Registro en Unity Catalog (`04_models_in_uc`)  · **Code → UI**
# MAGIC
# MAGIC ```python
# MAGIC mlflow.set_registry_uri("databricks-uc")
# MAGIC best = mlflow.search_runs(experiment_ids=exp_id, order_by=["metrics.val_f1_score DESC"], max_results=1)
# MAGIC d = mlflow.register_model(f"runs:/{best.iloc[0].run_id}/sklearn_model", f"{catalog}.{db}.mlops_churn")
# MAGIC client.set_registered_model_alias(f"{catalog}.{db}.mlops_churn", "Challenger", d.version)
# MAGIC ```
# MAGIC Luego **UI:** Catalog → Models → `mlops_churn` → ve versiones, alias **@Challenger**, descripción, **lineage** (qué run, qué datos) y **Permissions**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5 — Validación del Challenger (`05_challenger_validation`)  · **Code (+ UI)**
# MAGIC
# MAGIC Comparas Challenger vs Champion (descripción mínima, F1 ≥ Champion, impacto de negocio). Si pasa todos los checks, promueves:
# MAGIC ```python
# MAGIC if has_description and metric_f1_passed:
# MAGIC     client.set_registered_model_alias(name, "Champion", version)
# MAGIC ```
# MAGIC En la UI del modelo verás los **tags** (`has_description`, `metric_f1_passed`) que dejó la validación.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6 — Batch inference (`06_batch_inference`)  · **Code (+ UI)**
# MAGIC
# MAGIC ```python
# MAGIC udf = mlflow.pyfunc.spark_udf(spark, f"models:/{catalog}.{db}.mlops_churn@Champion", env_manager="virtualenv")
# MAGIC cols = udf.metadata.get_input_schema().input_names()
# MAGIC scored = spark.table("mlops_churn_inference").withColumn("prediction", udf(*cols))
# MAGIC ```
# MAGIC Explora la tabla puntuada en **Catalog Explorer**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación rápida — ¿quedó el Champion en UC?

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
mlflow.set_registry_uri("databricks-uc")

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
MODEL = f"{CATALOG}.{SCHEMA}.mlops_churn"

try:
    champ = MlflowClient().get_model_version_by_alias(MODEL, "Champion")
    print(f"✅ Champion listo: {MODEL} v{champ.version}")
    print("   → Sigue en ../../CP/MLOps/ para servirlo (endpoint) y orquestarlo (Job).")
except Exception as e:
    print(f"Aún no hay Champion. Completa los notebooks de labs/ml_en_databricks. Detalle: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Features → AutoML (UI + API) → train + tracking → registro UC → Champion/Challenger → batch
# MAGIC ✅ Cada sub-tarea con su patrón UI/Code deliberado
# MAGIC ✅ Modelo **Champion** gobernado en Unity Catalog
# MAGIC
# MAGIC ## Continuar → `05 - Cierre y Workshop Preview` · y luego `../../CP/MLOps/`
