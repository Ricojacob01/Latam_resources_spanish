# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — AutoML, Entrenamiento y Tracking 🤖📒
# MAGIC
# MAGIC Generamos un baseline con **AutoML** y entrenamos un **LightGBM** con **MLflow tracking**.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Lado a lado (UI ↔ API)**
# MAGIC AutoML es la **misma capacidad** desde la **UI glass-box** y desde la **API**. Lanza ambas y compara: la UI para explorar e iterar, la API para reproducir en CI/CD. Luego entrenamos un modelo propio en código con MLflow y lo comparamos en la **Experiments UI**.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — AutoML lado a lado
# MAGIC
# MAGIC **UI (🖱️):** Experiments → **Create AutoML Experiment** → *Classification* → dataset `mlops_churn_training`, target `churn`, *timeout* 10 min. AutoML genera trials + el **notebook glass-box** del mejor modelo.
# MAGIC
# MAGIC **API (código):** lo de abajo hace lo mismo de forma reproducible. (Descomenta para correrlo; tarda ~10 min.)

# COMMAND ----------

# from databricks import automl
# summary = automl.classify(
#     dataset=spark.table("mlops_churn_training").drop("split", "customer_id"),
#     target_col="churn",
#     timeout_minutes=10,
# )
# print("Mejor run AutoML:", summary.best_trial.mlflow_run_id)
print("AutoML (API) disponible arriba. Para el lab seguimos con el LightGBM de abajo (más rápido).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Entrenar LightGBM con MLflow tracking (código)

# COMMAND ----------

import mlflow
from mlflow.models.signature import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import f1_score
from lightgbm import LGBMClassifier

pdf = spark.table("mlops_churn_training").toPandas()
label = "churn"
drop_cols = ["churn", "split", "customer_id"]
X = pdf.drop(columns=drop_cols)
y = (pdf[label] == "Yes").astype(int)
is_train = pdf["split"] == "train"
X_train, y_train = X[is_train], y[is_train]
X_test, y_test = X[~is_train], y[~is_train]

num_cols = X.select_dtypes("number").columns.tolist()
cat_cols = [c for c in X.columns if c not in num_cols]

pre = ColumnTransformer([
    ("num", Pipeline([("imp", SimpleImputer()), ("sc", StandardScaler())]), num_cols),
    ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
])
model = Pipeline([("pre", pre), ("clf", LGBMClassifier(n_estimators=200, learning_rate=0.05))])

# experimento bajo el usuario
xp = f"/Users/{_user}/mlops_churn_cp"
mlflow.set_experiment(xp)

with mlflow.start_run(run_name="lgbm_cp") as run:
    mlflow.sklearn.autolog(log_models=False, silent=True)
    model.fit(X_train, y_train)
    val_f1 = f1_score(y_test, model.predict(X_test))
    mlflow.log_metric("val_f1_score", val_f1)
    sig = infer_signature(X_train, model.predict(X_train))
    mlflow.sklearn.log_model(model, "sklearn_model",
                             input_example=X_train.iloc[:2], signature=sig)
    RUN_ID = run.info.run_id

print(f"✓ run_id = {RUN_ID}  ·  val_f1_score = {val_f1:.4f}")
dbutils.jobs.taskValues.set(key="run_id", value=RUN_ID) if hasattr(dbutils, "jobs") else None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Comparar en la Experiments UI (🖱️)
# MAGIC
# MAGIC 1. **Sidebar → Experiments → `mlops_churn_cp`**.
# MAGIC 2. Ordena los runs por `val_f1_score`. Abre el run `lgbm_cp`: params, métricas, **artefacto del modelo**, `input_example` y **signature**.
# MAGIC 3. (Si corriste AutoML) compara el F1 de tu LightGBM contra el mejor trial de AutoML.
# MAGIC
# MAGIC ## Continuar → `04 - Registro en UC y Champion-Challenger`
