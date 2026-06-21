# Databricks notebook source
# MAGIC %md
# MAGIC # pipeline/02_train_and_register — Tarea del Job
# MAGIC Entrena LightGBM, lo trackea en MLflow y lo registra en UC como **@Challenger**.

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
from mlflow.models.signature import infer_signature
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.metrics import f1_score
from lightgbm import LGBMClassifier

pdf = spark.table("mlops_churn_training").toPandas()
y = (pdf["churn"] == "Yes").astype(int)
X = pdf.drop(columns=["churn", "split", "customer_id"])
tr = pdf["split"] == "train"
num = X.select_dtypes("number").columns.tolist()
cat = [c for c in X.columns if c not in num]

pre = ColumnTransformer([
    ("num", Pipeline([("imp", SimpleImputer()), ("sc", StandardScaler())]), num),
    ("cat", OneHotEncoder(handle_unknown="ignore"), cat)])
model = Pipeline([("pre", pre), ("clf", LGBMClassifier(n_estimators=200, learning_rate=0.05))])

mlflow.set_experiment(f"/Users/{_user}/mlops_churn_cp")
with mlflow.start_run(run_name="job_train") as run:
    mlflow.sklearn.autolog(log_models=False, silent=True)
    model.fit(X[tr], y[tr])
    val_f1 = f1_score(y[~tr], model.predict(X[~tr]))
    mlflow.log_metric("val_f1_score", val_f1)
    mlflow.sklearn.log_model(model, "sklearn_model",
        input_example=X[tr].iloc[:2], signature=infer_signature(X[tr], model.predict(X[tr])))
    run_id = run.info.run_id

client = MlflowClient()
d = mlflow.register_model(f"runs:/{run_id}/sklearn_model", MODEL_NAME)
client.update_model_version(MODEL_NAME, d.version, description=f"LightGBM (Job). F1={round(val_f1*100,2)}%.")
client.set_model_version_tag(MODEL_NAME, d.version, "f1_score", f"{round(val_f1,4)}")
client.set_registered_model_alias(MODEL_NAME, "Challenger", d.version)
print(f"✓ registrado v{d.version} como @Challenger (F1={val_f1:.4f})")
