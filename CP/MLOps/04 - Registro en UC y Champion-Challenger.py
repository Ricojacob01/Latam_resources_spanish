# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Registro en UC y Champion/Challenger 🏛️
# MAGIC
# MAGIC Registramos el mejor modelo en Unity Catalog, le ponemos alias **@Challenger**, lo **validamos** y lo promovemos a **@Champion**.
# MAGIC
# MAGIC ## 🧭 Enfoque UI vs Code — **Code → UI**
# MAGIC Registras y asignas alias por **API** (reproducible, automatizable en el Job); luego **gobiernas en *Models in Unity Catalog* (UI)**: versiones, alias, lineage, permisos, tags.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 — Registrar el mejor run como @Challenger (código)

# COMMAND ----------

import mlflow
from mlflow import MlflowClient
client = MlflowClient()

xp = mlflow.set_experiment(f"/Users/{_user}/mlops_churn_cp")
best = mlflow.search_runs(experiment_ids=[xp.experiment_id],
                          order_by=["metrics.val_f1_score DESC"], max_results=1)
run_id = best.iloc[0]["run_id"]
best_f1 = float(best.iloc[0]["metrics.val_f1_score"])

d = mlflow.register_model(f"runs:/{run_id}/sklearn_model", MODEL_NAME)
client.update_registered_model(MODEL_NAME, description="Predice churn de clientes telecom. Workshop CP/MLOps.")
client.update_model_version(MODEL_NAME, d.version,
    description=f"LightGBM. F1 de validación = {round(best_f1*100,2)}%.")
client.set_model_version_tag(MODEL_NAME, d.version, "f1_score", f"{round(best_f1,4)}")
client.set_registered_model_alias(MODEL_NAME, "Challenger", d.version)
print(f"✓ Registrado {MODEL_NAME} v{d.version} como @Challenger (F1={best_f1:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 — Validar el Challenger (código)
# MAGIC
# MAGIC Checks: (1) descripción mínima, (2) F1 ≥ Champion (o no hay Champion aún).

# COMMAND ----------

ch = client.get_model_version_by_alias(MODEL_NAME, "Challenger")
has_desc = bool(ch.description and len(ch.description) > 20)
client.set_model_version_tag(MODEL_NAME, ch.version, "has_description", str(has_desc))

ch_f1 = mlflow.get_run(ch.run_id).data.metrics["val_f1_score"]
try:
    champ = client.get_model_version_by_alias(MODEL_NAME, "Champion")
    champ_f1 = mlflow.get_run(champ.run_id).data.metrics["val_f1_score"]
    f1_passed = ch_f1 >= champ_f1
    print(f"Challenger F1={ch_f1:.4f} vs Champion F1={champ_f1:.4f}")
except Exception:
    f1_passed = True
    print(f"No hay Champion previo → Challenger F1={ch_f1:.4f} pasa por default")
client.set_model_version_tag(MODEL_NAME, ch.version, "metric_f1_passed", str(f1_passed))

print(f"Checks → has_description={has_desc} · metric_f1_passed={f1_passed}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Promover a @Champion si pasa (código)

# COMMAND ----------

if has_desc and f1_passed:
    client.set_registered_model_alias(MODEL_NAME, "Champion", ch.version)
    print(f"🏆 Promovido a @Champion: {MODEL_NAME} v{ch.version}")
else:
    print("❌ No promovido — revisa los checks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 — Gobernar en la UI (🖱️)
# MAGIC
# MAGIC **Catalog → Models → `mlops_churn`**:
# MAGIC - Versiones y aliases **@Champion / @Challenger**.
# MAGIC - Tags `f1_score`, `has_description`, `metric_f1_passed` (los dejó la validación).
# MAGIC - **Lineage**: del run y el dataset de entrenamiento al modelo.
# MAGIC - **Permissions**: quién puede usar/desplegar el modelo.
# MAGIC
# MAGIC ## Continuar → `05 - Model Serving (UI + API)` ⭐
