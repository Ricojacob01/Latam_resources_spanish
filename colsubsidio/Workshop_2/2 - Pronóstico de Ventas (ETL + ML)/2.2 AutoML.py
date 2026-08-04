# Databricks notebook source
# MAGIC %md
# MAGIC # 2.2 · AutoML — modelo baseline automático
# MAGIC
# MAGIC **AutoML** de Databricks automatiza el entrenamiento y la selección de modelos: prueba múltiples
# MAGIC algoritmos e hiperparámetros, genera un **notebook por cada intento**, selecciona el **modelo campeón** y
# MAGIC produce un **notebook de exploración de datos**. Es el punto de partida ideal antes de afinar a mano.
# MAGIC
# MAGIC En este notebook aprenderás a lanzar un experimento AutoML de **regresión** (pronóstico de `label_unidades`)
# MAGIC de dos formas: por la **interfaz gráfica** (recomendada en el taller) y por la **API programática**.
# MAGIC
# MAGIC > **Nota de compute:** la **API `databricks.automl`** requiere un cluster con **Databricks Runtime for ML**
# MAGIC > (no serverless). Si estás en serverless, usa la ruta por interfaz gráfica, que funciona igual.

# COMMAND ----------

# DBTITLE 1,Parámetros
dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")
CATALOGO = dbutils.widgets.get("catalogo").strip()
usuario  = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO   = usuario.split("@")[0].replace(".", "_").replace("-", "_")
ESQUEMA  = f"ws2_{SUFIJO}"
FEATURE_TABLE = f"{CATALOGO}.{ESQUEMA}.ft_ventas_features"
print(f"Feature table para AutoML: {FEATURE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción A — Interfaz gráfica (recomendada)
# MAGIC
# MAGIC 1. En la barra lateral, ve a **Experiments** y elige **Create AutoML Experiment**
# MAGIC    *(o Machine Learning → AutoML)*.
# MAGIC 2. **ML problem type:** `Regression`.
# MAGIC 3. **Dataset:** selecciona la tabla `ft_ventas_features` de tu esquema `ws2_<usuario>`.
# MAGIC 4. **Prediction target:** `label_unidades`.
# MAGIC 5. **Excluir columnas** que no son predictoras: `fecha`, `periodo`
# MAGIC    (son identificadores, no características).
# MAGIC 6. En **Advanced**, fija la **métrica** a optimizar (p. ej. `r2` o `rmse`) y un **timeout** de ~10-15 min.
# MAGIC 7. Clic en **Start AutoML**.
# MAGIC
# MAGIC Al finalizar tendrás: el **mejor modelo** (con su notebook reproducible), un **notebook de exploración de
# MAGIC datos**, y todos los intentos registrados en el experimento MLflow — listos para comparar y registrar en
# MAGIC Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción B — API programática (requiere Runtime for ML)
# MAGIC
# MAGIC Si ejecutas en un cluster con Databricks Runtime for ML, puedes lanzar el mismo experimento por código.
# MAGIC Descomenta y ejecuta la celda siguiente.

# COMMAND ----------

# DBTITLE 1,AutoML por código (opcional)
# from databricks import automl
#
# df = spark.table(FEATURE_TABLE).drop("fecha", "periodo")
#
# summary = automl.regress(
#     dataset=df,
#     target_col="label_unidades",
#     primary_metric="r2",
#     timeout_minutes=15,
# )
#
# print("Mejor intento (trial):")
# print(summary.best_trial)
# print("\nNotebook del mejor modelo:", summary.best_trial.notebook_url)
# print("Experimento MLflow:", summary.experiment.experiment_id)
#
# # El modelo campeón puede registrarse luego en Unity Catalog (ver notebook 2.4):
# #   import mlflow
# #   mlflow.set_registry_uri("databricks-uc")
# #   mlflow.register_model(summary.best_trial.model_path, f"{CATALOGO}.{ESQUEMA}.modelo_pronostico_ventas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cuándo usar AutoML vs entrenamiento manual
# MAGIC
# MAGIC | AutoML | Entrenamiento manual (2.3) |
# MAGIC |--------|----------------------------|
# MAGIC | Baseline rápido, exploración inicial | Control total del pipeline y features |
# MAGIC | Genera notebooks reproducibles | Integración con Feature Store (`FeatureLookup`) |
# MAGIC | Ideal para comparar algoritmos | Ideal para afinar el modelo elegido |
# MAGIC
# MAGIC **Buena práctica:** usa AutoML para el baseline, toma su mejor notebook como punto de partida y refina en
# MAGIC el notebook **2.3** con Feature Store + seguimiento MLflow.
# MAGIC
# MAGIC **Siguiente:** `2.3 Entrenamiento con Feature Store + MLflow`.

