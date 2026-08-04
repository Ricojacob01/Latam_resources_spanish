# Databricks notebook source
# MAGIC %md
# MAGIC # 2.3 · Entrenamiento con Feature Store + MLflow
# MAGIC
# MAGIC Entrenamos el modelo de pronóstico usando la **feature table** del notebook 2.1 mediante un
# MAGIC **`FeatureLookup`** (el modelo queda "empaquetado" con sus features, para que la inferencia sea
# MAGIC consistente). Aplicamos las buenas prácticas del workshop de referencia:
# MAGIC
# MAGIC * **MLflow autolog** — registro automático de parámetros, métricas y artefactos
# MAGIC * **Linaje de datos** — `mlflow.data.load_delta` + `mlflow.log_input`
# MAGIC * **Firma del modelo** — `infer_signature`
# MAGIC * **Evaluación** — `mlflow.evaluate` sobre validación
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-2-v2.png?raw=true" width="1000">

# COMMAND ----------

# DBTITLE 1,Dependencias
# MAGIC %pip install --quiet databricks-feature-engineering mlflow scikit-learn --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parámetros y experimento MLflow
import mlflow
dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")

CATALOGO = dbutils.widgets.get("catalogo").strip()
usuario  = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO   = usuario.split("@")[0].replace(".", "_").replace("-", "_")
ESQUEMA  = f"ws2_{SUFIJO}"
FQN      = f"{CATALOGO}.{ESQUEMA}"

FEATURE_TABLE = f"{FQN}.ft_ventas_features"
MODELO        = f"{FQN}.modelo_pronostico_ventas"

mlflow.set_registry_uri("databricks-uc")
xp_path = f"/Users/{usuario}/ws2_pronostico_experiment"
mlflow.set_experiment(xp_path)
print(f"Feature table : {FEATURE_TABLE}")
print(f"Modelo (UC)   : {MODELO}")
print(f"Experimento   : {xp_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Construir el training set con Feature Store
# MAGIC
# MAGIC Partimos de un DataFrame "spine" con las claves + la etiqueta, y dejamos que el Feature Store haga el
# MAGIC **join de las features** vía `FeatureLookup`. Así el modelo registra qué features usó (linaje) y la
# MAGIC inferencia batch podrá recuperarlas automáticamente.

# COMMAND ----------

# DBTITLE 1,Crear el training set con FeatureLookup
from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup

fe = FeatureEngineeringClient()

FEATURE_COLS = ["lag_1","lag_7","lag_14","lag_28","ma_7","ma_28","dow","mes","doy",
                "es_finde","es_decembrina","ipc_inflacion_anual","trm","tasa_interes","icc_confianza_consumidor"]

# Spine: claves primarias + etiqueta (leemos la etiqueta desde la misma feature table)
spine = spark.table(FEATURE_TABLE).select("producto_familia", "fecha", "label_unidades")

training_set = fe.create_training_set(
    df=spine,
    feature_lookups=[FeatureLookup(
        table_name=FEATURE_TABLE,
        lookup_key=["producto_familia", "fecha"],
        feature_names=FEATURE_COLS,
    )],
    label="label_unidades",
    exclude_columns=["fecha"],   # la fecha no es predictora
)

training_pdf = training_set.load_df().toPandas()
print(f"✔ Training set: {training_pdf.shape[0]:,} filas × {training_pdf.shape[1]} columnas")
display(training_set.load_df().limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Linaje de datos
# MAGIC
# MAGIC Capturamos el linaje de la feature table para poder hacer análisis de causa raíz si el modelo se degrada.

# COMMAND ----------

# DBTITLE 1,Cargar el dataset como objeto MLflow (linaje)
latest_version = max(
    spark.sql(f"DESCRIBE HISTORY {FEATURE_TABLE}").toPandas()["version"]
)
src_dataset = mlflow.data.load_delta(table_name=FEATURE_TABLE, version=str(latest_version))
print(f"✔ Feature table version para linaje: {latest_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Entrenar con MLflow
# MAGIC
# MAGIC Split temporal-agnóstico (aleatorio 80/20 para simplicidad del taller), `GradientBoostingRegressor`
# MAGIC dentro de un `Pipeline` con `OneHotEncoder` para `producto_familia`. Registramos el modelo con la API del
# MAGIC **Feature Store** (`fe.log_model`) para conservar el linaje de features.

# COMMAND ----------

# DBTITLE 1,Entrenar y registrar con fe.log_model
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_absolute_percentage_error, r2_score

X = training_pdf.drop(columns=["label_unidades"])
y = training_pdf["label_unidades"]
X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, random_state=42)

pre = ColumnTransformer(
    [("fam", OneHotEncoder(handle_unknown="ignore"), ["producto_familia"])],
    remainder="passthrough")
modelo_pipe = Pipeline([
    ("preprocessor", pre),
    ("regressor", GradientBoostingRegressor(n_estimators=300, max_depth=3,
                                            learning_rate=0.05, random_state=42)),
])

mlflow.sklearn.autolog(log_models=False, silent=True)

with mlflow.start_run(run_name="gbr_baseline") as run:
    modelo_pipe.fit(X_train, y_train)
    pred = modelo_pipe.predict(X_val)

    mape = mean_absolute_percentage_error(y_val, pred)
    r2   = r2_score(y_val, pred)
    precision = round((1 - mape) * 100, 2)
    mlflow.log_metrics({"val_mape": mape, "val_r2": r2, "val_precision_pct": precision})

    # Linaje de datos de origen
    mlflow.log_input(src_dataset, context="training-input")

    # Registrar con Feature Store → conserva el FeatureLookup para inferencia
    fe.log_model(
        model=modelo_pipe,
        artifact_path="modelo",
        flavor=mlflow.sklearn,
        training_set=training_set,
        registered_model_name=MODELO,
    )
    run_id = run.info.run_id

print(f"✔ MAPE      : {mape:.4f}")
print(f"✔ R²        : {r2:.4f}")
print(f"✔ Precisión : {precision}%   (meta > 95%)")
print(f"✔ run_id    : {run_id}")
print(f"✔ Modelo registrado en UC (con linaje de features): {MODELO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC El modelo quedó registrado en Unity Catalog **con su Feature Store lineage**. Continúa con:
# MAGIC
# MAGIC * **2.4 Registro y ciclo de vida** — asignar alias `@Challenger` / `@Champion`
# MAGIC * **2.5 Inferencia batch** — pronóstico con `fe.score_batch` y write-back a SAP HANA

