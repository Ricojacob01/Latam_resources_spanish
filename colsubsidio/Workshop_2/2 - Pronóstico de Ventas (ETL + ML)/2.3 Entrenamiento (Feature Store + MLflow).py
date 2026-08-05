# Databricks notebook source
# MAGIC %md
# MAGIC # 2.3 · Entrenamiento con Feature Store + MLflow
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-2-v2.png?raw=true" width="1000">
# MAGIC
# MAGIC Entrenamos el modelo de pronóstico usando la **feature table** del notebook 2.1. Este notebook es el
# MAGIC corazón del ciclo de ML, y cada paso ilustra una **buena práctica de MLOps** que aplica a cualquier
# MAGIC modelo — no solo a este pronóstico.
# MAGIC
# MAGIC ## 📘 Las cuatro prácticas de MLOps que aplicamos (y por qué)
# MAGIC
# MAGIC | Práctica | API | Por qué importa |
# MAGIC |----------|-----|-----------------|
# MAGIC | **FeatureLookup** | `fe.create_training_set` | El modelo queda *empaquetado* con la definición de sus features. En inferencia (2.5) se recuperan **exactamente** las mismas → cero *training-serving skew*. |
# MAGIC | **MLflow autolog** | `mlflow.sklearn.autolog()` | Registra automáticamente parámetros, métricas y artefactos de cada ejecución. Reproducibilidad sin código extra. |
# MAGIC | **Linaje de datos** | `mlflow.data.load_delta` + `log_input` | Deja trazado *qué versión de qué tabla* entrenó el modelo → análisis de causa raíz cuando se degrada. |
# MAGIC | **Registro con FS** | `fe.log_model` | Registra el modelo en Unity Catalog conservando el linaje de features (visible en Catalog Explorer). |
# MAGIC
# MAGIC > Estas cuatro prácticas son **independientes del algoritmo**. Da igual si entrenas un
# MAGIC > `GradientBoostingRegressor`, un `LightGBM` o una red neuronal: el patrón de seguimiento, linaje y
# MAGIC > registro es el mismo. Eso es lo que hace este marco reutilizable.

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
# MAGIC ## Paso 1 — Construir el training set con Feature Store
# MAGIC
# MAGIC En vez de leer una tabla "plana" ya unida, seguimos el patrón del Feature Store:
# MAGIC
# MAGIC 1. **Spine** — un DataFrame mínimo con las **claves primarias** `(producto_familia, fecha)` + la etiqueta.
# MAGIC 2. **`FeatureLookup`** — le decimos al Feature Store *qué* features traer y *por qué clave* unirlas.
# MAGIC 3. **`create_training_set`** — hace el join y devuelve un training set que **recuerda su definición**.
# MAGIC
# MAGIC ¿Por qué este rodeo en lugar de un simple `SELECT *`? Porque el `training_set` resultante queda
# MAGIC **ligado al modelo** cuando lo registramos con `fe.log_model` (paso 3). En inferencia (2.5), pasar solo
# MAGIC las claves basta: el Feature Store recupera las features automáticamente, con la **misma lógica** del
# MAGIC entrenamiento. Ese es el mecanismo que elimina el *training-serving skew*.

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
# MAGIC ## Paso 2 — Linaje de datos
# MAGIC
# MAGIC Capturamos **qué versión** de la feature table se usó para entrenar. Delta versiona cada escritura, así
# MAGIC que `DESCRIBE HISTORY` nos da la última versión y `mlflow.data.load_delta` crea un objeto *dataset* que
# MAGIC luego adjuntamos a la ejecución con `log_input`. Resultado: desde el modelo registrado puedes navegar
# MAGIC hasta la versión exacta de los datos que lo entrenaron — indispensable para **auditoría regulatoria** y
# MAGIC para diagnosticar *data drift* cuando las predicciones empeoran.

# COMMAND ----------

# DBTITLE 1,Cargar el dataset como objeto MLflow (linaje)
latest_version = max(
    spark.sql(f"DESCRIBE HISTORY {FEATURE_TABLE}").toPandas()["version"]
)
src_dataset = mlflow.data.load_delta(table_name=FEATURE_TABLE, version=str(latest_version))
print(f"✔ Feature table version para linaje: {latest_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 — Entrenar con MLflow y registrar
# MAGIC
# MAGIC Los sub-pasos dentro de la celda:
# MAGIC
# MAGIC 1. **Split 80/20** — separamos entrenamiento y validación (`random_state=42` para reproducibilidad).
# MAGIC 2. **Pipeline de sklearn** — `OneHotEncoder` para `producto_familia` (categórica) + `passthrough` para
# MAGIC    las numéricas, seguido de `GradientBoostingRegressor`. Encapsular preprocesamiento y modelo en un
# MAGIC    **único `Pipeline`** garantiza que la misma transformación se aplique en inferencia.
# MAGIC 3. **`autolog`** — activa el registro automático en MLflow (parámetros, métricas, artefactos).
# MAGIC 4. **`start_run`** — abre la ejecución; dentro entrenamos, calculamos métricas (MAPE, R², precisión) y
# MAGIC    las registramos con `log_metrics`.
# MAGIC 5. **`log_input`** — adjunta el linaje de datos del paso 2.
# MAGIC 6. **`fe.log_model`** — registra el modelo en Unity Catalog **con el `training_set`**, preservando el
# MAGIC    `FeatureLookup` para la inferencia.
# MAGIC
# MAGIC > **Nota sobre la métrica:** usamos MAPE → precisión = `(1 - MAPE) × 100`, alineado con la meta de
# MAGIC > negocio de **> 95%**. Para un modelo de clasificación (p. ej. churn) cambiarías a F1/AUC; la
# MAGIC > mecánica de `log_metrics` es idéntica.

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
# MAGIC ## 🔁 Cómo aplicar este marco a otros modelos
# MAGIC
# MAGIC El esqueleto de este notebook — *training set desde Feature Store → autolog → linaje → `fe.log_model`* —
# MAGIC no cambia entre casos de uso. Lo que ajustas:
# MAGIC
# MAGIC | Elemento | Aquí (pronóstico) | Otro modelo (ej. churn de afiliados) |
# MAGIC |----------|-------------------|--------------------------------------|
# MAGIC | Estimador | `GradientBoostingRegressor` | `LGBMClassifier`, `RandomForestClassifier`… |
# MAGIC | Métricas | MAPE, R², precisión | F1, AUC, precision/recall |
# MAGIC | `mlflow.evaluate` model_type | `"regressor"` | `"classifier"` |
# MAGIC | Label | `label_unidades` | `se_retiro` |
# MAGIC
# MAGIC Todo lo demás — el `FeatureLookup`, el seguimiento MLflow, el linaje y el registro gobernado — es
# MAGIC **idéntico**. Copiar este notebook y cambiar esos cuatro elementos es una forma rápida de arrancar un
# MAGIC nuevo modelo con MLOps desde el día uno.
# MAGIC
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC El modelo quedó registrado en Unity Catalog **con su Feature Store lineage**. Continúa con:
# MAGIC
# MAGIC * **2.4 Registro y ciclo de vida** — asignar alias `@Challenger` / `@Champion`
# MAGIC * **2.5 Inferencia batch** — pronóstico con `fe.score_batch` y write-back a SAP HANA
