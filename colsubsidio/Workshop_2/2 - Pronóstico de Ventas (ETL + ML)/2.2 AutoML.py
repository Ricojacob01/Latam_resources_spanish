# Databricks notebook source
# MAGIC %md
# MAGIC # 2.2 · AutoML — modelo baseline automático
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-1-v2.png?raw=true" width="1000">
# MAGIC
# MAGIC ## 📘 ¿Qué es AutoML y cuándo usarlo?
# MAGIC
# MAGIC **Databricks AutoML** automatiza las partes más tediosas del desarrollo de un modelo: prueba múltiples
# MAGIC **algoritmos** (árboles, boosting, regresión lineal, etc.) e **hiperparámetros**, evalúa cada intento
# MAGIC con validación cruzada y selecciona el **modelo campeón** según la métrica que elijas.
# MAGIC
# MAGIC Lo que lo distingue de otras herramientas AutoML es que es **"glass-box"** (caja de cristal), no
# MAGIC "black-box": por cada intento genera un **notebook reproducible y editable**, además de un **notebook de
# MAGIC exploración de datos**. No obtienes solo un modelo, sino el **código** que lo produjo — que puedes tomar
# MAGIC como punto de partida y refinar (exactamente lo que hacemos en el notebook 2.3).
# MAGIC
# MAGIC #### En este notebook aprenderás a
# MAGIC * Lanzar un experimento AutoML de **regresión** (pronóstico de `label_unidades`);
# MAGIC * Analizar los resultados y encontrar el modelo campeón;
# MAGIC * Entender cuándo conviene AutoML frente al entrenamiento manual.
# MAGIC
# MAGIC > **Nota de compute:** la **API `databricks.automl`** requiere un cluster con **Databricks Runtime for ML**
# MAGIC > (no serverless). Si estás en serverless, usa la **ruta por interfaz gráfica** (Opción A), que funciona
# MAGIC > igual y es la recomendada para el taller.

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
# MAGIC ## Opción A — Interfaz gráfica (recomendada en el taller)
# MAGIC
# MAGIC Sigue estos pasos. Cada uno corresponde a una decisión de modelado que vale la pena entender:
# MAGIC
# MAGIC 1. **Machine Learning → Experiments → Create AutoML Experiment.**
# MAGIC 2. **Compute:** elige un cluster con *Runtime for ML*.
# MAGIC 3. **ML problem type: `Regression`.**
# MAGIC    *(Pronosticamos una cantidad continua — unidades. Si predijéramos una clase, p. ej. "se retira sí/no",
# MAGIC    elegiríamos `Classification`; para series temporales puras existe además `Forecasting`.)*
# MAGIC 4. **Dataset:** la tabla `ft_ventas_features` de tu esquema `ws2_<usuario>`.
# MAGIC 5. **Prediction target: `label_unidades`.**
# MAGIC 6. **Excluir columnas** no predictoras: `fecha`, `periodo`
# MAGIC    *(son identificadores, no señales; incluirlas podría causar sobreajuste o leakage).*
# MAGIC 7. **Advanced → Evaluation metric:** `r2` o `rmse` *(qué se optimiza).* **Timeout:** ~10-15 min.
# MAGIC 8. **Start AutoML.**
# MAGIC
# MAGIC **Al finalizar tendrás:** el modelo campeón con su notebook reproducible, un notebook de exploración de
# MAGIC datos, y todos los intentos en un experimento MLflow — listos para comparar y registrar en Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Opción B — API programática (requiere Runtime for ML)
# MAGIC
# MAGIC La misma funcionalidad por código — útil para **automatizar** el reentrenamiento dentro de un Job.
# MAGIC Descomenta y ejecuta si estás en un cluster con Runtime for ML.

# COMMAND ----------

# DBTITLE 1,AutoML por código (opcional)
# from databricks import automl
#
# # Excluimos identificadores; AutoML usa el resto como features y label_unidades como objetivo.
# df = spark.table(FEATURE_TABLE).drop("fecha", "periodo")
#
# summary = automl.regress(
#     dataset=df,
#     target_col="label_unidades",
#     primary_metric="r2",      # métrica a optimizar
#     timeout_minutes=15,       # presupuesto de tiempo
# )
#
# print("Mejor intento (trial):", summary.best_trial)
# print("Notebook del mejor modelo:", summary.best_trial.notebook_url)
# print("Experimento MLflow:", summary.experiment.experiment_id)
#
# # El modelo campeón se registra igual que en 2.4:
# #   import mlflow
# #   mlflow.set_registry_uri("databricks-uc")
# #   mlflow.register_model(summary.best_trial.model_path,
# #                         f"{CATALOGO}.{ESQUEMA}.modelo_pronostico_ventas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoML vs. entrenamiento manual — cuándo usar cada uno
# MAGIC
# MAGIC | AutoML (2.2) | Entrenamiento manual (2.3) |
# MAGIC |--------------|----------------------------|
# MAGIC | Baseline rápido; explora muchos algoritmos | Control total del pipeline y las features |
# MAGIC | Genera notebooks reproducibles como punto de partida | Integración con Feature Store (`FeatureLookup`) |
# MAGIC | Ideal para una primera iteración o benchmark | Ideal para afinar y productivizar el modelo elegido |
# MAGIC
# MAGIC **Buena práctica (la que seguimos aquí):** usa AutoML para el **baseline**, revisa el notebook de su
# MAGIC mejor intento para entender qué funcionó, y luego **refina** en 2.3 con Feature Store + seguimiento
# MAGIC MLflow y registro gobernado.
# MAGIC
# MAGIC ## 🔁 Cómo aplicar este marco a otros modelos
# MAGIC
# MAGIC AutoML es el acelerador universal del ciclo de vida. Para otro caso de Colsubsidio solo cambias **tres
# MAGIC campos** en el formulario (o en la llamada a la API):
# MAGIC
# MAGIC * **Problem type** — `Regression` (pronóstico, monto de mora), `Classification` (retiro de afiliado,
# MAGIC   fraude, aprobación de crédito), o `Forecasting` (series temporales puras).
# MAGIC * **Dataset** — la feature table del caso (creada con el patrón de 2.1).
# MAGIC * **Prediction target** — la etiqueta a predecir.
# MAGIC
# MAGIC Todo lo demás — comparación de algoritmos, notebooks reproducibles, registro en MLflow — funciona igual.
# MAGIC
# MAGIC **Siguiente:** `2.3 Entrenamiento con Feature Store + MLflow`.

