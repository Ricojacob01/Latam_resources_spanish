# Databricks notebook source
# MAGIC %md
# MAGIC # 2.4 · Ciclo de vida del modelo en Unity Catalog
# MAGIC
# MAGIC Gestionamos el modelo registrado con **alias** (`@Challenger`, `@Champion`), descripciones y etiquetas —
# MAGIC el patrón de MLOps del workshop de referencia. El alias indica en qué fase del ciclo de vida está cada
# MAGIC versión, y permite que la inferencia (2.5) referencie siempre `@Champion` sin hardcodear números de versión.

# COMMAND ----------

# DBTITLE 1,Dependencias
# MAGIC %pip install --quiet mlflow --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Parámetros
import mlflow
from mlflow import MlflowClient

dbutils.widgets.text("catalogo", "classic_stable_paco_catalog", "Catálogo compartido")
CATALOGO = dbutils.widgets.get("catalogo").strip()
usuario  = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO   = usuario.split("@")[0].replace(".", "_").replace("-", "_")
FQN      = f"{CATALOGO}.ws2_{SUFIJO}"
MODELO   = f"{FQN}.modelo_pronostico_ventas"

mlflow.set_registry_uri("databricks-uc")
client = MlflowClient()
print(f"Modelo: {MODELO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Seleccionar la mejor versión y marcarla como Challenger
# MAGIC
# MAGIC Tomamos la **última versión** registrada del modelo (la que entrenó 2.3) y la marcamos como
# MAGIC **`@Challenger`** — candidata a reemplazar al `@Champion` en producción.

# COMMAND ----------

# DBTITLE 1,Última versión → alias Challenger
versiones = client.search_model_versions(f"name = '{MODELO}'")
ultima = max(versiones, key=lambda v: int(v.version))
print(f"✔ Última versión: {ultima.version}")

# Descripción del modelo (una sola vez)
client.update_registered_model(
    name=MODELO,
    description="Pronostica unidades de venta por familia de producto y fecha. "
                "Features desde el Feature Store (origen SAP HANA). Resultados escritos de vuelta a SAP HANA.",
)

# Recuperar métrica de la run asociada para documentar la versión
run = client.get_run(ultima.run_id)
r2 = run.data.metrics.get("val_r2")
precision = run.data.metrics.get("val_precision_pct")
client.update_model_version(
    name=MODELO, version=ultima.version,
    description=f"R² validación = {round(r2,4) if r2 else 'n/a'}, precisión = {precision}%.",
)
client.set_model_version_tag(MODELO, ultima.version, "val_r2", str(round(r2,4) if r2 else ""))

client.set_registered_model_alias(name=MODELO, alias="Challenger", version=ultima.version)
print(f"✔ Alias @Challenger → versión {ultima.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Validar el Challenger y promover a Champion
# MAGIC
# MAGIC En producción, la promoción se automatiza en un notebook de validación dentro de un **Databricks Job**
# MAGIC (pruebas de calidad, umbrales de métrica, etc.). Aquí aplicamos una **regla de umbral simple**: si el R²
# MAGIC de validación supera el mínimo, promovemos el Challenger a **`@Champion`**.

# COMMAND ----------

# DBTITLE 1,Regla de promoción
R2_MINIMO = 0.60   # umbral de ejemplo para el taller

challenger = client.get_model_version_by_alias(MODELO, "Challenger")
run_ch = client.get_run(challenger.run_id)
r2_ch = run_ch.data.metrics.get("val_r2", 0.0)

print(f"Challenger v{challenger.version} · R² = {round(r2_ch,4)} · umbral = {R2_MINIMO}")

if r2_ch >= R2_MINIMO:
    client.set_registered_model_alias(name=MODELO, alias="Champion", version=challenger.version)
    print(f"✔ APROBADO → @Champion = versión {challenger.version}")
else:
    print("✗ Rechazado: el Challenger no supera el umbral. @Champion se mantiene.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspeccionar el estado del registro
# MAGIC
# MAGIC En **Catalog Explorer → Models** verás la descripción, los alias `@Challenger`/`@Champion`, las etiquetas
# MAGIC y el **linaje** hacia la feature table y los datos de origen.

# COMMAND ----------

# DBTITLE 1,Resumen de versiones y alias
for v in sorted(client.search_model_versions(f"name = '{MODELO}'"), key=lambda v: int(v.version)):
    print(f"  v{v.version}  aliases={list(v.aliases)}  tags={dict(v.tags)}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Siguiente:** `2.5 Inferencia batch + write-back a SAP HANA` — usa el modelo `@Champion`.

