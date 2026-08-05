# Databricks notebook source
# MAGIC %md
# MAGIC # 2.4 · Ciclo de vida del modelo en Unity Catalog
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-3-v2.png?raw=true" width="1000">
# MAGIC
# MAGIC ## 📘 El problema que resuelven los alias
# MAGIC
# MAGIC Uno de los grandes retos de MLOps es la falta de un **repositorio central** para modelos, sus versiones
# MAGIC y su promoción a lo largo del ciclo de vida. **Modelos en Unity Catalog** resuelve esto: piensa en
# MAGIC registrar un modelo como hacer *commit* de código en un control de versiones. Cada reentrenamiento crea
# MAGIC una nueva **versión** (v1, v2, …), y los **alias** en texto libre (`@Challenger`, `@Champion`) marcan
# MAGIC *qué versión está en qué fase*.
# MAGIC
# MAGIC | Concepto | Qué es | Para qué sirve |
# MAGIC |----------|--------|----------------|
# MAGIC | **Versión** | Cada modelo registrado (v1, v2…) | Historial inmutable y auditable |
# MAGIC | **`@Challenger`** | Candidato recién entrenado | Se valida antes de promover |
# MAGIC | **`@Champion`** | Modelo actualmente en producción | Lo que consume la inferencia (2.5) |
# MAGIC | **Tags** | Metadatos (`val_r2`, `has_description`…) | Registrar qué pruebas pasó |
# MAGIC
# MAGIC **La gran ventaja:** la inferencia referencia `models:/<modelo>@Champion` — **nunca un número de versión
# MAGIC fijo**. Cuando promueves un nuevo Champion, la inferencia usa el nuevo modelo automáticamente, sin tocar
# MAGIC una sola línea del notebook de scoring. Así se desacopla el *entrenamiento* del *despliegue*.
# MAGIC
# MAGIC > **Humano en el circuito → automatización.** Al empezar con MLOps conviene un humano validando cada
# MAGIC > promoción. A medida que el proceso madura, estos pasos se automatizan en un **Databricks Job** de
# MAGIC > validación (documentación, umbral de métrica, pruebas Champion-Challenger). Aquí lo mostramos como
# MAGIC > notebook interactivo con una regla de umbral simple.

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
# MAGIC En MLOps real, validar un modelo es **más que la precisión**. Un notebook de validación típico
# MAGIC comprueba, entre otros:
# MAGIC
# MAGIC * **Documentación** — ¿el modelo tiene descripción suficiente? (gobernanza)
# MAGIC * **Métrica de desempeño** — ¿supera el umbral y/o al Champion actual?
# MAGIC * **Inferencia sobre datos de producción** — ¿corre sin errores end-to-end?
# MAGIC * **KPIs de negocio** — ¿el impacto económico es aceptable? (no confundir con A/B testing, que es online)
# MAGIC
# MAGIC Cada comprobación se registra como **tag** en la versión, dejando trazado qué se validó. Aquí aplicamos
# MAGIC una **regla de umbral simple** sobre R²; si la primera versión no tiene Champion previo, se acepta como
# MAGIC el primero (patrón "bootstrap").

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
# Nota: los objetos de search_model_versions no exponen aliases/tags de forma fiable;
# consultamos cada versión con get_model_version, que sí devuelve aliases (lista) y tags (dict).
for sv in sorted(client.search_model_versions(f"name = '{MODELO}'"), key=lambda v: int(v.version)):
    v = client.get_model_version(MODELO, sv.version)
    print(f"  v{v.version}  aliases={list(v.aliases)}  tags={dict(v.tags)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 Cómo aplicar este marco a otros modelos
# MAGIC
# MAGIC La gestión del ciclo de vida es **totalmente independiente del tipo de modelo** — es pura gobernanza.
# MAGIC El mismo notebook sirve tal cual para churn, fraude, riesgo de crédito, etc.; solo cambia:
# MAGIC
# MAGIC * El **nombre del modelo** (`MODELO`) y su descripción.
# MAGIC * La **métrica** de la regla de promoción (R² para regresión → F1/AUC para clasificación).
# MAGIC * Opcionalmente, **más comprobaciones** de validación según los requisitos regulatorios del caso.
# MAGIC
# MAGIC Los alias `@Challenger`/`@Champion`, las tags y el desacople entrenamiento↔despliegue funcionan igual
# MAGIC para cualquier modelo del catálogo de Colsubsidio. En producción, este notebook se convierte en una
# MAGIC **tarea de un Databricks Job** disparada tras cada reentrenamiento.
# MAGIC
# MAGIC **Siguiente:** `2.5 Inferencia batch + write-back a SAP HANA` — usa el modelo `@Champion`.
