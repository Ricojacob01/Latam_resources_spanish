# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bienvenida 👋 · Track MLOps 🧪
# MAGIC
# MAGIC **Duración:** ~1.5 horas · **Tipo:** Hands-on
# MAGIC
# MAGIC Del dato al modelo gobernado: el ciclo de ML en Databricks con **MLflow** + **Unity Catalog**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Qué van a salir sabiendo?
# MAGIC
# MAGIC 1. **Feature engineering** reproducible.
# MAGIC 2. **AutoML** — en la UI (glass-box) y por API.
# MAGIC 3. **MLflow tracking** (autolog, signature, dataset lineage).
# MAGIC 4. **Modelos en Unity Catalog**: registro + alias **Champion/Challenger**.
# MAGIC 5. **Validación** del Challenger (métricas técnicas + de negocio) y **batch inference**.
# MAGIC
# MAGIC > El **Model Serving** (endpoints en tiempo real) y la **orquestación con Jobs** se cubren a fondo en `../../CP/MLOps/`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este track
# MAGIC
# MAGIC | Sub-tarea (módulo 02) | Patrón | Por qué |
# MAGIC |---|---|---|
# MAGIC | Feature engineering | **Code** | Transformaciones → código. |
# MAGIC | AutoML | **Lado a lado** | UI glass-box *y* `automl.classify`: misma capacidad. |
# MAGIC | Registro en UC | **Code → UI** | Registras por API; gobiernas en Models UI. |
# MAGIC | Challenger validation | **Code (+ inspección UI)** | Lógica en código; tags/desc en la UI del modelo. |
# MAGIC | Batch inference | **Code (+ inspección UI)** | `spark_udf`; tabla en Catalog Explorer. |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC | Tiempo | Actividad | Notebook |
# MAGIC |---|---|---|
# MAGIC | 0–5 | **Bienvenida** | `00` (este) |
# MAGIC | 5–25 | **Product Tour** | `01` |
# MAGIC | 25–95 | **LAB — Del notebook a UC** | `02` (+ `labs/ml_en_databricks/*`) |
# MAGIC | 95–105 | **Cierre** + puente a CP/MLOps | `05` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check rápido
# MAGIC
# MAGIC ⚠️ Conéctate al cluster clásico **`ml_workshop_databricks`** (los notebooks ML usan librerías que no están en Serverless).

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")
spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")
print(f"Usuario: {_user}\nCatalog: {CATALOG}\nSchema:  {SCHEMA}")

try:
    import mlflow, sklearn
    print(f"\nmlflow {mlflow.__version__} · sklearn {sklearn.__version__}  ✅")
except ImportError as e:
    print(f"\n❌ Librerías ML no disponibles: {e}\n→ Conéctate al cluster 'ml_workshop_databricks'.")

print("\n✅ Continúa con `01 - Product Tour (MLOps en Databricks)`")
