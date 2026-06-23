# Databricks notebook source
# DBTITLE 1,Header with banner
# MAGIC %md
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>
# MAGIC
# MAGIC # 00 — Bienvenida 👋 · Workshop MLOps end-to-end 🧪🚀
# MAGIC
# MAGIC <img src="https://github.com/databricks-demos/dbdemos-resources/blob/main/images/product/mlops/mlops-uc-end2end-0-v2.png?raw=true" width="1200">
# MAGIC
# MAGIC **Duración:** ~3 horas · **Tipo:** Hands-on completo
# MAGIC
# MAGIC Del feature engineering al **endpoint en producción**, **orquestado automáticamente**. Caso: predicción de **churn** de clientes telecom.

# COMMAND ----------

# DBTITLE 1,What you'll learn
# MAGIC %md
# MAGIC ## ¿Qué van a salir sabiendo?
# MAGIC
# MAGIC 1. **Feature engineering** + gobernanza de la tabla en Unity Catalog.
# MAGIC 2. **AutoML** (UI glass-box + API) y entrenamiento con **MLflow tracking**.
# MAGIC 3. **Registro en UC** + alias **Champion/Challenger** + validación y promoción.
# MAGIC 4. ⭐ **Model Serving + AI Gateway**: un endpoint REST gobernado (UI + API).
# MAGIC 5. **Batch inference** a escala.
# MAGIC 6. ⭐ **Orquestación**: un Job que corre todo el pipeline en schedule (Jobs UI + Asset Bundle).
# MAGIC
# MAGIC ⭐ = módulos avanzados que completan el ciclo MLOps end-to-end.

# COMMAND ----------

# DBTITLE 1,UI vs Code table
# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code (este workshop)
# MAGIC
# MAGIC Vives **las dos caras** de Databricks; cada módulo declara su elección:
# MAGIC
# MAGIC | Módulo | Patrón |
# MAGIC |---|---|
# MAGIC | 02 Feature eng | Code (+ inspección UI) |
# MAGIC | 03 AutoML/Train | Lado a lado (UI ↔ API) |
# MAGIC | 04 Registro UC | Code → UI |
# MAGIC | 05 **Model Serving + AI Gateway** | **UI → Code** |
# MAGIC | 06 Batch | Code (+ inspección UI) |
# MAGIC | 07 **Orquestación** | **UI → Code** |

# COMMAND ----------

# DBTITLE 1,Agenda
# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC | Tiempo | Módulo |
# MAGIC |---|---|
# MAGIC | 0–10 | 00 Bienvenida · 01 Product Tour |
# MAGIC | 10–40 | 02 Feature Engineering y Gobernanza |
# MAGIC | 40–75 | 03 AutoML, Entrenamiento y Tracking |
# MAGIC | 75–100 | 04 Registro en UC y Champion/Challenger |
# MAGIC | 100–140 | 05 ⭐ Model Serving + AI Gateway (UI + API) |
# MAGIC | 135–155 | 06 Batch Inference |
# MAGIC | 155–185 | 07 ⭐ Orquestación — Job del pipeline ML |
# MAGIC | 185–195 | 08 Cierre |

# COMMAND ----------

# DBTITLE 1,Pre-check
# MAGIC %md
# MAGIC ## Pre-check
# MAGIC
# MAGIC ⚠️ Puedes usar **Serverless** o un cluster con **ML Runtime**. El setup instala automáticamente las librerías necesarias (mlflow, lightgbm) vía `%pip install`.

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

# DBTITLE 1,Library validation
# El setup ya hizo pip install de mlflow y descargó los datos
try:
    import mlflow, sklearn
    print(f"mlflow {mlflow.__version__} · sklearn {sklearn.__version__} ✅")
    try:
        import lightgbm
        print(f"lightgbm {lightgbm.__version__} ✅")
    except ImportError:
        print("lightgbm se instalará en el notebook 03 (entrenamiento).")
except ImportError as e:
    print(f"⚠️ {e} — se instalará automáticamente al ejecutar cada notebook vía _resources/00-setup.")

print(f"\nDatos listos: {spark.table('mlops_churn_bronze_customers').count()} clientes")
print("✅ Continúa con `01 - Product Tour (MLOps end-to-end)`")
