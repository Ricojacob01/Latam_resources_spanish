# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bienvenida 👋 · Workshop MLOps end-to-end 🧪🚀
# MAGIC
# MAGIC **Duración:** ~3 horas · **Tipo:** Hands-on completo
# MAGIC
# MAGIC Del feature engineering al **endpoint en producción**, **orquestado automáticamente**. Caso: predicción de **churn** de clientes telecom.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Qué van a salir sabiendo?
# MAGIC
# MAGIC 1. **Feature engineering** + gobernanza de la tabla en Unity Catalog.
# MAGIC 2. **AutoML** (UI glass-box + API) y entrenamiento con **MLflow tracking**.
# MAGIC 3. **Registro en UC** + alias **Champion/Challenger** + validación y promoción.
# MAGIC 4. ⭐ **Model Serving**: un endpoint REST en tiempo real (UI + API).
# MAGIC 5. **Batch inference** a escala.
# MAGIC 6. ⭐ **Orquestación**: un Job que corre todo el pipeline en schedule (Jobs UI + Asset Bundle).
# MAGIC
# MAGIC ⭐ = lo que el `ML_workshop` original no cubría y aquí **agregamos**.

# COMMAND ----------

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
# MAGIC | 05 **Model Serving** | **UI → Code** |
# MAGIC | 06 Batch | Code (+ inspección UI) |
# MAGIC | 07 **Orquestación** | **UI → Code** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC | Tiempo | Módulo |
# MAGIC |---|---|
# MAGIC | 0–10 | 00 Bienvenida · 01 Product Tour |
# MAGIC | 10–40 | 02 Feature Engineering y Gobernanza |
# MAGIC | 40–75 | 03 AutoML, Entrenamiento y Tracking |
# MAGIC | 75–100 | 04 Registro en UC y Champion/Challenger |
# MAGIC | 100–135 | 05 ⭐ Model Serving (UI + API) |
# MAGIC | 135–155 | 06 Batch Inference |
# MAGIC | 155–185 | 07 ⭐ Orquestación — Job del pipeline ML |
# MAGIC | 185–195 | 08 Cierre |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check
# MAGIC
# MAGIC ⚠️ Conéctate al cluster **`ml_workshop_databricks`** (ML Runtime).

# COMMAND ----------

# MAGIC %run ./_resources/00-setup

# COMMAND ----------

try:
    import mlflow, sklearn, lightgbm
    print(f"mlflow {mlflow.__version__} · sklearn {sklearn.__version__} · lightgbm {lightgbm.__version__}  ✅")
except ImportError as e:
    print(f"❌ Falta una librería ML: {e}\n→ Usa el cluster 'ml_workshop_databricks'.")

print(f"\nDatos listos: {spark.table('mlops_churn_bronze_customers').count()} clientes")
print("✅ Continúa con `01 - Product Tour (MLOps end-to-end)`")
