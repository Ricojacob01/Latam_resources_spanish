# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Bienvenida 👋 · Track Data Engineering 🛠️
# MAGIC
# MAGIC **Duración:** ~2 horas · **Tipo:** Hands-on
# MAGIC
# MAGIC De datos crudos a tablas listas para BI/ML con la **Databricks Data Intelligence Platform**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Qué van a salir sabiendo?
# MAGIC
# MAGIC 1. **Ingesta incremental** con Auto Loader (`read_files` + `STREAM`) y la **arquitectura medallion** (bronze → silver → gold).
# MAGIC 2. **Spark Declarative Pipelines** (Lakeflow): definir tablas como código y dejar que Databricks gestione el flujo, el orden y el procesamiento incremental.
# MAGIC 3. **Calidad de datos** con *expectations* (`CONSTRAINT ... EXPECT ... ON VIOLATION ...`).
# MAGIC 4. **CDC** declarativo con `AUTO CDC INTO` (SCD Tipo 1).
# MAGIC 5. **Orquestación** del pipeline con Lakeflow **Jobs** (en la UI y como código / Asset Bundle).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧭 Enfoque UI vs Code de este track
# MAGIC
# MAGIC Este workshop te hace vivir **las dos caras de Databricks** — la **UI** (intuición, descubrimiento) y el **código/API** (reproducibilidad, CI/CD). Cada módulo abre con una nota que dice si vamos **lado a lado** o **secuencial** y por qué:
# MAGIC
# MAGIC | Módulo | Patrón | Por qué |
# MAGIC |---|---|---|
# MAGIC | 02 Ingesta y Medallion | **Code → UI** | La ingesta incremental se *entiende* escribiendo `read_files`; luego Catalog Explorer confirma tablas y lineage. |
# MAGIC | 03 Declarative Pipeline | **Code + UI a la vez** | El pipeline *es* código (SQL), pero su valor está en operarlo en la **UI de Pipelines** (grafo, expectations, runs). |
# MAGIC | 04 Orquestación con Jobs | **UI → Code** | Un Job se *entiende* visual en la Jobs UI; luego lo volvemos código (Asset Bundle / JSON / SDK). |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agenda
# MAGIC
# MAGIC | Tiempo | Actividad | Notebook |
# MAGIC |---|---|---|
# MAGIC | 0–5 | **Bienvenida** | `00` (este) |
# MAGIC | 5–25 | **Product Tour** — Lakehouse & Lakeflow | `01` |
# MAGIC | 25–50 | **LAB Express** — Ingesta + Medallion | `02` |
# MAGIC | 50–85 | **LAB** — Declarative Pipeline (Calidad + CDC) | `03` |
# MAGIC | 85–110 | **LAB** — Orquestación con Jobs | `04` |
# MAGIC | 110–120 | **Cierre** + preview | `05` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pre-check rápido
# MAGIC
# MAGIC Si no corriste `../00_Setup/00_verify_environment`, hazlo. Esta celda valida lo mínimo.

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

spark.sql(f"USE CATALOG `{CATALOG}`")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
spark.sql(f"USE SCHEMA `{SCHEMA}`")

print(f"Usuario: {_user}")
print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")
print("\n✅ Listo. Continúa con `01 - Product Tour (Lakehouse & Lakeflow)`")
