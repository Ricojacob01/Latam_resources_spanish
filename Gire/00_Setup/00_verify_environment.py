# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src=https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC # 00 - Verificación del Entorno
# MAGIC
# MAGIC Este notebook valida que tienes todo lo necesario para ejecutar el workshop:
# MAGIC
# MAGIC 1. Compute correcto (Serverless v2 para la mayoría de los labs)
# MAGIC 2. Acceso al catálogo compartido `ardemo_classic_dnubtw_catalog`
# MAGIC 3. Permisos para crear tu schema personal (`ws_<usuario>`)
# MAGIC 4. Librerías ML disponibles (para Lab 04)
# MAGIC
# MAGIC **Si todas las verificaciones pasan, estás listo para empezar con `Lab_01_Genie_y_Apps`.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup del lab
# MAGIC
# MAGIC Catálogo compartido: `ardemo_classic_dnubtw_catalog`. Schema personal por usuario: `ws_<usuario>`.
# MAGIC Esta es la misma celda que verás al principio de cada lab.

# COMMAND ----------

CATALOG = catalog = CATALOGO = "ardemo_classic_dnubtw_catalog"
_user = spark.sql("SELECT current_user()").collect()[0][0]
SCHEMA = db = schema = ESQUEMA = "ws_" + _user.split("@")[0].replace(".", "_").replace("-", "_")

print(f"Usuario: {_user}")
print(f"Catalog: {CATALOG}")
print(f"Schema:  {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificar acceso al catálogo compartido

# COMMAND ----------

try:
    spark.sql(f"USE CATALOG {CATALOG}")
    print(f"OK — acceso al catálogo {CATALOG}")
except Exception as e:
    print(f"FALLO — no se puede acceder al catálogo {CATALOG}")
    print(f"   {e}")
    print("   Solicita acceso al administrador del workspace.")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear schema personal

# COMMAND ----------

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{CATALOG}`.`{SCHEMA}`")
    spark.sql(f"USE SCHEMA `{SCHEMA}`")
    print(f"OK — schema {CATALOG}.{SCHEMA} listo")
except Exception as e:
    print(f"FALLO — no se puede crear el schema")
    print(f"   {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verificar permisos de creación de tablas

# COMMAND ----------

try:
    spark.sql(f"CREATE TABLE IF NOT EXISTS {CATALOG}.{SCHEMA}._test_table (id INT)")
    spark.sql(f"DROP TABLE {CATALOG}.{SCHEMA}._test_table")
    print(f"OK — puedes crear y eliminar tablas en {CATALOG}.{SCHEMA}")
except Exception as e:
    print(f"FALLO — no tienes permisos para crear tablas")
    print(f"   {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificar compute serverless
# MAGIC
# MAGIC Para confirmar la versión del entorno Serverless, abre el menú **Connect** arriba a la derecha y revisa que estés conectado a `Serverless` (Environment v2).
# MAGIC Para los notebooks de ML (Lab 04), conéctate al cluster `ml_workshop_databricks`.

# COMMAND ----------

import sys
print(f"Python: {sys.version}")

try:
    import mlflow, sklearn
    print(f"mlflow:   {mlflow.__version__}")
    print(f"sklearn:  {sklearn.__version__}")
except ImportError as e:
    print(f"AVISO — librerías ML no disponibles: {e}")
    print("   Esto es normal en Serverless. Para Lab 04 usa el cluster ml_workshop_databricks.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC Si llegaste hasta aquí sin errores, estás listo para empezar el workshop.
# MAGIC
# MAGIC **Próximo paso:** abre `Lab_01_Genie_y_Apps/01_Introduccion_Apps_y_Genie`
# MAGIC
# MAGIC Tu schema personal (lo verás repetido en cada lab):

# COMMAND ----------

print(f"MI SCHEMA: {CATALOG}.{SCHEMA}")
