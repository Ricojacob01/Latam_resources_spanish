# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título
# MAGIC %md
# MAGIC # 00 - Setup del Workshop
# MAGIC
# MAGIC 1. Seleccione compute **Serverless**
# MAGIC 2. Cambie **user_suffix** y **catalog_name** en los widgets de arriba
# MAGIC 3. Ejecute **Run All**
# MAGIC 4. Verifique su catálogo en Catalog Explorer

# COMMAND ----------

# DBTITLE 1,Parámetros del Workshop
# ============================================================
# Parámetros del workshop — cambie los valores en los widgets ↑
# ============================================================
dbutils.widgets.text("user_suffix", "rico", "👤 Tu usuario")
dbutils.widgets.text("catalog_name", "classic_stable_paco_catalog", "📦 Catálogo")
print("✅ Widgets creados — cambie los valores arriba y ejecute Run All")

# COMMAND ----------

# DBTITLE 1,Cargar funciones auxiliares
# MAGIC %run "./00 - Setup/00_variables"

# COMMAND ----------

# DBTITLE 1,Configurar variables desde widgets
# Leer parámetros de los widgets (sobrescribe valores de 00_variables)
user_suffix = dbutils.widgets.get("user_suffix").strip().lower()
catalog_name = dbutils.widgets.get("catalog_name").strip()

# Recomputar variables derivadas
schema_raw    = f"{user_suffix}_raw"
schema_bronze = f"{user_suffix}_bronze"
schema_silver = f"{user_suffix}_silver"
schema_gold   = f"{user_suffix}_gold"
volume   = "transacciones"
vol_path = f"/Volumes/{catalog_name}/{schema_raw}/{volume}"

# Validar
if not user_suffix:
    raise ValueError("⚠️ user_suffix no puede estar vacío")
if "carga_datos" not in dir():
    raise RuntimeError("Función carga_datos no encontrada. Verifique %run ./00 - Setup/00_variables")

print(f"✅ Parámetros configurados:")
print(f"   Usuario     : {user_suffix}")
print(f"   Catálogo    : {catalog_name}")
print(f"   Schemas     : {schema_raw}, {schema_bronze}, {schema_silver}, {schema_gold}")
print(f"   Volumen     : {vol_path}")

# COMMAND ----------

# DBTITLE 1,Crear catálogo y esquemas
# Validar que el catálogo exista; si no, pida al instructor que lo cree.
_catalogs = [r[0] for r in spark.sql("SHOW CATALOGS").collect()]
if catalog_name not in _catalogs:
    raise RuntimeError(
        f"❌ El catálogo '{catalog_name}' no existe. "
        "Pida al instructor que lo cree antes de continuar."
    )

spark.sql(f"USE CATALOG {catalog_name}")

for s in [schema_raw, schema_bronze, schema_silver, schema_gold]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{s}")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_raw}.{volume}")
dbutils.fs.mkdirs(vol_path)

print("✅ Catálogo y esquemas creados:")
display(spark.sql(f"SHOW SCHEMAS IN {catalog_name}"))

# COMMAND ----------

# DBTITLE 1,Cargar datos iniciales
carga_datos("initial")

# COMMAND ----------

# DBTITLE 1,Verificación
# MAGIC %md
# MAGIC ## Verificación

# COMMAND ----------

# DBTITLE 1,Listar volumen
display(dbutils.fs.ls(vol_path))

# COMMAND ----------

# DBTITLE 1,Conteo por carpeta
for folder in dbutils.fs.ls(vol_path):
    if folder.isDir():
        n = len(dbutils.fs.ls(folder.path))
        print(f"{folder.name}: {n} archivos")