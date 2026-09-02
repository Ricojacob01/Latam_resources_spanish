# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Configuración del taller
# MAGIC %md
# MAGIC # 00 · Configuración del taller  ·  
# MAGIC
# MAGIC **Corre este notebook primero.** Los demás (`01`–`03`) lo invocan solos con `%run ./00_config`,
# MAGIC así que la configuración vive en **un único lugar**.
# MAGIC
# MAGIC ### Patrón de laboratorio: catálogo y esquema compartidos
# MAGIC
# MAGIC | Parámetro | Cómo se define | Valor por defecto |
# MAGIC |---|---|---|
# MAGIC | `catalog` | **Widget** — lo configura el instructor | `classic_stable_paco_catalog` |
# MAGIC | `schema` | **Widget** — lo configura el instructor | `ts_ai_gateway` |
# MAGIC
# MAGIC > 🧭 ¿No sabes qué catálogo usar? Corre `list_catalogs()` en una celda y pregúntale al instructor.

# COMMAND ----------

# DBTITLE 1,Setup code
# Catálogo y esquema compartidos — valores por defecto configurados por el instructor.
dbutils.widgets.text("catalog", "classic_stable_paco_catalog", "Catálogo compartido")
dbutils.widgets.text("schema", "ts_ai_gateway", "Esquema compartido")


def list_catalogs():
    """Muestra los catálogos que puedes ver — útil para llenar el widget 'catalog'."""
    display(spark.sql("SHOW CATALOGS"))


catalog = dbutils.widgets.get("catalog").strip()
schema = dbutils.widgets.get("schema").strip()
assert catalog, (
    "⛔ Escribe el catálogo compartido en el widget 'catalog' (arriba) y vuelve a correr.\n"
    "   ¿No lo sabes? Corre list_catalogs() en una celda nueva o pregúntale al instructor."
)
assert schema, (
    "⛔ Escribe el esquema en el widget 'schema' (arriba) y vuelve a correr."
)

current_user = spark.sql("SELECT current_user()").first()[0]
fq_schema = f"`{catalog}`.`{schema}`"

# Crea el esquema dentro del catálogo compartido y fija el contexto de la sesión.
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {fq_schema} "
    f"COMMENT 'Esquema del Taller Genie — {current_user}'"
)
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")


def show_tables():
    """Lista las tablas en el esquema del taller."""
    display(spark.sql(f"SHOW TABLES IN {fq_schema}"))


print("✅ Configuración lista")
print(f"   Usuario  : {current_user}")
print(f"   Catálogo : {catalog}")
print(f"   Esquema  : {schema}")
print(f"   Contexto activo (USE CATALOG / USE SCHEMA): {catalog}.{schema}")
print("   → Los notebooks 01–03 y los scripts usarán este esquema automáticamente.")
print("   → Variables disponibles tras %run: catalog, schema, fq_schema  ·  helpers: list_catalogs(), show_tables()")