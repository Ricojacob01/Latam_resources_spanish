# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuración del taller  ·  (multi-usuario, reutilizable en cualquier cuenta)
# MAGIC
# MAGIC **Corre este notebook primero.** Los demás (`01`–`03`) lo invocan solos con `%run ./00_config`,
# MAGIC así que la configuración vive en **un único lugar**.
# MAGIC
# MAGIC ### Patrón de laboratorio: catálogo compartido + un esquema por usuario
# MAGIC En la mayoría de las cuentas de taller **todos comparten un catálogo** y **cada quien tiene su propio esquema**.
# MAGIC Este notebook implementa exactamente eso, sin colisiones entre participantes:
# MAGIC
# MAGIC | Parámetro | Cómo se define | Ejemplo |
# MAGIC |---|---|---|
# MAGIC | `catalog` (compartido) | **Widget** que llena el instructor — sin valor por defecto para que sea portable a cualquier cuenta | `workshop_catalog` |
# MAGIC | `schema` (tuyo) | **Se deriva automáticamente** de tu usuario: `taller_genie_<usuario>` | `taller_genie_rico_martinez` |
# MAGIC
# MAGIC > 🧭 ¿No sabes qué catálogo usar? Corre `list_catalogs()` en una celda y pregúntale al instructor.

# COMMAND ----------

import re

# Único parámetro que el instructor debe fijar. Sin default → portable a cualquier workspace.
dbutils.widgets.text("catalog", "", "Catálogo compartido (lo pone el instructor)")


def list_catalogs():
    """Muestra los catálogos que puedes ver — útil para llenar el widget 'catalog'."""
    display(spark.sql("SHOW CATALOGS"))


def _derive_schema(user: str) -> str:
    """Deriva un nombre de esquema único y válido a partir del usuario (parte antes de @)."""
    handle = user.split("@")[0].lower()
    return "taller_genie_" + re.sub(r"[^a-z0-9]+", "_", handle).strip("_")


catalog = dbutils.widgets.get("catalog").strip()
assert catalog, (
    "⛔ Escribe el catálogo compartido en el widget 'catalog' (arriba) y vuelve a correr.\n"
    "   ¿No lo sabes? Corre list_catalogs() en una celda nueva o pregúntale al instructor."
)

current_user = spark.sql("SELECT current_user()").first()[0]
schema = _derive_schema(current_user)
fq_schema = f"`{catalog}`.`{schema}`"

# Crea TU esquema dentro del catálogo compartido y fija el contexto de la sesión.
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {fq_schema} "
    f"COMMENT 'Esquema personal del Taller Genie — {current_user}'"
)
spark.sql(f"USE CATALOG `{catalog}`")
spark.sql(f"USE SCHEMA `{schema}`")


def show_tables():
    """Lista las tablas en TU esquema del taller."""
    display(spark.sql(f"SHOW TABLES IN {fq_schema}"))


print("✅ Configuración lista")
print(f"   Usuario  : {current_user}")
print(f"   Catálogo : {catalog}   (compartido por todos)")
print(f"   Esquema  : {schema}   (solo tuyo)")
print(f"   Contexto activo (USE CATALOG / USE SCHEMA): {catalog}.{schema}")
print("   → Los notebooks 01–03 y los scripts usarán este esquema automáticamente.")
print("   → Variables disponibles tras %run: catalog, schema, fq_schema  ·  helpers: list_catalogs(), show_tables()")
