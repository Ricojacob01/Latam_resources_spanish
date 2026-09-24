# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# DBTITLE 1,🧹 Limpieza del Workshop
# MAGIC %md
# MAGIC # 07 - Limpieza del Workshop DE
# MAGIC
# MAGIC > **⚠️ Ejecute este notebook SOLO al finalizar el taller.**
# MAGIC >
# MAGIC > Elimina **todos** los recursos creados durante el workshop:
# MAGIC > esquemas (con todas sus tablas y volúmenes), jobs, queries guardadas,
# MAGIC > pipelines, archivos de Lakeflow Designer y dashboards AI/BI.
# MAGIC >
# MAGIC > **El catálogo NO se elimina.**
# MAGIC
# MAGIC 1. Verifique los valores de `user_suffix` y `catalog_name` en los widgets
# MAGIC 2. Ejecute **Run All**
# MAGIC 3. Revise el resumen final

# COMMAND ----------

# DBTITLE 1,Parámetros del Workshop
dbutils.widgets.text("user_suffix", "rico", "👤 Tu usuario")
dbutils.widgets.text("catalog_name", "classic_stable_paco_catalog", "📦 Catálogo")

user_suffix  = dbutils.widgets.get("user_suffix").strip().lower()
catalog_name = dbutils.widgets.get("catalog_name").strip()

schema_raw    = f"{user_suffix}_raw"
schema_bronze = f"{user_suffix}_bronze"
schema_silver = f"{user_suffix}_silver"
schema_gold   = f"{user_suffix}_gold"
schemas       = [schema_bronze, schema_silver, schema_gold, schema_raw]

print(f"ℹ️  Catálogo : {catalog_name}")
print(f"ℹ️  Usuario  : {user_suffix}")
print(f"ℹ️  Esquemas : {', '.join(schemas)}")
print(f"\n⚠️  Ejecute 'Run All' para eliminar TODOS los recursos del workshop.")

# COMMAND ----------

# DBTITLE 1,Inicializar SDK y contadores
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.dashboards import LifecycleState

w = WorkspaceClient()
resumen = []  # (tipo, nombre, estado)

# COMMAND ----------

# DBTITLE 1,1️⃣ Eliminar esquemas (CASCADE → tablas + volúmenes)
for schema in schemas:
    fqn = f"{catalog_name}.{schema}"
    try:
        spark.sql(f"DROP SCHEMA IF EXISTS {fqn} CASCADE")
        resumen.append(("Schema", fqn, "✅ eliminado"))
        print(f"✅ Esquema eliminado: {fqn}")
    except Exception as e:
        resumen.append(("Schema", fqn, f"⚠️ {e}"))
        print(f"⚠️  {fqn}: {e}")

# COMMAND ----------

# DBTITLE 1,2️⃣ Eliminar Jobs
job_pattern = f"BNCR Workshop Pipeline - {user_suffix}"
try:
    for j in w.jobs.list(name=job_pattern):
        w.jobs.delete(j.job_id)
        resumen.append(("Job", f"{j.job_id} ({j.settings.name})", "✅ eliminado"))
        print(f"✅ Job eliminado: {j.job_id} ({j.settings.name})")
except Exception as e:
    resumen.append(("Job", job_pattern, f"⚠️ {e}"))
    print(f"⚠️  Job: {e}")

# COMMAND ----------

# DBTITLE 1,3️⃣ Eliminar Queries SQL guardadas
query_pattern = f"BNCR Gold Resumen - {user_suffix}"
try:
    for q in w.queries.list():
        if q.display_name and query_pattern in q.display_name:
            w.queries.delete(q.id)
            resumen.append(("Query", f"{q.id} ({q.display_name})", "✅ eliminada"))
            print(f"✅ Query eliminada: {q.id} ({q.display_name})")
except Exception as e:
    resumen.append(("Query", query_pattern, f"⚠️ {e}"))
    print(f"⚠️  Query: {e}")

# COMMAND ----------

# DBTITLE 1,4️⃣ Eliminar Pipelines (por nombre)
pipeline_pattern = f"BNCR Medallion Pipeline - {user_suffix}"
try:
    for p in w.pipelines.list_pipelines(filter=f"name LIKE '%{user_suffix}%'"):
        if p.name and user_suffix in p.name:
            w.pipelines.delete(p.pipeline_id)
            resumen.append(("Pipeline", f"{p.pipeline_id} ({p.name})", "✅ eliminado"))
            print(f"✅ Pipeline eliminado: {p.pipeline_id} ({p.name})")
except Exception as e:
    resumen.append(("Pipeline", pipeline_pattern, f"⚠️ {e}"))
    print(f"⚠️  Pipeline: {e}")

# COMMAND ----------

# DBTITLE 1,5️⃣ Eliminar archivos Lakeflow Designer (.designer)
import os

# Buscar en la carpeta del workshop y en home
search_paths = [
    "/Workspace/Users/rico.martinez@databricks.com/DE_Rico/Data_Engineering",
    "/Workspace/Users/rico.martinez@databricks.com",
]
for search_path in search_paths:
    try:
        for item in w.workspace.list(search_path):
            if item.path and ".designer" in item.path.lower():
                w.workspace.delete(item.path)
                resumen.append(("Designer", item.path, "✅ eliminado"))
                print(f"✅ Designer eliminado: {item.path}")
    except Exception as e:
        resumen.append(("Designer", search_path, f"⚠️ {e}"))
        print(f"⚠️  Designer ({search_path}): {e}")

# COMMAND ----------

# DBTITLE 1,6️⃣ Eliminar Dashboards AI/BI (BNCR*)
try:
    for d in w.lakeview.list():
        if (
            d.display_name
            and "BNCR" in d.display_name
            and d.lifecycle_state != LifecycleState.TRASHED
        ):
            w.lakeview.trash(d.dashboard_id)
            resumen.append(("Dashboard", f"{d.dashboard_id} ({d.display_name})", "✅ eliminado"))
            print(f"✅ Dashboard eliminado: {d.dashboard_id} ({d.display_name})")
except Exception as e:
    resumen.append(("Dashboard", "BNCR*", f"⚠️ {e}"))
    print(f"⚠️  Dashboard: {e}")

# COMMAND ----------

# DBTITLE 1,📊 Resumen de limpieza
print("\n" + "="*60)
print("  RESUMEN DE LIMPIEZA")
print("="*60)

if not resumen:
    print("  No se encontraron recursos para eliminar.")
else:
    for tipo, nombre, estado in resumen:
        print(f"  [{tipo:10s}] {nombre} → {estado}")

print("="*60)
print(f"  Catálogo preservado: {catalog_name}")
print(f"  Total recursos procesados: {len(resumen)}")
print("\n🧹 Limpieza completa")