# Databricks notebook source
# MAGIC %md
# MAGIC # 🧹 _resources/99-cleanup — Limpieza del Workshop
# MAGIC
# MAGIC Borra los recursos creados durante el workshop para **dejar de incurrir en costos**.
# MAGIC
# MAGIC | Recurso | Tipo | Ámbito |
# MAGIC |---|---|---|
# MAGIC | App del agente | Databricks App | por asistente |
# MAGIC | Endpoint del agente | Model Serving | por asistente |
# MAGIC | Monitor de `reservas_delta` | Lakehouse Monitoring | por asistente |
# MAGIC | Índice `kb_index` | Vector Search | por asistente |
# MAGIC | Modelo `agente_afiliados` | UC Model | por asistente |
# MAGIC | Tablas/funciones del workshop | UC | por asistente |
# MAGIC | **Instancia Lakebase + endpoint VS** | infra | **compartido** (solo si lo activas) |
# MAGIC
# MAGIC > ⚠️ Pon **`borrar_compartidos = si`** SOLO si eres quien creó la instancia Lakebase y el endpoint de Vector
# MAGIC > Search (recursos compartidos). En un workshop con varios asistentes, déjalo en `no`.

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk databricks-vectorsearch mlflow psycopg2-binary
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.dropdown("borrar_compartidos", "no", ["no", "si"],
                         "¿Borrar recursos COMPARTIDOS (instancia Lakebase + endpoint VS)?")
BORRAR_COMPARTIDOS = dbutils.widgets.get("borrar_compartidos") == "si"

# Constantes (mismas que 00-setup, en línea para no re-sembrar nada)
CATALOG = "ardemo_classic_dnubtw_catalog"
user = spark.sql("SELECT current_user()").collect()[0][0]
username = user.split("@")[0].replace(".", "_").replace("-", "_")
SCHEMA = f"ws_{username}"
VS_ENDPOINT = "comfama_vs_endpoint"
VS_INDEX = f"{CATALOG}.{SCHEMA}.kb_index"
AGENT_MODEL_NAME = f"{CATALOG}.{SCHEMA}.agente_afiliados"
AGENT_ENDPOINT = f"agente_afiliados_{username}"
LAKEBASE_PROJECT = "comfama-afiliados"
LAKEBASE_DB = f"comfama_{username}"
APP_NAME = f"agente-afiliados-{username}".replace("_", "-")

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
resultados = []
def borrar(label, fn):
    try:
        fn(); resultados.append(f"✅ {label}")
    except Exception as e:
        resultados.append(f"➖ {label} ({type(e).__name__}: {str(e)[:80]})")

# COMMAND ----------

# MAGIC %md ## 1. Recursos por asistente

# COMMAND ----------

borrar(f"App {APP_NAME}",            lambda: w.apps.delete(name=APP_NAME))
borrar(f"Serving {AGENT_ENDPOINT}",  lambda: w.serving_endpoints.delete(name=AGENT_ENDPOINT))
borrar("Monitor reservas_delta",     lambda: w.quality_monitors.delete(table_name=f"{CATALOG}.{SCHEMA}.reservas_delta"))

# Vector Search index
def _del_index():
    from databricks.vector_search.client import VectorSearchClient
    VectorSearchClient(disable_notice=True).delete_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
borrar(f"VS index {VS_INDEX}", _del_index)

# Modelo registrado en UC (todas las versiones)
def _del_model():
    import mlflow
    mlflow.set_registry_uri("databricks-uc")
    from mlflow import MlflowClient
    MlflowClient(registry_uri="databricks-uc").delete_registered_model(AGENT_MODEL_NAME)
borrar(f"Modelo {AGENT_MODEL_NAME}", _del_model)

# Tu base de datos Lakebase (dentro de la instancia compartida) — la instancia NO se borra aquí
def _drop_lakebase_db():
    import psycopg2, uuid
    inst = w.database.get_database_instance(name=LAKEBASE_PROJECT)
    tok = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[LAKEBASE_PROJECT]).token
    con = psycopg2.connect(host=inst.read_write_dns, port=5432, dbname="databricks_postgres",
                           user=user, password=tok, sslmode="require")
    con.autocommit = True
    with con.cursor() as cur:
        cur.execute(f'DROP DATABASE IF EXISTS {LAKEBASE_DB} WITH (FORCE)')
    con.close()
borrar(f"Lakebase DB {LAKEBASE_DB}", _drop_lakebase_db)

for r in resultados: print(r)

# COMMAND ----------

# MAGIC %md ## 2. Objetos de UC (tablas, vistas, funciones, inference tables)

# COMMAND ----------

for t in ["programas","afiliados","beneficios_afiliado","kb_documentos","reservas_delta"]:
    spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{t}")
spark.sql(f"DROP VIEW IF EXISTS {CATALOG}.{SCHEMA}.v_programas_agotados")
for f in ["mask_documento","mask_email","filtro_ciudad"]:
    spark.sql(f"DROP FUNCTION IF EXISTS {CATALOG}.{SCHEMA}.{f}")
# Inference tables del AI Gateway (prefijo gw_agente)
for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect():
    if row.tableName.startswith("gw_agente"):
        spark.sql(f"DROP TABLE IF EXISTS {CATALOG}.{SCHEMA}.{row.tableName}")
print("✅ Objetos de UC del workshop eliminados (el schema se conserva)")

# COMMAND ----------

# MAGIC %md ## 3. Recursos compartidos (opcional)

# COMMAND ----------

if BORRAR_COMPARTIDOS:
    # Endpoint de Vector Search
    def _del_vs_ep():
        from databricks.vector_search.client import VectorSearchClient
        VectorSearchClient(disable_notice=True).delete_endpoint(name=VS_ENDPOINT)
    borrar(f"VS endpoint {VS_ENDPOINT}", _del_vs_ep)
    # Instancia Lakebase (mayor ahorro)
    def _del_lakebase():
        try:
            w.database.delete_database_instance(name=LAKEBASE_PROJECT, purge=True)
        except TypeError:
            w.database.delete_database_instance(name=LAKEBASE_PROJECT)
    borrar(f"Lakebase {LAKEBASE_PROJECT}", _del_lakebase)
    print("\n".join(resultados[-2:]))
else:
    print("➖ Recursos compartidos conservados (borrar_compartidos = no)")

# COMMAND ----------

# MAGIC %md ## ✅ Resumen

# COMMAND ----------

print("\n".join(resultados))
print("\nLimpieza completada.")
dbutils.notebook.exit("\n".join(resultados))

