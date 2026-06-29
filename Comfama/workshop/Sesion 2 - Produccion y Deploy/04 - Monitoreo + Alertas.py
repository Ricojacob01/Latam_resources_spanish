# Databricks notebook source
# MAGIC %md
# MAGIC # 📊 Sesión 2 · 04 — Monitoreo + Alertas
# MAGIC
# MAGIC **Meta:** vigilar la operación del agente con **Lakehouse Monitoring** (perfil + drift sobre las reservas) y
# MAGIC disparar **Databricks SQL Alerts** cuando un programa se queda **sin cupos**.
# MAGIC
# MAGIC > **Equivale a: `AlertEvaluator`.** En vez de un evaluador de alertas custom, monitores y alertas declarativos
# MAGIC > sobre tablas gobernadas.
# MAGIC
# MAGIC Módulo **dual-mode**: crear monitor/alerta **🖱️ por la UI** o **⌨️ por SDK/SQL**. ⚠️ **Validar en dry-run**.

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Llevar `reservas` a Delta (para monitorear)
# MAGIC Lakehouse Monitoring opera sobre tablas Delta en UC. Exportamos las reservas de Lakebase a una tabla Delta
# MAGIC (en producción esto sería una **synced table** Lakebase→UC continua, ver módulo 03).

# COMMAND ----------

import psycopg2, uuid
from databricks.sdk import WorkspaceClient
w = WorkspaceClient(); EMAIL = w.current_user.me().user_name
inst = w.database.get_database_instance(name=LAKEBASE_PROJECT)
token = w.database.generate_database_credential(request_id=str(uuid.uuid4()),
                                                instance_names=[LAKEBASE_PROJECT]).token
conn = psycopg2.connect(host=inst.read_write_dns, port=5432, dbname=LAKEBASE_DB,
                        user=EMAIL, password=token, sslmode="require")
import pandas as pd
df = pd.read_sql("""SELECT r.reserva_id, r.afiliado_id, p.nombre AS programa, p.categoria,
                           r.estado, r.creada_en
                    FROM reservas r JOIN programas p ON p.programa_id=r.programa_id""", conn)
conn.close()
(spark.createDataFrame(df).write.mode("overwrite").option("overwriteSchema","true")
      .saveAsTable(f"{CATALOG}.{SCHEMA}.reservas_delta"))
print(f"✅ {len(df)} reservas exportadas a {CATALOG}.{SCHEMA}.reservas_delta")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — crear el monitor
# MAGIC 1. **Catalog** → tabla **`reservas_delta`** → pestaña **Quality** → **Create monitor**.
# MAGIC 2. Tipo **Snapshot** (o **Time series** usando `creada_en` como timestamp).
# MAGIC 3. **Create**. Databricks genera tablas `_profile_metrics` y `_drift_metrics` + un dashboard.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — `quality_monitors.create`

# COMMAND ----------

from databricks.sdk.service.catalog import MonitorTimeSeries, MonitorInfo

table_fqn = f"{CATALOG}.{SCHEMA}.reservas_delta"
try:
    w.quality_monitors.create(
        table_name=table_fqn,
        assets_dir=f"/Workspace/Users/{EMAIL}/lakehouse_monitoring/{table_fqn}",
        output_schema_name=f"{CATALOG}.{SCHEMA}",
        time_series=MonitorTimeSeries(timestamp_col="creada_en", granularities=["1 day"]),
    )
    print("✅ Monitor creado sobre", table_fqn)
except Exception as e:
    print(f"Monitor ya existe o requiere ajuste: {type(e).__name__}: {str(e)[:160]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🚨 SQL Alert — programas sin cupos
# MAGIC Creamos una consulta que cuenta programas agotados y una **alerta** que notifica si hay alguno.

# COMMAND ----------

# Vista de capacidad (sobre la semilla Delta; en vivo sería la synced table de Lakebase)
spark.sql(f"""
CREATE OR REPLACE VIEW {CATALOG}.{SCHEMA}.v_programas_agotados AS
SELECT count(*) AS programas_sin_cupo
FROM {CATALOG}.{SCHEMA}.programas WHERE cupos_disponibles = 0
""")
display(spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.v_programas_agotados"))

# COMMAND ----------

# MAGIC %md
# MAGIC **🖱️ UI (crear la alerta):**
# MAGIC 1. **SQL** → **Queries** → New query:
# MAGIC    `SELECT programas_sin_cupo FROM ardemo_classic_dnubtw_catalog.ws_<usuario>.v_programas_agotados` → Save.
# MAGIC 2. **SQL** → **Alerts** → **Create alert** → elige la query → condición **`programas_sin_cupo > 0`**.
# MAGIC 3. Programa la frecuencia (p.ej. cada 15 min) y agrega destinatarios (email/Slack).
# MAGIC
# MAGIC **⌨️ Código:** se puede crear con la SQL Alerts API (`w.alerts.create` / Jobs API). Lo dejamos como ejemplo de
# MAGIC automatización para el módulo 06 (Deploy-as-Code).

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC Monitoreo continuo de las reservas (perfil + drift + dashboard) y una alerta que avisa cuando un programa se
# MAGIC agota — todo declarativo, sin un evaluador de alertas custom.
# MAGIC
# MAGIC ### ▶️ Siguiente: `05 - FinOps`

