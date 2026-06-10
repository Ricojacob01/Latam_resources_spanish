# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Monitoring con Lakehouse Monitoring
# MAGIC
# MAGIC **Reemplaza**: monitores de calidad / drift custom + parte de `AlertEvaluator`
# MAGIC
# MAGIC ## ¿Qué hace su código actual?
# MAGIC
# MAGIC No tienen un sistema de monitoreo de datos / modelos como tal — dependerían de:
# MAGIC - Chequeos ad-hoc en notebooks
# MAGIC - Métricas Prometheus exportadas por `TelemetryManager`
# MAGIC - Alarmas manuales sobre umbrales
# MAGIC
# MAGIC ## ¿Qué hace Databricks?
# MAGIC
# MAGIC **Lakehouse Monitoring** corre automáticamente sobre cualquier Delta table:
# MAGIC - **Profile metrics**: stats por columna (mean, null count, cardinality, percentiles)
# MAGIC - **Drift metrics**: distribución vs baseline o vs ventana de tiempo
# MAGIC - **Quality metrics**: para tablas de inferencia (model performance over time)
# MAGIC - **Custom metrics**: cualquier expresión SQL que quieras trackear
# MAGIC - **Dashboards auto-generados**: Lakeview dashboard creado solo
# MAGIC - **Alertas integradas**: dispara SQL Alerts cuando se degrada
# MAGIC
# MAGIC **Tiempo estimado:** 7 min

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk>=0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
TABLE = f"{FULL_SCHEMA}.metricas_agente_gold"

print(f"Tabla a monitorear: {TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tipos de monitor disponibles
# MAGIC
# MAGIC | Tipo | Uso | Cuándo |
# MAGIC |---|---|---|
# MAGIC | `TimeSeries` | Tablas con columna temporal | Métricas por hora/día (nuestro caso) |
# MAGIC | `Snapshot` | Tablas estáticas | Catálogos de productos, dimensiones |
# MAGIC | `InferenceLog` | Tablas con predicción + label | Performance de modelo en producción |
# MAGIC
# MAGIC Nuestra tabla `metricas_agente_gold` tiene la columna `hora` → vamos con `TimeSeries`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear el monitor

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    MonitorTimeSeries,
    MonitorMetric,
    MonitorMetricType,
)

w = WorkspaceClient()

# Custom metrics adicionales a las que Databricks calcula por defecto
custom_metrics = [
    MonitorMetric(
        name="pct_runs_exitosos",
        type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        input_columns=[":table"],
        definition="100.0 * SUM(runs_exitosos) / SUM(total_runs)",
        output_data_type='{"name":"output","type":"double"}',
    ),
    MonitorMetric(
        name="costo_total_usd",
        type=MonitorMetricType.CUSTOM_METRIC_TYPE_AGGREGATE,
        input_columns=[":table"],
        definition="SUM(costo_usd)",
        output_data_type='{"name":"output","type":"double"}',
    ),
]

monitor_kwargs = dict(
    table_name=TABLE,
    assets_dir=f"/Workspace/Users/{spark.sql('SELECT current_user() as u').collect()[0]['u']}/lakehouse_monitoring",
    output_schema_name=FULL_SCHEMA,
    time_series=MonitorTimeSeries(
        granularities=["1 hour", "1 day"],
        timestamp_col="hora",
    ),
    slicing_exprs=["model"],
    custom_metrics=custom_metrics,
)

try:
    monitor = w.quality_monitors.get(table_name=TABLE)
    print(f"✓ Monitor ya existe: status={monitor.status}")
except Exception:
    monitor = w.quality_monitors.create(**monitor_kwargs)
    print(f"✓ Monitor creado: status={monitor.status}")

print(f"\nVer en UI: Catalog → {SCHEMA} → metricas_agente_gold → tab 'Quality'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Refresh manual del monitor
# MAGIC
# MAGIC Después de un refresh, las tablas de profile + drift se materializan.

# COMMAND ----------

import time

# Lanzar un refresh
refresh = w.quality_monitors.run_refresh(table_name=TABLE)
print(f"Refresh lanzado: {refresh.refresh_id}")

# Esperar hasta que termine (típicamente 1-3 min)
for _ in range(30):
    r = w.quality_monitors.get_refresh(table_name=TABLE, refresh_id=refresh.refresh_id)
    if r.state in ("SUCCESS", "FAILED", "CANCELED"):
        print(f"  → {r.state}")
        break
    print(f"  ... {r.state}")
    time.sleep(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspeccionar las tablas de métricas generadas
# MAGIC
# MAGIC El monitor crea 2 tablas auxiliares en el output_schema_name:
# MAGIC - `<table>_profile_metrics` — estadísticas por columna y por ventana
# MAGIC - `<table>_drift_metrics` — comparación entre ventanas consecutivas

# COMMAND ----------

monitor = w.quality_monitors.get(table_name=TABLE)
print(f"Profile metrics table: {monitor.profile_metrics_table_name}")
print(f"Drift metrics table:   {monitor.drift_metrics_table_name}")
print(f"Dashboard ID:          {monitor.dashboard_id}")

# COMMAND ----------

# Profile metrics — promedio de latencia y costo por hora x modelo
# Estas tablas se materializan después del primer refresh exitoso (puede tomar varios minutos)
try:
    if monitor.profile_metrics_table_name:
        display(spark.sql(f"""
        SELECT
          window.start AS hora,
          slice_value AS modelo,
          column_name,
          ROUND(avg, 2) AS promedio,
          ROUND(percentile_50, 2) AS mediana,
          ROUND(percentile_95, 2) AS p95
        FROM {monitor.profile_metrics_table_name}
        WHERE column_name IN ('latencia_promedio_ms', 'costo_usd', 'feedback_negativo')
          AND log_type = 'INPUT'
        ORDER BY window.start DESC, modelo, column_name
        LIMIT 30
        """))
except Exception as e:
    print(f"⚠ Profile metrics aún no disponibles (refresh corriendo): {e}")
    print("  Reintentar en unos minutos cuando el primer refresh termine.")

# COMMAND ----------

# Drift metrics — qué tan diferentes son las ventanas consecutivas
try:
    if monitor.drift_metrics_table_name:
        display(spark.sql(f"""
        SELECT
          window.start AS hora,
          slice_value AS modelo,
          column_name,
          ROUND(drift_metric, 4) AS drift,
          drift_type
        FROM {monitor.drift_metrics_table_name}
        WHERE column_name IN ('latencia_promedio_ms', 'feedback_score_avg')
        ORDER BY drift DESC NULLS LAST
        LIMIT 20
        """))
except Exception as e:
    print(f"⚠ Drift metrics aún no disponibles (necesita ≥2 ventanas de refresh): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Custom Metrics queryable
# MAGIC
# MAGIC Las métricas custom que definimos también están en la tabla de profile.

# COMMAND ----------

try:
    if monitor.profile_metrics_table_name:
        display(spark.sql(f"""
        SELECT
          window.start AS hora,
          slice_value AS modelo,
          pct_runs_exitosos,
          ROUND(costo_total_usd, 4) AS costo_usd
        FROM {monitor.profile_metrics_table_name}
        WHERE pct_runs_exitosos IS NOT NULL
        ORDER BY hora DESC, modelo
        LIMIT 20
        """))
except Exception as e:
    print(f"⚠ Custom metrics aún no disponibles: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Dashboard auto-generado
# MAGIC
# MAGIC Databricks crea un Lakeview dashboard automáticamente con todas las métricas. Esto sustituye el dashboard custom de Grafana/PowerBI.

# COMMAND ----------

dashboard_url = f"/sql/dashboardsv3/{monitor.dashboard_id}"
print(f"Dashboard URL: {dashboard_url}")
print(f"\nAbrir en el workspace:")
print(f"  Sidebar → Dashboards → busca 'metricas_agente_gold'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Comparación side-by-side
# MAGIC
# MAGIC | Capacidad | Su código actual | Lakehouse Monitoring |
# MAGIC |---|---|---|
# MAGIC | Profile metrics | No tienen | Auto-generadas por columna |
# MAGIC | Drift detection | No tienen | Auto (vs baseline o ventana anterior) |
# MAGIC | Custom metrics | Implementar a mano | Definidas declarativamente |
# MAGIC | Refresh schedule | Cron jobs | Schedule managed (`schedule=...`) |
# MAGIC | Dashboard | Construir en Grafana | Auto-generado en Lakeview |
# MAGIC | Alertas | Implementar a mano | Integradas vía SQL Alerts (notebook 05) |
# MAGIC | Cobertura | Solo lo que instrumenten | Cualquier Delta table |
# MAGIC | **Configuración** | Código + cron + dashboard + alertas | **1 call al SDK** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo paso
# MAGIC
# MAGIC Continuar con: `05 - SQL Alerts` — vamos a poner alertas sobre las métricas que acabamos de capturar.

