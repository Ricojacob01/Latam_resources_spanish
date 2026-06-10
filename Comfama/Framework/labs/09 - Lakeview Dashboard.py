# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — Lakeview Dashboard
# MAGIC
# MAGIC Crea un dashboard Lakeview que consolida métricas del agente, FinOps y monitoreo.
# MAGIC
# MAGIC **Reemplaza:** dashboard custom en Grafana/PowerBI sobre métricas exportadas.
# MAGIC
# MAGIC **Tiempo estimado:** 3 min

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk>=0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
DASHBOARD_NAME = "Comfama Agente — Observability & FinOps"

CURRENT_USER = spark.sql("SELECT current_user() as u").collect()[0]["u"]
PARENT_PATH = f"/Users/{CURRENT_USER}/Latam_resources_spanish/Comfama_framework"

print(f"Dashboard: {DASHBOARD_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Identificar el warehouse a usar

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

warehouses = list(w.warehouses.list())
warehouse_id = None
for wh in warehouses:
    if wh.state and wh.state.value == "RUNNING":
        warehouse_id = wh.id
        print(f"Using warehouse: {wh.name} ({wh.id})")
        break

if not warehouse_id and warehouses:
    warehouse_id = warehouses[0].id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Definir el dashboard (Lakeview JSON spec)

# COMMAND ----------

# Dashboards de Lakeview se definen via "serialized_dashboard" JSON
# Ref: https://docs.databricks.com/en/dashboards/lakeview/index.html

dashboard_spec = {
    "datasets": [
        {
            "name": "metricas_hora",
            "displayName": "Métricas por hora",
            "queryLines": [
                f"SELECT hora, model, total_runs, runs_exitosos, latencia_promedio_ms, latencia_p95_ms, costo_usd, feedback_score_avg FROM {FULL_SCHEMA}.metricas_agente_gold ORDER BY hora"
            ],
        },
        {
            "name": "consumo_dbu",
            "displayName": "Consumo DBU diario",
            "queryLines": [
                "SELECT date(usage_start_time) AS dia, sku_name, ROUND(SUM(usage_quantity), 2) AS dbus FROM system.billing.usage WHERE usage_start_time >= current_date() - INTERVAL 7 DAYS GROUP BY dia, sku_name ORDER BY dia"
            ],
        },
        {
            "name": "intents",
            "displayName": "Top intents",
            "queryLines": [
                f"SELECT intent, COUNT(*) AS total, ROUND(AVG(latency_ms), 0) AS latencia_avg FROM {FULL_SCHEMA}.eventos_agente_silver GROUP BY intent ORDER BY total DESC"
            ],
        },
        {
            "name": "feedback_dia",
            "displayName": "Feedback diario",
            "queryLines": [
                f"SELECT date(timestamp) AS dia, SUM(CASE WHEN user_feedback='thumbs_up' THEN 1 ELSE 0 END) AS positivos, SUM(CASE WHEN user_feedback='thumbs_down' THEN 1 ELSE 0 END) AS negativos FROM {FULL_SCHEMA}.eventos_agente_silver GROUP BY dia ORDER BY dia"
            ],
        },
    ],
    "pages": [
        {
            "name": "main",
            "displayName": "Agente Comfama",
            "layout": [
                {
                    "widget": {
                        "name": "title",
                        "textbox_spec": "# 🤝 Comfama — Agente Conversacional\n\n_Métricas de observability + costos + calidad_",
                    },
                    "position": {"x": 0, "y": 0, "width": 6, "height": 2},
                },
                {
                    "widget": {
                        "name": "chart_latencia",
                        "queries": [
                            {
                                "name": "main_query",
                                "query": {
                                    "datasetName": "metricas_hora",
                                    "fields": [
                                        {"name": "hora", "expression": "`hora`"},
                                        {"name": "model", "expression": "`model`"},
                                        {"name": "latencia_p95_ms", "expression": "`latencia_p95_ms`"},
                                    ],
                                    "disaggregated": True,
                                },
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "line",
                            "encodings": {
                                "x": {"fieldName": "hora", "scale": {"type": "temporal"}, "displayName": "Hora"},
                                "y": {"fieldName": "latencia_p95_ms", "scale": {"type": "quantitative"}, "displayName": "Latencia P95 (ms)"},
                                "color": {"fieldName": "model", "scale": {"type": "categorical"}},
                            },
                            "frame": {"showTitle": True, "title": "Latencia P95 por modelo"},
                        },
                    },
                    "position": {"x": 0, "y": 2, "width": 3, "height": 4},
                },
                {
                    "widget": {
                        "name": "chart_runs",
                        "queries": [
                            {
                                "name": "main_query",
                                "query": {
                                    "datasetName": "metricas_hora",
                                    "fields": [
                                        {"name": "hora", "expression": "`hora`"},
                                        {"name": "total_runs", "expression": "`total_runs`"},
                                    ],
                                    "disaggregated": True,
                                },
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "hora", "scale": {"type": "temporal"}, "displayName": "Hora"},
                                "y": {"fieldName": "total_runs", "scale": {"type": "quantitative"}, "displayName": "Total runs"},
                            },
                            "frame": {"showTitle": True, "title": "Volumen de conversaciones"},
                        },
                    },
                    "position": {"x": 3, "y": 2, "width": 3, "height": 4},
                },
                {
                    "widget": {
                        "name": "chart_costos",
                        "queries": [
                            {
                                "name": "main_query",
                                "query": {
                                    "datasetName": "consumo_dbu",
                                    "fields": [
                                        {"name": "dia", "expression": "`dia`"},
                                        {"name": "sku_name", "expression": "`sku_name`"},
                                        {"name": "dbus", "expression": "`dbus`"},
                                    ],
                                    "disaggregated": True,
                                },
                            }
                        ],
                        "spec": {
                            "version": 3,
                            "widgetType": "bar",
                            "encodings": {
                                "x": {"fieldName": "dia", "scale": {"type": "temporal"}, "displayName": "Día"},
                                "y": {"fieldName": "dbus", "scale": {"type": "quantitative"}, "displayName": "DBUs"},
                                "color": {"fieldName": "sku_name", "scale": {"type": "categorical"}},
                            },
                            "frame": {"showTitle": True, "title": "Consumo DBU por SKU"},
                        },
                    },
                    "position": {"x": 0, "y": 6, "width": 3, "height": 4},
                },
                {
                    "widget": {
                        "name": "table_intents",
                        "queries": [
                            {
                                "name": "main_query",
                                "query": {
                                    "datasetName": "intents",
                                    "fields": [
                                        {"name": "intent", "expression": "`intent`"},
                                        {"name": "total", "expression": "`total`"},
                                        {"name": "latencia_avg", "expression": "`latencia_avg`"},
                                    ],
                                    "disaggregated": True,
                                },
                            }
                        ],
                        "spec": {
                            "version": 1,
                            "widgetType": "table",
                            "encodings": {
                                "columns": [
                                    {"fieldName": "intent", "displayName": "Intent"},
                                    {"fieldName": "total", "displayName": "Total"},
                                    {"fieldName": "latencia_avg", "displayName": "Latencia avg (ms)"},
                                ],
                            },
                            "frame": {"showTitle": True, "title": "Top intents"},
                        },
                    },
                    "position": {"x": 3, "y": 6, "width": 3, "height": 4},
                },
            ],
        }
    ],
}

print(f"✓ Spec construida ({len(dashboard_spec['datasets'])} datasets, {len(dashboard_spec['pages'][0]['layout'])} widgets)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear el dashboard via API

# COMMAND ----------

from databricks.sdk.service.dashboards import Dashboard

dashboard = w.lakeview.create(
    dashboard=Dashboard(
        display_name=DASHBOARD_NAME,
        parent_path=PARENT_PATH,
        serialized_dashboard=json.dumps(dashboard_spec),
        warehouse_id=warehouse_id,
    )
)

print(f"✓ Dashboard creado")
print(f"  ID: {dashboard.dashboard_id}")
print(f"  Path: {dashboard.path}")
print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Publicar el dashboard

# COMMAND ----------

try:
    published = w.lakeview.publish(
        dashboard_id=dashboard.dashboard_id,
        warehouse_id=warehouse_id,
    )
    print(f"✓ Publicado")
except Exception as e:
    print(f"Publicación: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
host = w.config.host

print(f"🎉 Dashboard Lakeview creado: {DASHBOARD_NAME}")
print(f"   ID:  {dashboard.dashboard_id}")
print(f"   URL: {host}/dashboardsv3/{dashboard.dashboard_id}")
print()
print("Datasets:")
for ds in dashboard_spec["datasets"]:
    print(f"  - {ds['name']}: {ds['displayName']}")
print()
print("Widgets:")
print(f"  - Latencia P95 por modelo (line)")
print(f"  - Volumen de conversaciones (bar)")
print(f"  - Consumo DBU por SKU (bar)")
print(f"  - Top intents (table)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo paso
# MAGIC
# MAGIC `10 - Architecture & Monitoring Summary` — el notebook final que tie todos los assets con su gobierno y observability.

