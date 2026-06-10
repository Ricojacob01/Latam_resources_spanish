# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — SQL Alerts
# MAGIC
# MAGIC **Reemplaza**: `observability/AlertEvaluator` + `AlertProtocols` (sistema de alertas custom)
# MAGIC
# MAGIC ## ¿Qué hace su código actual?
# MAGIC
# MAGIC En `comfama-ai-core/observability/alerts.py`:
# MAGIC - `AlertEvaluator` que corre evaluaciones periódicas sobre métricas
# MAGIC - `AlertProtocols` con clases para email / Slack / webhook
# MAGIC - Cron jobs custom que disparan las evaluaciones
# MAGIC - Configuración YAML de umbrales
# MAGIC
# MAGIC ## ¿Qué hace Databricks?
# MAGIC
# MAGIC **Databricks SQL Alerts**:
# MAGIC - Cualquier query SQL puede convertirse en alerta
# MAGIC - Evaluación schedule managed (cada N minutos)
# MAGIC - Notificaciones a email, Slack, Teams, webhooks, PagerDuty
# MAGIC - Estado vivo en UI + histórico
# MAGIC - Definidas declarativamente (no más cron jobs)
# MAGIC
# MAGIC **Tiempo estimado:** 6 min

# COMMAND ----------

# MAGIC %pip install -q databricks-sdk>=0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Anatomía de una SQL Alert
# MAGIC
# MAGIC Una alerta = **Query SQL** + **Threshold** + **Schedule** + **Destination**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Query base: latencia P95 por modelo en última hora

# COMMAND ----------

# La query que vamos a usar como base de la alerta
ALERT_QUERY = f"""
SELECT
  model,
  ROUND(percentile(latency_ms, 0.95), 0) AS p95_latency_ms,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) AS errores
FROM {FULL_SCHEMA}.eventos_agente_silver
WHERE timestamp >= current_timestamp() - INTERVAL 1 HOUR
GROUP BY model
HAVING p95_latency_ms > 1800  -- threshold de degradación
"""

print("Query de la alerta:")
print(ALERT_QUERY)
print("\nVamos a verla en acción:")
display(spark.sql(ALERT_QUERY))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Crear un Warehouse SQL si no existe
# MAGIC
# MAGIC SQL Alerts requieren un SQL Warehouse para ejecutarse.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    EndpointInfoWarehouseType,
    CreateWarehouseRequestWarehouseType,
)

w = WorkspaceClient()

# Buscar un warehouse existente
warehouse_id = None
for wh in w.warehouses.list():
    if "starter" in (wh.name or "").lower() or "serverless" in (wh.name or "").lower():
        warehouse_id = wh.id
        print(f"Usando warehouse existente: {wh.name} ({wh.id})")
        break

if not warehouse_id:
    # Tomar el primero disponible
    for wh in w.warehouses.list():
        warehouse_id = wh.id
        print(f"Usando warehouse: {wh.name} ({wh.id})")
        break

if not warehouse_id:
    print("⚠ No hay warehouses. Crea uno desde SQL → SQL Warehouses antes de continuar.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Crear la query en SQL Warehouse + la alerta
# MAGIC
# MAGIC La API moderna es `w.alerts_v2` (v2 reemplaza el SDK viejo).

# COMMAND ----------

from databricks.sdk.service.sql import (
    CreateQueryRequestQuery,
)

# Primera vez: crear la query
query_name = "comfama_latencia_p95_alert"

# Buscar si ya existe
existing_query = None
for q in w.queries.list():
    if q.display_name == query_name:
        existing_query = q
        break

if existing_query:
    print(f"Query existe: {existing_query.id}")
    query_id = existing_query.id
else:
    new_query = w.queries.create(
        query=CreateQueryRequestQuery(
            display_name=query_name,
            description="Alert query: latencia P95 por modelo en última hora",
            query_text=ALERT_QUERY,
            warehouse_id=warehouse_id,
        )
    )
    query_id = new_query.id
    print(f"✓ Query creada: {query_id}")

# COMMAND ----------

# Crear la alerta sobre esa query
from databricks.sdk.service.sql import (
    AlertV2,
    AlertV2Evaluation,
    AlertV2OperandColumn,
    AlertV2Operand,
    AlertV2OperandValue,
    ComparisonOperator,
    AlertEvaluationState,
    CronSchedule,
)

alert_name = "comfama_alert_latencia_alta"

# Buscar si ya existe
existing_alert = None
try:
    for a in w.alerts_v2.list_alerts():
        if a.display_name == alert_name:
            existing_alert = a
            break
except Exception as e:
    print(f"(list_alerts fallback: {e})")

evaluation = AlertV2Evaluation(
    comparison_operator=ComparisonOperator.GREATER_THAN,
    threshold=AlertV2Operand(
        value=AlertV2OperandValue(double_value=1800.0)
    ),
    source=AlertV2OperandColumn(name="p95_latency_ms"),
    empty_result_state=AlertEvaluationState.OK,
)

if existing_alert:
    print(f"✓ Alerta ya existe: {existing_alert.id}")
    alert = existing_alert
else:
    alert = w.alerts_v2.create_alert(
        alert=AlertV2(
            display_name=alert_name,
            query_text=ALERT_QUERY,
            warehouse_id=warehouse_id,
            evaluation=evaluation,
            schedule=CronSchedule(
                quartz_cron_schedule="0 */5 * * * ?",
                timezone_id="America/Bogota",
            ),
        )
    )
    print(f"✓ Alerta creada: {alert.id}")
    print(f"  Eval cuando p95_latency_ms > 1800ms en cualquier fila")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Catálogo de alertas que tendrías para el agente
# MAGIC
# MAGIC Lo que probablemente quieran cubrir en producción:

# COMMAND ----------

ALERT_CATALOG = [
    {
        "nombre": "agente_latencia_alta",
        "query": "p95 latencia > 2000ms en última hora",
        "threshold": "p95 > 2000",
        "severity": "warn",
        "destino": "Slack #plataforma-ia",
    },
    {
        "nombre": "agente_error_rate",
        "query": "% errores en última hora",
        "threshold": "errors / total > 5%",
        "severity": "critical",
        "destino": "Email + PagerDuty",
    },
    {
        "nombre": "agente_feedback_negativo",
        "query": "thumbs_down en última hora",
        "threshold": "negativos > 10",
        "severity": "warn",
        "destino": "Slack #producto",
    },
    {
        "nombre": "costo_dia_excedido",
        "query": "costo_usd_dia > budget",
        "threshold": "costo > $50/día",
        "severity": "warn",
        "destino": "Email finanzas + Slack #finops",
    },
    {
        "nombre": "drift_latencia",
        "query": "Lakehouse Monitoring drift score",
        "threshold": "drift > 0.3 vs baseline",
        "severity": "warn",
        "destino": "Slack #plataforma-ia",
    },
    {
        "nombre": "tabla_no_se_actualiza",
        "query": "MAX(hora) en metricas_agente_gold",
        "threshold": "más viejo que 2 horas",
        "severity": "critical",
        "destino": "PagerDuty oncall",
    },
]

import pandas as pd
display(spark.createDataFrame(pd.DataFrame(ALERT_CATALOG)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schedule + Notification (vía UI)
# MAGIC
# MAGIC La parte de cadencia y destinos es más cómoda en la UI:
# MAGIC
# MAGIC 1. Sidebar izquierdo → **SQL** → **Alerts** → busca `comfama_alert_latencia_alta`
# MAGIC 2. Click en la alerta → tab **Schedule** → seleccionar cron (ej. cada 5 min)
# MAGIC 3. Tab **Notifications** → agregar destinos (email, Slack via webhook, Teams, PagerDuty)
# MAGIC
# MAGIC También se puede hacer programáticamente con `w.alerts_v2.update(...)`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. (Opcional) Trigger una alerta sintética (forzar p95 alto)
# MAGIC
# MAGIC Si quieres ver la alerta dispararse en la UI, ejecuta esta sección. La saltamos en el auto-deploy para evitar tipos no inferibles.

# COMMAND ----------

dbutils.notebook.exit("Alert + query deployed. Skipping synthetic injection in auto-deploy.")

# COMMAND ----------

# Inyectamos 50 eventos con latencia muy alta para hacer disparar la alerta
import random
from datetime import datetime, timedelta

bad_rows = []
now = datetime.utcnow()
for i in range(50):
    bad_rows.append({
        "run_id": f"run_bad_{i:04d}",
        "user_id": "user_test",
        "timestamp": now - timedelta(minutes=random.randint(0, 30)),
        "model": "llama-3-3-70b",
        "tool_used": "search_docs",
        "intent": "general",
        "tokens_input": 2000,
        "tokens_output": 800,
        "tokens_total": 2800,
        "costo_usd_aprox": 0.002,
        "latency_ms": random.randint(2500, 4500),  # alta latencia
        "success": True,
        "user_feedback": None,
        "feedback_score": 0,
        "hora": (now - timedelta(minutes=random.randint(0, 30))).replace(minute=0, second=0, microsecond=0),
    })

spark.createDataFrame(bad_rows).write.mode("append") \
    .saveAsTable(f"{FULL_SCHEMA}.eventos_agente_silver")

print("✓ 50 eventos con latencia alta inyectados")
print("\nLa query de la alerta ahora debe retornar filas:")
display(spark.sql(ALERT_QUERY))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Trigger manual + ver estado de la alerta

# COMMAND ----------

# Ver historia de evaluaciones
print("Estado actual de la alerta:")
try:
    current = w.alerts_v2.get_alert(id=alert.id)
    print(f"  Display name: {current.display_name}")
    print(f"  Lifecycle:    {current.lifecycle_state}")
except Exception as e:
    print(f"(get_alert: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Comparación side-by-side
# MAGIC
# MAGIC | Capacidad | Su código actual | Databricks SQL Alerts |
# MAGIC |---|---|---|
# MAGIC | Definición de alerta | Clase `AlertEvaluator` + YAML | SQL query + threshold (UI o API) |
# MAGIC | Schedule | Cron jobs custom | Managed (`cron` o `every N min`) |
# MAGIC | Email | `EmailProtocol` custom | Nativo |
# MAGIC | Slack | `SlackProtocol` custom (webhook) | Webhook destination nativo |
# MAGIC | PagerDuty | Implementar | Nativo |
# MAGIC | Historial | Tabla custom | UI + API + audit log |
# MAGIC | Re-evaluar manual | Trigger custom | `alerts_v2.trigger()` o UI |
# MAGIC | Estado en vivo | (no tienen) | Lifecycle states |
# MAGIC | **LOC en su repo** | **~300** | **0** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo paso
# MAGIC
# MAGIC Continuar con: `06 - FinOps (System Tables)` — query a costos reales DBU + budget.

