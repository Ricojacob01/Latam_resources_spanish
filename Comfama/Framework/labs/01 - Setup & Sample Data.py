# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Setup & Sample Data
# MAGIC
# MAGIC Crea el schema, volume y tablas Bronze/Silver/Gold que el resto del demo va a consumir.
# MAGIC
# MAGIC **Lo que reemplaza:**
# MAGIC - El bootstrap manual de Cosmos DB + Storage Account + Container Apps
# MAGIC - El YAML de configuración que viven en `TemplateAgentes`
# MAGIC
# MAGIC **Tiempo estimado:** 3 min

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuración

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
VOLUME = "archivos"

FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

print(f"Catalog: {CATALOG}")
print(f"Schema:  {FULL_SCHEMA}")
print(f"Volume:  {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema + Volume

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FULL_SCHEMA} COMMENT 'Demo Comfama × Databricks — reemplazo del framework custom'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FULL_SCHEMA}.{VOLUME}")
print(f"✓ Schema {FULL_SCHEMA} listo")
print(f"✓ Volume {VOLUME_PATH} listo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Eventos crudos del agente
# MAGIC
# MAGIC Simula los eventos que su `ExecutionTracker` actualmente persiste en la tabla `ai_execution_runs`. Aquí los vamos a generar localmente para no depender de servicios externos.

# COMMAND ----------

import random
import json
from datetime import datetime, timedelta
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType, BooleanType

random.seed(42)

# Genera ~500 eventos sintéticos de interacciones con un agente conversacional
N = 500
START = datetime(2026, 5, 1)
USERS = [f"user_{i:03d}" for i in range(40)]
MODELS = ["llama-3-3-70b", "claude-3-7-sonnet", "gpt-4o"]
TOOLS = ["search_docs", "query_lakebase", "call_mcp_business_systems", "summarize", None]
INTENTS = ["pregunta_subsidio", "estado_solicitud", "queja", "consulta_servicio", "general"]

rows = []
for i in range(N):
    ts = START + timedelta(minutes=i * 3 + random.randint(0, 30))
    rows.append({
        "run_id": f"run_{i:05d}",
        "user_id": random.choice(USERS),
        "timestamp": ts,
        "model": random.choice(MODELS),
        "tool_used": random.choice(TOOLS),
        "intent": random.choice(INTENTS),
        "tokens_input": random.randint(100, 4000),
        "tokens_output": random.randint(50, 1500),
        "latency_ms": int(random.gauss(1200, 400) + (300 if random.random() < 0.05 else 0)),
        "success": random.random() > 0.03,
        "user_feedback": random.choice([None, None, None, "thumbs_up", "thumbs_down"]),
    })

bronze_df = spark.createDataFrame(rows)
bronze_df.write.mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{FULL_SCHEMA}.eventos_agente_bronze")

display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.eventos_agente_bronze LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Enriquecimiento

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.eventos_agente_silver AS
SELECT
  run_id,
  user_id,
  timestamp,
  date_trunc('hour', timestamp) AS hora,
  model,
  tool_used,
  intent,
  tokens_input,
  tokens_output,
  tokens_input + tokens_output AS tokens_total,
  -- Costo aproximado: $0.50/M input, $1.50/M output para llama-3-3-70b
  ROUND((tokens_input * 0.5 + tokens_output * 1.5) / 1000000.0, 6) AS costo_usd_aprox,
  latency_ms,
  success,
  user_feedback,
  CASE WHEN user_feedback = 'thumbs_up' THEN 1
       WHEN user_feedback = 'thumbs_down' THEN -1
       ELSE 0 END AS feedback_score
FROM {FULL_SCHEMA}.eventos_agente_bronze
""")

print(f"✓ {FULL_SCHEMA}.eventos_agente_silver creado")
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.eventos_agente_silver LIMIT 5"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Métricas agregadas por hora
# MAGIC
# MAGIC Esta es la tabla que vamos a monitorear, alertar, y dashboardear en los siguientes notebooks.

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {FULL_SCHEMA}.metricas_agente_gold AS
SELECT
  hora,
  model,
  COUNT(*) AS total_runs,
  SUM(CASE WHEN success THEN 1 ELSE 0 END) AS runs_exitosos,
  ROUND(AVG(latency_ms), 0) AS latencia_promedio_ms,
  percentile(latency_ms, 0.95) AS latencia_p95_ms,
  SUM(tokens_total) AS tokens_total,
  ROUND(SUM(costo_usd_aprox), 4) AS costo_usd,
  ROUND(AVG(feedback_score), 3) AS feedback_score_avg,
  SUM(CASE WHEN user_feedback = 'thumbs_down' THEN 1 ELSE 0 END) AS feedback_negativo
FROM {FULL_SCHEMA}.eventos_agente_silver
GROUP BY hora, model
ORDER BY hora DESC, model
""")

print(f"✓ {FULL_SCHEMA}.metricas_agente_gold creado")
display(spark.sql(f"SELECT * FROM {FULL_SCHEMA}.metricas_agente_gold LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC Verifica que todo quedó listo:

# COMMAND ----------

display(spark.sql(f"""
SELECT
  table_name,
  table_type,
  comment
FROM system.information_schema.tables
WHERE table_catalog = '{CATALOG}'
  AND table_schema = '{SCHEMA}'
ORDER BY table_name
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguientes pasos
# MAGIC
# MAGIC Las tablas están listas. Ahora puedes ejecutar cualquiera de los siguientes notebooks:
# MAGIC
# MAGIC - `02 - Observability (MLflow Tracing)` — tracing GenAI de un agente
# MAGIC - `03 - Governance (Unity Catalog)` — lineage + audit + ABAC
# MAGIC - `04 - Monitoring (Lakehouse Monitoring)` — drift sobre `metricas_agente_gold`
# MAGIC - `05 - SQL Alerts` — alertas automáticas sobre latencia y errores
# MAGIC - `06 - FinOps (System Tables)` — análisis de consumo DBU

