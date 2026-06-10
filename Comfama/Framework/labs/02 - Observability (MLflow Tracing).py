# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Observability con MLflow Tracing
# MAGIC
# MAGIC **Reemplaza**: `observability/TelemetryManager` (Singleton + OTLP + Prometheus + decoradores `@instrument`)
# MAGIC
# MAGIC ## ¿Qué hace su código actual?
# MAGIC
# MAGIC En `comfama-ai-core/observability/telemetry.py` mantienen ~600 líneas que:
# MAGIC - Instancian un `TelemetryManager` Singleton
# MAGIC - Configuran exporters OTLP a Prometheus + Console
# MAGIC - Decoran cada función del agente con `@instrument` y `@instrument_method`
# MAGIC - Persisten trazas en una tabla custom `ai_execution_runs`
# MAGIC - `ExecutionTracker` con lógica de "se modifica solo en primer step"
# MAGIC
# MAGIC ## ¿Qué hace Databricks?
# MAGIC
# MAGIC **MLflow 3 GenAI Tracing** captura automáticamente:
# MAGIC - Cada llamada a un LLM (input, output, tokens, latencia)
# MAGIC - Cada step intermedio de un agente (retrieve, tool call, generation)
# MAGIC - Inputs / outputs estructurados
# MAGIC - Eventos OTel-compatibles (OTLP nativo)
# MAGIC
# MAGIC Y lo expone en UI + SQL + API. Sin decoradores manuales.
# MAGIC
# MAGIC **Tiempo estimado:** 8 min

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %pip install -q mlflow>=3.0.0 databricks-sdk>=0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

EXPERIMENT_NAME = f"/Users/{spark.sql('SELECT current_user() as u').collect()[0]['u']}/comfama_observability_demo"

import mlflow
mlflow.set_experiment(EXPERIMENT_NAME)
print(f"✓ MLflow experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tracing automático con autolog
# MAGIC
# MAGIC La forma más simple de observability: **una línea**. Compare esto contra las ~600 LOC de su `TelemetryManager`.

# COMMAND ----------

import mlflow

# Esto reemplaza TODO el setup de OpenTelemetry + Prometheus + console exporters
mlflow.openai.autolog()  # auto-trace OpenAI / Databricks Foundation Models

print("✓ Autolog activado para todas las llamadas LLM")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tracing manual con decoradores `@mlflow.trace`
# MAGIC
# MAGIC Para funciones custom del agente — equivalente a su `@instrument`, pero builtin.

# COMMAND ----------

import mlflow
import time
import random


@mlflow.trace(span_type="RETRIEVER")
def retrieve_context(query: str) -> list[str]:
    """Simula búsqueda en Vector Search."""
    time.sleep(random.uniform(0.05, 0.15))
    return [
        "Subsidio escolar: cobertura hasta el grado 11",
        "Subsidio de vivienda: monto vigente $20M",
        "Servicios de salud: cobertura para afiliados categoría A y B",
    ]


@mlflow.trace(span_type="TOOL")
def query_lakebase(user_id: str) -> dict:
    """Simula query a Lakebase (estado conversacional)."""
    time.sleep(random.uniform(0.02, 0.08))
    return {"last_intent": "estado_solicitud", "session_count": 3}


@mlflow.trace(span_type="LLM")
def llm_generate(prompt: str, context: list[str]) -> str:
    """Simula llamada al LLM (en producción usarías Model Serving)."""
    time.sleep(random.uniform(0.5, 1.2))
    return f"Basado en el contexto proporcionado, la respuesta para '{prompt}' es..."


@mlflow.trace(span_type="CHAIN", name="agente_comfama")
def run_agente(query: str, user_id: str) -> dict:
    """Endpoint completo del agente — toda la traza se captura automáticamente."""
    ctx = retrieve_context(query)
    user_state = query_lakebase(user_id)
    answer = llm_generate(query, ctx)
    return {
        "answer": answer,
        "sources": ctx,
        "user_state": user_state,
    }


# Ejecuta el agente — la traza completa se persiste automáticamente
result = run_agente(query="¿Cuál es el monto del subsidio de vivienda?", user_id="user_001")
print("Respuesta:", result["answer"][:100], "...")
print()
print("✓ Traza completa registrada en MLflow")
print(f"  Ver en: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Ejecutar 50 corridas — observar trazas en agregado
# MAGIC
# MAGIC Esto se vería en su sistema actual como 50 inserts a `ai_execution_runs` + 50 spans manuales. En MLflow es automático.

# COMMAND ----------

queries = [
    "¿Cómo solicito el subsidio escolar?",
    "Estado de mi solicitud de vivienda",
    "Horarios de atención en la sede principal",
    "Necesito hablar con un humano",
    "¿Qué documentos requiero?",
]

for i in range(50):
    q = random.choice(queries)
    user = f"user_{random.randint(1, 40):03d}"
    run_agente(q, user)

print("✓ 50 ejecuciones registradas con tracing completo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Query a las trazas (vía API)
# MAGIC
# MAGIC Reemplaza el `ExecutionTracker.query()` custom.

# COMMAND ----------

from mlflow.client import MlflowClient

client = MlflowClient()
exp = client.get_experiment_by_name(EXPERIMENT_NAME)

# Buscar últimas trazas
traces = client.search_traces(experiment_ids=[exp.experiment_id], max_results=10)

print(f"Total trazas recientes: {len(traces)}\n")
for t in traces[:3]:
    print(f"Trace {t.info.trace_id[:12]}...")
    print(f"  Duración: {(t.info.execution_time_ms or 0):.0f}ms")
    print(f"  Spans:    {len(t.data.spans)}")
    print(f"  Estado:   {t.info.status}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Comparación side-by-side
# MAGIC
# MAGIC | Capacidad | Su código actual | MLflow 3 |
# MAGIC |---|---|---|
# MAGIC | Trace span por función | Decorator `@instrument` custom | `@mlflow.trace` builtin |
# MAGIC | Exporter OTLP | Setup manual de `OTLPSpanExporter` | Nativo |
# MAGIC | Exporter Prometheus | `PrometheusMetricReader` config | Inferences tables + System Tables |
# MAGIC | Persistencia de runs | Tabla custom `ai_execution_runs` | MLflow Tracking Store |
# MAGIC | UI para explorar trazas | (no tienen) | MLflow UI |
# MAGIC | Búsqueda de trazas | SQL ad-hoc sobre la tabla | `client.search_traces(...)` |
# MAGIC | Linking traces ↔ código | Manual con `run_id` | Automático con `trace_id` |
# MAGIC | Evaluation harness | No tienen | `mlflow.evaluate` builtin |
# MAGIC | **Líneas de código en su repo** | **~600 LOC** | **0 (managed)** |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Inference Tables (bonus)
# MAGIC
# MAGIC Cuando despliegues el agente a Model Serving, Databricks auto-genera una **inference table** que persiste cada request con su trace. Esto reemplaza completamente la tabla `ai_execution_runs`.
# MAGIC
# MAGIC ```python
# MAGIC # Al crear el endpoint, activa inference table:
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC from databricks.sdk.service.serving import EndpointCoreConfigInput, AutoCaptureConfigInput
# MAGIC
# MAGIC w = WorkspaceClient()
# MAGIC w.serving_endpoints.create(
# MAGIC     name="agente_comfama",
# MAGIC     config=EndpointCoreConfigInput(
# MAGIC         served_entities=[...],
# MAGIC         auto_capture_config=AutoCaptureConfigInput(
# MAGIC             catalog_name="ardemo_classic_dnubtw_catalog",
# MAGIC             schema_name="comfama",
# MAGIC             table_name_prefix="agente_inference",
# MAGIC             enabled=True,
# MAGIC         ),
# MAGIC     ),
# MAGIC )
# MAGIC # → Crea automáticamente la tabla `comfama.agente_inference_payload` con request/response/latency
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Próximo paso
# MAGIC
# MAGIC Ver el experiment en la UI:
# MAGIC
# MAGIC - Sidebar izquierdo → **Experiments** → busca `comfama_observability_demo`
# MAGIC - Cada run muestra el árbol completo de spans con timing
# MAGIC - Click en cualquier trace para ver input/output de cada step
# MAGIC
# MAGIC Continuar con: `03 - Governance (Unity Catalog)`

