# Databricks notebook source
# MAGIC %md
# MAGIC # 🔭 Sesión 2 · 02 — Observabilidad (MLflow Tracing)
# MAGIC
# MAGIC **Meta:** ver **por dentro** cada interacción del agente — qué tools llamó, qué recuperó del RAG, cuántos tokens,
# MAGIC cuánto tardó — con **MLflow 3 Tracing**, sin instrumentar nada a mano.
# MAGIC
# MAGIC > **Equivale a: `TelemetryManager`** (OTLP + Prometheus). El tracing es automático para agentes servidos y se
# MAGIC > explora en la UI de MLflow; no hay que mantener colectores ni dashboards de telemetría custom.
# MAGIC
# MAGIC Módulo **dual-mode**: explorar trazas **🖱️ en la UI de MLflow** o **⌨️ con `mlflow.search_traces`**.

# COMMAND ----------

# MAGIC %pip install -U mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — trazas automáticas del agente servido
# MAGIC El agente desplegado con `agents.deploy` **traza automáticamente** cada request.
# MAGIC 1. **Machine Learning** → **Experiments** → experimento del agente `agente_afiliados`.
# MAGIC 2. Pestaña **Traces**: cada fila es una conversación. Ábrela para ver el **árbol de spans**:
# MAGIC    `chat → tool_call(buscar_conocimiento) → retrieval → tool_call(crear_reserva) → respuesta`, con latencia y tokens.
# MAGIC 3. También en **Serving** → endpoint del agente → pestaña de **traces** las ves en vivo.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — trazar y consultar
# MAGIC Envolvemos las llamadas al agente con `@mlflow.trace` para registrarlas en un experimento y consultarlas con
# MAGIC `mlflow.search_traces` (que devuelve un DataFrame, listo para analítica).

# COMMAND ----------

import mlflow
from mlflow.deployments import get_deploy_client
from databricks.sdk import WorkspaceClient

EMAIL = WorkspaceClient().current_user.me().user_name
mlflow.set_experiment(f"/Users/{EMAIL}/comfama_agente_obs")
client = get_deploy_client("databricks")

@mlflow.trace(name="consulta_afiliado")
def consulta(texto):
    r = client.predict(endpoint=AGENT_ENDPOINT, inputs={"messages": [{"role": "user", "content": texto}]})
    return r["messages"][-1]["content"]

for q in ["¿Cómo postulo al subsidio de vivienda?",
          "Reserva el programa 9 para el afiliado 1003",
          "¿Qué cursos de educación con cupo hay en Medellín?"]:
    print("👤", q, "\n🤖", consulta(q), "\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consultar y analizar las trazas

# COMMAND ----------

traces = mlflow.search_traces(max_results=20)
print(f"Trazas encontradas: {len(traces)}")
if len(traces):
    col = "execution_time_ms" if "execution_time_ms" in traces.columns else None
    if col:
        print(f"Latencia (ms): p50 = {traces[col].median():.0f} | p95 = {traces[col].quantile(0.95):.0f}")
display(traces)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC Observabilidad completa del agente — spans, tools, RAG, tokens y latencia — sin mantener un stack de telemetría.
# MAGIC Estas trazas se persisten y son la base de **evaluación de calidad** y del **monitoreo** (módulo 04).
# MAGIC
# MAGIC ### ▶️ Siguiente: `03 - Gobernanza (Unity Catalog)`

