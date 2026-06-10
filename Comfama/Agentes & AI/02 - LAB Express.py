# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — LAB Express 🧪
# MAGIC
# MAGIC **25 minutos.** Tocamos Playground en la UI + algunas llamadas reales desde notebook.
# MAGIC
# MAGIC No es hands-on completo — es para que **tengan la primera experiencia táctil**. El workshop deep-dive del fin de mes profundiza en cada paso.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — Playground (10 min, UI)
# MAGIC
# MAGIC Vamos a la UI del Playground. **No vamos a escribir código en esta parte.**
# MAGIC
# MAGIC ### Pasos
# MAGIC
# MAGIC 1. **Abre el Playground** en otra pestaña del browser:
# MAGIC 2. En la esquina superior derecha hay un dropdown de modelo. Por default suele estar **Llama 3.3 70B**.
# MAGIC 3. Escribe en el chat: *"Explícame qué es un subsidio de vivienda en Colombia en 2 párrafos"*
# MAGIC 4. Lee la respuesta. **OK, eso fue una llamada a un Foundation Model.**
# MAGIC 5. Cambia el modelo a **Claude Sonnet 4.5** (dropdown).
# MAGIC 6. Manda el mismo prompt. Compara las respuestas.
# MAGIC 7. Click en el botón **"Compare"** arriba — ahora puedes mandar el mismo prompt a 2 modelos lado a lado.
# MAGIC 8. **Click en "View code"** arriba a la derecha — verás el código Python que reproduce esa misma llamada. Esto es la magia: del prototipo en UI al notebook con un click.
# MAGIC
# MAGIC ### Tu turno
# MAGIC
# MAGIC - Prueba un caso de uso real de Comfama: *"Redacta un mensaje cordial para un afiliado que tiene una solicitud de subsidio en estudio"*
# MAGIC - Compara la respuesta de 2 modelos
# MAGIC - Click "View code" → copia el snippet (lo vas a usar en la siguiente sección)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Llamadas directas desde notebook (10 min)
# MAGIC
# MAGIC Ahora vamos a llamar los mismos modelos pero desde Python. Idéntica API. La misma capa que el Playground está usando por debajo.

# COMMAND ----------

# MAGIC %pip install -q openai>=1.40 databricks-sdk>=0.30.0 mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# Para llamar a Foundation Models desde notebook, usamos el cliente de MLflow Deployments
# (que también es el cliente del AI Gateway)
import mlflow
from mlflow.deployments import get_deploy_client
client = get_deploy_client("databricks")

# Llamada a Llama 3.3
respuesta = client.predict(
    endpoint="databricks-meta-llama-3-3-70b-instruct",
    inputs={
        "messages": [
            {"role": "user", "content": "En una frase corta: ¿qué es Mosaic AI Gateway?"},
        ],
        "max_tokens": 100,
    },
)
print("LLAMA 3.3:")
print(respuesta["choices"][0]["message"]["content"])

# COMMAND ----------

# Cambiar modelo es literalmente cambiar el endpoint name
respuesta2 = client.predict(
    endpoint="databricks-claude-haiku-4-5",
    inputs={
        "messages": [
            {"role": "user", "content": "En una frase corta: ¿qué es Mosaic AI Gateway?"},
        ],
        "max_tokens": 100,
    },
)
print("CLAUDE HAIKU 4.5:")
print(respuesta2["choices"][0]["message"]["content"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Punto clave
# MAGIC
# MAGIC La **misma función Python** (`client.predict`) habla con cualquier modelo cambiando solo el `endpoint=`. Esto es el **Unified Query Interface** que vimos en los slides.
# MAGIC
# MAGIC Si Comfama tiene hoy código diferente para Azure OpenAI, Anthropic, etc., todo eso se reemplaza por este patrón único.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — AI Gateway features en endpoints existentes (5 min)
# MAGIC
# MAGIC Los Foundation Model endpoints (los `databricks-*`) ya vienen con Gateway activado. Veamos qué features tiene cada uno.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Ver config de un endpoint
ep = w.serving_endpoints.get("databricks-meta-llama-3-3-70b-instruct")
print(f"Endpoint: {ep.name}")
print(f"State: {ep.state.ready}")
print(f"\nAI Gateway config:")
if ep.ai_gateway:
    print(f"  Usage tracking: {ep.ai_gateway.usage_tracking_config}")
    print(f"  Inference table: {ep.ai_gateway.inference_table_config}")
    print(f"  Rate limits: {ep.ai_gateway.rate_limits}")
    print(f"  Guardrails: {ep.ai_gateway.guardrails}")
    print(f"  Fallback config: {ep.ai_gateway.fallback_config if hasattr(ep.ai_gateway, 'fallback_config') else 'N/A'}")
else:
    print("  Sin AI Gateway explícito (modelo gestionado por Databricks)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Ver usage real en System Tables (5 min)
# MAGIC
# MAGIC Cada invocación al modelo queda en `system.serving.endpoint_usage`. **Las llamadas que acabamos de hacer ya están ahí.**

# COMMAND ----------

# Endpoint usage de los últimos 7 días
display(spark.sql("""
SELECT
  date(u.request_time) AS dia,
  e.endpoint_name,
  e.served_entity_name,
  COUNT(*) AS requests,
  SUM(u.input_token_count + u.output_token_count) AS total_tokens
FROM system.serving.endpoint_usage u
JOIN system.serving.served_entities e
  ON u.served_entity_id = e.served_entity_id
WHERE u.request_time >= current_date() - INTERVAL 7 DAYS
  AND e.endpoint_name LIKE 'databricks-%'
GROUP BY dia, e.endpoint_name, e.served_entity_name
ORDER BY dia DESC, requests DESC
LIMIT 15
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen del LAB
# MAGIC
# MAGIC ✅ Usaron el Playground para comparar 2 modelos
# MAGIC ✅ Vieron cómo "View code" cierra el gap UI → producción
# MAGIC ✅ Llamaron 2 modelos diferentes con la misma API desde notebook
# MAGIC ✅ Inspeccionaron la config de Gateway de un endpoint
# MAGIC ✅ Vieron que el consumo ya quedó registrado en System Tables
# MAGIC
# MAGIC Lo que **no** hicieron (porque es del workshop deep-dive):
# MAGIC
# MAGIC - Crear un endpoint custom con Gateway desde cero
# MAGIC - Configurar guardrails de PII y safety
# MAGIC - Setup de traffic routing con fallback Llama → Claude
# MAGIC - Construir un agente con Mosaic AI Agent Framework
# MAGIC - Conectar Vector Search a un agente
# MAGIC - Deploy a Databricks App con Gateway integrado
# MAGIC
# MAGIC ## Continuar con `03 - Cierre y Workshop Preview`
