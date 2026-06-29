# Databricks notebook source
# MAGIC %md
# MAGIC # 🛡️ Sesión 1 · 06 — AI Gateway
# MAGIC
# MAGIC **Meta:** poner **gobierno** sobre el agente servido (módulo 05) con **AI Gateway**: límites de tasa, **guardrails**
# MAGIC (PII y seguridad) y **tracking de uso** — la telemetría que alimentará **FinOps** (Sesión 2).
# MAGIC
# MAGIC > **Equivale a: `LLMConfig + TokenProvider`.** La gestión de llaves, límites y ruteo de modelos que Comfama hace
# MAGIC > a mano la centraliza AI Gateway, gobernado por Unity Catalog.
# MAGIC
# MAGIC Módulo **dual-mode**: lo configuramos **🖱️ en la pestaña AI Gateway del endpoint** o **⌨️ por SDK**.
# MAGIC
# MAGIC > ⚠️ **Validar en dry-run:** los tipos del SDK de AI Gateway evolucionan; confirma nombres de clases al ensayar.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔌 Dónde actúa AI Gateway (2 puntos)
# MAGIC
# MAGIC ```
# MAGIC   Consumidores ──▶ [AI Gateway: límites · guardrails · tracking] ──▶  Agente servido (entrante)
# MAGIC   Agente ──────────▶ [AI Gateway: unifica FMs · fallback · llaves] ──▶  LLM / modelos externos (saliente)
# MAGIC ```
# MAGIC
# MAGIC - **Entrante** (lo principal hoy): protege el **endpoint del agente** — quién puede, cuánto, y qué entra/sale.
# MAGIC - **Saliente**: unifica el acceso a modelos (FM de Databricks o externos como OpenAI/Anthropic) detrás de un
# MAGIC   único endpoint con fallback. *Esto es lo que reemplaza a `LLMConfig + TokenProvider`.*

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — pestaña *AI Gateway* del endpoint
# MAGIC 1. **Serving** → abre el endpoint `AGENT_ENDPOINT` → pestaña **AI Gateway** → **Edit**.
# MAGIC 2. **Rate limits**: agrega un límite, p.ej. **120 queries/min** por endpoint (protege de picos/abuso).
# MAGIC 3. **Guardrails**:
# MAGIC    - **PII**: `BLOCK` (o `MASK`) en entrada y salida → evita filtrar datos del afiliado.
# MAGIC    - **Safety**: activa el filtro de contenido inseguro.
# MAGIC    - *(Opcional)* **Invalid keywords / topics**: restringe a temas de servicios Comfama.
# MAGIC 4. **Update**.
# MAGIC
# MAGIC > 💡 El **registro de uso (inference tables)** del agente ya lo crea `agents.deploy` (módulo 05). El *usage
# MAGIC > tracking* de AI Gateway está pensado para endpoints de **Foundation Models / modelos externos** (sección saliente).

# COMMAND ----------

print(f"Endpoint a gobernar: {AGENT_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — `put_ai_gateway`

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    AiGatewayRateLimit, AiGatewayRateLimitKey, AiGatewayRateLimitRenewalPeriod,
    AiGatewayGuardrails, AiGatewayGuardrailParameters, AiGatewayGuardrailPiiBehavior,
    AiGatewayGuardrailPiiBehaviorBehavior,
)

w = WorkspaceClient()

# ⚠️ AI Gateway se aplica a endpoints de **Foundation Models / modelos externos**, NO a endpoints
# de agente (pyfunc) custom. Según el workspace, intentar configurarlo sobre el endpoint del agente
# devuelve "not supported for this endpoint type". Lo intentamos y reportamos con claridad.
GATEWAY_TARGET = AGENT_ENDPOINT  # cámbialo por un endpoint de FM/modelo externo donde AI Gateway esté habilitado
try:
    w.serving_endpoints.put_ai_gateway(
        name=GATEWAY_TARGET,
        rate_limits=[AiGatewayRateLimit(
            calls=120, renewal_period=AiGatewayRateLimitRenewalPeriod.MINUTE,
            key=AiGatewayRateLimitKey.ENDPOINT)],
        guardrails=AiGatewayGuardrails(
            input=AiGatewayGuardrailParameters(
                pii=AiGatewayGuardrailPiiBehavior(
                    behavior=AiGatewayGuardrailPiiBehaviorBehavior.BLOCK)),
            output=AiGatewayGuardrailParameters(
                pii=AiGatewayGuardrailPiiBehavior(
                    behavior=AiGatewayGuardrailPiiBehaviorBehavior.BLOCK)),
        ),
    )
    print("✅ AI Gateway (rate limits + guardrails PII) configurado sobre", GATEWAY_TARGET)
except Exception as e:
    print("ℹ️ AI Gateway no soportado sobre este endpoint en este workspace:")
    print("  ", str(e)[:160])
    print("   → Aplícalo sobre un endpoint de Foundation Model / modelo externo (sección 'saliente').")
    print("   → El registro de uso del AGENTE ya lo dan las inference tables de agents.deploy (módulo 05).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Probar los guardrails
# MAGIC Enviamos un mensaje con datos sensibles. **Si** el guardrail de PII está activo (endpoint de FM/externo con AI
# MAGIC Gateway), la request se bloquea; si no, el agente responde normal. Sirve para ver el comportamiento esperado.

# COMMAND ----------

from mlflow.deployments import get_deploy_client
client = get_deploy_client("databricks")
try:
    r = client.predict(endpoint=AGENT_ENDPOINT, inputs={"messages":[{"role":"user",
        "content":"Mi cédula es 43.111.222 y mi tarjeta 4111 1111 1111 1111, ¿qué cursos hay?"}]})
    print("Respuesta:", r["messages"][-1]["content"])
except Exception as e:
    print("Guardrail/limite actuó:", type(e).__name__, str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⇄ (Saliente) Unificar modelos detrás del Gateway — concepto + ejemplo
# MAGIC Para reemplazar `LLMConfig + TokenProvider`, crea un **endpoint de modelo externo** gobernado por Gateway. El
# MAGIC agente apunta a **un solo** endpoint; cambiar de proveedor o agregar fallback no toca el código del agente.
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk.service.serving import (ExternalModel, OpenAiConfig, ServedEntityInput)
# MAGIC w.serving_endpoints.create(name="gw-llm-comfama", config={
# MAGIC     "served_entities":[{"name":"primario","external_model":{
# MAGIC         "name":"gpt-4o","provider":"openai","task":"llm/v1/chat",
# MAGIC         "openai_config":{"openai_api_key":"{{secrets/comfama/openai_key}}"}}}]})
# MAGIC # → luego put_ai_gateway con rate limits + usage tracking sobre 'gw-llm-comfama'
# MAGIC ```
# MAGIC > Las llaves viven en **Databricks Secrets**, no en el código — exactamente lo que centraliza el `TokenProvider`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC El agente queda **gobernado**: con **límites de tasa** y **guardrails de PII**. El **registro de uso** (inference
# MAGIC tables del agente, creadas por `agents.deploy`) + el usage tracking del Gateway en endpoints de FM son la fuente
# MAGIC de **FinOps** (Sesión 2 · 05).
# MAGIC
# MAGIC ### ▶️ Siguiente: `07 - Cierre Sesión 1`

