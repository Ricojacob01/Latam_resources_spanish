# Databricks notebook source
# MAGIC %md
# MAGIC # 🛡️ Sesión 1 · 06 — AI Gateway
# MAGIC
# MAGIC **Meta:** poner **gobierno** sobre la **capa de modelo** (el LLM que usa el agente) con **AI Gateway**: límites de
# MAGIC tasa, **guardrails** (PII y seguridad) y **tracking de uso** — la telemetría que alimentará **FinOps** (Sesión 2).
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
# MAGIC - **Saliente — donde SÍ va AI Gateway**: gobierna el **modelo que el agente llama** (límites, guardrails PII,
# MAGIC   tracking, fallback, unificación de FMs/externos detrás de un endpoint). *Esto reemplaza a `LLMConfig + TokenProvider`.*
# MAGIC - **Entrante (usuario → agente)**: AI Gateway **no** aplica al endpoint de agente custom; ese control va por
# MAGIC   **permisos del endpoint / la App (OBO)**. El uso del agente lo registran las **inference tables** de `agents.deploy`.

# COMMAND ----------

# MAGIC %pip install -U databricks-sdk mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — pestaña *AI Gateway* del endpoint de **modelo**
# MAGIC > Aplica AI Gateway sobre el **endpoint del LLM que usa el agente** (no sobre el endpoint del agente), e idealmente
# MAGIC > uno **que tú controlas** (Provisioned Throughput o External Model). Sobre el FM del **sistema compartido** puede
# MAGIC > estar restringido o **afectar a todos** los usuarios.
# MAGIC 1. **Serving** → abre tu **endpoint de modelo** → pestaña **AI Gateway** → **Edit**.
# MAGIC 2. **Rate limits**: agrega un límite, p.ej. **120 llamadas/min** (protege de picos/abuso).
# MAGIC 3. **Guardrails**:
# MAGIC    - **PII**: `BLOCK` (o `MASK`) en entrada y salida → evita filtrar datos del afiliado.
# MAGIC    - **Safety**: activa el filtro de contenido inseguro.
# MAGIC    - *(Opcional)* **Invalid keywords / topics**: restringe a temas de servicios Comfama.
# MAGIC 4. **Update**.
# MAGIC
# MAGIC > 💡 El **registro de uso** del agente ya lo crea `agents.deploy` (inference tables, módulo 05). El *usage tracking*
# MAGIC > de AI Gateway aplica aquí, en la **capa de modelo**.

# COMMAND ----------

print(f"Endpoint de modelo a gobernar (LLM del agente): {LLM_ENDPOINT}")

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

# AI Gateway va sobre la CAPA DE MODELO: el endpoint del LLM que usa el agente, NO el del agente.
GATEWAY_TARGET = LLM_ENDPOINT

# ⚠️ Ejecuta SOLO si GATEWAY_TARGET es un endpoint que TÚ controlas (Provisioned Throughput / External Model).
# Sobre el FM del sistema COMPARTIDO no lo apliques: tu config (límites/guardrails) afectaría a TODOS los usuarios.
APLICAR = False   # pon True cuando GATEWAY_TARGET apunte a tu propio endpoint de modelo

if APLICAR:
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
        print("ℹ️ No se pudo configurar AI Gateway sobre", GATEWAY_TARGET, ":", str(e)[:160])
        print("   → Usa un endpoint de modelo propio (Provisioned Throughput / External Model) — sección 'saliente'.")
else:
    print("⏭️  Demo segura: NO se aplicó AI Gateway sobre", GATEWAY_TARGET,
          "(es el FM del sistema compartido).")
    print("   Apunta GATEWAY_TARGET a tu propio endpoint de modelo y pon APLICAR=True para configurarlo.")
    print("   El uso del AGENTE ya lo registran las inference tables de agents.deploy (módulo 05).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧪 Probar los guardrails
# MAGIC Llamamos al **agente** (que internamente invoca el LLM gobernado). **Si** el guardrail de PII está activo en la
# MAGIC capa de modelo, la PII se bloquea/enmascara en esa llamada; si el modelo no tiene Gateway (como en esta demo
# MAGIC con `APLICAR=False`), el agente responde normal.

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
# MAGIC Gobierno en la **capa de modelo** (límites, guardrails PII, usage tracking) sobre el endpoint del LLM que usa el
# MAGIC agente — el patrón que reemplaza a `LLMConfig + TokenProvider`. El **registro de uso del agente** lo dan las
# MAGIC inference tables de `agents.deploy`; ese uso + el del Gateway alimentan **FinOps** (Sesión 2 · 05).
# MAGIC
# MAGIC ### ▶️ Siguiente: `07 - Cierre Sesión 1`

