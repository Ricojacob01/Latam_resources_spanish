# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour 📊
# MAGIC
# MAGIC ~20 min. Recorrido visual por los productos. Slides extraídos de los decks oficiales internos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — helper para mostrar slides embebidos

# COMMAND ----------

import os, base64
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLIDES_PATH = f"/Workspace/Users/{CURRENT_USER}/Latam_resources_spanish/Comfama/Agentes & AI/imagenes"

def show_slide(filename, width=1100, caption=""):
    """Show a slide embedded inline (works without external URLs)."""
    full_path = f"{SLIDES_PATH}/{filename}"
    try:
        with open(full_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        html = f'<div style="margin:8px 0"><img src="data:image/png;base64,{b64}" style="max-width:{width}px;width:100%;border:1px solid #ddd;border-radius:6px"/>'
        if caption:
            html += f'<div style="font-size:13px;color:#666;font-style:italic;margin-top:6px">{caption}</div>'
        html += "</div>"
        displayHTML(html)
    except FileNotFoundError:
        displayHTML(f'<div style="padding:20px;background:#fee;border:1px solid #fcc">Slide no encontrado: {full_path}</div>')

print(f"Slides path: {SLIDES_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 — El problema que resuelve Databricks
# MAGIC
# MAGIC > _"Todo el mundo está construyendo agentes. Hay sprawl, fragmentación, y razonamiento de baja calidad sobre data empresarial."_

# COMMAND ----------

show_slide("14_everyone_building_agents.png",
           caption="El ecosistema actual: SaaS, coding tools, plataformas de agentes — proliferación de herramientas")

# COMMAND ----------

show_slide("15_agent_sprawl.png",
           caption="Agent sprawl: cada equipo construye sus propios agentes con su stack — sin governance común")

# COMMAND ----------

show_slide("16_low_quality_reasoning.png",
           caption="Razonamiento de baja calidad sin acceso a las semánticas de los datos empresariales")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 — Mosaic AI Gateway: capa de governance unificada
# MAGIC
# MAGIC AI Gateway resuelve dos cosas críticas que hoy Comfama mantiene a mano:
# MAGIC
# MAGIC 1. **Governance** — quién puede llamar qué modelo, con qué rate limit, con qué payload logging
# MAGIC 2. **Una sola API** para todos los providers (OpenAI, Anthropic, Llama, etc.)

# COMMAND ----------

show_slide("01_gateway_cover.png", caption="Mosaic AI Gateway — la capa de governance para todos los LLMs")

# COMMAND ----------

show_slide("02_gateway_governance.png",
           caption="Una sola capa de governance sobre Model Serving + Vector Search + Foundation Models")

# COMMAND ----------

show_slide("05_request_response_flow.png",
           caption="El flujo: Request → permissions → rate limits → input guardrails → modelo → output guardrails → response. TODO loggeado.")

# COMMAND ----------

show_slide("11_governance.png", caption="Controles de seguridad: quién accede y cuánto")

# COMMAND ----------

show_slide("12_guardrails.png",
           caption="AI Guardrails: previene data leakage y requests/responses unsafe (PII, profanity, prompt injection)")

# COMMAND ----------

show_slide("13_traffic_routing.png",
           caption="Traffic Routing: A/B test y fallback automático entre proveedores. Máximo 2 fallbacks por endpoint.")

# COMMAND ----------

show_slide("06_usage_tracking.png",
           caption="Usage tracking centralizado a través de todos los modelos GenAI — directo a System Tables")

# COMMAND ----------

show_slide("07_one_line_swap.png",
           caption="UNA LÍNEA de código para cambiar de proveedor o modelo. Y conexión nativa con Playground + Agent Evaluation.")

# COMMAND ----------

show_slide("08_benefits.png", caption="Los 3 beneficios principales: Governance · Unified Query · Production-ready routing")

# COMMAND ----------

show_slide("09_features.png", caption="Features completas de AI Gateway")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 — Agent Bricks: construir agentes declarativamente
# MAGIC
# MAGIC AI Gateway es la **capa de plumbing**. Agent Bricks es **cómo construyes y desplegas el agente** encima.

# COMMAND ----------

show_slide("17_agent_bricks_platform.png",
           caption="Agent Bricks: plataforma unificada para construir, desplegar y evaluar agentes")

# COMMAND ----------

show_slide("18_build_custom_agents.png",
           caption="Construir agentes custom con cualquier framework + servir todos los modelos frontier con un solo contrato")

# COMMAND ----------

show_slide("19_agents_models_data_arch.png",
           caption="Arquitectura completa: Agents + Models + Data + MCP Servers + Skills + External Agents — todo bajo access control y discovery")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4 — Playground: probar todo sin código
# MAGIC
# MAGIC El **AI Playground** es UI para experimentar con cualquier modelo + agente sin escribir nada. Es el on-ramp natural a todo lo que vimos arriba.

# COMMAND ----------

show_slide("10_unified_query.png",
           caption="Unified Query Interface: la misma API para todos los LLMs. El Playground es la UI de esto.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 5 — Roadmap (qué viene)

# COMMAND ----------

show_slide("20_roadmap.png", caption="Roadmap de Mosaic AI Gateway")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen visual del stack
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                     Tu aplicación / agente                       │
# MAGIC │                            ↓                                     │
# MAGIC │  ┌────────────────────────────────────────────────────────────┐ │
# MAGIC │  │              🚪 Mosaic AI Gateway                          │ │
# MAGIC │  │  permissions · rate limits · guardrails · payload logging  │ │
# MAGIC │  └────────────────────────────────────────────────────────────┘ │
# MAGIC │                            ↓                                     │
# MAGIC │  ┌─────────────┬──────────────┬──────────────┬────────────────┐ │
# MAGIC │  │ Llama 3.3   │ Claude 4.5   │ GPT-5        │ Tu modelo       │ │
# MAGIC │  │ (FM API)    │ (FM API)     │ (external)   │ (custom)        │ │
# MAGIC │  └─────────────┴──────────────┴──────────────┴────────────────┘ │
# MAGIC │                            ↓                                     │
# MAGIC │  ┌────────────────────────────────────────────────────────────┐ │
# MAGIC │  │ 📊 Inference Tables · system.billing.usage · Lakeview       │ │
# MAGIC │  │   (auto-capture de cada request + costos + dashboards)     │ │
# MAGIC │  └────────────────────────────────────────────────────────────┘ │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Listo para el hands-on? → `02 - LAB Express`
