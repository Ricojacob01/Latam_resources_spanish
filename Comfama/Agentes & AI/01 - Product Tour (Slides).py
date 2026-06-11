# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour 📊
# MAGIC
# MAGIC ~20 min. Arco narrativo:
# MAGIC
# MAGIC > **El problema** → **Playground** (por dónde empiezas) → **Agent Bricks** (lo que construyes) → **AI Gateway** (cómo se gobierna) → **Beneficios + Roadmap**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup — helper para mostrar slides embebidos

# COMMAND ----------

import os, base64
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLIDES_PATH = f"/Workspace/Users/{CURRENT_USER}/Latam_resources_spanish/Comfama/Agentes & AI/imagenes"

def show_slide(filename, width=1100, caption=""):
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

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 1 — El problema
# MAGIC
# MAGIC > _"Todo el mundo está construyendo agentes. Pero hay fragmentación, governance gap, y razonamiento de baja calidad sobre datos empresariales."_

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
# MAGIC # 🎬 Acto 2 — Playground: por dónde empiezas 🛝
# MAGIC
# MAGIC La forma más rápida de probar todo: **una UI** para hablar con cualquier modelo + agente, sin escribir código. El **on-ramp** natural a la plataforma.

# COMMAND ----------

show_slide("10_unified_query.png",
           caption="Unified Query Interface: la misma API para TODOS los LLMs (Llama, Claude, GPT, custom). El Playground es la UI de esto.")

# COMMAND ----------

show_slide("07_one_line_swap.png",
           caption="UNA LÍNEA de código para cambiar de proveedor o modelo. El Playground conecta nativo con Agent Evaluation.")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 3 — Agent Bricks: lo que construyes 🧱
# MAGIC
# MAGIC Playground es para experimentar. **Agent Bricks** es para **producir** agentes declarativos sin pelear con frameworks.

# COMMAND ----------

show_slide("17_agent_bricks_platform.png",
           caption="Agent Bricks: plataforma unificada para construir, desplegar y evaluar agentes")

# COMMAND ----------

show_slide("18_build_custom_agents.png",
           caption="Construir agentes custom con cualquier framework + servir todos los modelos frontier con un solo contrato")

# COMMAND ----------

show_slide("19_agents_models_data_arch.png",
           caption="La arquitectura: Agents + Models + Data + MCP Servers + Skills + External Agents — todo bajo access control y discovery")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 4 — AI Gateway: cómo se gobierna 🚪
# MAGIC
# MAGIC Bricks resuelve el "qué construyes". **AI Gateway** resuelve el _"cómo lo gobiernas en producción"_ — permisos, rate limits, guardrails, observability.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.1 — Qué es

# COMMAND ----------

show_slide("01_gateway_cover.png", caption="Mosaic AI Gateway — la capa de governance para todos los LLMs")

# COMMAND ----------

show_slide("02_gateway_governance.png",
           caption="Una sola capa de governance sobre Model Serving + Vector Search + Foundation Models")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.2 — Cómo funciona

# COMMAND ----------

show_slide("05_request_response_flow.png",
           caption="El flujo: Request → permissions → rate limits → input guardrails → modelo → output guardrails → response. TODO loggeado.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.3 — Las 3 capacidades core

# COMMAND ----------

show_slide("11_governance.png", caption="① Governance — controles de seguridad sobre quién accede y cuánto")

# COMMAND ----------

show_slide("12_guardrails.png",
           caption="② Guardrails — previene data leakage y requests/responses unsafe (PII, profanity, prompt injection)")

# COMMAND ----------

show_slide("13_traffic_routing.png",
           caption="③ Traffic Routing — A/B test y fallback automático entre proveedores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.4 — Observability nativa

# COMMAND ----------

show_slide("06_usage_tracking.png",
           caption="Usage tracking centralizado a través de todos los modelos GenAI — directo a System Tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4.5 — Spend Controls (Beta) 💰
# MAGIC
# MAGIC No solo *trackear* el costo — también **ponerle límites**. Set AI budgets a nivel user, workspace u org. Detén el sangrado antes de la factura.

# COMMAND ----------

show_slide("21_spend_controls.png",
           caption="Spend Controls (Beta): set AI budgets a nivel user/workspace/org · stop surprise bills · track costs across every model and provider · govern AI spend alongside your data")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🎬 Acto 5 — Beneficios + roadmap 🎯

# COMMAND ----------

show_slide("08_benefits.png", caption="Los 3 beneficios principales: Governance · Unified Query · Production-ready routing")

# COMMAND ----------

show_slide("09_features.png", caption="Features completas de AI Gateway")

# COMMAND ----------

show_slide("20_roadmap.png", caption="Roadmap de Mosaic AI Gateway")

# COMMAND ----------

# MAGIC %md
# MAGIC # 🧩 Recap visual del stack
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
# MAGIC
# MAGIC **Arco que acabas de ver:**
# MAGIC
# MAGIC 1. Hay un problema → agentes everywhere sin control
# MAGIC 2. **Playground** = tu ON-RAMP (sin código, comparar modelos)
# MAGIC 3. **Agent Bricks** = lo que CONSTRUYES (declarativo, end-to-end)
# MAGIC 4. **AI Gateway** = cómo se GOBIERNA en producción
# MAGIC 5. Todo medido + auditado + facturado en System Tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## ¿Listo para el hands-on? → `02 - LAB Express`
