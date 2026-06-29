# Databricks notebook source
# MAGIC %md
# MAGIC # 🎬 Sesión 1 · 07 — Cierre
# MAGIC
# MAGIC ¡Felicitaciones! En 3 horas construyeron un **agente de IA servido y gobernado**, de punta a punta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧱 Lo que construimos hoy
# MAGIC
# MAGIC ```
# MAGIC                  ┌──────────────────────────────────────┐
# MAGIC                  │   Agente servido (Model Serving)      │   ✅ módulo 05
# MAGIC                  │        + AI Gateway                   │   ✅ módulo 06 (límites · PII · tracking)
# MAGIC                  └───────┬───────────────┬──────────────┘
# MAGIC                          │               │
# MAGIC                ┌─────────▼───┐   ┌───────▼────────────┐
# MAGIC                │ Vector Search│   │   Lakebase (OLTP)  │   ✅ módulos 02 / 03
# MAGIC                │  (RAG KB)    │   │ afiliados·programas│
# MAGIC                │             │   │ reservas·cupos     │
# MAGIC                └─────────────┘   └────────────────────┘
# MAGIC                          ▲               ▲
# MAGIC                          └──── 4 tools ──┘   ✅ módulo 04 (RAG + 3 tools Lakebase)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔁 Scorecard: framework Comfama → Databricks (Sesión 1)
# MAGIC
# MAGIC | Pieza Comfama | Databricks | Estado |
# MAGIC |---|---|---|
# MAGIC | `TemplateAgentes` | Mosaic AI Agent Framework | ✅ módulo 04 |
# MAGIC | **Cosmos DB** | **Lakebase** | ✅ módulo 03 |
# MAGIC | `LLMConfig + TokenProvider` | **AI Gateway + FM APIs** | ✅ módulo 06 |
# MAGIC | Retrieval/embeddings custom | Vector Search | ✅ módulo 02 |
# MAGIC
# MAGIC **Lo que quedó andando:** un endpoint REST del agente con scale-to-zero, transacciones reales de reserva en
# MAGIC Lakebase, RAG sobre la base de conocimiento, y gobierno con guardrails + tracking.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Qué revisar en la UI (lo que construiste hoy)
# MAGIC Recorre estos lugares para *ver* el resultado, no solo el código:
# MAGIC
# MAGIC | Qué | Dónde mirar |
# MAGIC |---|---|
# MAGIC | **Vector Search (RAG)** | *Compute → Vector Search* → endpoint `comfama_vs_endpoint`; el índice en *Catalog → tu schema `ws_<usuario>` → `kb_index`* (estado **Online**) |
# MAGIC | **Lakebase (OLTP)** | *Compute → Database instances → `comfama-afiliados`* → base `comfama`; revisa la tabla `reservas` y cómo baja `programas.cupos_disponibles` con cada reserva |
# MAGIC | **Agente registrado** | *Catalog → `ardemo_classic_dnubtw_catalog` → tu schema → Models → `agente_afiliados`*: versiones, firma y **recursos** declarados (FM, índice VS, **Lakebase**) |
# MAGIC | **Endpoint del agente** | *Serving → `agente_afiliados_<usuario>`*: estado **READY**, scale-to-zero, panel **Query**; + sus **inference tables** (registro de requests) |
# MAGIC
# MAGIC > 💡 Pruébalo: en *Serving → endpoint → **Query*** (o el Playground con el **endpoint del agente** seleccionado) pide
# MAGIC > *"reserva el programa X para el afiliado Y"* — verás aparecer la fila en `reservas` y el cupo bajar en `programas`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔮 Sesión 2 — Producción y Deploy
# MAGIC
# MAGIC | # | Módulo | Equivale a |
# MAGIC |---|---|---|
# MAGIC | 01 | **Databricks App** (frontend de chat, OBO) | Azure Container Apps |
# MAGIC | 02 | **Observabilidad** (MLflow Tracing) | `TelemetryManager` |
# MAGIC | 03 | **Gobernanza** (Unity Catalog, ABAC, lineage) | `security/` |
# MAGIC | 04 | **Monitoreo + Alertas** (Lakehouse Monitoring + SQL Alerts) | `AlertEvaluator` |
# MAGIC | 05 | **FinOps** (System Tables + usage del Gateway) | `FinOpsAnalyzer` |
# MAGIC | 06 | **Deploy-as-Code** (Asset Bundle · API · SDK) → su framework | — |
# MAGIC | 07 | Cierre y Recap | — |
# MAGIC
# MAGIC > En la Sesión 2 le ponemos **cara** al agente (App), lo **observamos/gobernamos/monitoreamos**, controlamos su
# MAGIC > **costo**, y mostramos cómo **desplegar todo como código** para integrarlo al framework de agentes de Comfama.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🧹 (Opcional) Pausar costos hasta la Sesión 2
# MAGIC El endpoint del agente ya tiene **scale-to-zero**, así que no consume sin tráfico. Lakebase (Autoscaling) también
# MAGIC escala a cero. No es necesario borrar nada: en la Sesión 2 retomamos exactamente desde aquí.
# MAGIC
# MAGIC ¡Gracias y nos vemos en la **Sesión 2**! 🚀

