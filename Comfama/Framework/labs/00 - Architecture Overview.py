# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Architecture Overview
# MAGIC
# MAGIC **Demo Comfama × Databricks**
# MAGIC
# MAGIC Este notebook explica la arquitectura del demo y cómo cada componente Databricks reemplaza una pieza del framework de IA que Comfama mantiene hoy a mano.

# COMMAND ----------

# MAGIC %md
# MAGIC ## El framework actual de Comfama
# MAGIC
# MAGIC ```
# MAGIC comfama-ai-core/
# MAGIC ├── config/         ← LLM, Embedding, VectorStore, OCR, Agent, MCP, A2A
# MAGIC ├── security/       ← AuthManager (JWT + Entra ID), SecretProvider
# MAGIC ├── observability/  ← TelemetryManager, FinOpsAnalyzer, AlertEvaluator
# MAGIC ├── storage/        ← CosmosDBClient, SQLClient, FabricClient, BlobClient
# MAGIC ├── devops/         ← WorkflowContext / StepContext sobre dbutils.taskValues
# MAGIC └── utils/          ← errores, async helpers
# MAGIC
# MAGIC comfama-ai-cognitive/  ← Capacidades de IA: agentes, RAG, evaluación
# MAGIC
# MAGIC Templates:
# MAGIC ├── Framework.Comfama.IA.TemplateAgentes   ← FastAPI + LangGraph + Azure OpenAI
# MAGIC ├── Framework.Comfama.IA.TemplateMCP       ← FastMCP + observability
# MAGIC └── Framework.Comfama.IA.TemplateWorkflow  ← Bronze/Silver/Gold + DABs
# MAGIC ```
# MAGIC
# MAGIC **Observación**: aproximadamente el 40 % del repo es código de **plataforma** (telemetría, FinOps, evals, alertas, auth) que Databricks entrega como producto.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Mapeo capa por capa
# MAGIC
# MAGIC | Capa Comfama (construido a mano) | Reemplazo Databricks | Notebook que lo demuestra |
# MAGIC |---|---|---|
# MAGIC | `observability/TelemetryManager` + OTLP + Prometheus + `@instrument` | **MLflow 3 autologging + GenAI tracing** | `02 - Observability (MLflow Tracing)` |
# MAGIC | `security/AuthManager` + UC custom | **Unity Catalog** (gobierno + lineage + audit) | `03 - Governance (Unity Catalog)` |
# MAGIC | Monitoreo de calidad / drift (custom) | **Lakehouse Monitoring** | `04 - Monitoring (Lakehouse Monitoring)` |
# MAGIC | `observability/AlertEvaluator` + `AlertProtocols` | **Databricks SQL Alerts** | `05 - SQL Alerts` |
# MAGIC | `observability/FinOpsAnalyzer` (costos DBU) | **System Tables** (`system.billing.usage`) + Budget Policies | `06 - FinOps (System Tables)` |
# MAGIC | `TemplateAgentes` (FastAPI + LangGraph) | **Mosaic AI Agent Framework** + Agent Bricks | (referencias en este notebook) |
# MAGIC | `TemplateMCP` (FastMCP) | **Managed MCP Servers** | (referencias en este notebook) |
# MAGIC | `LLMConfig` + `TokenProvider` OAuth2 | **AI Gateway** + Foundation Model APIs | (referencias en este notebook) |
# MAGIC | `VectorStoreConfig` → Azure Search | **Mosaic AI Vector Search** | (referencias en este notebook) |
# MAGIC | Cosmos DB para estado del agente | **Lakebase** (Postgres OLTP managed) | (referencias en este notebook) |
# MAGIC | Container Apps (frontend + backend) | **Databricks Apps** | (referencias en este notebook) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema y assets de este demo
# MAGIC
# MAGIC Todos los recursos viven en:
# MAGIC
# MAGIC ```
# MAGIC ardemo_classic_dnubtw_catalog.comfama
# MAGIC ```
# MAGIC
# MAGIC | Tipo | Asset | Notebook que lo crea |
# MAGIC |---|---|---|
# MAGIC | Schema | `comfama` | `01 - Setup` (ya existe) |
# MAGIC | Volume | `archivos` | `01 - Setup` |
# MAGIC | Tabla Bronze | `eventos_agente_bronze` | `01 - Setup` |
# MAGIC | Tabla Silver | `eventos_agente_silver` | `01 - Setup` |
# MAGIC | Tabla Gold | `metricas_agente_gold` | `01 - Setup` |
# MAGIC | MLflow Experiment | `comfama_observability_demo` | `02 - Observability` |
# MAGIC | Inference Table | (auto-creada por Model Serving) | `02 - Observability` |
# MAGIC | Lakehouse Monitor | sobre `metricas_agente_gold` | `04 - Monitoring` |
# MAGIC | SQL Alert | latencia / errores | `05 - SQL Alerts` |
# MAGIC | Lakeview Dashboard | FinOps + Quality | `05 - SQL Alerts` + `06 - FinOps` |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Productos referenciados (no demostrados en notebooks)
# MAGIC
# MAGIC Los siguientes productos completan la arquitectura DESPUÉS del presentation pero requieren setup más grande que un solo notebook. Se documentan aquí como referencia:
# MAGIC
# MAGIC ### Mosaic AI Agent Framework + Agent Bricks
# MAGIC
# MAGIC Reemplaza `TemplateAgentes` (FastAPI + LangGraph + Azure OpenAI).
# MAGIC
# MAGIC ```yaml
# MAGIC # bricks.yaml (equivalente a su TemplateAgentes)
# MAGIC name: agente_comfama
# MAGIC description: Agente conversacional para Comfama
# MAGIC tools:
# MAGIC   - type: managed_mcp
# MAGIC     server: business_systems
# MAGIC   - type: vector_search
# MAGIC     index: comfama.documentos_index
# MAGIC model: databricks-meta-llama-3-3-70b-instruct
# MAGIC ```
# MAGIC
# MAGIC Docs: <https://docs.databricks.com/en/generative-ai/agent-framework/index.html>
# MAGIC
# MAGIC ### Managed MCP Servers
# MAGIC
# MAGIC Reemplaza `TemplateMCP` (FastMCP custom).
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk import WorkspaceClient
# MAGIC w = WorkspaceClient()
# MAGIC # Listar managed MCP servers
# MAGIC for mcp in w.mcp_servers.list():
# MAGIC     print(mcp.name, mcp.url)
# MAGIC ```
# MAGIC
# MAGIC ### AI Gateway + Foundation Model APIs
# MAGIC
# MAGIC Reemplaza `LLMConfig` + `EmbeddingConfig` + `TokenProvider` OAuth2.
# MAGIC
# MAGIC ```python
# MAGIC from mlflow.deployments import get_deploy_client
# MAGIC client = get_deploy_client("databricks")
# MAGIC # Un endpoint con guardrails + rate limit + payload logging
# MAGIC response = client.predict(
# MAGIC     endpoint="databricks-meta-llama-3-3-70b-instruct",
# MAGIC     inputs={"messages": [{"role": "user", "content": "Hola"}]},
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Mosaic AI Vector Search
# MAGIC
# MAGIC Reemplaza `VectorStoreConfig` → Azure Search.
# MAGIC
# MAGIC ```python
# MAGIC from databricks.vector_search.client import VectorSearchClient
# MAGIC vsc = VectorSearchClient()
# MAGIC index = vsc.create_delta_sync_index(
# MAGIC     endpoint_name="comfama_vs_endpoint",
# MAGIC     index_name="ardemo_classic_dnubtw_catalog.comfama.documentos_index",
# MAGIC     source_table_name="ardemo_classic_dnubtw_catalog.comfama.documentos",
# MAGIC     pipeline_type="TRIGGERED",  # o "CONTINUOUS"
# MAGIC     primary_key="id",
# MAGIC     embedding_source_column="texto",
# MAGIC     embedding_model_endpoint_name="databricks-gte-large-en",
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Lakebase (Postgres OLTP managed)
# MAGIC
# MAGIC Reemplaza Cosmos DB para estado conversacional del agente.
# MAGIC
# MAGIC ```bash
# MAGIC databricks database instances create \\
# MAGIC   --json '{"name":"comfama_lakebase","capacity":"CU_1"}'
# MAGIC ```
# MAGIC
# MAGIC ```python
# MAGIC import psycopg  # SDK Postgres estándar — sin clientes custom
# MAGIC conn = psycopg.connect("postgresql://<host>/comfama_db")
# MAGIC ```
# MAGIC
# MAGIC ### Databricks Apps
# MAGIC
# MAGIC Reemplaza Container Apps (frontend + agent backend). Auth Entra ID SSO incluido.
# MAGIC
# MAGIC ```yaml
# MAGIC # app.yaml
# MAGIC command: ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
# MAGIC env:
# MAGIC   - name: DATABRICKS_WAREHOUSE_ID
# MAGIC     valueFrom: warehouse-id-secret
# MAGIC resources:
# MAGIC   - name: warehouse-id-secret
# MAGIC     description: SQL warehouse for the app
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagrama de arquitectura DESPUÉS (referencia visual)
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │              Comfama Subscription — Databricks Workspace            │
# MAGIC │                                                                     │
# MAGIC │  ┌────────────────┐    ┌──────────────────┐    ┌─────────────────┐  │
# MAGIC │  │ Databricks     │───▶│ Mosaic AI Agent  │───▶│ Managed MCP     │  │
# MAGIC │  │ Apps           │    │ Framework        │    │ Servers         │  │
# MAGIC │  │ (FE + backend) │    │ (Agent Bricks)   │    │ (tools)         │  │
# MAGIC │  └────────────────┘    └────────┬─────────┘    └─────────────────┘  │
# MAGIC │           │                     │                                   │
# MAGIC │           ▼                     ▼                                   │
# MAGIC │  ┌────────────────┐    ┌──────────────────┐    ┌─────────────────┐  │
# MAGIC │  │ Lakebase       │    │ AI Gateway       │───▶│ Model Serving + │  │
# MAGIC │  │ (estado + hist)│    │ (guardrails+PII) │    │ Foundation APIs │  │
# MAGIC │  └────────────────┘    └──────────────────┘    └─────────────────┘  │
# MAGIC │           │                     │                                   │
# MAGIC │           │                     ▼                                   │
# MAGIC │           │             ┌──────────────────┐                        │
# MAGIC │           │             │ MLflow 3 tracing │  ← este demo (02)      │
# MAGIC │           │             └──────────────────┘                        │
# MAGIC │           │                     │                                   │
# MAGIC │           ▼                     ▼                                   │
# MAGIC │  ┌────────────────────────────────────────────────────────────────┐ │
# MAGIC │  │ Unity Catalog (gobierno + lineage + audit) ← este demo (03)    │ │
# MAGIC │  └────────────────────────────────────────────────────────────────┘ │
# MAGIC │                              │                                      │
# MAGIC │                              ▼                                      │
# MAGIC │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐   │
# MAGIC │  │ Lakehouse        │  │ SQL Alerts       │  │ System Tables    │   │
# MAGIC │  │ Monitoring (04)  │  │ (05)             │  │ + Budget (06)    │   │
# MAGIC │  └──────────────────┘  └──────────────────┘  └──────────────────┘   │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Siguientes pasos
# MAGIC
# MAGIC 1. Ejecutar `01 - Setup & Sample Data` para crear la base de datos.
# MAGIC 2. Recorrer `02` a `06` en orden (cada uno toma 5-10 min).
# MAGIC 3. Al final tendrás:
# MAGIC    - Una arquitectura end-to-end funcionando
# MAGIC    - Tracing nativo de un agente
# MAGIC    - Audit + lineage automático
# MAGIC    - Monitor de calidad sobre las tablas Gold
# MAGIC    - SQL Alert que dispara si la latencia degrada
# MAGIC    - Queries de FinOps sobre System Tables
# MAGIC
# MAGIC ## Recursos
# MAGIC
# MAGIC - Documentación: <https://docs.databricks.com>
# MAGIC - MLflow GenAI: <https://docs.databricks.com/en/mlflow/genai-tracing.html>
# MAGIC - Lakehouse Monitoring: <https://docs.databricks.com/en/lakehouse-monitoring/index.html>
# MAGIC - System Tables: <https://docs.databricks.com/en/admin/system-tables/index.html>

