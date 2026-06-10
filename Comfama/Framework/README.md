# Comfama Framework — Demo Databricks-native

Esta carpeta demuestra cómo Databricks puede reemplazar las capas commodity del framework de IA de Comfama (`comfama-ai-core`, `comfama-ai-cognitive`, templates `Workflow` / `MCP` / `Agentes`) con productos managed.

El foco de este demo está en las **4 capacidades transversales** que su framework implementa hoy a mano:

| Capacidad transversal Comfama | Reemplazo Databricks (lo que demostramos) |
|---|---|
| `observability/` (TelemetryManager + OTLP + Prometheus) | **MLflow 3 autologging + GenAI tracing** |
| `security/` (UC governance) | **Unity Catalog** (lineage, ABAC, audit) |
| Monitoreo continuo de datos / modelos | **Lakehouse Monitoring** |
| Sistema de alertas custom (`AlertEvaluator`) | **Databricks SQL Alerts** |
| `FinOpsAnalyzer` custom (costos DBU) | **System Tables + Budget API** |

## Estructura

```
Comfama_framework/
├── README.md
├── labs/
│   ├── 00 - Architecture Overview              ← Mapeo capa-por-capa, qué hace cada componente
│   ├── 01 - Setup & Sample Data                ← Crea schema, volumes, tablas Bronze/Silver/Gold
│   ├── 02 - Observability (MLflow Tracing)     ← Sustituye TelemetryManager
│   ├── 03 - Governance (Unity Catalog)         ← Sustituye security/ + audit
│   ├── 04 - Monitoring (Lakehouse Monitoring)  ← Profile + drift sobre tablas
│   ├── 05 - SQL Alerts                         ← Sustituye AlertEvaluator
│   └── 06 - FinOps (System Tables)             ← Sustituye FinOpsAnalyzer
└── datos/
    └── sample_data.json                        ← Datos de ejemplo
```

## Schema

Todos los assets viven en:

```
ardemo_classic_dnubtw_catalog.comfama
```

## Cómo usar este demo

1. **Clúster**: Serverless (recomendado) o un clúster DBR 15.4+ con Unity Catalog habilitado.
2. **Ejecutar en orden**:
   - `00 - Architecture Overview` — leer primero
   - `01 - Setup & Sample Data` — crea las tablas base
   - `02` → `06` — pueden correrse independientemente, pero el orden recomendado es secuencial
3. **Tiempo total**: ~30-45 min si se ejecutan todos los notebooks.

## Para el equipo de Comfama

Cada notebook incluye al inicio una caja **"Lo que reemplaza"** que mapea explícitamente el código de Python custom que están manteniendo hoy contra el producto Databricks que lo reemplaza. La intención es que el equipo vea **qué archivo del repo actual puede borrar** después de adoptar cada producto.

## Componentes adicionales (referencias)

El presentation completo cubre 9 productos Databricks. Este demo profundiza en 4, pero los otros 5 están documentados en el notebook `00 - Architecture Overview`:

- **Databricks Apps** — frontend + agent backend (reemplaza Container Apps)
- **Mosaic AI Agent Framework + Agent Bricks** — reemplaza `TemplateAgentes`
- **Managed MCP Servers** — reemplaza `TemplateMCP`
- **AI Gateway + Foundation Model APIs** — reemplaza `LLMConfig + TokenProvider`
- **Lakebase** — reemplaza Cosmos DB para estado de agente
- **Mosaic AI Vector Search** — reemplaza Azure Search

---

_Demo construido para conversaciones técnicas con el equipo de plataforma de Comfama._
