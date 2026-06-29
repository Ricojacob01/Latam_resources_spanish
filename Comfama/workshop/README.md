<img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png">

# Comfama — Workshop Agentes en Producción 🤖🚀

Workshop **hands-on en español** que construye un **agente de IA listo para producción** end-to-end sobre la Databricks Data Intelligence Platform — y, a medida que avanza, muestra el **equivalente managed de Databricks** para cada pieza del framework custom de IA de Comfama (hoy en Azure).

Es el **deep-dive unificado** que sigue a las Sesiones Express (Agentes & AI · Apps · Lakebase).

> **Formato: 2 sesiones de 3 horas.**
> - **Sesión 1 — Construir y Servir el Agente:** del dato al agente servido y gobernado.
> - **Sesión 2 — Producción y Deploy:** frontend (App), capacidades transversales y despliegue-como-código.

---

## 🖱️ Enfoque didáctico: UI primero, código como alternativa ejecutable

Cada módulo está pensado para dos públicos a la vez:

1. **UI primero** — todo lo que se puede hacer desde la interfaz trae **instrucciones paso a paso** (qué botón, qué pantalla). El asistente puede completar el módulo **solo con la UI**.
2. **Celda ejecutable equivalente** — junto a los pasos de UI hay una **celda de código** que hace exactamente lo mismo. El asistente puede **ejecutar el notebook** en lugar de hacer clic.

> Así cada quien elige su camino: *seguir la UI* o *correr la celda*. Ambos dejan el mismo asset desplegado.

**El código de automatización (Asset Bundle · API · SDK) se concentra al final**, en el módulo de cierre de la Sesión 2: ahí se muestra **cómo encajar todo en el framework de agentes de Comfama** (CI/CD) — el ejemplo de "esto también se puede desplegar como código".

---

## 🎯 Caso de uso: Agente de Servicios al Afiliado Comfama

Un asistente de autoservicio para afiliados que **no solo responde — transacciona**. Esa diferencia es la que pone a **Lakebase en el centro**: un demo solo-analítico no necesitaría OLTP; un agente que **reserva cupos** sí.

El afiliado conversa con el agente para:
- **Preguntar** por beneficios y programas (recreación, educación, salud, subsidios) → **RAG** sobre la base de conocimiento.
- **Consultar** sus inscripciones y la disponibilidad de un programa → lectura OLTP en Lakebase.
- **Reservar** un cupo → **escritura transaccional** en Lakebase (verifica cupo → inserta reserva → descuenta capacidad, atómico).

---

## 🧱 Arquitectura (lo que se construye en las 2 sesiones)

```
                 ┌──────────────────────────────────────────────┐
                 │            Databricks App (chat UI)            │  ← equivale a Azure Container Apps  [S2]
                 │                  OBO auth                      │
                 └───────────────┬───────────────┬──────────────┘
                                 │               │
                 ┌───────────────▼──────┐   ┌────▼─────────────────────┐
                 │   Agente servido      │   │      Lakebase (OLTP)     │  ← equivale a Cosmos DB     [S1]
                 │ (Model Serving) [S1]  │   │  afiliados · programas   │
                 │   + AI Gateway  [S1]  │   │  reservas · cupos        │
                 │  guardrails · límites │   │  conversaciones/mensajes │
                 └───────┬───────┬──────┘   └────┬─────────────────────┘
                         │       │               │ sync ⇅
              ┌──────────▼─┐  ┌──▼───────────┐   │
              │  FM (LLM)  │  │ Vector Search│   │
              │ vía Gateway│  │  (RAG KB)    │   │
              └────────────┘  └──────┬───────┘   │
                                     │           │
                 ┌───────────────────▼───────────▼──────────────┐
                 │          Unity Catalog (Delta · gobierno)     │  ← equivale a security/         [S2]
                 │   KB · tablas analíticas · lineage · audit    │
                 └───────────────────────────────────────────────┘
       Transversales [S2]:  MLflow Tracing · Lakehouse Monitoring · SQL Alerts · System Tables/Budget
       Cierre [S2]:  Deploy-as-Code (Asset Bundle · API · SDK) → framework de agentes de Comfama
```

---

## 📚 Sesión 1 — Construir y Servir el Agente (~3h)

| # | Módulo | Qué haces | Modo | Equivale a |
|---|---|---|---|---|
| 00 | **Bienvenida y Agenda** | Objetivos de las 2 sesiones, pre-check | — | — |
| 01 | **Product Tour (Agente end-to-end)** | El agente completo y dónde encaja cada producto | Conceptual | (mapa general) |
| 02 | **Setup & Knowledge Base** | Catálogo/schema, volumes, docs → índice Vector Search | 🖱️ UI + celda | — |
| 03 | **Lakebase (datos del afiliado)** | Proyecto/branch, modelo OLTP, `crear_reserva` transaccional | 🖱️ UI + celda | **Cosmos DB** |
| 04 | **Construir el Agente** | Retriever RAG + 3 tools (`consultar_beneficios`, `consultar_disponibilidad`, `crear_reserva`), registro en UC | 🖱️ UI + celda | `TemplateAgentes` |
| 05 | **Servir el Agente** | Endpoint de Model Serving del agente | 🖱️ UI + celda | — |
| 06 | **AI Gateway** | Rate limits, guardrails (PII/seguridad), usage tracking; FM unificado | 🖱️ UI + celda | `LLMConfig + TokenProvider` |
| 07 | **Cierre Sesión 1** | Recap + preview de la Sesión 2 | — | — |

## 📚 Sesión 2 — Producción y Deploy (~3h)

| # | Módulo | Qué haces | Modo | Equivale a |
|---|---|---|---|---|
| 00 | **Bienvenida y Recap** | Repaso de lo construido en la Sesión 1 | — | — |
| 01 | **Databricks App** | Frontend de chat conectado al agente + Lakebase, OBO auth | 🖱️ UI + celda | Azure Container Apps |
| 02 | **Observabilidad (MLflow Tracing)** | Trazas del agente end-to-end | 🖱️ UI + celda | `TelemetryManager` |
| 03 | **Gobernanza (Unity Catalog)** | Lineage, ABAC sobre datos del afiliado, audit | 🖱️ UI + celda | `security/` |
| 04 | **Monitoreo + Alertas** | Lakehouse Monitoring sobre `reservas` + SQL Alert de capacidad | 🖱️ UI + celda | `AlertEvaluator` |
| 05 | **FinOps** | System Tables + usage del Gateway + Budget API | 🖱️ UI + celda | `FinOpsAnalyzer` |
| 06 | **Deploy-as-Code para su framework** ⭐ | El mismo despliegue como **Asset Bundle**, **API** y **SDK** — cómo encajarlo en el framework de agentes de Comfama (CI/CD) | ⌨️ Código (ejemplo) | — |
| 07 | **Cierre y Recap** | Arquitectura final, qué sigue | — | — |

⭐ = único módulo donde el código/automatización es el protagonista.

---

## 🗂️ Estructura de carpetas

```
workshop/
├── README.md                              ← estás aquí
├── _resources/00-setup                    ← setup compartido (%run): schema, KB, datos — usado por ambas sesiones
├── Sesion 1 - Construir y Servir el Agente/
│   ├── 00 - Bienvenida y Agenda
│   ├── 01 - Product Tour (Agente end-to-end)
│   ├── 02 - Setup & Knowledge Base
│   ├── 03 - Lakebase (datos del afiliado)
│   ├── 04 - Construir el Agente
│   ├── 05 - Servir el Agente
│   ├── 06 - AI Gateway
│   ├── 07 - Cierre Sesion 1
│   └── imagenes/
├── Sesion 2 - Produccion y Deploy/
│   ├── 00 - Bienvenida y Recap
│   ├── 01 - Databricks App
│   ├── 02 - Observabilidad (MLflow Tracing)
│   ├── 03 - Gobernanza (Unity Catalog)
│   ├── 04 - Monitoreo + Alertas
│   ├── 05 - FinOps
│   ├── 06 - Deploy-as-Code para su framework
│   ├── 07 - Cierre y Recap
│   └── imagenes/
├── pipeline/                              ← notebooks-tarea encadenados por el Job (módulo Deploy-as-Code)
├── app_source/                           ← la Databricks App (app.py / app.yaml / requirements.txt)
└── bundle/                               ← databricks.yml (Asset Bundle) + job.json  (módulo Deploy-as-Code)
```

---

## 🔌 Cómo se usa Lakebase (detalle)

| Rol | Tablas | Capacidad Lakebase |
|---|---|---|
| Sistema de registro operacional (OLTP) | `afiliados`, `programas` (`cupos`), `reservas`, `beneficios_afiliado`, `casos` | lecturas/escrituras transaccionales de baja latencia |
| Memoria conversacional / estado de sesión | `conversaciones`, `mensajes` | lectura de historial + escritura por turno (sub-10ms) |
| Integración lakehouse | `programas` (Delta→Lakebase synced) · `reservas`/`mensajes` (Lakebase→UC) | synced tables (reverse-ETL) + database catalog en UC |
| Dev/test + aislamiento del workshop | — | **branching** por asistente (copy-on-write) + **scale-to-zero** |

> Tier **Autoscaling** (`databricks postgres`): jerarquía Project → Branch → Endpoint. Cada asistente crea **su propio branch** desde `production` para aislar su trabajo y, de paso, ver la feature en vivo. En la UI: *Compute → Database instances*.

---

## ✅ Prerrequisitos

- **Serverless** o cluster DBR 15.4+ con Unity Catalog.
- Catálogo `ardemo_classic_dnubtw_catalog`, schema personal `ws_<usuario>` (se crea solo en el setup).
- Proyecto Lakebase (Autoscaling) — creado en la Sesión 1; cada asistente usa su branch.
- Permiso para crear endpoints de Model Serving, Vector Search, Apps y Jobs.
- El setup instala automáticamente las librerías necesarias vía `%pip install`.

---

## 🆕 Diferencias vs las Sesiones Express

1. Pasa de **tour surface-level** a **workshop hands-on** end-to-end (todos construyen el agente real).
2. Un **solo caso de uso** (Agente de Afiliados) hila Agentes + AI Gateway + Lakebase + Apps + las capacidades transversales — repartido en **2 sesiones de 3h**.
3. **UI primero**: cada módulo se puede completar solo con la interfaz; el código es alternativa ejecutable.
4. **Lakebase load-bearing**: transacciones reales (`crear_reserva`), no solo concepto.
5. **Deploy-as-code al final** como ejemplo de integración con el **framework de agentes de Comfama** (Asset Bundle · API · SDK).
