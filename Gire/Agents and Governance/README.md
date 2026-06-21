# Track 2 — Agents and Governance 🤖

Gobernanza con **Unity Catalog** (incluyendo clasificación y enmascaramiento **asistidos por IA**), **AI Functions** en SQL, **Genie + Apps** y **Agent Bricks** (Knowledge Assistant). El hilo conductor: los datos gobernados son la base sobre la que construyes agentes confiables.

**Fuentes que adapta:** `Data_governace` (clasificación/masking con IA), `Genie_App_workshop` y labs Gire previos (Genie + Streamlit App), `databricks-genai-lab` (AI Functions, batch inference, Agent Bricks).

## Módulos

| # | Módulo | Tiempo | Qué haces | Enfoque UI vs Code |
|---|---|---|---|---|
| 00 | **Bienvenida y Agenda** | 5 min | Objetivos, agenda, pre-check | — |
| 01 | **Product Tour (UC + Genie + Agent Bricks)** | 20 min | El "por qué": gobernanza → NLQ → agentes | Conceptual |
| 02 | **LAB Gobernanza con Unity Catalog** | 30 min | Comentarios con `ai_gen`, clasificación con `ai_query`, tags y *column masking* | **Secuencial (UI → Code)** |
| 03 | **LAB AI Functions (SQL)** | 25 min | `ai_query`, `ai_classify`, `ai_extract`, `ai_analyze_sentiment`, batch inference | **Lado a lado (Playground UI ↔ SQL)** |
| 04 | **LAB Genie y Apps** | 35 min | Crear un Genie space + una App Streamlit que lo consume | **Secuencial (UI → Code)** |
| 05 | **LAB Agent Bricks (Knowledge Assistant)** | 30 min | Agente RAG sobre un PDF, sin escribir el retriever | **Secuencial (Code → UI)** |
| 06 | **Cierre y Workshop Preview** | 10 min | Recap + qué sigue | — |

## Carpeta `labs/`

Contiene el contenido hands-on detallado que estos módulos enmarcan (reutilizado del repo):

- `labs/genie_y_apps/` — 01 Introducción, 02 Crear Genie, 03 App Streamlit.
- `labs/ai_functions/01_ai_functions_sql.sql` — catálogo completo de AI Functions.
- `labs/agent_bricks/01_knowledge_assistant.py` — preparación de datos para el Knowledge Assistant.

## 🧭 Decisiones UI vs Code de este track (resumen)

- **02 Gobernanza — UI → Code.** Primero etiquetas/ocultas una columna **a mano en Catalog Explorer** (clicks: tags, *column mask*) para *entender* el control; luego lo **automatizas con IA** (`ai_gen` para comentarios, `ai_query` para clasificar, `ALTER ... SET TAGS/MASK` en bucle) sobre todo el esquema. La UI enseña el concepto; el código lo escala.
- **03 AI Functions — Lado a lado.** El **Playground/AI Functions UI** y el **SQL** son intercambiables para la misma tarea (clasificar, extraer, resumir). El participante prueba un prompt en el Playground y a la vez ejecuta el `ai_query` equivalente en SQL — ve que es la *misma* capacidad.
- **04 Genie y Apps — UI → Code.** El **Genie space** se crea en la UI (intuición de NLQ y *instructions*); luego una **App Streamlit (código)** lo consume vía el SDK (`w.genie...`) para llevarlo a una experiencia productiva.
- **05 Agent Bricks — Code → UI.** El **código** prepara los datos (parseo de PDF con `ai_parse_document`, tabla Delta con CDF); luego el agente RAG completo (chunking, embeddings, Vector Search, endpoint) se construye **sin código en la UI de Agent Bricks**. El código alimenta; la UI ensambla el agente.

## Prerrequisitos

- Corre `../00_Setup/00_verify_environment`.
- Catálogo `ardemo_classic_dnubtw_catalog`, schema `ws_<usuario>`. Serverless v2.
- Foundation Models habilitados (Llama 3.3, Claude). Permiso para crear Genie spaces y agentes.
