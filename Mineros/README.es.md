# Taller de 2 Días — De los Datos a los Agentes en Databricks

Un taller práctico de **2 días (~3:30 h cada uno)** que recorre el ciclo completo de la
plataforma Databricks: desde ingeniería de datos y gobernanza, hasta Genie, Databricks Apps
y agentes de IA. Todo el material está en **español** y usa un **único caso de negocio**
(un retailer con pedidos y clientes) como hilo conductor.

---

## 🎯 Arco del taller

```
  Día 1: Datos                                Día 2: Productos de IA
  ───────────────────────────────            ──────────────────────────────
  Pipeline (Bronze→Silver→Gold)              App Streamlit que integra Genie
        │  calidad + AUTO CDC                       │
        ▼                                           ▼
  Gobernanza (Unity Catalog)                 Agentes + Agent Bricks (UI-first)
        │                                           
        ▼                                           
  BI Dashboard + Genie  ─────────────────────▶ (se reutiliza en la App del Día 2)
```

El mismo esquema `academia.<tu_apellido>` y sus tablas Gold conectan ambos días.

---

## 📅 Día 1 — Data Engineering, Gobernanza y Genie

Carpeta: **`Dia 1 — Data Engineering, Gobernanza y Genie/`**

| # | Notebook | Duración | Qué se aprende |
|---|----------|----------|----------------|
| 00 | Bienvenida y Agenda | 15 min | Contexto y caso de negocio |
| 01 | Setup | 15 min | Tu esquema en `academia`, volumen raw, datos de ejemplo |
| 02 | Lab Pipeline con Calidad de Datos | 45 min | Medallion, Auto Loader, expectativas |
| 03 | Lab CDC y Producción | 50 min | AUTO CDC, SCD1, scheduling |
| 04 | Gobernanza (Unity Catalog) | 30 min | Permisos, linaje, seguridad |
| 05 | BI Dashboard | 20 min | Panel DBSQL con filtros y drill-down |
| 06 | Crear un Genie | 15 min | Genie en español sobre las tablas Gold |
| 07 | Cierre Día 1 | 5 min | Recap + qué anotar para el Día 2 |

Carpetas de apoyo: `transformations/` (SQL de los pipelines), `kpis.sql`.

## 📅 Día 2 — Databricks Apps y Agentes

Carpeta: **`Dia 2 — Databricks Apps y Agentes/`**

| # | Notebook | Duración | Qué se aprende |
|---|----------|----------|----------------|
| 00 | Bienvenida y Recap | 15 min | Repaso del Día 1 |
| 01 | Intro a Apps y Genie | 20 min | Conceptos de Databricks Apps |
| 02 | Lab App Streamlit + Genie | 60 min | App que lee datos, escribe notas (write-back) e integra Genie |
| 03 | Intro a Agentes | 20 min | De Genie a agentes con herramientas |
| 04 | Lab Crear un Agente | 50 min | Foundation Models, Playground, tools + Apéndice: Batch Inference (AI Functions) |
| 05 | Lab Agent Bricks | 20 min | Knowledge Assistant (UI) |
| 06 | Cierre y Próximos Pasos | 5 min | Recap + roadmap |

Apoyo: `app_source/` (código desplegable de la App) y `_data_setup_agentes.py`.
El **Batch Inference** (AI Functions a escala) vive como *Apéndice* al final de `04 - Lab Crear un Agente`.

Recursos compartidos: **`_recursos/`** (datos de agentes, imágenes, utilidades).

---

## ✅ Prerrequisitos

- Workspace de Databricks con **Unity Catalog** y **Serverless** habilitados.
- Privilegios **CREATE CATALOG**.
- Un **SQL Warehouse** disponible (para Genie, BI y la App).
- Para el Día 2: permisos para crear **Databricks Apps** y usar **Foundation Models**.

## 🔑 Datos que el participante debe anotar al final del Día 1

1. **Catálogo/esquema:** `academia.<tu_apellido>` (el esquema se deriva del usuario automáticamente).
2. **Genie Space ID** (de la Lección 6).
3. **HTTP Path** del SQL Warehouse.

Estos tres valores se usan en la App del Día 2 (`app_source/app.yaml`).

---

## 🗂️ Notas de diseño

- **Aislamiento por usuario:** catálogo compartido `academia`, cada participante en su propio esquema `academia.<tu_apellido>`; la capa medallion se distingue por el sufijo de tabla (`_bronze`/`_silver`/`_gold`).
- **Genie y App sobre el mismo dataset:** la App del Día 2 consume las tablas Gold del Día 1
  (pedidos/clientes). Es **solo lectura** porque las tablas las gestiona el pipeline
  (streaming tables / materialized views); el patrón de escritura se muestra en el
  *Apéndice* de la Lección 2 (tabla propia de la app → escala a Lakebase).
- **Agentes UI-first:** los labs de agentes usan su propio dataset (`academia.ia`) y se
  enfocan en entender el modelo mental, no en ingeniería de producción.

---

**Versión:** 2.0 (taller de 2 días) · **Idioma:** Español
