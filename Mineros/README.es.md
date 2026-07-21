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

El mismo catálogo `sdp_workshop_<usuario>` y sus tablas Gold conectan ambos días.

---

## 📅 Día 1 — Data Engineering, Gobernanza y Genie

Carpeta: **`Dia 1 — Data Engineering, Gobernanza y Genie/`**

| # | Notebook | Duración | Qué se aprende |
|---|----------|----------|----------------|
| 00 | Bienvenida y Agenda | 15 min | Contexto y caso de negocio |
| 01 | Setup | 15 min | Catálogo, esquemas, volumen, datos de ejemplo |
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
| 02 | Lab App Streamlit + Genie | 60 min | Desplegar App que integra Genie |
| 03 | Intro a Agentes | 20 min | De Genie a agentes con herramientas |
| 04 | Lab Crear un Agente | 50 min | Foundation Models, Playground, tools |
| 05 | Lab Agent Bricks | 20 min | Knowledge Assistant (UI) |
| 06 | Cierre y Próximos Pasos | 5 min | Recap + roadmap |

Apoyo: `app_source/` (código desplegable de la App), `_data_setup_agentes.py`,
`APPENDICE - Batch Inference.py` (extra opcional).

Recursos compartidos: **`_recursos/`** (datos de agentes, imágenes, utilidades).

---

## 🛠️ Checklist del Administrador — Antes del Taller

Tareas que un **admin del workspace** debe completar antes de que lleguen los participantes.
Los pasos del Día 1 son bloqueantes desde el primer minuto; los del Día 2 pueden hacerse
durante el Día 1 si es necesario.

### Antes del Día 1

- [ ] Confirmar que **Unity Catalog** y **Serverless compute** están habilitados en el workspace.
- [ ] Confirmar que hay al menos un **SQL Warehouse** disponible (Pro o Serverless) al que los participantes tengan acceso `CAN USE`.
- [ ] Otorgar a cada participante el privilegio **`CREATE CATALOG`** a nivel de metastore — el notebook `01 - Setup` lo necesita para crear `sdp_workshop_<usuario>`:
  ```sql
  GRANT CREATE CATALOG ON METASTORE TO `<usuario_o_grupo>`;
  ```

### Antes del Día 2

- [ ] Crear el catálogo compartido **`academia`** si no existe. Los labs de agentes (`04`, `05` y el Apéndice) usan `academia.ia` y `academia.agent_bricks`:
  ```sql
  CREATE CATALOG IF NOT EXISTS academia;
  ```
- [ ] Otorgar permisos sobre `academia` a los participantes:
  ```sql
  GRANT USE CATALOG ON CATALOG academia TO `<usuario_o_grupo>`;
  GRANT CREATE SCHEMA ON CATALOG academia TO `<usuario_o_grupo>`;
  ```
- [ ] Confirmar que **Databricks Apps** está habilitado en el workspace y que los participantes tienen permisos para crear Apps.
- [ ] Confirmar que los **Foundation Models** pay-per-token están habilitados. Los labs usan:
  - `databricks-meta-llama-3-3-70b-instruct`
  - `databricks-gpt-oss-120b`
- [ ] Confirmar que los participantes pueden crear **Vector Search endpoints** (Lab 04, Ejercicio 02.c). La creación del endpoint tarda varios minutos — advertirlo al inicio del bloque de agentes.
- [ ] _(Opcional)_ Si el acceso a internet está restringido en el workspace, pre-cargar los archivos de datos en `academia.ia` manualmente desde `_recursos/datos_agentes/` (los notebooks tienen un ejercicio alternativo de carga manual).

---

## ✅ Prerrequisitos del Participante

- Workspace de Databricks con **Unity Catalog** y **Serverless** habilitados.
- Privilegios **`CREATE CATALOG`** (el admin los otorga antes del Día 1).
- Un **SQL Warehouse** disponible (para Genie, BI y la App).
- Para el Día 2: permisos para crear **Databricks Apps**, usar **Foundation Models** y acceso a `academia` (el admin lo configura antes del Día 2).

## 🔑 Datos que el participante debe anotar al final del Día 1

1. **Catálogo:** `sdp_workshop_<usuario>` (se deriva del usuario automáticamente).
2. **Genie Space ID** (de la Lección 6).
3. **HTTP Path** del SQL Warehouse.

Estos tres valores se usan en la App del Día 2 (`app_source/app.yaml`).

---

## 🗂️ Notas de diseño

- **Aislamiento por usuario:** cada participante trabaja en su propio catálogo `sdp_workshop_<usuario>`.
- **Genie y App sobre el mismo dataset:** la App del Día 2 consume las tablas Gold del Día 1
  (pedidos/clientes). Es **solo lectura** porque las tablas las gestiona el pipeline
  (streaming tables / materialized views); el patrón de escritura se muestra en el
  *Apéndice* de la Lección 2 (tabla propia de la app → escala a Lakebase).
- **Agentes UI-first:** los labs de agentes usan su propio dataset (`academia.ia`) y se
  enfocan en entender el modelo mental, no en ingeniería de producción.
- **Catálogo `academia` compartido:** a diferencia del catálogo del Día 1, `academia` es
  compartido entre todos los participantes. El `CREATE SCHEMA IF NOT EXISTS` de cada notebook
  es idempotente — múltiples participantes corriendo en paralelo no generan conflictos.

---

**Versión:** 2.0 (taller de 2 días) · **Idioma:** Español
