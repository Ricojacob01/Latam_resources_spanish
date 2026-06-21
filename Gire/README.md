<img src="https://raw.githubusercontent.com/aestaire/ml_workshop/refs/heads/main/files/images/hands-on.png">

# GIRE x Databricks — Workshop Hands-On (3 tracks)

Entrenamiento práctico de la **Databricks Data Intelligence Platform**, organizado en **tres focus tracks**. Cada track sigue la misma forma de módulo que el resto del repo (estilo `Comfama`): **Bienvenida/Agenda → Product Tour → LAB Express → LAB(s) deep-dive → Cierre**.

## Los 3 tracks

| Track | Carpeta | De qué trata | Fuentes que adapta |
|---|---|---|---|
| 🛠️ **Data Engineering** | [`Data Engineering/`](./Data%20Engineering/) | Ingesta → Medallion → Spark Declarative Pipelines (calidad de datos + CDC) → Orquestación con Jobs | `SDP Workshop`, `lakehouse-iot-plataform_full_spanish` (ingesta + orquestación) |
| 🤖 **Agents and Governance** | [`Agents and Governance/`](./Agents%20and%20Governance/) | Unity Catalog (gobernanza + clasificación con IA) → AI Functions SQL → Genie + Apps → Agent Bricks | `Data_governace`, `Genie_App_workshop`, `databricks-genai-lab`, labs Gire previos |
| 🧪 **MLOps** | [`MLOps/`](./MLOps/) | Feature engineering → AutoML → entrenamiento → registro en UC (Champion/Challenger) → validación → batch inference | `ML_workshop`, `lakehouse-iot-plataform_full_spanish` (ML/GenAI) |

> ¿Buscas MLOps **end-to-end con Model Serving + Job de orquestación automática**? Está en el workshop ampliado [`../CP/MLOps/`](../CP/MLOps/).

## Catálogo y schema (obligatorio leer antes de empezar)

Todos los notebooks usan el mismo patrón:

- **Catálogo compartido**: `ardemo_classic_dnubtw_catalog`
- **Schema personal por usuario**: `ws_<usuario>` (ej. `ws_juan_perez`) — se crea solo.

La primera celda **Setup del lab** de cada notebook valida acceso y crea tu schema si no existe — no necesitas configurar nada manualmente. Antes de cualquier track corre [`00_Setup/00_verify_environment`](./00_Setup/00_verify_environment.py).

- **Compute:** Serverless Environment v2 para casi todo; cluster clásico `ml_workshop_databricks` para el track de MLOps.

## 🧭 Patrón "UI vs Code" (aplica a los 3 tracks)

El workshop está diseñado para que cada participante viva **las dos caras de Databricks**: la **UI** (clicks, intuición, descubrimiento) y el **código/notebook/API** (reproducibilidad, automatización, CI/CD). **No es code-only.**

Cada módulo abre con una nota **"Enfoque UI vs Code"** que declara, para ese módulo, si presentamos UI y código **lado a lado** (la misma tarea de las dos formas, a la vez) o **secuencial** (primero UI para construir intuición, luego código — o viceversa), y **por qué**.

Criterio general:

- **Secuencial (UI → Code):** cuando la UI ayuda a *entender* el concepto antes de automatizar (crear un pipeline, un Genie space o un Job la primera vez en la UI, luego definirlo como código).
- **Secuencial (Code → UI):** cuando el código *produce* el activo y la UI sirve para *inspeccionarlo/gobernarlo/monitorearlo* (registrar un modelo por API y luego verlo en UC; correr un pipeline y ver el grafo).
- **Lado a lado:** cuando UI y código son intercambiables para la misma tarea y queremos que el participante elija su flujo (GRANT de permisos, query de un endpoint, lanzar una corrida).

El detalle por módulo está en el README de cada track.

## Estructura

```
Gire/
├── 00_Setup/
│   └── 00_verify_environment            ← EMPIEZA AQUI
├── Data Engineering/                    ← TRACK 1 (nuevo)
│   ├── 00 - Bienvenida y Agenda
│   ├── 01 - Product Tour (Lakehouse & Lakeflow)
│   ├── 02 - LAB Express - Ingesta y Medallion
│   ├── 03 - LAB Spark Declarative Pipeline (Calidad + CDC)
│   ├── 04 - LAB Orquestacion con Jobs
│   ├── 05 - Cierre y Workshop Preview
│   └── pipelines/  (definiciones SQL del pipeline)
├── Agents and Governance/               ← TRACK 2
│   ├── 00 - Bienvenida y Agenda
│   ├── 01 - Product Tour (UC + Genie + Agent Bricks)
│   ├── 02 - LAB Gobernanza con Unity Catalog
│   ├── 03 - LAB AI Functions (SQL)
│   ├── 04 - LAB Genie y Apps
│   ├── 05 - LAB Agent Bricks (Knowledge Assistant)
│   ├── 06 - Cierre y Workshop Preview
│   └── labs/  (contenido hands-on existente reutilizado)
└── MLOps/                               ← TRACK 3
    ├── 00 - Bienvenida y Agenda
    ├── 01 - Product Tour (MLOps en Databricks)
    ├── 02 - LAB Express - del notebook a UC
    ├── 05 - Cierre y Workshop Preview
    └── labs/ml_en_databricks/  (pipeline ML completo reutilizado)
```

## Material de referencia

- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/en/dlt/)
- [Lakeflow Jobs](https://docs.databricks.com/aws/en/jobs/)
- [Unity Catalog](https://docs.databricks.com/aws/en/data-governance/unity-catalog/)
- [Genie](https://docs.databricks.com/aws/en/genie/)
- [AI Functions](https://docs.databricks.com/aws/en/large-language-models/ai-functions)
- [Agent Bricks](https://docs.databricks.com/aws/en/generative-ai/agent-bricks/)
- [MLflow](https://docs.databricks.com/aws/en/mlflow/) · [AutoML](https://docs.databricks.com/aws/en/machine-learning/automl/)
