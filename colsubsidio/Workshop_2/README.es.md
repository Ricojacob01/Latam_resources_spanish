# Colsubsidio · Taller SAP HANA ↔ Databricks (Workshop 2)

Taller práctico que muestra cómo usar **Databricks como motor de procesamiento (ETL + ML)** sobre datos que
viven en **SAP HANA**, **sin convertir a Databricks en un repositorio de datos**.

## Principio del taller

> **Databricks NO almacena las tablas de los casos de uso.** El procesamiento es **transitorio**
> (DataFrames y vistas temporales) y los resultados se **escriben de vuelta a SAP HANA** vía JDBC.
>
> Lo único que se persiste en el catálogo de Databricks es:
> 1. **Los datos sintéticos de origen** que el notebook `0 - SETUP` genera para *simular SAP HANA*
>    (porque en el laboratorio no hay un HANA real conectado), y
> 2. **Los activos del ciclo de vida de ML** del Módulo 2: la **feature table** (Feature Store en Unity
>    Catalog) y el **modelo registrado** (con alias `@Champion`). Es la excepción acordada: *Databricks
>    persiste solo lo de ML*.

## Catálogo y esquemas

- **Catálogo compartido** — todos los participantes usan el mismo catálogo (widget `catalogo`,
  por defecto `classic_stable_paco_catalog`).
- **Esquema por usuario** — cada participante trabaja en su propio esquema `ws2_<tu_usuario>`
  (derivado de `current_user()`), para que no haya colisiones de datos.

## Estructura

```
Workshop_2/
├── README.es.md                                  ← este archivo
├── 0 - Setup/
│   └── 0 - SETUP.es                              ← Ejecutar PRIMERO (genera los datos)
├── 1 - Validación Regulatoria (ETL)/
│   └── 1 - Validacion Regulatoria.es             ← Módulo 1 (~45 min)
├── 2 - Pronóstico de Ventas (ETL + ML)/          ← Módulo 2 (~60 min) — flujo MLOps
│   ├── 2.1 Feature Engineering + Feature Store   ← features + Feature Store (UC)
│   ├── 2.2 AutoML                                ← baseline automático (UI + API)
│   ├── 2.3 Entrenamiento (Feature Store + MLflow)← train con FeatureLookup + MLflow
│   ├── 2.4 Registro y Ciclo de Vida (UC)         ← alias @Challenger/@Champion
│   └── 2.5 Inferencia Batch + Write-back         ← score_batch + write-back a SAP HANA
└── SAP HANA Synthetic Orders Demo                ← Notebook de referencia (patrón JDBC)
```

## Agenda

| # | Módulo | Tipo | Tiempo | Tema |
|---|--------|------|--------|------|
| 0 | Setup | Generación de datos | ~5 min | Esquemas + datos sintéticos que simulan SAP HANA |
| 1 | Validación Regulatoria | ETL | ~45 min | Catálogo de validaciones, cruces, comparación histórica, CSV→XML, write-back |
| 2 | Pronóstico de Ventas | ETL + ML | ~60 min | Feature Store, AutoML, MLflow, Model Registry + alias, inferencia batch, write-back |
| — | *SAP HANA Synthetic Orders Demo* | Referencia | — | Patrón JDBC read/enrich/write-back |

El **Módulo 2** sigue el flujo de MLOps del workshop de referencia (`ML_workshop/ml_workshop-main`):
feature engineering → AutoML → entrenamiento con seguimiento MLflow → modelos en Unity Catalog con alias
Challenger/Champion → inferencia batch. Adaptado al caso de pronóstico y al principio de escribir de vuelta
a SAP HANA.

## Los dos casos de uso

### Módulo 1 — Validación Regulatoria (ETL)
Planeación recibe 4 bases desde Afiliaciones, las valida y reporta al ente de control. Hoy tarda **~40 min**;
la meta es **~10 min** con **comparaciones históricas** habilitadas.

- Lee las 4 bases desde SAP HANA (patrón JDBC) o del stand-in del laboratorio.
- Ejecuta un **catálogo de validaciones** (formato, calidad, cruces entre bases, duplicados, reglas de
  personas a cargo) que produce el **informe de inconsistencias**.
- Compara contra el periodo anterior para detectar **cambios de documento de identidad**.
- Genera el **XML regulatorio**.
- Escribe el informe y los cambios **de vuelta a SAP HANA**. No se persiste ninguna tabla de negocio en Databricks.

### Módulo 2 — Pronóstico de Ventas (ETL + ML)
Pronóstico comercial/institucional para Retail Farma, con **datos externos** (Banco de la República, DANE).
Meta: **precisión > 95%**. Flujo MLOps completo en 5 notebooks:

- **2.1** — Ingeniería de características + **Feature Store** (`FeatureEngineeringClient.create_table` en UC,
  clave primaria `(producto_familia, fecha)`, linaje).
- **2.2** — **AutoML** para un baseline automático (ruta por interfaz gráfica + API `databricks.automl`).
- **2.3** — Entrenamiento usando la feature table con **`FeatureLookup`**, **MLflow autolog**, linaje de datos
  (`log_input`), firma del modelo y registro con `fe.log_model`.
- **2.4** — Ciclo de vida en **Unity Catalog**: descripciones, etiquetas y alias **`@Challenger` → `@Champion`**
  con una regla de promoción por umbral de métrica.
- **2.5** — **Inferencia batch** con `fe.score_batch` sobre `@Champion` y **write-back a SAP HANA** para SAP
  Analytics Cloud.
- Hoja de ruta: Fase 1 (as-is) → Fase 2 (Feature Store/MLflow/Model Registry) → Fase 3 (Genie y agentes).

> **Nota de compute:** la **API** de AutoML (`databricks.automl`) requiere **Databricks Runtime for ML**; en
> serverless usa la ruta por interfaz gráfica descrita en el notebook 2.2. El resto de notebooks del Módulo 2
> corren en serverless (instalan `databricks-feature-engineering`, `mlflow`, `scikit-learn` con `%pip`).

## Prerrequisitos

- Workspace de Databricks con Unity Catalog y **compute Serverless** habilitado.
- El **catálogo compartido debe existir** y el atendee debe tener `USE CATALOG` + `CREATE SCHEMA` sobre él.
- Para el write-back **real** a SAP HANA: una **Databricks Connection** registrada en
  *Catalog Explorer → External Data → Connections* (el notebook la referencia por nombre, sin credenciales
  en el código). En el laboratorio, las banderas `LEER_DESDE_HANA` / `ESCRIBIR_A_HANA` quedan en `False` y
  todo corre contra el stand-in sintético.

## Cómo empezar

1. Abre `0 - Setup/0 - SETUP.es` → ejecuta todas las celdas (ajusta el widget `catalogo` si aplica).
2. Continúa con `1 - Validación Regulatoria (ETL)/` → luego `2 - Pronóstico de Ventas (ETL + ML)/`.
3. Cada notebook deriva tu esquema `ws2_<tu_usuario>` automáticamente — no hay colisiones entre participantes.
