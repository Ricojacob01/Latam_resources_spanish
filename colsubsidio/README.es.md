# Colsubsidio · Taller de Lakeflow Spark Declarative Pipelines

Workshop práctico de ~90 minutos enfocado en construir un pipeline declarativo de datos con calidad y CDC usando **Lakeflow Spark Declarative Pipelines** (antes Delta Live Tables), todo dentro del catálogo compartido `colsubsidio_workshop`.

## Catálogo y schemas

- **Catálogo compartido**: `colsubsidio_workshop`
- **Schemas por usuario**: `sdp_workshop_<tu_usuario>_bronze`, `_silver`, `_gold`
  - `<tu_usuario>` = `current_user()` con `@` cortado y `.`/`-` reemplazados por `_`
- **Volume**: `/Volumes/colsubsidio_workshop/sdp_workshop_<tu_usuario>_default/raw`

> Cada participante tiene sus propios schemas dentro del mismo catálogo — sin colisiones de datos.

## Estructura

```
colsubsidio/
├── 0 - Setup/
│   └── 0 - SETUP.es                              ← Ejecutar PRIMERO
├── 01.5 - Governance/
│   └── 01_2_unity_catalog_data_governance.es     ← Gobernanza con UC (opcional)
├── 1 - Building Pipeline with Data Quality/
│   └── 1 - Building Pipeline with Data Quality.es ← Ejercicio 1 (~40 min)
├── 2 - CDC and Production/
│   ├── 2 - CDC and Production.es                 ← Ejercicio 2 (~50 min)
│   └── customers_pipeline.es.sql                 ← Se mueve a transformations/ en Ej.2
├── transformations/
│   └── orders_pipeline.es.sql                    ← Asset del pipeline (Ej.1)
└── utilities/
    └── utils.py
```

## Agenda

| # | Sección | Tiempo | Tema |
| -- | -- | -- | -- |
| 00 | Setup | 5 min | Crear schemas, volume, datos de ejemplo |
| 01 | Building Pipeline with Data Quality | 40 min | Lakeflow Pipelines Editor, expectations |
| 01.5 | Governance *(opcional)* | 15 min | Unity Catalog, grants, lineage |
| 02 | CDC and Production | 50 min | AUTO CDC INTO, scheduling con Jobs |

## Cambios para la nueva UI de Lakeflow (2026)

El notebook **1 - Building** ya refleja el flujo actual:

1. **Sidebar → `+ New` → `ETL Pipeline`** abre directamente el **Lakeflow Pipelines Editor**
   (ya no hay modal intermedio de "Add existing assets")
2. **Toolbar arriba del editor**: edita el nombre del pipeline, el catalog/schema por defecto, y el lenguaje desde ahí
3. **⚙️ Settings → Pipeline assets → Code assets**: aquí se conecta la carpeta `transformations/` del workshop
4. **Configuration** (Settings → Configuration tab): aquí se añaden los key/value para `source`, `bronze`, `silver`, `gold`

## Prerrequisitos

> **El workspace admin debe crear el catálogo `colsubsidio_workshop` antes del workshop** (CREATE CATALOG en el metastore no se otorga a los atendees). Si ya existe un catálogo con otro nombre, los atendees pueden ajustarlo en el widget **Shared catalog** al inicio del notebook `0 - SETUP`.


- Workspace de Databricks con Unity Catalog
- Compute Serverless habilitado
- El catálogo `colsubsidio_workshop` debe existir y el atendee debe tener `USE CATALOG` + `CREATE SCHEMA`

## Cómo empezar

1. Importar este folder al workspace del participante
2. Abrir `0 - Setup/0 - SETUP.es` → ejecutar todas las celdas
3. Anotar los valores impresos (catalog/schema/volume) — los usarás en el Ejercicio 1
4. Continuar con `1 - Building Pipeline with Data Quality/`
