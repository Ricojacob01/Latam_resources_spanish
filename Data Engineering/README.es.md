# Workshop Databricks — Ingeniería de Datos (BNCR)

Taller práctico de **4 horas** para el **Banco Nacional de Costa Rica (BNCR)**.  
**Instructor:** Freddy Pérez — Solutions Architect  
**Fecha:** Martes 28 de julio de 2026  
**Participantes:** 15–20 personas (cada uno en su propio workspace Databricks)

## Objetivos del taller

Al finalizar, los participantes podrán:

- Describir la **Databricks Data Intelligence Platform** y el rol de **Lakeflow**
- Configurar **Unity Catalog** con gobierno, linaje y permisos
- Explorar **federación de datos** con Marketplace y Delta Sharing
- Ingerir datos con **Auto Loader** (patrón equivalente a Lakeflow Connect)
- Construir pipelines **Lakeflow Declarative Pipelines** con arquitectura **Medallón**
- Orquestar cargas con **Lakeflow Jobs**, monitoreo y linaje

## Escenario: Transacciones Bancarias BNCR

Pipeline de datos de transacciones del Banco Nacional:

```
Archivos (Volume) → Bronze → Silver → Gold → SQL / BI
 transacciones      raw     limpio   agregados
 clientes_cdc
 cuentas
 sucursales
```

## Estructura del repositorio

```
Data Engineering/
├── 00 - Setup/
│   ├── 00_variables.ipynb          # Variables globales (catálogo BNS)
│   ├── 00_setup.ipynb              # Crear catálogo, esquemas, volumen
│   ├── 00_generar_datos.ipynb    # Generar datos sintéticos a escala (opcional)
│   └── 99_incremental.ipynb      # Carga incremental durante el taller
├── 01 - Unity Catalog y Gobierno/
│   └── 01_unity_catalog_gobierno.ipynb
├── 02 - Federacion de Datos/
│   └── 02_federacion_onelake_marketplace.ipynb
├── 03 - Lakeflow Connect/
│   └── 03_lakeflow_connect_demo.ipynb
├── 04 - Lakeflow Declarative Pipelines/
│   ├── 04_ldp_medallion_lab.ipynb
│   ├── 04a_tour_pipeline.ipynb
│   └── transformations/
│       ├── 01-bronze.sql
│       ├── 02-silver.sql
│       └── 03-gold.sql
├── 05 - Lakeflow Jobs/
│   └── 05_jobs_orquestacion.ipynb
└── Files/
    ├── initial/                    # Carga inicial (~20K transacciones)
    └── incremental/                # Lotes incrementales
```

## Prerrequisitos

| Recurso | Especificación |
|---------|----------------|
| Workspace Databricks | Propio de cada participante, Unity Catalog habilitado |
| Compute interactivo | **Serverless** 4 vCPU / 16 GB (recomendado) |
| SQL Warehouse | **Serverless Small**, auto-scale 1–10 |
| Permisos UC | `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE` |
| Catálogo | **`BNS`** (cada participante lo crea en su workspace) |

## Primeros pasos

### 1. Importar el repositorio

```text
https://github.com/Ricojacob01/Latam_resources_spanish
```

En Databricks: **Workspace → Import → Git folder** → carpeta `Data Engineering`.

### 2. Ejecutar Setup

1. Abrir `00 - Setup/00_variables.ipynb` → ejecutar todas las celdas
2. Abrir `00 - Setup/00_setup.ipynb` → ejecutar todas las celdas
3. Ejecutar la celda de carga de datos (`carga_datos("initial")`)
4. Guardar la ruta del volumen: `/Volumes/BNS/raw/transacciones`

### 3. Seguir los labs en orden

| # | Notebook | Duración |
|---|----------|----------|
| 01 | Unity Catalog y Gobierno | 20 min |
| 02 | Federación OneLake / Marketplace | 15 min |
| 03 | Lakeflow Connect (demo instructor) | 15 min |
| 04 | LDP — Arquitectura Medallón | 45 min |
| 05 | Lakeflow Jobs | 30 min |

## Configuración del Pipeline (Lab 04)

Al crear el **Lakeflow Declarative Pipeline**, configurar:

| Parámetro | Valor |
|-----------|-------|
| **Catalog** | `BNS` |
| **Target schema** | `bronze` (el pipeline crea silver/gold según SQL) |
| **Configuration** | `catalog` = `BNS`, `schema` = `raw` |
| **Source code** | Carpeta `04 - Lakeflow Declarative Pipelines/transformations/` |
| **Compute** | Serverless |

## Cronograma sugerido (4 horas)

| Hora | Actividad | Tipo |
|------|-----------|------|
| 0:00–0:20 | Intro Databricks + Plataforma | Teoría |
| 0:20–0:40 | Unity Catalog y Gobierno | Teoría + Lab 01 |
| 0:40–1:00 | Federación OneLake + Lakeflow | Teoría + Lab 02 |
| 1:00–1:15 | **Setup** + carga inicial | Lab |
| 1:15–1:35 | Carga incremental (Auto Loader) | Lab |
| 1:35–1:55 | **Demo** Lakeflow Connect (instructor) | Demo |
| 1:55–2:35 | **Lab 04** — LDP Medallón | Lab |
| 2:35–2:45 | Break | — |
| 2:45–3:15 | **Lab 05** — Jobs y linaje | Lab |
| 3:15–3:30 | Carga incremental + consultas SQL Gold | Lab |
| 3:30–4:00 | Q&A y próximos pasos | Cierre |

## Logística del evento

> **Pendiente de confirmar:** WiFi y lugar del evento (Costa Rica).

## Resolución de problemas

### `Catalog 'BNS' not found`
Ejecutar `00_setup.ipynb` completo antes de los labs.

### `Variable 'catalog' not found` en el pipeline
En **Pipeline Settings → Configuration**, agregar:
- `catalog` = `BNS`
- `schema` = `raw`

### Sin acceso a GitHub desde Databricks
Subir manualmente la carpeta `Files/initial/` al volumen:
**Catálogo → BNS → raw → transacciones → Upload to Volume**

### Pipeline no detecta archivos nuevos
Ejecutar `99_incremental.ipynb` y volver a correr el pipeline.

## Recursos adicionales

- [Lakeflow Declarative Pipelines](https://docs.databricks.com/aws/es/dlt/)
- [Lakeflow Jobs](https://docs.databricks.com/aws/es/jobs/)
- [Lakeflow Connect Demo](https://www.databricks.com/resources/demos/tours/platform/discover-databricks-lakeflow-connect-demo)
- [Unity Catalog Best Practices](https://docs.databricks.com/aws/es/data-governance/unity-catalog/best-practices.html)

---

**Versión:** 1.0 — BNCR Workshop Julio 2026
