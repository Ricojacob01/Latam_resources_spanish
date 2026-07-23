# Checklist Pre-Workshop — BNCR (Freddy Pérez)

Ejecutar **48 horas antes** del evento y **el día del taller** en el workspace del instructor.

## En Databricks (instructor)

- [ ] Git folder sincronizado: `Latam_resources_spanish` → `Data Engineering/`
- [ ] Ejecutar `00 - Setup/00_PREFLIGHT.ipynb` → todo ✅
- [ ] Ejecutar `00 - Setup/00_setup.ipynb` → catálogo **BNS** creado
- [ ] Pipeline LDP creado y **1 ejecución exitosa**
- [ ] Job de prueba con 3 tareas (incremental → pipeline → SQL)
- [ ] Presentación `BNCR_v3.pptx` lista

## Por participante (enviar a IT BNCR)

- [ ] Laptop + navegador Chrome/Edge
- [ ] Acceso activo a workspace Databricks propio
- [ ] Unity Catalog habilitado
- [ ] Permisos: `CREATE CATALOG`, `CREATE SCHEMA`, `CREATE TABLE`, `CREATE VOLUME`
- [ ] Serverless notebooks habilitado (4 vCPU / 16 GB)
- [ ] Lakeflow Declarative Pipelines habilitado
- [ ] SQL Warehouse Serverless (Small)
- [ ] Acceso a `github.com`

## Orden de notebooks el día del taller

| Orden | Notebook | Duración |
|-------|----------|----------|
| 1 | `00_setup.ipynb` | 15 min |
| 2 | `01_unity_catalog_gobierno.ipynb` | 15 min |
| 3 | `02_federacion_onelake_marketplace.ipynb` | 10 min |
| 4 | `03_lakeflow_connect_demo.ipynb` | 10 min (demo) |
| 5 | `04_ldp_medallion_lab.ipynb` | 45 min |
| 6 | `99_incremental.ipynb` + re-run pipeline | 10 min |
| 7 | `05_genie_code_pipelines.ipynb` | 20 min |
| 8 | `06_lakeflow_designer.ipynb` | 20 min |
| 9 | `07 - Lakeflow Jobs/05_jobs_orquestacion.ipynb` | 25 min |

## Errores comunes y solución

| Error | Solución |
|-------|----------|
| `catalog_name is not defined` | Ejecutar **Run All** en `00_setup` desde la celda 1 |
| `carga_datos` falla | Haga **Git Pull** y **Run All** en `00_setup`; la carga usa Git folder o GitHub API (sin `/tmp` local) |
| Pipeline no encuentra archivos | Config: `catalog=BNS`, `schema=raw` |
| `%run` falla por ruta | Usar Git folder con estructura de carpetas intacta |

## Link materiales

https://github.com/Ricojacob01/Latam_resources_spanish/tree/main/Data%20Engineering
