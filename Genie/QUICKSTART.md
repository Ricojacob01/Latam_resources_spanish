# 🚀 Quickstart del participante — Taller Genie (1 página)

Bienvenido/a. Sigue estos pasos en orden. Cada quien trabaja en **su propio esquema**
dentro de un **catálogo compartido** — no te pisas con nadie.

## Antes de empezar (30 seg)
- Pregúntale al instructor el **nombre del catálogo compartido** (ej. `workshop_catalog`).
- Ten a la mano un **SQL Warehouse encendido** (o serverless).

---

## Día 1 — Explorar

**1. Configura (una vez).** Abre **`00_config`** → en el widget **`catalog`** escribe el catálogo
compartido → corre el notebook.
> Verás: `✅ Configuración lista … Esquema: taller_genie_<tu_usuario>`. Ese esquema es **tuyo**.
> ¿No sabes el catálogo? Corre `list_catalogs()` en una celda.

**2. Consigue datos.**
- ¿El cliente ya tiene tablas? Sáltate al paso 3 y explóralas.
- ¿No hay datos aún? Abre **`01_data_setup`** y córrelo → crea 4 tablas de demo en tu esquema.

**3. Explora con Genie Code.** Abre **`02_genie_code_exploration`** → corre la celda de contexto →
abre el panel ✨ **Genie/Assistant** y pega los prompts (descubrimiento, métricas, calidad).
> Anota: prompts que funcionaron → *sample questions*; los que fallaron → *benchmarks*.

**4. Prepara y define.** Abre **`03_genie_code_tareas`** → usa Genie Code para preparar datos,
calidad, métricas, sinónimos, sample questions, benchmarks e instrucciones.

**5. Tarea.** Llena 10–15 preguntas en `benchmarks_TEMPLATE.csv` y marca las 5 **tier-1** (las críticas).

---

## Día 2 — Construir, probar, afinar

**6. Genera el agente** (en una terminal con el CLI de Databricks):
```bash
python3 04_build_genie_agent.py --profile <PERFIL> --catalog <CAT_COMPARTIDO> --auto-user-schema
```

**7. Crea el espacio Genie:**
```bash
PROFILE=<PERFIL> WAREHOUSE_ID=<WH> TITLE="Mi agente" \
  AGENT_JSON=genie_agent.generated.json ./05_create_genie_agent.sh
```
> ¿Quieres partir del ejemplo de ventas? Usa `CATALOG=<cat> AGENT_JSON=genie_agent.json` (se localiza solo a tu esquema).

**8. Configura en la UI:** sample questions, sinónimos, ejemplos SQL, instrucciones (de tu glosario).

**9. Puntúa (sin Workbench):**
```bash
python3 06_benchmark_agent.py --profile <PERFIL> --space-id <SPACE_ID> --benchmarks benchmarks.csv
```
> Abre el CSV de resultados, marca **pass/fail**, y afina lo que falle. **Meta: ≥85% en tier-1.**

---

### Comandos útiles
| Necesitas | Comando |
|---|---|
| Ver catálogos | `list_catalogs()` (en notebook) |
| Ver warehouses | `databricks warehouses list --profile <PERFIL>` |
| Ver espacios Genie | `databricks genie list-spaces --profile <PERFIL>` |

¿Dudas? Pregúntale al SA/instructor. La guía completa está en **`README.md`**.
