# Kit de Taller Genie — reutilizable en cualquier cuenta

Un taller práctico de 2 sesiones (3 h c/u) que lleva **un dataset real de cualquier cliente**
desde cero hasta un **Genie Agent útil** con benchmarks validados. **Genérico y parametrizado** —
apúntalo al catálogo/esquema del cliente y funciona; no hay nada hardcodeado.

**Formato:** 2 × 3 h · **Herramientas:** Genie UI + **Genie Code** (en todo el ciclo) · **Sala:** BI developer + power users + SA.

---

## 🧑‍🤝‍🧑 Multi-usuario: catálogo compartido + un esquema por usuario

La mayoría de las cuentas de taller comparten **un catálogo** y le dan a **cada participante su propio esquema**.
El kit implementa ese patrón de forma automática:

| Parámetro | Cómo se define | Ejemplo |
|---|---|---|
| `catalog` (compartido) | **Widget** que fija el instructor — **sin default**, para portar a cualquier cuenta | `workshop_catalog` |
| `schema` (tuyo) | **Se deriva solo** de tu usuario: `taller_genie_<usuario>` | `taller_genie_rico_martinez` |

Todo esto vive en **`00_config`**, que cada notebook invoca con `%run ./00_config`. Cada participante
corre los mismos notebooks y obtiene **su propio esquema aislado** — sin pisarse. Portar a otra cuenta =
cambiar el valor del widget `catalog` (y el `WAREHOUSE_ID` / `PARENT_PATH` en los scripts `.sh`).

---

## 📂 Contenido — secuencia numerada, fácil de seguir

| Paso | Archivo | Qué hace |
|---|---|---|
| — | `00_config.py` | **Config común.** Catálogo compartido (widget) + tu esquema (auto). Lo llaman los demás con `%run`. |
| 1 | `01_data_setup.sql` | **(Opcional)** dataset de demo sintético en **tu** esquema — para dry runs o clientes sin datos. |
| 2 | `02_genie_code_exploration.py` | **Explora** cualquier dataset con **Genie Code** (descubrimiento, métricas, calidad). |
| 3 | `03_genie_code_tareas.py` | **Genie Code para distintas tareas**: preparar datos, calidad, métricas, sinónimos, sample questions, benchmarks, instrucciones. |
| 4 | `04_build_genie_agent.py` | **Genera** un `genie_agent.json` introspeccionando tus tablas (`--auto-user-schema`). |
| 5 | `05_create_genie_agent.sh` | **Crea** el Genie Agent desde el `serialized_space`. |
| 6 | `06_benchmark_agent.py` | **Puntúa** el agente contra tus benchmarks vía Conversation API (sin Workbench). |
| ref | `QUICKSTART.md` | **Guía de 1 página para participantes** — qué correr y en qué orden. |
| ref | `genie_agent.json` | Ejemplo *completo* (dominio ventas) — plantilla portable con tokens `__CATALOG__`/`__SCHEMA__` que `05` localiza a tu esquema. |
| ref | `benchmarks.csv` | Ejemplo *completo* de benchmarks (dominio ventas). |
| ref | `benchmarks_TEMPLATE.csv` | Plantilla en blanco (15 benchmarks, 5 tier-1 "deal-breakers"). |

---

## ⚙️ Configurar para un nuevo cliente / cuenta (5 min)

1. Consigue del champion: caso de uso prioritario, **2–4 tablas gold**, dueño de cada tabla,
   descripciones, métricas/definiciones, claves de unión, ~10 preguntas de negocio, y requisitos de acceso/RLS.
2. Abre `00_config` y llena el widget **`catalog`** con el catálogo compartido de la cuenta
   (¿no lo sabes? corre `list_catalogs()`). Tu esquema se deriva solo.
3. En los scripts `.sh`, ajusta `WAREHOUSE_ID` (`databricks warehouses list`) y `PARENT_PATH`.
4. Si el cliente **aún no tiene datos**, corre `01_data_setup.sql` para un dry run con datos sintéticos.

---

## 🗓️ Flujo del taller

### Día 1 (3 h) — Fundamentos, datos y Genie Code → *consumo el mismo día*
| Tiempo | Segmento |
|---|---|
| 0:00–0:30 | Descubrimiento del caso + **capa de definiciones** (cómo se calcula cada métrica) + RLS/gobernanza |
| 0:30–1:05 | `00_config` (cada quien fija `catalog`) → apuntar `02_...` a las tablas del cliente **o** correr `01_data_setup.sql` (demo) → *consumo* |
| 1:05–1:15 | Break |
| 1:15–2:05 | **Genie Code — explorar** con `02_...`: esquema, perfilado, relaciones, métricas |
| 2:05–2:50 | **Genie Code — distintas tareas** con `03_...`: preparar datos, calidad, métricas, sinónimos, sample questions, benchmarks |
| 2:50–3:00 | Cierre + tarea: power users finalizan 10–15 benchmarks (usa `benchmarks_TEMPLATE.csv`) |

### Entre sesiones
Power users cierran benchmarks + respuestas esperadas y marcan los 5 **tier-1**.
SA corre `04_build_genie_agent.py --auto-user-schema` para pre-generar `genie_agent.generated.json` y curar la base.

### Día 2 (3 h) — Construir, benchmark, afinar, producción
| Tiempo | Segmento |
|---|---|
| 0:00–0:20 | Crear el espacio: `04_build_genie_agent.py ... --create` (o `05_create_genie_agent.sh`) |
| 0:20–1:05 | **Configurar** (co-creado): sample questions, sinónimos, ejemplos SQL, instrucciones (del glosario Día 1) |
| 1:05–1:15 | Break |
| 1:15–2:05 | **Benchmark** — `06_benchmark_agent.py` corre las 15 preguntas y captura el SQL; marca pass/fail (foco tier-1) |
| 2:05–2:40 | **Afinar a ≥85%** — sinónimos para valores de filtro, más ejemplos SQL/instrucciones, re-test |
| 2:40–3:00 | **Ruta a producción** — próximos datasets, más usuarios, gobernanza/RLS, propiedad, cadencia GenieOps |

---

## 🧞 Genie Code en todo el ciclo (no solo explorar)

`03_genie_code_tareas.py` usa Genie Code para 7 tareas: **(1)** preparar/denormalizar datos,
**(2)** calidad de datos, **(3)** construir métricas certificadas, **(4)** descubrir sinónimos y valores reales,
**(5)** generar sample questions, **(6)** generar benchmarks, **(7)** redactar las `text_instructions`.
Regla de oro: prompt → revisa el SQL → si sirve, **guárdalo** (material del agente); si falla, **anota qué le faltó** (instrucción).

---

## 🚀 Ejemplos de uso de los scripts

```bash
# Generar el agente desde TU esquema del taller (esquema derivado automáticamente)
python3 04_build_genie_agent.py --profile ucode --catalog <CAT_COMPARTIDO> --auto-user-schema

# Tablas específicas + crear el espacio de una vez
python3 04_build_genie_agent.py --profile ucode --catalog <CAT> --auto-user-schema \
  --tables gold_oportunidades --warehouse <WH_ID> \
  --parent-path /Users/<tu>@databricks.com/Latam_resources_spanish/Genie --title "<Cliente>" --create

# O crear desde un JSON con el script bash
PROFILE=ucode WAREHOUSE_ID=<WH> TITLE="<Cliente>" \
  AGENT_JSON=genie_agent.generated.json ./05_create_genie_agent.sh

# Crear desde el EJEMPLO (plantilla): CATALOG localiza los tokens a tu esquema del taller
PROFILE=ucode WAREHOUSE_ID=<WH> CATALOG=<CAT_COMPARTIDO> TITLE="Ejemplo ventas" \
  AGENT_JSON=genie_agent.json ./05_create_genie_agent.sh

# Puntuar contra benchmarks (sin Genie Workbench)
python3 06_benchmark_agent.py --profile ucode --space-id <SPACE_ID> --benchmarks benchmarks.csv
```

---

## 📊 Cómo puntuar sin Genie Workbench
`06_benchmark_agent.py` corre cada pregunta del CSV vía **Conversation API**, captura el SQL generado
y deja una columna `pass_fail` para que marques manualmente. Mantén el CSV como hoja de control
(pregunta · SQL esperado · tier · pass/fail · fix). **Meta: ≥85% en tier-1.** Registra baseline vs. afinado.

## ⚠️ Límites a considerar
~5 preguntas por minuto · máx. 30 tablas (5–7 ideal) · no mezclar metric views con tablas normales ·
denormalizar mejora la fiabilidad · descripciones + instrucciones + ejemplos SQL son lo que mueve la precisión.

## 🎯 Métricas de éxito
≥85% tier-1 · consumo visible el Día 1 · usuarios consultando en 30 días · playbook replicado a 2–3 datasets en 60 días.
