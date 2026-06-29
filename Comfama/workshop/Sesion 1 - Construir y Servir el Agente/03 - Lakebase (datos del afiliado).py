# Databricks notebook source
# MAGIC %md
# MAGIC # 🐘 Sesión 1 · 03 — Lakebase: la capa operacional del agente
# MAGIC
# MAGIC **Meta:** crear el **sistema de registro operacional** del agente sobre **Lakebase** (Postgres serverless
# MAGIC gestionado). Aquí vive todo lo **transaccional y de baja latencia**: afiliados, programas con **cupos en vivo**,
# MAGIC reservas y la **memoria conversacional**.
# MAGIC
# MAGIC > **Equivale a: Cosmos DB.** Hoy Comfama usa Cosmos para el estado del agente. Lakebase lo reemplaza con Postgres
# MAGIC > estándar, **integrado al lakehouse** (sync con Delta, gobierno en Unity Catalog, branching y scale-to-zero).
# MAGIC
# MAGIC Módulo **dual-mode**: aprovisionamos por **🖱️ UI** y trabajamos los datos por **⌨️ código (psql/psycopg2)**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🧠 ¿Por qué Lakebase y no Delta para esto?
# MAGIC
# MAGIC | Necesidad del agente | Delta (lakehouse) | **Lakebase (OLTP)** |
# MAGIC |---|---|---|
# MAGIC | Reservar un cupo concurrentemente (fila a fila) | ❌ no es transaccional fila a fila | ✅ transacción Postgres |
# MAGIC | Leer el historial de chat en cada turno (<10ms) | ❌ latencia de query analítico | ✅ lectura OLTP |
# MAGIC | Escritura puntual de alta frecuencia | ❌ pensado para batch/streaming | ✅ INSERT/UPDATE puntual |
# MAGIC | Análisis sobre millones de filas | ✅ | ➖ (para eso se sincroniza ⇄ Delta) |

# COMMAND ----------

# MAGIC %pip install -U psycopg2-binary databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — crear la instancia y la base de datos
# MAGIC
# MAGIC 1. Menú izquierdo → **Compute** → pestaña **Database instances** (Lakebase).
# MAGIC 2. **Create database instance**:
# MAGIC    - **Name**: el valor de `LAKEBASE_PROJECT` (mostrado abajo).
# MAGIC    - **Tier**: *Autoscaling* (scale-to-zero + branching). Capacidad min 0.5 CU.
# MAGIC    - **Create**. Espera a estado **Available** (1–2 min).
# MAGIC 3. Abre la instancia → botón **Connect**. Verás el **host** y un botón para **generar un token** (OAuth).
# MAGIC    Cópialos: los pegaremos abajo (o los obtenemos por SDK en la celda siguiente).
# MAGIC 4. En la pestaña **Databases** de la instancia → **Create database** → nombre: el valor de `LAKEBASE_DB`.
# MAGIC
# MAGIC > 💡 La instancia es un recurso **compartido del workspace** (créala una vez). Más abajo cada asistente crea su
# MAGIC > **branch** personal — esa es la feature estrella de Lakebase para dev/test.

# COMMAND ----------

print(f"Instancia (LAKEBASE_PROJECT): {LAKEBASE_PROJECT}")
print(f"Base de datos (LAKEBASE_DB) : {LAKEBASE_DB}")
print(f"Branch personal             : {LAKEBASE_BRANCH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — crear la instancia y conectar
# MAGIC Creamos la instancia si no existe (idempotente), esperamos a que esté **AVAILABLE**, generamos un **token OAuth**
# MAGIC y creamos la base `comfama`.

# COMMAND ----------

import psycopg2, uuid, time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()
EMAIL = w.current_user.me().user_name

# Crear la instancia si no existe (idempotente)
try:
    inst = w.database.get_database_instance(name=LAKEBASE_PROJECT)
    print(f"Instancia {LAKEBASE_PROJECT} ya existe (estado {inst.state}).")
except Exception:
    print(f"Creando instancia {LAKEBASE_PROJECT} (CU_1) ...")
    w.database.create_database_instance(DatabaseInstance(name=LAKEBASE_PROJECT, capacity="CU_1"))

# Esperar a AVAILABLE
for _ in range(40):
    inst = w.database.get_database_instance(name=LAKEBASE_PROJECT)
    if "AVAILABLE" in str(inst.state):
        break
    print("estado:", inst.state); time.sleep(15)

LAKEBASE_HOST = inst.read_write_dns
LAKEBASE_TOKEN = w.database.generate_database_credential(
    request_id=str(uuid.uuid4()), instance_names=[LAKEBASE_PROJECT]).token
print(f"✅ Instancia lista. host={LAKEBASE_HOST}")

# COMMAND ----------

# La instancia trae la base 'databricks_postgres'. Creamos 'comfama' si no existe.
admin = psycopg2.connect(host=LAKEBASE_HOST, port=5432, dbname="databricks_postgres",
                         user=EMAIL, password=LAKEBASE_TOKEN, sslmode="require")
admin.autocommit = True
with admin.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname=%s", (LAKEBASE_DB,))
    if not cur.fetchone():
        cur.execute(f"CREATE DATABASE {LAKEBASE_DB}")
        print(f"✅ Base '{LAKEBASE_DB}' creada")
    else:
        print(f"Base '{LAKEBASE_DB}' ya existe")
admin.close()

# COMMAND ----------

def get_conn(dbname=LAKEBASE_DB):
    """Conexión psycopg2 a Lakebase. Reusada por el agente (módulo 04)."""
    return psycopg2.connect(
        host=LAKEBASE_HOST, port=5432, dbname=dbname,
        user=EMAIL, password=LAKEBASE_TOKEN, sslmode="require",
    )

# Smoke test
with get_conn() as c, c.cursor() as cur:
    cur.execute("SELECT version();")
    print(cur.fetchone()[0])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🏗️ Esquema operacional
# MAGIC Creamos las tablas que el agente leerá/escribirá. `programas.cupos_disponibles` es el contador **mutable** en vivo.

# COMMAND ----------

DDL = """
CREATE TABLE IF NOT EXISTS afiliados (
    afiliado_id   INT PRIMARY KEY,
    nombre        TEXT NOT NULL,
    documento     TEXT,
    categoria     CHAR(1),
    email         TEXT,
    ciudad        TEXT
);
CREATE TABLE IF NOT EXISTS programas (
    programa_id        INT PRIMARY KEY,
    nombre             TEXT NOT NULL,
    categoria          TEXT,
    sede               TEXT,
    ciudad             TEXT,
    cupos_totales      INT,
    cupos_disponibles  INT,
    costo_afiliado     NUMERIC(12,2)
);
CREATE TABLE IF NOT EXISTS reservas (
    reserva_id    BIGSERIAL PRIMARY KEY,
    afiliado_id   INT REFERENCES afiliados(afiliado_id),
    programa_id   INT REFERENCES programas(programa_id),
    estado        TEXT DEFAULT 'confirmada',
    creada_en     TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS conversaciones (
    conversacion_id BIGSERIAL PRIMARY KEY,
    afiliado_id     INT,
    iniciada_en     TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS mensajes (
    mensaje_id      BIGSERIAL PRIMARY KEY,
    conversacion_id BIGINT REFERENCES conversaciones(conversacion_id),
    rol             TEXT,         -- 'user' | 'assistant'
    contenido       TEXT,
    creado_en       TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS casos (
    caso_id      BIGSERIAL PRIMARY KEY,
    afiliado_id  INT,
    asunto       TEXT,
    estado       TEXT DEFAULT 'abierto',
    creado_en    TIMESTAMPTZ DEFAULT now()
);
"""
with get_conn() as c, c.cursor() as cur:
    cur.execute(DDL)
    # Permitir que cualquier principal de Databricks (incl. el service principal del agente
    # servido en el módulo 05) lea/escriba estas tablas. Para demo; en prod se otorga por rol.
    cur.execute("GRANT ALL ON ALL TABLES IN SCHEMA public TO PUBLIC")
    cur.execute("GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO PUBLIC")
    cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO PUBLIC")
    cur.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO PUBLIC")
    c.commit()
print("✅ Esquema operacional creado (+ permisos para el SP del agente)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔄 Semilla: Delta → Lakebase
# MAGIC Cargamos `afiliados` y `programas` desde las tablas Delta del setup. (En producción esto sería una **synced
# MAGIC table** continua; ver la sección de sync más abajo.)

# COMMAND ----------

afiliados_pdf = spark.table(f"{CATALOG}.{SCHEMA}.afiliados").select(
    "afiliado_id","nombre","documento","categoria","email","ciudad").toPandas()
programas_pdf = spark.table(f"{CATALOG}.{SCHEMA}.programas").select(
    "programa_id","nombre","categoria","sede","ciudad","cupos_totales","cupos_disponibles","costo_afiliado").toPandas()

with get_conn() as c, c.cursor() as cur:
    cur.executemany(
        "INSERT INTO afiliados VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT (afiliado_id) DO NOTHING",
        list(afiliados_pdf.itertuples(index=False, name=None)))
    cur.executemany(
        "INSERT INTO programas VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (programa_id) DO NOTHING",
        list(programas_pdf.itertuples(index=False, name=None)))
    c.commit()
    cur.execute("SELECT count(*) FROM afiliados"); n_af = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM programas"); n_pr = cur.fetchone()[0]
print(f"✅ Sembrados: {n_af} afiliados, {n_pr} programas")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⭐ La transacción: `crear_reserva`
# MAGIC El corazón del caso de uso. Reservar un cupo debe ser **atómico**: verificar disponibilidad, insertar la reserva
# MAGIC y descontar el cupo, **todo o nada**, a prueba de concurrencia (`SELECT ... FOR UPDATE` bloquea la fila).
# MAGIC
# MAGIC Esta es exactamente la operación que **Delta no puede** hacer bien y por la que Lakebase existe en la arquitectura.

# COMMAND ----------

def crear_reserva(afiliado_id: int, programa_id: int) -> dict:
    """Reserva atómica de un cupo. Devuelve dict con resultado. Reusada como tool del agente (módulo 04)."""
    with get_conn() as c, c.cursor() as cur:
        try:
            # Bloquea la fila del programa para evitar sobre-reserva concurrente
            cur.execute("SELECT nombre, cupos_disponibles FROM programas WHERE programa_id = %s FOR UPDATE",
                        (programa_id,))
            row = cur.fetchone()
            if row is None:
                c.rollback(); return {"ok": False, "motivo": "programa_inexistente"}
            nombre, disponibles = row
            if disponibles <= 0:
                c.rollback(); return {"ok": False, "motivo": "sin_cupos", "programa": nombre}
            cur.execute("INSERT INTO reservas (afiliado_id, programa_id) VALUES (%s,%s) RETURNING reserva_id",
                        (afiliado_id, programa_id))
            reserva_id = cur.fetchone()[0]
            cur.execute("UPDATE programas SET cupos_disponibles = cupos_disponibles - 1 WHERE programa_id = %s",
                        (programa_id,))
            c.commit()
            return {"ok": True, "reserva_id": reserva_id, "programa": nombre, "cupos_restantes": disponibles - 1}
        except Exception as e:
            c.rollback(); return {"ok": False, "motivo": f"error: {e}"}

# Prueba: María (1001) reserva la Jornada de Salud Preventiva (5)
print(crear_reserva(1001, 5))
# Prueba: reservar el Diplomado Excel (3) que está SIN cupos → debe fallar limpio
print(crear_reserva(1002, 3))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Lecturas que usará el agente
# MAGIC Las otras dos tools son lecturas simples (baja latencia).

# COMMAND ----------

def consultar_disponibilidad(texto_programa: str) -> list:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT programa_id, nombre, sede, ciudad, cupos_disponibles, costo_afiliado
                       FROM programas WHERE nombre ILIKE %s AND cupos_disponibles > 0
                       ORDER BY cupos_disponibles DESC""", (f"%{texto_programa}%",))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

def consultar_beneficios(afiliado_id: int) -> list:
    with get_conn() as c, c.cursor() as cur:
        cur.execute("""SELECT r.reserva_id, p.nombre, r.estado, r.creada_en
                       FROM reservas r JOIN programas p ON p.programa_id = r.programa_id
                       WHERE r.afiliado_id = %s ORDER BY r.creada_en DESC""", (afiliado_id,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

print("Disponibilidad 'curso':", consultar_disponibilidad("curso"))
print("Beneficios de María (1001):", consultar_beneficios(1001))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🌿 Branching (dev/test) — feature estrella
# MAGIC
# MAGIC Cada asistente puede crear un **branch** copy-on-write de la base, instantáneo, para probar sin tocar producción.
# MAGIC
# MAGIC **🖱️ UI:** en la instancia → pestaña **Branches** → **Create branch** desde `production` → nombre `LAKEBASE_BRANCH`.
# MAGIC
# MAGIC **⌨️ Código (CLI):**
# MAGIC ```bash
# MAGIC databricks postgres create-branch projects/comfama-afiliados <tu-branch> \
# MAGIC   --json '{"spec": {"source_branch": "projects/comfama-afiliados/branches/production", "no_expiry": true}}'
# MAGIC ```
# MAGIC > Requiere CLI ≥ 0.285.0. El branch hereda los datos al instante (copy-on-write) y tiene su propio endpoint.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⇄ Sync con el lakehouse (concepto + ejemplo)
# MAGIC
# MAGIC - **Delta → Lakebase** (*synced table*, reverse-ETL): mantén `programas` curado y gobernado en UC/Delta y
# MAGIC   sincronízalo a Lakebase para servirlo con baja latencia.
# MAGIC - **Lakebase → UC**: registra la base como **database catalog** en UC para que `reservas`/`mensajes` se consulten
# MAGIC   con SQL en los módulos de Monitoreo y FinOps (Sesión 2).
# MAGIC
# MAGIC **🖱️ UI:** Catalog → *Create* → *Synced table* (o *Database catalog*).
# MAGIC **⌨️ CLI (ejemplo):**
# MAGIC ```bash
# MAGIC databricks database create-synced-database-table \
# MAGIC   ardemo_classic_dnubtw_catalog.ws_<usuario>.programas \
# MAGIC   --database-instance-name comfama-afiliados --logical-database-name comfama
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC Lakebase ya es el sistema operacional del agente: tablas, **transacción `crear_reserva`** y lecturas listas. En el
# MAGIC **módulo 04** estas tres funciones se convierten en las **tools** del agente.
# MAGIC
# MAGIC ### ▶️ Siguiente: `04 - Construir el Agente`

