# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — LAB Express: usar TU instancia Lakebase 🧪
# MAGIC
# MAGIC **25 min.** Vamos a:
# MAGIC 1. Conectar con psycopg estándar (sin clientes custom)
# MAGIC 2. Crear schema "sesiones de agente" (lo que hoy Comfama tiene en Cosmos)
# MAGIC 3. Hacer queries OLTP con JSONB
# MAGIC 4. **Branching en vivo** — clonar la base instantáneamente
# MAGIC 5. Ver cambios en cada branch independientes
# MAGIC
# MAGIC ⚠️ Necesitas haber completado `01 - Product Tour` que crea tu instancia.

# COMMAND ----------

# MAGIC %pip install -q psycopg[binary]>=3.1 databricks-sdk>=0.30.0
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recuperar tu instancia

# COMMAND ----------

import re
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLUG = re.sub(r"[^a-z0-9]+", "-", CURRENT_USER.split("@")[0].lower()).strip("-")[:25]
INSTANCE_NAME = f"lakebase-{SLUG}"

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

inst = w.api_client.do("GET", f"/api/2.0/database/instances/{INSTANCE_NAME}")
print(f"Instance:      {inst.get('name')}")
print(f"State:         {inst.get('state')}")
print(f"Host:          {inst.get('read_write_dns')}")

HOST = inst.get("read_write_dns")
PORT = 5432
DB = "databricks_postgres"
USER = CURRENT_USER

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte A — Conectar con psycopg estándar (3 min)

# COMMAND ----------

import psycopg

def get_token():
    """OAuth token fresh — los tokens duran ~1h, así que es bueno refrescar."""
    headers = w.config.authenticate()
    return headers.get("Authorization", "").split(" ", 1)[1]

def conn_str(host=HOST, db=DB):
    token = get_token()
    return f"host={host} port={PORT} dbname={db} user={USER} password={token} sslmode=require"

with psycopg.connect(conn_str()) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT current_user, current_database(), version()")
        row = cur.fetchone()
        print(f"✓ Conectado como {row[0]}")
        print(f"  database: {row[1]}")
        print(f"  version:  {row[2][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte B — Schema "sesiones de agente" (5 min)
# MAGIC
# MAGIC Vamos a crear lo que hoy Comfama mantiene en Cosmos DB: tabla para sesiones conversacionales de su agente. JSONB para el state, índices para queries comunes.

# COMMAND ----------

with psycopg.connect(conn_str()) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sesiones_agente (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                conversation_id TEXT UNIQUE NOT NULL,
                started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_message_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                turn_count INT DEFAULT 0,
                state JSONB NOT NULL DEFAULT '{}',
                metadata JSONB DEFAULT '{}'
            );
            CREATE INDEX IF NOT EXISTS idx_sesiones_user ON sesiones_agente(user_id);
            CREATE INDEX IF NOT EXISTS idx_sesiones_state_gin ON sesiones_agente USING GIN (state);
        """)
        conn.commit()
        print("✓ Tabla sesiones_agente creada (con índices)")

# COMMAND ----------

# Insertar data de muestra
import json
import random

SAMPLE_SESSIONS = [
    ("user_001", "conv_001", {"last_intent": "subsidio_vivienda", "language": "es", "channel": "app"}),
    ("user_001", "conv_002", {"last_intent": "estado_solicitud", "language": "es", "channel": "web"}),
    ("user_002", "conv_003", {"last_intent": "credito", "language": "es", "channel": "whatsapp"}),
    ("user_003", "conv_004", {"last_intent": "subsidio_escolar", "language": "es", "channel": "app"}),
    ("user_002", "conv_005", {"last_intent": "queja", "language": "es", "channel": "web"}),
]

with psycopg.connect(conn_str()) as conn:
    with conn.cursor() as cur:
        for uid, conv, state in SAMPLE_SESSIONS:
            cur.execute("""
                INSERT INTO sesiones_agente (user_id, conversation_id, turn_count, state)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (conversation_id) DO NOTHING
            """, (uid, conv, random.randint(1, 8), json.dumps(state)))
        conn.commit()

# Leer back
with psycopg.connect(conn_str()) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id, conversation_id, turn_count,
                   state->>'last_intent' AS intent,
                   state->>'channel' AS channel
            FROM sesiones_agente
            ORDER BY started_at DESC
        """)
        print(f"{'user':10s} {'conversation':14s} {'turns':6s} {'intent':22s} {'channel':10s}")
        print("-" * 70)
        for row in cur.fetchall():
            print(f"{row[0]:10s} {row[1]:14s} {row[2]:6d} {row[3]:22s} {row[4]:10s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query JSONB típica — filtrar por intent

# COMMAND ----------

with psycopg.connect(conn_str()) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT user_id, conversation_id, state->>'channel' AS channel
            FROM sesiones_agente
            WHERE state @> '{"language": "es"}'
              AND state->>'channel' IN ('app', 'web')
            ORDER BY started_at DESC
        """)
        print("Conversaciones en español por app/web:")
        for row in cur.fetchall():
            print(f"  {row}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 🎯 Punto clave
# MAGIC
# MAGIC Es **Postgres estándar**. JSONB para semi-structured (perfecto para state de agente), índices GIN para queries rápidas. Cualquier ORM (SQLAlchemy, Prisma) o cliente (pgAdmin, DBeaver) funciona. **Sin clientes custom como el `CosmosDBClient` de su framework.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte C — Branching en vivo 🌿 (10 min)
# MAGIC
# MAGIC La feature killer. Vamos a:
# MAGIC 1. Crear un branch `dev` desde tu instancia main
# MAGIC 2. Insertar data SOLO en el branch
# MAGIC 3. Confirmar que el main no la tiene
# MAGIC 4. Ese es el patrón: **dev/test isolation con git-like workflow**

# COMMAND ----------

# Step 1: Crear un branch
BRANCH_NAME = f"{INSTANCE_NAME}-dev"

try:
    existing_branch = w.api_client.do("GET", f"/api/2.0/database/instances/{BRANCH_NAME}")
    print(f"Branch ya existe: {existing_branch.get('name')}")
    branch_exists = True
except Exception:
    branch_exists = False

if not branch_exists:
    print(f"Creando branch {BRANCH_NAME} desde {INSTANCE_NAME}...")
    branch_body = {
        "name": BRANCH_NAME,
        "capacity": "CU_1",
        "parent_instance_ref": {"name": INSTANCE_NAME},
    }
    branch = w.api_client.do("POST", "/api/2.0/database/instances", body=branch_body)
    print(f"✓ Branch creado")
    print(f"  El storage es copy-on-write — instantáneo y barato")

# Esperar AVAILABLE
import time
for i in range(20):
    b = w.api_client.do("GET", f"/api/2.0/database/instances/{BRANCH_NAME}")
    state = b.get("state", "?")
    print(f"  [{i+1:02d}] state={state}")
    if str(state).upper() == "AVAILABLE":
        print("✅ Branch ready")
        break
    time.sleep(15)

# COMMAND ----------

# Step 2: Conectar al branch y verificar que tiene la data del parent
branch = w.api_client.do("GET", f"/api/2.0/database/instances/{BRANCH_NAME}")
BRANCH_HOST = branch.get("read_write_dns")

with psycopg.connect(conn_str(host=BRANCH_HOST)) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM sesiones_agente")
        n = cur.fetchone()[0]
        print(f"Branch tiene {n} filas (copiadas del parent al instante)")

# COMMAND ----------

# Step 3: Insertar EN EL BRANCH una sesión que NO existe en main
with psycopg.connect(conn_str(host=BRANCH_HOST)) as conn:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO sesiones_agente (user_id, conversation_id, turn_count, state)
            VALUES (%s, %s, %s, %s)
        """, ("user_branch_test", "conv_branch_only", 99, json.dumps({"last_intent": "TEST_BRANCH_ONLY", "channel": "branch"})))
        conn.commit()
        print("✓ Insertada sesión 'conv_branch_only' SOLO en el branch dev")

# COMMAND ----------

# Step 4: Comparar — esa sesión NO existe en main
print("=== MAIN ===")
with psycopg.connect(conn_str()) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT conversation_id, state->>'last_intent' FROM sesiones_agente WHERE conversation_id = 'conv_branch_only'")
        rows = cur.fetchall()
        print(f"  filas con conv_branch_only: {len(rows)}")

print("\n=== BRANCH (dev) ===")
with psycopg.connect(conn_str(host=BRANCH_HOST)) as conn:
    with conn.cursor() as cur:
        cur.execute("SELECT conversation_id, state->>'last_intent' FROM sesiones_agente WHERE conversation_id = 'conv_branch_only'")
        rows = cur.fetchall()
        print(f"  filas con conv_branch_only: {len(rows)}")
        for row in rows:
            print(f"    {row}")

print("\n🎯 El branch tiene su propia data; el main NO se vio afectado.")
print("   Use case real: dev/QA isolation, PR previews, migration testing")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte D — Ver tu instancia + branch en la UI (3 min)
# MAGIC
# MAGIC 1. **Sidebar izquierdo** → **Compute** → tab **Database instances**
# MAGIC 2. Verás:
# MAGIC    - `lakebase-<tu-username>` (main)
# MAGIC    - `lakebase-<tu-username>-dev` (branch)
# MAGIC 3. Click en el main → tab **Branches** → debe listar el dev branch con su parent_instance_lsn (el "commit" del cual brancheó)
# MAGIC 4. Cada branch puede tener su propio compute (scale-to-zero independiente)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte E — Cost tracking (2 min)

# COMMAND ----------

# Cost en system tables
display(spark.sql("""
SELECT
  date(usage_start_time) AS dia,
  sku_name,
  ROUND(SUM(usage_quantity), 4) AS dbus
FROM system.billing.usage
WHERE (sku_name LIKE '%LAKEBASE%' OR sku_name LIKE '%DATABASE%')
  AND usage_start_time >= current_date() - INTERVAL 7 DAYS
GROUP BY dia, sku_name
ORDER BY dia DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (opcional)
# MAGIC
# MAGIC Si **no** vas a usar tus instancias después de la sesión:

# COMMAND ----------

# Borrar el branch primero (el branch depende del parent)
# w.api_client.do("DELETE", f"/api/2.0/database/instances/{BRANCH_NAME}")

# Luego borrar el main
# w.api_client.do("DELETE", f"/api/2.0/database/instances/{INSTANCE_NAME}")

print("Para cleanup, descomenta las líneas arriba.")
print("Si no, el scale-to-zero automático apaga el compute pero el storage sigue (cost mínimo).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Crearon su propia instancia Lakebase (notebook 01)
# MAGIC ✅ Se conectaron con `psycopg` estándar
# MAGIC ✅ Crearon una tabla `sesiones_agente` con JSONB + índices GIN
# MAGIC ✅ Insertaron datos + queries JSONB filtrando por campo
# MAGIC ✅ **Crearon un branch dev** desde su instancia main
# MAGIC ✅ Verificaron isolation: insert en branch no afecta main
# MAGIC ✅ Vieron el cost tracking en System Tables
# MAGIC
# MAGIC ## Lo que **no** hicieron (workshop deep-dive del fin de mes)
# MAGIC
# MAGIC - Sync bi-direccional Lakebase ↔ Delta (federation en UC)
# MAGIC - Registrar Lakebase en UC para que aparezca en Catalog Explorer
# MAGIC - Snapshots manuales + point-in-time restore
# MAGIC - Read replicas para alta concurrencia
# MAGIC - Conectar Lakebase al agente Comfama (reemplazar Cosmos)
# MAGIC - Power BI / Fabric leyendo de Lakebase directamente
# MAGIC - Migración de datos desde Postgres on-prem a Lakebase
# MAGIC
# MAGIC ## Continuar → `03 - Cierre y Workshop Preview`

