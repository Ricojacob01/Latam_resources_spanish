# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Product Tour + Crear TU instancia Lakebase 📊 🚀
# MAGIC
# MAGIC ~25 min. Primero slides oficiales del Lakebase deck. Al final, **cada uno va a crear su propia instancia Lakebase** — que usamos en notebook 02.

# COMMAND ----------

import os, base64
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLIDES_PATH = f"/Workspace/Users/{CURRENT_USER}/Latam_resources_spanish/Comfama/Lakebase/imagenes"

def show_slide(filename, width=1100, caption=""):
    full_path = f"{SLIDES_PATH}/{filename}"
    try:
        with open(full_path, "rb") as f:
            b64 = base64.b64encode(f.read()).decode()
        html = f'<div style="margin:8px 0"><img src="data:image/png;base64,{b64}" style="max-width:{width}px;width:100%;border:1px solid #ddd;border-radius:6px"/>'
        if caption:
            html += f'<div style="font-size:13px;color:#666;font-style:italic;margin-top:6px">{caption}</div>'
        html += "</div>"
        displayHTML(html)
    except FileNotFoundError:
        displayHTML(f'<div style="padding:20px;background:#fee;border:1px solid #fcc">Slide no encontrado: {full_path}</div>')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1 — Por qué Lakebase

# COMMAND ----------

show_slide("01_lakebase_intro.png", caption="Introducción a Lakebase")

# COMMAND ----------

show_slide("02_apps_democratizan.png", caption="Las aplicaciones democratizan la inteligencia — pero necesitan storage operacional")

# COMMAND ----------

show_slide("03_apps_evolucion.png", caption="Las apps evolucionaron a stateless/serverless. Las DBs siguen monolíticas e imprácticas para esto.")

# COMMAND ----------

show_slide("04_problemas_tradicionales.png",
           caption="Problemas con DBs tradicionales: costosas, complicadas, lock-in con proveedor, agravadas por agentes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2 — La arquitectura de Lakebase

# COMMAND ----------

show_slide("05_separa_computo.png", caption="Separa cómputo y almacenamiento — Postgres completamente administrado y serverless")

# COMMAND ----------

show_slide("06_menor_tco.png", caption="Menor TCO + plataforma integrada con governance centralizada")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3 — Los 5 superpoderes

# COMMAND ----------

show_slide("07_branching.png",
           caption="🌿 Branching — clona tu base instantáneamente para dev/test/QA. Trata la DB como código.")

# COMMAND ----------

show_slide("08_snapshots.png",
           caption="📸 Snapshots programados — backups instantáneos + restauración a punto en el tiempo")

# COMMAND ----------

show_slide("09_autoescalamiento.png",
           caption="📈 Autoescalamiento — dimensionamiento dinámico de cómputo, sin ajustes manuales")

# COMMAND ----------

show_slide("10_scale_to_zero.png",
           caption="💰 Scale-to-zero — solo pagas cuando hay actividad. Inicio rápido al volver.")

# COMMAND ----------

show_slide("11_escalabilidad_lectura.png",
           caption="📚 Aislamiento de lectura — cómputo dedicado de solo-lectura para alta concurrencia sin complejidad")

# COMMAND ----------

show_slide("12_integrado_dip.png",
           caption="🔗 Integrado en la Data Intelligence Platform — sin stacks separados ni modelos de governance dobles")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4 — Use cases

# COMMAND ----------

show_slide("13_que_puedes_hacer.png",
           caption="¿Qué puedes hacer? Historial de pedidos/chat, analítica sobre lakehouse, construir apps")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 5 — Tracción + caso de éxito

# COMMAND ----------

show_slide("14_clientes_2000.png", caption="2000+ clientes — ingresos creciendo al doble del ritmo de DBSQL")

# COMMAND ----------

show_slide("15_hafnia.png", caption="Hafnia — meses de esfuerzo se convirtieron en minutos con Lakebase")

# COMMAND ----------

show_slide("16_comienza.png", caption="GA en AWS y Azure — databricks.com/product/lakebase")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comparación con su Cosmos DB actual
# MAGIC
# MAGIC | Capacidad | Cosmos DB | Lakebase |
# MAGIC |---|---|---|
# MAGIC | Modelo | NoSQL multi-API | Postgres estándar |
# MAGIC | Costo cuando idle | RU/s siempre on | Scale-to-zero |
# MAGIC | Backup / point-in-time restore | Configurable | Nativo, sin setup |
# MAGIC | Branching | ❌ | ✅ (clone instantáneo) |
# MAGIC | Sync con Delta | ETL custom | Sync bi-direccional nativo |
# MAGIC | Governance | Azure RBAC | Unity Catalog |
# MAGIC | Cómputo separado de storage | Sí | Sí |
# MAGIC | SDK | Cosmos SDK propio | psycopg / cualquier driver Postgres |

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚀 Hands-on: cada uno crea SU propia instancia
# MAGIC
# MAGIC Suficiente teoría. Cada uno va a:
# MAGIC
# MAGIC 1. Crear una instancia Lakebase con su nombre
# MAGIC 2. Esperar a que esté `AVAILABLE` (1-3 min)
# MAGIC 3. Anotar el connection string
# MAGIC
# MAGIC En el siguiente notebook nos conectamos y la usamos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Definir tu instancia

# COMMAND ----------

import re
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLUG = re.sub(r"[^a-z0-9]+", "-", CURRENT_USER.split("@")[0].lower()).strip("-")[:25]

INSTANCE_NAME = f"lakebase-{SLUG}"
CAPACITY = "CU_1"  # más pequeño, ideal para demo

print(f"Instance name:  {INSTANCE_NAME}")
print(f"Capacity:       {CAPACITY}")
print(f"Owner:          {CURRENT_USER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Crear la instancia

# COMMAND ----------

from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Verificar si ya existe
try:
    existing = w.api_client.do("GET", f"/api/2.0/database/instances/{INSTANCE_NAME}")
    print(f"Instancia ya existe: {existing.get('name')}")
    print(f"  state: {existing.get('state')}")
    print(f"  capacity: {existing.get('capacity')}")
    instance_exists = True
except Exception:
    instance_exists = False

if not instance_exists:
    create_body = {
        "name": INSTANCE_NAME,
        "capacity": CAPACITY,
    }
    created = w.api_client.do("POST", "/api/2.0/database/instances", body=create_body)
    print(f"✓ Instancia creada: {created.get('name')}")
    print(f"  state inicial: {created.get('state')}")
    print(f"  Esperando AVAILABLE (1-3 min)...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Esperar a AVAILABLE

# COMMAND ----------

import time

for i in range(30):
    inst = w.api_client.do("GET", f"/api/2.0/database/instances/{INSTANCE_NAME}")
    state = inst.get("state", "?")
    print(f"  [{i+1:02d}] state={state}")
    if str(state).upper() == "AVAILABLE":
        print("✅ Lista")
        break
    if str(state).upper() in ("FAILING_OVER", "FAILED", "STOPPED"):
        print(f"⚠️ Estado inesperado: {state}")
        break
    time.sleep(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Detalles de TU instancia

# COMMAND ----------

inst = w.api_client.do("GET", f"/api/2.0/database/instances/{INSTANCE_NAME}")

displayHTML(f'''
<div style="padding:20px;background:#E8F2F4;border:2px solid #1B5161;border-radius:10px">
  <h2 style="margin:0 0 16px 0;color:#1B3037">🐘 Tu Lakebase está corriendo</h2>
  <table style="font-family:monospace;font-size:14px;color:#1B3037">
    <tr><td style="padding:4px 12px 4px 0"><b>Name:</b></td><td>{inst.get('name')}</td></tr>
    <tr><td style="padding:4px 12px 4px 0"><b>State:</b></td><td>{inst.get('state')}</td></tr>
    <tr><td style="padding:4px 12px 4px 0"><b>Capacity:</b></td><td>{inst.get('capacity')}</td></tr>
    <tr><td style="padding:4px 12px 4px 0"><b>PG version:</b></td><td>{inst.get('pg_version')}</td></tr>
    <tr><td style="padding:4px 12px 4px 0"><b>Read/Write DNS:</b></td><td>{inst.get('read_write_dns')}</td></tr>
  </table>
</div>
''')

print(f"\nConnection details (psycopg style):")
print(f"  host:     {inst.get('read_write_dns')}")
print(f"  port:     5432")
print(f"  dbname:   databricks_postgres")
print(f"  user:     {CURRENT_USER}")
print(f"  password: <OAuth token>")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verlo en UI
# MAGIC
# MAGIC ### Pasos
# MAGIC
# MAGIC 1. **Sidebar izquierdo** → **Compute** → tab **Database instances**
# MAGIC 2. Click en tu instancia (`lakebase-<tu-username>`)
# MAGIC 3. Ve las tabs:
# MAGIC    - **Overview** — endpoint, estado, capacity
# MAGIC    - **Catalog** — cuando la registres en UC, aparecen las tablas Postgres ahí
# MAGIC    - **Branches** — donde aparecerán los branches que hagamos
# MAGIC    - **Backups** — snapshots automáticos
# MAGIC    - **Monitoring** — CPU, RAM, conexiones, queries

# COMMAND ----------

# MAGIC %md
# MAGIC ## Variable para el siguiente notebook

# COMMAND ----------

dbutils.jobs.taskValues.set(key="instance_name", value=INSTANCE_NAME)
print(f"✓ Instance name guardada para notebook 02: {INSTANCE_NAME}")
print(f"✓ En notebook 02 vamos a: conectar con psycopg + crear tabla + branching")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Continuar → `02 - LAB Express` (usar TU instancia)

