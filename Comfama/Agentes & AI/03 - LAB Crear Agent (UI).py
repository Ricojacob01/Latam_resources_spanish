# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — LAB: Crear un Agent Bricks desde la UI 🧠
# MAGIC
# MAGIC **25 min.** Cada uno va a construir un **Agente de Facturas** que responde preguntas sobre 12 PDFs de invoices.
# MAGIC
# MAGIC Es 100% no-code — Agent Bricks hace toda la ingesta, parsing de PDFs, embedding, vector search y deployment **por ti**.
# MAGIC
# MAGIC ⚠️ **Importante:** el agente que creen aquí lo vamos a reutilizar en la **sesión de Apps** (donde lo conectamos al chatbot). Anoten el nombre exacto del agente.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lo que vamos a construir
# MAGIC
# MAGIC Un **Knowledge Assistant** que responde preguntas sobre facturas (sample data de Superstore). Ejemplos de preguntas que va a contestar:
# MAGIC
# MAGIC - *"¿Cuánto compró Aaron Bergman en total?"*
# MAGIC - *"¿Qué productos llevó Brendan Dodson?"*
# MAGIC - *"Lista todas las facturas de Alyssa Tate"*
# MAGIC - *"¿Cuál fue la factura más grande?"*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Setup — paths y schema

# COMMAND ----------

import re
CURRENT_USER = spark.sql("SELECT current_user() AS u").collect()[0]["u"]
SLUG = re.sub(r"[^a-z0-9]+", "_", CURRENT_USER.split("@")[0].lower()).strip("_")[:25]

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
VOLUME = f"invoices_{SLUG}"
VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

AGENT_NAME = f"agente_facturas_{SLUG}"

# Source PDFs (compartidos por todos)
SOURCE_FOLDER = "/Workspace/Users/rico.martinez@databricks.com/Latam_resources_spanish/AI_BI_supply_chain_Demo_pdf_invoice/Images_pdf_invoice"

print(f"Tu volumen UC:   {VOLUME_PATH}")
print(f"Tu agente:       {AGENT_NAME}")
print(f"PDFs source:     {SOURCE_FOLDER}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Crear tu propio Volume UC

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
print(f"✓ Volume creado: {VOLUME_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Listar los 12 PDFs source + previewar

# COMMAND ----------

import os

pdfs = sorted([f for f in os.listdir(SOURCE_FOLDER) if f.lower().endswith(".pdf")])
print(f"Total PDFs disponibles: {len(pdfs)}\n")

import pandas as pd
df = pd.DataFrame([{
    "archivo": p,
    "tamaño_kb": round(os.path.getsize(f"{SOURCE_FOLDER}/{p}") / 1024, 1),
} for p in pdfs])
display(spark.createDataFrame(df))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preview de una factura (embedded PDF)

# COMMAND ----------

import base64

sample_pdf = pdfs[0]
sample_path = f"{SOURCE_FOLDER}/{sample_pdf}"

with open(sample_path, "rb") as f:
    pdf_b64 = base64.b64encode(f.read()).decode()

displayHTML(f'''
<div style="margin:8px 0">
  <div style="font-weight:600;color:#1B3037;margin-bottom:8px">📄 Preview: {sample_pdf}</div>
  <embed src="data:application/pdf;base64,{pdf_b64}"
         type="application/pdf"
         width="100%" height="500"
         style="border:1px solid #ddd;border-radius:6px"/>
</div>
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Copiar los PDFs a TU Volume

# COMMAND ----------

import shutil

copied = 0
for pdf in pdfs:
    src = f"{SOURCE_FOLDER}/{pdf}"
    dst = f"{VOLUME_PATH}/{pdf}"
    try:
        shutil.copy(src, dst)
        copied += 1
    except Exception as e:
        print(f"  ⚠ {pdf}: {e}")

print(f"✓ {copied}/{len(pdfs)} PDFs copiados a {VOLUME_PATH}")

# Verificar
import os
files_in_volume = sorted([f for f in os.listdir(VOLUME_PATH) if f.lower().endswith(".pdf")])
print(f"\nArchivos en tu Volume:")
for f in files_in_volume:
    size_kb = round(os.path.getsize(f"{VOLUME_PATH}/{f}") / 1024, 1)
    print(f"  📄 {f}  ({size_kb} KB)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Crear el Knowledge Assistant en la UI
# MAGIC
# MAGIC Ahora viene la parte 100% UI. **Sigue los pasos exactos.**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 — Abrir Agent Bricks
# MAGIC
# MAGIC 1. **Sidebar izquierdo** → click **AI/ML** (o **Agents** según versión del workspace)
# MAGIC 2. En la sección **Agent Bricks**, click el botón **+ Build agent**
# MAGIC 3. Te aparece la pantalla con templates de agentes:
# MAGIC    - 📚 Knowledge Assistant ← **este vamos a usar**
# MAGIC    - 🔧 Information Extraction
# MAGIC    - 🤖 Multi-agent Supervisor
# MAGIC    - 💬 Chat Bot
# MAGIC
# MAGIC 4. Click **Knowledge Assistant**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 — Configurar el agente
# MAGIC
# MAGIC En la pantalla de configuración:
# MAGIC
# MAGIC | Campo | Qué poner |
# MAGIC |---|---|
# MAGIC | **Agent name** | Pega el valor de la celda siguiente (es único para ti) |
# MAGIC | **Description** | `Asistente de facturas — responde preguntas sobre invoices del demo Superstore` |
# MAGIC | **Knowledge sources** | Click **+ Add source** → tipo **Volume** → navega a `ardemo_classic_dnubtw_catalog` → `comfama` → tu volumen `invoices_<tu_slug>` |
# MAGIC | **LLM** | Deja el default (Llama 3.3 70B o Claude). Si quieres cambiarlo: dropdown → seleccionar |
# MAGIC | **Embedding model** | Default (`databricks-gte-large-en`) |
# MAGIC | **Output guidelines** | _(opcional)_ "Responde en español formal. Si la información no está en los documentos, di claramente 'No tengo esa información'." |

# COMMAND ----------

print(f"📋 Copia y pega esto en el campo 'Agent name':\n")
print(f"   {AGENT_NAME}")
print(f"\n📋 En 'Knowledge sources' navega a tu volumen:")
print(f"   ardemo_classic_dnubtw_catalog → comfama → {VOLUME}")
print(f"\n   (Path completo: {VOLUME_PATH})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 — Lanzar la creación
# MAGIC
# MAGIC 1. Click **Create agent** (botón abajo a la derecha)
# MAGIC 2. **Espera 3-8 min** mientras Databricks:
# MAGIC    - Parsea cada PDF con `ai_parse_document` (extrae texto)
# MAGIC    - Chunkea el texto
# MAGIC    - Genera embeddings con `databricks-gte-large-en`
# MAGIC    - Crea un Vector Search index
# MAGIC    - Configura el agente con retrieval + LLM
# MAGIC    - Deploya un endpoint de Model Serving
# MAGIC
# MAGIC 3. Verás el estado **Indexing → Deploying → Ready**

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 — Probar tu agente
# MAGIC
# MAGIC Cuando el estado sea **Ready**:
# MAGIC
# MAGIC 1. En la página de tu agente, click la tab **Chat** o **Playground**
# MAGIC 2. Manda estas preguntas (una por una):

# COMMAND ----------

# MAGIC %md
# MAGIC #### Prompts sugeridos
# MAGIC
# MAGIC ```
# MAGIC 1. ¿Cuántas facturas hay en mi base de conocimiento?
# MAGIC 2. ¿Quién es Aaron Bergman y qué compró?
# MAGIC 3. Lista todas las facturas de Alyssa Tate con sus montos
# MAGIC 4. ¿Cuál es la factura con el monto más alto?
# MAGIC 5. ¿Qué productos llevó Brendan Dodson?
# MAGIC 6. Compara las compras de Aaron Bergman y Brendan Murry
# MAGIC 7. Dame el total facturado a Muhammed Yedwab
# MAGIC ```
# MAGIC
# MAGIC Después de cada respuesta, **revisa las citas** (Sources). Agent Bricks muestra exactamente de qué PDF sacó la información.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 — Tu turno (5 min)
# MAGIC
# MAGIC - Manda 3 preguntas de tu propia inventiva
# MAGIC - Intenta romperlo: pregunta algo que **no esté** en los invoices y ve qué responde
# MAGIC - Click en **Evaluation** (tab en la UI del agente) → Agent Bricks tiene evals automáticos

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: ¿Qué creó Databricks por ti? (5 min)
# MAGIC
# MAGIC Vamos a inspeccionar los assets que Agent Bricks generó automáticamente.

# COMMAND ----------

# Verificar el endpoint del agente
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()

# Buscar endpoints relacionados con tu agente
print(f"Buscando endpoints que contengan '{SLUG}'...")
matches = []
for e in w.serving_endpoints.list():
    if SLUG in (e.name or ""):
        matches.append(e.name)

if matches:
    for name in matches:
        print(f"  ✓ {name}")
else:
    print("  (sin matches todavía — espera a que el deploy termine)")

# COMMAND ----------

# Vector Search index que creó Agent Bricks
try:
    indexes = w.api_client.do("GET", "/api/2.0/vector-search/indexes")
    print("Vector Search indexes existentes:")
    for idx in indexes.get("vector_indexes", []):
        if SLUG in (idx.get("name", "") or ""):
            print(f"  ✓ {idx.get('name')}")
except Exception as e:
    print(f"VS API: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Guarda el nombre del agente para la sesión de Apps

# COMMAND ----------

displayHTML(f'''
<div style="padding:20px;background:#FFF4E5;border:2px solid #FFAB00;border-radius:10px">
  <h3 style="margin:0 0 12px 0;color:#1B3037">📌 Anota esto — lo necesitas en la sesión de Apps</h3>
  <div style="font-family:monospace;font-size:16px;background:#fff;padding:12px;border-radius:6px;margin:8px 0">
    Agent name: <b>{AGENT_NAME}</b>
  </div>
  <div style="font-size:13px;color:#618793;margin-top:10px">
    En la sesión de Apps vamos a conectar el chatbot que creaste a este agente — el chatbot dejará de hablar con Llama directo y empezará a responder con conocimiento de las facturas.
  </div>
</div>
''')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup (opcional, después de la sesión)
# MAGIC
# MAGIC ⚠️ **No borres el agente hoy** — lo vamos a reusar en la sesión de Apps.
# MAGIC
# MAGIC Cuando hayas terminado todas las sesiones:
# MAGIC
# MAGIC ```python
# MAGIC # Borrar el endpoint del agente
# MAGIC # w.api_client.do("DELETE", f"/api/2.0/serving-endpoints/{AGENT_NAME}")
# MAGIC
# MAGIC # Borrar el volume con los PDFs
# MAGIC # spark.sql(f"DROP VOLUME IF EXISTS {CATALOG}.{SCHEMA}.{VOLUME}")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC ✅ Crearon un Volume UC personal con 12 PDFs
# MAGIC ✅ Construyeron un Knowledge Assistant con Agent Bricks (sin código)
# MAGIC ✅ Agent Bricks generó automáticamente: PDF parsing + embedding + Vector Search index + LLM endpoint
# MAGIC ✅ Probaron el agente con preguntas reales
# MAGIC ✅ Vieron las citas (Sources) en cada respuesta
# MAGIC ✅ **El agente queda corriendo** — lo usaremos en la sesión de Apps
# MAGIC
# MAGIC ## Lo que **no** hicieron (workshop deep-dive del fin de mes)
# MAGIC
# MAGIC - Custom agent code (Mosaic AI Agent Framework con LangChain/DSPy)
# MAGIC - Multi-agent supervisor (un agente que orquesta a otros)
# MAGIC - Agent Evaluation con judge models custom
# MAGIC - Conectar el agente a tools externos vía MCP
# MAGIC - Versioning + canary deployment del agente
# MAGIC - Inference Tables + Lakehouse Monitoring sobre el agente
# MAGIC
# MAGIC ## Continuar → `04 - Cierre y Workshop Preview`

