# Databricks notebook source
# MAGIC %md
# MAGIC # 📚 Sesión 1 · 02 — Setup & Knowledge Base (Vector Search)
# MAGIC
# MAGIC **Meta del módulo:** convertir la base de conocimiento de Comfama (`kb_documentos`) en un **índice de Vector
# MAGIC Search** que el agente consultará vía **RAG**.
# MAGIC
# MAGIC Este es el **primer módulo dual-mode**: puedes hacerlo **🖱️ por la UI** (instrucciones paso a paso) o **⌨️
# MAGIC ejecutando las celdas**. Ambos caminos crean el mismo índice.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 📋 Lo que vamos a crear
# MAGIC 1. Un **Vector Search endpoint** (la "compute" que sirve búsquedas vectoriales) — compartido en el workspace.
# MAGIC 2. Un **índice Delta-Sync** sobre `kb_documentos`, con **embeddings managed** (el modelo los calcula solo).
# MAGIC 3. Una **prueba de búsqueda** semántica para validar.

# COMMAND ----------

# MAGIC %pip install -U databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

print("Vamos a crear / usar:")
print(f"  Endpoint Vector Search : {VS_ENDPOINT}")
print(f"  Tabla fuente           : {CATALOG}.{SCHEMA}.kb_documentos")
print(f"  Índice                 : {VS_INDEX}")
print(f"  Modelo de embeddings   : {EMBEDDING_MODEL}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Camino UI — crear el endpoint y el índice
# MAGIC
# MAGIC **A. Crear el endpoint de Vector Search**
# MAGIC 1. En el menú izquierdo: **Compute** → pestaña **Vector Search**.
# MAGIC 2. Clic en **Create** → nombre: el valor de `VS_ENDPOINT` (arriba) → tipo **Standard** → **Confirm**.
# MAGIC 3. Espera a que el estado pase a **Online** (1–2 min).
# MAGIC
# MAGIC **B. Crear el índice desde la tabla**
# MAGIC 1. Ve a **Catalog** → navega a `ardemo_classic_dnubtw_catalog` → tu schema `ws_<usuario>` → tabla **`kb_documentos`**.
# MAGIC 2. Botón **Create** (arriba a la derecha) → **Vector search index**.
# MAGIC 3. Configura:
# MAGIC    - **Name**: el valor de `VS_INDEX`.
# MAGIC    - **Endpoint**: el endpoint del paso A.
# MAGIC    - **Primary key**: `doc_id`.
# MAGIC    - **Embedding source**: columna **`contenido`** → *Compute embeddings* → modelo `EMBEDDING_MODEL`.
# MAGIC    - **Sync mode**: **Triggered**.
# MAGIC 4. **Create**. El índice quedará **Online** tras la primera sincronización.
# MAGIC
# MAGIC > 💡 La tabla `kb_documentos` ya tiene **Change Data Feed** habilitado (lo hizo el setup) — requisito del índice Delta-Sync.

# COMMAND ----------

# MAGIC %md
# MAGIC ## ⌨️ Camino código — lo mismo, por SDK
# MAGIC Si prefieres no usar la UI, ejecuta esta celda. Es **idempotente**: si el endpoint o el índice ya existen, los reutiliza.

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient

vsc = VectorSearchClient(disable_notice=True)

# --- A. Endpoint (crear si no existe) ---
existing_endpoints = [e["name"] for e in vsc.list_endpoints().get("endpoints", [])]
if VS_ENDPOINT not in existing_endpoints:
    print(f"Creando endpoint {VS_ENDPOINT} ...")
    vsc.create_endpoint_and_wait(name=VS_ENDPOINT, endpoint_type="STANDARD")
else:
    print(f"Endpoint {VS_ENDPOINT} ya existe ✅")

# --- B. Índice Delta-Sync con embeddings managed (crear si no existe) ---
def index_exists(endpoint, index_name):
    try:
        vsc.get_index(endpoint_name=endpoint, index_name=index_name).describe()
        return True
    except Exception:
        return False

if not index_exists(VS_ENDPOINT, VS_INDEX):
    print(f"Creando índice {VS_INDEX} ...")
    vsc.create_delta_sync_index_and_wait(
        endpoint_name=VS_ENDPOINT,
        index_name=VS_INDEX,
        source_table_name=f"{CATALOG}.{SCHEMA}.kb_documentos",
        pipeline_type="TRIGGERED",
        primary_key="doc_id",
        embedding_source_column="contenido",
        embedding_model_endpoint_name=EMBEDDING_MODEL,
    )
else:
    print(f"Índice {VS_INDEX} ya existe ✅ — disparando sync ...")
    vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX).sync()

print("Listo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🔎 Validar — búsqueda semántica
# MAGIC Probemos que el índice responde a una pregunta típica de un afiliado. Fíjate que la consulta no usa las mismas
# MAGIC palabras que el documento: el match es **semántico**.

# COMMAND ----------

index = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)

pregunta = "¿qué pasa si quiero echar para atrás una inscripción que ya pagué?"
resultados = index.similarity_search(
    query_text=pregunta,
    columns=["doc_id", "titulo", "categoria", "contenido"],
    num_results=3,
)

print(f"Pregunta: {pregunta}\n")
for row in resultados["result"]["data_array"]:
    doc_id, titulo, categoria, contenido = row[0], row[1], row[2], row[3]
    print(f"• [{categoria}] {titulo}")
    print(f"  {contenido[:160]}...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Resultado
# MAGIC La base de conocimiento de Comfama ya es consultable semánticamente. En el **módulo 04** el agente usará este
# MAGIC índice como su **retriever RAG**.
# MAGIC
# MAGIC > **Equivale a:** la capa de recuperación/embeddings que el framework de Comfama implementaría a mano. Aquí es
# MAGIC > un servicio managed, gobernado en Unity Catalog y sincronizado automáticamente con la tabla fuente.
# MAGIC
# MAGIC ### ▶️ Siguiente: `03 - Lakebase (datos del afiliado)` — la capa operacional OLTP del agente.

