# Databricks notebook source
# MAGIC %md
# MAGIC # 🤖 Sesión 1 · 04 — Construir el Agente
# MAGIC
# MAGIC **Meta:** ensamblar el **Agente de Servicios al Afiliado** — tool-calling con **RAG** (índice del módulo 02) +
# MAGIC **3 tools** contra Lakebase (módulo 03): `consultar_beneficios`, `consultar_disponibilidad`, `crear_reserva`.
# MAGIC
# MAGIC > **Equivale a: `TemplateAgentes`** — la orquestación la da el **Mosaic AI Agent Framework** (`ChatAgent` de MLflow).
# MAGIC >
# MAGIC > Construir un agente con tools custom es **código**; la **UI** entra para probarlo (AI Playground) e inspeccionar
# MAGIC > el modelo en Catalog. Usamos el patrón **models-from-code** (`agent.py` + `set_model`), que es el correcto para
# MAGIC > servir (el módulo 05 lo despliega tal cual).

# COMMAND ----------

# MAGIC %pip install -U mlflow databricks-agents databricks-vectorsearch openai psycopg2-binary databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run ../_resources/00-setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Escribir el agente como código (`agent.py`)
# MAGIC Generamos el archivo del agente con la config de tu workspace ya incrustada. Define el RAG, las 3 tools de
# MAGIC Lakebase y la clase `ChatAgent`, y lo registra con `set_model`.

# COMMAND ----------

AGENT_HEADER = f"""# agent.py — generado por el notebook 04
CATALOG = {CATALOG!r}
SCHEMA = {SCHEMA!r}
VS_ENDPOINT = {VS_ENDPOINT!r}
VS_INDEX = {VS_INDEX!r}
LLM_ENDPOINT = {LLM_ENDPOINT!r}
LAKEBASE_PROJECT = {LAKEBASE_PROJECT!r}
LAKEBASE_DB = {LAKEBASE_DB!r}
"""

AGENT_BODY = r'''
import json, uuid
import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from mlflow.deployments import get_deploy_client
import mlflow
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentMessage, ChatAgentResponse

_w = WorkspaceClient()
_EMAIL = _w.current_user.me().user_name
_HOST = _w.database.get_database_instance(name=LAKEBASE_PROJECT).read_write_dns
_vsc = VectorSearchClient(disable_notice=True)
_index = _vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=VS_INDEX)
_llm = get_deploy_client("databricks")

def _conn():
    tok = _w.database.generate_database_credential(
        request_id=str(uuid.uuid4()), instance_names=[LAKEBASE_PROJECT]).token
    return psycopg2.connect(host=_HOST, port=5432, dbname=LAKEBASE_DB,
                            user=_EMAIL, password=tok, sslmode="require")

def buscar_conocimiento(pregunta: str) -> str:
    r = _index.similarity_search(query_text=pregunta,
                                 columns=["titulo", "contenido", "url"], num_results=3)
    docs = r["result"]["data_array"]
    return "\n\n".join(f"[{d[0]}] {d[1]} (fuente: {d[2]})" for d in docs) or "Sin resultados."

def consultar_disponibilidad(texto_programa: str) -> str:
    with _conn() as c, c.cursor() as cur:
        cur.execute("""SELECT programa_id, nombre, sede, ciudad, cupos_disponibles, costo_afiliado
                       FROM programas WHERE nombre ILIKE %s AND cupos_disponibles > 0
                       ORDER BY cupos_disponibles DESC LIMIT 5""", (f"%{texto_programa}%",))
        rows = cur.fetchall()
    if not rows:
        return f"No hay programas con cupo que coincidan con '{texto_programa}'."
    return "\n".join(f"#{r[0]} {r[1]} — {r[2]}, {r[3]} | cupos: {r[4]} | costo: ${r[5]:,.0f}" for r in rows)

def consultar_beneficios(afiliado_id: int) -> str:
    with _conn() as c, c.cursor() as cur:
        cur.execute("""SELECT p.nombre, r.estado, r.creada_en FROM reservas r
                       JOIN programas p ON p.programa_id=r.programa_id
                       WHERE r.afiliado_id=%s ORDER BY r.creada_en DESC""", (afiliado_id,))
        rows = cur.fetchall()
    return "\n".join(f"- {r[0]} ({r[1]}, {r[2]:%Y-%m-%d})" for r in rows) or "Sin inscripciones registradas."

def crear_reserva(afiliado_id: int, programa_id: int) -> str:
    with _conn() as c, c.cursor() as cur:
        cur.execute("SELECT nombre, cupos_disponibles FROM programas WHERE programa_id=%s FOR UPDATE",
                    (programa_id,))
        row = cur.fetchone()
        if row is None:
            c.rollback(); return "No encontré ese programa."
        nombre, disp = row
        if disp <= 0:
            c.rollback(); return f"Lo siento, '{nombre}' no tiene cupos disponibles."
        cur.execute("INSERT INTO reservas (afiliado_id,programa_id) VALUES (%s,%s) RETURNING reserva_id",
                    (afiliado_id, programa_id))
        rid = cur.fetchone()[0]
        cur.execute("UPDATE programas SET cupos_disponibles=cupos_disponibles-1 WHERE programa_id=%s",
                    (programa_id,))
        c.commit()
        return f"Reserva #{rid} confirmada para '{nombre}'. Cupos restantes: {disp-1}."

TOOLS_IMPL = {"buscar_conocimiento": buscar_conocimiento,
              "consultar_disponibilidad": consultar_disponibilidad,
              "consultar_beneficios": consultar_beneficios,
              "crear_reserva": crear_reserva}

TOOLS_SPEC = [
  {"type":"function","function":{"name":"buscar_conocimiento",
     "description":"Busca en la base de conocimiento de Comfama (beneficios, requisitos, procesos, sedes, FAQ).",
     "parameters":{"type":"object","properties":{"pregunta":{"type":"string"}},"required":["pregunta"]}}},
  {"type":"function","function":{"name":"consultar_disponibilidad",
     "description":"Lista programas con cupo disponible que coinciden con un texto.",
     "parameters":{"type":"object","properties":{"texto_programa":{"type":"string"}},"required":["texto_programa"]}}},
  {"type":"function","function":{"name":"consultar_beneficios",
     "description":"Devuelve las inscripciones/reservas de un afiliado por su id.",
     "parameters":{"type":"object","properties":{"afiliado_id":{"type":"integer"}},"required":["afiliado_id"]}}},
  {"type":"function","function":{"name":"crear_reserva",
     "description":"Reserva un cupo en un programa para un afiliado (transacción atómica).",
     "parameters":{"type":"object","properties":{"afiliado_id":{"type":"integer"},"programa_id":{"type":"integer"}},
                   "required":["afiliado_id","programa_id"]}}},
]

SYSTEM_PROMPT = (
  "Eres el Asistente de Servicios al Afiliado de Comfama. Respondes en español, claro y cordial. "
  "Usa 'buscar_conocimiento' para preguntas informativas (beneficios, requisitos, procesos). "
  "Usa 'consultar_disponibilidad' antes de reservar para confirmar cupo y obtener el programa_id. "
  "Usa 'consultar_beneficios' para ver lo que el afiliado ya tiene. "
  "Usa 'crear_reserva' SOLO cuando el afiliado confirme que quiere reservar. "
  "Nunca inventes datos de programas o cupos: si no estás seguro, usa una tool."
)

class AgenteAfiliados(ChatAgent):
    def predict(self, messages, context=None, custom_inputs=None) -> ChatAgentResponse:
        convo = [{"role": "system", "content": SYSTEM_PROMPT}]
        convo += [{"role": m.role, "content": m.content} for m in messages]
        for _ in range(6):
            resp = _llm.predict(endpoint=LLM_ENDPOINT,
                                inputs={"messages": convo, "tools": TOOLS_SPEC, "tool_choice": "auto"})
            msg = resp["choices"][0]["message"]
            tcs = msg.get("tool_calls")
            if not tcs:
                return ChatAgentResponse(messages=[ChatAgentMessage(
                    role="assistant", content=msg.get("content") or "", id=str(uuid.uuid4()))])
            convo.append({"role": "assistant", "content": msg.get("content"), "tool_calls": tcs})
            for tc in tcs:
                args = json.loads(tc["function"]["arguments"] or "{}")
                out = TOOLS_IMPL[tc["function"]["name"]](**args)
                convo.append({"role": "tool", "tool_call_id": tc["id"], "content": str(out)})
        return ChatAgentResponse(messages=[ChatAgentMessage(
            role="assistant", content="No pude completar la solicitud.", id=str(uuid.uuid4()))])

AGENT = AgenteAfiliados()
mlflow.models.set_model(AGENT)
'''

with open("agent.py", "w") as f:
    f.write(AGENT_HEADER + AGENT_BODY)
print("✅ agent.py escrito")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Probar el agente (local, desde `agent.py`)

# COMMAND ----------

import sys, os
sys.path.insert(0, os.getcwd())
import agent  # ejecuta agent.py: conecta a Lakebase + VS con TU identidad
from mlflow.types.agent import ChatAgentMessage
import uuid

def chat(texto):
    r = agent.AGENT.predict([ChatAgentMessage(role="user", content=texto, id=str(uuid.uuid4()))])
    print("👤", texto, "\n🤖", r.messages[-1].content, "\n")

chat("¿Puedo cancelar una inscripción que ya pagué?")                    # RAG
chat("Soy el afiliado 1001, ¿qué tengo inscrito?")                       # consultar_beneficios
chat("Busco un curso con cupo disponible")                               # consultar_disponibilidad
chat("Quiero reservar el programa 9 para el afiliado 1003, confírmalo")  # crear_reserva

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Registrar el agente en Unity Catalog (models-from-code)

# COMMAND ----------

import mlflow
from mlflow.models.resources import (DatabricksServingEndpoint, DatabricksVectorSearchIndex,
                                     DatabricksLakebase)

mlflow.set_registry_uri("databricks-uc")

with mlflow.start_run(run_name="agente_afiliados"):
    info = mlflow.pyfunc.log_model(
        name="agente",
        python_model="agent.py",
        registered_model_name=AGENT_MODEL_NAME,
        pip_requirements=["mlflow","openai","psycopg2-binary",
                          "databricks-vectorsearch","databricks-sdk"],
        resources=[
            DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
            DatabricksServingEndpoint(endpoint_name=EMBEDDING_MODEL),
            DatabricksVectorSearchIndex(index_name=VS_INDEX),
            DatabricksLakebase(database_instance_name=LAKEBASE_PROJECT),  # auto-otorga acceso al SP servido
        ],
        input_example={"messages":[{"role":"user","content":"¿Qué cursos hay con cupo?"}]},
    )
print("✅ Registrado:", AGENT_MODEL_NAME, "| uri:", info.model_uri)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 🖱️ Inspeccionar / probar en la UI
# MAGIC - **Catalog** → tu schema → **Models** → `agente_afiliados`: versiones, firma, recursos declarados.
# MAGIC - **AI Playground**: carga `LLM_ENDPOINT` y adjunta el índice de VS como retriever para probar el RAG visualmente.
# MAGIC
# MAGIC > ℹ️ **Nota de serving (módulo 05):** el endpoint del agente usará un **service principal**. Para que las tools de
# MAGIC > Lakebase funcionen servidas, ese SP necesita permiso sobre la instancia `comfama-afiliados` (lo cubrimos al desplegar).
# MAGIC
# MAGIC ### ▶️ Siguiente: `05 - Servir el Agente`

