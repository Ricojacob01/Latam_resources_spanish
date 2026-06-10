# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Deploy Real Agent (Vector Search + Model Serving)
# MAGIC
# MAGIC Este notebook despliega un **agente real funcional** end-to-end:
# MAGIC 1. Crea una tabla Delta con documentos de muestra (servicios de Comfama)
# MAGIC 2. Crea un índice Vector Search delta-sync sobre la tabla
# MAGIC 3. Construye un agente con retrieval + generación (PyFunc)
# MAGIC 4. Registra el agente en Unity Catalog
# MAGIC 5. Despliega a Model Serving **con Inference Table activada**
# MAGIC 6. Prueba el endpoint
# MAGIC
# MAGIC **Tiempo estimado:** 15-25 min (índice VS + cold start del serving endpoint)

# COMMAND ----------

# MAGIC %pip install -q -U mlflow>=3.0.0 databricks-vectorsearch databricks-sdk>=0.30.0 openai
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

VS_ENDPOINT = "ka-3b905361-vs-endpoint"  # reutilizar endpoint existente
INDEX_NAME = f"{FULL_SCHEMA}.documentos_index"
DOCS_TABLE = f"{FULL_SCHEMA}.documentos_subsidios"
AGENT_MODEL_NAME = f"{FULL_SCHEMA}.agente_comfama"
SERVING_ENDPOINT = "agente_comfama"

EMBEDDING_MODEL = "databricks-gte-large-en"
LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"

print(f"VS endpoint:    {VS_ENDPOINT}")
print(f"Index:          {INDEX_NAME}")
print(f"Docs:           {DOCS_TABLE}")
print(f"Agent model:    {AGENT_MODEL_NAME}")
print(f"Serving:        {SERVING_ENDPOINT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabla Delta de documentos (sample knowledge base)

# COMMAND ----------

import uuid

# Documentos simulando servicios de Comfama (caja de compensación familiar)
DOCS = [
    {
        "titulo": "Subsidio de vivienda",
        "categoria": "vivienda",
        "texto": "El subsidio de vivienda de Comfama es un beneficio económico para afiliados de categorías A y B que desean adquirir vivienda de interés social. El monto máximo vigente es de $30.000.000 pesos para 2026. Para solicitar el subsidio, el afiliado debe haber estado afiliado por al menos 12 meses continuos. El subsidio puede aplicarse a vivienda nueva o usada, siempre que cumpla los requisitos de vivienda de interés social. Documentos requeridos: cédula, certificación de afiliación, promesa de compraventa, y avalúo del inmueble.",
    },
    {
        "titulo": "Subsidio escolar",
        "categoria": "educacion",
        "texto": "El subsidio escolar cubre hijos de afiliados desde grado 0 hasta grado 11 en colegios públicos y privados. El monto mensual depende de la categoría: A=$120.000, B=$90.000, C=$60.000. Se desembolsa los primeros 10 días de cada mes lectivo. Para mantener el beneficio, el estudiante debe demostrar matrícula activa cada semestre. Hijos hasta los 26 años en educación superior también califican si están en una IES (Institución de Educación Superior) certificada.",
    },
    {
        "titulo": "Servicios de salud",
        "categoria": "salud",
        "texto": "Comfama opera 4 IPS (Instituciones Prestadoras de Salud) en Antioquia con servicios de medicina general, especializada, odontología, optometría y psicología. Las citas se solicitan vía la app o al 444-4123. Tiempo promedio de asignación: 2 días para general, 7 días para especialista. Cobertura: afiliados y beneficiarios categoría A y B con cuota moderadora del 10%. Categoría C paga cuota completa.",
    },
    {
        "titulo": "Crédito de libre inversión",
        "categoria": "credito",
        "texto": "Comfama ofrece créditos de libre inversión para afiliados con tasas preferenciales desde 1.2% mes vencido. Plazos: 12 a 60 meses. Monto máximo: $50.000.000 sujeto a estudio de crédito. Requisitos: certificación laboral, declaración de ingresos, score Datacrédito mínimo 650. Aprobación en 48 horas. El crédito se descuenta directamente del salario via libranza, con tope del 50% del ingreso disponible.",
    },
    {
        "titulo": "Recreación y turismo",
        "categoria": "recreacion",
        "texto": "Acceso a 9 parques recreativos en Antioquia con tarifas subsidiadas para afiliados (categoría A 70% de descuento, B 50%, C 30%). Programa de turismo social: paquetes nacionales con financiación a 12 meses sin interés para afiliados A. Destinos: Eje Cafetero, Costa Atlántica, San Andrés. Bonos de cumpleaños y aguinaldo navideño anual para afiliados activos.",
    },
    {
        "titulo": "Procesos de afiliación",
        "categoria": "afiliacion",
        "texto": "La afiliación a Comfama es a través del empleador y se activa al primer aporte mensual. Empleados de empresas afiliadas tienen acceso inmediato. Independientes y pensionados pueden afiliarse directamente con un aporte voluntario del 2% del ingreso. Beneficiarios: cónyuge, hijos hasta 18 años (26 si estudian), padres mayores de 60 que dependan económicamente. La cuota de afiliación para empleadores es 4% de la nómina.",
    },
    {
        "titulo": "Estado de solicitudes",
        "categoria": "tramites",
        "texto": "Para consultar el estado de cualquier solicitud (subsidio, crédito, cita) el afiliado puede usar: 1) la app móvil Comfama, 2) el portal web mi.comfama.com, 3) la línea 444-4123 con su número de cédula, o 4) cualquier sede física presentando documento de identidad. Las solicitudes en estado 'En estudio' tienen plazo máximo de 15 días hábiles. 'Aprobado' significa que el desembolso/asignación se hará en los próximos 5 días. 'Rechazado' incluye motivos y opción de apelación dentro de los 10 días siguientes.",
    },
    {
        "titulo": "Educación continua",
        "categoria": "educacion",
        "texto": "Programa de educación continua: cursos cortos, diplomados y certificaciones técnicas. Categoría A: 80% subsidio, B: 50%, C: 25%. Áreas: tecnología, administración, oficios, idiomas. Modalidad presencial en sedes de Comfama o virtual via plataforma propia. Catálogo 2026 incluye más de 200 programas. Inscripciones abiertas todo el año, cohortes mensuales.",
    },
]

# Agregar IDs y timestamp
from datetime import datetime
import json

rows = []
for i, doc in enumerate(DOCS):
    rows.append({
        "id": f"doc_{i:03d}",
        "titulo": doc["titulo"],
        "categoria": doc["categoria"],
        "texto": doc["texto"],
        "longitud_chars": len(doc["texto"]),
        "fecha_actualizacion": datetime(2026, 5, 15),
    })

df = spark.createDataFrame(rows)

# Crear con Change Data Feed habilitado (requerido para delta-sync index)
spark.sql(f"DROP TABLE IF EXISTS {DOCS_TABLE}")
df.write.mode("overwrite") \
    .option("delta.enableChangeDataFeed", "true") \
    .saveAsTable(DOCS_TABLE)

# Asegurar que CDF esté habilitado
spark.sql(f"ALTER TABLE {DOCS_TABLE} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

print(f"✓ Tabla {DOCS_TABLE} creada con {df.count()} documentos")
display(spark.sql(f"SELECT id, titulo, categoria, longitud_chars FROM {DOCS_TABLE}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Crear índice Vector Search (delta-sync)

# COMMAND ----------

from databricks.vector_search.client import VectorSearchClient
import time

vsc = VectorSearchClient(disable_notice=True)

# Verificar si el índice ya existe
try:
    existing = vsc.get_index(endpoint_name=VS_ENDPOINT, index_name=INDEX_NAME)
    print(f"✓ Índice {INDEX_NAME} ya existe")
    print(f"  Status: {existing.describe().get('status', {}).get('detailed_state', '?')}")
    idx = existing
    create_new = False
except Exception:
    create_new = True

if create_new:
    print(f"Creando índice {INDEX_NAME}...")
    idx = vsc.create_delta_sync_index(
        endpoint_name=VS_ENDPOINT,
        index_name=INDEX_NAME,
        source_table_name=DOCS_TABLE,
        pipeline_type="TRIGGERED",
        primary_key="id",
        embedding_source_column="texto",
        embedding_model_endpoint_name=EMBEDDING_MODEL,
    )
    print(f"✓ Índice creado: {INDEX_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Esperar a que el índice esté listo y sincronizar

# COMMAND ----------

import time

print("Esperando a que el índice esté listo (puede tomar 5-15 min la primera vez)...")
for attempt in range(60):  # max 30 min
    try:
        desc = idx.describe()
        state = desc.get("status", {}).get("detailed_state", "UNKNOWN")
        ready = desc.get("status", {}).get("ready", False)
        print(f"  [{attempt+1:02d}] state={state}  ready={ready}")
        if ready or state == "ONLINE_NO_PENDING_UPDATE":
            print("✓ Índice listo")
            break
        if "FAILED" in state:
            print(f"⚠ Estado de fallo: {state}")
            break
    except Exception as e:
        print(f"  Error: {e}")
    time.sleep(30)

# COMMAND ----------

# Trigger sync para asegurar últimos datos
try:
    idx.sync()
    print("Sync triggered. Esperando...")
    for attempt in range(20):
        desc = idx.describe()
        state = desc.get("status", {}).get("detailed_state", "UNKNOWN")
        print(f"  [{attempt+1:02d}] sync state={state}")
        if "NO_PENDING_UPDATE" in state:
            break
        time.sleep(15)
except Exception as e:
    print(f"Sync: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Probar el índice con búsqueda semántica

# COMMAND ----------

try:
    results = idx.similarity_search(
        query_text="¿Cuánto es el subsidio para vivienda?",
        columns=["id", "titulo", "categoria", "texto"],
        num_results=3,
    )
    print("Top 3 resultados:")
    for r in results.get("result", {}).get("data_array", []):
        # Cada row es una lista: [id, titulo, categoria, texto, score]
        print(f"  [{r[-1]:.3f}] {r[1]} ({r[2]})")
        print(f"      {r[3][:120]}...")
        print()
except Exception as e:
    print(f"⚠ Search no disponible aún: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Construir el agente (PyFunc)

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
import pandas as pd

CURRENT_USER = spark.sql("SELECT current_user() as u").collect()[0]["u"]
EXPERIMENT_NAME = f"/Users/{CURRENT_USER}/comfama_agente_real"

mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(EXPERIMENT_NAME)
print(f"Experiment: {EXPERIMENT_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Definir el agente con tracing automático

# COMMAND ----------

import mlflow
import pandas as pd

# Setup MLflow tracing
try:
    mlflow.openai.autolog()
except Exception:
    pass

SYSTEM_PROMPT = """Eres un asistente virtual de Comfama, caja de compensación familiar colombiana.
Respondes preguntas sobre servicios: subsidios (vivienda, escolar, recreación), salud, créditos, afiliación.

Reglas:
- Usa SOLO la información del contexto proporcionado.
- Si la respuesta no está en el contexto, di claramente "No tengo esa información en mi base de conocimiento. Por favor consulta con un asesor al 444-4123."
- Cita la sección del documento cuando sea relevante.
- Sé conciso, máximo 4 párrafos.
- Idioma: español formal colombiano."""


class ComfamaAgent(mlflow.pyfunc.PythonModel):
    """Agente conversacional Comfama: retrieve from Vector Search + generate via FM API."""

    VS_ENDPOINT = "ka-3b905361-vs-endpoint"
    INDEX_NAME = "ardemo_classic_dnubtw_catalog.comfama.documentos_index"
    LLM_MODEL = "databricks-meta-llama-3-3-70b-instruct"

    def load_context(self, context):
        from databricks.vector_search.client import VectorSearchClient
        from mlflow.deployments import get_deploy_client
        self.vsc = VectorSearchClient(disable_notice=True)
        self.idx = self.vsc.get_index(endpoint_name=self.VS_ENDPOINT, index_name=self.INDEX_NAME)
        self.llm = get_deploy_client("databricks")

    @mlflow.trace(span_type="RETRIEVER")
    def retrieve(self, query: str, k: int = 3) -> list:
        results = self.idx.similarity_search(
            query_text=query,
            columns=["id", "titulo", "categoria", "texto"],
            num_results=k,
        )
        rows = results.get("result", {}).get("data_array", [])
        return [
            {"id": r[0], "titulo": r[1], "categoria": r[2], "texto": r[3], "score": r[-1]}
            for r in rows
        ]

    @mlflow.trace(span_type="LLM")
    def generate(self, query: str, context: list) -> str:
        context_text = "\n\n".join([
            f"[{c['titulo']} - {c['categoria']}]\n{c['texto']}" for c in context
        ])
        prompt = f"CONTEXTO:\n{context_text}\n\nPREGUNTA: {query}"
        resp = self.llm.predict(
            endpoint=self.LLM_MODEL,
            inputs={
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                "temperature": 0.1,
                "max_tokens": 500,
            },
        )
        return resp["choices"][0]["message"]["content"]

    @mlflow.trace(span_type="CHAIN", name="comfama_agent")
    def predict(self, context, model_input):
        import pandas as pd
        results = []
        for _, row in model_input.iterrows():
            q = row["query"]
            ctx = self.retrieve(q, k=3)
            answer = self.generate(q, ctx)
            results.append({
                "answer": answer,
                "sources": [{"id": c["id"], "titulo": c["titulo"], "score": c["score"]} for c in ctx],
            })
        return pd.DataFrame(results)

print("✓ ComfamaAgent class defined")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Loggear el agente a MLflow

# COMMAND ----------

import mlflow
import pandas as pd
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksVectorSearchIndex,
)

# Input signature
input_example = pd.DataFrame({"query": ["¿Cómo solicito el subsidio escolar?"]})

# Resources: declaramos qué endpoints/recursos consume el agente para que
# Model Serving auto-inyecte credenciales en runtime.
resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_MODEL),
    DatabricksServingEndpoint(endpoint_name=EMBEDDING_MODEL),
    DatabricksVectorSearchIndex(index_name=INDEX_NAME),
]

with mlflow.start_run(run_name="comfama_agent_v1") as run:
    mlflow.pyfunc.log_model(
        artifact_path="agent",
        python_model=ComfamaAgent(),
        input_example=input_example,
        registered_model_name=AGENT_MODEL_NAME,
        resources=resources,
        pip_requirements=[
            "mlflow>=3.0.0",
            "databricks-vectorsearch",
            "databricks-sdk>=0.30.0",
        ],
    )
    run_id = run.info.run_id

print(f"✓ Modelo registrado en UC: {AGENT_MODEL_NAME}")
print(f"  Run ID: {run_id}")
print(f"  Resources declarados: {len(resources)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Asignar alias "production" a la última versión

# COMMAND ----------

from mlflow import MlflowClient

client = MlflowClient(registry_uri="databricks-uc")
versions = client.search_model_versions(f"name='{AGENT_MODEL_NAME}'")
latest = max(versions, key=lambda v: int(v.version))

client.set_registered_model_alias(
    name=AGENT_MODEL_NAME,
    alias="production",
    version=latest.version,
)
print(f"✓ Alias 'production' → version {latest.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Deploy a Model Serving (con Inference Table)

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.serving import (
    EndpointCoreConfigInput,
    ServedEntityInput,
    AiGatewayConfig,
    AiGatewayInferenceTableConfig,
)

w = WorkspaceClient()

# Verificar si el endpoint ya existe
existing_endpoint = None
try:
    existing_endpoint = w.serving_endpoints.get(SERVING_ENDPOINT)
    print(f"Endpoint {SERVING_ENDPOINT} ya existe — actualizando")
except Exception:
    print(f"Creando nuevo endpoint: {SERVING_ENDPOINT}")

served_entity = ServedEntityInput(
    name=f"{SERVING_ENDPOINT}-entity",
    entity_name=AGENT_MODEL_NAME,
    entity_version=str(latest.version),
    workload_size="Small",
    scale_to_zero_enabled=True,
)

ai_gateway = AiGatewayConfig(
    inference_table_config=AiGatewayInferenceTableConfig(
        enabled=True,
        catalog_name=CATALOG,
        schema_name=SCHEMA,
        table_name_prefix="agente_inference",
    )
)

if existing_endpoint:
    operation = w.serving_endpoints.update_config(
        name=SERVING_ENDPOINT,
        served_entities=[served_entity],
    )
    # AI Gateway config se actualiza separadamente
    try:
        w.serving_endpoints.put_ai_gateway(name=SERVING_ENDPOINT, inference_table_config=ai_gateway.inference_table_config)
    except Exception as e:
        print(f"AI gateway update: {e}")
else:
    operation = w.serving_endpoints.create(
        name=SERVING_ENDPOINT,
        ai_gateway=ai_gateway,
        config=EndpointCoreConfigInput(
            name=SERVING_ENDPOINT,
            served_entities=[served_entity],
        ),
    )

print(f"✓ Deploy lanzado para {SERVING_ENDPOINT}")
print("  Inference table:")
print(f"    {FULL_SCHEMA}.agente_inference_payload  (auto-creada)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Esperar a que el endpoint esté listo (10-15 min cold start)

# COMMAND ----------

import time

print("Esperando READY...")
for attempt in range(40):  # max 20 min
    try:
        e = w.serving_endpoints.get(SERVING_ENDPOINT)
        ready = e.state.ready
        config_state = e.state.config_update
        print(f"  [{attempt+1:02d}] ready={ready}  config_update={config_state}")
        if str(ready) in ("Ready.READY", "READY"):
            print("✓ Endpoint listo")
            break
    except Exception as ex:
        print(f"  err: {ex}")
    time.sleep(30)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Probar el agente desplegado

# COMMAND ----------

import os
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Get a token for the test
DATABRICKS_HOST = w.config.host
DATABRICKS_TOKEN = w.config.token or os.environ.get("DATABRICKS_TOKEN", "")

import requests
headers = {"Authorization": f"Bearer {DATABRICKS_TOKEN}", "Content-Type": "application/json"}
url = f"{DATABRICKS_HOST}/serving-endpoints/{SERVING_ENDPOINT}/invocations"

test_queries = [
    "¿Cuál es el monto del subsidio de vivienda?",
    "¿Cómo solicito una cita médica?",
    "Hola, ¿qué es Comfama?",
]

for q in test_queries:
    try:
        resp = requests.post(
            url,
            headers=headers,
            json={"dataframe_records": [{"query": q}]},
            timeout=60,
        )
        if resp.status_code == 200:
            data = resp.json()
            answer = data.get("predictions", [{}])[0].get("answer", "")
            sources = data.get("predictions", [{}])[0].get("sources", [])
            print(f"❓ {q}")
            print(f"💬 {answer[:200]}...")
            if sources:
                print(f"📎 Fuentes: {', '.join(s.get('titulo','?') for s in sources)}")
            print()
        else:
            print(f"❌ {q}: HTTP {resp.status_code} - {resp.text[:200]}")
    except Exception as e:
        print(f"❌ {q}: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verificar Inference Table
# MAGIC
# MAGIC La inference table captura cada request/response/latency automáticamente. Tarda 5-15 min en aparecer la primera fila después del primer invoke.

# COMMAND ----------

# Esperar a que aparezca la tabla
inference_table = f"{FULL_SCHEMA}.agente_inference_payload"
try:
    display(spark.sql(f"SELECT * FROM {inference_table} LIMIT 5"))
except Exception as e:
    print(f"Inference table aún no materializada: {e}")
    print(f"Espera ~10 min y ejecuta:")
    print(f"  SELECT * FROM {inference_table} ORDER BY databricks_request_id DESC LIMIT 10")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen — assets desplegados
# MAGIC
# MAGIC | Asset | Ubicación |
# MAGIC |---|---|
# MAGIC | Documents Delta table | `ardemo_classic_dnubtw_catalog.comfama.documentos_subsidios` |
# MAGIC | Vector Search index | `ardemo_classic_dnubtw_catalog.comfama.documentos_index` |
# MAGIC | Registered model | `ardemo_classic_dnubtw_catalog.comfama.agente_comfama` |
# MAGIC | Model Serving endpoint | `agente_comfama` |
# MAGIC | Inference table | `ardemo_classic_dnubtw_catalog.comfama.agente_inference_payload` |
# MAGIC | MLflow experiment | `comfama_agente_real` |
# MAGIC
# MAGIC ## Siguiente paso
# MAGIC
# MAGIC Notebook `08 - Databricks App` para crear el frontend que llama a este agente.

