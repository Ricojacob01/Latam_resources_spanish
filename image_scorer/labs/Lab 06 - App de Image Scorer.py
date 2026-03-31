# Databricks notebook source
# MAGIC %md
# MAGIC # Hands-On LAB 06 - App de Image Scorer
# MAGIC
# MAGIC En este laboratorio crearemos una **Databricks App** con interfaz grafica
# MAGIC para que los usuarios puedan subir imagenes de displays y obtener una puntuacion
# MAGIC de forma sencilla e interactiva.
# MAGIC </br></br>
# MAGIC
# MAGIC ## Objetivos del Ejercicio
# MAGIC
# MAGIC - Crear una aplicacion Streamlit desplegada como Databricks App
# MAGIC - Interfaz para subir imagenes y obtener puntuaciones en tiempo real
# MAGIC - Visualizar displays similares y explicabilidad del resultado
# MAGIC - Configurar filtros interactivos (marca, tipo de tienda)
# MAGIC
# MAGIC ## Arquitectura de la App
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────┐
# MAGIC │                    Databricks App                        │
# MAGIC │                   (Streamlit UI)                         │
# MAGIC │                                                          │
# MAGIC │  ┌─────────────┐  ┌──────────────┐  ┌───────────────┐  │
# MAGIC │  │   Subir      │  │  Resultados  │  │ Explicabilidad│  │
# MAGIC │  │   Imagen     │  │  y Score     │  │ y Top 5       │  │
# MAGIC │  └──────┬──────┘  └──────▲───────┘  └───────▲───────┘  │
# MAGIC │         │                │                    │          │
# MAGIC └─────────┼────────────────┼────────────────────┼──────────┘
# MAGIC           │                │                    │
# MAGIC           ▼                │                    │
# MAGIC ┌─────────────────┐       │                    │
# MAGIC │  Model Serving  │       │                    │
# MAGIC │  (CLIP Embedder)│       │                    │
# MAGIC │                 │       │                    │
# MAGIC │  imagen ──► vec │       │                    │
# MAGIC └────────┬────────┘       │                    │
# MAGIC          │                │                    │
# MAGIC          ▼                │                    │
# MAGIC ┌─────────────────────────┴────────────────────┴──────┐
# MAGIC │                   Vector Search                      │
# MAGIC │              (Standard Endpoint)                     │
# MAGIC │                                                      │
# MAGIC │  Delta Sync Index ◄──── Delta Table                 │
# MAGIC │  (self-managed emb.)    (displays_referencia)        │
# MAGIC │                                                      │
# MAGIC │  query_vector ──► Top-K vecinos ──► Score + Labels  │
# MAGIC └─────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ### Componentes de la App:
# MAGIC
# MAGIC | Componente | Tecnologia | Funcion |
# MAGIC |-----------|------------|---------|
# MAGIC | **Frontend** | Streamlit | UI interactiva con upload, filtros, tabs |
# MAGIC | **Embedding** | Model Serving (CLIP) | Convierte imagen a vector |
# MAGIC | **Busqueda** | Vector Search SDK | Busca displays similares |
# MAGIC | **Scoring** | Python (numpy) | Calcula puntuacion ponderada |
# MAGIC | **Auth** | Databricks Apps OAuth | Autenticacion automatica |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Preparacion
# MAGIC
# MAGIC 1. En la esquina superior derecha, haga clic en **Connect**
# MAGIC 2. Seleccione el cluster: **Serverless**

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %run "../config"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 01 - Estructura de la App
# MAGIC
# MAGIC Nuestra Databricks App tiene la siguiente estructura de archivos:
# MAGIC
# MAGIC ```
# MAGIC image_scorer_app/
# MAGIC ├── app.py              ← Aplicacion Streamlit principal
# MAGIC ├── app.yaml            ← Configuracion de Databricks App
# MAGIC └── requirements.txt    ← Dependencias de Python
# MAGIC ```
# MAGIC
# MAGIC Vamos a crear cada archivo.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Crear directorio de la app

# COMMAND ----------

import os

app_dir = f"/Workspace/Users/{spark.conf.get('spark.databricks.workspaceUrl').split('//')[0]}"

# Usar el directorio actual del usuario
username = spark.sql("SELECT current_user()").first()[0]
app_dir = f"/Workspace/Users/{username}/Latam_resources_spanish/image_scorer/image_scorer_app"

os.makedirs(app_dir, exist_ok=True)
print(f"Directorio de la app: {app_dir}")

# COMMAND ----------

app_dir

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 Crear app.yaml
# MAGIC
# MAGIC Este archivo configura como Databricks ejecuta nuestra app:
# MAGIC - **command**: como iniciar Streamlit
# MAGIC - **env**: variables de entorno (catalogo, esquema, endpoints, etc.)
# MAGIC - **resources**: permisos para acceder a Model Serving y Vector Search

# COMMAND ----------

# DBTITLE 1,Escribir app.yaml
app_yaml = f"""command:
  - "streamlit"
  - "run"
  - "app.py"
  - "--server.port"
  - "8000"
  - "--server.address"
  - "0.0.0.0"
  - "--server.enableCORS"
  - "false"
  - "--server.enableXsrfProtection"
  - "false"

env:
  - name: CATALOG
    value: "{CATALOG}"
  - name: SCHEMA
    value: "{SCHEMA}"
  - name: VOLUME
    value: "{VOLUME}"
  - name: VS_ENDPOINT_NAME
    value: "{VS_ENDPOINT_NAME}"
  - name: VS_INDEX_NAME
    value: "{VS_INDEX_NAME}"
  - name: EMBEDDING_ENDPOINT
    value: "{EMBEDDING_ENDPOINT}"
  - name: EMBEDDING_DIM
    value: "{EMBEDDING_DIM}"
  - name: TOP_K
    value: "{TOP_K}"
  - name: SCORE_WEIGHT_NEIGHBORS
    value: "{SCORE_WEIGHT_NEIGHBORS}"
  - name: SCORE_WEIGHT_MODEL
    value: "{SCORE_WEIGHT_MODEL}"

resources:
  - name: serving-endpoint
    serving_endpoint:
      name: "{EMBEDDING_ENDPOINT}"
      permission: CAN_QUERY
  - name: vector-search-endpoint
    serving_endpoint:
      name: "{VS_ENDPOINT_NAME}"
      permission: CAN_QUERY
"""

with open(f"{app_dir}/app.yaml", "w") as f:
    f.write(app_yaml)

print("app.yaml creado!")
print(app_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.3 Crear requirements.txt

# COMMAND ----------

# DBTITLE 1,Escribir requirements.txt
requirements = """streamlit>=1.38.0
databricks-sdk>=0.36.0
databricks-vectorsearch>=0.40
Pillow>=10.0.0
numpy>=1.24.0
requests>=2.31.0
plotly>=5.18.0
"""

with open(f"{app_dir}/requirements.txt", "w") as f:
    f.write(requirements)

print("requirements.txt creado!")
print(requirements)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.4 Crear app.py - La aplicacion Streamlit
# MAGIC
# MAGIC Esta es la aplicacion principal. Incluye:
# MAGIC
# MAGIC | Seccion | Descripcion |
# MAGIC |---------|-------------|
# MAGIC | **Header** | Titulo y descripcion de la app |
# MAGIC | **Sidebar** | Filtros de marca/tienda, parametros de puntuacion |
# MAGIC | **Upload** | Area para subir imagenes de displays |
# MAGIC | **Tab Puntuacion** | Score card con desglose y formula |
# MAGIC | **Tab Similares** | Galeria de Top-K displays similares con imagenes |
# MAGIC | **Tab Explicabilidad** | Razonamiento, distribucion y recomendaciones |
# MAGIC | **Tab Datos** | Tabla comparativa y JSON de respuesta |

# COMMAND ----------

# DBTITLE 1,Escribir app.py
app_code = r'''"""
Image Scorer - Databricks App
Puntua displays de productos usando Vector Search con embeddings de imagenes.
"""

import streamlit as st
import os
import json
import numpy as np
import requests
import time
from io import BytesIO
from PIL import Image

# ---------------------------------------------------------------------------
# Configuracion
# ---------------------------------------------------------------------------
CATALOG = os.getenv("CATALOG", "academia")
SCHEMA = os.getenv("SCHEMA", "image_scorer")
VOLUME = os.getenv("VOLUME", "archivos")
VS_ENDPOINT_NAME = os.getenv("VS_ENDPOINT_NAME", "image-scorer-vs-endpoint")
VS_INDEX_NAME = os.getenv("VS_INDEX_NAME", "academia.image_scorer.displays_index")
EMBEDDING_ENDPOINT = os.getenv("EMBEDDING_ENDPOINT", "image-embedding-endpoint")
EMBEDDING_DIM = int(os.getenv("EMBEDDING_DIM", "512"))
TOP_K = int(os.getenv("TOP_K", "5"))
SCORE_WEIGHT_NEIGHBORS = float(os.getenv("SCORE_WEIGHT_NEIGHBORS", "0.7"))
SCORE_WEIGHT_MODEL = float(os.getenv("SCORE_WEIGHT_MODEL", "0.3"))

PATH_VOLUME = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

# ---------------------------------------------------------------------------
# Pagina config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Image Scorer - Display Scoring",
    page_icon="\U0001F4CA",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Estilos CSS
# ---------------------------------------------------------------------------
st.markdown("""
<style>
    .score-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border-radius: 16px;
        padding: 24px;
        text-align: center;
        border: 1px solid #2a2a4a;
        margin-bottom: 16px;
    }
    .score-value {
        font-size: 3.5rem;
        font-weight: 800;
        margin: 8px 0;
        line-height: 1;
    }
    .score-label {
        font-size: 0.85rem;
        color: #888;
        text-transform: uppercase;
        letter-spacing: 2px;
    }
    .score-classification {
        font-size: 1.3rem;
        font-weight: 700;
        padding: 6px 20px;
        border-radius: 20px;
        display: inline-block;
        margin-top: 8px;
    }
    .cls-excelente { background: #0d6e3a; color: #a7f3d0; }
    .cls-bueno { background: #1e5f8a; color: #93c5fd; }
    .cls-regular { background: #92400e; color: #fcd34d; }
    .cls-deficiente { background: #991b1b; color: #fca5a5; }
    .formula-box {
        background: #0f172a;
        border: 1px solid #334155;
        border-radius: 12px;
        padding: 20px;
        font-family: 'Courier New', monospace;
        font-size: 1.05rem;
    }
    .app-header {
        background: linear-gradient(90deg, #1e3a5f 0%, #2563eb 50%, #1e3a5f 100%);
        padding: 30px;
        border-radius: 16px;
        text-align: center;
        margin-bottom: 24px;
    }
    .app-header h1 { margin: 0; font-size: 2rem; }
    .app-header p { margin: 8px 0 0 0; opacity: 0.8; }
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# Databricks clients
# ---------------------------------------------------------------------------
@st.cache_resource
def get_workspace_client():
    from databricks.sdk import WorkspaceClient
    return WorkspaceClient()


@st.cache_resource
def get_vs_index():
    from databricks.vector_search.client import VectorSearchClient
    w = get_workspace_client()
    # In Databricks Apps, auth uses service principal OAuth credentials
    client_id = os.getenv("DATABRICKS_CLIENT_ID")
    client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
    if client_id and client_secret:
        vsc = VectorSearchClient(
            workspace_url=w.config.host,
            service_principal_client_id=client_id,
            service_principal_client_secret=client_secret,
        )
    else:
        # Fallback for notebook/local context
        vsc = VectorSearchClient(workspace_url=w.config.host, personal_access_token=w.config.token)
    return vsc.get_index(endpoint_name=VS_ENDPOINT_NAME, index_name=VS_INDEX_NAME)


# ---------------------------------------------------------------------------
# Embedding functions
# ---------------------------------------------------------------------------
def normalize_vector(vec):
    arr = np.array(vec, dtype=np.float32)
    norm = np.linalg.norm(arr)
    return (arr / norm).tolist() if norm > 0 else arr.tolist()


def get_image_embedding_from_serving(image_bytes):
    import base64
    w = get_workspace_client()
    img_b64 = base64.b64encode(image_bytes).decode("utf-8")
    url = f"{w.config.host}/serving-endpoints/{EMBEDDING_ENDPOINT}/invocations"
    headers = {"Authorization": f"Bearer {w.config.token}", "Content-Type": "application/json"}
    payload = {"dataframe_records": [{"image_base64": img_b64}]}
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return normalize_vector(resp.json()["predictions"][0])


def get_simulated_embedding(image_bytes):
    try:
        img = Image.open(BytesIO(image_bytes)).convert("RGB")
        img_small = img.resize((32, 32))
        pixels = np.array(img_small, dtype=np.float32).flatten()
        seed = int(np.sum(pixels)) % (2**31)
        rng = np.random.RandomState(seed)
        emb = rng.randn(EMBEDDING_DIM).astype(np.float32)
        emb[0] = np.mean(pixels[0::3]) / 255.0 * 3
        emb[1] = np.std(pixels) / 255.0 * 3
        emb[2] = np.mean(pixels[0::3]) / 255.0 * 2
        emb[3] = np.mean(pixels[1::3]) / 255.0 * 2
        emb[4] = np.mean(pixels[2::3]) / 255.0 * 2
        return normalize_vector(emb.tolist())
    except Exception:
        return normalize_vector(np.random.RandomState(42).randn(EMBEDDING_DIM).tolist())


def get_embedding(image_bytes):
    try:
        return get_image_embedding_from_serving(image_bytes)
    except Exception:
        st.sidebar.warning("Usando embedding simulado (endpoint no disponible)")
        return get_simulated_embedding(image_bytes)


# ---------------------------------------------------------------------------
# Vector Search & Scoring
# ---------------------------------------------------------------------------
def search_similar_displays(query_vector, top_k=TOP_K, filters=None):
    idx = get_vs_index()
    params = {
        "query_vector": query_vector,
        "columns": ["display_id", "image_url", "brand", "store_type",
                     "region", "ideal_score", "compliance_score", "quality_label"],
        "num_results": top_k,
    }
    if filters:
        params["filters"] = filters
    return idx.similarity_search(**params)


def compute_display_score(search_results, w_neighbors=SCORE_WEIGHT_NEIGHBORS, w_model=SCORE_WEIGHT_MODEL):
    data = search_results.get("result", {}).get("data_array", [])
    if not data:
        return {"error": "No se encontraron vecinos similares", "final_score": 0}

    ideal_scores = [r[5] for r in data if r[5] is not None]
    compliance_scores = [r[6] for r in data if r[6] is not None]
    quality_labels = [r[7] for r in data if r[7] is not None]

    nn_ideal = float(np.mean(ideal_scores)) if ideal_scores else 0
    nn_compliance = float(np.mean(compliance_scores)) if compliance_scores else 0
    nn_score = (nn_ideal + nn_compliance) / 2.0

    quality_map = {"excelente": 95, "bueno": 75, "regular": 55, "deficiente": 30}
    from collections import Counter
    dominant = Counter(quality_labels).most_common(1)[0][0] if quality_labels else "N/A"
    model_score = quality_map.get(dominant, 50)

    final = w_neighbors * nn_score + w_model * model_score

    if final >= 85: cls = "EXCELENTE"
    elif final >= 70: cls = "BUENO"
    elif final >= 50: cls = "REGULAR"
    else: cls = "DEFICIENTE"

    return {
        "nn_ideal_score": round(nn_ideal, 2),
        "nn_compliance_score": round(nn_compliance, 2),
        "nn_combined_score": round(nn_score, 2),
        "model_score": model_score,
        "final_score": round(final, 2),
        "classification": cls,
        "dominant_quality": dominant,
        "neighbors_count": len(data),
        "quality_distribution": {q: quality_labels.count(q) for q in set(quality_labels)} if quality_labels else {},
    }


def load_reference_image(image_url):
    try:
        if image_url and image_url.startswith("/Volumes"):
            w = get_workspace_client()
            resp = w.files.download(image_url)
            return Image.open(BytesIO(resp.contents.read()))
        elif image_url and image_url.startswith("http"):
            resp = requests.get(image_url, timeout=10)
            return Image.open(BytesIO(resp.content))
    except Exception:
        pass
    return None


# ---------------------------------------------------------------------------
# UI Renderers
# ---------------------------------------------------------------------------
def render_score_card(score):
    cls = score["classification"].lower()
    color_map = {"excelente": "#10b981", "bueno": "#3b82f6", "regular": "#f59e0b", "deficiente": "#ef4444"}
    color = color_map.get(cls, "#888")
    st.markdown(f"""
    <div class="score-card">
        <div class="score-label">Puntuacion Final</div>
        <div class="score-value" style="color: {color};">{score['final_score']:.1f}</div>
        <div class="score-label">de 100</div>
        <div class="score-classification cls-{cls}">{score['classification']}</div>
    </div>
    """, unsafe_allow_html=True)


def render_score_breakdown(score, w_n, w_m):
    st.markdown("#### Desglose de Puntuacion")
    c1, c2, c3 = st.columns(3)
    c1.metric("Ideal (vecinos)", f"{score['nn_ideal_score']:.1f}")
    c2.metric("Compliance (vecinos)", f"{score['nn_compliance_score']:.1f}")
    c3.metric("Modelo (rubric)", f"{score['model_score']}")
    st.markdown(f"""
    <div class="formula-box">
        <strong>Formula:</strong><br>
        final = {w_n} x {score['nn_combined_score']:.2f} + {w_m} x {score['model_score']}
        = <strong>{score['final_score']:.2f}</strong>
    </div>
    """, unsafe_allow_html=True)


def render_explanation(score):
    cls = score["classification"]
    dominant = score["dominant_quality"]
    n = score["neighbors_count"]
    dist = score["quality_distribution"]
    dist_text = ", ".join([f"{c} {l}" for l, c in sorted(dist.items(), key=lambda x: x[1], reverse=True)])

    explanations = {
        "EXCELENTE": "Este display demuestra una ejecucion sobresaliente. Los displays mas similares son predominantemente excelentes.",
        "BUENO": "Este display tiene buena ejecucion general, con oportunidades menores de mejora.",
        "REGULAR": "Este display necesita atencion. Los displays similares muestran calidad mixta.",
        "DEFICIENTE": "Este display requiere accion inmediata. Es similar a displays con calificaciones bajas.",
    }
    recommendations = {
        "EXCELENTE": ["Mantener estandar actual como referencia", "Documentar como mejor practica", "Considerar para programa de reconocimiento"],
        "BUENO": ["Revisar senalizacion y precios", "Verificar stock completo", "Comparar con displays excelentes de la misma marca"],
        "REGULAR": ["Reorganizar productos", "Reponer stock faltante", "Instalar senalizacion actualizada", "Seguimiento en 48h"],
        "DEFICIENTE": ["Accion inmediata: reorganizar completamente", "Contactar representante de marca", "Verificar inventario", "Escalar a supervisor", "Re-auditoria en 24h"],
    }

    st.markdown("#### Razonamiento")
    st.info(f"**Clasificacion: {cls}**\n\n{explanations.get(cls, '')}\n\n"
            f"**Evidencia:** {n} displays de referencia ({dist_text}). Calidad dominante: **{dominant}**.")
    st.markdown("#### Recomendaciones")
    for rec in recommendations.get(cls, []):
        st.markdown(f"- {rec}")


def render_similar_displays(search_results):
    data = search_results.get("result", {}).get("data_array", [])
    if not data:
        st.warning("No se encontraron displays similares.")
        return
    st.markdown(f"#### Top {len(data)} Displays Similares")
    st.caption("Displays de referencia mas parecidos al evaluado.")
    cols = st.columns(min(len(data), 5))
    indicators = {"excelente": "\U0001F7E2", "bueno": "\U0001F535", "regular": "\U0001F7E1", "deficiente": "\U0001F534"}
    for i, (col, row) in enumerate(zip(cols, data)):
        display_id, image_url, brand, store_type, region, ideal, compliance, quality, *_ = row
        with col:
            img = load_reference_image(image_url)
            if img:
                st.image(img, use_column_width=True)
            else:
                st.markdown(f'<div style="background:#1a1a2e;border-radius:8px;padding:40px 10px;text-align:center;border:1px solid #2a2a4a;">'
                            f'<div style="font-size:2rem;">\U0001F5BC\uFE0F</div><div style="color:#666;font-size:0.75rem;">Display {display_id}</div></div>',
                            unsafe_allow_html=True)
            st.markdown(f"**#{i+1}** {indicators.get(quality, '')} {quality}")
            st.caption(f"{brand} | {store_type} | {region}")
            m1, m2 = st.columns(2)
            m1.metric("Ideal", f"{ideal:.0f}" if ideal else "-")
            m2.metric("Compl.", f"{compliance:.0f}" if compliance else "-")


def render_comparison_table(search_results):
    import pandas as pd
    data = search_results.get("result", {}).get("data_array", [])
    if not data: return
    st.markdown("#### Tabla Comparativa")
    df = pd.DataFrame([r[:8] for r in data], columns=["ID", "Imagen", "Marca", "Tienda", "Region", "Ideal", "Compliance", "Calidad"])
    df["#"] = range(1, len(df) + 1)
    st.dataframe(df[["#", "ID", "Marca", "Tienda", "Region", "Calidad", "Ideal", "Compliance"]],
                 hide_index=True)


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
def render_sidebar():
    with st.sidebar:
        st.markdown("### Configuracion")
        st.markdown("**Filtros de busqueda**")
        brand = st.selectbox("Marca", ["Todas", "Coca-Cola", "PepsiCo", "Nestle", "Bimbo"])
        store = st.selectbox("Tipo de tienda", ["Todas", "supermercado", "conveniencia"])
        st.markdown("---")
        st.markdown("**Parametros de puntuacion**")
        top_k = st.slider("Vecinos (K)", 3, 10, TOP_K)
        w_n = st.slider("Peso vecinos", 0.0, 1.0, SCORE_WEIGHT_NEIGHBORS, 0.05)
        w_m = round(1.0 - w_n, 2)
        st.caption(f"Peso modelo: {w_m}")
        st.markdown("---")
        st.caption(f"Indice: `{VS_INDEX_NAME}`")
        st.caption(f"Endpoint: `{VS_ENDPOINT_NAME}`")
        return {
            "brand": brand if brand != "Todas" else None,
            "store_type": store if store != "Todas" else None,
            "top_k": top_k, "w_n": w_n, "w_m": w_m,
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    st.markdown("""<div class="app-header"><h1>\U0001F4CA Image Scorer</h1>
    <p>Sistema de puntuacion de displays con Vector Search</p></div>""", unsafe_allow_html=True)
    cfg = render_sidebar()

    st.markdown("### \U0001F4E4 Subir imagen del display")
    st.caption("Sube una foto del display de productos que deseas evaluar.")
    uc, pc = st.columns([2, 1])
    with uc:
        uploaded = st.file_uploader("Arrastra o selecciona una imagen", type=["jpg", "jpeg", "png", "webp"])
    with pc:
        if uploaded:
            st.image(Image.open(uploaded), caption="Imagen a evaluar", use_column_width=True)

    if not uploaded:
        st.markdown("---")
        st.markdown("### Como funciona")
        steps = [("\u0031\uFE0F\u20E3", "Subir imagen", "Foto del display"),
                 ("\u0032\uFE0F\u20E3", "Embedding", "Vector numerico via CLIP"),
                 ("\u0033\uFE0F\u20E3", "Buscar", "Top-K vecinos en VS"),
                 ("\u0034\uFE0F\u20E3", "Puntuar", "Score + explicabilidad")]
        for col, (ic, t, d) in zip(st.columns(4), steps):
            col.markdown(f'<div style="text-align:center;padding:20px;"><div style="font-size:2.5rem;">{ic}</div>'
                         f'<div style="font-weight:700;margin:8px 0;">{t}</div><div style="color:#888;font-size:0.85rem;">{d}</div></div>',
                         unsafe_allow_html=True)
        return

    if st.button("\U0001F50D Evaluar Display", type="primary"):
        uploaded.seek(0)
        img_bytes = uploaded.read()
        with st.status("Procesando imagen...", expanded=True) as status:
            st.write("Generando embedding...")
            emb = get_embedding(img_bytes)
            st.write(f"Embedding: {len(emb)} dims")
            st.write("Buscando displays similares...")
            flt = {k: v for k, v in {"brand": cfg["brand"], "store_type": cfg["store_type"]}.items() if v}
            try:
                sr = search_similar_displays(emb, top_k=cfg["top_k"], filters=flt or None)
                n = len(sr.get("result", {}).get("data_array", []))
                st.write(f"Encontrados {n} displays similares")
            except Exception as e:
                st.error(f"Error en Vector Search: {e}")
                status.update(label="Error", state="error")
                return
            st.write("Calculando puntuacion...")
            sc = compute_display_score(sr, cfg["w_n"], cfg["w_m"])
            if "error" in sc and sc.get("final_score", 0) == 0:
                st.error(sc["error"])
                status.update(label="Error", state="error")
                return
            status.update(label="Completado", state="complete")
        st.session_state["score"] = sc
        st.session_state["sr"] = sr
        st.session_state["cfg"] = cfg

    if "score" in st.session_state:
        sc = st.session_state["score"]
        sr = st.session_state["sr"]
        c = st.session_state["cfg"]
        st.markdown("---")
        t1, t2, t3, t4 = st.tabs(["\U0001F4CA Puntuacion", "\U0001F5BC\uFE0F Similares", "\U0001F4A1 Explicabilidad", "\U0001F4CB Datos"])
        with t1:
            s1, s2 = st.columns([1, 2])
            with s1: render_score_card(sc)
            with s2:
                render_score_breakdown(sc, c["w_n"], c["w_m"])
                if sc.get("quality_distribution"):
                    import plotly.graph_objects as go
                    dist = sc["quality_distribution"]
                    cm = {"excelente": "#10b981", "bueno": "#3b82f6", "regular": "#f59e0b", "deficiente": "#ef4444"}
                    fig = go.Figure(data=[go.Bar(x=list(dist.keys()), y=list(dist.values()),
                                                 marker_color=[cm.get(k, "#888") for k in dist.keys()])])
                    fig.update_layout(title="Distribucion de calidad", height=300,
                                      margin=dict(t=40, b=20, l=40, r=20),
                                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                      font=dict(color="#ccc"))
                    st.plotly_chart(fig, use_container_width=True)
        with t2: render_similar_displays(sr)
        with t3: render_explanation(sc)
        with t4:
            render_comparison_table(sr)
            with st.expander("JSON completo"):
                st.json({"score": sc, "config": {"top_k": c["top_k"], "w_neighbors": c["w_n"], "w_model": c["w_m"]}})

if __name__ == "__main__":
    main()
'''

with open(f"{app_dir}/app.py", "w") as f:
    f.write(app_code)

print(f"app.py creado! ({len(app_code)} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 02 - Desplegar la Databricks App
# MAGIC
# MAGIC Ahora vamos a crear y desplegar la app en Databricks.
# MAGIC
# MAGIC ### Proceso de despliegue:
# MAGIC 1. **Crear** la app (registrarla en Databricks)
# MAGIC 2. **Desplegar** el codigo fuente
# MAGIC 3. **Esperar** a que el compute este listo
# MAGIC 4. **Acceder** a la URL de la app

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Crear la app

# COMMAND ----------

# DBTITLE 1,Crear la Databricks App
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import App

w = WorkspaceClient()
app_name = "image-scorer"

try:
    app = w.apps.create_and_wait(
        App(
            name=app_name,
            description="Sistema de puntuacion de displays de productos usando Vector Search con embeddings de imagenes"
        )
    )
    print(f"App creada: {app.name}")
    print(f"URL: {app.url}")
except Exception as e:
    if "already exists" in str(e).lower():
        app = w.apps.get(app_name)
        print(f"App ya existe: {app.name}")
        print(f"URL: {app.url}")
    else:
        raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Desplegar la app

# COMMAND ----------

# DBTITLE 1,Desplegar el codigo de la app
from databricks.sdk.service.apps import AppDeployment

print("Deployment iniciado, esperando a que este listo...")

deployment = w.apps.deploy_and_wait(
    app_name=app_name,
    app_deployment=AppDeployment(source_code_path=app_dir),
)

state = deployment.status.state.value if deployment.status else "UNKNOWN"
print(f"Deployment: {deployment.deployment_id}")
print(f"Estado: {state}")

if state == "SUCCEEDED":
    app_info = w.apps.get(app_name)
    print(f"\nApp desplegada exitosamente!")
    print(f"URL: {app_info.url}")
else:
    print(f"\nError en el deployment. Revise los logs en la consola de Apps.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Verificar que la app esta corriendo

# COMMAND ----------

# DBTITLE 1,Estado de la app
app_info = w.apps.get(app_name)

print(f"Nombre: {app_info.name}")
print(f"URL: {app_info.url}")
print(f"Estado compute: {app_info.compute_status.state.value if app_info.compute_status else 'N/A'}")
print(f"Service Principal: {app_info.service_principal_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 03 - Usar la App
# MAGIC
# MAGIC ### Abrir la app
# MAGIC
# MAGIC Haz clic en la URL proporcionada arriba para abrir la app en una nueva pestana.
# MAGIC
# MAGIC ### Flujo de uso:
# MAGIC
# MAGIC 1. **Subir imagen**: Arrastra o selecciona una foto de un display de productos
# MAGIC 2. **Configurar filtros** (opcional): En la barra lateral, filtra por marca o tipo de tienda
# MAGIC 3. **Ajustar parametros** (opcional): Modifica el numero de vecinos o los pesos de la formula
# MAGIC 4. **Evaluar**: Haz clic en el boton "Evaluar Display"
# MAGIC 5. **Explorar resultados**: Navega por las pestanas:
# MAGIC    - **Puntuacion**: Score card, desglose de la formula, distribucion de calidad
# MAGIC    - **Similares**: Galeria con los Top-5 displays de referencia mas parecidos
# MAGIC    - **Explicabilidad**: Razonamiento en lenguaje natural y recomendaciones de accion
# MAGIC    - **Datos**: Tabla comparativa y JSON de respuesta completa
# MAGIC
# MAGIC ### Pruebas sugeridas:
# MAGIC
# MAGIC 1. Sube una de las imagenes generadas en Lab 00 y verifica que la puntuacion coincide
# MAGIC 2. Filtra por "Coca-Cola" y observa como cambian los resultados
# MAGIC 3. Ajusta el peso de vecinos de 0.7 a 0.9 y nota la diferencia
# MAGIC 4. Sube una foto real de tu celular y observa la evaluacion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 04 - Configurar permisos de la App (si es necesario)
# MAGIC
# MAGIC La app necesita permisos para acceder a:
# MAGIC - **Model Serving endpoint** (para generar embeddings)
# MAGIC - **Vector Search endpoint** (para buscar similares)
# MAGIC - **Unity Catalog Volume** (para leer imagenes de referencia)
# MAGIC
# MAGIC Estos permisos se definen en `app.yaml` en la seccion `resources`.
# MAGIC Si la app no puede acceder a los endpoints, verificar los permisos del Service Principal.

# COMMAND ----------

# DBTITLE 1,Verificar permisos del Service Principal
print("Service Principal de la App:")
print(f"  Nombre: {app_info.service_principal_name}")
print(f"  Client ID: {app_info.service_principal_client_id}")
print()
print("Recursos configurados:")
if app_info.resources:
    for r in app_info.resources:
        print(f"  - {r.name}: {r}")
else:
    print("  (ninguno configurado via API - verificar app.yaml)")
print()
print("Si la app tiene errores de permisos, ejecutar:")
print(f"  1. Otorgar CAN_QUERY en el endpoint '{EMBEDDING_ENDPOINT}' al Service Principal")
print(f"  2. Otorgar CAN_QUERY en el endpoint '{VS_ENDPOINT_NAME}' al Service Principal")
print(f"  3. Otorgar USE SCHEMA en '{CATALOG}.{SCHEMA}' al Service Principal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ejercicio 05 - Limpiar la App (cuando termine el workshop)

# COMMAND ----------

# DBTITLE 1,Eliminar la app (solo si es necesario)
# ATENCION: Descomente solo cuando desee eliminar la app

# try:
#     w.apps.delete_and_wait(app_name)
#     print(f"App '{app_name}' eliminada.")
# except Exception as e:
#     print(f"Error eliminando app: {e}")

print(">> Descomente el codigo anterior para eliminar la app <<")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Felicidades!
# MAGIC
# MAGIC Has creado una **Databricks App** funcional para puntuacion de displays!
# MAGIC
# MAGIC ### Resumen de la App:
# MAGIC
# MAGIC | Componente | Detalle |
# MAGIC |-----------|---------|
# MAGIC | **Framework** | Streamlit |
# MAGIC | **Auth** | OAuth via Databricks Apps |
# MAGIC | **Embedding** | Model Serving (CLIP) con fallback simulado |
# MAGIC | **Busqueda** | Vector Search SDK (Standard endpoint) |
# MAGIC | **UI** | Upload, filtros, score card, galeria, explicabilidad |
# MAGIC | **Tabs** | Puntuacion, Similares, Explicabilidad, Datos |
# MAGIC
# MAGIC ### Workshop completo:
# MAGIC
# MAGIC | Lab | Tema |
# MAGIC |-----|------|
# MAGIC | 00 | Generacion de imagenes de referencia |
# MAGIC | 01 | Setup y datos de referencia en Delta |
# MAGIC | 02 | Embeddings de imagenes con CLIP/Model Serving |
# MAGIC | 03 | Endpoint e indice de Vector Search |
# MAGIC | 04 | Consulta, puntuacion y explicabilidad |
# MAGIC | 05 | Evaluacion de calidad, costos y limpieza |
# MAGIC | 06 | **App de Image Scorer (Databricks App)** |
