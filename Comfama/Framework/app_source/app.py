import streamlit as st
import requests
import os
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Asistente Comfama", page_icon="🤝", layout="wide")

# ──────────────────────────────────────────────────────────
# Auth
# ──────────────────────────────────────────────────────────
def get_databricks_auth():
    w = WorkspaceClient()
    host = w.config.host or os.environ.get("DATABRICKS_HOST", "")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    try:
        headers = w.config.authenticate()
        auth_val = headers.get("Authorization", "")
        if auth_val.startswith("Bearer "):
            return host, auth_val.split(" ", 1)[1]
    except Exception:
        pass
    return host, w.config.token or ""


@st.cache_data(ttl=60)
def get_databricks_host():
    return get_databricks_auth()[0]


SERVING_ENDPOINT = "agente_comfama"
CATALOG = "ardemo_classic_dnubtw_catalog"
SCHEMA = "comfama"
FULL_SCHEMA = f"{CATALOG}.{SCHEMA}"

# ──────────────────────────────────────────────────────────
# Sidebar — navigation + branding
# ──────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🤝 Comfama")
    st.markdown("**Asistente Virtual**")
    st.markdown("---")
    page = st.radio(
        "Páginas",
        ["💬 Chat", "🏗️ Arquitectura", "📊 Estado en vivo"],
        label_visibility="collapsed",
    )
    st.markdown("---")
    st.caption("Powered by Databricks")
    st.caption("Mosaic AI · Vector Search · Unity Catalog")


# ──────────────────────────────────────────────────────────
# Helper: Databricks SQL query
# ──────────────────────────────────────────────────────────
def run_sql(stmt, wh_id="115ac536f6a2927c"):
    """Execute SQL against the workspace's warehouse. Returns list of dicts."""
    host, token = get_databricks_auth()
    resp = requests.post(
        f"{host}/api/2.0/sql/statements",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"warehouse_id": wh_id, "statement": stmt, "wait_timeout": "30s"},
        timeout=45,
    )
    if resp.status_code != 200:
        return None, f"HTTP {resp.status_code}: {resp.text[:200]}"
    data = resp.json()
    if data.get("status", {}).get("state") != "SUCCEEDED":
        return None, data.get("status", {}).get("error", {}).get("message", "unknown error")
    cols = [c["name"] for c in data.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = data.get("result", {}).get("data_array", []) or []
    return [dict(zip(cols, r)) for r in rows], None


# ──────────────────────────────────────────────────────────
# PAGE: Chat
# ──────────────────────────────────────────────────────────
if page == "💬 Chat":
    st.title("Asistente Virtual Comfama")
    st.caption("Pregunta sobre subsidios, servicios de salud, créditos y más")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    if st.button("🗑️ Limpiar conversación"):
        st.session_state.messages = []

    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
            if msg.get("sources"):
                with st.expander("📎 Fuentes consultadas"):
                    for src in msg["sources"]:
                        st.markdown(f"- **{src['titulo']}** (score: {src.get('score',0):.2f})")

    if prompt := st.chat_input("¿En qué te puedo ayudar?"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        with st.chat_message("assistant"):
            with st.spinner("Consultando..."):
                try:
                    host, token = get_databricks_auth()
                    url = f"{host}/serving-endpoints/{SERVING_ENDPOINT}/invocations"
                    resp = requests.post(
                        url,
                        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                        json={"dataframe_records": [{"query": prompt}]},
                        timeout=60,
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        pred = data.get("predictions", [{}])[0]
                        answer = pred.get("answer", "Sin respuesta")
                        sources = pred.get("sources", [])
                        st.markdown(answer)
                        if sources:
                            with st.expander("📎 Fuentes consultadas"):
                                for src in sources:
                                    st.markdown(f"- **{src['titulo']}** (score: {src.get('score',0):.2f})")
                        st.session_state.messages.append({
                            "role": "assistant", "content": answer, "sources": sources,
                        })
                    else:
                        error_msg = f"❌ Error del endpoint: HTTP {resp.status_code}\n```\n{resp.text[:300]}\n```"
                        st.error(error_msg)
                        st.session_state.messages.append({"role": "assistant", "content": error_msg})
                except Exception as e:
                    st.error(f"❌ Error: {e}")
                    st.session_state.messages.append({"role": "assistant", "content": f"Error: {e}"})


# ──────────────────────────────────────────────────────────
# PAGE: Arquitectura
# ──────────────────────────────────────────────────────────
elif page == "🏗️ Arquitectura":
    st.title("🏗️ Arquitectura del Agente")
    st.caption("Componentes desplegados, sus relaciones, y trazabilidad de datos")

    # Architecture diagram (graphviz)
    st.subheader("Diagrama de componentes")
    graph_dot = """
digraph Architecture {
    rankdir=LR;
    bgcolor="white";
    node [shape=box, style="rounded,filled", fontname="Helvetica", margin=0.2];
    edge [color="#1B5161", fontname="Helvetica", fontsize=10];

    user [label="👤 Usuario", fillcolor="#FFF4E5", color="#FFAB00"];
    app [label="📱 Databricks App\\n(Streamlit)", fillcolor="#E8F2F4", color="#1B5161"];
    serving [label="🚀 Model Serving\\n(agente_comfama)\\nFM API Llama-3.3-70B", fillcolor="#E8F2F4", color="#1B5161"];
    agent [label="🧠 Mosaic AI Agent\\n(PyFunc registered in UC)", fillcolor="#E8F2F4", color="#1B5161"];
    vs [label="🔍 Vector Search\\n(documentos_index)", fillcolor="#E8F2F4", color="#1B5161"];
    delta [label="📄 Delta Table\\n(documentos_subsidios)", fillcolor="#E8F2F4", color="#1B5161"];
    inference [label="📊 Inference Table\\n(agente_inference_payload)\\nAI Gateway auto-capture", fillcolor="#FFE8E5", color="#98102A"];
    mlflow [label="📈 MLflow Tracing\\n(experimento)", fillcolor="#FFE8E5", color="#98102A"];
    uc [label="🛡️ Unity Catalog\\n(governance + lineage)", fillcolor="#F0F0F0", color="#618793"];
    monitor [label="🔬 Lakehouse Monitor\\n(metricas_agente_gold)", fillcolor="#FFE8E5", color="#98102A"];

    user -> app [label="1. HTTPS SSO"];
    app -> serving [label="2. invoke"];
    serving -> agent;
    agent -> vs [label="3. retrieve"];
    agent -> serving [label="4. FM API call", style="dashed"];
    vs -> delta [label="delta-sync"];
    serving -> inference [label="auto-capture", style="dotted", color="#98102A"];
    agent -> mlflow [label="traces", style="dotted", color="#98102A"];
    uc -> delta [style="dotted", color="#618793"];
    uc -> vs [style="dotted", color="#618793"];
    uc -> agent [style="dotted", color="#618793"];
    inference -> monitor [style="dotted", color="#98102A"];
}
    """
    st.graphviz_chart(graph_dot, use_container_width=True)

    # Component status
    st.subheader("Estado de cada componente")
    col1, col2, col3, col4 = st.columns(4)
    host = get_databricks_host()
    headers = {"Authorization": f"Bearer {get_databricks_auth()[1]}"}

    # Model Serving endpoint
    try:
        r = requests.get(f"{host}/api/2.0/serving-endpoints/agente_comfama", headers=headers, timeout=10)
        if r.status_code == 200:
            state = r.json().get("state", {}).get("ready", "?")
            col1.metric("🚀 Model Serving", state)
        else:
            col1.metric("🚀 Model Serving", "?")
    except Exception as e:
        col1.metric("🚀 Model Serving", "err")

    # Vector Search
    try:
        r = requests.get(f"{host}/api/2.0/vector-search/indexes/ardemo_classic_dnubtw_catalog.comfama.documentos_index",
                         headers=headers, timeout=10)
        if r.status_code == 200:
            d = r.json().get("status", {})
            col2.metric("🔍 Vector Search",
                        "READY" if d.get("ready") else "?",
                        f"{d.get('indexed_row_count', '?')} docs")
    except Exception:
        col2.metric("🔍 Vector Search", "err")

    # App itself
    col3.metric("📱 App", "RUNNING", "ese eres tú 😉")

    # Inference table count
    rows, _ = run_sql(f"SELECT COUNT(*) AS n FROM {FULL_SCHEMA}.agente_inference_payload")
    n = rows[0]["n"] if rows else "?"
    col4.metric("📊 Inference rows", n)

    # Lineage
    st.subheader("Lineage de datos (system.access.table_lineage)")
    rows, err = run_sql(f"""
        SELECT
          source_table_full_name AS source,
          target_table_full_name AS target,
          source_type,
          MAX(event_time) AS last_access
        FROM system.access.table_lineage
        WHERE source_table_full_name LIKE '{FULL_SCHEMA}.%'
           OR target_table_full_name LIKE '{FULL_SCHEMA}.%'
        GROUP BY source, target, source_type
        ORDER BY last_access DESC
        LIMIT 20
    """)
    if rows:
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    elif err:
        st.warning(f"Lineage no disponible: {err}")
    else:
        st.info("Aún no hay lineage capturado. Ejecuta queries que toquen las tablas para generarlo.")

    # Schema map — todas las tablas
    st.subheader("Schema map")
    rows, _ = run_sql(f"""
        SELECT table_name, table_type, table_owner, created, comment
        FROM {CATALOG}.information_schema.tables
        WHERE table_schema = '{SCHEMA}'
        ORDER BY created DESC
    """)
    if rows:
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Tags
    st.subheader("Governance tags")
    rows, _ = run_sql(f"""
        SELECT table_name, column_name, tag_name, tag_value
        FROM {CATALOG}.information_schema.column_tags
        WHERE schema_name = '{SCHEMA}'
        ORDER BY table_name, column_name
    """)
    if rows:
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("No hay tags aplicados — ejecuta notebook 03 para agregarlos.")

    # Direct links
    st.subheader("🔗 Links directos a la UI")
    st.markdown(f"""
- **Endpoint del agente:** [{host}/ml/endpoints/agente_comfama]({host}/ml/endpoints/agente_comfama)
- **Vector Search index:** [Catalog → comfama → documentos_index]({host}/explore/data/{CATALOG}/{SCHEMA}/documentos_index)
- **Modelo registrado:** [models/comfama.agente_comfama]({host}/explore/data/models/{CATALOG}/{SCHEMA}/agente_comfama)
- **Inference Table:** [agente_inference_payload]({host}/explore/data/{CATALOG}/{SCHEMA}/agente_inference_payload)
- **Lakeview Dashboard:** [Control Center]({host}/dashboardsv3/01f15f643ca6152e87a895360f744ecd)
- **Lakehouse Monitor:** [metricas_agente_gold → tab Quality]({host}/explore/data/{CATALOG}/{SCHEMA}/metricas_agente_gold)
""")


# ──────────────────────────────────────────────────────────
# PAGE: Estado en vivo
# ──────────────────────────────────────────────────────────
elif page == "📊 Estado en vivo":
    st.title("📊 Estado en vivo")
    st.caption("Métricas en tiempo real desde la inference table")

    col1, col2, col3, col4 = st.columns(4)

    # KPIs hoy
    rows, _ = run_sql(f"""
        SELECT
          COUNT(*) AS total,
          ROUND(AVG(execution_duration_ms), 0) AS lat_avg,
          ROUND(percentile(execution_duration_ms, 0.95), 0) AS p95,
          SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS errores
        FROM {FULL_SCHEMA}.agente_inference_payload
        WHERE request_date = current_date()
    """)
    if rows:
        r = rows[0]
        col1.metric("Requests hoy", r.get("total", 0))
        col2.metric("Latencia avg (ms)", r.get("lat_avg") or 0)
        col3.metric("P95 (ms)", r.get("p95") or 0)
        col4.metric("Errores", r.get("errores") or 0)

    st.divider()

    # Últimas inferencias
    st.subheader("Últimas inferencias")
    rows, _ = run_sql(f"""
        SELECT
          request_time,
          requester,
          status_code,
          execution_duration_ms AS latency_ms,
          substring(request, 1, 80) AS request_preview,
          substring(response, 1, 80) AS response_preview
        FROM {FULL_SCHEMA}.agente_inference_payload
        ORDER BY request_time DESC
        LIMIT 10
    """)
    if rows:
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    else:
        st.info("Aún no hay inferencias. Haz una pregunta en la página de Chat.")
