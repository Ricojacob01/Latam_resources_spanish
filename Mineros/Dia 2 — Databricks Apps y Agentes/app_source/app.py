"""
Databricks App — Panel de Pedidos + Notas + Chatbot Genie
=========================================================
App Streamlit que:
  1. Muestra un panel de analítica (solo lectura) sobre las tablas del Día 1.
  2. Escribe notas de seguimiento (write-back) sobre una tabla propia de la app.
  3. Integra el espacio Genie del Día 1 como chatbot.

Autenticación: usa el Service Principal de la App (sin tokens hardcodeados).
Configura las variables en app.yaml (GENIE_SPACE_ID, SQL_HTTP_PATH, CATALOG, SCHEMA).

Nota: la app NO modifica las tablas del pipeline (Lakeflow, solo lectura). El
write-back apunta a `app_notas_clientes`, una tabla propia creada en el notebook.
"""

import os
import pandas as pd
import streamlit as st
from databricks import sql
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# -----------------------------------------------------------------------------
# Configuración (desde variables de entorno definidas en app.yaml)
# -----------------------------------------------------------------------------
CATALOG = os.getenv("CATALOG", "academia")              # catálogo compartido
SCHEMA = os.getenv("SCHEMA", "tu_apellido")             # tu esquema del Día 1
SQL_HTTP_PATH = os.getenv("SQL_HTTP_PATH", "")          # /sql/1.0/warehouses/xxxx
GENIE_SPACE_ID = os.getenv("GENIE_SPACE_ID", "")        # ID del espacio Genie (Día 1)

# Prefijo de esquema totalmente calificado para las consultas
NS = f"{CATALOG}.{SCHEMA}"
NOTES_TABLE = f"{NS}.app_notas_clientes"                # tabla de write-back (creada en el notebook)

cfg = Config()  # Resuelve host + credenciales del Service Principal de la App
st.set_page_config(page_title="Pedidos & Clientes · Genie", layout="wide")
st.title("📦 Panel de Pedidos y Clientes")
st.caption("Datos del pipeline Lakeflow (Día 1) · Notas · Chatbot con Genie")


# -----------------------------------------------------------------------------
# Conexión al SQL Warehouse (cacheada)
# -----------------------------------------------------------------------------
@st.cache_resource(ttl="1h")
def get_connection(http_path: str):
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def run_query(query: str, conn) -> pd.DataFrame:
    with conn.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


def execute(statement: str, conn, params=None) -> None:
    """Ejecuta una sentencia sin resultado (MERGE/INSERT/UPDATE). Usa parámetros
    para evitar inyección de SQL con la entrada del usuario."""
    with conn.cursor() as cursor:
        cursor.execute(statement, params or {})


# -----------------------------------------------------------------------------
# Panel de analítica (solo lectura)
# -----------------------------------------------------------------------------
http_path = SQL_HTTP_PATH or st.text_input(
    "HTTP Path del SQL Warehouse:", placeholder="/sql/1.0/warehouses/xxxxxx"
)

if http_path:
    conn = get_connection(http_path)

    # KPIs
    resumen = run_query(
        f"SELECT COUNT(*) AS total_pedidos, "
        f"COUNT(DISTINCT customer_id) AS clientes FROM {NS}.orders_silver",
        conn,
    )
    c1, c2 = st.columns(2)
    c1.metric("Pedidos totales", int(resumen["total_pedidos"][0]))
    c2.metric("Clientes con pedidos", int(resumen["clientes"][0]))

    # Tendencia diaria (Gold)
    st.subheader("Pedidos por día")
    tendencia = run_query(
        f"SELECT order_date, total_daily_orders "
        f"FROM {NS}.order_summary_gold ORDER BY order_date",
        conn,
    )
    if not tendencia.empty:
        st.line_chart(tendencia.set_index("order_date")["total_daily_orders"])

    # Pedidos por ciudad (JOIN)
    st.subheader("Pedidos por ciudad")
    por_ciudad = run_query(
        f"""
        SELECT c.city, COUNT(o.order_id) AS total_pedidos,
               COUNT(DISTINCT o.customer_id) AS clientes_unicos
        FROM {NS}.orders_silver o
        JOIN {NS}.customers_silver c ON o.customer_id = c.customer_id
        GROUP BY c.city ORDER BY total_pedidos DESC
        """,
        conn,
    )
    if not por_ciudad.empty:
        st.bar_chart(por_ciudad.set_index("city")["total_pedidos"])
        st.dataframe(por_ciudad, use_container_width=True)

    # -------------------------------------------------------------------------
    # Notas de seguimiento (write-back sobre la tabla propia de la app)
    # -------------------------------------------------------------------------
    st.markdown("---")
    st.subheader("📝 Notas de seguimiento del cliente")
    st.caption(
        "Escribe sobre `app_notas_clientes` (tabla propia de la app). "
        "Las tablas del pipeline son de solo lectura."
    )

    with st.form("form_nota", clear_on_submit=True):
        col_a, col_b = st.columns([2, 1])
        f_customer = col_a.text_input("Customer ID", placeholder="Ej.: CUST0001")
        f_prioridad = col_b.selectbox("Prioridad", ["baja", "media", "alta"], index=1)
        f_nota = st.text_area("Nota", placeholder="Ej.: Cliente pidió cambio; contactar la próxima semana.")
        enviado = st.form_submit_button("Guardar nota")

    if enviado:
        if not f_customer.strip() or not f_nota.strip():
            st.warning("Ingresa un Customer ID y una nota.")
        else:
            # MERGE idempotente (upsert por customer_id). Parámetros → sin inyección de SQL.
            execute(
                f"""
                MERGE INTO {NOTES_TABLE} t
                USING (SELECT :cid AS customer_id, :nota AS nota,
                              :prio AS prioridad, current_user() AS autor,
                              current_timestamp() AS actualizado) s
                ON t.customer_id = s.customer_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """,
                conn,
                params={"cid": f_customer.strip(), "nota": f_nota.strip(), "prio": f_prioridad},
            )
            st.success(f"Nota guardada para {f_customer.strip()}.")

    # Mostrar las notas existentes
    notas = run_query(
        f"SELECT customer_id, prioridad, nota, autor, actualizado "
        f"FROM {NOTES_TABLE} ORDER BY actualizado DESC",
        conn,
    )
    if not notas.empty:
        st.dataframe(notas, use_container_width=True)
    else:
        st.info("Aún no hay notas registradas.")


# -----------------------------------------------------------------------------
# Chatbot Genie
# -----------------------------------------------------------------------------
st.markdown("---")
st.subheader("💬 Pregúntale a tus datos (Genie)")

if not GENIE_SPACE_ID:
    st.info("Configura GENIE_SPACE_ID en app.yaml para habilitar el chat.")
else:
    w = WorkspaceClient()

    def display_message(msg):
        if msg.get("content"):
            st.markdown(msg["content"])
        if msg.get("data") is not None:
            st.dataframe(msg["data"], use_container_width=True)
        if msg.get("code"):
            with st.expander("Ver SQL generado"):
                st.code(msg["code"], language="sql")

    def query_result(statement_id):
        result = w.statement_execution.get_statement(statement_id)
        return pd.DataFrame(
            result.result.data_array,
            columns=[c.name for c in result.manifest.schema.columns],
        )

    def process(conversation):
        for a in conversation.attachments:
            if a.text:
                display_message({"content": a.text.content})
            elif a.query:
                data = query_result(conversation.query_result.statement_id)
                display_message(
                    {"content": a.query.description, "data": data, "code": a.query.query}
                )

    if prompt := st.chat_input("Ej.: ¿Cuántos pedidos hubo por ciudad?"):
        st.chat_message("user").markdown(prompt)
        with st.chat_message("assistant"):
            conv_id = st.session_state.get("conversation_id")
            if conv_id:
                conv = w.genie.create_message_and_wait(GENIE_SPACE_ID, conv_id, prompt)
            else:
                conv = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, prompt)
                st.session_state["conversation_id"] = conv.conversation_id
            process(conv)
