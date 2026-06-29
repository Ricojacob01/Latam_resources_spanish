
import os, streamlit as st
from databricks.sdk import WorkspaceClient
from mlflow.deployments import get_deploy_client

st.set_page_config(page_title="Comfama · Asistente al Afiliado", page_icon="🤖")
st.title("🤖 Asistente de Servicios al Afiliado")

AGENT_ENDPOINT = os.environ["AGENT_ENDPOINT"]

# OBO: token del usuario que abrió la app (lo inyecta Databricks Apps)
def user_token():
    try:
        return st.context.headers.get("X-Forwarded-Access-Token")
    except Exception:
        return None

afiliado_id = st.sidebar.number_input("Tu # de afiliado", min_value=1000, value=1001, step=1)

if "msgs" not in st.session_state:
    st.session_state.msgs = []

for m in st.session_state.msgs:
    st.chat_message(m["role"]).write(m["content"])

if prompt := st.chat_input("Pregunta por beneficios, programas o reserva un cupo..."):
    st.session_state.msgs.append({"role": "user", "content": prompt})
    st.chat_message("user").write(prompt)
    client = get_deploy_client("databricks")
    convo = [{"role": "system", "content": f"El afiliado autenticado es el id {afiliado_id}."}]
    convo += st.session_state.msgs
    resp = client.predict(endpoint=AGENT_ENDPOINT, inputs={"messages": convo})
    answer = resp["messages"][-1]["content"]
    st.session_state.msgs.append({"role": "assistant", "content": answer})
    st.chat_message("assistant").write(answer)
