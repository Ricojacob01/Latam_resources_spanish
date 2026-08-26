import logging
import os
import sys
from pathlib import Path

from databricks.sdk import WorkspaceClient
from dotenv import load_dotenv
from fastapi.staticfiles import StaticFiles
from mlflow.genai.agent_server import AgentServer, setup_mlflow_git_based_version_tracking

# Load env vars from .env before importing the agent for proper auth
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env", override=True)

# Route our own INFO diagnostics (resolved config, list counts) to stdout so they
# land in the app logs. The app's root logger only surfaces WARNING+, which
# silently dropped our INFO lines; give the agent_server package its own stdout
# handler at INFO and stop propagating (so warnings aren't double-logged).
_srv_logger = logging.getLogger("agent_server")
if not _srv_logger.handlers:
    _handler = logging.StreamHandler(sys.stdout)
    _handler.setFormatter(logging.Formatter("%(asctime)s [agent_server] %(levelname)s %(message)s"))
    _srv_logger.addHandler(_handler)
_srv_logger.setLevel(logging.INFO)
_srv_logger.propagate = False

# Need to import the agent to register the functions with the server
import agent_server.agent  # noqa: E402
from agent_server.agent import (  # noqa: E402
    gateway_status,
    get_model_service_detail,
    get_rate_limit,
    get_system_prompt_detail,
    list_model_services,
    log_resolved_config,
    palette_for_ui,
)

# Log the deploy-resolved catalog/schema/SP/prompt config once at startup, so the
# app logs confirm those env vars were pulled through as expected.
log_resolved_config()

# No chat proxy: this app serves its own minimal static chat UI directly from the
# agent server (single process, no separate frontend to forward to).
agent_server = AgentServer("ResponsesAgent")
# Define the app as a module level variable to enable multiple workers
app = agent_server.app  # noqa: F841
setup_mlflow_git_based_version_tracking()


@app.get("/api/model-services")
def model_services():
    """Model services the dropdown offers (the schema's ``<schema>.*`` wildcard)."""
    return {"modelServices": list_model_services()}


@app.get("/api/palette")
def palette():
    """Approved color palette for the sidebar swatches (primary + custom)."""
    return palette_for_ui()


@app.get("/api/gateway-status")
def gateway_status_route():
    """Whether the app can route through Unity AI Gateway yet — drives the overlay.

    Returns {"state": "ready" | "no_model_service" | "no_execute", ...}. The UI
    shows an onboarding overlay for the two non-ready states, walking the learner
    through creating a model service and granting the app SP EXECUTE on it.
    """
    return gateway_status()


@app.get("/api/host")
def host():
    """Workspace URL, so the overlay can link the learner into Unity AI Gateway."""
    return {"host": WorkspaceClient().config.host.rstrip("/")}


@app.get("/api/rate-limit")
def rate_limit(model: str):
    """Rate limit (QPM / tokens-per-minute) configured on a model service.

    Drives the sidebar usage gauge. The limit is service-level and per-minute;
    the UI tracks this session's own usage against it (there is no platform API
    for live remaining quota).
    """
    return get_rate_limit(model)


@app.get("/api/model-service-detail")
def model_service_detail(model: str):
    """Metadata about a model service for the "My Agent" panel.

    Returns where the service lives (catalog/schema) and what it routes to (the
    underlying foundation model), so the panel can show more than the service name.
    """
    return get_model_service_detail(model)


@app.get("/api/system-prompt")
def system_prompt():
    """The agent's system prompt for the "My Agent" panel.

    Returns the registered MLflow prompt (name, version, uri, template) when one is
    configured, else the built-in default with registered=false.
    """
    return get_system_prompt_detail()


@app.get("/api/experiment")
def experiment():
    """Workspace URL of the MLflow experiment the app logs traces to.

    Built from MLFLOW_EXPERIMENT_ID (set via app.yaml) + the workspace host. The
    UI shows a button linking here; the link opens in the user's own workspace
    session, so they see it only if they have permission on the experiment.
    Returns ``{"url": None}`` when no experiment is configured, so the UI hides
    the button rather than erroring.
    """
    exp_id = os.environ.get("MLFLOW_EXPERIMENT_ID")
    if not exp_id:
        return {"url": None}
    host = WorkspaceClient().config.host.rstrip("/")
    return {"url": f"{host}/ml/experiments/{exp_id}"}


# Serve the static chat UI at the root. Registered AFTER the agent server's API
# routes (/invocations, /health) and the route above, so those explicit routes
# match first; the root mount only handles the UI (/, /index.html).
_static_dir = Path(__file__).parent / "static"
app.mount("/", StaticFiles(directory=str(_static_dir), html=True), name="static")


def main():
    agent_server.run(app_import_string="agent_server.start_server:app")
