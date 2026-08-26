import logging
import os
from contextlib import AsyncExitStack
from datetime import datetime
from pathlib import Path
from typing import AsyncGenerator
from uuid import uuid4

import mlflow
from agents import (
    Agent,
    ModelSettings,
    Runner,
    function_tool,
    set_default_openai_api,
    set_default_openai_client,
)
from agents.tracing import set_trace_processors
from openai.types.shared import Reasoning
from databricks.sdk import WorkspaceClient
from databricks_openai import AsyncDatabricksOpenAI
from databricks_openai.agents import McpServer
from mlflow.genai.agent_server import invoke, stream
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
)

from agent_server.utils import (
    build_mcp_url,
    get_session_id,
    get_user_workspace_client,
    process_agent_stream_events,
)

logger = logging.getLogger(__name__)
# Emit our INFO diagnostics (resolved config, list counts) to the app logs. The
# module logger otherwise defaults to WARNING, which hid the startup config we
# want visible when verifying a deploy in a new workspace.
logger.setLevel(logging.INFO)

# Which Unity Catalog schema(s) the UI dropdown may pick model services from
# (comma-separated "catalog.schema"), set at deploy time by AppDeployer. The user
# chooses a model service per request; we only honor it if it lives in one of these
# schemas — governance in the app layer (defense in depth on top of the UC EXECUTE
# grants the gateway enforces). There is no configured default model service: the
# learner creates the services and grants access themselves (a hands-on lab step).
ALLOWED_MODEL_SERVICE_SCHEMAS = {
    s.strip()
    for s in os.environ.get("MODEL_SERVICE_SCHEMAS", "").split(",")
    if s.strip()
}

# Route through Unity AI Gateway whenever the app is configured with model-service
# schema(s). When on, the Databricks OpenAI client routes calls through the gateway
# control plane ({host}/ai-gateway/...) — where rate limits, budgets, usage tracking,
# and service policies are enforced — instead of hitting a serving endpoint directly.
# Access is governed by a UC EXECUTE grant on the model service to the app's service
# principal (a hands-on lab step; until it's granted the app shows an onboarding
# overlay). For local dev (no schemas configured), fall back to a direct call.
USE_AI_GATEWAY = bool(ALLOWED_MODEL_SERVICE_SCHEMAS)
DEFAULT_MODEL = ""  # local-dev fallback only (gateway off)


# The UC model-services list API is METASTORE-WIDE and paginated. Two behaviors,
# both verified against the live gateway, drive how we call it:
#   1. Its catalog_name/schema_name query params are SILENTLY IGNORED — a bogus
#      filter still returns every service in the metastore. So we scope to the
#      allowed schema(s) CLIENT-SIDE via the ``<catalog>.<schema>`` prefix match.
#   2. It PAGINATES via next_page_token, and returns FEWER than page_size per page
#      (it post-filters each page by the caller's permissions). A page can even
#      come back empty while more pages remain — so we loop strictly on the
#      presence of next_page_token, never stopping early on an empty page.
# Without paginating, the app only saw page 1 (dominated by system.ai and other
# users' services in a shared classroom metastore) and a learner's own service in
# their course schema was missed entirely — leaving the onboarding overlay stuck
# and the model card empty even after the service existed.
_LIST_PAGE_SIZE = 100  # API caps page_size at 100; larger values are rejected (InvalidParameterValue, HTTP 400)
_LIST_MAX_PAGES = 400  # safety cap against a runaway token loop (~40k services)


def list_model_services() -> list[dict]:
    """List the Unity AI Gateway model services the user can choose from.

    Pages through the metastore-wide Unity Catalog model-services API (the server
    filter is ignored — see the note above) and keeps only services that live in
    an allowed schema (the ``<schema>.*`` wildcard). Returns a list of
    ``{"fullName": catalog.schema.name, "label": name}`` dicts, sorted by label.
    Returns an empty list if nothing is configured or on error; the onboarding
    overlay then guides the learner to create a model service.
    """
    services: dict[str, dict] = {}
    w = WorkspaceClient()
    page_token = None
    pages = 0
    scanned = 0
    for _ in range(_LIST_MAX_PAGES):
        query: dict = {"page_size": _LIST_PAGE_SIZE}
        if page_token:
            query["page_token"] = page_token
        try:
            resp = w.api_client.do(
                "GET", "/api/2.1/unity-catalog/model-services", query=query
            )
        except Exception:
            logger.warning("Failed to list model services", exc_info=True)
            break
        if not isinstance(resp, dict):
            break
        pages += 1
        for svc in resp.get("model_services", []):
            scanned += 1
            full = (svc.get("name") or "").removeprefix("model-services/")
            if full.count(".") != 2:
                continue
            catalog, schema, _ = full.split(".")
            # Keep only services that actually live in an allowed schema.
            if f"{catalog}.{schema}" in ALLOWED_MODEL_SERVICE_SCHEMAS:
                services[full] = {"fullName": full, "label": full.rsplit(".", 1)[-1]}
        page_token = resp.get("next_page_token")
        if not page_token:
            break
    # So the app logs show the catalog/schema filter working: how many services
    # the metastore-wide list returned vs. how many matched the allowed schema(s).
    logger.info(
        "list_model_services: scanned %d service(s) over %d page(s); %d matched allowed schema(s) %s -> %s",
        scanned,
        pages,
        len(services),
        sorted(ALLOWED_MODEL_SERVICE_SCHEMAS),
        sorted(services),
    )
    return sorted(services.values(), key=lambda m: m["label"])


def get_rate_limit(full_name: str) -> dict:
    """Return the Unity AI Gateway rate limit configured on a model service.

    Reads the model service's ``config.rate_limits`` from the UC model-services
    API. The limit is service-level (all callers share it), renewed per minute.
    Returns ``{"requests": int|None, "tokens": int|None, "renewalPeriod": str}``
    — either value is None when that limit isn't configured. On error / when the
    service isn't in an allowed schema, returns all-None so the UI hides the gauge.
    """
    empty = {"requests": None, "tokens": None, "renewalPeriod": "minute"}
    if full_name.count(".") != 2:
        return empty
    catalog, schema, _ = full_name.split(".")
    if f"{catalog}.{schema}" not in ALLOWED_MODEL_SERVICE_SCHEMAS:
        return empty
    try:
        resp = WorkspaceClient().api_client.do(
            "GET", f"/api/2.1/unity-catalog/model-services/{full_name}"
        )
    except Exception:
        logger.warning("Failed to read rate limit for %s", full_name, exc_info=True)
        return empty

    # A non-dict response (e.g. an empty body coming back as None) would make the
    # .get() chain below raise; degrade to all-None like every other error path.
    if not isinstance(resp, dict):
        return empty

    # config.rate_limits is a list; the gateway returns requests and tokens as
    # SEPARATE entries (e.g. one {requests: "2"} and one {tokens: "200"}), so we
    # merge across all entries rather than reading only the first.
    limits = (resp.get("config") or {}).get("rate_limits") or []
    result = dict(empty)
    for rl in limits:
        # API returns strings like "2"; renewal_period like RATE_LIMIT_RENEWAL_PERIOD_MINUTE.
        # Guard int() so a non-numeric value leaves that limit unset instead of 500-ing.
        try:
            if rl.get("requests") is not None:
                result["requests"] = int(rl["requests"])
            if rl.get("tokens") is not None:
                result["tokens"] = int(rl["tokens"])
        except (TypeError, ValueError):
            logger.warning("Non-numeric rate limit on %s: %r", full_name, rl, exc_info=True)
        period = (rl.get("renewal_period") or "").lower()
        if period:
            result["renewalPeriod"] = "minute" if "minute" in period else period
    return result


def get_model_service_detail(full_name: str) -> dict:
    """Return metadata about a model service for the "My Agent" panel.

    A model service (``catalog.schema.name``) is a Unity Catalog securable that
    routes to an underlying model — the service name (e.g. ``…gpt-5-5``) is just a
    label and can point at any foundation model. We read the single-service UC API
    (same call as ``get_rate_limit``) and surface where the service lives and what
    it actually routes to:

      { "fullName", "catalog", "schema", "modelName",   # from the name
        "underlyingModel", "destinationType", "id" }    # from config.routing

    Any missing piece is None so the UI can hide that row. On error / out-of-schema
    / malformed name, returns catalog+schema+modelName from the name where possible
    and None for the rest, so the panel still renders.
    """
    empty = {
        "fullName": full_name,
        "catalog": None,
        "schema": None,
        "modelName": None,
        "underlyingModel": None,
        "destinationType": None,
        "id": None,
    }
    if full_name.count(".") != 2:
        return empty
    catalog, schema, name = full_name.split(".")
    result = dict(empty, catalog=catalog, schema=schema, modelName=name)
    if f"{catalog}.{schema}" not in ALLOWED_MODEL_SERVICE_SCHEMAS:
        return result
    try:
        resp = WorkspaceClient().api_client.do(
            "GET", f"/api/2.1/unity-catalog/model-services/{full_name}"
        )
    except Exception:
        logger.warning("Failed to read detail for %s", full_name, exc_info=True)
        return result

    result["id"] = resp.get("id")
    # config.routing.destinations is a list of weighted targets; pick the one
    # carrying 100% traffic if present, else the first. The underlying model is in
    # pay_per_token_config.model (e.g. "models/system.ai.databricks-gpt-5-nano") or
    # the destination's own name; strip the "models/" prefix for display.
    destinations = ((resp.get("config") or {}).get("routing") or {}).get("destinations") or []
    if destinations:
        dest = next(
            (d for d in destinations if d.get("traffic_percentage") == 100),
            destinations[0],
        )
        model = (dest.get("pay_per_token_config") or {}).get("model") or dest.get("name")
        if model:
            result["underlyingModel"] = model.removeprefix("models/")
        result["destinationType"] = dest.get("destination_type")
    return result


# The app's own service principal id (application/client id), injected at deploy
# time by AppDeployer as an env var. The running app can't reliably discover its
# own SP identity via the SDK, so we pass it in. Absent in local dev.
APP_SERVICE_PRINCIPAL_ID = os.environ.get("APP_SERVICE_PRINCIPAL_ID")


def _sp_has_execute(full_name: str, sp_id: str) -> bool:
    """Whether service principal ``sp_id`` effectively holds EXECUTE on a model service.

    This is the access-policy half of Unity AI Gateway governance (the UC grant),
    the same "Can they reach it?" check the lecture's section D diagram describes.
    We read *effective* permissions (direct + inherited) so a grant made on EITHER
    the model service itself OR its parent schema counts — the schema-level grant is
    inherited by every service in it. Returns True if either securable grants EXECUTE.

    Best-effort: any API error returns False (treated as "not granted"), so the
    onboarding overlay stays up and the learner is guided to grant it — the intended
    hands-on flow — rather than the app silently proceeding.
    """
    if full_name.count(".") != 2:
        return False
    catalog, schema, _ = full_name.split(".")
    parent_schema = f"{catalog}.{schema}"
    # (securable_type, securable_full_name). The schema probe is the well-supported
    # path; the model-service securable is attempted additively (Beta API — the exact
    # type token may need confirmation against the live workspace).
    targets = [
        ("schema", parent_schema),
        ("model-service", full_name),
    ]
    w = WorkspaceClient()
    for securable_type, name in targets:
        try:
            resp = w.api_client.do(
                "GET",
                f"/api/2.1/unity-catalog/effective-permissions/{securable_type}/{name}",
                query={"principal": sp_id},
            )
        except Exception:
            logger.warning(
                "Effective-permissions probe failed for %s '%s'",
                securable_type,
                name,
                exc_info=True,
            )
            continue
        if not isinstance(resp, dict):
            continue
        for assignment in resp.get("privilege_assignments") or []:
            for priv in assignment.get("privileges") or []:
                # Entries may be plain strings or {"privilege": "EXECUTE", ...} dicts.
                value = priv.get("privilege") if isinstance(priv, dict) else priv
                if (value or "").upper() == "EXECUTE":
                    return True
    return False


def gateway_status() -> dict:
    """Report whether the app is ready to route calls through Unity AI Gateway.

    Drives the onboarding overlay. Three states:
      - ``ready``            — the app can call a model service (or local dev, no gateway).
      - ``no_model_service`` — no model service exists yet in the allowed schema(s);
                               the learner must create one in Unity AI Gateway.
      - ``no_execute``       — a model service exists but the app's service principal
                               lacks EXECUTE; the learner must grant it.

    The two non-ready states map to the two hands-on steps in lab §B1: create the
    model service, then grant the app SP access to it.
    """
    # Local dev / no gateway configured — nothing to govern, always ready.
    if not USE_AI_GATEWAY:
        return {"state": "ready"}

    services = list_model_services()
    if not services:
        return {
            "state": "no_model_service",
            "schemas": sorted(ALLOWED_MODEL_SERVICE_SCHEMAS),
        }

    # Without the app SP id we can't probe — assume ready rather than block the app
    # on missing deploy-time wiring (local dev already returned above).
    if not APP_SERVICE_PRINCIPAL_ID:
        return {"state": "ready"}

    # Ready if the app SP holds EXECUTE on ANY listed service — the user can then
    # pick that service and route. A schema-level grant is inherited by every
    # service, so the first probe short-circuits immediately; probing the rest
    # only matters for per-service grants, where checking just services[0] would
    # wrongly pin the whole app behind the overlay even though a usable service
    # exists. Best-effort: _sp_has_execute fails closed, so an undetermined probe
    # keeps the overlay up and guides the learner to grant access.
    for svc in services:
        if _sp_has_execute(svc["fullName"], APP_SERVICE_PRINCIPAL_ID):
            return {"state": "ready"}
    return {"state": "no_execute", "modelService": services[0]["fullName"]}


# NOTE: this will work for all databricks models OTHER than GPT-OSS, which uses a slightly different API
set_default_openai_client(AsyncDatabricksOpenAI(use_ai_gateway=USE_AI_GATEWAY))
set_default_openai_api("chat_completions")
set_trace_processors([])  # only use mlflow for trace processing
mlflow.openai.autolog()
logging.getLogger("mlflow.utils.autologging_utils").setLevel(logging.ERROR)


@function_tool
def get_current_time() -> str:
    """Get the current date and time."""
    return datetime.now().isoformat()


async def init_mcp_server(workspace_client: WorkspaceClient):
    return McpServer(
        url=build_mcp_url("/api/2.0/mcp/functions/system/ai", workspace_client=workspace_client),
        name="system.ai UC function MCP server",
        workspace_client=workspace_client,
    )


async def connect_healthy_mcp_servers(
    stack: AsyncExitStack, servers: list[McpServer]
) -> tuple[list[McpServer], list[str]]:
    """Connect each MCP server and verify it can actually list its tools.

    The Agents SDK lists each server's tools lazily inside ``Runner.run``, so a server that
    connects but fails at list time (e.g. an unauthorized Genie space) would otherwise crash
    the whole request — including unrelated turns. We list tools here, per server: healthy
    servers are kept; any that fails to connect OR to list is dropped and its name returned,
    so the agent runs with whatever is available instead of erroring out.

    Returns (healthy_servers, unavailable_names).
    """
    healthy: list[McpServer] = []
    unavailable: list[str] = []
    for server in servers:
        name = getattr(server, "name", "MCP server")
        try:
            connected = await stack.enter_async_context(server)
            await connected.list_tools()  # forces the connectivity + authorization check now
            healthy.append(connected)
        except Exception:
            logger.warning("MCP server %r unavailable; continuing without it.", name, exc_info=True)
            unavailable.append(name)
    return healthy, unavailable


def resolve_model(request: ResponsesAgentRequest) -> str:
    """Pick the model service for this request.

    The UI sends the chosen model service (catalog.schema.name) in
    custom_inputs.model_service, and we honor it only if it lives in an allowed
    schema — an app-layer check that's defense in depth on top of the UC EXECUTE
    grants the gateway already enforces, so a crafted request can't point the agent
    at an ungoverned model.

    There is no configured default model service (the learner creates them), so the
    fallbacks below return DEFAULT_MODEL — a direct foundation-model name. When the
    gateway is on this is a defensive last resort only: the onboarding overlay blocks
    the UI until a service exists, and the dropdown always sends a valid one.
    """
    if not USE_AI_GATEWAY:
        # Local dev / no gateway — always use the direct fallback model.
        return DEFAULT_MODEL

    requested = None
    if request.custom_inputs and isinstance(request.custom_inputs, dict):
        requested = request.custom_inputs.get("model_service")

    if not requested:
        return DEFAULT_MODEL

    if requested.count(".") != 2:
        logger.warning("Ignoring malformed model_service %r", requested)
        return DEFAULT_MODEL

    catalog, schema, _ = requested.split(".")
    if f"{catalog}.{schema}" not in ALLOWED_MODEL_SERVICE_SCHEMAS:
        logger.warning(
            "Ignoring model_service %r outside allowed schemas %s",
            requested,
            sorted(ALLOWED_MODEL_SERVICE_SCHEMAS),
        )
        return DEFAULT_MODEL

    return requested


# Diagrams are large HTML/SVG payloads — a rich one runs well over the default
# output cap, which truncates the reply mid-SVG (no closing fence → the UI can't
# render it). Give every generation plenty of headroom.
MAX_OUTPUT_TOKENS = 16000


def model_settings_for(model: str, underlying_model: str | None = None) -> ModelSettings:
    """Per-model request settings for the chat_completions API.

    The gateway's model families need different handling with function tools
    (all verified against the live gateway). The agent always has a function tool,
    so:
      - GPT services (e.g. gpt-5.5) REQUIRE reasoning_effort="none", else 400.
      - Claude services REJECT reasoning_effort, and on multi-turn tool calls they
        return extended-thinking content as a list of reasoning blocks, which the
        chat_completions converter can't parse (it expects a string). We disable
        thinking via extra_body {"thinking": {"type": "disabled"}}, which makes the
        content a plain string while still allowing tool calls. (The gateway does
        not support the Responses API for Claude, so chat_completions is the only
        path.)
      - Direct FMAPI models (local dev) get no special settings.

    We match the family against BOTH the model-service name AND the underlying model
    it routes to. The service name can be opaque (e.g. ``ts-demo-ms``), which hides
    the family — but a service pointing at ``system.ai.databricks-claude-opus-5``
    still needs the Claude handling. Passing the resolved underlying_model makes the
    family detection robust to arbitrary service names.

    All branches raise max_tokens so large diagrams aren't truncated mid-SVG.
    """
    if not USE_AI_GATEWAY:
        return ModelSettings(max_tokens=MAX_OUTPUT_TOKENS)

    haystack = f"{model} {underlying_model or ''}".lower()
    if "gpt" in haystack:
        return ModelSettings(max_tokens=MAX_OUTPUT_TOKENS, reasoning=Reasoning(effort="none"))
    if "claude" in haystack:
        return ModelSettings(
            max_tokens=MAX_OUTPUT_TOKENS,
            extra_body={"thinking": {"type": "disabled"}},
        )
    return ModelSettings(max_tokens=MAX_OUTPUT_TOKENS)


# The agent is a %md-sandbox diagram specialist: given a request, it returns a
# single self-contained HTML block (inline CSS + SVG, optional <script>) that
# renders as a diagram in a Databricks %md-sandbox notebook cell. The app renders
# each reply in a sandboxed iframe and offers a copy button for the full cell.

# --- Approved color palette (single source of truth) ---------------------------
# Structured data so it can drive BOTH the sidebar swatches (via /api/palette) and
# the system prompt. Each entry: name, hex, usage.
PALETTE_PRIMARY = [
    {"name": "Dark Teal", "hex": "#0b2026", "usage": "Primary text, dark headers"},
    {"name": "Warm White", "hex": "#F9F7F4", "usage": "Card backgrounds"},
    {"name": "Coral Red", "hex": "#FF5F46", "usage": "Accent bars, highlights, alerts"},
    {"name": "Light Gray", "hex": "#EEEDE9", "usage": "Borders, table alternating rows"},
    {"name": "Deep Teal", "hex": "#1B5162", "usage": "Header backgrounds, borders"},
    {"name": "Green", "hex": "#00A972", "usage": "Success, accent bars, highlights"},
    {"name": "Dark Red", "hex": "#98102A", "usage": "Accent bars, warnings"},
    {"name": "Amber", "hex": "#FFAB00", "usage": "Accent bars, highlights"},
    {"name": "Muted Teal", "hex": "#618794", "usage": "Secondary text, arrows, muted"},
    {"name": "Blue", "hex": "#4299E0", "usage": "Accent bars, links, process boxes"},
]
PALETTE_CUSTOM = [
    {"name": "Light Gray", "hex": "#C2C2C2", "usage": "Disabled states"},
    {"name": "Dark Burgundy", "hex": "#801C17", "usage": "Error emphasis"},
    {"name": "Light Coral BG", "hex": "#FABFBA", "usage": "Light warning backgrounds"},
    {"name": "Salmon", "hex": "#FF9E94", "usage": "Soft accent"},
    {"name": "Bright Red", "hex": "#FF3621", "usage": "Strong alerts"},
    {"name": "Medium Coral", "hex": "#FF6952", "usage": "Button hover states"},
    {"name": "Light Coral", "hex": "#FF8774", "usage": "Soft accent"},
    {"name": "Bronze", "hex": "#CD7F32", "usage": "Special accent"},
    {"name": "Light Gold", "hex": "#FFCC66", "usage": "Highlight backgrounds"},
    {"name": "Very Dark Teal", "hex": "#1B3139", "usage": "Dark mode text"},
    {"name": "Slate", "hex": "#5A6F77", "usage": "Subtle text"},
    {"name": "Light Blue Gray", "hex": "#DCE0E2", "usage": "Subtle borders"},
    {"name": "Charcoal", "hex": "#303F47", "usage": "Code backgrounds"},
    {"name": "Muted Blue", "hex": "#90A5B1", "usage": "Placeholder text"},
    {"name": "Medium Blue", "hex": "#2272B4", "usage": "Links, active states"},
    {"name": "Silver Blue", "hex": "#C4CCD6", "usage": "Inactive borders"},
]


def palette_for_ui() -> dict:
    """Palette data for the sidebar swatches (GET /api/palette)."""
    return {"primary": PALETTE_PRIMARY, "custom": PALETTE_CUSTOM}


def _palette_table(rows: list[dict]) -> str:
    lines = ["| Color | Hex | Usage |", "|-------|-----|-------|"]
    for c in rows:
        lines.append(f"| {c['name']} | `{c['hex']}` | {c['usage']} |")
    return "\n".join(lines)


# The static system prompt (base rules + color/design guidance, independent of
# which colors are allowed) lives in prompts/system_prompt.md — the single source
# of truth shared with Classroom setup, which registers it in the MLflow Prompt
# Registry as a versioned Unity Catalog asset. This bundled copy is byte-identical
# to the registered version, so the agent runs it directly.
#
# Why not load the registered prompt at runtime: mlflow.genai.load_prompt performs
# a metadata write in the schema (both by-alias and by-version), which the app's
# service principal is not permitted to do (PERMISSION_DENIED). So the prompt is
# registered/versioned for governance and visibility, but the app serves this local
# copy. The registered version's metadata (name/version/alias/uri) is passed in as
# env vars at deploy time purely so the "My Agent" panel can display + link to it.
_PROMPT_FILE = Path(__file__).parent / "prompts" / "system_prompt.md"
try:
    _STATIC_INSTRUCTIONS = _PROMPT_FILE.read_text()
except Exception:
    logger.warning("Could not read %s; using empty base instructions", _PROMPT_FILE, exc_info=True)
    _STATIC_INSTRUCTIONS = ""

# Registered-prompt metadata, set at deploy time by AppDeployer (display only).
SYSTEM_PROMPT_NAME = os.environ.get("SYSTEM_PROMPT_NAME") or None
SYSTEM_PROMPT_ALIAS = os.environ.get("SYSTEM_PROMPT_ALIAS") or "champion"
SYSTEM_PROMPT_VERSION = os.environ.get("SYSTEM_PROMPT_VERSION") or None


def get_base_instructions() -> str:
    """The agent's base system prompt (bundled text; see note above)."""
    return _STATIC_INSTRUCTIONS


def get_system_prompt_detail() -> dict:
    """Registered-prompt metadata for the "My Agent" panel.

    Reflects what Classroom setup registered (name/version/alias), read from env
    vars — not a live registry call. ``registered`` is true when setup provided a
    name and version. ``template`` is the bundled text the agent actually runs
    (identical to the registered version).
    """
    registered = bool(SYSTEM_PROMPT_NAME and SYSTEM_PROMPT_VERSION)
    version = int(SYSTEM_PROMPT_VERSION) if (SYSTEM_PROMPT_VERSION or "").isdigit() else None
    return {
        "name": SYSTEM_PROMPT_NAME,
        "version": version,
        "alias": SYSTEM_PROMPT_ALIAS if registered else None,
        "uri": (f"prompts:/{SYSTEM_PROMPT_NAME}/{version}" if registered else None),
        "template": _STATIC_INSTRUCTIONS,
        "registered": registered,
    }


def log_resolved_config() -> None:
    """Log the deploy-resolved config at startup, so the app logs make it obvious
    whether the catalog/schema, app SP, and registered-prompt env vars were pulled
    through as expected.

    These are exactly the values that, when empty or wrong, drop the app into the
    onboarding overlay — logging them up front turns "why is the overlay stuck?"
    into a one-line log read instead of a live-API investigation. Called once at
    server startup (see start_server.py).
    """
    logger.info(
        "Resolved app config: use_ai_gateway=%s model_service_schemas=%s "
        "app_service_principal_id=%s system_prompt_name=%s version=%s alias=%s "
        "mlflow_experiment_id=%s",
        USE_AI_GATEWAY,
        sorted(ALLOWED_MODEL_SERVICE_SCHEMAS) or "(none)",
        APP_SERVICE_PRINCIPAL_ID or "(unset)",
        SYSTEM_PROMPT_NAME or "(unset)",
        SYSTEM_PROMPT_VERSION or "(unset)",
        SYSTEM_PROMPT_ALIAS or "(unset)",
        os.environ.get("MLFLOW_EXPERIMENT_ID") or "(unset)",
    )


def build_instructions(allowed_colors: list[str] | None = None) -> str:
    """Assemble the system prompt, optionally restricting the color palette.

    The base rules + design guidance come from get_base_instructions() (the
    registered prompt version when available, else the built-in text). The color
    palette section is assembled dynamically per request and appended, since it
    depends on the user's sidebar selection.

    allowed_colors: hex strings the user selected in the sidebar. When provided,
    the agent is told to use ONLY those colors (still honoring an explicit color
    request typed by the user). When None/empty, the full approved palette is
    injected, as before.
    """
    base = get_base_instructions()

    if allowed_colors:
        chosen = {c.upper() for c in allowed_colors}
        rows = [c for c in (PALETTE_PRIMARY + PALETTE_CUSTOM) if c["hex"].upper() in chosen]
        # Fall back to the full palette if the selection matched nothing.
        if rows:
            palette_section = (
                "### Color Palette (user-restricted) — OVERRIDES ALL COLOR GUIDANCE ABOVE\n"
                "The user has restricted the palette to the table below. Use ONLY these "
                "colors for everything you choose, and do not introduce any other color.\n"
                "**This supersedes every specific color value recommended earlier in "
                "these instructions** — the header, accent-bar, border, and muted-text "
                "hex suggestions above no longer apply. For each of those roles, pick a "
                "color from this table instead.\n"
                "(Structural callout tints `#F8F9FC` and `#FFF6F4`, and white/black "
                "for text/strokes, are still allowed.)\n"
                "> Exception: if the user *explicitly* names a specific color in their "
                "message, honor it even if it's not listed.\n"
                + _palette_table(rows)
            )
            return base + "\n" + palette_section

    palette_section = (
        "### Color Palette\n"
        "The two tables below (Theme Light + Custom) are the **approved color set — "
        "these are the only colors a user-requested recolor may use.** The Theme Light "
        "palette is the primary set; Custom colors are for accents and edge cases. Do "
        "not introduce colors outside these tables on your own.\n"
        "> Exception: if a user *explicitly* asks for a specific color that isn't in "
        "these tables, honor it. The approved set governs the colors **you** choose; it "
        "does not override a direct request from the user.\n"
        "**Theme Light (Primary)**\n"
        + _palette_table(PALETTE_PRIMARY)
        + "\n**Custom (Secondary, use sparingly)**\n"
        + _palette_table(PALETTE_CUSTOM)
    )
    return base + "\n" + palette_section


def allowed_colors_from_request(request: ResponsesAgentRequest) -> list[str] | None:
    """Read the user-selected color subset from custom_inputs.allowed_colors."""
    if request.custom_inputs and isinstance(request.custom_inputs, dict):
        colors = request.custom_inputs.get("allowed_colors")
        if isinstance(colors, list) and colors:
            return [c for c in colors if isinstance(c, str)]
    return None


def create_agent(
    model: str = DEFAULT_MODEL,
    mcp_servers: list[McpServer] | None = None,
    allowed_colors: list[str] | None = None,
    underlying_model: str | None = None,
) -> Agent:
    return Agent(
        name="Agent",
        instructions=build_instructions(allowed_colors),
        model=model,
        model_settings=model_settings_for(model, underlying_model),
        tools=[get_current_time],
        mcp_servers=mcp_servers or [],
    )


def normalize_history(items: list[dict]) -> list[dict]:
    """Flatten Responses-format message items into plain {role, content} messages.

    On multi-turn requests the client replays prior turns as Responses API items,
    e.g. an assistant reply as
        {"type": "message", "role": "assistant",
         "content": [{"type": "output_text", "text": "..."}]}
    The agent runs in chat_completions mode, whose converter raises
    "Unhandled item type or structure" on that shape. We collapse any message item
    whose content is a list of text parts (output_text / input_text / text) into a
    single string, which every converter accepts. Non-message items (function calls,
    tool outputs, etc.) pass through untouched.
    """
    _TEXT_PART_TYPES = {"output_text", "input_text", "text"}
    normalized: list[dict] = []
    for item in items:
        if (
            isinstance(item, dict)
            and item.get("type") == "message"
            and isinstance(item.get("content"), list)
        ):
            text = "".join(
                part.get("text", "")
                for part in item["content"]
                if isinstance(part, dict) and part.get("type") in _TEXT_PART_TYPES
            )
            normalized.append({"role": item.get("role", "assistant"), "content": text})
        else:
            normalized.append(item)
    return normalized


def _tag_trace_with_model(model_service: str) -> "str | None":
    """Tag the current trace with the model service AND the underlying model.

    The request only carries the model *service* (e.g. catalog.schema.ts-demo-ms),
    which doesn't reveal what it actually routes to. We resolve the underlying model
    (e.g. system.ai.databricks-claude-opus-5) via the service's routing config and
    add both as trace tags, so the MLflow experiment surfaces the real model in use.
    Returns the resolved underlying model (or None) so the caller can reuse it for
    per-model request settings without a second lookup. Best-effort: never let
    tagging break a request.
    """
    try:
        tags = {"model_service": model_service}
        detail = get_model_service_detail(model_service)
        underlying = detail.get("underlyingModel")
        if underlying:
            tags["underlying_model"] = underlying
        mlflow.update_current_trace(tags=tags)
        return underlying
    except Exception:
        logger.warning("Could not tag trace with model for %s", model_service, exc_info=True)
        return None


def _policy_denial_message(exc: Exception) -> "str | None":
    """If ``exc`` is a Unity AI Gateway service-policy denial, return a friendly
    chat message explaining it; otherwise return None.

    When a request or (more commonly) a response trips a service policy's guardrail,
    the gateway fails closed with HTTP 400 and
    ``reason == RESPONSE_BLOCKED_BY_POLICY``. Rather than surface that as a raw 500
    error, we turn it into a clear in-chat message naming the policy and phase — so
    a guardrail denial reads as intended governance, not an app crash.
    """
    # Prefer structured access (openai.BadRequestError exposes .body); fall back to
    # the string form so we still catch it if the shape differs.
    reason = policy = phase = detail = None
    body = getattr(exc, "body", None)
    if isinstance(body, dict):
        detail = body.get("message")
        for d in body.get("details") or []:
            if isinstance(d, dict) and d.get("reason") == "RESPONSE_BLOCKED_BY_POLICY":
                reason = d["reason"]
                meta = d.get("metadata") or {}
                policy = meta.get("policy_name")
                phase = meta.get("phase")
    text = str(exc)
    if reason is None and "RESPONSE_BLOCKED_BY_POLICY" not in text:
        return None

    import re

    if policy is None:
        m = re.search(r"policy_name['\"]?\s*[:=]\s*['\"]([^'\"]+)['\"]", text)
        if not m:
            m = re.search(r"output policy '([^']+)'", text)
        policy = m.group(1) if m else "a service policy"
    if phase is None:
        phase = "output" if "output policy" in text else "request/response"
    if not detail:
        m = re.search(r"policy '[^']+':\s*(.+?)(?:\", 'details'|$)", text)
        detail = m.group(1).strip() if m else ""

    msg = (
        f"⚠️ **Blocked by Unity AI Gateway service policy `{policy}`** "
        f"({phase} guardrail).\n\n"
        "The model service is reachable (access granted), but this "
        f"{phase} was denied by the service policy before it could be returned — "
        "Unity AI Gateway guardrails fail closed."
    )
    if detail:
        msg += f"\n\n> {detail}"
    return msg


@invoke()
async def invoke_handler(request: ResponsesAgentRequest) -> ResponsesAgentResponse:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})
    model = resolve_model(request)
    underlying_model = _tag_trace_with_model(model)
    # The agent runs inside an AsyncExitStack so any MCP servers stay open for the whole
    # request. To give the agent MCP tools, connect them with connect_healthy_mcp_servers,
    # which health-checks each server so one unavailable server can't crash the request
    # (the Agents SDK lists each server's tools lazily inside Runner.run):
    #   servers, unavailable = await connect_healthy_mcp_servers(
    #       stack, [await init_mcp_server(WorkspaceClient())])
    #   agent = create_agent(mcp_servers=servers)
    # WorkspaceClient() uses service principal credentials; use get_user_workspace_client()
    # for on-behalf-of user authentication.
    async with AsyncExitStack() as stack:
        agent = create_agent(
            model=model,
            allowed_colors=allowed_colors_from_request(request),
            underlying_model=underlying_model,
        )
        messages = normalize_history([i.model_dump() for i in request.input])
        try:
            result = await Runner.run(agent, messages)
        except Exception as exc:
            denial = _policy_denial_message(exc)
            if denial is None:
                raise
            # Surface the guardrail denial as a normal assistant message.
            return ResponsesAgentResponse(
                output=[{
                    "type": "message",
                    "id": str(uuid4()),
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": denial}],
                }]
            )
        return ResponsesAgentResponse(output=[item.to_input_item() for item in result.new_items])


@stream()
async def stream_handler(
    request: ResponsesAgentRequest,
) -> AsyncGenerator[ResponsesAgentStreamEvent, None]:
    if session_id := get_session_id(request):
        mlflow.update_current_trace(metadata={"mlflow.trace.session": session_id})
    model = resolve_model(request)
    underlying_model = _tag_trace_with_model(model)
    # The agent runs inside an AsyncExitStack so any MCP servers stay open for the whole
    # request. To give the agent MCP tools, connect them with connect_healthy_mcp_servers,
    # which health-checks each server so one unavailable server can't crash the request
    # (the Agents SDK lists each server's tools lazily inside Runner.run):
    #   servers, unavailable = await connect_healthy_mcp_servers(
    #       stack, [await init_mcp_server(WorkspaceClient())])
    #   agent = create_agent(mcp_servers=servers)
    # WorkspaceClient() uses service principal credentials; use get_user_workspace_client()
    # for on-behalf-of user authentication.
    async with AsyncExitStack() as stack:
        agent = create_agent(
            model=model,
            allowed_colors=allowed_colors_from_request(request),
            underlying_model=underlying_model,
        )
        messages = normalize_history([i.model_dump() for i in request.input])
        result = Runner.run_streamed(agent, input=messages)

        try:
            async for event in process_agent_stream_events(result.stream_events()):
                yield event
        except Exception as exc:
            denial = _policy_denial_message(exc)
            if denial is None:
                raise
            # Emit the guardrail denial as normal streamed assistant text so the UI
            # renders it in the chat bubble instead of showing a stream error.
            item_id = str(uuid4())
            yield ResponsesAgentStreamEvent(
                type="response.output_text.delta", item_id=item_id, delta=denial
            )
            yield ResponsesAgentStreamEvent(
                type="response.output_item.done",
                item={
                    "type": "message",
                    "id": item_id,
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": denial}],
                },
            )
