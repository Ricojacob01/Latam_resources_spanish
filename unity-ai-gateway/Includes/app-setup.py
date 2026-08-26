# Databricks notebook source
# MAGIC %md
# MAGIC # Classroom Setup 1 — Deploy the Agent App
# MAGIC
# MAGIC This notebook is run via `%run ./Includes/Classroom-Setup-1` from **02 Lab**.
# MAGIC It deploys the agent in `agent-app/` **for you** using the Databricks SDK — no
# MAGIC local CLI, no `.env`, no `databricks bundle`. Everything runs as the workspace
# MAGIC user executing the lab.
# MAGIC
# MAGIC **What it does**
# MAGIC 1. Connects with `WorkspaceClient()` (picks up your notebook credentials).
# MAGIC 2. Resolves the workspace path to `agent-app/` from this notebook's own location.
# MAGIC 3. Creates (or reuses) a per-user MLflow experiment for agent traces.
# MAGIC 4. Creates the app compute with the resources it needs attached:
# MAGIC    the experiment, and a Unity AI Gateway serving endpoint.
# MAGIC 5. Deploys the app source that is already synced into your workspace.
# MAGIC 6. Prints the live app URL.
# MAGIC
# MAGIC The notebook is **idempotent** — re-running it reuses the existing app and
# MAGIC experiment and pushes a fresh deployment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC **Unity AI Gateway model services** are **Unity Catalog securables** addressed by their
# MAGIC full name (`catalog.schema.name`) — *not* workspace serving endpoints. This lab exposes
# MAGIC every model service in one schema (`MODEL_SERVICE_SCHEMA`), and the app renders a
# MAGIC **dropdown** so the user can pick which one the agent uses per message — the
# MAGIC `<schema>.*` wildcard.
# MAGIC
# MAGIC Because they live in Unity Catalog, access is governed by UC grants: the setup below
# MAGIC grants the app's service principal `USE CATALOG` / `USE SCHEMA` on the parents and
# MAGIC `EXECUTE` on **each** model service in the schema (the app SP can only call what it's
# MAGIC been granted). The agent sets `use_ai_gateway=True`, so calls flow through the gateway
# MAGIC control plane where rate limits, budgets, usage tracking, and service policies apply.

# COMMAND ----------

# The Unity Catalog schema (catalog.schema) whose model services the lab exposes.
MODEL_SERVICE_SCHEMA = "labuser_matthew_mccoy.ts_ai_gateway"

# The model service selected by default (must live in MODEL_SERVICE_SCHEMA). The
# dropdown lets the user switch to any other model service in the schema.
DEFAULT_MODEL_SERVICE = f"{MODEL_SERVICE_SCHEMA}.gpt-5-5"

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.apps import (
    App,
    AppDeployment,
    AppResource,
    AppResourceExperiment,
    AppResourceExperimentExperimentPermission,
    EnvVar,
)
from databricks.sdk.service.catalog import (
    Privilege,
    PermissionsChange,
)

w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Resolve paths, user, and a per-user app name
# MAGIC
# MAGIC The app source lives at `../agent-app` relative to this `Includes/` notebook. We
# MAGIC derive its absolute workspace path from *this* notebook's path so the setup works
# MAGIC no matter which user cloned the repo or where. The app name is derived from your
# MAGIC username so multiple learners in the same workspace don't collide, and follows the
# MAGIC `agent-*` naming convention.

# COMMAND ----------

# This notebook's own workspace path, e.g.
#   /Users/you@databricks.com/.../genai-unity-ai-gateway/Includes/Classroom-Setup-1
_ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
notebook_path = _ctx.notebookPath().get()

# .../Includes/Classroom-Setup-1 -> .../Includes -> .../genai-unity-ai-gateway
course_dir = notebook_path.rsplit("/", 2)[0]
agent_app_path = f"{course_dir}/agent-app"

username = w.current_user.me().user_name  # e.g. you@databricks.com

# App names must be lowercase alphanumeric + hyphens. Build one from the user prefix.
_user_slug = re.sub(r"[^a-z0-9-]", "-", username.split("@")[0].lower()).strip("-")
app_name = f"agent-{_user_slug}"[:30].strip("-")

print(f"User:            {username}")
print(f"App name:        {app_name}")
print(f"App source path: {agent_app_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create (or reuse) the MLflow experiment
# MAGIC
# MAGIC The app logs traces to an MLflow experiment. `app.yaml` injects its ID via
# MAGIC `valueFrom: "experiment"`, which resolves to the app resource named `experiment`
# MAGIC (attached below).

# COMMAND ----------

experiment_name = f"/Users/{username}/agents-on-apps"
try:
    experiment_id = w.experiments.create_experiment(name=experiment_name).experiment_id
    print(f"Created experiment '{experiment_name}' (ID: {experiment_id})")
except Exception:
    # Already exists — reuse it.
    exp = w.experiments.get_by_name(experiment_name=experiment_name).experiment
    experiment_id = exp.experiment_id
    print(f"Reusing experiment '{experiment_name}' (ID: {experiment_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define the app resources
# MAGIC
# MAGIC - **experiment** — destination for the agent's traces (must be named `experiment`
# MAGIC   to match `valueFrom: "experiment"` in `app.yaml` and `manifest.yaml`).
# MAGIC
# MAGIC The Unity AI Gateway model service is **not** an app resource — it's a Unity Catalog
# MAGIC securable governed by a UC grant (done in a later cell), and passed to the app as a
# MAGIC plain env var. So the only resource we attach here is the experiment.

# COMMAND ----------

resources = [
    AppResource(
        name="experiment",
        experiment=AppResourceExperiment(
            experiment_id=experiment_id,
            permission=AppResourceExperimentExperimentPermission.CAN_EDIT,
        ),
    ),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the app compute
# MAGIC
# MAGIC `create_and_wait` blocks until the app compute is `ACTIVE` (up to 20 min). On a
# MAGIC re-run the app already exists, so we catch that and update its resources instead.

# COMMAND ----------

try:
    app = w.apps.create_and_wait(
        app=App(
            name=app_name,
            description="Unity AI Gateway agent app (deployed via the Databricks SDK)",
            resources=resources,
        )
    )
    print(f"Created app '{app_name}'")
except Exception as e:
    if "already exists" in str(e).lower():
        print(f"App '{app_name}' already exists — updating its resources.")
        app = w.apps.create_update_and_wait(
            app_name=app_name,
            update_mask="resources",
            app=App(name=app_name, resources=resources),
        )
    else:
        raise

# The app runs as its own service principal. Capture it so we can grant it access
# to the model service securable below.
app = w.apps.get(name=app_name)
app_sp = app.service_principal_client_id
print(f"App service principal (client id): {app_sp}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Grant the app access to the model services
# MAGIC
# MAGIC Reaching a model service is a UC grant — the first of the three control points from the
# MAGIC lecture ("can this principal reach the endpoint at all?"). Because the dropdown lets the
# MAGIC user pick **any** model service in the schema, we grant at the **schema** level so the
# MAGIC app's service principal gets three inherited privileges:
# MAGIC
# MAGIC - **`USE CATALOG`** on the parent catalog, and **`USE SCHEMA`** on the schema — without
# MAGIC   these traversal grants UC *hides* the objects, and the app sees them as `does not
# MAGIC   exist` even with EXECUTE on the model service itself.
# MAGIC - **`EXECUTE` on the schema** — in Unity Catalog, `EXECUTE` on a schema is **inherited by
# MAGIC   every model service in it, including ones created later**. This is the key to making
# MAGIC   new models appear in the dropdown *and* be callable automatically: add a model service
# MAGIC   to the schema and it just works — no re-run of this notebook, no redeploy. (A per-service
# MAGIC   grant would only cover the services that existed when this ran.)
# MAGIC
# MAGIC > **Beta note:** a model service is Unity Catalog securable type `MODEL_SERVICE`. `SCHEMA`
# MAGIC > and `CATALOG` are standard securable types. `grants.update` takes the type as a plain
# MAGIC > string. The cell is best-effort: if a grant fails, it prints guidance and continues, so
# MAGIC > an instructor can grant access in the UI (Catalog Explorer → Permissions).

# COMMAND ----------

_catalog, _schema = MODEL_SERVICE_SCHEMA.split(".")

# Schema-level grants. EXECUTE on the schema is inherited by all current AND future
# model services in it, so newly-added services are reachable without re-running setup.
_grants = [
    ("CATALOG", _catalog, Privilege.USE_CATALOG),
    ("SCHEMA", MODEL_SERVICE_SCHEMA, Privilege.USE_SCHEMA),
    ("SCHEMA", MODEL_SERVICE_SCHEMA, Privilege.EXECUTE),
]

for securable_type, full_name, privilege in _grants:
    try:
        w.grants.update(
            securable_type=securable_type,
            full_name=full_name,
            changes=[PermissionsChange(principal=app_sp, add=[privilege])],
        )
        print(f"Granted {privilege.value} on {securable_type} '{full_name}' to app SP {app_sp}")
    except Exception as e:
        print(
            f"Could not grant {privilege.value} on {securable_type} '{full_name}': {e}\n"
            f"Grant it manually in Catalog Explorer (Permissions → grant {privilege.value} to {app_sp})."
        )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Deploy the app source
# MAGIC
# MAGIC `deploy_and_wait` reads the source from the workspace path (already synced with the
# MAGIC repo) and blocks until the deployment `SUCCEEDS`.
# MAGIC
# MAGIC **Wiring the agent to the gateway:** we pass two env vars as deployment overrides:
# MAGIC `GATEWAY_MODEL_SERVICE` (the default model) and `MODEL_SERVICE_SCHEMAS` (the schema the
# MAGIC dropdown lists and the agent restricts requests to). The agent sets `use_ai_gateway=True`
# MAGIC and routes each request to the chosen model service — which the app SP now has `EXECUTE`
# MAGIC on — through the gateway.

# COMMAND ----------

deployment = w.apps.deploy_and_wait(
    app_name=app_name,
    app_deployment=AppDeployment(
        source_code_path=agent_app_path,
        env_vars=[
            EnvVar(name="GATEWAY_MODEL_SERVICE", value=DEFAULT_MODEL_SERVICE),
            EnvVar(name="MODEL_SERVICE_SCHEMAS", value=MODEL_SERVICE_SCHEMA),
        ],
    ),
)
print(f"Deployment status: {deployment.status.state if deployment.status else 'UNKNOWN'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Done — open your app

# COMMAND ----------

app = w.apps.get(name=app_name)
print("=" * 67)
print("Agent app deployed!")
print("=" * 67)
print(f"Name:            {app.name}")
print(f"URL:             {app.url}")
print(f"Default model:   {DEFAULT_MODEL_SERVICE}  (routed via Unity AI Gateway)")
print(f"Model dropdown:  every model service in {MODEL_SERVICE_SCHEMA}.* (inherited EXECUTE)")
print(f"Experiment:      {w.config.host}/ml/experiments/{experiment_id}")