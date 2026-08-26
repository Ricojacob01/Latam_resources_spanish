# _lib/app_deployer.py
#
# Deploys the agent in agent-app/ as a Databricks App via the Databricks SDK
# (no CLI, no DAB). Ported from the standalone Includes/app-setup notebook.

import re
import time
from typing import Dict, Optional

from databricks.sdk import WorkspaceClient
# The Apps API returns error code ALREADY_EXISTS (-> AlreadyExists); the experiment
# API returns RESOURCE_ALREADY_EXISTS (-> ResourceAlreadyExists). Both subclass
# ResourceConflict, so we catch that base to cover either.
from databricks.sdk.errors import ResourceConflict
from databricks.sdk.service.apps import (
    App,
    AppDeployment,
    AppResource,
    AppResourceExperiment,
    AppResourceExperimentExperimentPermission,
    ApplicationState,
    EnvVar,
)
from databricks.sdk.service.catalog import PermissionsChange, Privilege


class AppDeployer:
    """Deploys the agent app for the current user via the Databricks SDK.

    Responsible for:
    - Creating (or reusing) a per-user MLflow experiment for agent traces
    - Creating (or updating) the app compute with the experiment attached
    - Granting the app's service principal USE CATALOG / USE SCHEMA on the
      model-service schema so the app can *list* the services (the dropdown).
      It intentionally does NOT grant EXECUTE — that access grant is a hands-on
      lab step (see lab §B1): the learner grants the app SP EXECUTE on the model
      service through Unity AI Gateway. Until they do, the app shows an onboarding
      overlay (see gateway_status() in the agent server).
    - Deploying the app source (already synced into the workspace) with the
      gateway env vars (and the app's own SP id, for the overlay's access probe),
      so the app routes LLM calls through Unity AI Gateway
    """

    def __init__(
        self,
        catalog_name: str,
        schema_name: str,
        agent_app_path: str,
        username: str,
        system_prompt_name: Optional[str] = None,
        system_prompt_version: Optional[int] = None,
        system_prompt_alias: str = "champion",
        workspace_client: Optional[WorkspaceClient] = None,
    ):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        # The Apps deploy API needs a full workspace filesystem path
        # (/Workspace/Users/...). dbutils notebookPath() returns workspace paths
        # WITHOUT that prefix (e.g. /Users/...), so prepend it when missing.
        if agent_app_path.startswith("/") and not agent_app_path.startswith("/Workspace"):
            agent_app_path = f"/Workspace{agent_app_path}"
        self.agent_app_path = agent_app_path
        self.username = username
        # Registered system-prompt metadata, passed to the app for the "My Agent"
        # panel to display + link to. The app runs its bundled prompt text (the app
        # SP can't read the registry); these are display-only.
        self.system_prompt_name = system_prompt_name
        self.system_prompt_version = system_prompt_version
        self.system_prompt_alias = system_prompt_alias
        self.w = workspace_client or WorkspaceClient()

        # The schema whose model services the app's dropdown exposes (the
        # `<schema>.*` wildcard) and the agent restricts requests to.
        self.model_service_schema = f"{catalog_name}.{schema_name}"

        # App names must be lowercase alphanumeric + hyphens. Build one from the
        # user prefix so learners in a shared workspace don't collide.
        user_slug = re.sub(r"[^a-z0-9-]", "-", username.split("@")[0].lower()).strip("-")
        self.app_name = f"agent-{user_slug}"[:30].strip("-")

    def _setup_experiment(self) -> str:
        """Create or reuse the per-user MLflow experiment; return its ID.

        Only "already exists" is treated as reuse — any other failure
        (permissions, network) propagates instead of being silently retried as
        a get, which would otherwise surface as a confusing second error.
        """
        experiment_name = f"/Users/{self.username}/agents-on-apps"
        try:
            experiment_id = self.w.experiments.create_experiment(
                name=experiment_name
            ).experiment_id
            print(f"  Created experiment '{experiment_name}' (ID: {experiment_id})")
        except ResourceConflict:
            exp = self.w.experiments.get_by_name(experiment_name=experiment_name).experiment
            experiment_id = exp.experiment_id
            print(f"  Reusing experiment '{experiment_name}' (ID: {experiment_id})")
        return experiment_id

    def _create_or_update_app(self, resources: list) -> str:
        """Create the app (or update its resources if it exists); return app SP client id."""
        try:
            self.w.apps.create_and_wait(
                app=App(
                    name=self.app_name,
                    description="Unity AI Gateway agent app (deployed via the Databricks SDK)",
                    resources=resources,
                )
            )
            print(f"  Created app '{self.app_name}'")
        except ResourceConflict:
            print(f"  App '{self.app_name}' already exists — updating its resources.")
            self.w.apps.create_update_and_wait(
                app_name=self.app_name,
                update_mask="resources",
                app=App(name=self.app_name, resources=resources),
            )

        # The app runs as its own service principal; capture it for the UC grants.
        app = self.w.apps.get(name=self.app_name)
        app_sp = app.service_principal_client_id
        print(f"  App service principal (client id): {app_sp}")
        return app_sp

    def _grant_model_service_access(self, app_sp: str) -> None:
        """Grant the app SP schema-level access so it can LIST the model services.

        Grants USE CATALOG / USE SCHEMA only — enough for the app to enumerate the
        services in the dropdown. EXECUTE is deliberately NOT granted here: granting
        the app SP EXECUTE on the model service is a hands-on lab step (§B1), so the
        app starts up without access and its onboarding overlay walks the learner
        through creating a model service and granting EXECUTE in Unity AI Gateway.

        Best-effort: a failed grant prints guidance and continues.
        """
        grants = [
            ("CATALOG", self.catalog_name, Privilege.USE_CATALOG),
            ("SCHEMA", self.model_service_schema, Privilege.USE_SCHEMA),
        ]
        for securable_type, full_name, privilege in grants:
            try:
                self.w.grants.update(
                    securable_type=securable_type,
                    full_name=full_name,
                    changes=[PermissionsChange(principal=app_sp, add=[privilege])],
                )
                print(f"  Granted {privilege.value} on {securable_type} '{full_name}' to app SP {app_sp}")
            except Exception as e:
                print(
                    f"  Could not grant {privilege.value} on {securable_type} '{full_name}': {e}\n"
                    f"  Grant it manually in Catalog Explorer (Permissions → grant {privilege.value} to {app_sp})."
                )

    def _wait_until_running(self, timeout_seconds: int = 300) -> Optional[str]:
        """Poll the app compute until it reports RUNNING (or a terminal state).

        ``deploy_and_wait`` returns when the *deployment* SUCCEEDED, but the app
        process can still crash on startup afterward. Poll ``app_status`` so we
        report the app's real health, not just that the code was deployed.

        Returns the final app_status state string (e.g. "RUNNING", "CRASHED"),
        or None if it couldn't be determined within the timeout.
        """
        deadline = time.time() + timeout_seconds
        last_state = None
        while time.time() < deadline:
            app = self.w.apps.get(name=self.app_name)
            status = app.app_status
            last_state = status.state.value if status and status.state else None
            if last_state == ApplicationState.RUNNING.value:
                return last_state
            # CRASHED is terminal. UNAVAILABLE is NOT treated as terminal: an app
            # transiently reports UNAVAILABLE while starting up right after a
            # successful deploy, so keep polling — a genuinely stuck app is caught
            # by the timeout instead.
            if last_state == ApplicationState.CRASHED.value:
                msg = status.message if status else ""
                print(f"  App CRASHED: {msg}")
                return last_state
            time.sleep(10)
        return last_state

    def _smoke_test(self) -> bool:
        """Send one real /invocations request to the running app.

        Every deploy-time-green-but-runtime-broken issue we've hit (missing
        README, reasoning_effort, multi-turn conversion) only surfaced on an
        actual model call. A single turn against the default model catches those
        before a learner does. Best-effort: failures print guidance, don't raise.
        """
        import json
        import urllib.error
        import urllib.request

        app = self.w.apps.get(name=self.app_name)
        if not app.url:
            print("  Smoke test skipped: app URL not available.")
            return False

        # Authenticate as the notebook user (the app requires OAuth).
        headers = self.w.config.authenticate()
        headers["Content-Type"] = "application/json"
        body = json.dumps({"input": [{"role": "user", "content": "Say hi in 3 words."}]}).encode()
        req = urllib.request.Request(
            f"{app.url}/invocations", data=body, method="POST", headers=headers
        )
        try:
            with urllib.request.urlopen(req, timeout=90) as resp:
                if resp.status == 200:
                    print("  Smoke test passed: the model service responded (HTTP 200).")
                    return True
                print(f"  Smoke test: unexpected HTTP {resp.status}.")
                return False
        except urllib.error.HTTPError as e:
            print(
                f"  Smoke test failed: HTTP {e.code} from /invocations: {e.read().decode()[:300]}\n"
                f"  The app deployed but a model call errored. Check: databricks apps logs {self.app_name}"
            )
            return False
        except Exception as e:
            print(f"  Smoke test could not complete: {type(e).__name__}: {e}")
            return False

    def deploy(self, smoke_test: bool = False) -> Dict[str, str]:
        """Run the full app deploy and return a summary dict.

        Parameters
        ----------
        smoke_test : bool
            If True, send one real model call to the deployed app to confirm it
            works end-to-end, not just that it deployed. Off by default so
            classroom setup stays fast; enable it when validating changes.
            NOTE: since setup no longer grants the app SP EXECUTE (that's a
            hands-on lab step), a fresh deploy will FAIL the smoke test until the
            learner grants access — leave it off for classroom setup.
        """
        experiment_id = self._setup_experiment()

        # The only app resource is the experiment (destination for traces). The
        # model service is a UC securable governed by the grants below, passed to
        # the app as a plain env var — not an app resource.
        resources = [
            AppResource(
                name="experiment",
                experiment=AppResourceExperiment(
                    experiment_id=experiment_id,
                    permission=AppResourceExperimentExperimentPermission.CAN_EDIT,
                ),
            ),
        ]

        app_sp = self._create_or_update_app(resources)
        self._grant_model_service_access(app_sp)

        # Log the resolved config being wired into the app, so a setup run shows —
        # in one place — that the per-user catalog/schema, app SP, prompt, and
        # experiment were all pulled through as expected (these are exactly the
        # values that put the app into the onboarding overlay when wrong/empty).
        print("  Wiring app config:")
        print(f"    app_name              : {self.app_name}")
        print(f"    catalog.schema        : {self.model_service_schema}")
        print(f"    MODEL_SERVICE_SCHEMAS : {self.model_service_schema}")
        print(f"    APP_SERVICE_PRINCIPAL : {app_sp}")
        print(f"    MLFLOW_EXPERIMENT_ID  : {experiment_id}")
        print(f"    SYSTEM_PROMPT_NAME    : {self.system_prompt_name or '(none)'}")
        print(f"    SYSTEM_PROMPT_VERSION : {self.system_prompt_version or '(none)'}")
        print(f"    source_code_path      : {self.agent_app_path}")

        deployment = self.w.apps.deploy_and_wait(
            app_name=self.app_name,
            app_deployment=AppDeployment(
                source_code_path=self.agent_app_path,
                env_vars=[
                    # NOTE: this env_vars list REPLACES app.yaml's env at deploy time,
                    # so we must re-declare the MLflow vars app.yaml defines — otherwise
                    # the app has no experiment/tracking config and traces go nowhere.
                    EnvVar(name="MLFLOW_TRACKING_URI", value="databricks"),
                    EnvVar(name="MLFLOW_REGISTRY_URI", value="databricks-uc"),
                    # Trace destination: bound from the 'experiment' app resource
                    # (value_from), which points at the per-user agents-on-apps experiment.
                    EnvVar(name="MLFLOW_EXPERIMENT_ID", value_from="experiment"),
                    # The schema whose model services the app exposes. There's no
                    # configured default model service — the learner creates the
                    # services and grants access as a hands-on lab step; routing
                    # through the gateway turns on whenever this is set.
                    EnvVar(name="MODEL_SERVICE_SCHEMAS", value=self.model_service_schema),
                    # The app's own service principal id, so the onboarding overlay
                    # can probe whether the SP has been granted EXECUTE yet. The app
                    # can't reliably discover its own SP identity at runtime.
                    EnvVar(name="APP_SERVICE_PRINCIPAL_ID", value=app_sp),
                    # Registered system-prompt metadata (display-only). The app runs
                    # its bundled prompt text; these let the "My Agent" panel show the
                    # UC name/version/alias and link to it. (The app SP can't read the
                    # prompt registry at runtime, so we don't load it live.)
                    EnvVar(name="SYSTEM_PROMPT_NAME", value=self.system_prompt_name or ""),
                    EnvVar(name="SYSTEM_PROMPT_VERSION", value=str(self.system_prompt_version or "")),
                    EnvVar(name="SYSTEM_PROMPT_ALIAS", value=self.system_prompt_alias or ""),
                ],
            ),
        )
        deploy_state = deployment.status.state.value if deployment.status and deployment.status.state else "UNKNOWN"
        print(f"  Deployment status: {deploy_state}")

        # deploy_and_wait returns on deployment SUCCESS; confirm the app process
        # actually came up before reporting success.
        app_state = self._wait_until_running()
        print(f"  App status: {app_state}")

        smoke_passed = None
        if smoke_test and app_state == ApplicationState.RUNNING.value:
            smoke_passed = self._smoke_test()

        app = self.w.apps.get(name=self.app_name)
        return {
            "app_name": app.name,
            "app_url": app.url,
            "model_service_schema": self.model_service_schema,
            "experiment_id": experiment_id,
            "app_status": app_state,
            "smoke_test_passed": smoke_passed,
            "system_prompt_name": self.system_prompt_name,
            "system_prompt_version": self.system_prompt_version,
            "system_prompt_alias": self.system_prompt_alias,
        }
