# demo_setup/prompt_registry.py
#
# Registers the agent's system prompt in the MLflow Prompt Registry as a versioned
# Unity Catalog asset (catalog.schema.name). The agent app prefers this registered
# version at runtime and surfaces it in the "My Agent" panel.

from typing import Optional, Tuple

import mlflow


def register_system_prompt(
    catalog: str,
    schema: str,
    prompt_text: str,
    name: str = "agent_system_prompt",
    commit_message: str = "Registered by Classroom setup",
    alias: str = "champion",
) -> Tuple[str, Optional[int]]:
    """Register (or reuse) the agent's system prompt as a UC-governed prompt version.

    Idempotent: if the latest registered version's template already matches
    ``prompt_text``, no new version is created — we return the existing version. This
    keeps re-running setup from spamming identical versions.

    The ``alias`` (default ``champion``) is (re)pointed at the resulting version, so
    the app can load a stable ``@champion`` reference regardless of version number —
    matching the repo's champion-alias convention for agents.

    Returns ``(full_name, version)``. On any error, prints guidance and returns
    ``(full_name, None)`` so setup continues (the app falls back to its built-in
    prompt when nothing is registered).
    """
    full_name = f"{catalog}.{schema}.{name}"
    try:
        # Prompts are registered in Unity Catalog, like models.
        mlflow.set_registry_uri("databricks-uc")

        existing = mlflow.genai.load_prompt(
            f"prompts:/{full_name}@latest", allow_missing=True
        )
        if existing is not None and existing.template == prompt_text:
            version = existing.version
            print(f"  System prompt '{full_name}' already up to date (v{version}).")
        else:
            pv = mlflow.genai.register_prompt(
                name=full_name,
                template=prompt_text,
                commit_message=commit_message,
            )
            version = pv.version
            print(f"  Registered system prompt '{full_name}' (v{version}).")

        # Point the '@champion' alias at this version (idempotent — resets each run).
        mlflow.genai.set_prompt_alias(name=full_name, alias=alias, version=version)
        print(f"  Set alias '@{alias}' -> v{version} on '{full_name}'.")
        return full_name, version
    except Exception as e:
        print(
            f"  Could not register system prompt '{full_name}': {e}\n"
            f"  The app will fall back to its built-in prompt. You can register it "
            f"manually with mlflow.genai.register_prompt(name='{full_name}', template=...)."
        )
        return full_name, None


def associate_prompt_with_experiment(full_name: str, experiment_id: str) -> None:
    """Associate a registered prompt with an MLflow experiment so it surfaces there.

    Without this, MLflow tags the prompt with the *active* experiment at registration
    time — which during setup is the default experiment (id ``0``), so the prompt
    never shows up under the app's experiment. We set the ``_mlflow_experiment_ids``
    tag directly (synchronous), rather than MlflowClient._link_prompt_to_experiment,
    which runs in a fire-and-forget thread that may not finish before setup exits.

    The tag value is the comma-wrapped experiment id (e.g. ``,4236...,``) — the same
    format MLflow uses, where the leading/trailing commas prevent false prefix
    matches. Best-effort: any error prints guidance and continues.
    """
    if not full_name or not experiment_id:
        return
    try:
        from mlflow import MlflowClient

        mlflow.set_registry_uri("databricks-uc")
        MlflowClient().set_prompt_tag(
            name=full_name,
            key="_mlflow_experiment_ids",
            value=f",{experiment_id},",
        )
        print(f"  Associated prompt '{full_name}' with experiment {experiment_id}.")
    except Exception as e:
        print(
            f"  Could not associate prompt '{full_name}' with experiment "
            f"{experiment_id}: {e} (the prompt is still registered)."
        )
