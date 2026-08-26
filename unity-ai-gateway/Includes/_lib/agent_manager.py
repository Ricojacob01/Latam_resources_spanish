# Includes/agent_manager.py

import re
import warnings
from pathlib import Path
from typing import List, Dict

import mlflow
from mlflow.models.resources import DatabricksFunction, DatabricksServingEndpoint
from mlflow import MlflowClient
from databricks import agents


class AgentManager:
    """
    Manages agent lifecycle operations.

    Responsible for:
    - Discovering agent files
    - Mapping agent configurations
    - Registering agents to Unity Catalog
    - Deploying agents to serving endpoints
    """

    def __init__(
        self,
        catalog_name: str,
        schema_name: str,
        llm_endpoint_name: str,
        alias: str,
        username: str,
        artifacts_dir: Path,
        eval_config_output_path: Path,
        lakebase_instance_name: str = None,
        lakebase_autoscaling_project: str = None,
        lakebase_autoscaling_branch: str = None
    ):
        self.catalog_name = catalog_name
        self.schema_name = schema_name
        self.llm_endpoint_name = llm_endpoint_name
        self.alias = alias
        self.username = username
        self.artifacts_dir = artifacts_dir
        self.eval_config_output_path = eval_config_output_path
        self.lakebase_instance_name = lakebase_instance_name
        self.lakebase_autoscaling_project = lakebase_autoscaling_project
        self.lakebase_autoscaling_branch = lakebase_autoscaling_branch

        # Will be populated during discovery
        self.agent_configs: Dict[str, dict] = {}

        # Generate deployed endpoint name
        self.deployed_endpoint_name = username.split("@")[0].replace(".", "_") + "_agent"

    def get_agent_files(self, artifacts_dir: str | Path) -> List[Path]:
        """Get all .py files containing 'agent' in the name."""
        artifacts_path = Path(artifacts_dir)
        if not artifacts_path.exists():
            raise FileNotFoundError(f"Artifacts directory does not exist: {artifacts_path}")

        agent_files = [
            f for f in artifacts_path.glob("*.py") if "agent" in f.stem.lower()
        ]
        return sorted(agent_files)

    def map_agent_config(
        self,
        agent_file: Path,
        tool_count_func,
        agent_configs_dir: str | Path = "./agent configs",
    ) -> None:
        """
        Map an agent .py file to its corresponding config file and metadata.

        Parameters
        ----------
        agent_file : Path
            Path to the agent Python file.
        tool_count_func : callable
            Function to count $TOOL placeholders in a config template.
        agent_configs_dir : str | Path
            Directory containing agent YAML config templates.
        """
        agent_stem = agent_file.stem
        config_file_name = f"{agent_stem}_config.yaml"
        config_template_path = Path(agent_configs_dir) / config_file_name

        if not config_template_path.exists():
            print(f"  Warning: Config file not found for {agent_file.name}: {config_template_path}")
            return

        agent_name = f"{agent_stem.replace('_agent', '')}"
        experiment_name = f"{agent_stem.replace('_agent', '')}_experiment"
        endpoint_name = self.deployed_endpoint_name

        required_tools_count = tool_count_func(config_template_path)

        self.agent_configs[agent_name] = {
            "agent_file": agent_file,
            "config_template": config_template_path,
            "config_output": self.artifacts_dir / config_file_name,
            "required_tools_count": required_tools_count,
            "experiment_name": experiment_name,
            "uc_model_name": f"{self.catalog_name}.{self.schema_name}.{agent_name}",
            "endpoint_name": endpoint_name,
        }

        print(
            f"  - Mapped '{agent_name}' -> {config_file_name} "
            f"(endpoint: '{endpoint_name}', needs {required_tools_count} tools)"
        )

    def get_agent_config_files(self, agent_configs_dir: str | Path) -> List[Path]:
        """Get all *_agent_config.yaml files in the agent configs directory."""
        configs_path = Path(agent_configs_dir)
        if not configs_path.exists():
            return []
        return sorted(configs_path.glob("*_agent_config.yaml"))

    def map_config_only(
        self,
        config_template_path: Path,
        tool_count_func,
    ) -> None:
        """
        Map a config YAML template that has no corresponding agent .py file.

        Used when a config should be rendered (e.g. for notebook use) even
        though the agent code doesn't exist yet in the current lab.

        Parameters
        ----------
        config_template_path : Path
            Path to the *_agent_config.yaml template.
        tool_count_func : callable
            Function to count $TOOL placeholders in a config template.
        """
        config_stem = config_template_path.stem  # e.g. "long_term_lakebase_agent_config"
        # Derive agent_name by stripping "_agent_config" or "_config" suffix
        agent_name = re.sub(r"_agent_config$|_config$", "", config_stem)

        config_file_name = config_template_path.name
        experiment_name = f"{agent_name}_experiment"
        endpoint_name = self.deployed_endpoint_name
        required_tools_count = tool_count_func(config_template_path)

        self.agent_configs[agent_name] = {
            "agent_file": None,  # No agent .py file for this entry
            "config_template": config_template_path,
            "config_output": self.artifacts_dir / config_file_name,
            "required_tools_count": required_tools_count,
            "experiment_name": experiment_name,
            "uc_model_name": f"{self.catalog_name}.{self.schema_name}.{agent_name}",
            "endpoint_name": endpoint_name,
        }

        print(
            f"  - Mapped config-only '{agent_name}' -> {config_file_name} "
            f"(no agent .py, needs {required_tools_count} tools)"
        )

    def _load_pip_requirements(self, agent_file: Path) -> List[str]:
        """Load pip requirements from requirements.txt next to the agent file."""
        req_path = agent_file.parent / "requirements.txt"
        if req_path.is_file():
            return [
                line.strip()
                for line in req_path.read_text(encoding="utf-8").splitlines()
                if line.strip() and not line.strip().startswith("#")
            ]
        return []

    def register_agent(
        self,
        agent_name: str,
        config: dict,
        assigned_tools: List[str],
        experiment_path: str,
    ) -> None:
        """Register a specific agent to Unity Catalog via MLflow."""
        mlflow.set_tracking_uri("databricks")
        mlflow.set_experiment(experiment_path)

        resources = [DatabricksServingEndpoint(endpoint_name=self.llm_endpoint_name)]
        for tool in assigned_tools:
            uc_tool_name = f"{self.catalog_name}.{self.schema_name}.{tool}"
            resources.append(DatabricksFunction(function_name=uc_tool_name))

        input_example = {
            "input": [
                {
                    "role": "user",
                    "content": f"Describe the {assigned_tools[0]} tool" if assigned_tools else "Hello",
                }
            ]
        }

        print(input_example)

        warnings.filterwarnings("ignore", category=UserWarning, module="mlflow.types.type_hints")

        tags = {
            "training_type": "db_academytraining",
            "model": self.llm_endpoint_name,
            "agent_type": "TOOL-CALLING",
            "agent_id": agent_name,
        }

        artifacts = {"agent_config": str(config["config_output"])}
        if self.eval_config_output_path.exists():
            artifacts["agent_eval_config"] = str(self.eval_config_output_path)

        # Lakebase: create checkpoint tables before logging the model so the
        # agent's memory store is ready when the model is loaded at inference.
        if self.lakebase_instance_name:
            try:
                from databricks_langchain import CheckpointSaver
                with CheckpointSaver(instance_name=self.lakebase_instance_name) as saver:
                    saver.setup()
                print(f"  Lakebase checkpoint tables ready (instance: {self.lakebase_instance_name})")
            except ImportError:
                print("  Warning: databricks_langchain not installed — skipping CheckpointSaver setup")
        if self.lakebase_autoscaling_project and self.lakebase_autoscaling_branch:
            from databricks_langchain import CheckpointSaver
            with CheckpointSaver(
                project=self.lakebase_autoscaling_project, 
                branch=self.lakebase_autoscaling_branch
                ) as saver:
                saver.setup()
            print(f"  Lakebase checkpoint tables ready (project: {self.lakebase_autoscaling_project}, branch: {self.lakebase_autoscaling_branch})")
                    

        if config.get("agent_file") is None:
            print(
                f"  No agent .py file for '{agent_name}' — config rendered, "
                f"skipping MLflow registration."
            )
            return

        with mlflow.start_run(tags=tags):
            logged_agent_info = mlflow.pyfunc.log_model(
                name=agent_name,
                python_model=str(config["agent_file"]),
                model_config=str(config["config_output"]),
                artifacts=artifacts,
                input_example=input_example,
                pip_requirements=self._load_pip_requirements(config["agent_file"]),
                resources=resources,
            )

            mlflow.set_registry_uri("databricks-uc")

            uc_registered_model_info = mlflow.register_model(
                model_uri=logged_agent_info.model_uri,
                name=config["uc_model_name"],
            )

        mfc = MlflowClient()
        mfc.set_registered_model_alias(
            config["uc_model_name"],
            self.alias,
            uc_registered_model_info.version,
        )

        for key, value in tags.items():
            mfc.set_model_version_tag(
                name=config["uc_model_name"],
                version=uc_registered_model_info.version,
                key=key,
                value=value,
            )

        print(
            f"  Model registered: {config['uc_model_name']} "
            f"(version {uc_registered_model_info.version}, alias '{self.alias}')"
        )

        config["experiment_path"] = experiment_path

    def deploy_agent(self, agent_name: str, endpoint_name: str = None):
        """Deploy a specific agent to a Databricks serving endpoint."""
        if agent_name not in self.agent_configs:
            raise ValueError(f"Agent '{agent_name}' not found in configurations")

        config = self.agent_configs[agent_name]
        uc_model_name = config["uc_model_name"]

        if endpoint_name is None:
            endpoint_name = config["endpoint_name"]

        experiment_path = config.get("experiment_path")
        if experiment_path:
            mlflow.set_tracking_uri("databricks")
            mlflow.set_experiment(experiment_path)

        print("=" * 60)
        print(f"Deploying agent: {uc_model_name}")
        print(f"Endpoint name: {endpoint_name}")
        print("=" * 60)

        client = MlflowClient()
        mv = client.get_model_version_by_alias(uc_model_name, self.alias)
        model_version = int(mv.version)
        print(f"  Retrieved model version {model_version} with alias '{self.alias}'")

        try:
            from databricks.agents import get_deployments
            existing = get_deployments(endpoint_name=endpoint_name)
            if existing:
                print(f"  Endpoint already exists: {existing.endpoint_name}")
                return existing
        except Exception:
            pass

        try:
            deployment = agents.deploy(
                model_name=uc_model_name,
                model_version=model_version,
                endpoint_name=endpoint_name,
                scale_to_zero=True,
            )
            print(f"  Deployed endpoint: {deployment.endpoint_name}")
            return deployment
        except Exception as e:
            print(f"  Deployment failed: {e}")
            return None

    def load_agent(self, agent_name: str):
        """Load a specific agent from Unity Catalog."""
        if agent_name not in self.agent_configs:
            raise ValueError(f"Agent '{agent_name}' not found")
        mlflow.set_registry_uri("databricks-uc")
        uc_model_name = self.agent_configs[agent_name]["uc_model_name"]
        return mlflow.pyfunc.load_model(f"models:/{uc_model_name}@{self.alias}")

    def get_experiment_path(self, agent_name: str) -> str:
        """Get the experiment path for a specific agent."""
        if agent_name not in self.agent_configs:
            raise ValueError(f"Agent '{agent_name}' not found")
        experiment_path = self.agent_configs[agent_name].get("experiment_path")
        if not experiment_path:
            experiment_name = self.agent_configs[agent_name]["experiment_name"]
            experiment_path = f"/Workspace/Users/{self.username}/{experiment_name}"
        return experiment_path
