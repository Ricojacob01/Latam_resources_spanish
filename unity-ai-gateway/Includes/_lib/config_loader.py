import yaml
from pathlib import Path
from typing import Any, Dict, Optional


class ConfigLoader:
    """Loads and manages demo configuration from a YAML file."""

    def __init__(self, config_path: Optional[str | Path] = None):
        if config_path is None:
            config_path = Path(__file__).parent.parent / "config" / "config.yaml"
        self.config_path = Path(config_path)
        self.config = self._load_config()

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config file not found: {self.config_path}\n"
                "Please create a config file based on Includes_v2/config/config.yaml"
            )
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)
        if not config:
            raise ValueError(f"Config file is empty: {self.config_path}")
        return config

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value using dot notation (e.g. 'catalog.schema_name', 'agents.llm_endpoint_name')."""
        value = self.config
        for k in key.split("."):
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        return value

    def get_all(self) -> Dict[str, Any]:
        return self.config

    def get_catalog_config(self) -> Dict[str, Any]:
        return self.config.get("catalog", {})

    def get_data_config(self) -> Dict[str, Any]:
        return self.config.get("data", {})

    def get_genie_config(self) -> Dict[str, Any]:
        return self.config.get("genie", {})

    def get_agent_config(self) -> Dict[str, Any]:
        return self.config.get("agents", {})

    def get_apps_config(self) -> Dict[str, Any]:
        return self.config.get("apps", {})

    def get_agent_app_config(self) -> Dict[str, Any]:
        return self.config.get("agent_app", {})

    def get_mlflow_config(self) -> Dict[str, Any]:
        return self.config.get("mlflow", {})

    def get_vector_search_config(self) -> Dict[str, Any]:
        return self.config.get("vector_search", {})

    def get_libraries_config(self) -> Dict[str, Any]:
        return self.config.get("libraries", {})

    def validate(self) -> bool:
        """Validate required fields are present."""
        errors = []

        catalog = self.config.get("catalog", {})
        if not catalog.get("schema_name"):
            errors.append("catalog.schema_name is required")

        data = self.config.get("data", {})
        if data:
            if not data.get("table_name"):
                errors.append("data.table_name is required when data section exists")

        if errors:
            msg = "Configuration validation failed:\n"
            for i, e in enumerate(errors, 1):
                msg += f"  {i}. {e}\n"
            msg += f"\nConfig file: {self.config_path}"
            raise ValueError(msg)

        return True


def load_config(config_path: Optional[str | Path] = None) -> ConfigLoader:
    """Convenience function to load configuration."""
    return ConfigLoader(config_path)
