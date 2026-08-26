# Includes_v2/config_renderer.py

import re
from pathlib import Path
from string import Template
from typing import Optional


class ConfigRenderer:
    """
    Renders configuration files from templates.

    Responsible for:
    - Reading template files
    - Substituting $VARIABLE placeholders
    - Writing rendered configs
    """

    def __init__(self, artifacts_dir: Path):
        self.artifacts_dir = artifacts_dir
        self.eval_config_output_path = artifacts_dir / "agent_eval_config.yaml"

    def render_text_template(self, template_path: Path, substitutions: dict) -> str:
        """Read a text/YAML template and substitute $VARS using string.Template."""
        if not template_path.exists():
            raise FileNotFoundError(f"Template file not found: {template_path}")
        raw = template_path.read_text(encoding="utf-8")
        return Template(raw).safe_substitute(substitutions)

    def update_yaml_config(self, yaml_text: str, yaml_path: Path) -> None:
        """Write rendered YAML text to file."""
        yaml_path.parent.mkdir(parents=True, exist_ok=True)
        yaml_path.write_text(yaml_text, encoding="utf-8")

    def render_eval_config(
        self,
        correctness_endpoint: Optional[str] = None,
        retrieval_sufficiency_endpoint: Optional[str] = None,
        guidelines_endpoint: Optional[str] = None,
        custom_endpoint: Optional[str] = None,
        recall_endpoint: Optional[str] = None,
        safety_endpoint: Optional[str] = None,
        template_dir: str | Path = "./agent configs",
    ) -> None:
        """
        Render the evaluation config from the template, including only the
        eval datasets whose endpoints are configured (non-null).

        Parameters
        ----------
        correctness_endpoint : str | None
        retrieval_sufficiency_endpoint : str | None
        guidelines_endpoint : str | None
        custom_endpoint : str | None
        recall_endpoint : str | None
        safety_endpoint : str | None
        template_dir : str | Path
            Directory containing agent_eval_config.yaml template.
        """
        eval_template_path = Path(template_dir) / "agent_eval_config.yaml"
        if not eval_template_path.exists():
            raise FileNotFoundError(f"Eval config template not found: {eval_template_path}")

        # Build substitutions only for configured (non-null) endpoints
        substitutions = {}
        if correctness_endpoint:
            substitutions["CORRECTNESS_EVAL_ENDPOINT"] = correctness_endpoint
        if retrieval_sufficiency_endpoint:
            substitutions["RETRIEVAL_SUFFICIENCY_ENDPOINT"] = retrieval_sufficiency_endpoint
        if guidelines_endpoint:
            substitutions["GUIDELINES_ENDPOINT"] = guidelines_endpoint
        if custom_endpoint:
            substitutions["CUSTOM_EVAL_ENDPOINT"] = custom_endpoint
        if recall_endpoint:
            substitutions["RECALL_EVAL_ENDPOINT"] = recall_endpoint
        if safety_endpoint:
            substitutions["SAFETY_EVAL_ENDPOINT"] = safety_endpoint

        raw = eval_template_path.read_text(encoding="utf-8")
        # safe_substitute leaves unfilled $VAR placeholders as-is (no KeyError)
        rendered = Template(raw).safe_substitute(substitutions)
        # Remove lines whose value is still an unfilled placeholder: key: "$VAR"
        filtered_lines = [
            line for line in rendered.splitlines()
            if not re.search(r'"\$[A-Z_]+"', line)
        ]
        yaml_text = "\n".join(filtered_lines) + "\n"

        self.update_yaml_config(yaml_text, self.eval_config_output_path)

        configured = [k for k, v in {
            "correctness": correctness_endpoint,
            "retrieval_sufficiency": retrieval_sufficiency_endpoint,
            "guidelines": guidelines_endpoint,
            "custom": custom_endpoint,
            "recall": recall_endpoint,
            "safety": safety_endpoint,
        }.items() if v]
        print(f"  Evaluation config written ({', '.join(configured)}): {self.eval_config_output_path}")
