# _lib/library_installer.py
#
# Config-driven library installation for GenAI courses.
# Reads library groups from a config YAML, resolves packages + versions
# from the course's dependencies.toml, and installs them via pip.

import subprocess
import sys
import tomllib
import yaml
from pathlib import Path


def _build_spec(pkg: str, ver: str) -> str:
    """Build a pip install spec from a package name and version string."""
    if ver == "*":
        return pkg
    if ver[0] in ">=<~!":
        return f"{pkg}{ver}"
    return f"{pkg}=={ver}"


def _find_deps_file(config_path: str | Path) -> Path:
    """Locate the dependencies.toml for a given config file.

    Walks up from the config's directory looking for dependencies.toml
    in the course includes folder (the config's grandparent directory).
    """
    config_dir = Path(config_path).resolve().parent
    # config is at <course-includes>/config/config_N.yaml
    # dependencies.toml is at <course-includes>/dependencies.toml
    course_dir = config_dir.parent
    deps_file = course_dir / "dependencies.toml"
    if deps_file.exists():
        return deps_file
    raise FileNotFoundError(
        f"dependencies.toml not found at {deps_file}. "
        f"Each course includes folder must have its own dependencies.toml."
    )


def resolve_packages(config_path: str | Path) -> list[str]:
    """Resolve library groups from a config YAML into a deduplicated package list.

    Parameters
    ----------
    config_path : str | Path
        Path to a course config YAML containing a ``libraries.groups`` list.

    Returns
    -------
    list[str]
        Pip install specs, e.g. ``["pyyaml==6.0.3", "backoff==2.2.1"]``.
    """
    with open(config_path) as f:
        cfg = yaml.safe_load(f) or {}

    groups = (cfg.get("libraries") or {}).get("groups", [])
    if not groups:
        return []

    deps_file = _find_deps_file(config_path)
    with open(deps_file, "rb") as f:
        deps = tomllib.load(f)

    missing = [g for g in groups if g not in deps]
    if missing:
        raise KeyError(
            f"Library group(s) {missing} not found in {deps_file}. "
            f"Available: {sorted(deps.keys())}"
        )

    # Collect packages; last group wins on version conflicts.
    seen: dict[str, str] = {}
    for group in groups:
        for pkg, ver in deps[group].items():
            seen[pkg] = _build_spec(pkg, ver)

    return list(seen.values())


def install_libraries(config_path: str | Path) -> None:
    """Install libraries declared in a config YAML's ``libraries.groups`` section.

    Reads group names from the config, resolves them against the course's
    ``dependencies.toml``, and runs ``pip install -U`` with the resulting
    package list.

    Parameters
    ----------
    config_path : str | Path
        Path to a course config YAML.
    """
    packages = resolve_packages(config_path)
    if not packages:
        print("No library groups specified in config. Skipping install.")
        return

    print(f"Installing {len(packages)} package(s)...")

    # Install packages one at a time (matching the sequential !pip install
    # pattern used in Databricks notebooks) to avoid pip resolver conflicts
    # when pinned versions have overlapping transitive dependencies.
    for p in packages:
        print(f"  {p}")
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "-U", "-q", p]
        )
    print("Library installation complete.")
