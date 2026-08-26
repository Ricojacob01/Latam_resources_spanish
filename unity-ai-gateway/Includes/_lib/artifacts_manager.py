# demo_setup/artifacts_manager.py

import json
from pathlib import Path
from typing import Iterable, List, Set


class ArtifactsManager:
    """
    Manages artifacts directory structure and file copying operations.
    
    Responsible for:
    - Creating/cleaning the artifacts directory
    - Copying .py files, configs, and evaluation datasets
    - Managing both local and UC Volume file operations
    """
    
    def __init__(
        self,
        artifacts_dir: Path = Path("./artifacts"),
        volume_path: Path = None,
        eval_dir_switch: bool = True):
        """
        Initialize the artifacts manager.

        Parameters
        ----------
        artifacts_dir : Path
            Local artifacts directory path (default: ./artifacts)
        volume_path : Path
            UC Volume path for storing evaluation datasets
        eval_dir_switch : bool
            Whether to create and manage the evaluation_datasets directory (default: True)
        """
        self.artifacts_dir = artifacts_dir
        self.volume_path = volume_path
        self.configs_dir = artifacts_dir / "configs"
        self.eval_dir_switch = eval_dir_switch
        if self.eval_dir_switch:
            self.eval_datasets_dir = artifacts_dir / "evaluation_datasets"
    
    def copy_py_files_with_structure(
        self, 
        py_source_dir: str | Path,
        configs_source_dir: str | Path = "./agent configs",
        eval_datasets_source_dir: str | Path = None,
        recursive: bool = True
    ) -> None:
        """
        Copy files from source directories to create the artifacts structure locally and in UC volume.
        Deletes existing artifacts directory first to ensure clean state.
        
        Creates local TARGET structure:
            ./artifacts/
            ├── *.py files (agent files)
            ├── configs/
            │   └── *.yaml files (agent configs)
            └── evaluation_datasets/
                └── *.json files (evaluation datasets)
        
        Also copies to UC volume TARGET:
            /Volumes/{catalog}/{schema}/agent_vol/
            └── *.json files (evaluation datasets)
        
        Parameters
        ----------
        py_source_dir : str | Path
            SOURCE directory to crawl for .py files
        configs_source_dir : str | Path
            SOURCE directory for agent config YAML files
        eval_datasets_source_dir : str | Path
            SOURCE directory for evaluation dataset JSON files (optional)
        recursive : bool
            Whether to crawl subdirectories for .py files (default: True)
        """
        py_source = Path(py_source_dir)
        configs_source = Path(configs_source_dir)
        
        print("=" * 60)
        print("Creating Artifacts Structure")
        print("=" * 60)
        
        # Create directory structure (or reuse it if it already exists)
        self._create_directories()
        
        # Copy files
        self._copy_py_files(py_source, recursive)
        self._copy_config_files(configs_source)
        
        if eval_datasets_source_dir and self.eval_dir_switch:
            self._copy_evaluation_datasets(Path(eval_datasets_source_dir))
        
        print("=" * 60)
        print(f"✅ All files copied successfully")
        print("=" * 60)
    
    def _create_directories(self) -> None:
        """Create artifacts directory structure, or reuse it if it already exists."""
        existed = self.artifacts_dir.exists()

        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.configs_dir.mkdir(parents=True, exist_ok=True)

        if existed:
            print(f"♻️  Artifacts directory already exists — existing files will be overwritten.")
        else:
            print(f"✅ Created target folder structure:")

        print(f"  - {self.artifacts_dir}")
        print(f"  - {self.configs_dir}")
        if self.eval_dir_switch:
            self.eval_datasets_dir.mkdir(parents=True, exist_ok=True)
            print(f"  - {self.eval_datasets_dir}")
        print()
    
    def _copy_py_files(self, source: Path, recursive: bool) -> None:
        """Copy Python files from source to artifacts directory."""
        if not source.exists():
            print(f"⚠️ Source directory does not exist: {source}\n")
            return

        # Remove stale .py files so agents from previous runs don't persist
        for old_py in self.artifacts_dir.glob("*.py"):
            old_py.unlink()

        pattern = "**/*.py" if recursive else "*.py"
        py_count = 0

        print(f"Copying .py files from {source} to artifacts/...")
        for py_file in source.glob(pattern):
            if py_file.is_file():
                dest_path = self.artifacts_dir / py_file.name
                content = py_file.read_text(encoding="utf-8")
                dest_path.write_text(content, encoding="utf-8")
                print(f"  ✅ Copied: {py_file.name}")
                py_count += 1

        # Copy requirements.txt if present (used during UC model registration)
        req_file = source / "requirements.txt"
        if req_file.is_file():
            dest_req = self.artifacts_dir / "requirements.txt"
            dest_req.write_text(req_file.read_text(encoding="utf-8"), encoding="utf-8")
            print(f"  ✅ Copied: requirements.txt")

        print(f"✅ Copied {py_count} .py files\n")
    
    def _copy_config_files(self, source: Path) -> None:
        """Copy YAML config files from source to artifacts/configs/."""
        if not source.exists():
            print(f"⚠️ Config source directory does not exist: {source}\n")
            return
        
        yaml_count = 0
        
        print(f"Copying config files from {source} to artifacts/configs/...")
        for yaml_file in source.glob("*.yaml"):
            if yaml_file.is_file():
                dest_path = self.configs_dir / yaml_file.name
                content = yaml_file.read_text(encoding="utf-8")
                dest_path.write_text(content, encoding="utf-8")
                print(f"  ✅ Copied: {yaml_file.name}")
                yaml_count += 1
        
        print(f"✅ Copied {yaml_count} config files\n")
    
    def _copy_evaluation_datasets(self, source: Path) -> None:
        """Copy JSON evaluation datasets to both local artifacts and UC Volume."""
        if not source.exists():
            print(f"⚠️ Evaluation datasets source directory does not exist: {source}\n")
            return
        
        json_count = 0
        
        # Ensure UC Volume directory exists if volume_path is set
        if self.volume_path:
            self.volume_path.mkdir(parents=True, exist_ok=True)
        
        print(f"Copying evaluation datasets from {source}...")
        for json_file in source.glob("*.json"):
            if json_file.is_file():
                # Read and format JSON data once
                with json_file.open("r", encoding="utf-8") as f:
                    eval_dataset = json.load(f)
                if isinstance(eval_dataset, dict):
                    eval_dataset = [eval_dataset]
                
                # Copy to local artifacts/evaluation_datasets/
                local_dest = self.eval_datasets_dir / json_file.name
                with local_dest.open("w", encoding="utf-8") as f:
                    json.dump(eval_dataset, f, indent=2, ensure_ascii=False)
                
                # Copy to UC Volume if path is set
                if self.volume_path:
                    volume_dest = self.volume_path / json_file.name
                    with volume_dest.open("w", encoding="utf-8") as f:
                        json.dump(eval_dataset, f, indent=2, ensure_ascii=False)
                    print(f"  ✅ Copied: {json_file.name} (to local artifacts/ and UC volume)")
                else:
                    print(f"  ✅ Copied: {json_file.name} (to local artifacts/)")
                
                json_count += 1
        
        print(f"✅ Copied {json_count} evaluation datasets\n")
    
    def get_filenames_without_extension(
        self,
        root_dir: str | Path,
        extensions: Iterable[str] = (".txt", ".json", ".yaml", ".yml"),
        recursive: bool = True,
    ) -> List[str]:
        """
        Crawl a directory and return filenames without extensions.

        Parameters
        ----------
        root_dir : str | Path
            Root directory to crawl
        extensions : Iterable[str]
            File extensions to include (must include leading dot)
        recursive : bool
            Whether to crawl subdirectories

        Returns
        -------
        List[str]
            Filenames without extension
        """
        root = Path(root_dir)

        if not root.exists():
            print(f"⚠️ Directory does not exist: {root}")
            return []

        ext_set: Set[str] = {ext.lower() for ext in extensions}
        pattern = "**/*" if recursive else "*"

        results: List[str] = []

        for path in root.glob(pattern):
            if path.is_file() and path.suffix.lower() in ext_set:
                results.append(path.stem)

        return sorted(results)