import yaml
import os

def create_file(filepath: str, content: str = "", mode: str = "w", encoding: str = "utf-8"):
    """
    Create any file at the specified path.

    Args:
        filepath: Path to the file (e.g., "output/report.txt", "data.json")
        content:  Content to write to the file (default: empty)
        mode:     Write mode — 'w' (text), 'wb' (binary), 'a' (append)
        encoding: File encoding (ignored for binary mode)
    """

    # Create parent directories if they don't exist
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)

    if "b" in mode:
        with open(filepath, mode) as f:
            f.write(content if isinstance(content, bytes) else content.encode(encoding))
    else:
        with open(filepath, mode, encoding=encoding) as f:
            f.write(content)

    print(f"File created: {filepath}")

def create_folder(folderpath: str):
    """
    Create a folder (and any necessary parent folders).

    Args:
        folderpath: Path to the folder (e.g., "output/data/raw")
    """
    os.makedirs(folderpath, exist_ok=True)
    print(f"Folder created: {folderpath}")


def literal_str_representer(dumper, data):
    """Use block style (|) for multiline strings."""
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)

yaml.add_representer(str, literal_str_representer)

def create_yaml_file(filepath: str, data: dict):
    os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        yaml.dump(data, f, default_flow_style=False, allow_unicode=True)
    print(f"YAML file created: {filepath}")