from pathlib import Path
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()


def create_volume(
    catalog_name: str,
    schema_name: str,
    volume_name: str = "demo_vol",
) -> Path:
    """
    Create a Unity Catalog volume if it doesn't exist.

    Parameters
    ----------
    catalog_name : str
        The UC catalog name.
    schema_name : str
        The UC schema name.
    volume_name : str
        The volume name. Default: 'demo_vol'.

    Returns
    -------
    Path
        Path to the volume: /Volumes/{catalog}/{schema}/{volume}
    """
    spark.sql(f"USE CATALOG {catalog_name}")
    spark.sql(f"USE SCHEMA {schema_name}")
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {volume_name}")

    volume_path = Path(f"/Volumes/{catalog_name}/{schema_name}/{volume_name}")
    print(f"  Volume ready: {volume_path}")
    return volume_path


def copy_from_folder_to_vol(
    source_dir: str | Path,
    volume_path: str | Path,
    extensions: tuple | None = None,
) -> list[Path]:
    """
    Copy files from a local folder into a UC volume path.

    Parameters
    ----------
    source_dir : str | Path
        Source directory to copy from.
    volume_path : str | Path
        Destination UC volume path.
    extensions : tuple | None
        Optional tuple of file extensions to filter (e.g., ('.json', '.csv')).
        If None, all files are copied.

    Returns
    -------
    list[Path]
        List of destination paths for copied files.
    """
    source = Path(source_dir)
    dest = Path(volume_path)

    if not source.exists():
        print(f"  Warning: Source folder not found: {source}")
        return []

    dest.mkdir(parents=True, exist_ok=True)

    copied = []
    for file_path in sorted(source.iterdir()):
        if not file_path.is_file():
            continue
        if extensions and file_path.suffix.lower() not in extensions:
            continue

        dest_file = dest / file_path.name
        dest_file.write_bytes(file_path.read_bytes())
        copied.append(dest_file)
        print(f"  Copied: {file_path.name} → {dest_file}")

    return copied
