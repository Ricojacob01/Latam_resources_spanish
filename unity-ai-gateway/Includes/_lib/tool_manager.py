# Includes_v2/tool_manager.py

import re
from pathlib import Path
from typing import List, Set


class ToolManager:
    """
    Manages Unity Catalog tool functions.

    Responsible for:
    - Discovering tools for each agent
    - Creating UC functions from DDL templates
    """

    def __init__(self, catalog_name: str, schema_name: str):
        self.catalog_name = catalog_name
        self.schema_name = schema_name

    def discover_all_tools(self, agent_tools_base_dir: str | Path) -> Set[tuple]:
        """
        Crawl all subfolders in agent_tools_base_dir and collect every .txt DDL file.

        Expected structure: {agent_tools_base_dir}/{agent_name}/tool1.txt

        Returns a set of (tool_name, tools_dir) tuples suitable for create_tools().
        """
        base = Path(agent_tools_base_dir)
        if not base.exists():
            return set()

        all_tools: Set[tuple] = set()
        for subfolder in sorted(base.iterdir()):
            if not subfolder.is_dir():
                continue
            for ddl_file in sorted(subfolder.glob("*.txt")):
                all_tools.add((ddl_file.stem, subfolder))

        return all_tools

    def get_tools_for_agent(
        self,
        agent_name: str,
        agent_tools_base_dir: str | Path,
    ) -> List[str]:
        """
        Return the tool names that belong to a specific agent without creating them.

        Looks in {agent_tools_base_dir}/{agent_name}/ for .txt files.
        """
        agent_tools_dir = Path(agent_tools_base_dir) / agent_name
        if not agent_tools_dir.exists():
            return []
        return [f.stem for f in sorted(agent_tools_dir.glob("*.txt"))]

    def discover_agent_tools(
        self,
        agent_name: str,
        required_tools_count: int,
        get_filenames_func,
        agent_tools_base_dir: str | Path = "./agent tools",
    ) -> List[str]:
        """
        Discover tools for a specific agent from their subfolder.

        Expected structure: {agent_tools_base_dir}/{agent_name}/tool1.txt

        Parameters
        ----------
        agent_name : str
            Name of the agent.
        required_tools_count : int
            Expected number of tools.
        get_filenames_func : callable
            Function to get filenames without extension from a directory.
        agent_tools_base_dir : str | Path
            Base directory containing per-agent tool subfolders.
        """
        agent_tools_dir = Path(agent_tools_base_dir) / agent_name

        if not agent_tools_dir.exists():
            print(f"  Warning: Tool directory not found for '{agent_name}': {agent_tools_dir}")
            return []

        agent_tools = get_filenames_func(
            agent_tools_dir,
            extensions=(".txt",),
            recursive=False,
        )

        if len(agent_tools) != required_tools_count:
            print(
                f"  Warning: Agent '{agent_name}' expects {required_tools_count} tools, "
                f"found {len(agent_tools)} in {agent_tools_dir}"
            )

        print(f"  - {agent_name}: {len(agent_tools)} tools from {agent_tools_dir.name}/")
        return agent_tools

    def create_tools(self, all_tools: Set[tuple]) -> None:
        """
        Create all discovered tools as UC functions.

        Parameters
        ----------
        all_tools : Set[tuple]
            Set of (tool_name, tools_dir) tuples.
        """
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        print("\n" + "=" * 60)
        print("Creating Tools")
        print("=" * 60)

        for tool_name, tools_dir in all_tools:
            tool_path = Path(tools_dir) / f"{tool_name}.txt"
            if not tool_path.exists():
                raise FileNotFoundError(f"Tool DDL template not found: {tool_path}")

            create_stmt = tool_path.read_text(encoding="utf-8")

            spark.sql(f"DROP FUNCTION IF EXISTS {tool_name}")
            spark.sql(create_stmt).collect()

            print(f"  UC function created: {tool_name} (from {Path(tools_dir).name}/)")

    @staticmethod
    def count_tool_placeholders(config_path: Path) -> int:
        """Count how many $TOOL{n} placeholders exist in a config template."""
        if not config_path.exists():
            return 0
        content = config_path.read_text(encoding="utf-8")
        tool_placeholders = re.findall(r'\$TOOL\d+', content)
        return len(set(tool_placeholders))
