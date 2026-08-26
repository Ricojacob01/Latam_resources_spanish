# Databricks notebook source
import sys, os
from Includes._lib.library_installer import install_libraries

includes_dir = "./Includes"
course_dir = os.path.dirname(includes_dir)
sys.path.insert(0, course_dir)

config_path = f"{includes_dir}/config/config-1.yaml"
install_libraries(config_path)

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import sys, os
from Includes import setup_demo_environment, build_setup_summary_html

includes_dir = "./Includes"
course_dir = os.path.dirname(includes_dir)
sys.path.insert(0, course_dir)

config_path = f"{includes_dir}/config/config-1.yaml"

# Optional catalog override. Most learners leave this blank and the config's
# default (labuser_<user>) is used. Set it only when you can't create a catalog
# and must point setup at an existing one you have access to. The value is
# passed as an override to setup_demo_environment, which forces that exact
# catalog (it must already exist).
dbutils.widgets.text("catalog_override", "", "Catalog override (optional)")
dbutils.widgets.text("schema_override", "", "Schema override (optional)")
catalog_override = dbutils.widgets.get("catalog_override").strip()
schema_override = dbutils.widgets.get("schema_override").strip()

# Resolve the workspace path to agent-app/ for the SDK app deploy. This notebook
# is run via %run from the lab, so notebookPath() returns the caller's path;
# handle both that case and running this notebook standalone. AppDeployer
# normalizes the /Workspace prefix, so we don't add it here.
_nb_path = (
    dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
_course_dir_ws = os.path.dirname(_nb_path)
if _course_dir_ws.endswith("/Includes"):
    _course_dir_ws = os.path.dirname(_course_dir_ws)
agent_app_path = f"{_course_dir_ws}/agent-app"

# Only pass overrides when the widgets are non-empty, so setup falls back to
# the config defaults otherwise.
overrides = {}
if catalog_override:
    overrides["catalog_name"] = catalog_override
if schema_override:
    overrides["schema_name"] = schema_override

env = setup_demo_environment(config_path, agent_app_path=agent_app_path, **overrides)

catalog_name = env["catalog_name"]
schema_name  = env["schema_name"]
username     = env["username"]

# Show a summary card of the workspace assets that were created.
from databricks.sdk import WorkspaceClient
displayHTML(build_setup_summary_html(env, workspace_host=WorkspaceClient().config.host))