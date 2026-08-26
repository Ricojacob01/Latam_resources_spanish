import os

def _serverless_version_check(version_number: str):
    is_serverless = os.environ.get('IS_SERVERLESS', '')
    runtime_version = os.environ.get('DATABRICKS_RUNTIME_VERSION', '')

    return is_serverless == 'TRUE' and runtime_version.startswith(f'client.{version_number}.')