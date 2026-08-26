import json
import time
import uuid
import requests
from databricks.sdk.runtime import dbutils


def _get_auth_headers() -> tuple[str, dict]:
    """Resolve workspace host and auth headers from the notebook context."""
    host = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get()
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return host, headers


def _request_with_retry(method: str, url: str, headers: dict, **kwargs) -> requests.Response:
    """HTTP request with exponential backoff on 429/5xx responses."""
    base, max_delay, attempts = 1.0, 8.0, 0
    while True:
        resp = requests.request(method, url, headers=headers, **kwargs)
        if resp.status_code in (200, 201):
            return resp
        if resp.status_code == 429 or resp.status_code >= 500:
            attempts += 1
            delay = min(base * (2 ** (attempts - 1)), max_delay)
            time.sleep(delay + 0.1 * delay)
            continue
        resp.raise_for_status()


def _hex_id() -> str:
    return uuid.uuid4().hex


def _get_warehouse_id(host: str, headers: dict, warehouse_name: str) -> str:
    """Return the warehouse ID matching warehouse_name, or raise if not found."""
    url = f"{host}/api/2.0/sql/warehouses"
    resp = _request_with_retry("GET", url, headers=headers)
    warehouses = resp.json().get("warehouses", [])
    matches = [w for w in warehouses if w.get("name", "").lower() == warehouse_name.lower()]
    if not matches:
        raise ValueError(f"No warehouse found with name='{warehouse_name}'.")
    if len(matches) > 1:
        raise ValueError(f"Multiple warehouses matched '{warehouse_name}': {[w['id'] for w in matches]}")
    return matches[0]["id"]


def create_genie_space(
    source_table: str,
    warehouse_name: str,
    space_title: str,
    space_description: str,
) -> str:
    """
    Create a Databricks Genie space backed by a Delta table.

    Parameters
    ----------
    source_table : str
        Fully-qualified table name (catalog.schema.table).
    warehouse_name : str
        Display name of the SQL warehouse to use.
    space_title : str
        Title shown in the Genie UI.
    space_description : str
        Description shown in the Genie UI.

    Returns
    -------
    str
        The created Genie space ID.
    """
    host, headers = _get_auth_headers()

    user_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
    parent_path = f"/Workspace/Users/{user_name}"

    warehouse_id = _get_warehouse_id(host, headers, warehouse_name)

    # Build the serialized space config
    sample_questions = sorted(
        [{"id": _hex_id(), "question": ["What's the average price of all listings?"]}],
        key=lambda q: q["id"],
    )
    tables = sorted(
        [{"identifier": source_table}],
        key=lambda t: t["identifier"],
    )
    text_instructions = sorted(
        [{"id": _hex_id(), "content": ["Round monetary values to 2 decimals."]}],
        key=lambda i: i["id"],
    )
    example_sqls = sorted(
        [{
            "id": _hex_id(),
            "question": ["Total price by neighbourhood"],
            "sql": [
                "SELECT AVG(price) as avg_price ",
                f"FROM {source_table}"
            ],
        }],
        key=lambda e: e["id"],
    )

    serialized_space = json.dumps({
        "version": 2,
        "config": {"sample_questions": sample_questions},
        "data_sources": {"tables": tables},
        "instructions": {
            "text_instructions": text_instructions,
            "example_question_sqls": example_sqls,
        },
    })

    # Create the space
    create_payload = {
        "title": space_title,
        "description": space_description,
        "parent_path": parent_path,
        "warehouse_id": warehouse_id,
        "serialized_space": serialized_space,
    }
    resp = _request_with_retry(
        "POST", f"{host}/api/2.0/genie/spaces", headers=headers, json=create_payload
    )
    space = resp.json()
    space_id = space.get("space_id") or space.get("id")
    if not space_id:
        raise RuntimeError(f"Create response missing space_id: {space}")
    print(f"  Genie space created: {space_id}")

    all_users_permissions = False
    
    if all_users_permissions:
        # Grant CAN_RUN to all workspace users
        acl = {"access_control_list": [
            {"group_name": "users", "permission_level": "CAN_RUN"},
        ]}
        _request_with_retry(
            "PUT", f"{host}/api/2.0/permissions/genie/{space_id}", headers=headers, json=acl
        )
        print(f"  Permissions set on Genie space: {space_id}")

    return space_id
