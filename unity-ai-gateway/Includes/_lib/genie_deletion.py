import time
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
        if resp.status_code in (200, 201, 204):
            return resp
        if resp.status_code == 429 or resp.status_code >= 500:
            attempts += 1
            delay = min(base * (2 ** (attempts - 1)), max_delay)
            time.sleep(delay + 0.1 * delay)
            continue
        resp.raise_for_status()


def list_spaces_by_title(space_title: str) -> list[dict]:
    """Return all Genie spaces whose title matches (case-insensitive)."""
    host, headers = _get_auth_headers()
    resp = _request_with_retry("GET", f"{host}/api/2.0/genie/spaces", headers=headers)
    spaces = resp.json().get("spaces", [])
    return [s for s in spaces if s.get("title", "").lower() == space_title.lower()]


def delete_genie_space(space_id: str) -> None:
    """Delete a Genie space by ID."""
    host, headers = _get_auth_headers()
    _request_with_retry("DELETE", f"{host}/api/2.0/genie/spaces/{space_id}", headers=headers)
    print(f"  Genie space deleted: {space_id}")


def delete_genie_space_by_title(title: str) -> None:
    """
    Find and delete all Genie spaces matching title (case-insensitive).

    Deletes all matches so the subsequent create always starts from a clean slate.
    Silently skips if no match is found.
    """
    spaces = list_spaces_by_title(title)
    if not spaces:
        print(f"  No existing Genie space found with title '{title}' — skipping deletion")
        return
    for space in spaces:
        delete_genie_space(space["space_id"])
