# Agent Bricks Manager - API Wrapper for Databricks Agent Bricks
# Subset of operations needed for course setup (KA creation, MAS support, tile management)

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.knowledgeassistants import KnowledgeSource, IndexSpec

logger = logging.getLogger(__name__)


class TileType(Enum):
    KA = 3
    MAS = 5


class EndpointStatus(Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    PROVISIONING = "PROVISIONING"
    NOT_READY = "NOT_READY"


@dataclass(frozen=True)
class KAIds:
    tile_id: str
    name: str


class AgentBricksManager:
    """Wrapper for Agent Bricks tiles (Knowledge Assistants and Multi-Agent Supervisors)."""

    def __init__(self, w: WorkspaceClient, *, default_timeout_s: int = 600):
        self.w = w
        self.default_timeout_s = default_timeout_s

    @staticmethod
    def sanitize_name(name: str) -> str:
        sanitized = name.replace(" ", "_")
        sanitized = re.sub(r"[^a-zA-Z0-9_-]", "_", sanitized)
        sanitized = re.sub(r"[_-]{2,}", "_", sanitized)
        sanitized = sanitized.strip("_-")
        return sanitized or "knowledge_assistant"

    # ---------- KA operations ----------

    def ka_create(
        self,
        name: str,
        knowledge_sources: List[Dict[str, Any]],
        description: Optional[str] = None,
        instructions: Optional[str] = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "name": self.sanitize_name(name),
            "knowledge_sources": knowledge_sources,
        }
        if instructions:
            payload["instructions"] = instructions
        if description:
            payload["description"] = description
        return self._post("/api/2.0/knowledge-assistants", payload)

    def ka_get(self, tile_id: str) -> Optional[Dict[str, Any]]:
        try:
            return self._get(f"/api/2.0/knowledge-assistants/{tile_id}")
        except Exception as e:
            if "does not exist" in str(e).lower() or "not found" in str(e).lower():
                return None
            raise

    def ka_get_endpoint_status(self, tile_id: str) -> Optional[str]:
        ka = self.ka_get(tile_id)
        if not ka:
            return None
        return ka.get("knowledge_assistant", {}).get("status", {}).get("endpoint_status")

    def ka_wait_until_endpoint_online(self, tile_id: str, timeout_s: Optional[int] = None) -> Dict[str, Any]:
        timeout_s = timeout_s or self.default_timeout_s
        deadline = time.time() + timeout_s
        last_status = None

        while True:
            ka = self.ka_get(tile_id)
            status = ka.get("knowledge_assistant", {}).get("status", {}).get("endpoint_status") if ka else None
            if status != last_status:
                logger.info(f"KA status: {last_status} -> {status}")
                last_status = status
            if status == "ONLINE":
                return ka
            if time.time() >= deadline:
                raise TimeoutError(f"KA {tile_id} did not come ONLINE within {timeout_s}s (last: {last_status})")
            time.sleep(10)

    def share(self, tile_id: str, changes: List[Dict[str, Any]]) -> None:
        self._post(f"/api/2.0/knowledge-assistants/{tile_id}/share", {"changes": changes})

    def delete(self, tile_id: str) -> None:
        self._delete(f"/api/2.0/tiles/{tile_id}")

    def find_by_name(self, name: str) -> Optional[KAIds]:
        filter_q = f"name_contains={name}&&tile_type=KA"
        page_token = None
        while True:
            params = {"filter": filter_q}
            if page_token:
                params["page_token"] = page_token
            resp = self._get("/api/2.0/tiles", params=params)
            for t in resp.get("tiles", []):
                if t.get("name") == name:
                    return KAIds(tile_id=t["tile_id"], name=name)
            page_token = resp.get("next_page_token")
            if not page_token:
                break
        return None

    @staticmethod
    def ka_get_knowledge_sources_from_volumes(
        volume_paths: List[Tuple[str, Optional[str]]],
    ) -> List[Dict[str, Any]]:
        knowledge_sources = []
        for idx, (volume_path, description) in enumerate(volume_paths):
            path_parts = volume_path.rstrip("/").split("/")
            source_name = path_parts[-1] if path_parts else f"source_{idx + 1}"
            source_name = source_name.replace(" ", "_").replace(".", "_")
            knowledge_source = {
                "files_source": {
                    "name": source_name,
                    "type": "files",
                    "files": {"path": volume_path},
                }
            }
            knowledge_sources.append(knowledge_source)
        return knowledge_sources

    def ka_add_index_source(
        self,
        parent_name: str,
        index_name: str,
        text_col: str,
        doc_uri_col: str,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
    ) -> Any:
        """
        Attach a Vector Search index to an existing Knowledge Assistant as a
        knowledge source. Uses the SDK surface (w.knowledge_assistants) because
        the index/IndexSpec types are the documented entry point for index-backed
        sources.

        Args:
            parent_name: The KA's `name` (e.g. as returned from ka_create ->
                ka_result["knowledge_assistant"]["name"]). Note this is the KA
                *name*, not the tile_id.
            index_name: Fully-qualified UC index name (catalog.schema.index).
            text_col:    Column containing the chunk text.
            doc_uri_col: Column containing the source document URI.
        """

        source = KnowledgeSource(
            display_name=display_name or index_name.split(".")[-1],
            description=description,
            source_type="index",
            index=IndexSpec(
                index_name=index_name,
                text_col=text_col,
                doc_uri_col=doc_uri_col,
            ),
        )
        return self.w.knowledge_assistants.create_knowledge_source(
            parent=parent_name,
            knowledge_source=source,
        )

    # ---------- Tile listing ----------

    def list_all_agent_bricks(self, tile_type: Optional[TileType] = None, page_size: int = 100) -> List[Dict[str, Any]]:
        all_tiles = []
        filter_q = f"tile_type={tile_type.name}" if tile_type else None
        page_token = None
        while True:
            params = {"page_size": page_size}
            if filter_q:
                params["filter"] = filter_q
            if page_token:
                params["page_token"] = page_token
            resp = self._get("/api/2.0/tiles", params=params)
            for tile in resp.get("tiles", []):
                if tile_type:
                    tv = tile.get("tile_type")
                    if tv == tile_type.value or tv == tile_type.name:
                        all_tiles.append(tile)
                else:
                    all_tiles.append(tile)
            page_token = resp.get("next_page_token")
            if not page_token:
                break
        return all_tiles

    # ---------- HTTP wrappers ----------

    def _handle_response_error(self, response, method, path):
        if response.status_code >= 400:
            try:
                error_data = response.json()
                msg = error_data.get("message", error_data.get("error", str(error_data)))
                raise Exception(f"{method} {path} failed: {msg}")
            except ValueError:
                raise Exception(f"{method} {path} failed with status {response.status_code}: {response.text}")

    def _get(self, path, params=None, timeout=30):
        headers = self.w.config.authenticate()
        url = f"{self.w.config.host}{path}"
        r = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
        if r.status_code >= 400:
            self._handle_response_error(r, "GET", path)
        return r.json()

    def _post(self, path, body, timeout=120):
        headers = self.w.config.authenticate()
        headers["Content-Type"] = "application/json"
        url = f"{self.w.config.host}{path}"
        r = requests.post(url, headers=headers, json=body, timeout=timeout)
        if r.status_code >= 400:
            self._handle_response_error(r, "POST", path)
        return r.json()

    def _patch(self, path, body, timeout=60):
        headers = self.w.config.authenticate()
        headers["Content-Type"] = "application/json"
        url = f"{self.w.config.host}{path}"
        r = requests.patch(url, headers=headers, json=body, timeout=timeout)
        if r.status_code >= 400:
            self._handle_response_error(r, "PATCH", path)
        return r.json()

    def _delete(self, path, timeout=30):
        headers = self.w.config.authenticate()
        url = f"{self.w.config.host}{path}"
        r = requests.delete(url, headers=headers, timeout=timeout)
        if r.status_code >= 400:
            self._handle_response_error(r, "DELETE", path)
        return r.json()
