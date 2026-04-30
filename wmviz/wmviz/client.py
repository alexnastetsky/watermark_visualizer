"""Databricks SDK auth + warehouse selection."""
from __future__ import annotations

import os
from configparser import ConfigParser
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import EndpointInfoWarehouseType


@dataclass
class WarehouseChoice:
    id: str
    name: str
    state: str


def list_profiles(cfg_path: str | None = None) -> list[str]:
    path = Path(cfg_path or os.path.expanduser("~/.databrickscfg"))
    if not path.exists():
        return []
    parser = ConfigParser()
    try:
        parser.read(path)
    except Exception:
        return []
    return list(parser.sections()) + (["DEFAULT"] if parser.defaults() else [])


def make_client(profile: str | None = None) -> WorkspaceClient:
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


def list_warehouses(client: WorkspaceClient) -> list[WarehouseChoice]:
    out: list[WarehouseChoice] = []
    try:
        for w in client.warehouses.list():
            out.append(
                WarehouseChoice(
                    id=w.id,
                    name=w.name or w.id,
                    state=str(getattr(w, "state", "") or ""),
                )
            )
    except Exception:
        return []
    # Running warehouses first
    out.sort(key=lambda w: (0 if "RUN" in w.state.upper() else 1, w.name.lower()))
    return out


def pick_default_warehouse(client: WorkspaceClient) -> WarehouseChoice | None:
    whs = list_warehouses(client)
    if not whs:
        return None
    return whs[0]
