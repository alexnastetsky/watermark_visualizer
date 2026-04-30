"""FileReader implementations for local FS and Databricks workspace files / volumes."""
from __future__ import annotations

import os
from pathlib import Path

from databricks.sdk import WorkspaceClient


class LocalFileReader:
    """Reads from the local filesystem. Useful for testing with downloaded checkpoints."""

    def list(self, path: str) -> list[str]:
        p = Path(path)
        if not p.exists():
            return []
        return [str(child) for child in p.iterdir()]

    def read_text(self, path: str) -> str:
        return Path(path).read_text(encoding="utf-8")

    def exists(self, path: str) -> bool:
        return Path(path).exists()


class DatabricksFileReader:
    """Reads files via Databricks SDK.

    Volume paths (`/Volumes/...`) use the Files API. Workspace paths
    (`/Workspace/...`) use the Workspace API export. DBFS paths (`dbfs:/...`)
    use the DBFS API.
    """

    def __init__(self, client: WorkspaceClient):
        self.client = client

    def _kind(self, path: str) -> str:
        if path.startswith("/Volumes/"):
            return "volume"
        if path.startswith("dbfs:/"):
            return "dbfs"
        if path.startswith("/Workspace/"):
            return "workspace"
        # Assume volume by default — most checkpoints live there now
        return "volume"

    def list(self, path: str) -> list[str]:
        kind = self._kind(path)
        if kind == "volume":
            try:
                return [entry.path for entry in self.client.files.list_directory_contents(path)]
            except Exception:
                return []
        if kind == "dbfs":
            stripped = path[len("dbfs:") :]
            try:
                return [f"dbfs:{f.path}" for f in self.client.dbfs.list(stripped)]
            except Exception:
                return []
        if kind == "workspace":
            try:
                return [obj.path for obj in self.client.workspace.list(path)]
            except Exception:
                return []
        return []

    def read_text(self, path: str) -> str:
        kind = self._kind(path)
        if kind == "volume":
            resp = self.client.files.download(path)
            data = resp.contents.read() if hasattr(resp.contents, "read") else resp.contents
            return data.decode("utf-8")
        if kind == "dbfs":
            stripped = path[len("dbfs:") :]
            # dbfs.read returns base64 chunks; for small JSON files one read is enough
            chunks: list[bytes] = []
            offset = 0
            while True:
                resp = self.client.dbfs.read(stripped, offset=offset, length=1_000_000)
                if not resp.data:
                    break
                import base64
                chunk = base64.b64decode(resp.data)
                chunks.append(chunk)
                if len(chunk) == 0:
                    break
                offset += len(chunk)
                if resp.bytes_read < 1_000_000:
                    break
            return b"".join(chunks).decode("utf-8")
        if kind == "workspace":
            export = self.client.workspace.export(path)
            import base64
            return base64.b64decode(export.content).decode("utf-8")
        raise ValueError(f"Unsupported path kind for {path}")

    def exists(self, path: str) -> bool:
        kind = self._kind(path)
        try:
            if kind == "volume":
                # files.get_metadata works for files; for dirs we list parent
                try:
                    self.client.files.get_metadata(path)
                    return True
                except Exception:
                    pass
                # Fallback: try listing as a directory
                try:
                    next(iter(self.client.files.list_directory_contents(path)), None)
                    return True
                except Exception:
                    return False
            if kind == "dbfs":
                stripped = path[len("dbfs:") :]
                self.client.dbfs.get_status(stripped)
                return True
            if kind == "workspace":
                self.client.workspace.get_status(path)
                return True
        except Exception:
            return False
        return False


def make_reader(client: WorkspaceClient | None, source_path: str):
    """Pick a FileReader based on path prefix and whether a client is configured."""
    if source_path.startswith(("/Volumes/", "/Workspace/", "dbfs:/")):
        if client is None:
            raise RuntimeError(
                f"Path {source_path} requires a Databricks workspace client; "
                "configure a profile in the sidebar."
            )
        return DatabricksFileReader(client)
    return LocalFileReader()


def is_databricks_path(path: str) -> bool:
    return path.startswith(("/Volumes/", "/Workspace/", "dbfs:/"))
