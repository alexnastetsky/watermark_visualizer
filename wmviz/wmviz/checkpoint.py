"""Parse Spark Structured Streaming checkpoint files.

Checkpoint layout:
  <root>/offsets/<batchId>     # one file per batch (header + JSON lines)
  <root>/commits/<batchId>     # one file per committed batch
  <root>/sources/<sourceId>/   # source-specific files (e.g. Delta)
  <root>/sinks/0/metadata      # streaming sink metadata
  <root>/metadata              # query metadata

The file API is abstracted via a `FileReader` protocol so both local FS
and Databricks workspace files / volumes can plug in.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Iterable, Protocol


class FileReader(Protocol):
    def list(self, path: str) -> list[str]: ...
    def read_text(self, path: str) -> str: ...
    def exists(self, path: str) -> bool: ...


@dataclass
class SourceOffset:
    """One source's offset at a batch boundary. Schema varies by source type."""
    source_index: int
    raw: dict

    @property
    def reservoir_version(self) -> int | None:
        return self.raw.get("reservoirVersion")

    @property
    def reservoir_id(self) -> str | None:
        return self.raw.get("reservoirId")

    @property
    def index(self) -> int | None:
        return self.raw.get("index")

    @property
    def is_delta(self) -> bool:
        return "reservoirId" in self.raw or "reservoirVersion" in self.raw


@dataclass
class BatchOffsets:
    batch_id: int
    batch_timestamp_ms: int
    batch_watermark_ms: int
    conf: dict = field(default_factory=dict)
    sources: list[SourceOffset] = field(default_factory=list)


@dataclass
class BatchCommit:
    batch_id: int
    next_batch_watermark_ms: int | None
    state_unique_keys: int | None = None


_BATCH_FILENAME_RE = re.compile(r"^(\d+)(?:\.compact)?$")


def _list_batch_files(reader: FileReader, dir_path: str) -> list[tuple[int, str]]:
    """Return (batch_id, full_path) sorted by batch_id, ignoring tmp/.crc files."""
    out: list[tuple[int, str]] = []
    if not reader.exists(dir_path):
        return out
    for entry in reader.list(dir_path):
        name = entry.rsplit("/", 1)[-1]
        m = _BATCH_FILENAME_RE.match(name)
        if not m:
            continue
        out.append((int(m.group(1)), entry))
    out.sort(key=lambda t: t[0])
    return out


def parse_offsets_file(text: str, batch_id: int) -> BatchOffsets:
    """Parse the contents of an offsets/<batchId> file.

    Format (Spark 3.x+):
        v1
        {"batchWatermarkMs":...,"batchTimestampMs":...,"conf":{...}}
        {<source 0 offset>}
        {<source 1 offset>}
        ...
    """
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        raise ValueError(f"offsets/{batch_id}: empty file")

    body_start = 0
    if lines[0].startswith("v"):
        body_start = 1

    if len(lines) <= body_start:
        raise ValueError(f"offsets/{batch_id}: missing metadata line")

    metadata = json.loads(lines[body_start])
    sources_raw = lines[body_start + 1 :]

    sources = [
        SourceOffset(source_index=i, raw=json.loads(ln))
        for i, ln in enumerate(sources_raw)
        if ln.strip() and ln.strip() != "-"  # "-" means "no offset" for that slot
    ]

    return BatchOffsets(
        batch_id=batch_id,
        batch_timestamp_ms=int(metadata.get("batchTimestampMs", 0)),
        batch_watermark_ms=int(metadata.get("batchWatermarkMs", 0)),
        conf=metadata.get("conf", {}) or {},
        sources=sources,
    )


def parse_commit_file(text: str, batch_id: int) -> BatchCommit:
    """Parse the contents of a commits/<batchId> file.

    Format (Spark 3.x+):
        v1
        {"nextBatchWatermarkMs":..., "stateUniqueKeys":{...}}
    """
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return BatchCommit(batch_id=batch_id, next_batch_watermark_ms=None)

    body_start = 1 if lines[0].startswith("v") else 0
    if len(lines) <= body_start:
        return BatchCommit(batch_id=batch_id, next_batch_watermark_ms=None)

    body = json.loads(lines[body_start])
    return BatchCommit(
        batch_id=batch_id,
        next_batch_watermark_ms=body.get("nextBatchWatermarkMs"),
        state_unique_keys=_extract_state_unique_keys(body),
    )


def _extract_state_unique_keys(body: dict) -> int | None:
    """`stateUniqueKeys` may be int, dict-of-int, or absent depending on Spark version."""
    suk = body.get("stateUniqueKeys")
    if suk is None:
        return None
    if isinstance(suk, (int, float)):
        return int(suk)
    if isinstance(suk, dict):
        try:
            return sum(int(v) for v in suk.values())
        except (TypeError, ValueError):
            return None
    return None


def load_all_offsets(reader: FileReader, checkpoint_root: str) -> list[BatchOffsets]:
    out: list[BatchOffsets] = []
    for batch_id, path in _list_batch_files(reader, f"{checkpoint_root}/offsets"):
        try:
            text = reader.read_text(path)
            out.append(parse_offsets_file(text, batch_id))
        except Exception as e:
            # Don't kill the whole load on one bad file
            out.append(
                BatchOffsets(
                    batch_id=batch_id,
                    batch_timestamp_ms=0,
                    batch_watermark_ms=0,
                    conf={"_parse_error": str(e)},
                    sources=[],
                )
            )
    return out


def load_all_commits(reader: FileReader, checkpoint_root: str) -> list[BatchCommit]:
    out: list[BatchCommit] = []
    for batch_id, path in _list_batch_files(reader, f"{checkpoint_root}/commits"):
        try:
            text = reader.read_text(path)
            out.append(parse_commit_file(text, batch_id))
        except Exception:
            out.append(BatchCommit(batch_id=batch_id, next_batch_watermark_ms=None))
    return out


def list_source_indexes(reader: FileReader, checkpoint_root: str) -> list[int]:
    sources_dir = f"{checkpoint_root}/sources"
    if not reader.exists(sources_dir):
        return []
    out = []
    for entry in reader.list(sources_dir):
        name = entry.rsplit("/", 1)[-1]
        if name.isdigit():
            out.append(int(name))
    out.sort()
    return out


def reservoir_version_per_source(
    offsets: Iterable[BatchOffsets],
) -> dict[int, list[tuple[int, int | None]]]:
    """For Delta sources, map source_index -> list of (batch_id, reservoirVersion)."""
    out: dict[int, list[tuple[int, int | None]]] = {}
    for b in offsets:
        for s in b.sources:
            if s.is_delta:
                out.setdefault(s.source_index, []).append((b.batch_id, s.reservoir_version))
    return out
