"""Auto-discover input sources from a streaming checkpoint dir.

Each per-source JSON line in offsets/<n> identifies a source via
`reservoirId` (Delta), `path` (file source), or `topic` (Kafka). This is a
best-effort hint layer; the Streamlit sidebar always lets the user override
the resolved values.
"""
from __future__ import annotations

import json
from dataclasses import dataclass

from .checkpoint import FileReader, SourceOffset, load_all_offsets


@dataclass
class DiscoveredSource:
    source_index: int
    kind: str  # "delta", "file", "kafka", "unknown"
    hint: str  # human-readable hint (e.g. reservoirId, path, topic)
    path_or_table: str | None = None


def discover_sources(reader: FileReader, checkpoint_root: str) -> list[DiscoveredSource]:
    """Identify sources from the first available offsets file.

    Older checkpoints sometimes also have <root>/sources/<idx>/ but the
    offsets file is universal — every committed batch has it.
    """
    offsets = load_all_offsets(reader, checkpoint_root)
    if not offsets:
        return []
    # Use the first batch with at least one source line.
    first = next((b for b in offsets if b.sources), offsets[0])
    return [_describe_source_offset(s) for s in first.sources]


def _describe_source_offset(s: SourceOffset) -> DiscoveredSource:
    raw = s.raw
    if "reservoirId" in raw or "reservoirVersion" in raw:
        return DiscoveredSource(
            source_index=s.source_index,
            kind="delta",
            hint=f"reservoirId={raw.get('reservoirId', '?')}",
        )
    if "path" in raw:
        return DiscoveredSource(
            source_index=s.source_index,
            kind="file",
            hint=str(raw["path"]),
            path_or_table=str(raw["path"]),
        )
    if "topic" in raw:
        return DiscoveredSource(
            source_index=s.source_index,
            kind="kafka",
            hint=f"topic={raw['topic']}",
            path_or_table=str(raw["topic"]),
        )
    return DiscoveredSource(
        source_index=s.source_index,
        kind="unknown",
        hint=json.dumps(raw)[:200],
    )
