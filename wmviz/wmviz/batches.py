"""Build the unified per-batch DataFrame from checkpoint + Delta history."""
from __future__ import annotations

from dataclasses import dataclass

import pandas as pd
from databricks.sdk import WorkspaceClient

from .checkpoint import (
    BatchCommit,
    BatchOffsets,
    FileReader,
    SourceOffset,
    load_all_commits,
    load_all_offsets,
)
from .delta_history import describe_history, max_event_time_in_version_range


@dataclass
class SourceConfig:
    """User-supplied per-source mapping from checkpoint slot to a Delta table + event time column."""
    source_index: int
    table: str
    event_time_col: str
    label: str | None = None  # display name (defaults to table)


@dataclass
class SinkConfig:
    table: str
    label: str | None = None


def build_batches_frame(
    reader: FileReader,
    checkpoint_root: str,
    client: WorkspaceClient | None,
    warehouse_id: str | None,
    sources: list[SourceConfig],
    sink: SinkConfig | None,
    fetch_event_times: bool = True,
) -> tuple[pd.DataFrame, dict[int, pd.DataFrame]]:
    """Return (per-batch DataFrame, per-source DataFrame map).

    Per-batch columns:
      batch_id, batch_timestamp (datetime),
      pre_batch_watermark (datetime), post_batch_watermark (datetime),
      output_rows, output_commit_time, end_to_end_duration_s

    Per-source DataFrame columns:
      batch_id, reservoir_version, max_event_time (datetime|NaT)
    """
    offsets = load_all_offsets(reader, checkpoint_root)
    commits = load_all_commits(reader, checkpoint_root)

    base = _offsets_to_frame(offsets)
    base = base.merge(_commits_to_frame(commits), on="batch_id", how="left")

    # Output table history -> per-batch output rows + commit time
    if sink and client and warehouse_id:
        try:
            sink_hist = describe_history(client, warehouse_id, sink.table)
            # Coerce timestamp column to tz-aware UTC to align with batch_timestamp
            if not sink_hist.empty:
                sink_hist["timestamp"] = pd.to_datetime(
                    sink_hist["timestamp"], errors="coerce", utc=True
                )
            # Prefer batch_id from userMetadata; fall back to ordering by version
            sink_keyed = sink_hist.dropna(subset=["batch_id"])[
                ["batch_id", "num_output_rows", "timestamp"]
            ].rename(columns={"timestamp": "output_commit_time", "num_output_rows": "output_rows"})
            if sink_keyed.empty:
                # Fallback: align by ordering — earliest non-CREATE op = batch 0, etc.
                non_create = sink_hist[~sink_hist["operation"].str.upper().str.contains("CREATE")]
                non_create = non_create.reset_index(drop=True)
                non_create["batch_id"] = non_create.index.astype("Int64")
                sink_keyed = non_create[["batch_id", "num_output_rows", "timestamp"]].rename(
                    columns={"timestamp": "output_commit_time", "num_output_rows": "output_rows"}
                )
            base = base.merge(sink_keyed, on="batch_id", how="left")
        except Exception as e:
            base["output_rows"] = pd.NA
            base["output_commit_time"] = pd.Series(pd.NaT, index=base.index, dtype="datetime64[ns, UTC]")
            base.attrs["sink_error"] = str(e)
    else:
        base["output_rows"] = pd.NA
        base["output_commit_time"] = pd.Series(pd.NaT, index=base.index, dtype="datetime64[ns, UTC]")

    # Ensure output_commit_time is tz-aware UTC for arithmetic with batch_timestamp
    if "output_commit_time" in base.columns:
        oct_col = base["output_commit_time"]
        if oct_col.dtype == object or getattr(oct_col.dtype, "tz", None) is None:
            base["output_commit_time"] = pd.to_datetime(oct_col, errors="coerce", utc=True)

    base["end_to_end_duration_s"] = (
        (base["output_commit_time"] - base["batch_timestamp"]).dt.total_seconds()
    )

    # Per-source max(event_time) per batch
    per_source: dict[int, pd.DataFrame] = {}
    for s in sources:
        per_source[s.source_index] = _build_source_event_time_frame(
            offsets=offsets,
            source_cfg=s,
            client=client,
            warehouse_id=warehouse_id,
            fetch_event_times=fetch_event_times,
        )

    return base, per_source


def _offsets_to_frame(offsets: list[BatchOffsets]) -> pd.DataFrame:
    rows = []
    for b in offsets:
        rows.append(
            {
                "batch_id": b.batch_id,
                "batch_timestamp": pd.to_datetime(b.batch_timestamp_ms, unit="ms", utc=True),
                "pre_batch_watermark": pd.to_datetime(b.batch_watermark_ms, unit="ms", utc=True)
                if b.batch_watermark_ms
                else pd.NaT,
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("batch_id").reset_index(drop=True)
    return df


def _commits_to_frame(commits: list[BatchCommit]) -> pd.DataFrame:
    rows = []
    for c in commits:
        ts = (
            pd.to_datetime(c.next_batch_watermark_ms, unit="ms", utc=True)
            if c.next_batch_watermark_ms
            else pd.NaT
        )
        rows.append(
            {
                "batch_id": c.batch_id,
                "post_batch_watermark": ts,
                "state_unique_keys": c.state_unique_keys,
            }
        )
    return pd.DataFrame(rows)


def _build_source_event_time_frame(
    offsets: list[BatchOffsets],
    source_cfg: SourceConfig,
    client: WorkspaceClient | None,
    warehouse_id: str | None,
    fetch_event_times: bool,
) -> pd.DataFrame:
    rows = []
    prev_version: int | None = None
    for b in offsets:
        match: SourceOffset | None = next(
            (s for s in b.sources if s.source_index == source_cfg.source_index), None
        )
        version = match.reservoir_version if match else None
        rows.append(
            {
                "batch_id": b.batch_id,
                "batch_timestamp": pd.to_datetime(b.batch_timestamp_ms, unit="ms", utc=True),
                "reservoir_version": version,
                "prev_reservoir_version": prev_version,
            }
        )
        if version is not None:
            prev_version = version

    df = pd.DataFrame(rows)
    # Initialize as tz-aware UTC so subsequent .at[] assignments preserve tz.
    df["max_event_time"] = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns, UTC]")

    if df.empty or not fetch_event_times or not client or not warehouse_id:
        return df

    last_max: pd.Timestamp | None = None
    for i, row in df.iterrows():
        end_v = row["reservoir_version"]
        if pd.isna(end_v):
            df.at[i, "max_event_time"] = last_max
            continue
        try:
            mx = max_event_time_in_version_range(
                client=client,
                warehouse_id=warehouse_id,
                table=source_cfg.table,
                event_time_col=source_cfg.event_time_col,
                start_version=row["prev_reservoir_version"]
                if not pd.isna(row["prev_reservoir_version"])
                else None,
                end_version=int(end_v),
            )
        except Exception:
            mx = None
        if mx is not None:
            ts = pd.Timestamp(mx)
            last_max = ts if ts.tzinfo else ts.tz_localize("UTC")
        df.at[i, "max_event_time"] = last_max
    return df
