"""Query Delta history for batch correlation and per-batch row counts."""
from __future__ import annotations

import json

import pandas as pd
from databricks.sdk import WorkspaceClient

from .sql import execute_sql


def describe_history(client: WorkspaceClient, warehouse_id: str, table: str, limit: int = 1000) -> pd.DataFrame:
    """Run DESCRIBE HISTORY and parse the result into normalized columns.

    Returns a DataFrame with columns:
      version, timestamp (ms), operation, num_output_rows, batch_id (Optional[int]),
      operation_metrics (raw dict), user_metadata (raw str)
    """
    sql = f"DESCRIBE HISTORY {table} LIMIT {int(limit)}"
    raw = execute_sql(client, warehouse_id, sql)
    if raw.empty:
        return pd.DataFrame(
            columns=[
                "version", "timestamp", "operation", "num_output_rows",
                "batch_id", "operation_metrics", "user_metadata",
            ]
        )

    # operationMetrics may arrive as a JSON-string or a Python dict depending on driver.
    def _parse_dict_col(val):
        if isinstance(val, dict):
            return val
        if isinstance(val, str) and val:
            try:
                return json.loads(val)
            except Exception:
                pass
        return {}

    metrics = raw.get("operationMetrics", pd.Series([{}] * len(raw))).map(_parse_dict_col)
    user_meta = raw.get("userMetadata", pd.Series([""] * len(raw))).fillna("")

    def _num_rows(m: dict) -> int | None:
        for k in ("numOutputRows", "numTargetRowsInserted", "numCopiedRows"):
            if k in m:
                try:
                    return int(m[k])
                except (TypeError, ValueError):
                    return None
        return None

    def _batch_id(s: str) -> int | None:
        if not s:
            return None
        try:
            obj = json.loads(s)
        except Exception:
            return None
        if isinstance(obj, dict):
            for k in ("batchId", "batch_id", "streaming_batch_id"):
                if k in obj:
                    try:
                        return int(obj[k])
                    except (TypeError, ValueError):
                        return None
        return None

    out = pd.DataFrame(
        {
            "version": pd.to_numeric(raw["version"], errors="coerce").astype("Int64"),
            "timestamp": pd.to_datetime(raw["timestamp"], errors="coerce"),
            "operation": raw["operation"].astype(str),
            "num_output_rows": metrics.map(_num_rows).astype("Int64"),
            "batch_id": user_meta.map(_batch_id).astype("Int64"),
            "operation_metrics": metrics,
            "user_metadata": user_meta,
        }
    )
    return out.sort_values("version").reset_index(drop=True)


def max_event_time_in_version_range(
    client: WorkspaceClient,
    warehouse_id: str,
    table: str,
    event_time_col: str,
    start_version: int | None,
    end_version: int,
) -> pd.Timestamp | None:
    """MAX(event_time) among rows added between (start_version, end_version].

    Computed as the max in the snapshot at end_version that was *not* present at start_version.
    Returns None if either snapshot is empty or the column is missing.
    """
    if start_version is None or start_version < 0:
        sql = f"SELECT MAX({event_time_col}) AS et FROM {table} VERSION AS OF {int(end_version)}"
    else:
        sql = (
            f"WITH end_snap AS (SELECT {event_time_col} AS et "
            f"FROM {table} VERSION AS OF {int(end_version)}), "
            f"start_snap AS (SELECT MAX({event_time_col}) AS prev "
            f"FROM {table} VERSION AS OF {int(start_version)}) "
            f"SELECT MAX(et) AS et FROM end_snap, start_snap "
            f"WHERE et > start_snap.prev OR start_snap.prev IS NULL"
        )
    df = execute_sql(client, warehouse_id, sql)
    if df.empty or df["et"].isna().all():
        return None
    # Treat Delta TIMESTAMP results as UTC so they line up with checkpoint
    # watermark timestamps (which are unix-millis -> tz-aware UTC).
    return pd.to_datetime(df["et"].iloc[0], errors="coerce", utc=True)
