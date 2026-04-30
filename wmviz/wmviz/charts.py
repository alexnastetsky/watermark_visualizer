"""Plotly chart builders."""
from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go


def watermark_chart(
    base: pd.DataFrame,
    per_source: dict[int, pd.DataFrame],
    source_labels: dict[int, str],
) -> go.Figure:
    """X = wall-clock batch trigger time. Y = event-time.

    Lines per source: max event-time observed.
    Bold line: global watermark (post-batch).
    Optional line: pre-batch watermark.
    """
    fig = go.Figure()

    palette = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd"]
    for i, (idx, df) in enumerate(per_source.items()):
        if df.empty or df["max_event_time"].isna().all():
            continue
        color = palette[i % len(palette)]
        fig.add_trace(
            go.Scatter(
                x=df["batch_timestamp"],
                y=df["max_event_time"],
                mode="lines+markers",
                name=f"{source_labels.get(idx, f'source[{idx}]')} max(event_time)",
                line=dict(color=color, width=2),
                hovertemplate="batch=%{customdata}<br>wall=%{x}<br>event=%{y}<extra></extra>",
                customdata=df["batch_id"],
            )
        )

    if "post_batch_watermark" in base.columns and base["post_batch_watermark"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=base["batch_timestamp"],
                y=base["post_batch_watermark"],
                mode="lines",
                name="global watermark (post-batch)",
                line=dict(color="black", width=3, dash="solid"),
                hovertemplate="batch=%{customdata}<br>wall=%{x}<br>watermark=%{y}<extra></extra>",
                customdata=base["batch_id"],
            )
        )

    if "pre_batch_watermark" in base.columns and base["pre_batch_watermark"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=base["batch_timestamp"],
                y=base["pre_batch_watermark"],
                mode="lines",
                name="global watermark (pre-batch)",
                line=dict(color="grey", width=1, dash="dot"),
                hovertemplate="batch=%{customdata}<br>wall=%{x}<br>watermark=%{y}<extra></extra>",
                customdata=base["batch_id"],
            )
        )

    fig.update_layout(
        title="Watermark vs event-time over wall-clock",
        xaxis_title="Batch trigger time (wall-clock)",
        yaxis_title="Event time",
        hovermode="closest",
        height=520,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="left", x=0),
    )
    return fig


def rows_chart(base: pd.DataFrame) -> go.Figure:
    """Output rows per batch as bars. Input rows added later when source histories are merged in."""
    fig = go.Figure()
    if "output_rows" in base.columns and base["output_rows"].notna().any():
        fig.add_trace(
            go.Bar(
                x=base["batch_id"],
                y=base["output_rows"].fillna(0),
                name="output rows",
                marker_color="#2ca02c",
            )
        )
    fig.update_layout(
        title="Per-batch output rows",
        xaxis_title="batch_id",
        yaxis_title="rows",
        height=380,
    )
    return fig


def duration_chart(base: pd.DataFrame) -> go.Figure:
    """End-to-end duration approximation per batch (output commit - batch trigger)."""
    fig = go.Figure()
    if "end_to_end_duration_s" in base.columns and base["end_to_end_duration_s"].notna().any():
        fig.add_trace(
            go.Scatter(
                x=base["batch_id"],
                y=base["end_to_end_duration_s"],
                mode="lines+markers",
                name="end-to-end duration (s)",
                line=dict(color="#9467bd", width=2),
            )
        )
    fig.update_layout(
        title="Approximate batch duration (end-to-end: output_commit_time − batch_trigger_time)",
        xaxis_title="batch_id",
        yaxis_title="seconds",
        height=380,
    )
    return fig
