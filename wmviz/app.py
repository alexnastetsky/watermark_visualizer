"""wmviz — Spark Structured Streaming watermark visualizer (Streamlit)."""
from __future__ import annotations

import os
import traceback

import streamlit as st

from wmviz.batches import SinkConfig, SourceConfig, build_batches_frame
from wmviz.charts import duration_chart, rows_chart, watermark_chart
from wmviz.client import list_profiles, list_warehouses, make_client
from wmviz.discover import discover_sources
from wmviz.fs import is_databricks_path, make_reader

DEFAULT_CHECKPOINT = "/Volumes/alexn/default/v/checkpoints/wmdemo"
DEFAULT_OUTPUT_TABLE = "alexn.wmdemo.matched_clicks"
DEFAULT_INPUT_TABLES = ["alexn.wmdemo.impressions", "alexn.wmdemo.clicks"]
DEFAULT_EVENT_TIME_COLS = ["impression_time", "click_time"]

# When deployed on Databricks Apps, DATABRICKS_CLIENT_ID is auto-injected.
ON_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_CLIENT_ID"))

st.set_page_config(page_title="wmviz", layout="wide")
st.title("wmviz — Streaming watermark visualizer")


# ---------------------------------------------------------------------------
# Sidebar: connection + checkpoint configuration
# ---------------------------------------------------------------------------
with st.sidebar:
    if ON_DATABRICKS_APP:
        st.caption("Running on Databricks Apps — using service principal auth.")
        profile = None
    else:
        st.header("Workspace")
        profiles = list_profiles() or ["DEFAULT"]
        default_profile = os.environ.get("DATABRICKS_CONFIG_PROFILE", "DEFAULT")
        if default_profile not in profiles:
            profiles = [default_profile] + profiles
        profile = st.selectbox("Profile", profiles, index=profiles.index(default_profile))

    st.header("Checkpoint")
    checkpoint_root = st.text_input("Checkpoint dir", value=DEFAULT_CHECKPOINT)

    st.header("Compute")
    if ON_DATABRICKS_APP:
        warehouse_id_manual = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
        st.caption(f"SQL warehouse: `{warehouse_id_manual or '(not bound)'}`")
    else:
        warehouse_id_manual = st.text_input(
            "SQL warehouse ID (optional)",
            value=os.environ.get("DATABRICKS_WAREHOUSE_ID", ""),
            help="Leave blank to pick from a list once connected.",
        )

    fetch_event_times = st.checkbox(
        "Query input tables for max(event_time) per batch",
        value=True,
        help="Disable to skip Delta time-travel queries (faster but no event-time line on the chart).",
    )

    connect = st.button("Connect & load", type="primary")


# ---------------------------------------------------------------------------
# Connection state
# ---------------------------------------------------------------------------
@st.cache_resource(show_spinner=False)
def _get_client(profile_name: str | None):
    """Cached so we don't recreate the SDK client on every Streamlit rerun."""
    return make_client(profile=profile_name)


if "loaded" not in st.session_state:
    st.session_state["loaded"] = False

if connect:
    st.session_state["loaded"] = True
    st.session_state["profile"] = profile
    st.session_state["checkpoint"] = checkpoint_root
    st.session_state["warehouse_id_manual"] = warehouse_id_manual
    st.session_state["fetch_event_times"] = fetch_event_times


if not st.session_state.get("loaded"):
    st.info("Configure the sidebar and click **Connect & load** to begin.")
    st.stop()


checkpoint_root = st.session_state["checkpoint"]
profile = st.session_state["profile"]
fetch_event_times = st.session_state["fetch_event_times"]

client = None
warehouse_id = st.session_state.get("warehouse_id_manual") or None

needs_workspace = is_databricks_path(checkpoint_root)
if needs_workspace:
    try:
        client = _get_client(profile)
    except Exception as e:
        if profile:
            st.error(f"Could not initialize Databricks workspace client for profile `{profile}`: {e}")
        else:
            st.error(f"Could not initialize Databricks workspace client: {e}")
        st.stop()

    if not warehouse_id:
        with st.spinner("Listing SQL warehouses…"):
            try:
                whs = list_warehouses(client)
            except Exception as e:
                whs = []
                st.warning(f"Could not list warehouses: {e}")
        if whs:
            label_to_id = {f"{w.name} [{w.state}]": w.id for w in whs}
            picked = st.sidebar.selectbox("SQL warehouse", list(label_to_id.keys()))
            warehouse_id = label_to_id[picked]
        else:
            st.warning(
                "No SQL warehouses visible to this profile. Delta history & event-time queries will be skipped."
            )


# ---------------------------------------------------------------------------
# Discover sources
# ---------------------------------------------------------------------------
try:
    reader = make_reader(client, checkpoint_root)
except Exception as e:
    st.error(str(e))
    st.stop()

with st.spinner(f"Reading checkpoint {checkpoint_root}…"):
    try:
        sources_disc = discover_sources(reader, checkpoint_root)
    except Exception as e:
        st.error(f"Failed to read checkpoint: {e}")
        st.code(traceback.format_exc())
        st.stop()

st.subheader("Discovered sources")
if not sources_disc:
    st.write("(none detected)")
for s in sources_disc:
    st.write(f"`source[{s.source_index}]` — *{s.kind}* — {s.hint}")


# ---------------------------------------------------------------------------
# User-supplied table mappings (since reservoirId -> UC table is not in checkpoint)
# ---------------------------------------------------------------------------
st.subheader("Configure source/sink tables")

source_configs: list[SourceConfig] = []
if sources_disc:
    for i, s in enumerate(sources_disc):
        cols = st.columns([2, 2, 2])
        default_table = (
            DEFAULT_INPUT_TABLES[i] if i < len(DEFAULT_INPUT_TABLES) else ""
        )
        default_etc = (
            DEFAULT_EVENT_TIME_COLS[i] if i < len(DEFAULT_EVENT_TIME_COLS) else "event_time"
        )
        with cols[0]:
            tbl = st.text_input(
                f"source[{s.source_index}] table",
                value=default_table,
                key=f"src_table_{s.source_index}",
            )
        with cols[1]:
            etc = st.text_input(
                f"source[{s.source_index}] event-time column",
                value=default_etc,
                key=f"src_etc_{s.source_index}",
            )
        with cols[2]:
            label = st.text_input(
                f"source[{s.source_index}] label",
                value=tbl.split(".")[-1] or f"source[{s.source_index}]",
                key=f"src_label_{s.source_index}",
            )
        if tbl and etc:
            source_configs.append(
                SourceConfig(source_index=s.source_index, table=tbl, event_time_col=etc, label=label)
            )

sink_table = st.text_input("Output (sink) table", value=DEFAULT_OUTPUT_TABLE)
sink_cfg = SinkConfig(table=sink_table) if sink_table else None


# ---------------------------------------------------------------------------
# Build batches frame
# ---------------------------------------------------------------------------
with st.spinner("Building per-batch frame…"):
    try:
        base, per_source = build_batches_frame(
            reader=reader,
            checkpoint_root=checkpoint_root,
            client=client,
            warehouse_id=warehouse_id,
            sources=source_configs,
            sink=sink_cfg,
            fetch_event_times=fetch_event_times and warehouse_id is not None,
        )
    except Exception as e:
        st.error(f"Failed to build batch frame: {e}")
        st.code(traceback.format_exc())
        st.stop()

if base.empty:
    st.warning(
        "No batches found in the checkpoint. The query may not have committed any batches yet, "
        "or the checkpoint path is wrong."
    )
    st.stop()


# ---------------------------------------------------------------------------
# Charts
# ---------------------------------------------------------------------------
st.subheader("Watermark vs event-time")
labels = {s.source_index: (s.label or s.table) for s in source_configs}
st.plotly_chart(watermark_chart(base, per_source, labels), use_container_width=True)

st.subheader("Per-batch output rows")
st.plotly_chart(rows_chart(base), use_container_width=True)

st.subheader("Approximate batch duration")
st.plotly_chart(duration_chart(base), use_container_width=True)


# ---------------------------------------------------------------------------
# Raw data inspector
# ---------------------------------------------------------------------------
with st.expander("Per-batch DataFrame"):
    st.dataframe(base, use_container_width=True)

for idx, df in per_source.items():
    label = labels.get(idx, f"source[{idx}]")
    with st.expander(f"Per-batch event-time samples — {label}"):
        st.dataframe(df, use_container_width=True)

if base.attrs.get("sink_error"):
    st.warning(f"Output table history error: {base.attrs['sink_error']}")
