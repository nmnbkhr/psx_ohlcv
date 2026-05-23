"""Jobs Monitor — read-only viewer of the worker queue.

Lists rows from /v1/jobs with filters + a detail panel. Includes a
single submission button ("Submit Ping") to verify the full UI → API
→ worker → result loop. Auto-refreshes every 5s.

Real ETL job submission (Sync Indices, Rebuild Summary, etc.) becomes
available as those handlers get migrated to the worker in Milestone
1.5+.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None  # type: ignore[assignment]

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer

_STATUS_COLORS = {
    "pending":   "#FFB300",  # amber
    "running":   "#2F81F7",  # blue
    "ok":        "#00C853",  # green
    "failed":    "#FF5252",  # red
    "cancelled": "#6B7280",  # gray
}


def _status_badge(status: str) -> str:
    color = _STATUS_COLORS.get(status, "#9AA4B2")
    return (
        f'<span style="color:{color};font-weight:600;'
        f'font-family:ui-monospace,monospace;">{status}</span>'
    )


def render_jobs_monitor() -> None:
    """Render the Jobs Monitor page."""
    st.markdown("### Jobs Monitor")
    st.caption(
        "Read-only view of the `jobs` queue. The pakfindata-worker.service "
        "polls every 2s and dispatches handlers from the registry."
    )

    if not api_client.render_api_status_banner_if_down():
        render_footer()
        return

    if HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=5000, limit=None, key="jobs_monitor_refresh")

    # ──────────────────────────────────────────────────────────────
    # Filter bar
    # ──────────────────────────────────────────────────────────────
    c1, c2, c3, c4 = st.columns([2, 2, 1, 1])
    with c1:
        status_filter = st.selectbox(
            "Status",
            ["all", "pending", "running", "ok", "failed", "cancelled"],
            index=0,
        )
    with c2:
        type_filter = st.text_input("Job type filter", value="").strip()
    with c3:
        limit = int(st.number_input("Limit", min_value=10, max_value=500, value=50))
    with c4:
        if st.button("Submit Ping", type="primary", width="stretch",
                     help="Enqueue ping(sleep_seconds=2) — proves the loop"):
            resp = api_client.submit_job("ping", {"sleep_seconds": 2})
            if resp:
                st.toast(f"Submitted job #{resp['job_id']}")
                # Force a re-render with the freshest data.
                st.cache_data.clear()
                st.rerun()
            else:
                st.error("Failed to submit ping (see API logs).")

    # ──────────────────────────────────────────────────────────────
    # Listing
    # ──────────────────────────────────────────────────────────────
    rows = api_client.get_jobs(
        status=None if status_filter == "all" else status_filter,
        job_type=type_filter or None,
        limit=limit,
    )
    if rows is None:
        st.warning("Could not fetch jobs (API call failed).")
        render_footer()
        return
    if not rows:
        st.info("No jobs match the current filters.")
        render_footer()
        return

    df = pd.DataFrame(rows)
    # Project a tidy column set; keep param/result detail for the panel below.
    display_cols = [
        "id", "job_type", "status", "source",
        "enqueued_at", "started_at", "finished_at",
        "duration_ms", "worker_pid",
    ]
    display = df[[c for c in display_cols if c in df.columns]].copy()

    st.markdown(
        f"<div style='font-size:12px;color:#6B7280;'>"
        f"{len(display)} job(s) — newest first. Auto-refresh every 5s."
        f"</div>",
        unsafe_allow_html=True,
    )
    st.dataframe(
        display,
        width="stretch",
        hide_index=True,
        height=360,
        column_config={
            "id": st.column_config.NumberColumn("ID", format="%d", width="small"),
            "job_type": st.column_config.TextColumn("Type", width="small"),
            "status": st.column_config.TextColumn("Status", width="small"),
            "source": st.column_config.TextColumn("Source", width="small"),
            "enqueued_at": st.column_config.TextColumn("Enqueued"),
            "started_at": st.column_config.TextColumn("Started"),
            "finished_at": st.column_config.TextColumn("Finished"),
            "duration_ms": st.column_config.NumberColumn(
                "Duration (ms)", format="%d", width="small"
            ),
            "worker_pid": st.column_config.NumberColumn(
                "PID", format="%d", width="small"
            ),
        },
    )

    # ──────────────────────────────────────────────────────────────
    # Detail panel
    # ──────────────────────────────────────────────────────────────
    job_ids = df["id"].tolist()
    with st.expander("Job detail", expanded=False):
        selected = st.selectbox(
            "Inspect job_id", options=job_ids, index=0 if job_ids else None,
            key="jobs_monitor_select",
        )
        if selected is not None:
            row = df[df["id"] == selected].iloc[0].to_dict()
            badge = _status_badge(row.get("status", "?"))
            st.markdown(
                f"**Job #{selected}** — type `{row.get('job_type')}` "
                f"&nbsp;&nbsp; {badge}",
                unsafe_allow_html=True,
            )
            cdetail1, cdetail2 = st.columns(2)
            with cdetail1:
                st.markdown("**Params**")
                st.json(row.get("params") or {}, expanded=True)
                if row.get("notes"):
                    st.caption(f"Notes: {row['notes']}")
            with cdetail2:
                if row.get("status") == "ok":
                    st.markdown("**Result**")
                    st.json(row.get("result") or {}, expanded=True)
                elif row.get("status") == "failed":
                    st.markdown("**Error**")
                    st.code(row.get("error") or "(no message)")
                    if row.get("error_detail"):
                        with st.expander("Traceback", expanded=False):
                            st.code(row["error_detail"])
                elif row.get("status") in ("pending", "running"):
                    st.caption(
                        "Result will appear here once the job finishes."
                    )
                else:
                    st.caption("Job was cancelled before it ran.")

    render_footer()
