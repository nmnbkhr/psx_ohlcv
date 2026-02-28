"""Benchmark Monitor — SBP benchmark rate history and trends."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


def render_benchmark_monitor():
    """Render the Benchmark Monitor page — SBP benchmark history."""
    st.markdown("## Benchmark Monitor")
    st.caption("SBP benchmark rate snapshot history and trends")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    from pakfindata.db.repositories.bond_market import (
        init_bond_market_schema,
        get_benchmark_snapshot,
        get_bond_market_status,
    )
    init_bond_market_schema(con)

    # ── Latest Snapshot ──────────────────────────────────────────
    snap = get_benchmark_snapshot(con)
    if not snap:
        st.info("No benchmark data. Click sync below to populate.")
    else:
        snap_date = snap.pop("_date", None)
        if snap_date:
            st.caption(f"Latest snapshot: **{snap_date}**")

        # Key rates summary
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Policy Rate", f"{snap.get('policy_rate', 'N/A')}%")
        c2.metric("KIBOR 3M", f"{snap.get('kibor_3m_offer', 'N/A')}%")
        c3.metric("KIBOR 6M", f"{snap.get('kibor_6m_offer', 'N/A')}%")
        c4.metric("MTB 3M", f"{snap.get('mtb_3m', 'N/A')}%")
        c5.metric("PIB 10Y", f"{snap.get('pib_10y', 'N/A')}%")

    st.divider()

    # ── Historical Trend Chart ───────────────────────────────────
    _render_benchmark_history(con)

    st.divider()

    # ── Data Status ──────────────────────────────────────────────
    status = get_bond_market_status(con)
    st.subheader("Data Coverage")
    c1, c2, c3 = st.columns(3)
    c1.metric("Benchmark Days", status.get("benchmark_days", 0))
    c2.metric("Date Range",
              f"{status.get('benchmark_earliest', '?')} → {status.get('benchmark_latest', '?')}")
    c3.metric("Trading Days", status.get("trading_days", 0))

    # ── Sync Controls ────────────────────────────────────────────
    st.divider()
    with st.expander("Sync Benchmark Data"):
        if st.button("Scrape Latest Benchmark", key="bm_sync"):
            with st.spinner("Fetching from SBP..."):
                try:
                    from pakfindata.sources.sbp_bond_market import SBPBondMarketScraper
                    result = SBPBondMarketScraper().sync_benchmark(con)
                    if result["status"] == "ok":
                        st.success(f"Stored {result['metrics_stored']} metrics for {result['date']}")
                    else:
                        st.error(result.get("error", "Unknown error"))
                except Exception as e:
                    st.error(f"Failed: {e}")

    render_footer()


def _render_benchmark_history(con):
    """Historical trend for selected benchmark metrics."""
    st.subheader("Benchmark History")

    metric_options = {
        "Policy Rate": "policy_rate",
        "Overnight Repo": "overnight_repo",
        "KIBOR 1W Offer": "kibor_1w_offer",
        "KIBOR 1M Offer": "kibor_1m_offer",
        "KIBOR 3M Offer": "kibor_3m_offer",
        "KIBOR 6M Offer": "kibor_6m_offer",
        "KIBOR 9M Offer": "kibor_9m_offer",
        "KIBOR 12M Offer": "kibor_12m_offer",
        "MTB 3M": "mtb_3m",
        "MTB 6M": "mtb_6m",
        "MTB 12M": "mtb_12m",
        "PIB 2Y": "pib_2y",
        "PIB 3Y": "pib_3y",
        "PIB 5Y": "pib_5y",
        "PIB 10Y": "pib_10y",
        "PIB 15Y": "pib_15y",
        "SBP Reserves": "sbp_reserves_m_usd",
    }

    selected = st.multiselect(
        "Select metrics to chart",
        options=list(metric_options.keys()),
        default=["Policy Rate", "KIBOR 6M Offer", "PIB 10Y"],
        key="bm_hist_metrics",
    )

    if not selected:
        return

    fig = go.Figure()
    for label in selected:
        metric = metric_options[label]
        try:
            df = pd.read_sql_query(
                "SELECT date, value FROM sbp_benchmark_snapshot "
                "WHERE metric = ? ORDER BY date",
                con, params=(metric,),
            )
            if not df.empty:
                fig.add_trace(go.Scatter(
                    x=df["date"], y=df["value"],
                    mode="lines", name=label,
                    line=dict(width=2),
                ))
        except Exception:
            pass

    if fig.data:
        fig.update_layout(
            height=450,
            xaxis_title="Date",
            yaxis_title="Value",
            legend=dict(orientation="h", y=-0.2),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No historical data. Sync daily to build history.")
