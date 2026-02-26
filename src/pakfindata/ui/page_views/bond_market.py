"""OTC Bond Market page — SBP benchmark rates + trading volume dashboard."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


def render_bond_market():
    """Render the OTC Bond Market dashboard."""
    st.header("OTC Bond Market")
    st.caption(
        "SBP benchmark rates, yield curve, and secondary market data. "
        "Pakistan's bond market is 99% OTC interbank — data from SBP, not PSX."
    )

    con = get_connection()

    # Sync button
    with st.expander("Sync from SBP", expanded=False):
        if st.button("Scrape Benchmark Snapshot"):
            with st.spinner("Fetching from SBP MSM page..."):
                from pakfindata.sources.sbp_bond_market import SBPBondMarketScraper
                from pakfindata.db.repositories.bond_market import init_bond_market_schema
                init_bond_market_schema(con)
                scraper = SBPBondMarketScraper()
                result = scraper.sync_benchmark(con)
                if result["status"] == "ok":
                    st.success(
                        f"Stored {result['metrics_stored']} metrics for {result['date']}"
                    )
                else:
                    st.error(result.get("error", "Unknown error"))

    # Ensure schema exists
    from pakfindata.db.repositories.bond_market import (
        init_bond_market_schema,
        get_benchmark_snapshot,
        get_benchmark_history,
        get_bond_market_status,
    )
    init_bond_market_schema(con)

    snap = get_benchmark_snapshot(con)
    if not snap:
        st.info(
            "No benchmark data yet. Click **Scrape Benchmark Snapshot** above."
        )
        render_footer()
        return

    snap_date = snap.pop("_date", None)
    if snap_date:
        st.caption(f"Data as of: **{snap_date}**")

    # Section 1: Policy Rate Corridor
    _render_policy_rates(snap)

    st.markdown("---")

    # Section 2: KIBOR Panel
    _render_kibor_panel(snap)

    st.markdown("---")

    # Section 3: Yield Curve (MTB + PIB)
    _render_yield_curve(snap, con)

    st.markdown("---")

    # Section 4: FX Reserves + Rate
    _render_reserves(snap)

    st.markdown("---")

    # Section 5: Benchmark History
    _render_benchmark_history(con)

    # Section 6: Bond Trading Data (when SMTV becomes available)
    status = get_bond_market_status(con)
    if status.get("trading_rows", 0) > 0:
        st.markdown("---")
        _render_trading_volume(con, status)

    render_footer()


def _render_policy_rates(snap: dict):
    """Policy rate corridor display."""
    st.subheader("SBP Policy Rate Corridor")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Policy Rate", f"{snap.get('policy_rate', 'N/A')}%")
    c2.metric("Ceiling (Rev. Repo)", f"{snap.get('ceiling_rate', 'N/A')}%")
    c3.metric("Floor (Repo)", f"{snap.get('floor_rate', 'N/A')}%")
    c4.metric("Overnight WA Repo", f"{snap.get('overnight_repo', 'N/A')}%")


def _render_kibor_panel(snap: dict):
    """KIBOR bid/offer panel."""
    st.subheader("KIBOR Rates")

    tenors = ["3m", "6m", "12m"]
    cols = st.columns(len(tenors))
    for i, t in enumerate(tenors):
        bid = snap.get(f"kibor_{t}_bid")
        offer = snap.get(f"kibor_{t}_offer")
        with cols[i]:
            st.metric(
                f"KIBOR {t.upper()}",
                f"{offer:.2f}%" if offer else "N/A",
                delta=f"Bid: {bid:.2f}%" if bid else None,
                delta_color="off",
            )


def _render_yield_curve(snap: dict, con):
    """Yield curve from MTB + PIB cutoff yields."""
    st.subheader("Yield Curve (MTB + PIB Cutoffs)")

    # Build curve data from snapshot
    curve_points = []
    mtb_tenors = [("1m", 1, "MTB"), ("3m", 3, "MTB"), ("6m", 6, "MTB"), ("12m", 12, "MTB")]
    pib_tenors = [("2y", 24, "PIB"), ("3y", 36, "PIB"), ("5y", 60, "PIB"),
                  ("10y", 120, "PIB"), ("15y", 180, "PIB")]

    for key, months, sec_type in mtb_tenors + pib_tenors:
        val = snap.get(f"{'mtb' if sec_type == 'MTB' else 'pib'}_{key}")
        if val is not None:
            curve_points.append({
                "tenor_months": months,
                "yield_pct": val,
                "security": sec_type,
                "label": key.upper(),
            })

    if not curve_points:
        st.info("No yield curve data available.")
        return

    df = pd.DataFrame(curve_points)

    # Plotly chart
    fig = go.Figure()
    for sec_type, color in [("MTB", "#1f77b4"), ("PIB", "#ff7f0e")]:
        mask = df["security"] == sec_type
        sub = df[mask]
        if not sub.empty:
            fig.add_trace(go.Scatter(
                x=sub["tenor_months"],
                y=sub["yield_pct"],
                mode="lines+markers+text",
                name=sec_type,
                text=sub["label"],
                textposition="top center",
                line=dict(color=color, width=2),
                marker=dict(size=8),
            ))

    # Add PKRV overlay if available
    try:
        pkrv_df = pd.read_sql_query(
            """SELECT tenor_months, yield_pct FROM pkrv_daily
               WHERE date = (SELECT MAX(date) FROM pkrv_daily)
               ORDER BY tenor_months""",
            con,
        )
        if not pkrv_df.empty:
            fig.add_trace(go.Scatter(
                x=pkrv_df["tenor_months"],
                y=pkrv_df["yield_pct"],
                mode="lines",
                name="PKRV Curve",
                line=dict(color="#2ca02c", width=2, dash="dot"),
            ))
    except Exception:
        pass

    fig.update_layout(
        title="Pakistan Sovereign Yield Curve",
        xaxis_title="Tenor (months)",
        yaxis_title="Yield (%)",
        height=450,
        showlegend=True,
        xaxis=dict(
            tickvals=[1, 3, 6, 12, 24, 36, 60, 120, 180],
            ticktext=["1M", "3M", "6M", "1Y", "2Y", "3Y", "5Y", "10Y", "15Y"],
        ),
    )
    st.plotly_chart(fig, use_container_width=True)

    # Show data table
    with st.expander("View Data Table"):
        st.dataframe(df[["label", "security", "yield_pct"]].rename(columns={
            "label": "Tenor", "security": "Type", "yield_pct": "Yield %"
        }), use_container_width=True)


def _render_reserves(snap: dict):
    """FX reserves and rate display."""
    st.subheader("FX Reserves & Rate")

    c1, c2, c3, c4, c5 = st.columns(5)
    sbp_res = snap.get("sbp_reserves_m_usd")
    bank_res = snap.get("bank_reserves_m_usd")
    total_res = snap.get("total_reserves_m_usd")
    m2m = snap.get("fx_m2m_rate")
    wa_bid = snap.get("fx_wa_bid")

    c1.metric("SBP Reserves", f"${sbp_res:,.0f}M" if sbp_res else "N/A")
    c2.metric("Bank Reserves", f"${bank_res:,.0f}M" if bank_res else "N/A")
    c3.metric("Total Reserves", f"${total_res:,.0f}M" if total_res else "N/A")
    c4.metric("M2M Rate", f"PKR {m2m:,.4f}" if m2m else "N/A")
    c5.metric("WA Bid", f"PKR {wa_bid:,.4f}" if wa_bid else "N/A")


def _render_benchmark_history(con):
    """Historical trend for selected benchmark metrics."""
    st.subheader("Benchmark History")

    metric_options = {
        "Policy Rate": "policy_rate",
        "Overnight Repo": "overnight_repo",
        "KIBOR 3M Offer": "kibor_3m_offer",
        "KIBOR 6M Offer": "kibor_6m_offer",
        "KIBOR 12M Offer": "kibor_12m_offer",
        "MTB 3M": "mtb_3m",
        "MTB 6M": "mtb_6m",
        "MTB 12M": "mtb_12m",
        "PIB 5Y": "pib_5y",
        "PIB 10Y": "pib_10y",
        "PIB 15Y": "pib_15y",
        "SBP Reserves": "sbp_reserves_m_usd",
        "FX M2M Rate": "fx_m2m_rate",
    }

    selected = st.multiselect(
        "Select metrics to chart",
        options=list(metric_options.keys()),
        default=["Policy Rate", "KIBOR 6M Offer", "PIB 10Y"],
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
                    mode="lines+markers", name=label,
                ))
        except Exception:
            pass

    if fig.data:
        fig.update_layout(
            height=400,
            xaxis_title="Date",
            yaxis_title="Value",
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No historical data available yet. Sync daily to build history.")


def _render_trading_volume(con, status: dict):
    """OTC bond trading volume (when SMTV data is available)."""
    st.subheader("OTC Bond Trading Volume")
    st.caption(
        f"Daily SMTV data: {status.get('trading_days', 0)} days "
        f"({status.get('trading_earliest', '?')} to {status.get('trading_latest', '?')})"
    )

    try:
        df = pd.read_sql_query(
            """SELECT date, security_type, segment,
                      SUM(face_amount) as face_m,
                      AVG(yield_weighted_avg) as avg_yield
               FROM sbp_bond_trading_daily
               WHERE date = (SELECT MAX(date) FROM sbp_bond_trading_daily)
               GROUP BY security_type, segment""",
            con,
        )
        if not df.empty:
            st.dataframe(df, use_container_width=True)
    except Exception:
        st.info("No trading volume data available.")
