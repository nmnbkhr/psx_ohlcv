"""NCCPL Flow Intelligence — sync, analytics, and investor flow dashboard."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from plotly.subplots import make_subplots

from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3", "gold": "#C8A96E",
}
_CHART = dict(
    paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], font_color=_C["text"],
    margin=dict(t=30, b=20, l=50, r=20),
)

_FLOW_REGIME_COLORS = {
    "BULLISH": "#22C55E", "BEARISH": "#EF4444",
    "DIVERGENT": "#F97316", "NEUTRAL": "#6B7280",
}


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.markdown("### NCCPL Flow Intelligence")
    st.caption("Foreign & local investor flows — FIPI/LIPI data from NCCPL via KhiStocks")

    api_client.render_api_status_banner_if_down()

    tab_dash, tab_sector, tab_sync = st.tabs(
        ["Flow Dashboard", "Sector Flows", "Sync & Backfill"]
    )

    with tab_dash:
        _render_dashboard()
    with tab_sector:
        _render_sector()
    with tab_sync:
        _render_sync()

    render_footer()


# ═══════════════════════════════════════════════════════
# TAB 1: Flow Dashboard
# ═══════════════════════════════════════════════════════


def _render_dashboard():
    coverage = api_client.get_nccpl_coverage() or {}
    if coverage.get("fipi_count", 0) == 0:
        st.warning("No NCCPL flow data. Go to **Sync & Backfill** tab to fetch data.")
        return

    derived_rows = api_client.get_nccpl_flows_derived(limit=10000) or []
    if not derived_rows:
        st.warning("Derived signals not computed. Run backfill first.")
        return

    derived = pd.DataFrame(derived_rows)
    latest = derived.iloc[-1]

    # ── Current regime banner ──
    regime = latest.get("flow_regime_signal", "NEUTRAL")
    rc = _FLOW_REGIME_COLORS.get(regime, _C["dim"])
    st.markdown(f"""
    <div style="background:{_C['card']};padding:20px;border-radius:12px;
                border-left:6px solid {rc};margin-bottom:16px;">
        <div style="color:{_C['dim']};font-size:0.8em;text-transform:uppercase;">
            Flow Regime Signal</div>
        <div style="color:{rc};font-size:2.2em;font-weight:700;display:inline-block;">
            {regime}</div>
        <div style="color:{_C['dim']};font-size:0.85em;margin-top:4px;">
            Date: {latest['date']} | Smart/Dumb Ratio: {latest.get('smart_dumb_ratio', 0):.2f}
        </div>
    </div>
    """, unsafe_allow_html=True)

    # ── 4-week rolling KPIs ──
    fpi_4w = latest.get("fpi_net_4w", 0) or 0
    mf_4w = latest.get("mf_net_4w", 0) or 0
    retail_4w = latest.get("retail_net_4w", 0) or 0
    bank_4w = latest.get("bank_net_4w", 0) or 0
    sdr = latest.get("smart_dumb_ratio", 0) or 0
    ic = latest.get("institutional_consensus", 0)
    fdd = latest.get("foreign_domestic_divergence", 0)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi("Foreign (FPI) 4W", _fmt_mn(fpi_4w), _C["up"] if fpi_4w > 0 else _C["down"])
    with c2:
        _kpi("Mutual Funds 4W", _fmt_mn(mf_4w), _C["up"] if mf_4w > 0 else _C["down"])
    with c3:
        warn = retail_4w > 0 and fpi_4w < 0
        _kpi("Retail 4W", _fmt_mn(retail_4w), _C["amber"] if warn else _C["text"])
    with c4:
        _kpi("Banks 4W", _fmt_mn(bank_4w), _C["up"] if bank_4w > 0 else _C["down"])

    st.markdown("")

    c5, c6, c7 = st.columns(3)
    with c5:
        r_color = _C["up"] if sdr > 0.5 else _C["down"] if sdr < -0.5 else _C["amber"]
        _kpi("Smart/Dumb Ratio", f"{sdr:.2f}", r_color)
    with c6:
        _kpi("Institutional Consensus", "YES" if ic else "NO",
             _C["up"] if ic else _C["down"])
    with c7:
        _kpi("FPI-Retail Divergence", "YES" if fdd else "NO",
             _C["amber"] if fdd else _C["dim"])

    # ── Time series charts ──
    st.markdown("#### Flow Regime History")

    # Regime timeline
    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.3, 0.4, 0.3], vertical_spacing=0.05,
        subplot_titles=["Flow Regime", "Smart/Dumb Ratio", "4W Rolling Flows"],
    )

    # Row 1: regime scatter
    for regime_name, color in _FLOW_REGIME_COLORS.items():
        mask = derived["flow_regime_signal"] == regime_name
        if mask.any():
            fig.add_trace(go.Scatter(
                x=derived.loc[mask, "date"], y=[regime_name] * mask.sum(),
                mode="markers", marker=dict(color=color, size=8),
                name=regime_name, showlegend=True,
            ), row=1, col=1)

    # Row 2: smart/dumb ratio
    fig.add_trace(go.Scatter(
        x=derived["date"], y=derived["smart_dumb_ratio"],
        line=dict(color=_C["gold"], width=2), name="S/D Ratio", showlegend=False,
    ), row=2, col=1)
    fig.add_hline(y=1.5, line_dash="dash", line_color=_C["up"], row=2, col=1)
    fig.add_hline(y=-1.5, line_dash="dash", line_color=_C["down"], row=2, col=1)
    fig.add_hline(y=0, line_dash="dot", line_color=_C["dim"], row=2, col=1)

    # Row 3: rolling flows
    fig.add_trace(go.Scatter(
        x=derived["date"], y=derived["fpi_net_4w"],
        line=dict(color=_C["cyan"], width=1.5), name="FPI 4W",
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=derived["date"], y=derived["mf_net_4w"],
        line=dict(color=_C["accent"], width=1.5), name="MF 4W",
    ), row=3, col=1)
    fig.add_trace(go.Scatter(
        x=derived["date"], y=derived["retail_net_4w"],
        line=dict(color=_C["amber"], width=1.5), name="Retail 4W",
    ), row=3, col=1)

    fig.update_layout(
        **_CHART, height=600,
        legend=dict(orientation="h", y=1.06, bgcolor="rgba(0,0,0,0)"),
    )
    for i in range(1, 4):
        fig.update_yaxes(gridcolor=_C["grid"], row=i, col=1)
        fig.update_xaxes(gridcolor=_C["grid"], row=i, col=1)

    st.plotly_chart(fig, width='stretch')

    # ── Daily net flows table ──
    with st.expander("Daily Net Flows (last 20 days)", expanded=False):
        fipi_rows = api_client.get_nccpl_fipi(limit=20) or []
        lipi_rows = api_client.get_nccpl_lipi(limit=20) or []
        if fipi_rows and lipi_rows:
            fipi = pd.DataFrame(fipi_rows)[["date", "fpi_net"]]
            lipi = pd.DataFrame(lipi_rows)[[
                "date", "mf_net", "insurance_net", "bank_net", "retail_net",
                "corporate_net", "broker_net",
            ]]
            merged = fipi.merge(lipi, on="date", how="inner")
            st.dataframe(merged, width='stretch', hide_index=True)


# ═══════════════════════════════════════════════════════
# TAB 2: Sector Flows
# ═══════════════════════════════════════════════════════


def _render_sector():
    coverage = api_client.get_nccpl_coverage() or {}
    if coverage.get("sector_count", 0) == 0:
        st.warning("No sector flow data. Run backfill first.")
        return

    dates = api_client.get_nccpl_sector_dates(limit=60) or []
    if not dates:
        return

    selected_date = st.selectbox("Date", dates, index=0)

    sector_rows = api_client.get_nccpl_sector(selected_date) or []
    sector_df = pd.DataFrame(sector_rows)
    if sector_df.empty:
        st.info(f"No sector data for {selected_date}")
        return

    # Sector bar chart
    fig = px.bar(
        sector_df, x="sector", y="fpi_net",
        color="fpi_net", color_continuous_scale="RdYlGn",
        title=f"Foreign Flow by Sector — {selected_date}",
    )
    fig.update_layout(
        **_CHART, height=400,
        xaxis=dict(tickangle=45, gridcolor=_C["grid"]),
        yaxis=dict(gridcolor=_C["grid"], title="Net Flow (PKR)"),
        coloraxis_colorbar=dict(title="Net"),
    )
    st.plotly_chart(fig, width='stretch')

    # Sector table
    st.dataframe(
        sector_df.style.format({
            "fpi_buy": "{:,.0f}", "fpi_sell": "{:,.0f}", "fpi_net": "{:,.0f}",
        }),
        width='stretch', hide_index=True,
    )

    # Sector flow heatmap over time
    st.markdown("#### Sector Flow Heatmap (Last 20 Days)")
    heatmap_rows = api_client.get_nccpl_sector_heatmap(days=20) or []
    heatmap_df = pd.DataFrame(heatmap_rows)
    if not heatmap_df.empty:
        pivot = heatmap_df.pivot_table(index="sector", columns="date", values="fpi_net", fill_value=0)
        fig2 = go.Figure(go.Heatmap(
            z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale=[[0, _C["down"]], [0.5, _C["bg"]], [1, _C["up"]]],
            text=np.where(pivot.values != 0, (pivot.values / 1e6).round(1).astype(str) + "M", ""),
            texttemplate="%{text}",
            textfont=dict(size=9),
        ))
        fig2.update_layout(**_CHART, height=400, yaxis=dict(autorange="reversed"))
        st.plotly_chart(fig2, width='stretch')


# ═══════════════════════════════════════════════════════
# TAB 3: Sync & Backfill
# ═══════════════════════════════════════════════════════


def _render_sync():
    coverage = api_client.get_nccpl_coverage() or {}

    # ── Current data status ──
    st.markdown("#### Data Coverage")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi("FIPI Days", str(coverage.get("fipi_count", 0)), _C["cyan"])
    with c2:
        _kpi("LIPI Days", str(coverage.get("lipi_count", 0)), _C["accent"])
    with c3:
        _kpi("Sector Rows", f"{coverage.get('sector_count', 0):,}", _C["gold"])
    with c4:
        _kpi("Derived Days", str(coverage.get("derived_count", 0)), _C["up"])

    date_min, date_max = coverage.get("date_min"), coverage.get("date_max")
    if date_min and date_max:
        st.caption(f"Date range: **{date_min}** to **{date_max}**")

    st.markdown("---")

    # ── Fetch today ──
    st.markdown("#### Fetch Today's Data")
    st.caption("Uses BRecorder (Tier 2a) — mirrors NCCPL daily data")

    if st.button("Fetch Today", type="primary", key="nccpl_fetch_today"):
        today = datetime.now().strftime("%Y-%m-%d")
        # Sync paths still open their own write connection — engine domain.
        from pakfindata.db.connection import connect
        from pakfindata.db.repositories.nccpl_flows import date_already_fetched
        con = connect()
        if date_already_fetched(con, today):
            st.info(f"{today} already in database")
        else:
            with st.spinner(f"Fetching {today} from BRecorder..."):
                from pakfindata.sources.nccpl_flows import fetch_with_fallback, compute_derived_signals
                result = fetch_with_fallback(today, con)
                compute_derived_signals(con)

            if result.get("source"):
                st.success(f"Fetched from **{result['source']}** (Tier {result['tier']})")
                st.cache_data.clear()
            else:
                st.error("All sources failed")

    st.markdown("---")

    # ── Historical backfill ──
    st.markdown("#### Historical Backfill")
    st.caption("Uses KhiStocks JSON API — data available back to 2020")

    bc1, bc2 = st.columns(2)
    with bc1:
        from_date = st.date_input("From", value=datetime(2025, 1, 1), key="nccpl_from")
    with bc2:
        to_date = st.date_input("To", value=datetime.now(), key="nccpl_to")

    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")

    bc3, bc4 = st.columns(2)

    with bc3:
        if st.button("Discover (Dry Run)", key="nccpl_discover"):
            with st.spinner("Discovering available data..."):
                from pakfindata.sources.nccpl_backfill import discover_khistocks
                info = discover_khistocks(from_str, to_str)

            st.markdown(f"""
            | Metric | Value |
            |---|---|
            | FIPI records | {info['fipi_records']:,} |
            | LIPI records | {info['lipi_records']:,} |
            | Trading days | {info['fipi_dates']} |
            | Date range | {info['date_range']} |
            """)

    with bc4:
        if st.button("Run Backfill", type="primary", key="nccpl_backfill"):
            with st.spinner(f"Backfilling {from_str} to {to_str}..."):
                from pakfindata.sources.nccpl_backfill import backfill_from_khistocks
                result = backfill_from_khistocks(from_str, to_str)

            st.success(
                f"Backfill complete: **{result['stored']}** new dates stored, "
                f"{result['skipped_dates']} skipped"
            )
            st.cache_data.clear()
            st.markdown(f"""
            | Metric | Value |
            |---|---|
            | FIPI raw rows | {result['fipi_raw_rows']:,} |
            | LIPI raw rows | {result['lipi_raw_rows']:,} |
            | Date range | {result['date_range']} |
            | New dates stored | {result['stored']} |
            | Skipped (existing) | {result['skipped_dates']} |
            """)

    st.markdown("---")

    # ── Recompute derived ──
    st.markdown("#### Recompute Derived Signals")
    st.caption("Recalculates smart/dumb ratio, flow regime, etc. from raw FIPI+LIPI data")

    if st.button("Recompute", key="nccpl_recompute"):
        from pakfindata.db.connection import connect
        from pakfindata.sources.nccpl_flows import compute_derived_signals
        con = connect()
        with st.spinner("Computing derived signals..."):
            df = compute_derived_signals(con)

        if df is not None:
            st.success(f"Derived signals computed for {len(df)} days")
            st.cache_data.clear()
        else:
            st.warning("Not enough data to compute derived signals")


# ═══════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════


def _fmt_mn(val: float) -> str:
    """Format large PKR values as millions."""
    if abs(val) >= 1e9:
        return f"{val / 1e9:+,.1f} Bn"
    if abs(val) >= 1e6:
        return f"{val / 1e6:+,.0f} Mn"
    return f"{val:+,.0f}"
