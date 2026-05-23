"""PMEX Analytics Dashboard — spreads, rollovers, global premium, alerts, contract calendar.

Standalone Streamlit page that can run independently:
    streamlit run src/pakfindata/ui/page_views/pmex_analytics_page.py

Or be integrated into the main app by adding to the page registry.

Tabs:
  1. Spreads          — Bid-ask spread tracking, anomaly detection
  2. Rollovers        — Volume crossover, DTE countdown
  3. Global Premium   — PMEX vs COMEX/NYMEX premium cards + time series
  4. Alerts           — Live alert panel, severity coloring
  5. Contract Calendar — Expiry dates, DTE bar chart, product chains
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Design system (matches existing PMEX page)
# ─────────────────────────────────────────────────────────────────────────────

_COLORS = {
    "gold": "#FFD700",
    "crude": "#FF6B35",
    "silver": "#C0C0C0",
    "fx": "#45B7D1",
    "index": "#9B59B6",
    "copper": "#B87333",
    "up": "#00C853",
    "down": "#FF5252",
    "neutral": "#78909C",
    "bg": "#0E1117",
    "card": "#1E1E2E",
    "text": "#E0E0E0",
    "accent": "#4ECDC4",
    "warning": "#FFA726",
    "critical": "#FF5252",
    "info": "#42A5F5",
}

_CHART_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11),
    margin=dict(l=10, r=10, t=35, b=10),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)", zeroline=False),
    yaxis=dict(gridcolor="rgba(255,255,255,0.08)", zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
)

# ─────────────────────────────────────────────────────────────────────────────
# Database connections
# ─────────────────────────────────────────────────────────────────────────────

COMMOD_DB_PATH = Path("/home/smnb/psxdata_rescue/commod/commod.db")
PSX_DB_PATH = Path("/home/smnb/psxdata_rescue/psx.sqlite")


@st.cache_resource
def _get_commod_con() -> sqlite3.Connection:
    con = sqlite3.connect(str(COMMOD_DB_PATH), check_same_thread=False, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


@st.cache_resource
def _get_psx_con() -> sqlite3.Connection:
    con = sqlite3.connect(str(PSX_DB_PATH), check_same_thread=False, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _styled_fig(height: int = 380, **kwargs) -> go.Figure:
    fig = go.Figure()
    layout = {**_CHART_LAYOUT, "height": height, **kwargs}
    fig.update_layout(**layout)
    return fig


def _severity_color(severity: str) -> str:
    return _COLORS.get(severity, _COLORS["neutral"])


def _metric_card(label: str, value: str, delta: str = "", color: str = "") -> str:
    delta_html = ""
    if delta:
        c = color or _COLORS["text"]
        delta_html = f'<div style="color:{c};font-size:13px">{delta}</div>'
    return f"""
    <div style="background:{_COLORS['card']};border-radius:8px;padding:14px 16px;
                border-left:3px solid {color or _COLORS['accent']}">
        <div style="color:{_COLORS['neutral']};font-size:11px;text-transform:uppercase">{label}</div>
        <div style="color:{_COLORS['text']};font-size:22px;font-weight:600;margin:4px 0">{value}</div>
        {delta_html}
    </div>
    """


# ─────────────────────────────────────────────────────────────────────────────
# Tab 1: Spreads
# ─────────────────────────────────────────────────────────────────────────────


def _render_spreads():
    from pakfindata.commodities.pmex_analytics import compute_spreads, spread_summary

    con = _get_psx_con()
    st.subheader("Bid-Ask Spread Analysis")

    # Summary table
    summary = spread_summary(con)
    if summary.empty:
        st.info("No spread data available. Sync PMEX market watch data first.")
        return

    # Anomaly highlight
    anomalies = summary[summary.get("spread_anomaly", False) == True]
    if not anomalies.empty:
        st.warning(f"{len(anomalies)} contracts with abnormally wide spreads (>2x 30-day avg)")

    # Category filter
    categories = sorted(summary["category"].unique())
    sel_cat = st.selectbox("Category", ["All"] + categories, key="spread_cat")
    if sel_cat != "All":
        summary = summary[summary["category"] == sel_cat]

    # Display table
    display_cols = ["contract", "category", "bid", "ask", "spread_abs", "spread_pct"]
    if "avg_spread_pct_30d" in summary.columns:
        display_cols.append("avg_spread_pct_30d")
    if "spread_anomaly" in summary.columns:
        display_cols.append("spread_anomaly")

    st.dataframe(
        summary[display_cols].style.format({
            "bid": "{:.2f}", "ask": "{:.2f}",
            "spread_abs": "{:.2f}", "spread_pct": "{:.4f}%",
            "avg_spread_pct_30d": "{:.4f}%",
        }),
        width='stretch',
        height=400,
    )

    # Time series chart for selected contract
    st.markdown("---")
    contracts = sorted(summary["contract"].unique())
    sel_contract = st.selectbox("Spread Time Series", contracts, key="spread_ts_contract")
    days = st.slider("Lookback (days)", 7, 90, 30, key="spread_days")

    ts_df = compute_spreads(con, contract=sel_contract, days=days)
    if not ts_df.empty:
        fig = _styled_fig(title=f"Spread — {sel_contract}")
        fig.add_trace(go.Scatter(
            x=ts_df["snapshot_date"], y=ts_df["spread_pct"],
            mode="lines+markers", name="Spread %",
            line=dict(color=_COLORS["accent"], width=2),
            marker=dict(size=4),
        ))
        fig.update_yaxes(title_text="Spread %")
        st.plotly_chart(fig, width='stretch')


# ─────────────────────────────────────────────────────────────────────────────
# Tab 2: Rollovers
# ─────────────────────────────────────────────────────────────────────────────


def _render_rollovers():
    from pakfindata.commodities.pmex_analytics import detect_rollovers
    from pakfindata.commodities.pmex_contract_calendar import get_rollover_calendar

    con_commod = _get_commod_con()

    st.subheader("Contract Rollover Monitor")

    # Rollover signals
    signals = detect_rollovers(con_commod, lookback_days=10)
    if signals:
        st.error(f"{len(signals)} rollover signals detected")
        sig_df = pd.DataFrame(signals)
        st.dataframe(sig_df, width='stretch', height=250)
    else:
        st.success("No rollover signals in the last 10 days")

    st.markdown("---")

    # Rollover calendar
    st.subheader("Days to Expiry")
    symbols_rows = con_commod.execute(
        "SELECT DISTINCT symbol FROM pmex_ohlc WHERE traded_volume > 0"
    ).fetchall()
    all_symbols = [r["symbol"] for r in symbols_rows]

    calendar = get_rollover_calendar(all_symbols)
    if calendar.empty:
        st.info("No active contracts found in OHLC data.")
        return

    # Highlight imminent rollovers
    imminent = calendar[calendar["rollover_imminent"] == True]
    if not imminent.empty:
        st.warning(f"{len(imminent)} products with expiry within 14 days")

    # DTE bar chart
    dte_data = calendar[calendar["near_dte"].notna()].sort_values("near_dte")
    if not dte_data.empty:
        fig = _styled_fig(height=max(300, len(dte_data) * 25), title="Days to Expiry")
        colors = [_COLORS["critical"] if d <= 14 else _COLORS["warning"] if d <= 30
                  else _COLORS["accent"] for d in dte_data["near_dte"]]
        fig.add_trace(go.Bar(
            x=dte_data["near_dte"],
            y=dte_data["base"],
            orientation="h",
            marker_color=colors,
            text=dte_data["near_contract"],
            textposition="auto",
        ))
        fig.update_xaxes(title_text="Days")
        st.plotly_chart(fig, width='stretch')

    # Full calendar table
    st.dataframe(calendar, width='stretch', height=350)


# ─────────────────────────────────────────────────────────────────────────────
# Tab 3: Global Premium
# ─────────────────────────────────────────────────────────────────────────────


def _render_global_premium():
    from pakfindata.commodities.pmex_crossref import (
        premium_dashboard_data,
        premium_timeseries,
    )

    con_commod = _get_commod_con()
    con_psx = _get_psx_con()

    st.subheader("PMEX vs Global Benchmark Premium")

    dash = premium_dashboard_data(con_commod, con_psx)
    if dash.empty:
        st.info("No premium data available. Ensure both PMEX OHLC and yfinance commodity data are synced.")
        return

    # Premium cards (top row)
    key_products = ["GO1OZ", "CRUDE10", "BRENT10", "NGAS1K"]
    card_data = dash[dash["base"].isin(key_products)]

    cols = st.columns(min(4, max(1, len(card_data))))
    for i, (_, row) in enumerate(card_data.iterrows()):
        prem = row.get("premium_pct")
        if prem is not None:
            color = _COLORS["up"] if prem > 0 else _COLORS["down"] if prem < 0 else _COLORS["neutral"]
            sign = "+" if prem > 0 else ""
            cols[i % len(cols)].markdown(
                _metric_card(
                    row["commodity"],
                    f"${row.get('pmex_usd', 0):,.2f}",
                    f"{sign}{prem:.2f}% vs global",
                    color,
                ),
                unsafe_allow_html=True,
            )

    st.markdown("---")

    # Full premium table
    display_cols = [
        "base", "commodity", "pmex_symbol", "pmex_close", "global_close",
        "pmex_usd", "premium_abs", "premium_pct",
    ]
    available_cols = [c for c in display_cols if c in dash.columns]
    st.dataframe(
        dash[available_cols].style.format({
            "pmex_close": "{:.2f}", "global_close": "{:.2f}",
            "pmex_usd": "{:.2f}", "premium_abs": "{:.2f}", "premium_pct": "{:.3f}%",
        }),
        width='stretch',
        height=350,
    )

    # Time series for selected product
    st.markdown("---")
    st.subheader("Premium Time Series")
    sel_base = st.selectbox("Product", sorted(dash["base"].unique()), key="prem_base")
    sel_row = dash[dash["base"] == sel_base].iloc[0]
    lookback = st.slider("Lookback (days)", 30, 365, 90, key="prem_lookback")

    ts = premium_timeseries(con_commod, con_psx, sel_row["pmex_symbol"], lookback)
    if not ts.empty:
        fig = _styled_fig(title=f"Premium — {sel_base} (PMEX vs Global)")
        fig.add_trace(go.Scatter(
            x=ts["date"], y=ts["premium_pct"],
            mode="lines", name="Premium %",
            line=dict(color=_COLORS["gold"], width=2),
            fill="tozeroy",
            fillcolor="rgba(255,215,0,0.1)",
        ))
        fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["neutral"])
        fig.update_yaxes(title_text="Premium %")
        st.plotly_chart(fig, width='stretch')

        # Stats
        c1, c2, c3 = st.columns(3)
        c1.metric("Avg Premium", f"{ts['premium_pct'].mean():.2f}%")
        c2.metric("Max Premium", f"{ts['premium_pct'].max():.2f}%")
        c3.metric("Min Premium", f"{ts['premium_pct'].min():.2f}%")
    else:
        st.info("No overlapping dates between PMEX and global data for this product.")


# ─────────────────────────────────────────────────────────────────────────────
# Tab 4: Alerts
# ─────────────────────────────────────────────────────────────────────────────


def _render_alerts():
    from pakfindata.commodities.pmex_alerts import run_all_checks

    con_commod = _get_commod_con()
    con_psx = _get_psx_con()

    st.subheader("PMEX Alert Monitor")

    # Config sidebar
    with st.expander("Alert Configuration", expanded=False):
        z_threshold = st.slider("Volume Z-Score Threshold", 1.5, 5.0, 2.5, 0.5, key="alert_z")
        spread_pctile = st.slider("Spread Percentile", 80, 99, 95, key="alert_spread")
        limit_pct = st.slider("Limit Proximity %", 1.0, 15.0, 5.0, 1.0, key="alert_limit")

    config = {
        "volume_z_threshold": z_threshold,
        "spread_percentile": spread_pctile,
        "limit_proximity_pct": limit_pct,
    }

    if st.button("Run Alert Checks", type="primary", key="run_alerts"):
        with st.spinner("Running all checks..."):
            alerts = run_all_checks(con_commod, con_psx, config)

        if not alerts:
            st.success("No alerts triggered. All clear.")
            return

        # Summary badges
        critical = [a for a in alerts if a.severity == "critical"]
        warnings = [a for a in alerts if a.severity == "warning"]
        info_alerts = [a for a in alerts if a.severity == "info"]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Alerts", len(alerts))
        c2.metric("Critical", len(critical))
        c3.metric("Warning", len(warnings))
        c4.metric("Info", len(info_alerts))

        st.markdown("---")

        # Alert cards
        for alert in alerts:
            color = _severity_color(alert.severity)
            icon = {"critical": "\u26a0\ufe0f", "warning": "\u26a0", "info": "\u2139\ufe0f"}.get(
                alert.severity, ""
            )
            st.markdown(
                f"""<div style="background:{_COLORS['card']};border-radius:6px;padding:10px 14px;
                    margin-bottom:8px;border-left:4px solid {color}">
                    <div style="display:flex;justify-content:space-between;align-items:center">
                        <span style="color:{color};font-weight:600;font-size:13px">
                            {icon} {alert.severity.upper()} — {alert.alert_type}
                        </span>
                        <span style="color:{_COLORS['neutral']};font-size:11px">{alert.contract}</span>
                    </div>
                    <div style="color:{_COLORS['text']};font-size:12px;margin-top:4px">{alert.message}</div>
                </div>""",
                unsafe_allow_html=True,
            )
    else:
        st.info("Click 'Run Alert Checks' to scan for active alerts.")


# ─────────────────────────────────────────────────────────────────────────────
# Tab 5: Contract Calendar
# ─────────────────────────────────────────────────────────────────────────────


def _render_contract_calendar():
    from pakfindata.commodities.pmex_contract_calendar import (
        classify_product,
        get_all_base_products,
        get_contract_chain,
        get_rollover_calendar,
        parse_contract,
    )

    con_commod = _get_commod_con()

    st.subheader("PMEX Contract Calendar")

    # Get all symbols
    symbols_rows = con_commod.execute("SELECT DISTINCT symbol FROM pmex_ohlc").fetchall()
    all_symbols = [r["symbol"] for r in symbols_rows]

    if not all_symbols:
        st.info("No PMEX OHLC data available.")
        return

    # Parse all contracts
    parsed = [parse_contract(s) for s in all_symbols]
    parsed_df = pd.DataFrame([{
        "contract": p.raw,
        "base": p.base,
        "commodity": p.commodity,
        "category": p.category,
        "expiry_code": p.expiry_code,
        "expiry_date": p.expiry_date.isoformat() if p.expiry_date else "N/A",
        "is_intraday": p.is_intraday,
        "is_weekly": p.is_weekly,
        "lot_size": p.lot_size,
        "currency": p.currency,
    } for p in parsed])

    # Summary metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Contracts", len(parsed))
    c2.metric("Base Products", parsed_df["base"].nunique())
    c3.metric("Commodities", parsed_df["commodity"].nunique())
    c4.metric("Categories", parsed_df["category"].nunique())

    # Contract table
    st.markdown("---")
    st.subheader("All Contracts")

    cat_filter = st.selectbox(
        "Filter by Category",
        ["All"] + sorted(parsed_df["category"].unique()),
        key="cal_cat",
    )
    if cat_filter != "All":
        parsed_df = parsed_df[parsed_df["category"] == cat_filter]

    st.dataframe(parsed_df, width='stretch', height=400)

    # Product chain explorer
    st.markdown("---")
    st.subheader("Product Chain Explorer")

    bases = get_all_base_products(all_symbols)
    sel_base = st.selectbox("Base Product", bases, key="cal_base")
    info = classify_product(sel_base)
    st.caption(f"**{info['commodity']}** — {info['lot_size']} — {info['currency']}")

    chain = get_contract_chain(sel_base, all_symbols)
    if chain:
        chain_data = [{
            "contract": p.raw,
            "expiry": p.expiry_date.isoformat() if p.expiry_date else "N/A",
            "days_to_expiry": (p.expiry_date - date.today()).days if p.expiry_date else None,
            "intraday": p.is_intraday,
        } for p in chain]
        st.table(pd.DataFrame(chain_data))
    else:
        st.info(f"No active (non-expired) contracts found for {sel_base}")


# ─────────────────────────────────────────────────────────────────────────────
# Main page renderer
# ─────────────────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────────────────
# Tab 6: Live Data & Sync Status
# ─────────────────────────────────────────────────────────────────────────────


def _render_live_data():
    from pakfindata.commodities.pmex_poller import single_poll, is_market_hours, list_daily_files
    from pakfindata.commodities.pmex_analytics import compute_intraday_spreads, intraday_volume_timeseries
    from pakfindata.commodities.commod_db import get_pmex_intraday_stats, get_pmex_intraday_latest

    con_commod = _get_commod_con()

    st.subheader("Live PMEX Data & Poller")

    # Market status
    from datetime import datetime, timezone, timedelta
    PKT = timezone(timedelta(hours=5))
    now = datetime.now(PKT)
    market_open = is_market_hours(now)

    c1, c2, c3 = st.columns(3)
    c1.metric("Market Status", "OPEN" if market_open else "CLOSED")
    c2.metric("Time (PKT)", now.strftime("%H:%M:%S"))
    c3.metric("Date", now.strftime("%Y-%m-%d"))

    # Intraday DB stats
    stats = get_pmex_intraday_stats(con_commod)
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Snapshots", f"{stats['total_rows']:,}")
    c2.metric("Contracts", stats["contracts"])
    c3.metric("Days Captured", stats["days"])
    c4.metric("Total Polls", stats["polls"])

    if stats["last_ts"]:
        st.caption(f"Last poll: {stats['last_ts']}")

    st.markdown("---")

    # Manual poll button
    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("Poll Now", type="primary", key="poll_now"):
            with st.spinner("Polling PMEX portal..."):
                result = single_poll(save_to_db=True, save_to_file=True)
            if result["errors"]:
                st.error(f"Errors: {result['errors']}")
            else:
                st.success(f"Fetched {result['contracts']} contracts, stored {result['rows_stored']} rows")

    with col_b:
        if st.button("Export Daily Files", key="export_daily"):
            with st.spinner("Exporting..."):
                from pakfindata.commodities.pmex_daily_files import export_daily
                result = export_daily()
            st.json(result)

    st.markdown("---")

    # Latest intraday snapshot
    st.subheader("Latest Intraday Snapshot")
    latest = get_pmex_intraday_latest(con_commod)
    if latest:
        df = pd.DataFrame(latest)
        display_cols = ["contract", "category", "last_price", "bid", "ask", "spread_pct", "total_vol", "change_pct"]
        available = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available], width='stretch', height=350)
    else:
        st.info("No intraday snapshots yet. Click 'Poll Now' or start the poller.")

    # Intraday spread chart
    st.markdown("---")
    st.subheader("Intraday Spread Tracker")
    intra_spreads = compute_intraday_spreads(con_commod)
    if not intra_spreads.empty:
        contracts = sorted(intra_spreads["contract"].unique())
        sel = st.selectbox("Contract", contracts, key="intra_spread_sel")
        c_data = intra_spreads[intra_spreads["contract"] == sel]

        if not c_data.empty:
            fig = _styled_fig(title=f"Intraday Spread — {sel}")
            fig.add_trace(go.Scatter(
                x=c_data["snapshot_ts"], y=c_data["spread_pct"],
                mode="lines+markers", name="Spread %",
                line=dict(color=_COLORS["accent"], width=2),
                marker=dict(size=3),
            ))
            fig.update_yaxes(title_text="Spread %")
            st.plotly_chart(fig, width='stretch')

            # Price + Volume
            vol_data = intraday_volume_timeseries(con_commod, sel)
            if not vol_data.empty:
                fig2 = _styled_fig(title=f"Intraday Price & Volume — {sel}")
                fig2.add_trace(go.Scatter(
                    x=vol_data["snapshot_ts"], y=vol_data["last_price"],
                    mode="lines", name="Price",
                    line=dict(color=_COLORS["gold"], width=2),
                ))
                fig2.add_trace(go.Bar(
                    x=vol_data["snapshot_ts"], y=vol_data["incremental_vol"],
                    name="Vol (incremental)", yaxis="y2",
                    marker_color="rgba(78,205,196,0.5)",
                ))
                fig2.update_layout(
                    yaxis2=dict(
                        overlaying="y", side="right",
                        gridcolor="rgba(255,255,255,0.03)",
                        title="Volume",
                    ),
                )
                st.plotly_chart(fig2, width='stretch')

    # File inventory
    st.markdown("---")
    st.subheader("Daily Files")
    jsonl_files = list_daily_files()
    if jsonl_files:
        st.dataframe(pd.DataFrame(jsonl_files), width='stretch', height=200)
    else:
        st.info("No intraday JSONL files yet.")

    from pakfindata.commodities.pmex_daily_files import list_exported_files
    exported = list_exported_files()
    for ftype, files in exported.items():
        if files:
            with st.expander(f"{ftype} ({len(files)} files)"):
                st.dataframe(pd.DataFrame(files), width='stretch')


def render_page():
    """Main entry point for the PMEX Analytics page."""
    st.title("PMEX Analytics")
    st.caption("Derived analytics, global benchmarks, alerts & contract intelligence")

    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "Live Data",
        "Spreads",
        "Rollovers",
        "Global Premium",
        "Alerts",
        "Contract Calendar",
    ])

    with tab1:
        _render_live_data()
    with tab2:
        _render_spreads()
    with tab3:
        _render_rollovers()
    with tab4:
        _render_global_premium()
    with tab5:
        _render_alerts()
    with tab6:
        _render_contract_calendar()


# Standalone mode
if __name__ == "__main__":
    st.set_page_config(
        page_title="PMEX Analytics",
        page_icon="\U0001f4ca",
        layout="wide",
    )
    render_page()
