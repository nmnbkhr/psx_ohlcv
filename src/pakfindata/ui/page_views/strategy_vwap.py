"""VWAP Execution Optimizer — order slicing to minimize market impact."""

from __future__ import annotations

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3",
}
_CHART = dict(paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], font_color=_C["text"],
              margin=dict(t=30, b=20, l=50, r=20))


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.markdown("### VWAP Execution Optimizer")
    st.caption("Minimize market impact by slicing orders proportional to volume profile")

    tab_plan, tab_bt, tab_profile, tab_method = st.tabs(["Execution Plan", "Backtest", "Volume Profile", "Methodology"])

    with tab_plan:
        _render_plan()
    with tab_bt:
        _render_backtest()
    with tab_profile:
        _render_profile()
    with tab_method:
        _render_methodology()

    render_footer()


def _render_plan():
    from pakfindata.engine.vwap_execution import generate_execution_plan

    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    with c1:
        symbol = st.text_input("Symbol", value="OGDC", key="vwap_sym").upper().strip()
    with c2:
        side = st.selectbox("Side", ["BUY", "SELL"], key="vwap_side")
    with c3:
        shares = st.number_input("Total Shares", value=500000, step=50000, min_value=1000, key="vwap_shares")
    with c4:
        strategy = st.selectbox("Strategy", ["VWAP", "TWAP", "AGGRESSIVE"], key="vwap_strat")

    max_part = st.slider("Max Participation Rate", 0.05, 0.30, 0.15, 0.01, key="vwap_part")

    if st.button("Generate Plan", type="primary", key="vwap_gen"):
        with st.spinner("Building volume profile & plan..."):
            plan = generate_execution_plan(symbol, side, shares, strategy, max_part)

        if not plan:
            st.error(f"No ohlcv_5s data for {symbol}")
            return

        # Warnings
        for w in plan.warnings:
            st.warning(w)

        # KPIs
        mc = st.columns(6)
        with mc[0]:
            _kpi("Slices", str(len(plan.slices)))
        with mc[1]:
            _kpi("Duration", f"{plan.duration_min}min")
        with mc[2]:
            _kpi("Arrival", f"{plan.arrival_price:.2f}")
        with mc[3]:
            _kpi("Est Slippage", f"{plan.estimated_slippage_bps:.1f}bps")
        with mc[4]:
            _kpi("Daily Vol", f"{plan.daily_avg_volume / 1e6:.1f}M")
        with mc[5]:
            pc = _C["down"] if plan.participation_total > 0.3 else _C["amber"] if plan.participation_total > 0.1 else _C["up"]
            _kpi("Participation", f"{plan.participation_total:.1%}", pc)

        # Slice schedule chart
        slices_df = pd.DataFrame([s.to_dict() for s in plan.slices])

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(go.Bar(
            x=slices_df["time_start"], y=slices_df["target_shares"],
            name="Target Shares", marker_color=_C["accent"], opacity=0.8,
        ), secondary_y=False)
        fig.add_trace(go.Scatter(
            x=slices_df["time_start"], y=slices_df["participation_rate"] * 100,
            name="Participation %", line=dict(color=_C["amber"], width=2),
            mode="lines+markers",
        ), secondary_y=True)
        fig.add_hline(y=max_part * 100, line_dash="dash", line_color=_C["down"],
                      annotation_text=f"Max {max_part:.0%}", secondary_y=True)

        fig.update_layout(**_CHART, height=350,
                          legend=dict(orientation="h", y=1.08, bgcolor="rgba(0,0,0,0)"))
        fig.update_yaxes(title_text="Shares", gridcolor=_C["grid"], secondary_y=False)
        fig.update_yaxes(title_text="Participation %", gridcolor=_C["grid"], secondary_y=True)
        st.plotly_chart(fig, use_container_width=True)

        # Slice table
        with st.expander("Full Execution Schedule"):
            show = slices_df[["slice_num", "time_start", "time_end", "target_shares",
                              "target_pct", "hist_volume", "participation_rate", "limit_price", "urgency"]].copy()
            show["target_pct"] = show["target_pct"].map(lambda x: f"{x:.1%}")
            show["hist_volume"] = show["hist_volume"].map(lambda x: f"{x:,.0f}")
            show["participation_rate"] = show["participation_rate"].map(lambda x: f"{x:.2%}")
            show["limit_price"] = show["limit_price"].map(lambda x: f"{x:.2f}")
            st.dataframe(show, use_container_width=True, hide_index=True)


def _render_backtest():
    from pakfindata.engine.vwap_execution import backtest_execution, get_available_dates

    c1, c2, c3, c4 = st.columns([2, 1, 1, 1])
    with c1:
        symbol = st.text_input("Symbol", value="OGDC", key="vwap_bt_sym").upper().strip()
    with c2:
        side = st.selectbox("Side", ["BUY", "SELL"], key="vwap_bt_side")
    with c3:
        shares = st.number_input("Shares", value=500000, step=50000, min_value=1000, key="vwap_bt_shares")
    with c4:
        strategy = st.selectbox("Strategy", ["VWAP", "TWAP", "AGGRESSIVE"], key="vwap_bt_strat")

    dates = get_available_dates(symbol) if symbol else []
    date_str = st.selectbox("Date", dates if dates else ["No data"], key="vwap_bt_date")

    if st.button("Run Backtest", type="primary", key="vwap_bt_run") and date_str != "No data":
        with st.spinner(f"Backtesting {strategy} on {date_str}..."):
            result = backtest_execution(symbol, side, shares, date_str, strategy)

        if not result:
            st.error("Backtest failed — not enough data")
            return

        # KPIs
        mc = st.columns(6)
        with mc[0]:
            _kpi("Exec VWAP", f"{result['exec_vwap']:.2f}")
        with mc[1]:
            _kpi("Market VWAP", f"{result['market_vwap']:.2f}")
        with mc[2]:
            _kpi("Arrival", f"{result['arrival_price']:.2f}")
        with mc[3]:
            sc = _C["up"] if result["impl_shortfall_bps"] < 0 else _C["down"]
            _kpi("Impl Shortfall", f"{result['impl_shortfall_bps']:+.1f}bps", sc)
        with mc[4]:
            sc = _C["up"] if result["vwap_slippage_bps"] < 0 else _C["down"]
            _kpi("VWAP Slippage", f"{result['vwap_slippage_bps']:+.1f}bps", sc)
        with mc[5]:
            _kpi("Participation", f"{result['participation']:.1%}")

        st.markdown(f"Slices executed: {result['slices_executed']} | Market vol: {result['total_market_vol']:,.0f}")

        # Compare strategies
        if st.checkbox("Compare all strategies", key="vwap_bt_compare"):
            rows = []
            for strat in ["VWAP", "TWAP", "AGGRESSIVE"]:
                r = backtest_execution(symbol, side, shares, date_str, strat)
                if r:
                    rows.append({
                        "Strategy": strat,
                        "Exec VWAP": f"{r['exec_vwap']:.2f}",
                        "Impl Shortfall (bps)": f"{r['impl_shortfall_bps']:+.1f}",
                        "VWAP Slippage (bps)": f"{r['vwap_slippage_bps']:+.1f}",
                        "Participation": f"{r['participation']:.1%}",
                    })
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _render_profile():
    from pakfindata.engine.vwap_execution import build_volume_profile

    symbol = st.text_input("Symbol", value="OGDC", key="vwap_prof_sym").upper().strip()
    lookback = st.slider("Lookback dates", 3, 20, 10, key="vwap_prof_lb")

    if not symbol:
        return

    with st.spinner("Building profile..."):
        profile = build_volume_profile(symbol, lookback_dates=lookback)

    if profile.empty:
        st.warning(f"No ohlcv_5s data for {symbol}")
        return

    st.markdown(f"**{symbol}** — {int(profile['days'].max())} days averaged, {len(profile)} intervals")

    # Volume profile bar chart
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(
        x=profile["time_start"], y=profile["avg_volume"] / 1e6,
        name="Avg Volume (M)", marker_color=_C["accent"], opacity=0.8,
    ), secondary_y=False)
    fig.add_trace(go.Scatter(
        x=profile["time_start"], y=profile["cum_pct"] * 100,
        name="Cumulative %", line=dict(color=_C["amber"], width=2),
    ), secondary_y=True)

    fig.update_layout(**_CHART, height=350,
                      legend=dict(orientation="h", y=1.08, bgcolor="rgba(0,0,0,0)"))
    fig.update_yaxes(title_text="Volume (M)", gridcolor=_C["grid"], secondary_y=False)
    fig.update_yaxes(title_text="Cumulative %", gridcolor=_C["grid"], secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    # Spread by time
    fig2 = go.Figure()
    fig2.add_trace(go.Bar(x=profile["time_start"], y=profile["spread_bps"],
                          marker_color=_C["cyan"], opacity=0.7, name="Spread (bps)"))
    fig2.update_layout(**_CHART, height=250, yaxis=dict(gridcolor=_C["grid"], title="Spread (bps)"))
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Profile Data"):
        st.dataframe(profile[["time_start", "time_end", "avg_volume", "pct_of_day", "spread_bps", "avg_price"]],
                     use_container_width=True, hide_index=True)


def _render_methodology():
    st.markdown("""
#### VWAP Execution

**Goal:** Execute a large order at or better than the day's VWAP.

**Method:** Slice the order into 15-minute intervals, sizing each proportional to
that interval's historical average volume. This matches the market's natural rhythm
and minimizes price impact.

---

#### Three Execution Strategies

| Strategy | Logic | Best For |
|---|---|---|
| **VWAP** | Proportional to historical volume curve | Most orders — benchmark matching |
| **TWAP** | Equal slices across time | When volume profile is uncertain |
| **Aggressive** | Front-load when spread is tight | Urgent orders, momentum names |

---

#### Performance Metrics

- **Implementation Shortfall** = (Exec VWAP - Arrival Price) / Arrival — total cost of execution
- **VWAP Slippage** = (Exec VWAP - Market VWAP) / Market VWAP — quality vs benchmark
- **Participation Rate** = Our volume / Market volume per interval

---

#### PSX-Specific
- Market hours: 09:30-15:30 (Mon-Thu), 09:30-16:30 (Fri)
- Volume is U-shaped: high at open/close, low midday
- Max participation 15% default — going above attracts unwanted attention
- Circuit breakers ±7.5% — execution must stop if limit hit
    """)
