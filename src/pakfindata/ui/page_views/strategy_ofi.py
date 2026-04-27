"""OFI Alpha Strategy — Order Flow Imbalance signals, backtest, scanner."""

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


def render_page():
    st.markdown("### OFI Alpha Strategy")
    st.caption("Order Flow Imbalance — bid/ask depth signals for PSX")

    tab_live, tab_bt, tab_scan, tab_research = st.tabs([
        "Live OFI", "Backtest", "Scanner", "Research"
    ])

    with tab_live:
        _render_live()
    with tab_bt:
        _render_backtest()
    with tab_scan:
        _render_scanner()
    with tab_research:
        _render_research()

    render_footer()


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.3em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════
# TAB 1: LIVE OFI
# ═══════════════════════════════════════════════════════

def _render_live():
    from pakfindata.engine.ofi_strategy import load_ticks_for_ofi, compute_ofi_bars, _duck_con

    c1, c2 = st.columns([1, 1])
    with c1:
        symbol = st.text_input("Symbol", value="HUBC", key="ofi_sym").upper().strip()
    with c2:
        con = _duck_con()
        dates = [r[0] for r in con.execute(
            "SELECT DISTINCT date AS d FROM tick_logs "
            "WHERE symbol = ? ORDER BY d DESC", [symbol],
        ).fetchall()] if symbol else []
        con.close()
        date_str = st.selectbox("Date", dates if dates else ["No data"], key="ofi_date")

    if not dates or date_str == "No data":
        st.info(f"No tick data for {symbol}")
        return

    bar_min = st.selectbox("Bar size", [5, 15, 30, 60], index=1, key="ofi_bar")

    with st.spinner("Computing OFI bars..."):
        ticks = load_ticks_for_ofi(symbol, date_str)
        bars = compute_ofi_bars(ticks, bar_minutes=bar_min)

    if bars.empty:
        st.warning("Not enough ticks for OFI computation")
        return

    last = bars.iloc[-1]
    ofi = float(last["ofi_normalized"])

    # KPI row
    mc = st.columns(5)
    with mc[0]:
        sc = _C["up"] if ofi > 0.3 else _C["down"] if ofi < -0.3 else _C["dim"]
        _kpi("Current OFI", f"{ofi:+.3f}", sc)
    with mc[1]:
        sig = "LONG" if ofi > 0.3 else "SHORT" if ofi < -0.3 else "FLAT"
        _kpi("Signal", sig, _C["up"] if sig == "LONG" else _C["down"] if sig == "SHORT" else _C["dim"])
    with mc[2]:
        _kpi("Bars", str(len(bars)))
    with mc[3]:
        _kpi("Ticks", f"{int(last.get('tick_count', 0)):,}")
    with mc[4]:
        _kpi("Spread", f"{last.get('spread_bps', 0):.1f} bps")

    # Price + OFI chart
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.65, 0.35], vertical_spacing=0.05)

    fig.add_trace(go.Scatter(x=bars["bar_time"], y=bars["close"], name="Price",
                             line=dict(color=_C["text"], width=1.5)), row=1, col=1)

    colors = [_C["up"] if x > 0 else _C["down"] for x in bars["ofi_normalized"]]
    fig.add_trace(go.Bar(x=bars["bar_time"], y=bars["ofi_normalized"], name="OFI",
                         marker_color=colors), row=2, col=1)
    fig.add_hline(y=0.3, line_dash="dash", line_color=_C["up"], opacity=0.5, row=2, col=1)
    fig.add_hline(y=-0.3, line_dash="dash", line_color=_C["down"], opacity=0.5, row=2, col=1)

    fig.update_layout(**_CHART, height=450, showlegend=False,
                      yaxis=dict(gridcolor=_C["grid"], title="Price"),
                      yaxis2=dict(gridcolor=_C["grid"], title="OFI", range=[-1, 1]))
    st.plotly_chart(fig, width='stretch')

    # Bar table
    with st.expander("Bar Details"):
        show = bars[["bar_time", "open", "close", "volume", "ofi_normalized", "ofi_delta_norm",
                      "bar_return", "spread_bps", "tick_count"]].copy()
        show.columns = ["Time", "Open", "Close", "Volume", "OFI", "OFI(delta)", "Return", "Spread(bps)", "Ticks"]
        st.dataframe(show, width='stretch', hide_index=True)


# ═══════════════════════════════════════════════════════
# TAB 2: BACKTEST
# ═══════════════════════════════════════════════════════

def _render_backtest():
    from pakfindata.engine.ofi_strategy import backtest_ofi_strategy

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        symbol = st.text_input("Symbol", value="HUBC", key="ofi_bt_sym").upper().strip()
    with c2:
        bar_min = st.selectbox("Bar", [5, 15, 30, 60], index=1, key="ofi_bt_bar")
    with c3:
        run = st.button("Run Backtest", type="primary", key="ofi_bt_run")

    c4, c5, c6, c7 = st.columns(4)
    with c4:
        long_th = st.slider("Long threshold", 0.1, 0.8, 0.3, 0.05, key="ofi_bt_lt")
    with c5:
        short_th = st.slider("Short threshold", -0.8, -0.1, -0.3, 0.05, key="ofi_bt_st")
    with c6:
        sl = st.slider("Stop loss %", 0.5, 5.0, 2.0, 0.5, key="ofi_bt_sl")
    with c7:
        tp = st.slider("Take profit %", 1.0, 10.0, 3.0, 0.5, key="ofi_bt_tp")

    if not run:
        st.info("Configure parameters and click Run Backtest.")
        return

    with st.spinner(f"Backtesting {symbol}..."):
        result = backtest_ofi_strategy(
            symbol, bar_minutes=bar_min,
            long_threshold=long_th, short_threshold=short_th,
            stop_loss_pct=sl / 100, take_profit_pct=tp / 100,
        )

    if "error" in result:
        st.error(result["error"])
        return

    m = result["metrics"]
    trades = result["trades"]

    # Metrics
    mc = st.columns(6)
    labels = ["Trades", "Win Rate", "Profit Factor", "Return", "Sharpe", "Max DD"]
    values = [
        str(m["total_trades"]), f"{m['win_rate']:.0%}", f"{m['profit_factor']:.2f}",
        f"{m['total_return']:.2%}", f"{m['sharpe_ratio']:.2f}", f"{m['max_drawdown']:.2%}",
    ]
    colors = [
        _C["text"], _C["up"] if m["win_rate"] > 0.5 else _C["down"],
        _C["up"] if m["profit_factor"] > 1 else _C["down"],
        _C["up"] if m["total_return"] > 0 else _C["down"],
        _C["up"] if m["sharpe_ratio"] > 0 else _C["down"], _C["down"],
    ]
    for i, col in enumerate(mc):
        with col:
            _kpi(labels[i], values[i], colors[i])

    st.markdown(f"Long: {m['long_trades']} ({m['long_win_rate']:.0%} WR) | Short: {m['short_trades']} ({m['short_win_rate']:.0%} WR) | Avg hold: {m['avg_bars_held']:.1f} bars | Dates: {m['dates_tested']}")

    if trades.empty:
        return

    # Equity curve
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=list(range(len(trades))), y=trades["cum_return"],
                             name="Equity", line=dict(color=_C["accent"], width=2), fill="tozeroy",
                             fillcolor="rgba(33,150,243,0.1)"))
    fig.add_hline(y=1.0, line_dash="dash", line_color=_C["dim"])
    fig.update_layout(**_CHART, height=300, yaxis=dict(gridcolor=_C["grid"], title="Cumulative Return"))
    st.plotly_chart(fig, width='stretch')

    # PnL distribution + exit reasons
    c1, c2 = st.columns(2)
    with c1:
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=trades["pnl_pct"] * 100, nbinsx=20,
                                   marker_color=_C["cyan"], name="P&L %"))
        fig.add_vline(x=0, line_color=_C["dim"])
        fig.update_layout(**_CHART, height=250, xaxis=dict(title="P&L %", gridcolor=_C["grid"]),
                          yaxis=dict(gridcolor=_C["grid"]))
        st.plotly_chart(fig, width='stretch')

    with c2:
        exits = m["exit_reasons"]
        fig = go.Figure(go.Pie(labels=list(exits.keys()), values=list(exits.values()),
                               hole=0.4, marker=dict(colors=[_C["up"], _C["down"], _C["amber"], _C["cyan"]])))
        fig.update_layout(**_CHART, height=250, showlegend=True)
        st.plotly_chart(fig, width='stretch')

    # Trade log
    with st.expander("Trade Log"):
        show = trades[["date", "direction", "entry_time", "entry_price", "exit_time",
                        "exit_price", "pnl_pct", "bars_held", "exit_reason", "entry_ofi"]].copy()
        show["pnl_pct"] = show["pnl_pct"].map(lambda x: f"{x:.2%}")
        show["entry_ofi"] = show["entry_ofi"].map(lambda x: f"{x:.3f}")
        st.dataframe(show, width='stretch', hide_index=True, height=400)


# ═══════════════════════════════════════════════════════
# TAB 3: SCANNER
# ═══════════════════════════════════════════════════════

def _render_scanner():
    from pakfindata.engine.ofi_strategy import scan_current_ofi

    c1, c2 = st.columns([2, 1])
    with c1:
        bar_min = st.selectbox("Bar", [5, 15, 30], index=1, key="ofi_scan_bar")
    with c2:
        run = st.button("Scan", type="primary", key="ofi_scan_run")

    if not run:
        st.info("Click Scan to find symbols with strong OFI signals.")
        return

    with st.spinner("Scanning..."):
        df = scan_current_ofi(bar_minutes=bar_min)

    if df.empty:
        st.warning("No strong OFI signals found.")
        return

    st.markdown(f"**{len(df)} symbols** with |OFI| > 0.15 on {df['date'].iloc[0]}")

    def _color_sig(val):
        c = {"LONG": "#1B5E20", "SHORT": "#B71C1C", "WEAK": "#333"}
        return f"background-color: {c.get(val, '#333')}"

    show = df[["symbol", "ofi", "signal", "price", "spread_bps", "tick_count", "bar_return"]].copy()
    show["ofi"] = show["ofi"].map(lambda x: f"{x:+.3f}")
    show["bar_return"] = show["bar_return"].map(lambda x: f"{x:+.2%}")
    show["spread_bps"] = show["spread_bps"].map(lambda x: f"{x:.1f}")

    styled = show.style.map(_color_sig, subset=["signal"])
    st.dataframe(styled, width='stretch', hide_index=True, height=500)


# ═══════════════════════════════════════════════════════
# TAB 4: RESEARCH
# ═══════════════════════════════════════════════════════

def _render_research():
    from pakfindata.engine.ofi_strategy import load_ticks_for_ofi, compute_ofi_bars, _duck_con

    symbol = st.text_input("Symbol", value="HUBC", key="ofi_res_sym").upper().strip()
    if not symbol:
        return

    con = _duck_con()
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT date AS d FROM tick_logs WHERE symbol = ? ORDER BY d", [symbol],
    ).fetchall()]
    con.close()

    if not dates:
        st.info(f"No tick data for {symbol}")
        return

    st.caption(f"{len(dates)} dates available for {symbol}")

    # Collect all bars across dates
    all_bars = []
    for d in dates:
        ticks = load_ticks_for_ofi(symbol, d)
        if len(ticks) < 30:
            continue
        bars = compute_ofi_bars(ticks, bar_minutes=15)
        if not bars.empty:
            all_bars.append(bars)

    if not all_bars:
        st.warning("No bars computed")
        return

    df = pd.concat(all_bars, ignore_index=True)
    df = df.dropna(subset=["ofi_normalized", "next_return"])

    st.markdown(f"**{len(df)} bars** across {len(dates)} dates")

    c1, c2 = st.columns(2)

    with c1:
        # OFI vs Next-Bar Return scatter
        corr = df["ofi_normalized"].corr(df["next_return"])
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["ofi_normalized"], y=df["next_return"] * 100,
            mode="markers", marker=dict(size=4, color=_C["cyan"], opacity=0.5),
            name="Bars",
        ))
        # Regression line
        if len(df) > 10:
            z = np.polyfit(df["ofi_normalized"], df["next_return"] * 100, 1)
            x_line = np.linspace(-1, 1, 50)
            fig.add_trace(go.Scatter(x=x_line, y=np.polyval(z, x_line),
                                     line=dict(color=_C["amber"], width=2), name=f"R={corr:.3f}"))
        fig.update_layout(**_CHART, height=350, title=dict(text=f"OFI vs Next-Bar Return (R={corr:.3f})", font=dict(size=12)),
                          xaxis=dict(title="OFI", gridcolor=_C["grid"]),
                          yaxis=dict(title="Next Return %", gridcolor=_C["grid"]))
        st.plotly_chart(fig, width='stretch')

    with c2:
        # OFI distribution
        fig = go.Figure()
        fig.add_trace(go.Histogram(x=df["ofi_normalized"], nbinsx=40, marker_color=_C["cyan"]))
        fig.add_vline(x=0.3, line_dash="dash", line_color=_C["up"])
        fig.add_vline(x=-0.3, line_dash="dash", line_color=_C["down"])
        fig.update_layout(**_CHART, height=350, title=dict(text="OFI Distribution", font=dict(size=12)),
                          xaxis=dict(title="OFI Normalized", gridcolor=_C["grid"]),
                          yaxis=dict(gridcolor=_C["grid"]))
        st.plotly_chart(fig, width='stretch')

    # OFI by time of day
    df["hour"] = pd.to_datetime(df["bar_time"]).dt.hour
    hourly = df.groupby("hour").agg(
        mean_ofi=("ofi_normalized", "mean"),
        ofi_next_corr=("ofi_normalized", lambda x: x.corr(df.loc[x.index, "next_return"])),
        bars=("ofi_normalized", "count"),
    ).reset_index()

    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=hourly["hour"], y=hourly["bars"], name="Bars",
                         marker_color=_C["dim"], opacity=0.4), secondary_y=False)
    fig.add_trace(go.Scatter(x=hourly["hour"], y=hourly["ofi_next_corr"], name="OFI→Return Corr",
                             line=dict(color=_C["amber"], width=2), mode="lines+markers"), secondary_y=True)
    fig.update_layout(**_CHART, height=300, title=dict(text="OFI Predictive Power by Hour", font=dict(size=12)))
    fig.update_yaxes(title_text="Bars", gridcolor=_C["grid"], secondary_y=False)
    fig.update_yaxes(title_text="Correlation", gridcolor=_C["grid"], secondary_y=True)
    st.plotly_chart(fig, width='stretch')

    st.markdown("""
    ---
    #### Methodology
    **OFI** = (bidVol - askVol) normalized by total depth. Measures net buying pressure at best bid/ask.

    **Signal rules:**
    - OFI > 0.3 → LONG (buyers dominant, expect price increase)
    - OFI < -0.3 → SHORT (sellers dominant, expect price decrease)
    - Exit: take profit (3%), stop loss (2%), OFI reversal, or max hold (4 bars)

    **Academic basis:** Cont, Kukanov & Stoikov (2014) showed OFI explains ~65% of short-term price changes.
    On PSX with no HFT, OFI signals may persist 15-60 minutes.
    """)
