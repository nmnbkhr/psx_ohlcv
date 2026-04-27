"""Futures Basis Mean-Reversion Strategy — basis spread trading."""

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
    st.markdown("### Futures Basis Mean-Reversion")
    st.caption("Trade the futures-spot spread when it deviates beyond normal range")

    tab_live, tab_bt, tab_scan, tab_method = st.tabs(["Live Basis", "Backtest", "Scanner", "Methodology"])

    with tab_live:
        _render_live()
    with tab_bt:
        _render_backtest()
    with tab_scan:
        _render_scanner()
    with tab_method:
        _render_methodology()

    render_footer()


def _render_live():
    from pakfindata.engine.basis_strategy import load_basis_history, generate_basis_signal, get_active_futures_symbols

    symbols = get_active_futures_symbols(min_dates=20)
    if not symbols:
        st.warning("No symbols with both spot and futures data")
        return

    symbol = st.selectbox("Base Symbol", symbols, key="basis_sym")
    if not symbol:
        return

    with st.spinner("Loading basis data..."):
        df = load_basis_history(symbol, lookback=300)

    if df.empty:
        st.warning(f"No basis data for {symbol}")
        return

    sig = generate_basis_signal(df, symbol)

    # KPIs
    mc = st.columns(6)
    latest = df.iloc[-1]
    with mc[0]:
        _kpi("Spot", f"{latest['spot']:.2f}")
    with mc[1]:
        _kpi("Futures", f"{latest['futures']:.2f}")
    with mc[2]:
        bc = _C["up"] if latest["basis_pct"] > 0 else _C["down"]
        _kpi("Basis", f"{latest['basis_pct']:+.2f}%", bc)
    with mc[3]:
        zc = _C["down"] if abs(latest["basis_zscore"]) > 2 else _C["amber"] if abs(latest["basis_zscore"]) > 1 else _C["dim"]
        _kpi("Z-Score", f"{latest['basis_zscore']:+.2f}", zc)
    with mc[4]:
        if sig:
            sc = _C["up"] if "BUY" in sig.signal else _C["down"] if "SELL" in sig.signal else _C["dim"]
            _kpi("Signal", sig.signal, sc)
        else:
            _kpi("Signal", "N/A")
    with mc[5]:
        _kpi("Days", str(len(df)))

    if sig:
        st.markdown(f"**{sig.reason}**")

    # Basis chart
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.4, 0.35, 0.25], vertical_spacing=0.05)

    # Price
    fig.add_trace(go.Scatter(x=df["date"], y=df["spot"], name="Spot", line=dict(color=_C["text"], width=1.5)), row=1, col=1)
    fig.add_trace(go.Scatter(x=df["date"], y=df["futures"], name="Futures", line=dict(color=_C["cyan"], width=1.5)), row=1, col=1)

    # Basis %
    colors = [_C["up"] if x > 0 else _C["down"] for x in df["basis_pct"]]
    fig.add_trace(go.Bar(x=df["date"], y=df["basis_pct"], name="Basis %", marker_color=colors), row=2, col=1)
    if "fair_basis" in df.columns:
        fig.add_trace(go.Scatter(x=df["date"], y=df["fair_basis"], name="Fair Basis",
                                 line=dict(color=_C["amber"], width=1, dash="dash")), row=2, col=1)

    # Z-score
    fig.add_trace(go.Scatter(x=df["date"], y=df["basis_zscore"], name="Z-Score",
                             line=dict(color=_C["accent"], width=2)), row=3, col=1)
    fig.add_hline(y=2.0, line_dash="dash", line_color=_C["down"], row=3, col=1)
    fig.add_hline(y=-2.0, line_dash="dash", line_color=_C["up"], row=3, col=1)
    fig.add_hline(y=0, line_color=_C["dim"], row=3, col=1)

    fig.update_layout(**_CHART, height=550, legend=dict(orientation="h", y=1.05, bgcolor="rgba(0,0,0,0)"),
                      yaxis=dict(gridcolor=_C["grid"], title="Price"),
                      yaxis2=dict(gridcolor=_C["grid"], title="Basis %"),
                      yaxis3=dict(gridcolor=_C["grid"], title="Z-Score"))
    st.plotly_chart(fig, width='stretch')


def _render_backtest():
    from pakfindata.engine.basis_strategy import backtest_basis_strategy, get_active_futures_symbols

    symbols = get_active_futures_symbols(min_dates=20)
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        symbol = st.selectbox("Symbol", symbols if symbols else ["N/A"], key="basis_bt_sym")
    with c2:
        entry_z = st.slider("Entry Z", 1.0, 3.0, 2.0, 0.25, key="basis_bt_ez")
    with c3:
        run = st.button("Run Backtest", type="primary", key="basis_bt_run")

    if not run or not symbol or symbol == "N/A":
        st.info("Select a symbol and click Run Backtest.")
        return

    with st.spinner(f"Backtesting {symbol}..."):
        result = backtest_basis_strategy(symbol, lookback=1000, entry_z=entry_z)

    if "error" in result:
        st.warning(result["error"])
        # Still show basis chart if available
        basis_df = result.get("basis_df")
        if basis_df is not None and not basis_df.empty:
            st.markdown(f"Basis data: {len(basis_df)} days available")
        return

    m = result["metrics"]
    mc = st.columns(6)
    labels = ["Trades", "Win Rate", "Profit Factor", "Return", "Sharpe", "Avg Hold"]
    values = [
        str(m["total_trades"]), f"{m['win_rate']:.0%}", f"{m['profit_factor']:.2f}",
        f"{m['total_return']:+.2%}", f"{m['sharpe_ratio']:.2f}", f"{m['avg_hold_days']:.0f}d",
    ]
    colors = [
        _C["text"], _C["up"] if m["win_rate"] > 0.5 else _C["down"],
        _C["up"] if m["profit_factor"] > 1 else _C["down"],
        _C["up"] if m["total_return"] > 0 else _C["down"],
        _C["up"] if m["sharpe_ratio"] > 0 else _C["down"], _C["text"],
    ]
    for i, col in enumerate(mc):
        with col:
            _kpi(labels[i], values[i], colors[i])

    trades = result["trades"]
    if not trades.empty:
        # Equity curve
        eq = result["equity"]
        fig = go.Figure()
        fig.add_trace(go.Scatter(y=eq, name="Equity", line=dict(color=_C["accent"], width=2)))
        fig.add_hline(y=1.0, line_dash="dash", line_color=_C["dim"])
        fig.update_layout(**_CHART, height=250, yaxis=dict(gridcolor=_C["grid"], title="Equity"))
        st.plotly_chart(fig, width='stretch')

        with st.expander("Trade Log"):
            st.dataframe(trades, width='stretch', hide_index=True)


def _render_scanner():
    from pakfindata.engine.basis_strategy import scan_basis_signals

    c1, c2 = st.columns([2, 1])
    with c1:
        top_n = st.slider("Symbols", 10, 100, 30, key="basis_scan_n")
    with c2:
        run = st.button("Scan", type="primary", key="basis_scan_run")

    if not run:
        st.info("Click Scan to find extreme basis deviations.")
        return

    with st.spinner(f"Scanning top {top_n} symbols..."):
        results = scan_basis_signals(min_dates=20, top_n=top_n)

    if not results:
        st.warning("No results.")
        return

    df = pd.DataFrame(results)
    st.markdown(f"**{len(df)} symbols** scanned")

    # Highlight extremes
    extreme = df[df["signal"] != "HOLD"]
    normal = df[df["signal"] == "HOLD"]

    if not extreme.empty:
        st.markdown(f"**{len(extreme)} signals:**")
        show = extreme[["symbol", "spot_price", "futures_price", "basis_pct", "basis_zscore", "signal", "confidence", "reason"]].copy()
        show["basis_pct"] = show["basis_pct"].map(lambda x: f"{x:+.2f}%")
        show["basis_zscore"] = show["basis_zscore"].map(lambda x: f"{x:+.2f}")
        show["confidence"] = show["confidence"].map(lambda x: f"{x:.0%}")
        st.dataframe(show, width='stretch', hide_index=True)

    if not normal.empty:
        with st.expander(f"{len(normal)} symbols in normal range"):
            show = normal[["symbol", "basis_pct", "basis_zscore"]].copy()
            show["basis_pct"] = show["basis_pct"].map(lambda x: f"{x:+.2f}%")
            show["basis_zscore"] = show["basis_zscore"].map(lambda x: f"{x:+.2f}")
            st.dataframe(show, width='stretch', hide_index=True)


def _render_methodology():
    st.markdown("""
#### Futures Basis Mean-Reversion

**Basis** = (Futures - Spot) / Spot x 100%

The basis reflects the market's expectation of future price movement plus carrying costs.
Fair basis = risk-free rate x days-to-expiry / 365.

**The structural edge:** Futures MUST converge to spot at settlement. When basis deviates
beyond 2 standard deviations from its rolling mean, mean-reversion is highly probable.

---

#### Signal Rules

| Condition | Signal | Action |
|---|---|---|
| Z-score > +2.0 | SELL_BASIS | Sell futures, buy spot (premium too wide) |
| Z-score < -2.0 | BUY_BASIS | Buy futures, sell spot (discount too deep) |
| Z-score crosses ±0.5 | EXIT | Close position (mean-reverted) |
| Z-score > ±4.0 | STOP | Force exit (basis breakdown) |

---

#### PSX-Specific

- **KIBOR** as risk-free rate proxy for fair basis calculation
- **Low liquidity** = basis overshoots more = more opportunities
- **Physical delivery** at settlement forces convergence
- **Rollover** ~3 days before expiry creates entry opportunities
- **Circuit breakers** ±7.5% on spot can cause temporary basis gaps
    """)
