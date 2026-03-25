"""Sector Rotation Momentum Strategy — monthly rebalance across PSX sectors."""

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
    st.markdown("### Sector Rotation Momentum")
    st.caption("Monthly rebalance — long top sectors, underweight bottom sectors")

    tab_rank, tab_bt, tab_method = st.tabs(["Current Rankings", "Backtest", "Methodology"])

    with tab_rank:
        _render_rankings()
    with tab_bt:
        _render_backtest()
    with tab_method:
        _render_methodology()

    render_footer()


def _render_rankings():
    from pakfindata.engine.sector_rotation import rank_sectors

    with st.spinner("Computing sector momentum..."):
        signals = rank_sectors(lookback_months=12)

    if not signals:
        st.warning("Not enough sector data")
        return

    st.markdown(f"**{len(signals)} sectors** ranked by 1-month momentum")

    # Top 3 and Bottom 3
    top = signals[:3]
    bottom = signals[-3:]

    c1, c2 = st.columns(2)
    with c1:
        st.markdown(f'<div style="color:{_C["up"]};font-weight:700;">OVERWEIGHT (Top 3)</div>', unsafe_allow_html=True)
        for s in top:
            st.markdown(f"""
            <div style="background:{_C['card']};padding:10px;border-radius:6px;border-left:4px solid {_C['up']};margin-bottom:6px;">
                <span style="color:{_C['text']};font-weight:600;">#{s.momentum_rank} {s.sector_name}</span>
                <span style="color:{_C['up']};float:right;">{s.return_1m:+.1%}</span>
            </div>
            """, unsafe_allow_html=True)

    with c2:
        st.markdown(f'<div style="color:{_C["down"]};font-weight:700;">UNDERWEIGHT (Bottom 3)</div>', unsafe_allow_html=True)
        for s in bottom:
            st.markdown(f"""
            <div style="background:{_C['card']};padding:10px;border-radius:6px;border-left:4px solid {_C['down']};margin-bottom:6px;">
                <span style="color:{_C['text']};font-weight:600;">#{s.momentum_rank} {s.sector_name}</span>
                <span style="color:{_C['down']};float:right;">{s.return_1m:+.1%}</span>
            </div>
            """, unsafe_allow_html=True)

    # Full rankings chart
    df = pd.DataFrame([s.to_dict() for s in signals])
    colors = [_C["up"] if r < 0.33 else _C["down"] if r > 0.66 else _C["dim"]
              for r in np.linspace(0, 1, len(df))]

    fig = go.Figure(go.Bar(
        y=df["sector_name"], x=df["return_1m"] * 100,
        orientation="h", marker_color=colors,
        text=[f"{r:+.1f}%" for r in df["return_1m"] * 100],
        textposition="auto",
    ))
    fig.update_layout(**_CHART, height=max(400, len(df) * 22),
                      xaxis=dict(title="1M Return %", gridcolor=_C["grid"]),
                      yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

    # Table
    with st.expander("Full Rankings Table"):
        show = df[["momentum_rank", "sector_name", "return_1m", "return_3m", "signal"]].copy()
        show["return_1m"] = show["return_1m"].map(lambda x: f"{x:+.2%}")
        show["return_3m"] = show["return_3m"].map(lambda x: f"{x:+.2%}")
        show.columns = ["Rank", "Sector", "1M Return", "3M Return", "Signal"]
        st.dataframe(show, use_container_width=True, hide_index=True)


def _render_backtest():
    from pakfindata.engine.sector_rotation import backtest_sector_rotation

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        lookback = st.selectbox("Lookback", [24, 36, 48, 60], index=3, key="sr_bt_lb")
    with c2:
        top_n = st.slider("Top N sectors", 1, 5, 3, key="sr_bt_top")
    with c3:
        run = st.button("Run Backtest", type="primary", key="sr_bt_run")

    if not run:
        st.info("Configure and click Run Backtest.")
        return

    with st.spinner(f"Backtesting {lookback} months..."):
        result = backtest_sector_rotation(lookback_months=lookback, top_n=top_n, bottom_n=top_n)

    if "error" in result:
        st.error(result["error"])
        return

    m = result["metrics"]

    mc = st.columns(6)
    labels = ["Strategy Ret", "B&H Ret", "Alpha", "Sharpe", "Max DD", "Win Rate"]
    values = [
        f"{m['strategy_return']:+.1%}", f"{m['bh_return']:+.1%}", f"{m['alpha']:+.1%}",
        f"{m['strategy_sharpe']:.2f}", f"{m['strategy_max_dd']:.1%}", f"{m['win_rate']:.0%}",
    ]
    colors = [
        _C["up"] if m["strategy_return"] > 0 else _C["down"],
        _C["up"] if m["bh_return"] > 0 else _C["down"],
        _C["up"] if m["alpha"] > 0 else _C["down"],
        _C["up"] if m["strategy_sharpe"] > 0.5 else _C["down"],
        _C["down"], _C["up"] if m["win_rate"] > 0.5 else _C["down"],
    ]
    for i, col in enumerate(mc):
        with col:
            _kpi(labels[i], values[i], colors[i])

    # Equity curve
    fig = go.Figure()
    fig.add_trace(go.Scatter(y=result["equity"], name="Rotation Strategy",
                             line=dict(color=_C["accent"], width=2)))
    fig.add_trace(go.Scatter(y=result["bh_equity"], name="Equal Weight B&H",
                             line=dict(color=_C["dim"], width=1, dash="dot")))
    fig.update_layout(**_CHART, height=350,
                      legend=dict(orientation="h", y=1.08, bgcolor="rgba(0,0,0,0)"),
                      yaxis=dict(gridcolor=_C["grid"], title="Equity"))
    st.plotly_chart(fig, use_container_width=True)

    # Monthly trades
    trades = result["trades"]
    if not trades.empty:
        with st.expander("Monthly Rotation Log"):
            show = trades[["month", "long", "short", "strategy_ret", "bh_ret"]].copy()
            show["long"] = show["long"].map(lambda x: ", ".join(x[:3]))
            show["short"] = show["short"].map(lambda x: ", ".join(x[:3]))
            show["strategy_ret"] = show["strategy_ret"].map(lambda x: f"{x:+.2%}")
            show["bh_ret"] = show["bh_ret"].map(lambda x: f"{x:+.2%}")
            show.columns = ["Month", "Long Sectors", "Short Sectors", "Strategy", "B&H"]
            st.dataframe(show, use_container_width=True, hide_index=True, height=400)


def _render_methodology():
    st.markdown("""
#### Sector Rotation Momentum

**Logic:** Each month, rank all 28 PSX sectors by trailing 1-month return.
Go long the top 3 (strongest momentum), underweight the bottom 3 (weakest).

**Rebalance:** Monthly — simple, low-turnover, actionable.

---

#### Pakistan's Sector Rotation Cycle

| Trigger | Winners | Losers |
|---|---|---|
| SBP rate cuts | Banks, Cement, Auto | — |
| PKR stability | Pharma, Tech | — |
| Oil price drop | Refineries, Power | E&P, Gas Marketing |
| Fiscal stimulus | Cement, Engineering, Steel | — |
| Textile export cycle | Textile Composite | — |

These rotations persist 3-6 months — much longer than in developed markets.

---

#### Why It Works on PSX

1. **Macro-driven:** Pakistan's sector cycles follow rate/FX/commodity cycles
2. **Persistent:** Low institutional arbitrage means momentum continues
3. **Observable:** SBP policy, oil prices, PKR are all public data
4. **Simple:** Monthly rebalance avoids overtrading
    """)
