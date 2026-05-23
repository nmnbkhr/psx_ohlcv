"""CVD Divergence Strategy — Price vs volume delta divergence detection."""

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
    st.markdown("### CVD Divergence Strategy")
    st.caption("Cumulative Volume Delta — detect accumulation/distribution divergences")

    tab_live, tab_scan, tab_method = st.tabs(["Live CVD", "Scanner", "Methodology"])

    with tab_live:
        _render_live()
    with tab_scan:
        _render_scanner()
    with tab_method:
        _render_methodology()

    render_footer()


def _render_live():
    from pakfindata.engine.cvd_strategy import analyze_cvd, get_tick_dates

    c1, c2 = st.columns([1, 1])
    with c1:
        symbol = st.text_input("Symbol", value="HUBC", key="cvd_sym").upper().strip()
    with c2:
        dates = get_tick_dates(symbol) if symbol else []
        date_str = st.selectbox("Date", dates if dates else ["No data"], key="cvd_date")

    if not dates or date_str == "No data":
        st.info(f"No tick data for {symbol}")
        return

    with st.spinner("Computing CVD..."):
        result = analyze_cvd(symbol, date_str)

    if not result:
        st.warning(f"Not enough ticks for {symbol} on {date_str}")
        return

    # KPIs
    mc = st.columns(6)
    cvd = result["cvd_final"]
    slope = result["cvd_slope"]
    ratio = result["buy_sell_ratio"]
    divs = result["divergences"]

    with mc[0]:
        c = _C["up"] if cvd > 0 else _C["down"]
        _kpi("CVD Final", f"{cvd / 1e6:+,.1f}M", c)
    with mc[1]:
        c = _C["up"] if slope > 0 else _C["down"]
        _kpi("CVD Slope", f"{slope:+,.0f}", c)
    with mc[2]:
        c = _C["up"] if ratio > 0.55 else _C["down"] if ratio < 0.45 else _C["dim"]
        _kpi("Buy/Sell", f"{ratio:.0%} / {1 - ratio:.0%}", c)
    with mc[3]:
        _kpi("Buy Vol", f"{result['buy_volume'] / 1e6:,.0f}M", _C["up"])
    with mc[4]:
        _kpi("Sell Vol", f"{result['sell_volume'] / 1e6:,.0f}M", _C["down"])
    with mc[5]:
        dc = _C["amber"] if divs else _C["dim"]
        _kpi("Divergences", str(len(divs)), dc)

    # Price + CVD chart
    bars = result["bars"]
    if not bars.empty:
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.55, 0.45], vertical_spacing=0.05)

        # Price candlestick-like
        fig.add_trace(go.Scatter(x=bars["bar_time"], y=bars["close"], name="Price",
                                 line=dict(color=_C["text"], width=1.5)), row=1, col=1)

        # CVD line
        cvd_color = _C["up"] if bars["cvd"].iloc[-1] > bars["cvd"].iloc[0] else _C["down"]
        fig.add_trace(go.Scatter(x=bars["bar_time"], y=bars["cvd"], name="CVD",
                                 line=dict(color=cvd_color, width=2),
                                 fill="tozeroy", fillcolor=f"rgba({','.join(str(int(cvd_color.lstrip('#')[i:i+2], 16)) for i in (0, 2, 4))},0.1)"),
                      row=2, col=1)
        fig.add_hline(y=0, line_color=_C["dim"], line_dash="dash", row=2, col=1)

        # Mark divergences
        for d in divs:
            dt = d["detected_at"]
            color = _C["down"] if d["signal"] == "SELL" else _C["up"]
            fig.add_vline(x=dt, line_color=color, line_dash="dot", opacity=0.7, row=1, col=1)
            fig.add_vline(x=dt, line_color=color, line_dash="dot", opacity=0.7, row=2, col=1)

        fig.update_layout(**_CHART, height=500, showlegend=True,
                          legend=dict(orientation="h", y=1.05, bgcolor="rgba(0,0,0,0)"),
                          yaxis=dict(gridcolor=_C["grid"], title="Price"),
                          yaxis2=dict(gridcolor=_C["grid"], title="CVD"))
        st.plotly_chart(fig, width='stretch')

    # Divergence details
    if divs:
        st.markdown("#### Detected Divergences")
        for d in divs:
            color = _C["down"] if d["signal"] == "SELL" else _C["up"]
            st.markdown(f"""
            <div style="background:{_C['card']};padding:12px;border-radius:6px;border-left:4px solid {color};margin-bottom:8px;">
                <span style="color:{color};font-weight:700;">{d['div_type']}</span> — {d['signal']}
                ({d['confidence']:.0%} confidence)<br>
                <span style="color:{_C['dim']};font-size:0.85em;">{d['reason']}</span>
            </div>
            """, unsafe_allow_html=True)
    else:
        st.info("No divergences detected — price and CVD are aligned.")

    # Volume breakdown
    cvd_series = result.get("cvd_series")
    if cvd_series is not None and not cvd_series.empty and "direction" in cvd_series.columns:
        buy_pct = (cvd_series["direction"] == "BUY").mean() * 100
        st.markdown(f"**Tick classification:** {buy_pct:.1f}% buy ticks / {100 - buy_pct:.1f}% sell ticks ({len(cvd_series):,} total)")


def _render_scanner():
    from pakfindata.engine.cvd_strategy import scan_divergences

    c1, c2 = st.columns([2, 1])
    with c1:
        top_n = st.slider("Symbols", 10, 80, 30, key="cvd_scan_n")
    with c2:
        run = st.button("Scan", type="primary", key="cvd_scan_run")

    if not run:
        st.info("Click Scan to find symbols with CVD divergences.")
        return

    with st.spinner(f"Scanning top {top_n} symbols..."):
        results = scan_divergences(top_n=top_n)

    if not results:
        st.warning("No results.")
        return

    df = pd.DataFrame(results)
    st.markdown(f"**{len(df)} symbols** scanned on {df['date'].iloc[0]}")

    # Highlight divergences
    has_div = df[df["divergences"] > 0]
    no_div = df[df["divergences"] == 0]

    if not has_div.empty:
        st.markdown(f"**{len(has_div)} symbols with divergences:**")
        show = has_div[["symbol", "ticks", "cvd_final", "cvd_slope", "buy_sell_ratio", "divergences", "div_types", "signal", "confidence"]].copy()
        show["cvd_final"] = show["cvd_final"].map(lambda x: f"{x / 1e6:+,.1f}M")
        show["cvd_slope"] = show["cvd_slope"].map(lambda x: f"{x:+,.0f}")
        show["buy_sell_ratio"] = show["buy_sell_ratio"].map(lambda x: f"{x:.0%}")
        show["confidence"] = show["confidence"].map(lambda x: f"{x:.0%}")
        st.dataframe(show, width='stretch', hide_index=True)

    st.markdown(f"**{len(no_div)} symbols aligned** (no divergence)")
    # Show top by CVD slope (strongest buyers/sellers)
    top_buy = no_div.nlargest(5, "cvd_slope")
    top_sell = no_div.nsmallest(5, "cvd_slope")

    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**Strongest net buying (CVD slope)**")
        show = top_buy[["symbol", "cvd_slope", "buy_sell_ratio"]].copy()
        show["cvd_slope"] = show["cvd_slope"].map(lambda x: f"{x:+,.0f}")
        show["buy_sell_ratio"] = show["buy_sell_ratio"].map(lambda x: f"{x:.0%}")
        st.dataframe(show, width='stretch', hide_index=True)
    with c2:
        st.markdown("**Strongest net selling (CVD slope)**")
        show = top_sell[["symbol", "cvd_slope", "buy_sell_ratio"]].copy()
        show["cvd_slope"] = show["cvd_slope"].map(lambda x: f"{x:+,.0f}")
        show["buy_sell_ratio"] = show["buy_sell_ratio"].map(lambda x: f"{x:.0%}")
        st.dataframe(show, width='stretch', hide_index=True)


def _render_methodology():
    st.markdown("""
#### Cumulative Volume Delta (CVD)

CVD is the running sum of (buy volume - sell volume) across all ticks.

**Tick classification:**
1. Price >= ask -> BUY (aggressive buyer lifting the offer)
2. Price <= bid -> SELL (aggressive seller hitting the bid)
3. Price between bid/ask -> classified by proximity to midpoint
4. Fallback: use price change direction

---

#### Divergence Types

| Type | Price | CVD | Meaning | Signal |
|---|---|---|---|---|
| **Bearish** | Higher high | Lower high | Smart money distributing | SELL |
| **Bullish** | Lower low | Higher low | Smart money accumulating | BUY |
| Hidden Bearish | Lower high | Higher high | Sellers absorbing demand | SELL |
| Hidden Bullish | Higher low | Lower low | Buyers absorbing supply | BUY |

---

#### PSX Edge

Institutional accumulation/distribution on PSX takes **days** due to low liquidity.
CVD captures this invisible flow before price reflects it. On NYSE this would be
arbitraged in milliseconds; on PSX the signal persists for hours.

---

#### Implementation
- Tick data from DuckDB `tick_logs` (bid/ask/bidVol/askVol per tick)
- Resampled to 5-minute bars for swing point detection
- Swing window: 10 bars (50 minutes) to detect pivots
- Minimum price move: 0.3% between pivots to filter noise
    """)
