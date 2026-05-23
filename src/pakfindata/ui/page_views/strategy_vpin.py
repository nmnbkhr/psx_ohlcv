"""VPIN Regime-Switching Strategy — Live signals, backtest, scanner."""

from __future__ import annotations

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pakfindata.ui.components.helpers import render_footer

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "magenta": "#E040FB", "accent": "#2196F3",
}


def render_page():
    st.markdown("### VPIN Regime-Switching Strategy")
    st.caption("Order flow toxicity + Hurst regime detection for PSX")

    tab_live, tab_bt, tab_scan, tab_method = st.tabs([
        "Live Signal", "Backtest", "Scanner", "Methodology"
    ])

    with tab_live:
        _render_live()
    with tab_bt:
        _render_backtest()
    with tab_scan:
        _render_scanner()
    with tab_method:
        _render_methodology()

    render_footer()


# ═══════════════════════════════════════════════════════
# TAB 1: LIVE SIGNAL
# ═══════════════════════════════════════════════════════

def _render_live():
    from pakfindata.engine.vpin_strategy import (
        compute_live_signal, get_tick_dates, compute_vpin_from_ticks,
        load_ticks, load_eod, compute_hurst, classify_hurst_regime,
    )

    col_sym, col_date = st.columns([1, 1])
    with col_sym:
        symbol = st.text_input("Symbol", value="HUBC", key="vpin_sym").upper().strip()
    with col_date:
        dates = get_tick_dates(symbol) if symbol else []
        date_str = st.selectbox("Tick Date", dates if dates else ["No tick data"], key="vpin_date")

    if not symbol or not dates or date_str == "No tick data":
        st.info(f"No tick data for {symbol}. Need JSONL tick logs in DuckDB.")
        return

    with st.spinner("Computing VPIN signal..."):
        sig = compute_live_signal(symbol, date_str)

    if not sig:
        st.warning(f"Could not compute signal for {symbol} on {date_str}")
        return

    # VPIN Gauge
    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        fig = go.Figure(go.Indicator(
            mode="gauge+number",
            value=sig.vpin,
            title={"text": "VPIN Toxicity", "font": {"color": _C["text"], "size": 14}},
            number={"font": {"color": _C["text"], "size": 28}},
            gauge={
                "axis": {"range": [0, 1], "tickcolor": _C["dim"]},
                "bar": {"color": "white", "thickness": 0.3},
                "steps": [
                    {"range": [0, 0.3], "color": "#1B5E20"},
                    {"range": [0.3, 0.5], "color": "#F57F17"},
                    {"range": [0.5, 0.7], "color": "#E65100"},
                    {"range": [0.7, 1.0], "color": "#B71C1C"},
                ],
                "threshold": {"line": {"color": "white", "width": 2}, "thickness": 0.8, "value": sig.vpin},
            },
        ))
        fig.update_layout(paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], height=220, margin=dict(t=40, b=10, l=30, r=30))
        st.plotly_chart(fig, width='stretch')

    with c2:
        state_colors = {
            "SAFE": _C["up"], "ELEVATED": _C["amber"], "WARNING": "#FF6D00",
            "TOXIC": _C["down"], "CLEARING": _C["cyan"],
        }
        sc = state_colors.get(sig.vpin_state.value, _C["dim"])
        st.markdown(f"""
        <div style="background:{_C['card']};padding:20px;border-radius:8px;border-left:4px solid {sc};height:180px;">
            <div style="color:{_C['dim']};font-size:0.75em;text-transform:uppercase;">VPIN State</div>
            <div style="color:{sc};font-size:2em;font-weight:700;">{sig.vpin_state.value}</div>
            <div style="color:{_C['dim']};font-size:0.75em;text-transform:uppercase;margin-top:16px;">Hurst Regime</div>
            <div style="color:{_C['text']};font-size:1.3em;font-weight:600;">
                {sig.hurst_regime.value} <span style="color:{_C['dim']};font-size:0.7em;">(H={sig.hurst:.3f})</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    with c3:
        sig_colors = {"BUY": _C["up"], "SELL": _C["down"], "HOLD": _C["dim"], "EXIT": _C["down"], "REDUCE": _C["amber"]}
        sigc = sig_colors.get(sig.signal, _C["dim"])
        st.markdown(f"""
        <div style="background:{_C['card']};padding:20px;border-radius:8px;border-left:4px solid {sigc};height:180px;">
            <div style="color:{_C['dim']};font-size:0.75em;text-transform:uppercase;">Signal</div>
            <div style="color:{sigc};font-size:2.5em;font-weight:700;">{sig.signal}</div>
            <div style="margin-top:8px;">
                <span style="color:{_C['dim']};font-size:0.75em;">Confidence:</span>
                <span style="color:{_C['text']};font-weight:600;"> {sig.confidence:.0%}</span>
                <span style="color:{_C['dim']};font-size:0.75em;margin-left:12px;">Size:</span>
                <span style="color:{_C['text']};font-weight:600;"> {sig.position_size:.0%}</span>
            </div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown(f"**Reason:** {sig.reason}")

    # VPIN time series chart
    st.markdown("---")
    st.markdown("#### Intraday VPIN Evolution")
    ticks = load_ticks(symbol, date_str)
    if len(ticks) > 50:
        vpin_df = compute_vpin_from_ticks(ticks, n_buckets=50)
        if not vpin_df.empty:
            fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.6, 0.4], vertical_spacing=0.05)
            fig.add_trace(go.Scatter(x=vpin_df["timestamp"], y=vpin_df["price"], name="Price",
                                     line=dict(color=_C["text"], width=1.5)), row=1, col=1)
            fig.add_trace(go.Scatter(x=vpin_df["timestamp"], y=vpin_df["vpin"], name="VPIN",
                                     line=dict(color=_C["cyan"], width=2), fill="tozeroy",
                                     fillcolor="rgba(0,188,212,0.1)"), row=2, col=1)
            fig.add_hline(y=0.7, line_dash="dash", line_color=_C["down"], annotation_text="TOXIC", row=2, col=1)
            fig.add_hline(y=0.3, line_dash="dash", line_color=_C["up"], annotation_text="SAFE", row=2, col=1)
            fig.update_layout(
                paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], font_color=_C["text"],
                height=400, margin=dict(t=20, b=20, l=50, r=20), showlegend=False,
                xaxis2=dict(gridcolor=_C["grid"]),
                yaxis=dict(gridcolor=_C["grid"], title="Price"),
                yaxis2=dict(gridcolor=_C["grid"], title="VPIN", range=[0, 1]),
            )
            st.plotly_chart(fig, width='stretch')


# ═══════════════════════════════════════════════════════
# TAB 2: BACKTEST
# ═══════════════════════════════════════════════════════

def _render_backtest():
    from pakfindata.engine.vpin_strategy import backtest_vpin_strategy

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        symbol = st.text_input("Symbol", value="HUBC", key="bt_sym").upper().strip()
    with c2:
        lookback = st.selectbox("Lookback", [250, 500, 1000], index=0, key="bt_lb")
    with c3:
        run = st.button("Run Backtest", type="primary", key="bt_run")

    if not run:
        st.info("Select a symbol and click Run Backtest.")
        return

    with st.spinner(f"Backtesting {symbol} ({lookback} days)..."):
        result = backtest_vpin_strategy(symbol, lookback_days=lookback)

    if "error" in result:
        st.error(result["error"])
        return

    m = result["metrics"]
    eq = result["equity_curve"]
    trades = result["trades"]

    # Metrics row
    mc = st.columns(6)
    labels = ["Return", "Ann. Return", "Sharpe", "Max DD", "Win Rate", "Alpha"]
    values = [
        f"{m['total_return']:.2%}", f"{m['annualized_return']:.2%}",
        f"{m['sharpe_ratio']:.2f}", f"{m['max_drawdown']:.2%}",
        f"{m['win_rate']:.0%}", f"{m['alpha']:+.2%}",
    ]
    colors = [
        _C["up"] if m["total_return"] > 0 else _C["down"],
        _C["up"] if m["annualized_return"] > 0 else _C["down"],
        _C["up"] if m["sharpe_ratio"] > 1 else _C["amber"] if m["sharpe_ratio"] > 0 else _C["down"],
        _C["down"], _C["up"] if m["win_rate"] > 0.5 else _C["down"],
        _C["up"] if m["alpha"] > 0 else _C["down"],
    ]
    for i, col in enumerate(mc):
        with col:
            st.markdown(f"""
            <div style="background:{_C['card']};padding:12px;border-radius:6px;text-align:center;">
                <div style="color:{_C['dim']};font-size:0.7em;text-transform:uppercase;">{labels[i]}</div>
                <div style="color:{colors[i]};font-size:1.4em;font-weight:700;">{values[i]}</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown(f"Trades: {m['total_trades']} | B&H: {m['buy_hold_return']:.2%}")

    if eq.empty:
        st.warning("No equity curve data.")
        return

    # Equity curve
    fig = make_subplots(rows=3, cols=1, shared_xaxes=True, row_heights=[0.5, 0.25, 0.25], vertical_spacing=0.05)

    # Equity vs buy-and-hold
    bh_equity = 1_000_000 * (eq["close"] / eq["close"].iloc[0])
    fig.add_trace(go.Scatter(x=eq["date"], y=eq["equity"], name="Strategy",
                             line=dict(color=_C["accent"], width=2)), row=1, col=1)
    fig.add_trace(go.Scatter(x=eq["date"], y=bh_equity, name="Buy & Hold",
                             line=dict(color=_C["dim"], width=1, dash="dot")), row=1, col=1)

    # VPIN overlay
    fig.add_trace(go.Scatter(x=eq["date"], y=eq["vpin"], name="VPIN",
                             line=dict(color=_C["cyan"], width=1),
                             fill="tozeroy", fillcolor="rgba(0,188,212,0.1)"), row=2, col=1)
    fig.add_hline(y=0.7, line_dash="dash", line_color=_C["down"], row=2, col=1)

    # Hurst
    fig.add_trace(go.Scatter(x=eq["date"], y=eq["hurst"], name="Hurst",
                             line=dict(color=_C["amber"], width=1)), row=3, col=1)
    fig.add_hline(y=0.55, line_dash="dash", line_color=_C["up"], row=3, col=1)
    fig.add_hline(y=0.45, line_dash="dash", line_color=_C["down"], row=3, col=1)

    fig.update_layout(
        paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"], font_color=_C["text"],
        height=600, margin=dict(t=20, b=20, l=50, r=20),
        legend=dict(orientation="h", y=1.05, bgcolor="rgba(0,0,0,0)"),
        yaxis=dict(gridcolor=_C["grid"], title="Equity (PKR)"),
        yaxis2=dict(gridcolor=_C["grid"], title="VPIN", range=[0, 1]),
        yaxis3=dict(gridcolor=_C["grid"], title="Hurst", range=[0.2, 0.8]),
    )
    st.plotly_chart(fig, width='stretch')

    # Trade log
    if not trades.empty:
        st.markdown("#### Trade Log")
        st.dataframe(trades, width='stretch', hide_index=True, height=300)


# ═══════════════════════════════════════════════════════
# TAB 3: SCANNER
# ═══════════════════════════════════════════════════════

def _render_scanner():
    from pakfindata.engine.vpin_strategy import scan_signals

    c1, c2 = st.columns([2, 1])
    with c1:
        top_n = st.slider("Top N symbols", 10, 100, 30, key="scan_n")
    with c2:
        run = st.button("Scan", type="primary", key="scan_run")

    if not run:
        st.info("Click Scan to compute VPIN signals for the most liquid symbols.")
        return

    with st.spinner(f"Scanning top {top_n} symbols..."):
        results = scan_signals(top_n=top_n)

    if not results:
        st.warning("No signals generated. Check tick data availability.")
        return

    df = pd.DataFrame(results)
    st.markdown(f"**{len(df)} symbols scanned** — latest tick date: {df['date'].iloc[0]}")

    # Color the state column
    def _color_state(val):
        colors = {"SAFE": "#1B5E20", "ELEVATED": "#F57F17", "WARNING": "#E65100", "TOXIC": "#B71C1C", "CLEARING": "#00838F"}
        return f"background-color: {colors.get(val, '#333')}"

    def _color_signal(val):
        colors = {"BUY": "#1B5E20", "SELL": "#B71C1C", "HOLD": "#333", "EXIT": "#B71C1C", "REDUCE": "#E65100"}
        return f"background-color: {colors.get(val, '#333')}"

    display_cols = ["symbol", "vpin", "vpin_state", "hurst", "hurst_regime", "signal", "confidence", "position_size", "reason"]
    show_df = df[display_cols].copy()
    show_df["vpin"] = show_df["vpin"].map(lambda x: f"{x:.3f}")
    show_df["hurst"] = show_df["hurst"].map(lambda x: f"{x:.3f}")
    show_df["confidence"] = show_df["confidence"].map(lambda x: f"{x:.0%}")
    show_df["position_size"] = show_df["position_size"].map(lambda x: f"{x:.0%}")

    styled = show_df.style.map(_color_state, subset=["vpin_state"]).map(_color_signal, subset=["signal"])
    st.dataframe(styled, width='stretch', hide_index=True, height=500)


# ═══════════════════════════════════════════════════════
# TAB 4: METHODOLOGY
# ═══════════════════════════════════════════════════════

def _render_methodology():
    st.markdown("""
#### VPIN (Volume-Synchronized Probability of Informed Trading)

VPIN measures the probability that trading activity is driven by informed traders rather than
noise/retail flow. It works by:

1. **Volume bucketing** — divide trades into equal-volume buckets (not time-based)
2. **Buy/sell classification** — use Bulk Volume Classification (BVC) based on price change within each bucket
3. **Order imbalance** — compute |Buy Volume - Sell Volume| per bucket
4. **Rolling VPIN** — average imbalance over N buckets, normalized by total volume

**PSX edge:** On mature markets (NYSE, LSE), VPIN signals last microseconds. On PSX with near-zero
algo penetration, a VPIN spike at 11 AM can still be actionable at 2 PM.

---

#### Hurst Exponent (R/S Analysis)

The Hurst exponent H determines the regime:
- **H > 0.55 → Trending** — prices persist in direction (use momentum)
- **H < 0.45 → Mean-reverting** — prices oscillate around a mean (fade extremes)
- **H ≈ 0.50 → Random walk** — no predictable pattern (reduce size)

Computed using Rescaled Range (R/S) analysis on the last 100 trading days.

---

#### Signal Matrix

| VPIN State | Hurst Regime | Action | Position Size |
|---|---|---|---|
| **TOXIC** (>0.7) | Any | EXIT | 0% |
| **WARNING** (0.5-0.7) | Any | REDUCE | 25% |
| **CLEARING** (dropping from toxic) | Trending + trend aligned | BUY/SELL | 70% |
| **CLEARING** | Mean-reverting + oversold/overbought | BUY/SELL (fade) | 60% |
| **CLEARING** | Random walk | HOLD | 15% |
| **SAFE** (<0.3) | Trending + strong OFI | BUY/SELL | 50% |
| **SAFE** | Other | HOLD | 20-30% |

---

#### Key Insight: The CLEARING Signal

The most valuable signal occurs when VPIN **drops** from TOXIC back toward SAFE.
This means:
- Informed traders have finished their block
- The market has absorbed the toxic flow
- A new equilibrium is forming

This is the entry point — combine with Hurst to determine direction.

---

#### Risk Management
- Circuit breaker awareness: PSX has ±7.5% daily limits
- Position sizing is signal-dependent (0% to 70%)
- TOXIC state always forces exit regardless of P&L
- No trades in RANDOM_WALK regime with SAFE VPIN
    """)
