"""Pairs Trading (Statistical Arbitrage) -- Streamlit page."""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
import streamlit.components.v1 as components

from pakfindata.engine.pairs_trading import (
    KNOWN_PAIR_CANDIDATES,
    backtest_pairs_strategy,
    find_cointegrated_pairs,
    generate_pairs_signal,
    kalman_hedge_ratio,
    load_pair_prices,
    scan_pair_opportunities,
    test_cointegration,
)

DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
    font_color="#c9d1d9", margin=dict(l=20, r=20, t=40, b=20),
)

SIGNAL_COLORS = {
    "LONG_SPREAD": "#22c55e", "SHORT_SPREAD": "#ef4444",
    "EXIT": "#eab308", "HOLD": "#6b7280", "WATCH": "#3b82f6",
}

# Pair type ranking for node color assignment (COINT best)
TC_RANK = {"COINT": 3, "HURST": 2, "CORR": 1}


@st.cache_data(ttl=600, show_spinner=False)
def _cached_pair_data(sym_a: str, sym_b: str, days: int = 500):
    """Cache-wrapped pair price + cointegration + signal computation."""
    prices_a, prices_b, merged = load_pair_prices(sym_a, sym_b, days=days)
    if merged.empty or len(merged) < 60:
        return None
    coint_result = test_cointegration(prices_a, prices_b)
    kalman_r = kalman_hedge_ratio(prices_a, prices_b)
    sig = generate_pairs_signal(sym_a, sym_b, lookback_days=days)
    return {
        "prices_a": prices_a, "prices_b": prices_b, "merged": merged,
        "coint": coint_result, "kalman": kalman_r, "signal": sig,
    }


# ---------------------------------------------------------------------------
# Tab 1: Pair Explorer
# ---------------------------------------------------------------------------

def _render_explorer_tab():
    st.subheader("Pair Explorer")

    # Quick-select known pairs
    pair_labels = [f"{a}/{b}" for a, b in KNOWN_PAIR_CANDIDATES]
    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        quick = st.selectbox("Known pairs", ["Custom..."] + pair_labels)
    with c2:
        if quick == "Custom...":
            sym_a = st.text_input("Symbol A", "OGDC").strip().upper()
        else:
            sym_a = quick.split("/")[0]
            st.text_input("Symbol A", sym_a, disabled=True)
    with c3:
        if quick == "Custom...":
            sym_b = st.text_input("Symbol B", "PPL").strip().upper()
        else:
            sym_b = quick.split("/")[1]
            st.text_input("Symbol B", sym_b, disabled=True)

    if not sym_a or not sym_b:
        return

    with st.spinner("Loading pair data..."):
        cached = _cached_pair_data(sym_a, sym_b, days=500)

    if cached is None:
        st.warning(f"Insufficient data for {sym_a}/{sym_b}")
        return

    prices_a = cached["prices_a"]
    prices_b = cached["prices_b"]
    merged = cached["merged"]
    coint = cached["coint"]
    kalman_r = cached["kalman"]
    sig = cached["signal"]

    hedge = kalman_r.dropna().iloc[-1] if not kalman_r.dropna().empty else coint.get("hedge_ratio", 1)

    spread = prices_a.values - hedge * prices_b.values
    hl = coint.get("half_life", 30)
    window = max(20, min(120, int(hl * 2)))
    sp_mean = np.mean(spread[-window:])
    sp_std = np.std(spread[-window:])
    zscore = (spread[-1] - sp_mean) / sp_std if sp_std > 0 else 0

    # Metrics
    cols = st.columns(7)
    cols[0].metric("Z-Score", f"{zscore:+.2f}")
    cols[1].metric("Hedge Ratio", f"{hedge:.3f}")
    cols[2].metric("Half-Life", f"{hl:.0f}d")
    cols[3].metric("Correlation", f"{prices_a.corr(prices_b):.3f}")
    cols[4].metric("Coint p-value", f"{coint.get('coint_pvalue', 1):.4f}")
    cols[5].metric("Hurst", f"{coint.get('hurst', 0.5):.3f}")
    cols[6].metric("Signal", sig.signal if sig else "N/A")

    if sig:
        color = SIGNAL_COLORS.get(sig.signal, "#8b949e")
        st.caption(f":{color[1:]}[{sig.signal}] -- {sig.reason}")
        if sig.signal in ("LONG_SPREAD", "SHORT_SPREAD"):
            st.info(f"Position: **{sig.position_a} {sig.shares_a:,} {sym_a}** + "
                    f"**{sig.position_b} {sig.shares_b:,} {sym_b}** (per 1M PKR)")

    # Charts
    rolling_z = (pd.Series(spread) - pd.Series(spread).rolling(window, min_periods=20).mean()) / \
                pd.Series(spread).rolling(window, min_periods=20).std()

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.4, 0.35, 0.25], vertical_spacing=0.05,
        subplot_titles=[f"{sym_a} vs {sym_b} (Normalized)", "Spread Z-Score", "Hedge Ratio (Kalman)"],
    )

    # Normalized prices
    fig.add_trace(go.Scatter(
        x=merged["date"], y=prices_a.values / prices_a.iloc[0] * 100,
        name=sym_a, line=dict(color="#22C55E"),
    ), row=1, col=1)
    fig.add_trace(go.Scatter(
        x=merged["date"], y=prices_b.values / prices_b.iloc[0] * 100,
        name=sym_b, line=dict(color="#3B82F6"),
    ), row=1, col=1)

    # Z-score with bands
    fig.add_trace(go.Scatter(
        x=merged["date"], y=rolling_z,
        name="Z-Score", line=dict(color="#C8A96E", width=1.5),
    ), row=2, col=1)
    fig.add_hline(y=2, line_dash="dash", line_color="#EF4444", row=2, col=1)
    fig.add_hline(y=-2, line_dash="dash", line_color="#22C55E", row=2, col=1)
    fig.add_hline(y=0, line_dash="dot", line_color="#6B7280", row=2, col=1)
    fig.add_hrect(y0=2, y1=5, fillcolor="rgba(239,68,68,0.1)", row=2, col=1, line_width=0)
    fig.add_hrect(y0=-5, y1=-2, fillcolor="rgba(34,197,94,0.1)", row=2, col=1, line_width=0)

    # Hedge ratio
    fig.add_trace(go.Scatter(
        x=merged["date"], y=kalman_r.values,
        name="Kalman beta", line=dict(color="#A855F7", width=1.5),
    ), row=3, col=1)

    fig.update_layout(**PLOT_LAYOUT, height=700, showlegend=True,
                      legend=dict(orientation="h", y=1.02))
    st.plotly_chart(fig, width='stretch')

    # Scatter plot
    with st.expander("Price Scatter (A vs B)"):
        fig_sc = go.Figure()
        fig_sc.add_trace(go.Scatter(
            x=prices_b.values, y=prices_a.values, mode="markers",
            marker=dict(size=4, color="#C8A96E", opacity=0.5),
        ))
        # Regression line
        beta = coint.get("hedge_ratio", 1)
        x_line = np.array([prices_b.min(), prices_b.max()])
        y_line = beta * x_line
        fig_sc.add_trace(go.Scatter(
            x=x_line, y=y_line, mode="lines",
            line=dict(color="#ef4444", dash="dash"), name=f"beta={beta:.3f}",
        ))
        fig_sc.update_layout(**PLOT_LAYOUT, height=350,
                             xaxis_title=sym_b, yaxis_title=sym_a,
                             title=f"{sym_a} vs {sym_b} Regression")
        st.plotly_chart(fig_sc, width='stretch')


# ---------------------------------------------------------------------------
# Tab 2: Pair Discovery
# ---------------------------------------------------------------------------

def _render_discovery_tab():
    st.subheader("Pair Discovery")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        min_corr = st.slider("Min correlation", 0.5, 0.95, 0.6, 0.05)
    with c2:
        max_pval = st.slider("Max coint p-value", 0.01, 0.15, 0.10, 0.01)
    with c3:
        hl_range = st.slider("Half-life range (days)", 3, 120, (3, 120))
    with c4:
        sector_only = st.checkbox("Same sector only", value=True)
        min_days_map = {"1 year": 200, "2 years": 400, "3 years": 600}
        min_days_label = st.selectbox("Min lookback", list(min_days_map.keys()))

    min_score = st.slider("Min composite score", 0, 80, 30, 5,
                           help="Weighted: coint 40% + corr 25% + hurst 20% + half-life 15%")

    if st.button("Scan for Cointegrated Pairs", type="primary", width='stretch'):
        with st.spinner("Scanning pairs..."):
            pairs = find_cointegrated_pairs(
                min_correlation=min_corr, max_pvalue=max_pval,
                min_half_life=hl_range[0], max_half_life=hl_range[1],
                min_days=min_days_map[min_days_label],
                sector_only=sector_only, top_n=40, min_score=min_score,
            )

        if not pairs:
            st.warning("No cointegrated pairs found. Try relaxing filters.")
            return

        st.success(f"Found **{len(pairs)}** cointegrated pairs")

        rows = []
        for p in pairs:
            direction = (
                "SHORT_SPREAD" if p.current_zscore > 1.5
                else "LONG_SPREAD" if p.current_zscore < -1.5
                else "WATCH" if abs(p.current_zscore) > 1.0
                else "HOLD"
            )
            rows.append({
                "Pair": f"{p.symbol_a}/{p.symbol_b}",
                "Score": p.composite_score,
                "Sector": p.sector,
                "Corr": p.correlation,
                "Coint p": p.cointegration_pvalue,
                "Half-Life": p.half_life,
                "Hurst": p.hurst_exponent,
                "Z-Score": p.current_zscore,
                "Signal": direction,
                "Hedge Ratio": p.hedge_ratio_kalman,
            })

        df = pd.DataFrame(rows)

        def _color_signal(val):
            return f"color: {SIGNAL_COLORS.get(val, '#8b949e')}"

        def _color_score(val):
            if val >= 70:
                return "color: #22c55e"
            elif val >= 50:
                return "color: #C8A96E"
            return "color: #8b949e"

        styled = df.style.map(_color_signal, subset=["Signal"]).map(
            _color_score, subset=["Score"],
        ).format({
            "Score": "{:.0f}", "Corr": "{:.3f}", "Coint p": "{:.4f}",
            "Half-Life": "{:.0f}d", "Hurst": "{:.3f}", "Z-Score": "{:+.2f}",
            "Hedge Ratio": "{:.3f}",
        })
        st.dataframe(styled, width='stretch', hide_index=True)

        st.session_state["pairs_discovered"] = pairs


# ---------------------------------------------------------------------------
# Tab 3: Backtest
# ---------------------------------------------------------------------------

def _render_backtest_tab():
    st.subheader("Pairs Backtest")

    pair_labels = [f"{a}/{b}" for a, b in KNOWN_PAIR_CANDIDATES]
    c1, c2 = st.columns(2)
    with c1:
        pair_sel = st.selectbox("Pair", pair_labels, key="bt_pair")
        sym_a, sym_b = pair_sel.split("/")
    with c2:
        use_kalman = st.checkbox("Use Kalman filter", value=True)
        lookback_map = {"2 years": 500, "3 years": 750, "5 years": 1250}
        lb_label = st.selectbox("Lookback", list(lookback_map.keys()))

    c3, c4, c5, c6 = st.columns(4)
    with c3:
        entry_z = st.slider("Entry z-score", 1.5, 3.0, 2.0, 0.1)
    with c4:
        exit_z = st.slider("Exit z-score", 0.1, 1.0, 0.5, 0.1)
    with c5:
        stop_z = st.slider("Stop loss z-score", 3.0, 6.0, 4.0, 0.5)
    with c6:
        max_hold = st.slider("Max hold (days)", 20, 120, 60)
        txn_cost = st.slider("Transaction cost %", 0.1, 1.0, 0.5, 0.1) / 100

    if st.button("Run Backtest", type="primary", width='stretch'):
        with st.spinner("Running backtest..."):
            result = backtest_pairs_strategy(
                sym_a, sym_b, entry_zscore=entry_z, exit_zscore=exit_z,
                stop_loss_zscore=stop_z, max_hold_days=max_hold,
                use_kalman=use_kalman, lookback_days=lookback_map[lb_label],
                transaction_cost=txn_cost,
            )

        if "error" in result:
            st.error(result["error"])
            if "spread_data" in result:
                _plot_spread(result["spread_data"], sym_a, sym_b, entry_z)
            return

        m = result["metrics"]
        trades_df = result["trades"]

        cols = st.columns(7)
        cols[0].metric("Trades", m["total_trades"])
        cols[1].metric("Win Rate", f"{m['win_rate']:.0%}")
        cols[2].metric("Profit Factor", f"{m['profit_factor']:.2f}")
        cols[3].metric("Total Return", f"{m['total_return']:.1%}")
        cols[4].metric("Sharpe", f"{m['sharpe_ratio']:.2f}")
        cols[5].metric("Max DD", f"{m['max_drawdown']:.1%}")
        cols[6].metric("Avg Hold", f"{m['avg_days_held']:.0f}d")

        st.caption(f"Half-life: {m['half_life']:.0f}d | Hurst: {m['hurst']:.3f} | "
                   f"Coint p: {m['cointegration_pvalue']:.4f} | "
                   f"Exits: {m['exit_reasons']}")

        # Spread chart with trade markers
        if "spread_data" in result:
            _plot_spread(result["spread_data"], sym_a, sym_b, entry_z, trades_df)

        # Equity curve
        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            x=trades_df["exit_date"], y=trades_df["cum_return"],
            mode="lines+markers", line=dict(color="#d4a017", width=2), name="Equity",
        ))
        fig_eq.add_hline(y=1.0, line_dash="dash", line_color="#666")
        fig_eq.update_layout(**PLOT_LAYOUT, height=300, title="Equity Curve",
                             yaxis_title="Growth of $1")
        st.plotly_chart(fig_eq, width='stretch')

        # Trade log
        st.subheader("Trade Log")
        display = trades_df[["entry_date", "exit_date", "direction", "entry_z", "exit_z",
                              "pnl_pct", "days_held", "exit_reason"]].copy()
        display["pnl_pct"] = display["pnl_pct"].map(lambda x: f"{x:+.2%}")
        display["entry_z"] = display["entry_z"].map(lambda x: f"{x:+.2f}")
        display["exit_z"] = display["exit_z"].map(lambda x: f"{x:+.2f}")
        st.dataframe(display, width='stretch', hide_index=True)


def _plot_spread(spread_data, sym_a, sym_b, entry_z, trades_df=None):
    """Plot spread z-score chart with optional trade markers."""
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True,
                        row_heights=[0.5, 0.5], vertical_spacing=0.05,
                        subplot_titles=[f"{sym_a} vs {sym_b} Prices", "Z-Score"])

    # Normalized prices
    pa = spread_data["price_a"]
    pb = spread_data["price_b"]
    fig.add_trace(go.Scatter(x=spread_data["date"], y=pa / pa.iloc[0] * 100,
                             name=sym_a, line=dict(color="#22C55E")), row=1, col=1)
    fig.add_trace(go.Scatter(x=spread_data["date"], y=pb / pb.iloc[0] * 100,
                             name=sym_b, line=dict(color="#3B82F6")), row=1, col=1)

    # Z-score
    fig.add_trace(go.Scatter(x=spread_data["date"], y=spread_data["zscore"],
                             name="Z-Score", line=dict(color="#C8A96E", width=1.5)),
                  row=2, col=1)
    fig.add_hline(y=entry_z, line_dash="dash", line_color="#EF4444", row=2, col=1)
    fig.add_hline(y=-entry_z, line_dash="dash", line_color="#22C55E", row=2, col=1)
    fig.add_hline(y=0, line_dash="dot", line_color="#6B7280", row=2, col=1)

    # Trade markers
    if trades_df is not None and not trades_df.empty:
        for _, t in trades_df.iterrows():
            color = "#22c55e" if t["pnl_pct"] > 0 else "#ef4444"
            fig.add_vline(x=t["entry_date"], line_dash="dot", line_color=color,
                          opacity=0.4, row=2, col=1)

    fig.update_layout(**PLOT_LAYOUT, height=500, showlegend=True,
                      legend=dict(orientation="h", y=1.02))
    st.plotly_chart(fig, width='stretch')


# ---------------------------------------------------------------------------
# Tab 4: Research
# ---------------------------------------------------------------------------

def _render_research_tab():
    st.subheader("Pairs Research")

    # Sector pair playbook
    st.markdown("### PSX Sector Pair Playbook")
    playbook = {
        "Banking": "HBL/UBL (most stable), MCB/ABL, NBP/BOP, BAHL/MEBL",
        "E&P": "OGDC/PPL (govt-owned), MARI/PPL",
        "Cement": "LUCK/DGKC (leaders), LUCK/MLCF, MTL/CHCC",
        "Fertilizer": "ENGRO/FFC (duopoly)",
        "Power": "HUBC/KAPCO (IPPs)",
        "Oil Marketing": "PSO/SHEL",
        "Refinery": "ATRL/NRL",
    }
    for sector, pairs in playbook.items():
        st.caption(f"**{sector}:** {pairs}")

    # Rolling cointegration stability
    st.markdown("### Rolling Cointegration Check")
    pair_labels = [f"{a}/{b}" for a, b in KNOWN_PAIR_CANDIDATES[:7]]
    pair_sel = st.selectbox("Pair", pair_labels, key="research_pair")
    sym_a, sym_b = pair_sel.split("/")

    if st.button("Check Stability"):
        with st.spinner("Computing rolling cointegration..."):
            prices_a, prices_b, merged = load_pair_prices(sym_a, sym_b, days=750)

            if len(merged) < 200:
                st.warning("Insufficient data for rolling analysis")
                return

            # Rolling 120-day cointegration p-value
            roll_window = 120
            pvalues = []
            hedge_ratios = []
            half_lives = []
            dates = []

            for i in range(roll_window, len(merged)):
                pa = prices_a.iloc[i - roll_window:i]
                pb = prices_b.iloc[i - roll_window:i]
                result = test_cointegration(pa, pb)
                pvalues.append(result.get("coint_pvalue", 1))
                hedge_ratios.append(result.get("hedge_ratio", 0))
                half_lives.append(min(result.get("half_life", 999), 120))
                dates.append(merged.iloc[i]["date"])

        fig = make_subplots(rows=3, cols=1, shared_xaxes=True,
                            row_heights=[0.33, 0.33, 0.33], vertical_spacing=0.05,
                            subplot_titles=["Rolling Coint p-value (120d)", "Rolling Hedge Ratio", "Rolling Half-Life"])

        fig.add_trace(go.Scatter(x=dates, y=pvalues, name="p-value",
                                 line=dict(color="#C8A96E")), row=1, col=1)
        fig.add_hline(y=0.05, line_dash="dash", line_color="#22c55e", row=1, col=1)

        fig.add_trace(go.Scatter(x=dates, y=hedge_ratios, name="Hedge Ratio",
                                 line=dict(color="#A855F7")), row=2, col=1)

        fig.add_trace(go.Scatter(x=dates, y=half_lives, name="Half-Life",
                                 line=dict(color="#3B82F6")), row=3, col=1)

        fig.update_layout(**PLOT_LAYOUT, height=600, showlegend=False)
        st.plotly_chart(fig, width='stretch')

        stable_pct = sum(1 for p in pvalues if p < 0.05) / len(pvalues) * 100
        st.caption(f"Cointegrated {stable_pct:.0f}% of rolling windows")

    # Methodology
    with st.expander("Methodology"):
        st.markdown("""
**Pairs Trading** exploits temporary mispricings between cointegrated stocks:

1. **Cointegration** (Engle-Granger): Two stocks share a long-run equilibrium. Unlike correlation, cointegration means the spread is mean-reverting.
2. **Kalman Filter**: Adapts the hedge ratio over time as the relationship drifts.
3. **Z-Score**: Measures how far the spread is from its mean in standard deviations.
4. **Half-Life**: How quickly the spread reverts to mean (5-60 days = tradeable).
5. **Hurst Exponent**: < 0.5 = mean-reverting, > 0.5 = trending.

**Entry**: |Z| > 2.0 (spread 2 sigma from mean)
**Exit**: |Z| < 0.5 (spread normalized) or stop-loss at |Z| > 4.0

**PSX Edge**: Small universe (~200 liquid stocks), sector-driven market, no HFT -- mispricings persist for days/weeks.
""")


# ---------------------------------------------------------------------------
# Tab 5: Network Graph
# ---------------------------------------------------------------------------

SECTOR_NAMES = {
    "801": "Modaraba", "802": "Leasing", "803": "Investment", "804": "Cement",
    "805": "Sugar", "806": "Food", "807": "Banking", "808": "Engineering",
    "809": "Auto", "810": "Chemical", "811": "Pharma", "812": "Textile Composite",
    "813": "Technology", "818": "Insurance", "819": "NBFC", "820": "Refinery",
    "821": "Oil Marketing", "822": "Power", "823": "Pharma", "824": "Textile Spinning",
    "825": "Paper", "826": "Transport", "827": "Tobacco", "828": "Telecom",
    "829": "Misc", "830": "Fertilizer", "833": "Glass", "836": "Cable",
    "837": "Real Estate", "838": "E&P",
}


def _build_pairs_network(pairs_json: str, nodes_json: str) -> str:
    """Build D3 force-directed network graph of pair trading relationships."""
    return f"""<!DOCTYPE html>
<html><head>
<script src="https://cdnjs.cloudflare.com/ajax/libs/d3/7.8.5/d3.min.js"></script>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:#0B0E11;font-family:'JetBrains Mono',monospace;color:#c9d1d9;overflow:hidden}}
.hdr{{display:flex;gap:16px;padding:8px 0;flex-wrap:wrap;align-items:center}}
.stats{{display:flex;gap:12px;flex-wrap:wrap}}
.stat{{background:rgba(128,128,128,0.08);border-radius:6px;padding:6px 12px;text-align:center}}
.stat-l{{font-size:10px;color:#8b949e}}.stat-v{{font-size:16px;font-weight:600;color:#c9d1d9}}
.ctrl{{display:flex;gap:10px;align-items:center;font-size:12px;color:#8b949e}}
.ctrl select,.ctrl input{{background:#161b22;border:1px solid #30363d;color:#c9d1d9;
  padding:3px 6px;border-radius:4px;font-size:11px;font-family:inherit}}
.legend{{display:flex;gap:14px;font-size:11px;color:#8b949e;padding:4px 0}}
.leg-dot{{width:8px;height:8px;border-radius:2px;display:inline-block;margin-right:3px}}
#net{{width:100%;border:1px solid rgba(200,169,110,0.15);border-radius:8px;background:#0B0E11}}
.tip{{position:absolute;padding:10px 14px;background:#161b22;color:#c9d1d9;
  border:1px solid #C8A96E;border-radius:6px;font-size:11px;pointer-events:none;
  opacity:0;transition:opacity 0.15s;z-index:10;min-width:180px;line-height:1.7;
  box-shadow:0 4px 12px rgba(0,0,0,0.5)}}
.tip b{{color:#C8A96E}}
.tip .m{{color:#8b949e}}
</style>
</head><body>
<div class="hdr">
  <div class="stats">
    <div class="stat"><div class="stat-l">Pairs</div><div class="stat-v" id="sp">0</div></div>
    <div class="stat"><div class="stat-l">Symbols</div><div class="stat-v" id="ss">0</div></div>
    <div class="stat"><div class="stat-l">Avg Score</div><div class="stat-v" id="sa">0</div></div>
    <div class="stat"><div class="stat-l">Best Pair</div><div class="stat-v" id="sb" style="font-size:12px">&mdash;</div></div>
  </div>
  <div style="flex:1"></div>
  <div class="ctrl">
    <span>Min score</span>
    <input type="range" min="0" max="90" value="30" step="5" id="thr" style="width:100px">
    <span id="thrv">30</span>
    <select id="tf">
      <option value="all">All types</option>
      <option value="COINT">Cointegrated</option>
      <option value="CORR">Correlation</option>
      <option value="HURST">Mean-reverting</option>
    </select>
  </div>
</div>
<div class="legend">
  <span><span class="leg-dot" style="background:#1D9E75"></span>Cointegrated (p&lt;0.10)</span>
  <span><span class="leg-dot" style="background:#378ADD"></span>High Correlation (&gt;0.80)</span>
  <span><span class="leg-dot" style="background:#D85A30"></span>Mean-reverting (Hurst&lt;0.45)</span>
</div>
<div style="position:relative">
  <svg id="net" height="440"></svg>
  <div class="tip" id="tip"></div>
</div>
<script>
const allPairs = {pairs_json};
const allNodes = {nodes_json};
const TC = {{COINT:'#1D9E75', CORR:'#378ADD', HURST:'#D85A30'}};
const W = document.getElementById('net').clientWidth || 700, H = 440;
const svg = d3.select('#net').attr('viewBox',`0 0 ${{W}} ${{H}}`);
const tip = document.getElementById('tip');
const g = svg.append('g');
const lG = g.append('g'), nG = g.append('g');
let fP = [...allPairs];

const sim = d3.forceSimulation(allNodes)
  .force('link', d3.forceLink(fP).id(d=>d.id).distance(d=>140-d.score*0.8))
  .force('charge', d3.forceManyBody().strength(-280))
  .force('center', d3.forceCenter(W/2, H/2))
  .force('collision', d3.forceCollide(24))
  .force('x', d3.forceX(W/2).strength(0.04))
  .force('y', d3.forceY(H/2).strength(0.04));

function render() {{
  const lk = lG.selectAll('line').data(fP, d=>d.a+d.b);
  lk.exit().remove();
  const lkE = lk.enter().append('line')
    .attr('stroke-width', d=>Math.max(1.5, d.score/20))
    .attr('stroke', d=>TC[d.type]||'#888')
    .attr('opacity', d=>0.25+d.score/160)
    .on('mouseover', (e,d) => {{
      tip.style.opacity=1;
      tip.innerHTML=`<b>${{d.a}} / ${{d.b}}</b><br>`+
        `<span class="m">Score:</span> ${{d.score}} <span class="m">| Type:</span> ${{d.type}}<br>`+
        `<span class="m">p-value:</span> ${{d.pval.toFixed(3)}} <span class="m">| Corr:</span> ${{d.corr.toFixed(2)}}<br>`+
        `<span class="m">Half-life:</span> ${{d.hl}}d <span class="m">| Hurst:</span> ${{d.hurst.toFixed(2)}}<br>`+
        `<span class="m">Spread Z:</span> ${{(d.zscore||0).toFixed(1)}}`;
      const r=document.getElementById('net').getBoundingClientRect();
      tip.style.left=(e.clientX-r.left+12)+'px';
      tip.style.top=(e.clientY-r.top-10)+'px';
    }})
    .on('mouseout', ()=>tip.style.opacity=0);

  const nd = nG.selectAll('g.n').data(allNodes, d=>d.id);
  nd.exit().remove();
  const en = nd.enter().append('g').attr('class','n').call(
    d3.drag().on('start',(e,d)=>{{if(!e.active)sim.alphaTarget(0.3).restart();d.fx=d.x;d.fy=d.y}})
    .on('drag',(e,d)=>{{d.fx=e.x;d.fy=e.y}})
    .on('end',(e,d)=>{{if(!e.active)sim.alphaTarget(0);d.fx=null;d.fy=null}})
  );
  en.append('circle').attr('r',14)
    .attr('fill',d=>TC[d.bestType]||'#888').attr('opacity',0.85)
    .attr('stroke','rgba(200,169,110,0.25)').attr('stroke-width',0.5);
  en.append('text').text(d=>d.id)
    .attr('text-anchor','middle').attr('dy',-18)
    .attr('font-size','10px').attr('font-weight',500)
    .attr('fill','#c9d1d9');

  const ac = new Set();
  fP.forEach(p=>{{ac.add(p.a);ac.add(p.b)}});
  nG.selectAll('g.n').attr('opacity',d=>ac.has(d.id)?1:0.12);

  document.getElementById('sp').textContent=fP.length;
  document.getElementById('ss').textContent=ac.size;
  const avg=fP.length?Math.round(fP.reduce((s,p)=>s+p.score,0)/fP.length):0;
  document.getElementById('sa').textContent=avg;
  const best=fP.length?fP.reduce((a,b)=>a.score>b.score?a:b):null;
  document.getElementById('sb').textContent=best?best.a+'/'+best.b:'\\u2014';
}}

sim.on('tick', ()=>{{
  lG.selectAll('line').attr('x1',d=>d.source.x).attr('y1',d=>d.source.y)
    .attr('x2',d=>d.target.x).attr('y2',d=>d.target.y);
  nG.selectAll('g.n').attr('transform',d=>`translate(${{d.x}},${{d.y}})`);
}});
render();

function applyF(){{
  const mn=+document.getElementById('thr').value;
  const tv=document.getElementById('tf').value;
  fP=allPairs.filter(p=>p.score>=mn&&(tv==='all'||p.type===tv)).map(function(p){{return Object.assign({{}},p,{{source:p.a,target:p.b}})}});
  document.getElementById('thrv').textContent=mn;
  sim.force('link').links(fP);
  sim.alpha(0.5).restart();
  render();
}}
document.getElementById('thr').addEventListener('input',applyF);
document.getElementById('tf').addEventListener('change',applyF);
svg.call(d3.zoom().scaleExtent([0.3,4]).on('zoom',e=>g.attr('transform',e.transform)));
</script>
</body></html>"""


@st.cache_data(ttl=600, show_spinner="Scanning pairs for network...")
def _scan_network_pairs() -> list:
    """Cached pair scan for the network graph tab."""
    pairs = find_cointegrated_pairs(
        min_correlation=0.6, max_pvalue=0.10,
        min_half_life=3, max_half_life=120,
        sector_only=True, top_n=50, min_score=25,
    )
    return pairs


def _render_network_tab():
    st.subheader("Pair Network Graph")
    st.caption("Drag nodes, hover edges for details, filter by score/type. Zoom with scroll.")

    pairs = st.session_state.get("pairs_discovered") or _scan_network_pairs()

    if not pairs:
        st.warning("No pairs found.")
        return

    # Build nodes + edges JSON for D3
    node_map: dict[str, dict] = {}
    edges = []
    for p in pairs:
        sector_label = SECTOR_NAMES.get(p.sector, p.sector or "Unknown")
        for sym in (p.symbol_a, p.symbol_b):
            if sym not in node_map:
                node_map[sym] = {"id": sym, "sector": sector_label, "bestType": p.pair_type}
            elif TC_RANK.get(p.pair_type, 0) > TC_RANK.get(node_map[sym]["bestType"], 0):
                node_map[sym]["bestType"] = p.pair_type

        edges.append({
            "source": p.symbol_a, "target": p.symbol_b,
            "a": p.symbol_a, "b": p.symbol_b,
            "score": round(p.composite_score, 1),
            "type": p.pair_type,
            "pval": round(p.cointegration_pvalue, 4),
            "corr": round(p.correlation, 3),
            "hl": round(p.half_life, 1),
            "hurst": round(p.hurst_exponent, 3),
            "zscore": round(p.current_zscore, 2),
        })

    nodes = list(node_map.values())
    html = _build_pairs_network(json.dumps(edges), json.dumps(nodes))
    components.html(html, height=560, scrolling=False)

    # Summary table below
    st.divider()
    st.markdown(f"**{len(pairs)} pairs** sorted by composite score")

    rows = []
    for p in pairs:
        rows.append({
            "Pair": f"{p.symbol_a}/{p.symbol_b}",
            "Score": p.composite_score,
            "Type": p.pair_type,
            "Sector": SECTOR_NAMES.get(p.sector, p.sector or "?"),
            "Corr": p.correlation,
            "Coint p": p.cointegration_pvalue,
            "HL": p.half_life,
            "Hurst": p.hurst_exponent,
            "Z": p.current_zscore,
        })
    df = pd.DataFrame(rows)
    st.dataframe(df.style.format({
        "Score": "{:.0f}", "Corr": "{:.3f}", "Coint p": "{:.4f}",
        "HL": "{:.0f}d", "Hurst": "{:.3f}", "Z": "{:+.2f}",
    }), width='stretch', hide_index=True)



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def render_strategy_pairs():
    st.title("Pairs Trading (Statistical Arbitrage)")
    st.caption("Cointegration-based spread trading with Kalman filter hedge ratios")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "Pair Explorer", "Pair Discovery", "Backtest", "Research", "Network Graph",
    ])

    with tab1:
        _render_explorer_tab()
    with tab2:
        _render_discovery_tab()
    with tab3:
        _render_backtest_tab()
    with tab4:
        _render_research_tab()
    with tab5:
        _render_network_tab()
