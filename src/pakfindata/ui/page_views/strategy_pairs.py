"""Pairs Trading (Statistical Arbitrage) -- Streamlit page."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

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
        prices_a, prices_b, merged = load_pair_prices(sym_a, sym_b, days=500)

    if merged.empty or len(merged) < 60:
        st.warning(f"Insufficient data for {sym_a}/{sym_b}")
        return

    # Cointegration test
    coint = test_cointegration(prices_a, prices_b)
    kalman_r = kalman_hedge_ratio(prices_a, prices_b)
    hedge = kalman_r.dropna().iloc[-1] if not kalman_r.dropna().empty else coint.get("hedge_ratio", 1)

    spread = prices_a.values - hedge * prices_b.values
    hl = coint.get("half_life", 30)
    window = max(20, min(120, int(hl * 2)))
    sp_mean = np.mean(spread[-window:])
    sp_std = np.std(spread[-window:])
    zscore = (spread[-1] - sp_mean) / sp_std if sp_std > 0 else 0

    # Signal
    sig = generate_pairs_signal(sym_a, sym_b)

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
    st.plotly_chart(fig, use_container_width=True)

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
        st.plotly_chart(fig_sc, use_container_width=True)


# ---------------------------------------------------------------------------
# Tab 2: Pair Discovery
# ---------------------------------------------------------------------------

def _render_discovery_tab():
    st.subheader("Pair Discovery")

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        min_corr = st.slider("Min correlation", 0.5, 0.95, 0.7, 0.05)
    with c2:
        max_pval = st.slider("Max coint p-value", 0.01, 0.10, 0.05, 0.01)
    with c3:
        hl_range = st.slider("Half-life range (days)", 3, 100, (5, 60))
    with c4:
        sector_only = st.checkbox("Same sector only", value=True)
        min_days_map = {"1 year": 200, "2 years": 400, "3 years": 600}
        min_days_label = st.selectbox("Min lookback", list(min_days_map.keys()))

    if st.button("Scan for Cointegrated Pairs", type="primary", use_container_width=True):
        with st.spinner("Scanning pairs... (this may take a minute)"):
            pairs = find_cointegrated_pairs(
                min_correlation=min_corr, max_pvalue=max_pval,
                min_half_life=hl_range[0], max_half_life=hl_range[1],
                min_days=min_days_map[min_days_label],
                sector_only=sector_only, top_n=20,
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

        styled = df.style.applymap(_color_signal, subset=["Signal"]).format({
            "Corr": "{:.3f}", "Coint p": "{:.4f}", "Half-Life": "{:.0f}d",
            "Hurst": "{:.3f}", "Z-Score": "{:+.2f}", "Hedge Ratio": "{:.3f}",
        })
        st.dataframe(styled, use_container_width=True, hide_index=True)

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

    if st.button("Run Backtest", type="primary", use_container_width=True):
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
        st.plotly_chart(fig_eq, use_container_width=True)

        # Trade log
        st.subheader("Trade Log")
        display = trades_df[["entry_date", "exit_date", "direction", "entry_z", "exit_z",
                              "pnl_pct", "days_held", "exit_reason"]].copy()
        display["pnl_pct"] = display["pnl_pct"].map(lambda x: f"{x:+.2%}")
        display["entry_z"] = display["entry_z"].map(lambda x: f"{x:+.2f}")
        display["exit_z"] = display["exit_z"].map(lambda x: f"{x:+.2f}")
        st.dataframe(display, use_container_width=True, hide_index=True)


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
    st.plotly_chart(fig, use_container_width=True)


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
        st.plotly_chart(fig, use_container_width=True)

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
# Main
# ---------------------------------------------------------------------------

def render_strategy_pairs():
    st.title("Pairs Trading (Statistical Arbitrage)")
    st.caption("Cointegration-based spread trading with Kalman filter hedge ratios")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Pair Explorer", "Pair Discovery", "Backtest", "Research",
    ])

    with tab1:
        _render_explorer_tab()
    with tab2:
        _render_discovery_tab()
    with tab3:
        _render_backtest_tab()
    with tab4:
        _render_research_tab()
