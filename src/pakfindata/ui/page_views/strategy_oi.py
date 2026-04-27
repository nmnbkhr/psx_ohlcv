"""OI Buildup/Unwind Strategy -- Streamlit page."""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.engine.oi_strategy import (
    OIState,
    backtest_oi_strategy,
    compute_oi_signals,
    get_rollover_calendar,
    load_oi_data,
    scan_oi_signals,
)

DARK_BG = "rgba(0,0,0,0)"
PLOT_LAYOUT = dict(
    paper_bgcolor=DARK_BG,
    plot_bgcolor=DARK_BG,
    font_color="#c9d1d9",
    margin=dict(l=20, r=20, t=40, b=20),
)

STATE_COLORS = {
    "LONG_BUILDUP": "#22c55e",
    "SHORT_BUILDUP": "#ef4444",
    "SHORT_COVERING": "#eab308",
    "LONG_UNWINDING": "#a855f7",
    "NEUTRAL": "#6b7280",
}

STATE_BG = {
    "LONG_BUILDUP": "rgba(34,197,94,0.15)",
    "SHORT_BUILDUP": "rgba(239,68,68,0.15)",
    "SHORT_COVERING": "rgba(234,179,8,0.15)",
    "LONG_UNWINDING": "rgba(168,85,247,0.15)",
    "NEUTRAL": "rgba(107,114,128,0.08)",
}

SIGNAL_COLORS = {
    "BUY": "#22c55e",
    "SELL": "#ef4444",
    "EXIT_LONG": "#eab308",
    "EXIT_SHORT": "#a855f7",
    "HOLD": "#6b7280",
}


def _get_futures_symbols() -> list[str]:
    """Get symbols that have futures data."""
    from pakfindata.engine.oi_strategy import _psx_con
    scon = _psx_con()
    try:
        syms = [r[0] for r in scon.execute("""
            SELECT DISTINCT base_symbol FROM futures_eod
            WHERE market_type = 'FUT' AND volume > 0
            GROUP BY base_symbol
            HAVING COUNT(DISTINCT date) > 30
            ORDER BY SUM(volume) DESC
        """).fetchall()]
    except Exception:
        syms = []
    finally:
        scon.close()
    return syms


# ---------------------------------------------------------------------------
# Tab 1: OI Matrix Live
# ---------------------------------------------------------------------------

def _render_live_tab():
    st.subheader("OI Matrix Live")

    symbols = _get_futures_symbols()
    if not symbols:
        st.warning("No futures symbols found.")
        return

    c1, c2 = st.columns([1, 3])
    with c1:
        symbol = st.selectbox("Symbol", symbols, index=symbols.index("HUBC") if "HUBC" in symbols else 0)
        lookback = st.selectbox("Lookback", [30, 60, 90, 180], index=1)

    with st.spinner("Loading OI data..."):
        oi_df = load_oi_data(symbol, days=lookback)

    if oi_df.empty:
        st.warning(f"No OI data for {symbol}")
        return

    signals = compute_oi_signals(oi_df, min_streak=2)
    if not signals:
        st.warning("No signals generated")
        return

    latest = signals[-1]

    # -- State badge + signal --
    with c2:
        st.markdown("---")
        cols = st.columns(6)
        cols[0].metric("State", latest.state.value.replace("_", " "))
        cols[1].metric("Signal", latest.signal,
                       delta=f"{latest.confidence:.0%} confidence")
        cols[2].metric("Streak", f"{latest.streak} days")
        cols[3].metric("OI", f"{latest.oi_contracts:,}")
        cols[4].metric("Basis", f"{latest.basis_pct:+.2f}%")
        cols[5].metric("DTE", f"{latest.days_to_expiry}")

    if latest.in_rollover:
        st.warning(f"ROLLOVER WINDOW -- {latest.days_to_expiry} days to expiry. Signals have reduced confidence.")

    st.caption(latest.reason)

    # -- OI + Price chart with state bands --
    sig_df = pd.DataFrame([{
        "date": s.date, "state": s.state.value, "signal": s.signal,
        "spot_price": s.spot_price, "futures_price": s.futures_price,
        "oi": s.oi_contracts, "oi_change_pct": s.oi_change_pct,
        "price_change_pct": s.price_change_pct, "streak": s.streak,
        "confidence": s.confidence, "volume": s.volume,
    } for s in signals])

    price_col = "spot_price" if sig_df["spot_price"].sum() > 0 else "futures_price"

    fig = make_subplots(
        rows=3, cols=1, shared_xaxes=True,
        row_heights=[0.5, 0.3, 0.2], vertical_spacing=0.03,
        subplot_titles=("Price", "Open Interest", "Volume"),
    )

    # Price line
    fig.add_trace(go.Scatter(
        x=sig_df["date"], y=sig_df[price_col],
        line=dict(color="#E0E0E0", width=1.5), name="Price",
    ), row=1, col=1)

    # State background bands
    for i in range(len(sig_df)):
        state = sig_df.iloc[i]["state"]
        color = STATE_BG.get(state, STATE_BG["NEUTRAL"])
        fig.add_vrect(
            x0=sig_df.iloc[i]["date"],
            x1=sig_df.iloc[min(i + 1, len(sig_df) - 1)]["date"],
            fillcolor=color, layer="below", line_width=0, row=1, col=1,
        )

    # BUY/SELL markers on price
    buys = sig_df[sig_df["signal"] == "BUY"]
    sells = sig_df[sig_df["signal"] == "SELL"]
    if not buys.empty:
        fig.add_trace(go.Scatter(
            x=buys["date"], y=buys[price_col],
            mode="markers", marker=dict(symbol="triangle-up", size=12, color="#22c55e"),
            name="BUY",
        ), row=1, col=1)
    if not sells.empty:
        fig.add_trace(go.Scatter(
            x=sells["date"], y=sells[price_col],
            mode="markers", marker=dict(symbol="triangle-down", size=12, color="#ef4444"),
            name="SELL",
        ), row=1, col=1)

    # OI area
    fig.add_trace(go.Scatter(
        x=sig_df["date"], y=sig_df["oi"],
        fill="tozeroy", fillcolor="rgba(200,169,110,0.2)",
        line=dict(color="#C8A96E", width=1.5), name="OI",
    ), row=2, col=1)

    # Volume bars
    fig.add_trace(go.Bar(
        x=sig_df["date"], y=sig_df["volume"],
        marker_color="#3b82f6", opacity=0.6, name="Volume",
    ), row=3, col=1)

    fig.update_layout(**PLOT_LAYOUT, height=600, showlegend=True,
                      legend=dict(orientation="h", y=1.02))
    st.plotly_chart(fig, width='stretch')

    # -- Signal history table --
    st.subheader("Signal History (last 20 days)")
    display = sig_df.tail(20)[["date", "state", "signal", "confidence", "streak",
                                "price_change_pct", "oi_change_pct", "oi", "volume"]].copy()
    display["price_change_pct"] = display["price_change_pct"].map(lambda x: f"{x:+.2%}")
    display["oi_change_pct"] = display["oi_change_pct"].map(lambda x: f"{x:+.2%}")
    display["confidence"] = display["confidence"].map(lambda x: f"{x:.0%}")
    display["oi"] = display["oi"].map(lambda x: f"{x:,}")
    display["volume"] = display["volume"].map(lambda x: f"{x:,}")
    st.dataframe(display, width='stretch', hide_index=True)

    # -- Rollover calendar --
    with st.expander("Rollover Calendar"):
        st.dataframe(get_rollover_calendar(3), width='stretch', hide_index=True)


# ---------------------------------------------------------------------------
# Tab 2: Backtest
# ---------------------------------------------------------------------------

def _render_backtest_tab():
    st.subheader("OI Strategy Backtest")

    symbols = _get_futures_symbols()
    if not symbols:
        st.warning("No futures symbols found.")
        return

    c1, c2, c3 = st.columns(3)
    with c1:
        symbol = st.selectbox("Symbol", symbols,
                              index=symbols.index("HUBC") if "HUBC" in symbols else 0,
                              key="bt_sym")
        min_streak = st.slider("Min streak (days)", 1, 5, 2)
    with c2:
        stop_loss = st.slider("Stop loss %", 1.0, 5.0, 3.0, 0.5) / 100
        take_profit = st.slider("Take profit %", 2.0, 10.0, 5.0, 0.5) / 100
    with c3:
        max_hold = st.slider("Max hold (days)", 5, 30, 15)
        exit_unwind = st.checkbox("Exit on OI unwind", value=True)
        skip_rollover = st.checkbox("Skip rollover signals", value=True)
        days_map = {"6 months": 180, "1 year": 365, "2 years": 730}
        days_label = st.selectbox("Lookback", list(days_map.keys()), index=1)

    if st.button("Run Backtest", type="primary", width='stretch'):
        with st.spinner("Running backtest..."):
            result = backtest_oi_strategy(
                symbol, min_streak=min_streak,
                stop_loss_pct=stop_loss, take_profit_pct=take_profit,
                max_hold_days=max_hold, exit_on_unwind=exit_unwind,
                skip_rollover=skip_rollover, days=days_map[days_label],
            )

        if "error" in result:
            st.error(result["error"])
            return

        m = result["metrics"]
        trades_df = result["trades"]

        # Metrics
        cols = st.columns(6)
        cols[0].metric("Total Trades", m["total_trades"])
        cols[1].metric("Win Rate", f"{m['win_rate']:.0%}")
        cols[2].metric("Profit Factor", f"{m['profit_factor']:.2f}")
        cols[3].metric("Total Return", f"{m['total_return']:.1%}")
        cols[4].metric("Max Drawdown", f"{m['max_drawdown']:.1%}")
        cols[5].metric("Avg Hold", f"{m['avg_days_held']:.1f}d")

        c1, c2 = st.columns(2)
        with c1:
            st.caption(f"Long: {m['long_trades']} | Short: {m['short_trades']}")
            st.caption(f"Avg Win: {m['avg_win']:.2%} | Avg Loss: {m['avg_loss']:.2%}")

        # By state breakdown
        with c2:
            if m["by_state"]:
                state_df = pd.DataFrame(m["by_state"]).T
                state_df.index.name = "State"
                st.dataframe(state_df, width='stretch')

        # Exit reason pie
        if m["by_exit_reason"]:
            fig_pie = go.Figure(go.Pie(
                labels=list(m["by_exit_reason"].keys()),
                values=list(m["by_exit_reason"].values()),
                marker_colors=["#22c55e", "#ef4444", "#eab308", "#3b82f6", "#a855f7", "#6b7280"],
            ))
            fig_pie.update_layout(**PLOT_LAYOUT, height=300, title="Exit Reasons")
            st.plotly_chart(fig_pie, width='stretch')

        # Equity curve
        fig_eq = go.Figure()
        fig_eq.add_trace(go.Scatter(
            x=trades_df["exit_date"], y=trades_df["cum_return"],
            mode="lines+markers", line=dict(color="#d4a017", width=2),
            name="Equity",
        ))
        fig_eq.add_hline(y=1.0, line_dash="dash", line_color="#666")
        fig_eq.update_layout(**PLOT_LAYOUT, height=350, title="Equity Curve",
                             yaxis_title="Growth of $1")
        st.plotly_chart(fig_eq, width='stretch')

        # Trade log
        st.subheader("Trade Log")
        display = trades_df[["entry_date", "exit_date", "direction", "entry_price",
                              "exit_price", "pnl_pct", "days_held", "exit_reason",
                              "entry_state", "exit_state"]].copy()
        display["pnl_pct"] = display["pnl_pct"].map(lambda x: f"{x:+.2%}")
        st.dataframe(display, width='stretch', hide_index=True)


# ---------------------------------------------------------------------------
# Tab 3: OI Scanner
# ---------------------------------------------------------------------------

def _render_scanner_tab():
    st.subheader("OI Signal Scanner")

    filter_mode = st.radio("Show", ["All signals", "BUY/SELL only"], horizontal=True)

    if st.button("Scan All Futures Symbols", type="primary"):
        with st.spinner("Scanning... (this may take a minute)"):
            scan_df = scan_oi_signals(days=30)

        if scan_df.empty:
            st.info("No active OI signals found.")
            return

        if filter_mode == "BUY/SELL only":
            scan_df = scan_df[scan_df["signal"].isin(["BUY", "SELL"])]
            if scan_df.empty:
                st.info("No BUY/SELL signals right now.")
                return

        st.success(f"Found **{len(scan_df)}** signals across futures symbols")

        # Color-code the table
        def _color_state(val):
            return f"color: {STATE_COLORS.get(val, '#8b949e')}"

        def _color_signal(val):
            return f"color: {SIGNAL_COLORS.get(val, '#8b949e')}"

        display = scan_df[["symbol", "date", "state", "signal", "confidence", "streak",
                            "oi", "oi_change_pct", "price_change_pct", "basis_pct",
                            "days_to_expiry", "in_rollover", "spot_price", "futures_price"]].copy()

        styled = display.style.map(
            _color_state, subset=["state"]
        ).map(
            _color_signal, subset=["signal"]
        ).format({
            "confidence": "{:.0%}",
            "oi": "{:,.0f}",
            "oi_change_pct": "{:+.2%}",
            "price_change_pct": "{:+.2%}",
            "basis_pct": "{:+.2f}%",
            "spot_price": "{:,.2f}",
            "futures_price": "{:,.2f}",
        })

        st.dataframe(styled, width='stretch', hide_index=True, height=600)

        # Summary
        buys = len(scan_df[scan_df["signal"] == "BUY"])
        sells = len(scan_df[scan_df["signal"] == "SELL"])
        st.caption(f"**{buys}** BUY | **{sells}** SELL | "
                   f"**{len(scan_df) - buys - sells}** HOLD/EXIT")


# ---------------------------------------------------------------------------
# Tab 4: Rollover & Research
# ---------------------------------------------------------------------------

def _render_research_tab():
    st.subheader("Rollover & Research")

    # Rollover calendar
    st.markdown("### Rollover Calendar")
    cal = get_rollover_calendar(6)

    def _color_status(val):
        if val == "EXPIRED":
            return "color: #6b7280"
        elif val == "ROLLOVER":
            return "color: #eab308"
        return "color: #22c55e"

    styled_cal = cal.style.map(_color_status, subset=["status"])
    st.dataframe(styled_cal, width='stretch', hide_index=True)

    # OI Matrix explanation
    st.markdown("### OI Interpretation Matrix")
    matrix_data = {
        "": ["Price UP", "Price DOWN"],
        "OI UP": ["LONG BUILDUP (BUY)", "SHORT BUILDUP (SELL)"],
        "OI DOWN": ["SHORT COVERING (EXIT LONG)", "LONG UNWINDING (EXIT SHORT)"],
    }
    st.table(pd.DataFrame(matrix_data).set_index(""))

    # State transition analysis
    st.markdown("### Signal Distribution")
    symbols = _get_futures_symbols()[:10]

    if st.button("Analyze Top 10 Symbols"):
        all_states = []
        for sym in symbols:
            oi_df = load_oi_data(sym, days=60)
            if oi_df.empty:
                continue
            sigs = compute_oi_signals(oi_df, min_streak=1)
            for s in sigs:
                all_states.append({"symbol": sym, "state": s.state.value, "signal": s.signal})

        if all_states:
            states_df = pd.DataFrame(all_states)

            # State distribution
            fig_dist = go.Figure(go.Bar(
                x=states_df["state"].value_counts().index.tolist(),
                y=states_df["state"].value_counts().values.tolist(),
                marker_color=[STATE_COLORS.get(s, "#666") for s in states_df["state"].value_counts().index],
            ))
            fig_dist.update_layout(**PLOT_LAYOUT, height=300, title="OI State Distribution (Top 10 symbols, 60d)")
            st.plotly_chart(fig_dist, width='stretch')

            # Transition matrix
            transitions = {}
            for sym in states_df["symbol"].unique():
                sub = states_df[states_df["symbol"] == sym]["state"].tolist()
                for i in range(1, len(sub)):
                    key = (sub[i - 1], sub[i])
                    transitions[key] = transitions.get(key, 0) + 1

            if transitions:
                all_st = sorted(set(s for pair in transitions for s in pair))
                matrix = pd.DataFrame(0, index=all_st, columns=all_st)
                for (from_s, to_s), cnt in transitions.items():
                    matrix.loc[from_s, to_s] = cnt
                # Normalize rows
                row_sums = matrix.sum(axis=1).replace(0, 1)
                matrix_pct = matrix.div(row_sums, axis=0)

                fig_tm = go.Figure(go.Heatmap(
                    z=matrix_pct.values,
                    x=matrix_pct.columns.tolist(),
                    y=matrix_pct.index.tolist(),
                    text=matrix_pct.values.round(2),
                    texttemplate="%{text:.0%}",
                    colorscale="YlOrRd",
                ))
                fig_tm.update_layout(**PLOT_LAYOUT, height=400,
                                     title="State Transition Probabilities",
                                     xaxis_title="To State", yaxis_title="From State")
                st.plotly_chart(fig_tm, width='stretch')

    # Methodology
    with st.expander("Methodology"):
        st.markdown("""
**OI Buildup/Unwind** is a classic derivatives interpretation framework:

- **Long Buildup** (Price UP + OI UP): New longs entering the market. Strong bullish.
- **Short Covering** (Price UP + OI DOWN): Shorts exiting. Rally may be exhausting.
- **Short Buildup** (Price DOWN + OI UP): New shorts entering. Strong bearish.
- **Long Unwinding** (Price DOWN + OI DOWN): Longs exiting. Decline may be exhausting.

**PSX Edge:** Physical delivery means OI represents real share commitment, not just speculation.

**Enhancements:**
- Multi-day streak confirmation (default 2 days)
- Volume filter (high volume = higher conviction)
- Rollover awareness (reduced confidence near expiry)
- Basis confirmation (futures premium/discount)
- OI percentile (crowded vs low participation)
""")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def render_strategy_oi():
    st.title("OI Buildup/Unwind Strategy")
    st.caption("Derivatives OI matrix signals -- Long Buildup, Short Buildup, Covering, Unwinding")

    tab1, tab2, tab3, tab4 = st.tabs([
        "OI Matrix Live", "Backtest", "OI Scanner", "Rollover & Research",
    ])

    with tab1:
        _render_live_tab()
    with tab2:
        _render_backtest_tab()
    with tab3:
        _render_scanner_tab()
    with tab4:
        _render_research_tab()
