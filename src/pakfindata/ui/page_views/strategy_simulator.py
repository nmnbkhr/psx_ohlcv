"""Strategy Fusion Simulator -- real-time decision engine.

Uses Streamlit's native rendering with st.empty() containers for
live updates. No external API needed — computes directly in Python
and updates containers in-place.
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from pathlib import Path
from dataclasses import asdict
import time

from pakfindata.ui.components.helpers import render_footer

DATA_ROOT = Path("/mnt/e/psxdata")
DUCKDB_PATH = "/mnt/e/psxdata/pakfindata.duckdb"

STRATEGIES = {
    "REGIME": [
        ("macro_hmm", "Macro Regime HMM", True),
        ("sector_rotation", "Sector Rotation", True),
    ],
    "FLOW": [
        ("vpin", "VPIN Toxicity", True),
        ("ofi", "OFI Alpha", True),
        ("cvd", "CVD Divergence", False),
        ("oi_buildup", "OI Buildup", True),
    ],
    "STRUCTURE": [
        ("basis_arb", "Basis Arb", True),
        ("pairs_trading", "Pairs Trading", False),
    ],
    "ALPHA": [
        ("ml_predictions", "ML Predictions", False),
        ("sentiment", "LLM Sentiment", False),
    ],
    "RESEARCH": [
        ("hawkes", "Hawkes Process", False),
        ("vwap", "VWAP Context", False),
    ],
}

_C = {
    "bg": "#0B0E11", "card": "#141820",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3", "gold": "#C8A96E",
}
DARK_BG = "rgba(0,0,0,0)"


def _fetch_latest_price(symbol: str) -> float:
    import duckdb
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    try:
        row = con.execute(
            "SELECT price FROM tick_logs WHERE symbol = ? AND price > 0 ORDER BY timestamp DESC LIMIT 1",
            [symbol],
        ).fetchone()
        if row:
            return float(row[0])
        row = con.execute(
            "SELECT close FROM eod_ohlcv WHERE symbol = ? AND close > 0 ORDER BY date DESC LIMIT 1",
            [symbol],
        ).fetchone()
        if row:
            return float(row[0])
    finally:
        con.close()
    return 0.0


def _kpi_html(label, value, color=None):
    c = color or _C["text"]
    return f"""<div style="background:{_C['card']};padding:10px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.65em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.1em;font-weight:700;">{value}</div>
    </div>"""


def _render_decision(d, container):
    """Render the full fusion decision into a container."""
    with container:
        dec_color = _C["up"] if "BUY" in d.decision else (_C["down"] if "SELL" in d.decision else _C["dim"])

        # Decision header
        st.markdown(f"""<div style="text-align:center;padding:15px 0;">
            <div style="color:{_C['dim']};font-size:0.75em;text-transform:uppercase;">FUSION DECISION</div>
            <div style="color:{dec_color};font-size:2.8em;font-weight:900;letter-spacing:3px;">{d.decision}</div>
            <div style="color:{_C['dim']};font-size:0.8em;margin-top:4px;">
                Score: {d.raw_score*100:.1f} | {d.agreeing_count} agree, {d.conflicting_count} conflict
                {f' | VETOED: {d.veto_reason}' if d.vetoed else ''}
            </div>
        </div>""", unsafe_allow_html=True)

        # Confidence bar
        st.progress(max(0, min(int(d.confidence), 100)))

        # Category KPIs
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        with k1:
            st.markdown(_kpi_html("Regime", f"{d.regime_score:+.2f}", _C["accent"]), unsafe_allow_html=True)
        with k2:
            st.markdown(_kpi_html("Flow", f"{d.flow_score:+.2f}", _C["cyan"]), unsafe_allow_html=True)
        with k3:
            st.markdown(_kpi_html("Structure", f"{d.structure_score:+.2f}", _C["gold"]), unsafe_allow_html=True)
        with k4:
            st.markdown(_kpi_html("Alpha", f"{d.alpha_score:+.2f}", "#BB86FC"), unsafe_allow_html=True)
        with k5:
            st.markdown(_kpi_html("Size", f"{d.suggested_size:,}", _C["text"]), unsafe_allow_html=True)
        with k6:
            st.markdown(_kpi_html("Price", f"{d.price:,.2f}", _C["text"]), unsafe_allow_html=True)

        # Signal heatmap
        st.markdown("##### Strategy Votes")
        cols = st.columns(6)
        for i, v in enumerate(d.votes):
            with cols[i % 6]:
                if not v["enabled"]:
                    bg, border = "#0B0E11", "#1E2530"
                    txt_color = "#374151"
                elif v["direction"] > 0:
                    bg, border = "rgba(0,230,118,0.2)", "#00E676"
                    txt_color = "#00E676"
                elif v["direction"] < 0:
                    bg, border = "rgba(255,82,82,0.2)", "#FF5252"
                    txt_color = "#FF5252"
                else:
                    bg, border = "rgba(107,114,128,0.15)", "#374151"
                    txt_color = "#9CA3AF"

                name = v["name"].replace("_", " ").upper()
                sig = v["signal"][:25]
                conf = f"{v['confidence']*100:.0f}%"
                st.markdown(f"""<div style="background:{bg};border:1px solid {border};
                    border-radius:4px;padding:6px;text-align:center;margin-bottom:4px;">
                    <div style="color:{txt_color};font-size:0.7em;font-weight:700;">{name}</div>
                    <div style="color:{_C['dim']};font-size:0.6em;margin-top:2px;">{sig}</div>
                    <div style="color:{txt_color};font-size:0.6em;">{conf}</div>
                </div>""", unsafe_allow_html=True)


def _render_portfolio(engine, container):
    """Render portfolio state."""
    with container:
        state = engine.get_state()
        p = state["portfolio"]

        k1, k2, k3, k4, k5 = st.columns(5)
        pnl_c = _C["up"] if p["total_pnl"] > 0 else (_C["down"] if p["total_pnl"] < 0 else _C["dim"])
        with k1:
            st.markdown(_kpi_html("Total P&L", f"{p['total_pnl']:+,.0f}", pnl_c), unsafe_allow_html=True)
        with k2:
            st.markdown(_kpi_html("Realized", f"{p['realized_pnl']:+,.0f}"), unsafe_allow_html=True)
        with k3:
            st.markdown(_kpi_html("Unrealized", f"{p['unrealized_pnl']:+,.0f}"), unsafe_allow_html=True)
        with k4:
            st.markdown(_kpi_html("Trades", str(p["trade_count"])), unsafe_allow_html=True)
        with k5:
            st.markdown(_kpi_html("Win Rate", f"{p['win_rate']:.0f}%"), unsafe_allow_html=True)

        # Equity curve
        if p["equity_curve"]:
            eq = pd.DataFrame(p["equity_curve"])
            fig = go.Figure()
            color = _C["up"] if eq["pnl"].iloc[-1] >= 0 else _C["down"]
            fig.add_trace(go.Scatter(
                x=eq["timestamp"], y=eq["pnl"], mode="lines",
                line=dict(color=color, width=2),
                fill="tozeroy", fillcolor=f"rgba({','.join(str(int(color.lstrip('#')[i:i+2], 16)) for i in (0,2,4))},0.1)",
            ))
            fig.update_layout(
                paper_bgcolor=DARK_BG, plot_bgcolor=DARK_BG,
                font_color="#c9d1d9", margin=dict(l=20, r=20, t=30, b=20),
                height=200, title_text="Equity Curve (PKR)", title_font_size=11,
                xaxis=dict(showgrid=False), yaxis=dict(gridcolor="#1E2530"),
            )
            st.plotly_chart(fig, use_container_width=True)

        # Positions
        col1, col2 = st.columns([2, 1])
        with col1:
            if p["positions"]:
                st.markdown("##### Open Positions")
                pos_df = pd.DataFrame(p["positions"])
                show = [c for c in ["symbol", "side", "entry_price", "current_price",
                                    "unrealized_pnl", "unrealized_pnl_pct"] if c in pos_df.columns]
                st.dataframe(pos_df[show].round(2), use_container_width=True, hide_index=True)
            else:
                st.caption("No open positions")

        with col2:
            if state["trade_log"]:
                st.markdown("##### Recent Trades")
                log_df = pd.DataFrame(state["trade_log"][-10:])
                show = [c for c in ["time", "action", "symbol", "shares", "price", "pnl"] if c in log_df.columns]
                st.dataframe(log_df[show].round(2), use_container_width=True, hide_index=True)


def render_page():
    st.markdown("### Strategy Fusion Simulator")
    st.caption("All 14 strategies fused into one real-time decision engine")

    # Sidebar
    with st.sidebar:
        st.markdown("#### Simulator Config")
        symbol = st.text_input("Symbol", "OGDC", key="sim_symbol")
        capital = st.number_input("Capital (PKR)", 100_000, 10_000_000, 1_000_000, 100_000, key="sim_capital")

        st.markdown("---")
        st.markdown("#### Strategies")

        enabled = {}
        for category, strats in STRATEGIES.items():
            st.markdown(f"**{category}**")
            for key, label, default in strats:
                enabled[key] = st.checkbox(label, value=default, key=f"sim_{key}")

        st.markdown("---")

        col1, col2 = st.columns(2)
        with col1:
            n_rounds = st.number_input("Rounds", 1, 100, 5, key="sim_rounds")
        with col2:
            interval = st.number_input("Interval (s)", 1, 60, 10, key="sim_interval")

        start = st.button("RUN SIMULATOR", type="primary", use_container_width=True)

    # Show info if not started
    if not start and "fusion_results" not in st.session_state:
        st.info("Configure in sidebar and click **RUN SIMULATOR**.")
        st.markdown("""
        ---
        #### How It Works

        1. Click **RUN** — simulator fetches latest tick price from DuckDB
        2. Runs all enabled strategies, fuses votes into BUY/SELL/HOLD
        3. Updates virtual portfolio (stop loss 2%, take profit 4%)
        4. Repeats for N rounds, refreshing price each time
        5. Shows live decision, signal heatmap, equity curve, positions

        | Category | Weight | Strategies |
        |----------|--------|-----------|
        | **REGIME** | 30% | Macro HMM, Sector Rotation |
        | **FLOW** | 30% | VPIN, OFI, CVD, OI Buildup |
        | **STRUCTURE** | 20% | Basis Arb, Pairs Trading |
        | **ALPHA** | 15% | ML Predictions, LLM Sentiment |
        | **RESEARCH** | 5% | Hawkes Process, VWAP Context |
        """)
        render_footer()
        return

    if start:
        # Initialize engine
        from pakfindata.engine.strategy_fusion import StrategyFusionEngine
        engine = StrategyFusionEngine(capital=capital)
        engine.set_enabled(enabled)

        sym = symbol.upper()
        n_enabled = sum(enabled.values())

        st.success(f"Running: **{sym}** | {n_enabled} strategies | {n_rounds} rounds @ {interval}s")

        # Create placeholder containers
        status_bar = st.empty()
        decision_container = st.container()
        portfolio_container = st.container()

        # Run simulation loop
        for i in range(n_rounds):
            # Status
            status_bar.progress((i + 1) / n_rounds,
                                text=f"Round {i+1}/{n_rounds} — fetching {sym} price & computing...")

            # Fetch latest price
            price = _fetch_latest_price(sym)
            if price <= 0:
                status_bar.warning(f"No price for {sym}")
                break

            # Compute fusion
            decision = engine.compute(sym, price)
            engine.update_portfolio(decision)

            # Render decision (replaces previous content)
            with decision_container:
                _render_decision(decision, decision_container)

            # Render portfolio
            with portfolio_container:
                _render_portfolio(engine, portfolio_container)

            # Wait between rounds (except last)
            if i < n_rounds - 1:
                for sec in range(interval):
                    status_bar.progress(
                        (i + 1) / n_rounds,
                        text=f"Round {i+1}/{n_rounds} done. Next in {interval - sec}s..."
                    )
                    time.sleep(1)

        status_bar.success(f"Simulation complete: {n_rounds} rounds")

        # Save results for history tab
        st.session_state["fusion_results"] = engine.get_state()
        st.session_state["fusion_engine"] = engine

    # Show last results if available
    elif "fusion_results" in st.session_state:
        state = st.session_state["fusion_results"]
        engine = st.session_state.get("fusion_engine")

        if state.get("decision"):
            from pakfindata.engine.strategy_fusion import FusionDecision
            d_dict = state["decision"]

            # Rebuild decision object for rendering
            class _D:
                pass
            d = _D()
            for k, v in d_dict.items():
                setattr(d, k, v)

            container = st.container()
            _render_decision(d, container)

        if engine:
            container2 = st.container()
            _render_portfolio(engine, container2)

    render_footer()
