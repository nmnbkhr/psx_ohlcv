"""Strategy Fusion Simulator -- real-time decision engine.

Uses Streamlit's native rendering with st.empty() containers for
live updates. No external API needed — computes directly in Python
and updates containers in-place.
"""

import streamlit as st
import pandas as pd
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


def _render_decision(d, container):
    """Render the full fusion decision into a container."""
    with container:
        dec_color = _C["up"] if "BUY" in d.decision else (_C["down"] if "SELL" in d.decision else _C["dim"])

        # Decision header — no nested f-strings
        veto_text = ""
        if d.vetoed:
            veto_text = " | VETOED: " + str(d.veto_reason)
        score_text = "Score: {:.1f} | {} agree, {} conflict{}".format(
            d.raw_score * 100, d.agreeing_count, d.conflicting_count, veto_text
        )

        st.markdown("#### " + d.decision)
        st.caption(score_text)
        st.progress(max(0, min(int(d.confidence), 100)))

        # Category KPIs
        k1, k2, k3, k4, k5, k6 = st.columns(6)
        with k1:
            st.metric("REGIME", f"{d.regime_score:+.2f}")
        with k2:
            st.metric("FLOW", f"{d.flow_score:+.2f}")
        with k3:
            st.metric("STRUCTURE", f"{d.structure_score:+.2f}")
        with k4:
            st.metric("ALPHA", f"{d.alpha_score:+.2f}")
        with k5:
            st.metric("SIZE", f"{d.suggested_size:,}")
        with k6:
            st.metric("PRICE", f"{d.price:,.2f}")

        # Signal heatmap as dataframe (reliable rendering)
        st.markdown("##### Strategy Votes")
        vote_rows = []
        for v in d.votes:
            dir_map = {1: "LONG", -1: "SHORT", 0: "--"}
            vote_rows.append({
                "Strategy": v["name"].replace("_", " ").title(),
                "Category": v["category"],
                "Status": "ON" if v["enabled"] else "OFF",
                "Direction": dir_map.get(v["direction"], "--"),
                "Confidence": f"{v['confidence']*100:.0f}%",
                "Signal": str(v["signal"])[:35],
                "Weight": f"{v['weight']:.0%}",
            })
        st.dataframe(pd.DataFrame(vote_rows), use_container_width=True, hide_index=True)


def _render_portfolio(engine, container):
    """Render portfolio state."""
    with container:
        state = engine.get_state()
        p = state["portfolio"]

        st.markdown("---")
        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            st.metric("Total P&L", f"{p['total_pnl']:+,.0f}")
        with k2:
            st.metric("Realized", f"{p['realized_pnl']:+,.0f}")
        with k3:
            st.metric("Unrealized", f"{p['unrealized_pnl']:+,.0f}")
        with k4:
            st.metric("Trades", str(p["trade_count"]))
        with k5:
            st.metric("Win Rate", f"{p['win_rate']:.0f}%")

        # Equity curve
        if len(p["equity_curve"]) > 1:
            eq = pd.DataFrame(p["equity_curve"])
            st.line_chart(eq.set_index("timestamp")["pnl"], height=200)

        # Positions and trades side by side
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
