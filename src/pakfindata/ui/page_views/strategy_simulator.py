"""Strategy Fusion Simulator -- unified real-time decision engine.

Embeds a JS panel that connects to ws_relay for real-time ticks
and fetches fusion signals from the FastAPI endpoint.
Fallback: Streamlit-only mode with manual compute button.
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
from pathlib import Path
from dataclasses import asdict

from pakfindata.ui.components.helpers import render_footer

DATA_ROOT = Path("/mnt/e/psxdata")

STRATEGIES = {
    "REGIME": [
        ("macro_hmm", "Macro Regime HMM", True),
        ("sector_rotation", "Sector Rotation", True),
    ],
    "FLOW": [
        ("vpin", "VPIN Toxicity", True),
        ("ofi", "OFI Alpha", True),
        ("cvd", "CVD Divergence", True),
        ("oi_buildup", "OI Buildup", True),
    ],
    "STRUCTURE": [
        ("basis_arb", "Basis Arb", True),
        ("pairs_trading", "Pairs Trading", True),
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

PANEL_HTML = Path(__file__).parent / "simulator_panel.html"

_C = {
    "bg": "#0B0E11", "card": "#141820", "grid": "#1a1f2e",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "accent": "#2196F3", "gold": "#C8A96E",
}


def _kpi(label, value, color=None):
    c = color or _C["text"]
    st.markdown(f"""
    <div style="background:{_C['card']};padding:10px;border-radius:6px;text-align:center;">
        <div style="color:{_C['dim']};font-size:0.65em;text-transform:uppercase;">{label}</div>
        <div style="color:{c};font-size:1.2em;font-weight:700;">{value}</div>
    </div>
    """, unsafe_allow_html=True)


def render_page():
    st.markdown("### Strategy Fusion Simulator")
    st.caption("All strategies -> one decision. Real-time virtual portfolio.")

    # Sidebar controls
    with st.sidebar:
        st.markdown("#### Simulator Config")
        symbol = st.text_input("Symbol", "OGDC", key="sim_symbol")
        capital = st.number_input("Capital (PKR)", 100_000, 10_000_000, 1_000_000, 100_000, key="sim_capital")
        mode = st.selectbox("Mode", ["Streamlit (Manual)", "Live (WebSocket)"], key="sim_mode")

        if mode == "Live (WebSocket)":
            ws_host = st.text_input("WS Relay", "ws://localhost:8765", key="sim_ws")
            api_host = st.text_input("API URL", "http://localhost:8765", key="sim_api")

        st.markdown("---")
        st.markdown("#### Enable Strategies")

        enabled = {}
        for category, strats in STRATEGIES.items():
            st.markdown(f"**{category}**")
            for key, label, default in strats:
                enabled[key] = st.checkbox(label, value=default, key=f"sim_{key}")

        st.markdown("---")
        start = st.button("START", type="primary", use_container_width=True)

    # Initialize engine
    if start:
        try:
            from pakfindata.engine.strategy_fusion import StrategyFusionEngine
            engine = StrategyFusionEngine(capital=capital)
            engine.set_enabled(enabled)
            st.session_state["fusion_engine"] = engine
            st.session_state["fusion_decisions"] = []
        except Exception as e:
            st.error(f"Failed to init engine: {e}")
            return

    engine = st.session_state.get("fusion_engine")

    # Live mode: embedded HTML panel
    if mode == "Live (WebSocket)" and PANEL_HTML.exists() and engine:
        html = PANEL_HTML.read_text()
        html = html.replace("WS_URL_PLACEHOLDER", ws_host)
        html = html.replace("API_URL_PLACEHOLDER", api_host)
        html = html.replace("SYMBOL_PLACEHOLDER", symbol.upper())
        components.html(html, height=900, scrolling=False)
        render_footer()
        return

    # Streamlit-only mode
    if not engine:
        st.info("Configure settings in the sidebar and click **START** to initialize the simulator.")

        # Show architecture overview
        st.markdown("""
        ---
        #### How It Works

        The Strategy Fusion Simulator orchestrates **all 14 strategy engines** into a single decision:

        | Category | Weight | Strategies |
        |----------|--------|-----------|
        | **REGIME** | 30% | Macro HMM, Sector Rotation |
        | **FLOW** | 30% | VPIN, OFI, CVD, OI Buildup |
        | **STRUCTURE** | 20% | Basis Arb, Pairs Trading |
        | **ALPHA** | 15% | ML Predictions, LLM Sentiment |
        | **RESEARCH** | 5% | Hawkes Process, VWAP Context |

        **Decision Logic:**
        1. Each enabled strategy votes: direction (-1/0/+1) x confidence x weight
        2. Weighted votes summed and normalized
        3. Score > +0.15 = BUY, > +0.30 = STRONG_BUY
        4. VPIN TOXIC vetoes all buy signals
        5. Hawkes BURST halves position size

        **Virtual Portfolio:**
        - Stop loss: 2%, Take profit: 4%
        - Max 10 concurrent positions
        - Max 5% capital per position
        """)
        render_footer()
        return

    # Active simulator
    n_enabled = sum(enabled.values())
    st.success(f"Simulator active: **{symbol}** | {n_enabled} strategies enabled | Capital: {capital:,.0f} PKR")

    tab_live, tab_history, tab_portfolio, tab_methodology = st.tabs([
        "Live Signals", "Decision History", "Portfolio", "Methodology"
    ])

    with tab_live:
        c1, c2 = st.columns([3, 1])
        with c1:
            price_input = st.number_input("Current Price", 0.01, 100000.0, 100.0, 0.01, key="sim_price")
        with c2:
            st.write("")
            st.write("")
            compute_btn = st.button("Compute Fusion Signal", type="primary", key="sim_compute")

        if compute_btn:
            progress = st.progress(0, text="Gathering strategy votes...")
            try:
                decision = engine.compute(symbol.upper(), price_input)
                progress.progress(80, text="Updating portfolio...")
                engine.update_portfolio(decision)
                progress.progress(100, text="Done!")
                progress.empty()

                if "fusion_decisions" not in st.session_state:
                    st.session_state["fusion_decisions"] = []
                st.session_state["fusion_decisions"].append(asdict(decision))

            except Exception as e:
                progress.empty()
                st.error(f"Error: {e}")
                return

            # Decision display
            d = decision
            dec_color = _C["up"] if "BUY" in d.decision else (_C["down"] if "SELL" in d.decision else _C["dim"])

            st.markdown(f"""
            <div style="background:{_C['card']};padding:20px;border-radius:8px;text-align:center;margin:10px 0;">
                <div style="color:{_C['dim']};font-size:0.8em;">FUSION DECISION</div>
                <div style="color:{dec_color};font-size:2.5em;font-weight:900;letter-spacing:2px;">{d.decision}</div>
                <div style="color:{_C['dim']};margin-top:4px;">
                    Score: {d.raw_score*100:.1f} | {d.agreeing_count} agree, {d.conflicting_count} conflict
                    {f' | VETOED: {d.veto_reason}' if d.vetoed else ''}
                </div>
                <div style="height:6px;background:#1E2530;border-radius:3px;margin-top:8px;">
                    <div style="height:100%;width:{d.confidence:.0f}%;background:{dec_color};border-radius:3px;"></div>
                </div>
                <div style="color:{_C['dim']};font-size:0.7em;margin-top:4px;">Confidence: {d.confidence:.0f}%</div>
            </div>
            """, unsafe_allow_html=True)

            # KPI row
            k1, k2, k3, k4, k5, k6 = st.columns(6)
            with k1:
                _kpi("Regime", f"{d.regime_score:+.2f}", _C["accent"])
            with k2:
                _kpi("Flow", f"{d.flow_score:+.2f}", _C["cyan"])
            with k3:
                _kpi("Structure", f"{d.structure_score:+.2f}", _C["gold"])
            with k4:
                _kpi("Alpha", f"{d.alpha_score:+.2f}", "#BB86FC")
            with k5:
                _kpi("Size", f"{d.suggested_size:,} sh")
            with k6:
                _kpi("Size %", f"{d.suggested_size_pct:.1f}%")

            # Signal heatmap
            st.markdown("#### Strategy Votes")
            vote_data = []
            for v in d.votes:
                dir_str = "LONG" if v["direction"] > 0 else ("SHORT" if v["direction"] < 0 else "--")
                status = "ON" if v["enabled"] else "OFF"
                vote_data.append({
                    "Strategy": v["name"],
                    "Category": v["category"],
                    "Status": status,
                    "Direction": dir_str,
                    "Confidence": f"{v['confidence']:.0%}",
                    "Signal": v["signal"][:40],
                    "Weight": f"{v['weight']:.0%}",
                })
            st.dataframe(pd.DataFrame(vote_data), use_container_width=True, hide_index=True)

    with tab_history:
        decisions = st.session_state.get("fusion_decisions", [])
        if decisions:
            st.markdown(f"**{len(decisions)} decisions computed this session**")
            hist_df = pd.DataFrame([{
                "Time": d["timestamp"],
                "Symbol": d["symbol"],
                "Price": d["price"],
                "Decision": d["decision"],
                "Score": f"{d['raw_score']:.3f}",
                "Confidence": f"{d['confidence']:.0f}%",
                "Agree/Conflict": f"{d['agreeing_count']}/{d['conflicting_count']}",
                "Vetoed": "Yes" if d["vetoed"] else "",
            } for d in decisions])
            st.dataframe(hist_df, use_container_width=True, hide_index=True)
        else:
            st.info("No decisions yet. Compute a signal in the Live Signals tab.")

    with tab_portfolio:
        state = engine.get_state()
        p = state["portfolio"]

        k1, k2, k3, k4, k5 = st.columns(5)
        with k1:
            pnl_c = _C["up"] if p["total_pnl"] > 0 else (_C["down"] if p["total_pnl"] < 0 else _C["dim"])
            _kpi("Total P&L", f"{p['total_pnl']:+,.0f}", pnl_c)
        with k2:
            _kpi("Realized", f"{p['realized_pnl']:+,.0f}")
        with k3:
            _kpi("Unrealized", f"{p['unrealized_pnl']:+,.0f}")
        with k4:
            _kpi("Trades", str(p["trade_count"]))
        with k5:
            _kpi("Win Rate", f"{p['win_rate']:.0f}%")

        if p["positions"]:
            st.markdown("#### Open Positions")
            pos_df = pd.DataFrame(p["positions"])
            display = [c for c in ["symbol", "side", "entry_price", "current_price",
                                    "unrealized_pnl", "unrealized_pnl_pct", "shares"] if c in pos_df.columns]
            st.dataframe(pos_df[display], use_container_width=True, hide_index=True)

        if p["equity_curve"]:
            st.markdown("#### Equity Curve")
            import plotly.graph_objects as go
            eq = pd.DataFrame(p["equity_curve"])
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=eq["timestamp"], y=eq["pnl"], mode="lines",
                line=dict(color=_C["cyan"], width=2),
                fill="tozeroy", fillcolor="rgba(0,188,212,0.1)",
            ))
            fig.update_layout(
                paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                font_color="#c9d1d9", margin=dict(l=20, r=20, t=20, b=20),
                height=300, yaxis_title="P&L (PKR)",
            )
            st.plotly_chart(fig, use_container_width=True)

        if state["trade_log"]:
            st.markdown("#### Trade Log")
            st.dataframe(pd.DataFrame(state["trade_log"]), use_container_width=True, hide_index=True)

    with tab_methodology:
        st.markdown("""
        ### Strategy Fusion Architecture

        **Weighted Majority Voting** with conflict resolution and veto system.

        | Category | Weight | Strategies | Role |
        |----------|--------|-----------|------|
        | **REGIME** | 30% | Macro HMM, Sector Rotation | Market environment context |
        | **FLOW** | 30% | VPIN, OFI, CVD, OI Buildup | Order flow signals |
        | **STRUCTURE** | 20% | Basis Arb, Pairs Trading | Relative value |
        | **ALPHA** | 15% | ML Predictions, LLM Sentiment | Predictive signals |
        | **RESEARCH** | 5% | Hawkes, VWAP | Risk/execution context |

        ---

        ### Decision Thresholds

        | Score Range | Decision |
        |-------------|----------|
        | > +0.30 | STRONG_BUY |
        | +0.15 to +0.30 | BUY |
        | -0.15 to +0.15 | HOLD |
        | -0.30 to -0.15 | SELL |
        | < -0.30 | STRONG_SELL |

        ---

        ### Veto System

        - **VPIN TOXIC**: Vetoes ALL buy signals when informed flow is detected
        - **Hawkes BURST**: Halves position size during activity bursts (does not veto)

        ---

        ### Virtual Portfolio Rules

        - **Position sizing**: |score| x 5% of capital
        - **Stop loss**: 2% (automatic)
        - **Take profit**: 4% (automatic)
        - **Max positions**: 10 concurrent
        - **No real money** -- simulation only
        """)

    render_footer()
