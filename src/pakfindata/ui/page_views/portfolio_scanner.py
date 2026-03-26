"""Portfolio Scanner -- multi-symbol signal scanner with realistic execution.

Reads portfolio_state.json, renders signal scanner table + portfolio.
"""

import json
import os
import time as _time
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pakfindata.ui.components.helpers import render_footer

DATA_ROOT = Path("/mnt/e/psxdata")
PORTFOLIO_STATE = DATA_ROOT / "portfolio_state.json"

_C = {"up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
      "dim": "#6B7280", "card": "#141820", "border": "#1E2530", "cyan": "#00BCD4"}


def _load():
    if not PORTFOLIO_STATE.exists():
        return None
    for _ in range(2):
        try:
            return json.loads(PORTFOLIO_STATE.read_text())
        except (json.JSONDecodeError, IOError):
            _time.sleep(0.3)
    return None


def _age():
    try:
        return _time.time() - os.path.getmtime(PORTFOLIO_STATE)
    except OSError:
        return 999


def _render_service_control():
    try:
        from pakfindata.services.portfolio_simulator import (
            is_portfolio_sim_running, start_portfolio_sim_background, stop_portfolio_sim,
        )
    except ImportError:
        st.caption("portfolio_simulator not available")
        return

    running, pid = is_portfolio_sim_running()
    if running:
        if st.button(f"Stop (PID {pid})", key="ps_stop", type="secondary"):
            ok, msg = stop_portfolio_sim()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    else:
        if st.button("Start Scanner", key="ps_start", type="primary"):
            ok, msg = start_portfolio_sim_background()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()


def render_page():
    age = _age()
    if age < 60 and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=10000, limit=None, key="pscan_refresh")

    h1, h2 = st.columns([3, 1])
    with h1:
        st.markdown("### Portfolio Scanner")
        st.caption("Multi-symbol signal scanner with realistic execution")
    with h2:
        _render_service_control()

    data = _load()
    if data is None:
        st.info("Portfolio simulator not running. Click **Start Scanner** or run:\n\n"
                "```\npython -m pakfindata.services.portfolio_simulator\n```")
        render_footer()
        return

    portfolio = data.get("portfolio", {})
    scan_results = data.get("scan_results", [])

    # Status KPIs
    s1, s2, s3, s4, s5, s6 = st.columns(6)
    running = data.get("running", False) and age < 60
    s1.metric("Status", "LIVE" if running else "DOWN")

    pnl = portfolio.get("pnl", 0)
    s2.metric("P&L", f"{pnl:+,.0f}")
    s3.metric("Equity", f"{portfolio.get('equity', 0):,.0f}")
    s4.metric("Win Rate", f"{portfolio.get('win_rate', 0):.0f}%")
    s5.metric("Positions", len(portfolio.get("positions", [])))
    s6.metric("Scanned", len(scan_results))

    st.divider()

    tab1, tab2, tab3 = st.tabs(["Signal Scanner", "Positions & Trades", "Signal Explainer"])

    with tab1:
        if not scan_results:
            st.info("No scan results yet.")
        else:
            rows = []
            for r in scan_results:
                dec = r.get("decision", {})
                exp = r.get("explanation", {})
                top_votes = r.get("top_votes", [])
                signals_str = " | ".join(
                    ("+" if v["direction"] > 0 else "-" if v["direction"] < 0 else "=") + v["label"]
                    for v in top_votes[:3]
                )
                rows.append({
                    "Symbol": r["symbol"],
                    "Price": r["price"],
                    "Chg%": r.get("change_pct", 0),
                    "Spread": r.get("spread_bps", 0),
                    "Decision": dec.get("decision", "HOLD"),
                    "Score": dec.get("raw_score", 0) * 100,
                    "Conf%": dec.get("confidence", 0),
                    "Agree": dec.get("agree", 0),
                    "Conflict": dec.get("conflict", 0),
                    "Top Signals": signals_str,
                    "Why": exp.get("tipping_factor", "")[:40],
                })

            df = pd.DataFrame(rows)

            def color_decision(val):
                if "BUY" in str(val):
                    return "color: #00E676; font-weight: bold"
                elif "SELL" in str(val):
                    return "color: #FF5252; font-weight: bold"
                return "color: #6B7280"

            def color_score(val):
                if val > 15:
                    return "color: #00E676"
                elif val < -15:
                    return "color: #FF5252"
                return ""

            styled = df.style.map(color_decision, subset=["Decision"]) \
                             .map(color_score, subset=["Score"]) \
                             .format({
                                 "Price": "{:,.2f}", "Chg%": "{:+.2f}%",
                                 "Spread": "{:.0f}bps", "Score": "{:+.0f}",
                                 "Conf%": "{:.0f}%",
                             })
            st.dataframe(styled, use_container_width=True, hide_index=True, height=500)

    with tab2:
        st.markdown("**Open Positions**")
        positions = portfolio.get("positions", [])
        if positions:
            pos_df = pd.DataFrame(positions)
            show = [c for c in ["symbol", "side", "entry_price", "current_price",
                                "shares", "pnl", "pnl_pct", "entry_reason", "explanation_summary"]
                    if c in pos_df.columns]
            st.dataframe(pos_df[show].round(2), use_container_width=True, hide_index=True)
        else:
            st.caption("No open positions")

        st.markdown("---")
        st.markdown("**Closed Trades**")
        closed = portfolio.get("closed", [])
        if closed:
            trade_df = pd.DataFrame(closed[-15:])
            show = [c for c in ["exit_time", "side", "symbol", "shares", "entry_price",
                                "exit_price", "pnl", "exit_reason"]
                    if c in trade_df.columns]
            st.dataframe(trade_df[show].round(2), use_container_width=True, hide_index=True)
        else:
            st.caption("No trades yet")

    with tab3:
        st.markdown("**Signal Explainer** -- select a symbol to see full explanation")
        if scan_results:
            symbols = [r["symbol"] for r in scan_results[:20]]
            selected = st.selectbox("Symbol", symbols, key="explain_sym")

            result = next((r for r in scan_results if r["symbol"] == selected), None)
            if result:
                exp = result.get("explanation", {})
                dec = result.get("decision", {})

                d = dec.get("decision", "HOLD")
                st.markdown(f"#### {d}")
                st.caption(f"Score: {dec.get('raw_score',0)*100:+.0f} | Confidence: {dec.get('confidence',0):.0f}%")

                st.markdown(f"**Why:** {exp.get('summary', '--')}")
                st.markdown(f"**Tipping factor:** {exp.get('tipping_factor', '--')}")

                if exp.get("vetoed"):
                    st.warning(f"VETOED: {exp.get('veto_reason', '')}")

                bc1, bc2 = st.columns(2)
                with bc1:
                    st.markdown("**Bull Case**")
                    for b in exp.get("bull_case", []):
                        st.markdown(f"- **{b['name']}**: {b['signal']} ({b['confidence']:.0%}) [{b['contribution']:+.1f}]")
                    if not exp.get("bull_case"):
                        st.caption("No bullish signals")
                with bc2:
                    st.markdown("**Bear Case**")
                    for b in exp.get("bear_case", []):
                        st.markdown(f"- **{b['name']}**: {b['signal']} ({b['confidence']:.0%}) [{b['contribution']:+.1f}]")
                    if not exp.get("bear_case"):
                        st.caption("No bearish signals")

                if exp.get("conflicts"):
                    st.markdown("**Conflicts:**")
                    for c in exp["conflicts"]:
                        st.caption(c)

    render_footer()
