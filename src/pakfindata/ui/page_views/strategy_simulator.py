"""Strategy Fusion Simulator -- reads fusion_state.json, renders with Plotly.

Same pattern as live_ticker.py: file-based, no ports, no iframes.
"""

import json
import os
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pakfindata.ui.components.helpers import render_footer

PKT = timezone(timedelta(hours=5))
DATA_ROOT = Path("/mnt/e/psxdata")
FUSION_STATE = DATA_ROOT / "fusion_state.json"

_C = {
    "bg": "#0B0E11", "card": "#141820", "border": "#1E2530",
    "text": "#E0E0E0", "dim": "#6B7280",
    "up": "#00E676", "down": "#FF5252", "amber": "#FFB300",
    "cyan": "#00BCD4", "blue": "#2196F3", "purple": "#BB86FC",
}


def _load():
    if not FUSION_STATE.exists():
        return None
    for _ in range(2):
        try:
            return json.loads(FUSION_STATE.read_text())
        except (json.JSONDecodeError, IOError):
            _time.sleep(0.3)
    return None


def _age():
    try:
        return _time.time() - os.path.getmtime(FUSION_STATE)
    except OSError:
        return 999


def _render_service_control():
    """START/STOP buttons for fusion_service."""
    try:
        from pakfindata.services.fusion_service import (
            is_fusion_running, start_fusion_background, stop_fusion_service,
        )
    except ImportError:
        st.caption("fusion_service not available")
        return

    running, pid = is_fusion_running()

    if running:
        if st.button(f"Stop (PID {pid})", key="fs_stop", type="secondary"):
            ok, msg = stop_fusion_service()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    else:
        c1, c2, c3 = st.columns(3)
        with c1:
            sym = st.text_input("Symbol", "OGDC", key="fs_sym", label_visibility="collapsed")
        with c2:
            interval = st.selectbox("Interval", [5, 10, 15, 30], index=1, key="fs_int",
                                    label_visibility="collapsed")
        with c3:
            if st.button("Start", key="fs_start", type="primary"):
                ok, msg = start_fusion_background(symbol=sym, interval=interval)
                if ok:
                    st.success(msg)
                else:
                    st.error(msg)
                st.rerun()


def render_page():
    age = _age()
    if age < 30 and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=5000, limit=None, key="fusion_sim_refresh")

    # Header + controls
    h1, h2 = st.columns([3, 2])
    with h1:
        st.markdown("### Strategy Fusion Simulator")
    with h2:
        _render_service_control()

    data = _load()

    if data is None:
        st.info(
            "Fusion service is not running. Click **Start** above, or run:\n\n"
            "```\npython -m pakfindata.services.fusion_service --symbol OGDC\n```"
        )
        st.markdown("""
        ---
        #### Architecture

        | Component | Role |
        |-----------|------|
        | **tick_service** | Writes `live_snapshot.json` (live prices from PSX) |
        | **fusion_service** | Reads prices, runs strategies, writes `fusion_state.json` |
        | **This page** | Reads `fusion_state.json`, renders charts (same as Live Ticker) |

        No ports. No iframes. No WebSocket from JS. Just two JSON files on disk.
        """)
        render_footer()
        return

    # Parse state
    running = data.get("running", False)
    symbol = data.get("symbol", "?")
    decision = data.get("decision", {})
    portfolio = data.get("portfolio", {})
    votes = data.get("votes", [])
    candles = data.get("candles", [])
    score_history = data.get("score_history", [])
    markers = data.get("markers", [])

    # Status
    if running and age < 30:
        status_text = "LIVE"
    elif age < 60:
        status_text = "STALE"
    else:
        status_text = "DOWN"

    # Status bar — all native st.metric
    s1, s2, s3, s4, s5, s6, s7 = st.columns(7)
    s1.metric("Status", status_text)
    s2.metric("Symbol", symbol)
    s3.metric("Price", f"{decision.get('price', 0):,.2f}")

    dec = decision.get("decision", "HOLD")
    s4.metric("Decision", dec)
    s5.metric("Confidence", f"{decision.get('confidence', 0):.0f}%")

    pnl = portfolio.get("pnl", 0)
    s6.metric("P&L", f"{pnl:+,.0f}")
    s7.metric("Trades", portfolio.get("trades", 0))

    if decision.get("vetoed"):
        st.warning(f"VETOED: {decision.get('veto_reason', '')}")

    # Candlestick chart + signal sub-chart
    if candles and len(candles) > 2:
        fig = make_subplots(
            rows=3, cols=1, shared_xaxes=True,
            row_heights=[0.55, 0.20, 0.25],
            vertical_spacing=0.03,
        )

        times = [datetime.fromtimestamp(c["time"], PKT) for c in candles]

        # Row 1: Candlestick
        fig.add_trace(go.Candlestick(
            x=times,
            open=[c["open"] for c in candles],
            high=[c["high"] for c in candles],
            low=[c["low"] for c in candles],
            close=[c["close"] for c in candles],
            increasing_line_color=_C["up"], decreasing_line_color=_C["down"],
            increasing_fillcolor=_C["up"], decreasing_fillcolor=_C["down"],
            name="Price",
        ), row=1, col=1)

        # Signal markers
        buy_m = [m for m in markers if "BUY" in m.get("decision", "")]
        sell_m = [m for m in markers if "SELL" in m.get("decision", "")]

        if buy_m:
            fig.add_trace(go.Scatter(
                x=[datetime.fromtimestamp(m["time"], PKT) for m in buy_m],
                y=[m["price"] * 0.998 for m in buy_m],
                mode="markers", name="BUY",
                marker=dict(symbol="triangle-up", size=12, color=_C["up"]),
            ), row=1, col=1)
        if sell_m:
            fig.add_trace(go.Scatter(
                x=[datetime.fromtimestamp(m["time"], PKT) for m in sell_m],
                y=[m["price"] * 1.002 for m in sell_m],
                mode="markers", name="SELL",
                marker=dict(symbol="triangle-down", size=12, color=_C["down"]),
            ), row=1, col=1)

        # Row 2: Volume
        fig.add_trace(go.Bar(
            x=times,
            y=[c.get("volume", 0) for c in candles],
            marker_color=[_C["up"] if c["close"] >= c["open"] else _C["down"] for c in candles],
            opacity=0.4, name="Volume",
        ), row=2, col=1)

        # Row 3: Fusion score
        if score_history:
            sh_times = [datetime.fromtimestamp(s["time"], PKT) for s in score_history]
            sh_scores = [s["score"] * 100 for s in score_history]
            fig.add_trace(go.Scatter(
                x=sh_times, y=sh_scores, mode="lines", name="Fusion Score",
                line=dict(color=_C["cyan"], width=1.5),
                fill="tozeroy", fillcolor="rgba(0,188,212,0.1)",
            ), row=3, col=1)
            fig.add_hline(y=15, line_dash="dot", line_color=_C["dim"], row=3, col=1)
            fig.add_hline(y=-15, line_dash="dot", line_color=_C["dim"], row=3, col=1)
            fig.add_hline(y=0, line_dash="solid", line_color="#333", row=3, col=1)

        fig.update_layout(
            paper_bgcolor=_C["bg"], plot_bgcolor=_C["bg"],
            font_color=_C["dim"], height=550,
            margin=dict(l=10, r=10, t=10, b=10),
            showlegend=False, xaxis_rangeslider_visible=False,
        )
        for ax in ["yaxis", "yaxis2", "yaxis3"]:
            fig.update_layout(**{ax: dict(gridcolor=_C["border"])})

        fig.update_yaxes(title_text=symbol, row=1, col=1)
        fig.update_yaxes(title_text="Vol", row=2, col=1)
        fig.update_yaxes(title_text="Score", row=3, col=1)

        st.plotly_chart(fig, use_container_width=True, key="fusion_chart")

    st.divider()

    # Strategy votes + Category scores
    left, right = st.columns([3, 2])

    with left:
        st.markdown("**Strategy Signals**")
        # Use st.dataframe — no HTML rendering issues
        vote_rows = []
        for v in votes:
            dir_map = {1: "LONG", -1: "SHORT", 0: "--"}
            vote_rows.append({
                "Strategy": v.get("label", v.get("name", "?")),
                "Category": v.get("cat", ""),
                "Status": "ON" if v.get("enabled") else "OFF",
                "Direction": dir_map.get(v.get("direction", 0), "--"),
                "Confidence": f"{v.get('confidence', 0):.0%}",
                "Signal": str(v.get("signal", ""))[:25],
            })
        if vote_rows:
            st.dataframe(pd.DataFrame(vote_rows), use_container_width=True, hide_index=True)

    with right:
        st.markdown("**Category Scores**")
        for label, key in [("REGIME", "regime"), ("FLOW", "flow"),
                           ("STRUCTURE", "structure"), ("ALPHA", "alpha")]:
            score = decision.get(key, 0)
            st.metric(label, f"{score*100:+.0f}")

        st.divider()
        st.markdown("**Portfolio**")
        pc1, pc2 = st.columns(2)
        pc1.metric("Equity", f"{portfolio.get('equity', 0):,.0f}")
        pc2.metric("Win Rate", f"{portfolio.get('win_rate', 0):.0f}%")
        pc3, pc4 = st.columns(2)
        pc3.metric("Drawdown", f"{portfolio.get('drawdown', 0):.1f}%")
        pc4.metric("Positions", len(portfolio.get("positions", [])))

    st.divider()

    # Positions + Trades
    p1, p2 = st.columns(2)

    with p1:
        st.markdown("**Open Positions**")
        positions = portfolio.get("positions", [])
        if positions:
            pos_df = pd.DataFrame(positions)
            show = [c for c in ["symbol", "side", "entry", "cur", "pnl", "pnl_pct", "shares"]
                    if c in pos_df.columns]
            st.dataframe(pos_df[show].round(2), use_container_width=True, hide_index=True)
        else:
            st.caption("No open positions")

    with p2:
        st.markdown("**Recent Trades**")
        closed = portfolio.get("closed", [])
        if closed:
            trade_df = pd.DataFrame(closed[-10:])
            show = [c for c in ["exit_time", "side", "symbol", "shares", "entry", "exit", "pnl", "exit_reason"]
                    if c in trade_df.columns]
            st.dataframe(trade_df[show].round(2), use_container_width=True, hide_index=True)
        else:
            st.caption("No trades yet")

    render_footer()
