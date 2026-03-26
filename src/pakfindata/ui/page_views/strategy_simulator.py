"""Strategy Fusion Simulator -- real-time decision engine.

Architecture:
  - Streamlit sidebar: controls (symbol, capital, strategy toggles, START)
  - On START: launches a background micro-API (Flask, port 8766) that runs
    the fusion engine and serves results as JSON
  - Main area: embedded HTML panel that polls the API every 5-10s
  - Panel renders at 60fps with Chart.js — no Streamlit reruns needed
"""

import streamlit as st
import streamlit.components.v1 as components
from pathlib import Path
import threading
import json
import time

from pakfindata.ui.components.helpers import render_footer

DATA_ROOT = Path("/mnt/e/psxdata")
DUCKDB_PATH = "/mnt/e/psxdata/pakfindata.duckdb"
PANEL_HTML = Path(__file__).parent / "simulator_panel.html"
API_PORT = 8766

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
    """Fetch latest price from tick_logs or eod_ohlcv."""
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


def _start_fusion_api(engine, symbol: str, refresh_sec: int = 10):
    """Start a background Flask micro-API that the HTML panel polls."""
    from flask import Flask, jsonify, request
    from flask_cors import CORS
    import logging

    app = Flask(__name__)
    CORS(app, resources={r"/*": {"origins": "*"}})
    log = logging.getLogger("werkzeug")
    log.setLevel(logging.ERROR)

    @app.after_request
    def add_cors_headers(response):
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response

    # Shared state
    state = {"last_computed": 0, "result": None, "symbol": symbol}

    @app.route("/fusion/state")
    def get_state():
        return jsonify(engine.get_state())

    @app.route("/fusion/compute", methods=["POST", "GET"])
    def compute():
        sym = request.args.get("symbol", state["symbol"])
        price_arg = request.args.get("price", None)

        if price_arg:
            price = float(price_arg)
        else:
            price = _fetch_latest_price(sym)

        if price <= 0:
            return jsonify({"error": "no price"})

        now = time.time()
        # Rate-limit: at most once per 5 seconds
        if now - state["last_computed"] < 5:
            return jsonify(engine.get_state())

        from dataclasses import asdict
        decision = engine.compute(sym, price)
        engine.update_portfolio(decision)
        state["last_computed"] = now
        state["result"] = asdict(decision)
        return jsonify(engine.get_state())

    @app.route("/fusion/toggle", methods=["POST"])
    def toggle():
        data = request.get_json() or {}
        strategy = data.get("strategy")
        enabled = data.get("enabled", True)
        if strategy:
            engine.set_enabled({strategy: enabled})
        return jsonify({"ok": True})

    @app.route("/fusion/price")
    def get_price():
        sym = request.args.get("symbol", state["symbol"])
        price = _fetch_latest_price(sym)
        return jsonify({"symbol": sym, "price": price})

    @app.route("/health")
    def health():
        return jsonify({"status": "ok", "symbol": state["symbol"]})

    app.run(host="0.0.0.0", port=API_PORT, threaded=True)


def _is_api_running() -> bool:
    """Check if fusion API is already running."""
    try:
        import urllib.request
        resp = urllib.request.urlopen(f"http://localhost:{API_PORT}/health", timeout=2)
        return resp.status == 200
    except Exception:
        return False


def render_page():
    st.markdown("### Strategy Fusion Simulator")
    st.caption("All 14 strategies fused into one real-time decision engine")

    # Sidebar controls
    with st.sidebar:
        st.markdown("#### Simulator Config")
        symbol = st.text_input("Symbol", "OGDC", key="sim_symbol")
        capital = st.number_input("Capital (PKR)", 100_000, 10_000_000, 1_000_000, 100_000, key="sim_capital")
        refresh_sec = st.select_slider("Refresh (sec)", [5, 10, 15, 30], value=10, key="sim_refresh")

        st.markdown("---")
        st.markdown("#### Enable Strategies")

        enabled = {}
        for category, strats in STRATEGIES.items():
            st.markdown(f"**{category}**")
            for key, label, default in strats:
                enabled[key] = st.checkbox(label, value=default, key=f"sim_{key}")

        st.markdown("---")
        start = st.button("START SIMULATOR", type="primary", use_container_width=True)

    # On START: init engine + launch background API
    if start:
        try:
            # Check flask-cors is available
            try:
                import flask_cors  # noqa
            except ImportError:
                st.error("Need flask-cors: `pip install flask flask-cors`")
                return

            from pakfindata.engine.strategy_fusion import StrategyFusionEngine
            engine = StrategyFusionEngine(capital=capital)
            engine.set_enabled(enabled)
            st.session_state["fusion_engine"] = engine
            st.session_state["fusion_symbol"] = symbol.upper()

            # Kill existing API if running
            if not _is_api_running():
                t = threading.Thread(
                    target=_start_fusion_api,
                    args=(engine, symbol.upper(), refresh_sec),
                    daemon=True,
                )
                t.start()
                time.sleep(1.5)  # wait for server to start

            st.session_state["fusion_api_running"] = True

        except Exception as e:
            st.error(f"Failed: {e}")
            return

    # Check if API is running
    api_running = st.session_state.get("fusion_api_running", False) or _is_api_running()

    if not api_running:
        st.info("Configure settings in the sidebar and click **START SIMULATOR**.")
        st.markdown("""
        ---
        #### How It Works

        1. Click **START** -- this launches a background micro-API
        2. An embedded real-time panel appears (HTML/JS, no Streamlit reruns)
        3. The panel auto-fetches the latest tick price from DuckDB
        4. Every 5-10s it calls the fusion engine and updates all panels smoothly

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

    # Show the real-time panel
    sym = st.session_state.get("fusion_symbol", symbol.upper())
    api_url = f"http://localhost:{API_PORT}"

    st.success(f"Simulator running: **{sym}** | API: `{api_url}` | Refresh: {refresh_sec}s")

    if PANEL_HTML.exists():
        html = PANEL_HTML.read_text()
        # The panel connects to our local micro-API, not ws_relay
        html = html.replace("WS_URL_PLACEHOLDER", "")  # no websocket needed
        html = html.replace("API_URL_PLACEHOLDER", api_url)
        html = html.replace("SYMBOL_PLACEHOLDER", sym)
        html = html.replace("const REFRESH_MS = 5000;", f"const REFRESH_MS = {refresh_sec * 1000};")
        components.html(html, height=900, scrolling=True)
    else:
        st.error("simulator_panel.html not found")

    render_footer()
