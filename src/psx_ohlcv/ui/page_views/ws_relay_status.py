"""WS Relay Status — Streamlit page showing WebSocket relay health.

100% self-contained — no psx_ohlcv imports. Reads from relay REST endpoints only.
"""

import streamlit as st

try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

RELAY_URL = "http://localhost:8765"


def _fetch(endpoint: str, timeout: float = 3.0) -> dict | None:
    """Fetch a relay REST endpoint. Returns None on failure."""
    if not HAS_REQUESTS:
        return None
    try:
        resp = requests.get(f"{RELAY_URL}{endpoint}", timeout=timeout)
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


def render_ws_relay_status():
    """WS Relay Status page."""

    if HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=5000, limit=None, key="relay_refresh")

    st.markdown("## WebSocket Relay")

    # -----------------------------------------------------------------
    # 1. Connection status header
    # -----------------------------------------------------------------
    health = _fetch("/health")

    if health and health.get("status") == "ok":
        connected = health.get("connected", False)
        conn_icon = "🟢" if connected else "🟡"
        conn_text = "RELAY ONLINE" if connected else "RELAY ONLINE (collector disconnected)"
        st.markdown(
            f'<div style="padding:16px; background:linear-gradient(135deg, #0f2027, #203a43);'
            f' border-radius:10px; border-left:4px solid #10b981; margin-bottom:20px;">'
            f'<span style="font-size:20px; font-weight:bold; color:#10b981;">'
            f'{conn_icon} {conn_text}</span>'
            f'<span style="color:#94a3b8; margin-left:16px;">port {RELAY_URL.split(":")[-1]}</span>'
            f'</div>',
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            '<div style="padding:16px; background:linear-gradient(135deg, #1a0000, #2d1111);'
            ' border-radius:10px; border-left:4px solid #ef4444; margin-bottom:20px;">'
            '<span style="font-size:20px; font-weight:bold; color:#ef4444;">'
            '🔴 RELAY OFFLINE</span>'
            '<span style="color:#94a3b8; margin-left:16px;">'
            'Is tick_service.py running?</span>'
            '</div>',
            unsafe_allow_html=True,
        )
        st.info(
            "Start the tick service to enable the relay:\n\n"
            "```\npython -m psx_ohlcv.services.tick_service\n```"
        )
        # Still render the reference sections below
        _render_connect_guide()
        _render_endpoint_reference()
        return

    # -----------------------------------------------------------------
    # 2. Stats row (3 metrics)
    # -----------------------------------------------------------------
    stats = _fetch("/stats")
    if stats:
        c1, c2, c3 = st.columns(3)
        c1.metric("WS Clients", stats.get("ws_clients", 0))
        c2.metric("Active Channels", len(stats.get("ws_channels", {})))
        c3.metric("Total Ticks", f"{stats.get('tick_count', 0):,}")

        st.divider()

        # Symbols / Indices counts
        sc1, sc2 = st.columns(2)
        sc1.metric("Symbols Tracked", stats.get("symbols", 0))
        sc2.metric("Indices Tracked", stats.get("indices", 0))

    # -----------------------------------------------------------------
    # 3. Channel details table
    # -----------------------------------------------------------------
    if stats:
        channels = stats.get("ws_channels", {})
        if channels:
            st.markdown("#### Channel Details")
            rows = [{"Channel": ch, "Clients": cnt} for ch, cnt in channels.items()]
            st.dataframe(rows, use_container_width=True, hide_index=True)
        else:
            st.caption("No active WebSocket channels (no clients connected)")

    st.divider()

    # -----------------------------------------------------------------
    # 4. Quick connect guide
    # -----------------------------------------------------------------
    _render_connect_guide()

    # -----------------------------------------------------------------
    # 5. Endpoint reference
    # -----------------------------------------------------------------
    _render_endpoint_reference()


def _render_connect_guide():
    """Quick connect code examples."""
    with st.expander("Quick Connect Guide", expanded=False):
        st.markdown("**Python**")
        st.code(
            'import asyncio, websockets\n\n'
            'async def listen():\n'
            '    async with websockets.connect("ws://localhost:8765/ws/ticks") as ws:\n'
            '        async for msg in ws:\n'
            '            print(msg)\n\n'
            'asyncio.run(listen())',
            language="python",
        )
        st.markdown("**JavaScript**")
        st.code(
            'const ws = new WebSocket("ws://localhost:8765/ws/ticks");\n'
            'ws.onmessage = (e) => console.log(JSON.parse(e.data));',
            language="javascript",
        )
        st.markdown("**Firehose (all data)**")
        st.code("ws://localhost:8765/ws/firehose")
        st.markdown(
            "**Swagger Docs**: [http://localhost:8765/docs](http://localhost:8765/docs)"
        )


def _render_endpoint_reference():
    """Endpoint reference table."""
    with st.expander("Endpoint Reference", expanded=False):
        endpoints = [
            {
                "Endpoint": "/ws/ticks",
                "Type": "WebSocket",
                "Description": "All stock ticks",
            },
            {
                "Endpoint": "/ws/ticks?market=REG",
                "Type": "WebSocket",
                "Description": "Filtered by market",
            },
            {
                "Endpoint": "/ws/indices",
                "Type": "WebSocket",
                "Description": "All index ticks",
            },
            {
                "Endpoint": "/ws/symbol/{sym}",
                "Type": "WebSocket",
                "Description": "Single symbol stream",
            },
            {
                "Endpoint": "/ws/firehose",
                "Type": "WebSocket",
                "Description": "Everything (stocks + indices)",
            },
            {
                "Endpoint": "/snapshot",
                "Type": "REST",
                "Description": "Current market state",
            },
            {
                "Endpoint": "/health",
                "Type": "REST",
                "Description": "Health check",
            },
            {
                "Endpoint": "/stats",
                "Type": "REST",
                "Description": "Server statistics",
            },
            {
                "Endpoint": "/docs",
                "Type": "REST",
                "Description": "Swagger UI",
            },
        ]
        st.dataframe(endpoints, use_container_width=True, hide_index=True)
