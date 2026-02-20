"""Live Ticker — real-time market dashboard powered by WebSocket tick service.

Reads a JSON snapshot file written by the tick service every 2 seconds.
Pure read-only: no DB queries, no API client, no session state complexity.

Features:
- Colored START / STOP button to control the tick service
- RAM, bars, raw-ticks memory metrics
- Market breadth bar, top movers, full sortable symbol table
"""

import json
import os
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

try:
    from psx_ohlcv.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

PKT = timezone(timedelta(hours=5))
SNAPSHOT_PATH = DATA_ROOT / "live_snapshot.json"
MARKETS = ["REG", "FUT", "ODL", "BNB"]


def _load_snapshot() -> dict | None:
    """Read snapshot JSON with retry on decode error (mid-write safety)."""
    if not SNAPSHOT_PATH.exists():
        return None
    for attempt in range(2):
        try:
            return json.loads(SNAPSHOT_PATH.read_text())
        except json.JSONDecodeError:
            if attempt == 0:
                _time.sleep(0.5)  # file may be mid-write, retry once
            continue
        except IOError:
            return None
    return None


def _file_age() -> float:
    """Seconds since snapshot file was last modified (mtime-based)."""
    try:
        return _time.time() - os.path.getmtime(SNAPSHOT_PATH)
    except OSError:
        return 999


def _format_volume(vol: int | float) -> str:
    if vol >= 1_000_000:
        return f"{vol / 1_000_000:.1f}M"
    elif vol >= 1_000:
        return f"{vol / 1_000:.0f}K"
    return str(int(vol))


def render_live_ticker():
    """Live Ticker page — lightweight, file-based, auto-refreshing."""

    age = _file_age()

    # Auto-refresh: use autorefresh if data is fresh, fallback rerun if stale
    if age < 5:
        if HAS_AUTOREFRESH and st_autorefresh:
            st_autorefresh(interval=2000, limit=None, key="live_ticker_refresh")
    elif age < 30:
        # STALE — data exists but autorefresh may have stopped
        if HAS_AUTOREFRESH and st_autorefresh:
            st_autorefresh(interval=3000, limit=None, key="live_ticker_refresh")

    # ------------------------------------------------------------------
    # HEADER + START/STOP + REFRESH BUTTON
    # ------------------------------------------------------------------
    hdr1, hdr2, hdr3 = st.columns([3, 1, 0.5])
    with hdr1:
        st.markdown("## Live Ticker")
    with hdr2:
        _render_service_control()
    with hdr3:
        if st.button("🔄", key="lt_refresh", help="Manual refresh"):
            st.rerun()

    data = _load_snapshot()

    if data is None:
        st.info(
            "Tick service is not running. Click **Start Collector** above, "
            "or run from CLI:\n\n"
            "```\npython -m psx_ohlcv.services.tick_service\n```"
        )
        return

    # ------------------------------------------------------------------
    # STATUS BAR — mtime-based LIVE/STALE/DOWN detection
    # ------------------------------------------------------------------
    connected = data.get("connected", False)
    if connected and age < 5:
        status_color = "#22c55e"
        status_text = "LIVE"
    elif age < 30:
        status_color = "#f59e0b"
        status_text = "STALE"
    else:
        status_color = "#ef4444"
        status_text = "DOWN"

    cols = st.columns(7)
    cols[0].markdown(
        f'<span style="color:{status_color};font-weight:bold;font-size:16px">'
        f'&#x25CF; {status_text}</span>',
        unsafe_allow_html=True,
    )
    cols[1].metric("Symbols", data.get("symbol_count", 0))
    cols[2].metric("Ticks", f"{data.get('tick_count', 0):,}")
    cols[3].metric("Bars (mem)", f"{data.get('bars_in_memory', 0):,}")
    cols[4].metric("Raw ticks", f"{data.get('raw_ticks_in_memory', 0):,}")
    cols[5].metric("RAM", f"{data.get('ram_mb', 0):.0f} MB")
    last_tick = data.get("timestamp", "")
    cols[6].metric("Updated", last_tick[11:19] if len(last_tick) > 19 else "--")

    # Stale/Down warnings
    if age > 30:
        st.error(f"Snapshot is {age:.0f}s old. Collector appears down.")
    elif age > 5:
        st.warning(f"Snapshot is {age:.0f}s old. Data may be stale.")

    # ------------------------------------------------------------------
    # MARKET BREADTH
    # ------------------------------------------------------------------
    breadth = data.get("breadth", {})
    g = breadth.get("gainers", 0)
    l_ = breadth.get("losers", 0)
    u = breadth.get("unchanged", 0)
    total = g + l_ + u

    b1, b2, b3 = st.columns(3)
    b1.metric("Gainers", g)
    b2.metric("Losers", l_)
    b3.metric("Unchanged", u)

    if total > 0:
        g_pct = g / total * 100
        l_pct = l_ / total * 100
        u_pct = u / total * 100
        st.markdown(
            f'<div style="display:flex;height:20px;border-radius:4px;overflow:hidden">'
            f'<div style="width:{g_pct}%;background:#22c55e" '
            f'title="Gainers {g}"></div>'
            f'<div style="width:{u_pct}%;background:#6b7280" '
            f'title="Unchanged {u}"></div>'
            f'<div style="width:{l_pct}%;background:#ef4444" '
            f'title="Losers {l_}"></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    st.divider()

    # ------------------------------------------------------------------
    # TOP MOVERS + MOST ACTIVE
    # ------------------------------------------------------------------
    mc1, mc2, mc3 = st.columns(3)

    with mc1:
        st.markdown("**Top Gainers**")
        for item in data.get("top_gainers", [])[:5]:
            chg = item.get("changePercent", 0)
            st.markdown(
                f'<span style="color:#22c55e;font-weight:bold">'
                f'{item["symbol"]}</span> '
                f'<span style="color:#22c55e">+{chg:.2f}%</span> '
                f'Rs.{item.get("price", 0):,.2f}',
                unsafe_allow_html=True,
            )

    with mc2:
        st.markdown("**Top Losers**")
        for item in data.get("top_losers", [])[:5]:
            chg = item.get("changePercent", 0)
            st.markdown(
                f'<span style="color:#ef4444;font-weight:bold">'
                f'{item["symbol"]}</span> '
                f'<span style="color:#ef4444">{chg:.2f}%</span> '
                f'Rs.{item.get("price", 0):,.2f}',
                unsafe_allow_html=True,
            )

    with mc3:
        st.markdown("**Most Active**")
        for item in data.get("most_active", [])[:5]:
            st.markdown(
                f'**{item["symbol"]}** '
                f'Rs.{item.get("price", 0):,.2f} '
                f'Vol: {_format_volume(item.get("volume", 0))}',
            )

    st.divider()

    # ------------------------------------------------------------------
    # FULL SYMBOL TABLE
    # ------------------------------------------------------------------
    symbols = data.get("symbols", [])
    if not symbols:
        st.info("No symbol data yet.")
        return

    # Filters
    f1, f2, f3 = st.columns([1, 1, 2])
    with f1:
        market_filter = st.selectbox(
            "Market", ["ALL"] + MARKETS, index=0, key="lt_market_filter"
        )
    with f2:
        sort_by = st.selectbox(
            "Sort by",
            ["Change%", "Volume", "Price", "Symbol"],
            index=0,
            key="lt_sort",
        )
    with f3:
        search = st.text_input(
            "Search", key="lt_search", placeholder="Symbol..."
        )

    df = pd.DataFrame(symbols)

    if market_filter != "ALL":
        df = df[df["market"] == market_filter]
    if search:
        df = df[df["symbol"].str.contains(search.upper(), na=False)]

    if df.empty:
        st.info("No matching symbols.")
        return

    # Select and rename columns
    cols_want = [
        "symbol", "market", "price", "change", "changePercent",
        "volume", "high", "low", "bid", "ask",
    ]
    available = [c for c in cols_want if c in df.columns]
    df = df[available].copy()

    rename = {
        "symbol": "Symbol", "market": "Mkt", "price": "Price",
        "change": "Chg", "changePercent": "Chg%", "volume": "Volume",
        "high": "High", "low": "Low", "bid": "Bid", "ask": "Ask",
    }
    df = df.rename(columns=rename)

    # Sort
    sort_map = {"Change%": "Chg%", "Volume": "Volume", "Price": "Price", "Symbol": "Symbol"}
    sort_col = sort_map.get(sort_by, "Chg%")
    if sort_col in df.columns:
        if sort_col == "Chg%":
            df["_abs"] = df["Chg%"].abs()
            df = df.sort_values("_abs", ascending=False).drop(columns=["_abs"])
        elif sort_col == "Symbol":
            df = df.sort_values(sort_col, ascending=True)
        else:
            df = df.sort_values(sort_col, ascending=False)

    # Color formatting
    def _color_change(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return "color: #22c55e"
            elif val < 0:
                return "color: #ef4444"
        return ""

    chg_cols = [c for c in ["Chg", "Chg%"] if c in df.columns]
    fmt = {}
    for c in ["Price", "High", "Low", "Bid", "Ask"]:
        if c in df.columns:
            fmt[c] = "{:,.2f}"
    if "Chg" in df.columns:
        fmt["Chg"] = "{:+,.2f}"
    if "Chg%" in df.columns:
        fmt["Chg%"] = "{:+,.2f}%"
    if "Volume" in df.columns:
        fmt["Volume"] = "{:,.0f}"

    styled = df.style.map(_color_change, subset=chg_cols).format(fmt)

    st.markdown(f"**{len(df)} symbols**")
    st.dataframe(
        styled,
        use_container_width=True,
        hide_index=True,
        height=600,
    )


# =========================================================================
# Service control — START / STOP button
# =========================================================================

def _render_service_control():
    """Render a colored START or STOP button for the tick service."""
    try:
        from psx_ohlcv.services.tick_service import (
            is_tick_service_running,
            start_tick_service_background,
            stop_tick_service,
        )
    except ImportError:
        st.caption("tick_service not available")
        return

    running, pid = is_tick_service_running()

    if running:
        # Red STOP button
        st.markdown(
            '<style>'
            'div[data-testid="stButton"] > button[kind="secondary"]'
            '#stop_tick_btn { background-color: #ef4444; color: white; '
            'font-weight: bold; border: none; }'
            '</style>',
            unsafe_allow_html=True,
        )
        if st.button(f"Stop Collector (PID {pid})", key="stop_tick_btn", type="primary"):
            ok, msg = stop_tick_service()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    else:
        # Green START button
        if st.button("Start Collector", key="start_tick_btn", type="primary"):
            ok, msg = start_tick_service_background()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
