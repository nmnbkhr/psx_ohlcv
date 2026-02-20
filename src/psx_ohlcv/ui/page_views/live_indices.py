"""Live Indices — real-time PSX index dashboard.

Reads the `indices` section from live_snapshot.json every 2 seconds.
Shows KSE-100 hero card, category tables, heatmap, and intraday sparklines.

Color scheme: Gold/Amber theme (distinct from green/red stock ticker).
- Up:   #f59e0b (amber)  / #fbbf24 (gold)
- Down: #8b5cf6 (violet) / #a78bfa (light violet)
- Hero: dark-to-navy gradient with amber accent
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

# Index categories
EQUITY_INDICES = ["KSE100", "KSE30", "KSE100PR", "ALLSHR"]
ISLAMIC_INDICES = ["KMI30", "KMIALLSHR", "MII30", "MZNPI"]
SECTORAL_INDICES = ["BKTI", "JSGBKTI", "JSMFI", "ACI"]

INDEX_NAMES = {
    "KSE100": "KSE-100",
    "KSE30": "KSE-30",
    "KSE100PR": "KSE-100 Price Return",
    "ALLSHR": "All Share",
    "KMI30": "KMI-30",
    "KMIALLSHR": "KMI All Shares",
    "MII30": "Mahaana Islamic 30",
    "MZNPI": "Meezan Pakistan",
    "BKTI": "Banks Tradable",
    "JSGBKTI": "JS Global Banks",
    "JSMFI": "JS Momentum Factor",
    "ACI": "Alfalah Consumer",
}

# Colors — amber/gold for up, violet for down (distinct from stock page)
C_UP = "#f59e0b"
C_UP_LIGHT = "#fbbf24"
C_DOWN = "#8b5cf6"
C_DOWN_LIGHT = "#a78bfa"
C_NEUTRAL = "#6b7280"
C_ACCENT = "#f59e0b"


def _load_snapshot() -> dict | None:
    """Read snapshot JSON with retry on decode error (mid-write safety)."""
    if not SNAPSHOT_PATH.exists():
        return None
    for attempt in range(2):
        try:
            return json.loads(SNAPSHOT_PATH.read_text())
        except json.JSONDecodeError:
            if attempt == 0:
                _time.sleep(0.5)
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


def render_live_indices():
    """Live Indices page — reads JSON snapshot, renders index dashboard."""

    age = _file_age()

    # Auto-refresh: use autorefresh if data is fresh, fallback if stale
    if age < 5:
        if HAS_AUTOREFRESH and st_autorefresh:
            st_autorefresh(interval=2000, limit=None, key="live_indices_refresh")
    elif age < 30:
        if HAS_AUTOREFRESH and st_autorefresh:
            st_autorefresh(interval=3000, limit=None, key="live_indices_refresh")

    # Header + start/stop + refresh button
    hdr1, hdr2, hdr3 = st.columns([3, 1, 0.5])
    with hdr1:
        st.markdown("## Live Indices")
    with hdr2:
        _render_service_control()
    with hdr3:
        if st.button("🔄", key="idx_refresh", help="Manual refresh"):
            st.rerun()

    data = _load_snapshot()
    if data is None:
        st.info(
            "Tick service is not running. Click **Start Collector** above, "
            "or run from CLI:\n\n"
            "```\npython -m psx_ohlcv.services.tick_service\n```"
        )
        return

    indices = data.get("indices", [])

    if not indices:
        connected = data.get("connected", False)
        if connected and age < 10:
            st.warning("Connected but no index (IDX) ticks received yet. Waiting...")
        else:
            st.info("No index data available. The IDX market subscription may not be active.")
        return

    idx_map = {i["symbol"]: i for i in indices}

    # ------------------------------------------------------------------
    # STATUS BAR — mtime-based LIVE/STALE/DOWN detection
    # ------------------------------------------------------------------
    connected = data.get("connected", False)
    if connected and age < 5:
        status_color = C_ACCENT
        status_text = "LIVE"
    elif age < 30:
        status_color = "#f59e0b"
        status_text = "STALE"
    else:
        status_color = "#ef4444"
        status_text = "DOWN"

    idx_count = data.get("index_count", len(indices))
    ts_str = data.get("timestamp", "")
    ts_display = ts_str[11:19] if len(ts_str) > 19 else "--"

    st.markdown(
        f'<div style="display:flex;align-items:center;gap:16px;margin-bottom:16px;">'
        f'<span style="color:{status_color};font-weight:bold;font-size:16px">'
        f'&#x25CF; {status_text}</span>'
        f'<span style="color:#94a3b8;font-size:14px;">'
        f'{idx_count} indices | {ts_display} PKT</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    if age > 30:
        st.error(f"Snapshot is {age:.0f}s old. Collector appears down.")
    elif age > 5:
        st.warning(f"Snapshot is {age:.0f}s old. Data may be stale.")

    # ------------------------------------------------------------------
    # HERO: KSE-100 card
    # ------------------------------------------------------------------
    kse100 = idx_map.get("KSE100")
    if kse100:
        _render_hero(kse100, data)

    # ------------------------------------------------------------------
    # Secondary indices: KSE-30, KMI-30, All Share
    # ------------------------------------------------------------------
    sec_syms = ["KSE30", "KMI30", "ALLSHR"]
    cols = st.columns(len(sec_syms))
    for i, sym in enumerate(sec_syms):
        idx = idx_map.get(sym)
        if idx:
            pct = idx.get("changePercent", 0) * 100
            cols[i].metric(
                label=INDEX_NAMES.get(sym, sym),
                value=f"{idx['value']:,.2f}",
                delta=f"{pct:+.2f}%",
            )

    st.divider()

    # ------------------------------------------------------------------
    # Index category tables
    # ------------------------------------------------------------------
    _render_index_group("Equity Indices", EQUITY_INDICES, idx_map)
    _render_index_group("Islamic Indices", ISLAMIC_INDICES, idx_map)
    _render_index_group("Sectoral Indices", SECTORAL_INDICES, idx_map)

    st.divider()

    # ------------------------------------------------------------------
    # Heatmap
    # ------------------------------------------------------------------
    _render_heatmap(indices)

    # ------------------------------------------------------------------
    # Intraday sparklines
    # ------------------------------------------------------------------
    _render_sparklines(data, idx_map)


def _render_hero(kse100: dict, data: dict):
    """KSE-100 hero card with gradient and gold/violet accent."""
    val = kse100.get("value", 0)
    chg = kse100.get("change", 0)
    pct = kse100.get("changePercent", 0) * 100
    hi = kse100.get("high", 0)
    lo = kse100.get("low", 0)
    opn = kse100.get("open", 0)

    if chg >= 0:
        color = C_UP
        arrow = "&#9650;"
        sign = "+"
    else:
        color = C_DOWN
        arrow = "&#9660;"
        sign = ""

    st.markdown(f"""
    <div style="background:linear-gradient(135deg, #1e1b4b, #0f172a);
                padding:24px; border-radius:12px;
                border-left:4px solid {color}; margin-bottom:20px;">
        <div style="display:flex; justify-content:space-between;
                    align-items:center; flex-wrap:wrap;">
            <div>
                <div style="color:{C_ACCENT}; font-size:13px;
                            font-weight:600; letter-spacing:1px;
                            margin-bottom:4px;">KSE-100 INDEX</div>
                <div style="color:white; font-size:36px;
                            font-weight:bold; font-family:monospace;">
                    {val:,.2f}</div>
                <div style="color:{color}; font-size:20px; margin-top:4px;">
                    {arrow} {sign}{chg:,.2f} ({sign}{pct:.2f}%)</div>
            </div>
            <div style="text-align:right;">
                <div style="color:#64748b; font-size:13px;">
                    Open: <span style="color:#e2e8f0;">{opn:,.2f}</span></div>
                <div style="color:#64748b; font-size:13px;">
                    High: <span style="color:{C_UP};">{hi:,.2f}</span></div>
                <div style="color:#64748b; font-size:13px;">
                    Low: <span style="color:{C_DOWN};">{lo:,.2f}</span></div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)

    # KSE-100 sparkline
    sparkline_data = data.get("index_sparklines", {}).get("KSE100", [])
    if len(sparkline_data) >= 2:
        spark_df = pd.DataFrame(sparkline_data)
        spark_df["time"] = pd.to_datetime(spark_df["ts"], unit="s")
        st.line_chart(
            spark_df.set_index("time")["value"],
            use_container_width=True,
            height=200,
        )


def _render_index_group(title: str, symbols: list[str], idx_map: dict):
    """Render a category of indices as a styled table."""
    rows = []
    for sym in symbols:
        idx = idx_map.get(sym)
        if not idx:
            continue
        pct = idx.get("changePercent", 0) * 100
        rows.append({
            "Index": INDEX_NAMES.get(sym, sym),
            "Symbol": sym,
            "Value": idx.get("value", 0),
            "Change": idx.get("change", 0),
            "Chg%": pct,
            "High": idx.get("high", 0),
            "Low": idx.get("low", 0),
            "Volume": idx.get("volume", 0),
        })

    if not rows:
        return

    st.markdown(f"#### {title}")
    df = pd.DataFrame(rows)

    def _color_chg(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return f"color: {C_UP}"
            elif val < 0:
                return f"color: {C_DOWN}"
        return ""

    chg_cols = [c for c in ["Change", "Chg%"] if c in df.columns]
    fmt = {
        "Value": "{:,.2f}",
        "Change": "{:+,.2f}",
        "Chg%": "{:+.2f}%",
        "High": "{:,.2f}",
        "Low": "{:,.2f}",
        "Volume": "{:,.0f}",
    }

    styled = df.style.map(_color_chg, subset=chg_cols).format(
        {k: v for k, v in fmt.items() if k in df.columns}
    )
    st.dataframe(styled, use_container_width=True, hide_index=True)


def _render_heatmap(indices: list[dict]):
    """Heatmap of all indices — amber for up, violet for down."""
    st.markdown("#### Index Heatmap")
    sorted_indices = sorted(
        indices, key=lambda x: x.get("changePercent", 0), reverse=True
    )

    html = '<div style="display:flex; flex-wrap:wrap; gap:8px; margin-bottom:20px;">'
    for idx in sorted_indices:
        sym = idx["symbol"]
        name = INDEX_NAMES.get(sym, sym)
        pct = idx.get("changePercent", 0) * 100
        val = idx.get("value", 0)

        if pct > 0:
            intensity = min(pct / 3.0, 1.0)
            bg = f"rgba(245,158,11,{0.15 + intensity * 0.55})"
            text_color = C_UP_LIGHT
        elif pct < 0:
            intensity = min(abs(pct) / 3.0, 1.0)
            bg = f"rgba(139,92,246,{0.15 + intensity * 0.55})"
            text_color = C_DOWN_LIGHT
        else:
            bg = "rgba(107,114,128,0.2)"
            text_color = "#9ca3af"

        sign = "+" if pct >= 0 else ""
        html += f'''
        <div style="background:{bg}; padding:12px 16px;
                    border-radius:8px; min-width:140px; flex:1;">
            <div style="color:#e2e8f0; font-size:12px;
                        font-weight:600;">{name}</div>
            <div style="color:white; font-size:18px;
                        font-weight:bold; font-family:monospace;">
                {val:,.0f}</div>
            <div style="color:{text_color}; font-size:14px;">
                {sign}{pct:.2f}%</div>
        </div>'''

    html += "</div>"
    st.markdown(html, unsafe_allow_html=True)


def _render_sparklines(data: dict, idx_map: dict):
    """Intraday sparklines for all indices, 4 per row."""
    all_sparklines = data.get("index_sparklines", {})
    if not any(len(v) >= 2 for v in all_sparklines.values()):
        return

    st.markdown("#### Intraday Movement")
    display_order = EQUITY_INDICES + ISLAMIC_INDICES + SECTORAL_INDICES

    for row_start in range(0, len(display_order), 4):
        row_syms = display_order[row_start:row_start + 4]
        cols = st.columns(len(row_syms))
        for i, sym in enumerate(row_syms):
            spark = all_sparklines.get(sym, [])
            if len(spark) >= 2:
                with cols[i]:
                    idx = idx_map.get(sym, {})
                    pct = idx.get("changePercent", 0) * 100
                    sign = "+" if pct >= 0 else ""
                    color = C_UP if pct >= 0 else C_DOWN
                    st.markdown(
                        f'<span style="font-weight:600">'
                        f'{INDEX_NAMES.get(sym, sym)}</span> '
                        f'<span style="color:{color}">{sign}{pct:.2f}%</span>',
                        unsafe_allow_html=True,
                    )
                    spark_df = pd.DataFrame(spark)
                    spark_df["time"] = pd.to_datetime(spark_df["ts"], unit="s")
                    st.line_chart(
                        spark_df.set_index("time")["value"],
                        height=120,
                    )


# =========================================================================
# Service control — START / STOP button (same as live_ticker)
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
        if st.button(f"Stop Collector (PID {pid})", key="idx_stop_tick_btn", type="primary"):
            ok, msg = stop_tick_service()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
    else:
        if st.button("Start Collector", key="idx_start_tick_btn", type="primary"):
            ok, msg = start_tick_service_background()
            if ok:
                st.success(msg)
            else:
                st.error(msg)
            st.rerun()
