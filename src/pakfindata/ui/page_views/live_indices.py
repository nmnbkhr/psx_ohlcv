"""Index Monitor -- real-time PSX indices with Bloomberg-style presentation.

Reads `indices` from live_snapshot.json. Falls back to DB-stored index data
when the tick service isn't running.
"""

import json
import os
import time as _time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

from pakfindata.ui.themes import get_plotly_layout
from pakfindata.ui.components.helpers import get_connection, render_footer

PKT = timezone(timedelta(hours=5))
SNAPSHOT_PATH = DATA_ROOT / "live_snapshot.json"

# Colors -- clean green/red for index, no amber
C_UP = "#00C853"
C_DN = "#FF5252"
C_NEU = "#6B7280"
C_BG = "#12161C"
C_BG_DARK = "#0B0E11"
C_BORDER = "#1E2329"
C_TEXT = "#EAECEF"
C_MUTED = "#6B7280"
C_ACCENT = "#2F81F7"

# Index metadata
INDEX_META = {
    "KSE100":    ("KSE-100",             "equity"),
    "KSE30":     ("KSE-30",              "equity"),
    "KSE100PR":  ("KSE-100 Price Ret",   "equity"),
    "ALLSHR":    ("All Share",            "equity"),
    "KMI30":     ("KMI-30",              "islamic"),
    "KMIALLSHR": ("KMI All Share",        "islamic"),
    "MII30":     ("Mahaana Islamic 30",   "islamic"),
    "MZNPI":     ("Meezan Pakistan",      "islamic"),
    "BKTI":      ("Banks Tradable",       "sectoral"),
    "JSGBKTI":   ("JS Global Banks",      "sectoral"),
    "JSMFI":     ("JS Momentum",          "sectoral"),
    "ACI":       ("Alfalah Consumer",     "sectoral"),
}


def _name(sym):
    return INDEX_META.get(sym, (sym, "other"))[0]


def _cat(sym):
    return INDEX_META.get(sym, (sym, "other"))[1]


# ── Snapshot loading ──────────────────────────────────────────────────────

def _load_snapshot():
    if not SNAPSHOT_PATH.exists():
        return None
    for attempt in range(2):
        try:
            return json.loads(SNAPSHOT_PATH.read_text())
        except (json.JSONDecodeError, IOError):
            if attempt == 0:
                _time.sleep(0.3)
    return None


def _file_age():
    try:
        return _time.time() - os.path.getmtime(SNAPSHOT_PATH)
    except OSError:
        return 999


# ── DB fallback ───────────────────────────────────────────────────────────

@st.cache_data(ttl=120)
def _get_db_indices(_con):
    """Load index data from psx_indices table as fallback."""
    try:
        df = pd.read_sql_query("""
            SELECT symbol, value, change, change_pct, high, low, volume, index_date
            FROM psx_indices
            WHERE index_date = (SELECT MAX(index_date) FROM psx_indices)
        """, _con)
        if df.empty:
            return None, None
        idx_map = {}
        for _, r in df.iterrows():
            idx_map[r["symbol"]] = {
                "symbol": r["symbol"],
                "value": r["value"] or 0,
                "change": r["change"] or 0,
                "changePercent": (r["change_pct"] or 0) / 100,
                "high": r["high"] or 0,
                "low": r["low"] or 0,
                "volume": r["volume"] or 0,
            }
        return idx_map, str(df["index_date"].iloc[0])
    except Exception:
        return None, None


@st.cache_data(ttl=120)
def _get_index_history(_con, symbol, days=30):
    """Get index history for mini chart."""
    try:
        return pd.read_sql_query("""
            SELECT index_date as date, value
            FROM psx_indices
            WHERE symbol = ? AND value > 0
            ORDER BY index_date DESC LIMIT ?
        """, _con, params=(symbol, days))
    except Exception:
        return pd.DataFrame()


# ── HTML builders ─────────────────────────────────────────────────────────

def _hero_html(idx, date_str=""):
    """KSE-100 hero card."""
    val = idx.get("value", 0)
    chg = idx.get("change", 0)
    pct = idx.get("changePercent", 0) * 100
    hi = idx.get("high", 0)
    lo = idx.get("low", 0)
    vol = idx.get("volume", 0)
    opn = idx.get("open", val - chg)

    if chg >= 0:
        c, arrow, sign = C_UP, "&#9650;", "+"
    else:
        c, arrow, sign = C_DN, "&#9660;", ""

    vol_str = f"{vol/1e6:.0f}M" if vol >= 1e6 else f"{vol:,.0f}" if vol else "---"
    rng = hi - lo if hi and lo else 0
    rng_str = f"{rng:,.2f}" if rng else "---"

    return f"""
    <div style="background:{C_BG};border:1px solid {C_BORDER};border-left:3px solid {c};
                border-radius:2px;padding:16px 20px;margin-bottom:10px;">
      <div style="display:flex;align-items:baseline;gap:16px;flex-wrap:wrap;">
        <span style="font-size:11px;color:{C_MUTED};font-weight:600;letter-spacing:0.08em;">KSE-100 INDEX</span>
        <span style="font-size:32px;font-weight:700;font-family:ui-monospace,monospace;color:{C_TEXT};">
          {val:,.2f}
        </span>
        <span style="font-size:18px;font-weight:600;color:{c};font-family:ui-monospace,monospace;">
          {arrow} {sign}{chg:,.2f} ({sign}{pct:.2f}%)
        </span>
        <span style="font-size:11px;color:{C_MUTED};margin-left:auto;">{date_str}</span>
      </div>
      <div style="display:flex;gap:20px;margin-top:8px;font-size:11px;font-family:ui-monospace,monospace;color:{C_MUTED};">
        <span>Open <span style="color:{C_TEXT}">{opn:,.2f}</span></span>
        <span>High <span style="color:{C_UP}">{hi:,.2f}</span></span>
        <span>Low <span style="color:{C_DN}">{lo:,.2f}</span></span>
        <span>Range <span style="color:{C_TEXT}">{rng_str}</span></span>
        <span>Vol <span style="color:{C_TEXT}">{vol_str}</span></span>
      </div>
    </div>"""


def _secondary_cards_html(indices_list, idx_map):
    """Compact secondary index cards in a flex row."""
    cards = []
    for sym in indices_list:
        idx = idx_map.get(sym)
        if not idx:
            continue
        val = idx.get("value", 0)
        pct = idx.get("changePercent", 0) * 100
        c = C_UP if pct > 0 else C_DN if pct < 0 else C_NEU
        sign = "+" if pct >= 0 else ""
        cards.append(f"""
        <div style="background:{C_BG};border:1px solid {C_BORDER};border-radius:2px;
                    padding:10px 14px;flex:1;min-width:140px;">
          <div style="font-size:10px;color:{C_MUTED};font-weight:600;letter-spacing:0.05em;">
            {_name(sym)}
          </div>
          <div style="font-size:18px;font-weight:700;font-family:ui-monospace,monospace;color:{C_TEXT};">
            {val:,.2f}
          </div>
          <div style="font-size:12px;font-weight:600;color:{c};font-family:ui-monospace,monospace;">
            {sign}{pct:.2f}%
          </div>
        </div>""")
    if not cards:
        return ""
    return f'<div style="display:flex;gap:8px;margin-bottom:10px;">{"".join(cards)}</div>'


def _heatmap_html(idx_map):
    """All indices as a heatmap grid."""
    sorted_syms = sorted(
        idx_map.keys(),
        key=lambda s: idx_map[s].get("changePercent", 0),
        reverse=True,
    )
    cells = []
    for sym in sorted_syms:
        idx = idx_map[sym]
        pct = idx.get("changePercent", 0) * 100
        val = idx.get("value", 0)

        if pct > 0:
            intensity = min(abs(pct) / 3.0, 1.0)
            bg = f"rgba(0,200,83,{0.08 + intensity * 0.4})"
            tc = C_UP
        elif pct < 0:
            intensity = min(abs(pct) / 3.0, 1.0)
            bg = f"rgba(255,82,82,{0.08 + intensity * 0.4})"
            tc = C_DN
        else:
            bg = f"rgba(107,114,128,0.1)"
            tc = C_NEU

        sign = "+" if pct >= 0 else ""
        cells.append(f"""
        <div style="background:{bg};border:1px solid {C_BORDER};border-radius:2px;
                    padding:8px 12px;min-width:130px;flex:1;">
          <div style="font-size:10px;color:{C_MUTED};font-weight:600">{_name(sym)}</div>
          <div style="font-size:16px;font-weight:700;font-family:ui-monospace,monospace;color:{C_TEXT};">
            {val:,.0f}
          </div>
          <div style="font-size:12px;font-weight:600;color:{tc}">{sign}{pct:.2f}%</div>
        </div>""")

    return f'<div style="display:flex;flex-wrap:wrap;gap:6px;margin-bottom:12px;">{"".join(cells)}</div>'


# ── Charts ────────────────────────────────────────────────────────────────

def _sparkline_chart(spark_data, sym, pct):
    """Small intraday sparkline using plotly."""
    df = pd.DataFrame(spark_data)
    df["time"] = pd.to_datetime(df["ts"], unit="s")

    c = C_UP if pct >= 0 else C_DN
    layout = get_plotly_layout()
    for k in ("margin", "xaxis", "yaxis"):
        layout.pop(k, None)

    fig = go.Figure(go.Scatter(
        x=df["time"], y=df["value"],
        mode="lines",
        line=dict(color=c, width=1.5),
        fill="tozeroy",
        fillcolor=f"rgba({','.join(str(int(c.lstrip('#')[i:i+2], 16)) for i in (0, 2, 4))},0.08)",
        hovertemplate="%{y:,.2f}<extra></extra>",
    ))
    fig.update_layout(
        **layout,
        height=120,
        margin=dict(l=0, r=0, t=0, b=0),
        xaxis=dict(showgrid=False, showticklabels=False, zeroline=False),
        yaxis=dict(showgrid=False, showticklabels=False, zeroline=False, side="right"),
        showlegend=False,
    )
    return fig


def _history_chart(df, sym):
    """30-day history line chart."""
    if df.empty:
        return None
    df = df.sort_values("date")
    c = C_UP if df["value"].iloc[-1] >= df["value"].iloc[0] else C_DN
    layout = get_plotly_layout()
    for k in ("margin", "title"):
        layout.pop(k, None)

    fig = go.Figure(go.Scatter(
        x=df["date"], y=df["value"],
        mode="lines",
        line=dict(color=c, width=1.5),
        fill="tozeroy",
        fillcolor=f"rgba({','.join(str(int(c.lstrip('#')[i:i+2], 16)) for i in (0, 2, 4))},0.06)",
        hovertemplate="%{x}<br>%{y:,.2f}<extra></extra>",
    ))
    fig.update_layout(
        **layout,
        height=200,
        margin=dict(l=10, r=10, t=30, b=30),
        title=dict(text=f"{_name(sym)} -- 30 Day", font=dict(size=11)),
    )
    return fig


# ── Index table ───────────────────────────────────────────────────────────

def _index_table(syms, idx_map, title):
    """Styled index table for a category."""
    rows = []
    for sym in syms:
        idx = idx_map.get(sym)
        if not idx:
            continue
        pct = idx.get("changePercent", 0) * 100
        rows.append({
            "Index": _name(sym),
            "Value": idx.get("value", 0),
            "Change": idx.get("change", 0),
            "Chg%": pct,
            "High": idx.get("high", 0),
            "Low": idx.get("low", 0),
        })
    if not rows:
        return

    st.markdown(
        f'<div style="color:{C_MUTED};font-size:11px;font-weight:600;'
        f'letter-spacing:0.05em;margin-bottom:4px;">{title}</div>',
        unsafe_allow_html=True,
    )
    df = pd.DataFrame(rows)

    def _color(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return f"color: {C_UP}"
            elif val < 0:
                return f"color: {C_DN}"
        return ""

    styled = df.style.map(_color, subset=["Change", "Chg%"]).format({
        "Value": "{:,.2f}",
        "Change": "{:+,.2f}",
        "Chg%": "{:+.2f}%",
        "High": "{:,.2f}",
        "Low": "{:,.2f}",
    })
    st.dataframe(styled, use_container_width=True, hide_index=True)


# ── Service control ───────────────────────────────────────────────────────

def _render_service_control():
    try:
        from pakfindata.services.tick_service import (
            is_tick_service_running, start_tick_service_background, stop_tick_service,
        )
    except ImportError:
        return

    running, pid = is_tick_service_running()
    if running:
        if st.button(f"Stop (PID {pid})", key="idx_stop_btn"):
            stop_tick_service()
            st.rerun()
    else:
        if st.button("Start Collector", key="idx_start_btn", type="primary"):
            ok, msg = start_tick_service_background()
            st.toast(msg)
            st.rerun()


# ── Main render ───────────────────────────────────────────────────────────

def render_live_indices():
    """Index Monitor -- live or DB fallback, Bloomberg-style."""

    # Auto-refresh for live data
    age = _file_age()
    if age < 5 and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=2000, limit=None, key="idx_autorefresh")
    elif age < 30 and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=5000, limit=None, key="idx_autorefresh")

    # ══════════════════════════════════════════════════════════════
    # HEADER
    # ══════════════════════════════════════════════════════════════
    h1, h2, h3 = st.columns([3, 1, 0.5])
    with h1:
        st.markdown("### Index Monitor")
    with h2:
        _render_service_control()
    with h3:
        if st.button("Refresh", key="idx_refresh"):
            st.cache_data.clear()
            st.rerun()

    # ══════════════════════════════════════════════════════════════
    # DATA SOURCE: live snapshot or DB fallback
    # ══════════════════════════════════════════════════════════════
    data = _load_snapshot()
    live_mode = False
    idx_map = {}
    date_str = ""
    sparklines = {}

    if data and data.get("indices"):
        live_mode = True
        idx_map = {i["symbol"]: i for i in data["indices"]}
        sparklines = data.get("index_sparklines", {})
        ts_str = data.get("timestamp", "")
        date_str = ts_str[:19] if ts_str else ""
    else:
        # DB fallback
        con = get_connection()
        if con:
            idx_map, date_str = _get_db_indices(con)
            if idx_map is None:
                idx_map = {}
            date_str = date_str or ""

    if not idx_map:
        st.info(
            "No index data available. Start the tick collector or sync indices from the Dashboard."
        )
        render_footer()
        return

    # Status bar
    if live_mode:
        connected = data.get("connected", False)
        if connected and age < 5:
            s_color, s_text = C_UP, "LIVE"
        elif age < 30:
            s_color, s_text = "#FFB300", "STALE"
        else:
            s_color, s_text = C_DN, "DOWN"
    else:
        s_color, s_text = C_ACCENT, "DB"

    st.markdown(
        f'<div style="display:flex;align-items:center;gap:12px;margin-bottom:8px;font-size:12px;">'
        f'<span style="color:{s_color};font-weight:700;">&#9679; {s_text}</span>'
        f'<span style="color:{C_MUTED}">{len(idx_map)} indices | {date_str}</span>'
        f'</div>',
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════════════════
    # HERO: KSE-100
    # ══════════════════════════════════════════════════════════════
    kse100 = idx_map.get("KSE100")
    if kse100:
        st.markdown(_hero_html(kse100, date_str), unsafe_allow_html=True)

        # KSE-100 sparkline (live) or history chart (DB)
        spark = sparklines.get("KSE100", [])
        if len(spark) >= 2:
            st.plotly_chart(
                _sparkline_chart(spark, "KSE100", kse100.get("changePercent", 0) * 100),
                use_container_width=True,
            )
        elif not live_mode:
            con = get_connection()
            if con:
                hist = _get_index_history(con, "KSE100", 30)
                fig = _history_chart(hist, "KSE100")
                if fig:
                    st.plotly_chart(fig, use_container_width=True)

    # ══════════════════════════════════════════════════════════════
    # SECONDARY CARDS: KSE-30, KMI-30, All Share
    # ══════════════════════════════════════════════════════════════
    st.markdown(
        _secondary_cards_html(["KSE30", "KMI30", "ALLSHR", "KMIALLSHR"], idx_map),
        unsafe_allow_html=True,
    )

    # ══════════════════════════════════════════════════════════════
    # HEATMAP
    # ══════════════════════════════════════════════════════════════
    st.markdown(
        f'<div style="color:{C_MUTED};font-size:11px;font-weight:600;'
        f'letter-spacing:0.05em;margin-bottom:4px;">INDEX HEATMAP</div>',
        unsafe_allow_html=True,
    )
    st.markdown(_heatmap_html(idx_map), unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════
    # DETAILED TABLES BY CATEGORY
    # ══════════════════════════════════════════════════════════════
    equity_syms = [s for s in idx_map if _cat(s) == "equity"]
    islamic_syms = [s for s in idx_map if _cat(s) == "islamic"]
    sectoral_syms = [s for s in idx_map if _cat(s) == "sectoral"]
    other_syms = [s for s in idx_map if _cat(s) == "other"]

    tc1, tc2 = st.columns(2)
    with tc1:
        _index_table(equity_syms, idx_map, "EQUITY INDICES")
    with tc2:
        _index_table(islamic_syms, idx_map, "ISLAMIC INDICES")

    if sectoral_syms or other_syms:
        tc3, tc4 = st.columns(2)
        with tc3:
            _index_table(sectoral_syms, idx_map, "SECTORAL INDICES")
        with tc4:
            _index_table(other_syms, idx_map, "OTHER INDICES")

    # ══════════════════════════════════════════════════════════════
    # SPARKLINES GRID (live mode only)
    # ══════════════════════════════════════════════════════════════
    has_sparks = any(len(v) >= 2 for v in sparklines.values())
    if has_sparks:
        st.markdown(
            f'<div style="color:{C_MUTED};font-size:11px;font-weight:600;'
            f'letter-spacing:0.05em;margin:12px 0 4px;">INTRADAY MOVEMENT</div>',
            unsafe_allow_html=True,
        )
        display_order = equity_syms + islamic_syms + sectoral_syms + other_syms
        for row_start in range(0, len(display_order), 4):
            row_syms = display_order[row_start:row_start + 4]
            cols = st.columns(len(row_syms))
            for i, sym in enumerate(row_syms):
                spark = sparklines.get(sym, [])
                if len(spark) >= 2:
                    with cols[i]:
                        idx = idx_map.get(sym, {})
                        pct = idx.get("changePercent", 0) * 100
                        c = C_UP if pct >= 0 else C_DN
                        sign = "+" if pct >= 0 else ""
                        st.markdown(
                            f'<span style="font-size:11px;font-weight:600;color:{C_TEXT}">'
                            f'{_name(sym)}</span> '
                            f'<span style="font-size:11px;color:{c}">{sign}{pct:.2f}%</span>',
                            unsafe_allow_html=True,
                        )
                        st.plotly_chart(
                            _sparkline_chart(spark, sym, pct),
                            use_container_width=True,
                        )
    elif not live_mode:
        # Show 30-day history for top indices when not live
        con = get_connection()
        if con:
            st.markdown(
                f'<div style="color:{C_MUTED};font-size:11px;font-weight:600;'
                f'letter-spacing:0.05em;margin:12px 0 4px;">30-DAY HISTORY</div>',
                unsafe_allow_html=True,
            )
            hist_syms = ["KSE30", "KMI30", "ALLSHR"]
            cols = st.columns(len(hist_syms))
            for i, sym in enumerate(hist_syms):
                with cols[i]:
                    hist = _get_index_history(con, sym, 30)
                    fig = _history_chart(hist, sym)
                    if fig:
                        st.plotly_chart(fig, use_container_width=True)

    render_footer()
