"""Market Pulse -- what moved today. Quant-worthy single-screen briefing."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer
from pakfindata.ui.themes import get_plotly_layout, get_chart_colors

# Bloomberg-style colors
_C = {
    "up": "#00C853", "dn": "#FF5252", "neu": "#6B7280",
    "bg": "#12161C", "border": "#1E2329", "text": "#EAECEF",
    "muted": "#6B7280", "accent": "#2F81F7",
}


# ── Cached queries ────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def _load_pulse(_con):
    """Load all market pulse data in one pass."""
    date = None
    try:
        row = _con.execute("SELECT MAX(date) FROM eod_ohlcv").fetchone()
        date = row[0] if row and row[0] else None
    except Exception:
        return None

    if not date:
        return None

    out = {"date": date}

    # Breadth
    try:
        b = _con.execute("""
            SELECT
                SUM(CASE WHEN close > prev_close THEN 1 ELSE 0 END) as gainers,
                SUM(CASE WHEN close < prev_close THEN 1 ELSE 0 END) as losers,
                SUM(CASE WHEN close = prev_close OR prev_close IS NULL OR prev_close = 0 THEN 1 ELSE 0 END) as unchanged,
                COUNT(*) as total,
                SUM(volume) as total_vol,
                SUM(close * volume) as total_value,
                ROUND(AVG(CASE WHEN prev_close > 0 THEN (close - prev_close) / prev_close * 100 END), 2) as avg_chg
            FROM eod_ohlcv WHERE date = ? AND prev_close > 0
        """, (date,)).fetchone()
        if b:
            out["breadth"] = dict(b)
    except Exception:
        pass

    # Top gainers
    try:
        out["gainers"] = pd.read_sql_query("""
            SELECT symbol, close, prev_close,
                   ROUND((close - prev_close) / prev_close * 100, 2) as chg_pct,
                   volume
            FROM eod_ohlcv
            WHERE date = ? AND prev_close > 0 AND close > prev_close
            ORDER BY chg_pct DESC LIMIT 10
        """, _con, params=(date,))
    except Exception:
        out["gainers"] = pd.DataFrame()

    # Top losers
    try:
        out["losers"] = pd.read_sql_query("""
            SELECT symbol, close, prev_close,
                   ROUND((close - prev_close) / prev_close * 100, 2) as chg_pct,
                   volume
            FROM eod_ohlcv
            WHERE date = ? AND prev_close > 0 AND close < prev_close
            ORDER BY chg_pct ASC LIMIT 10
        """, _con, params=(date,))
    except Exception:
        out["losers"] = pd.DataFrame()

    # Volume leaders
    try:
        out["vol_leaders"] = pd.read_sql_query("""
            SELECT symbol, close, volume,
                   ROUND((close - prev_close) / prev_close * 100, 2) as chg_pct
            FROM eod_ohlcv
            WHERE date = ? AND volume > 0 AND prev_close > 0
            ORDER BY volume DESC LIMIT 10
        """, _con, params=(date,))
    except Exception:
        out["vol_leaders"] = pd.DataFrame()

    # Value leaders (turnover proxy = close * volume)
    try:
        out["val_leaders"] = pd.read_sql_query("""
            SELECT symbol, close, volume, close * volume as value,
                   ROUND((close - prev_close) / prev_close * 100, 2) as chg_pct
            FROM eod_ohlcv
            WHERE date = ? AND volume > 0 AND prev_close > 0
            ORDER BY value DESC LIMIT 10
        """, _con, params=(date,))
    except Exception:
        out["val_leaders"] = pd.DataFrame()

    # Change distribution (for histogram)
    try:
        out["change_dist"] = pd.read_sql_query("""
            SELECT ROUND((close - prev_close) / prev_close * 100, 1) as chg_pct
            FROM eod_ohlcv
            WHERE date = ? AND prev_close > 0
        """, _con, params=(date,))
    except Exception:
        out["change_dist"] = pd.DataFrame()

    # Sector performance
    try:
        out["sectors"] = pd.read_sql_query("""
            SELECT s.sector_name as sector,
                   COUNT(*) as stocks,
                   ROUND(AVG((e.close - e.prev_close) / e.prev_close * 100), 2) as avg_chg,
                   SUM(e.volume) as total_vol,
                   SUM(CASE WHEN e.close > e.prev_close THEN 1 ELSE 0 END) as up,
                   SUM(CASE WHEN e.close < e.prev_close THEN 1 ELSE 0 END) as down
            FROM eod_ohlcv e
            JOIN sector_map s ON e.symbol = s.symbol
            WHERE e.date = ? AND e.prev_close > 0
            GROUP BY s.sector_name
            HAVING COUNT(*) >= 3
            ORDER BY avg_chg DESC
        """, _con, params=(date,))
    except Exception:
        out["sectors"] = pd.DataFrame()

    # Recent announcements
    try:
        out["announcements"] = pd.read_sql_query("""
            SELECT symbol, announcement_date as date, subject
            FROM company_announcements
            ORDER BY announcement_date DESC LIMIT 8
        """, _con)
    except Exception:
        try:
            out["announcements"] = pd.read_sql_query("""
                SELECT symbol, date, headline as subject
                FROM corporate_announcements
                ORDER BY date DESC LIMIT 8
            """, _con)
        except Exception:
            out["announcements"] = pd.DataFrame()

    return out


# ── Charts ────────────────────────────────────────────────────────────────

def _breadth_donut(g, l, u):
    """Compact breadth donut."""
    layout = get_plotly_layout()
    layout.pop("margin", None)
    fig = go.Figure(go.Pie(
        values=[g, l, u],
        labels=["Gainers", "Losers", "Unch"],
        marker=dict(colors=[_C["up"], _C["dn"], _C["neu"]]),
        hole=0.65,
        textinfo="value",
        textfont=dict(size=12, color=_C["text"]),
        hoverinfo="label+value+percent",
    ))
    fig.update_layout(
        **layout,
        showlegend=False,
        height=200,
        margin=dict(l=10, r=10, t=10, b=10),
        annotations=[dict(
            text=f"<b>{g}</b>/<b>{l}</b>",
            x=0.5, y=0.5, font=dict(size=16, color=_C["text"]),
            showarrow=False,
        )],
    )
    return fig


def _change_histogram(df):
    """Return distribution histogram."""
    layout = get_plotly_layout()
    for k in ("margin", "title", "xaxis", "yaxis"):
        layout.pop(k, None)
    fig = go.Figure(go.Histogram(
        x=df["chg_pct"],
        nbinsx=40,
        marker=dict(
            color=[_C["up"] if x >= 0 else _C["dn"] for x in df["chg_pct"]],
            line=dict(width=0),
        ),
        hovertemplate="Change: %{x:.1f}%<br>Count: %{y}<extra></extra>",
    ))
    fig.update_layout(
        **layout,
        height=200,
        margin=dict(l=10, r=10, t=30, b=30),
        title=dict(text="Return Distribution", font=dict(size=12)),
        xaxis=dict(title="Change %"),
        yaxis=dict(title="Stocks"),
        bargap=0.05,
    )
    return fig


def _sector_bar(df):
    """Horizontal sector performance bar chart."""
    layout = get_plotly_layout()
    for k in ("margin", "title", "xaxis", "yaxis"):
        layout.pop(k, None)
    df_sorted = df.sort_values("avg_chg", ascending=True).tail(12)
    colors = [_C["up"] if v >= 0 else _C["dn"] for v in df_sorted["avg_chg"]]
    fig = go.Figure(go.Bar(
        y=df_sorted["sector"],
        x=df_sorted["avg_chg"],
        orientation="h",
        marker=dict(color=colors),
        text=[f"{v:+.2f}%" for v in df_sorted["avg_chg"]],
        textposition="outside",
        textfont=dict(size=10, color=_C["muted"]),
        hovertemplate="%{y}: %{x:+.2f}%<extra></extra>",
    ))
    fig.update_layout(
        **layout,
        height=max(220, len(df_sorted) * 22),
        margin=dict(l=10, r=40, t=30, b=10),
        title=dict(text="Sector Performance", font=dict(size=12)),
        yaxis=dict(side="left", tickfont=dict(size=10)),
        xaxis=dict(title="Avg Change %"),
    )
    return fig


# ── Styled table helpers ──────────────────────────────────────────────────

def _styled_movers_table(df, columns_map):
    """Render a compact, color-coded movers table."""
    if df.empty:
        return
    display = df.rename(columns=columns_map)

    def _color_chg(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return f"color: {_C['up']}"
            elif val < 0:
                return f"color: {_C['dn']}"
        return ""

    chg_cols = [v for k, v in columns_map.items() if "chg" in k.lower() or "pct" in k.lower()]
    chg_cols = [c for c in chg_cols if c in display.columns]

    fmt = {}
    for c in display.columns:
        if "%" in c or "Chg" in c:
            fmt[c] = "{:+.2f}%"
        elif c == "Volume" or c == "Vol":
            fmt[c] = "{:,.0f}"
        elif c in ("Close", "Price", "Value", "Prev"):
            fmt[c] = "{:,.2f}"

    styled = display.style
    if chg_cols:
        styled = styled.map(_color_chg, subset=chg_cols)
    styled = styled.format({k: v for k, v in fmt.items() if k in display.columns})
    st.dataframe(styled, use_container_width=True, hide_index=True, height=340)


# ── Main render ───────────────────────────────────────────────────────────

def render_market_pulse():
    """Quant-worthy market pulse -- single-screen daily briefing."""

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    pulse = _load_pulse(con)
    if pulse is None:
        st.info("No market data available. Sync EOD data first.")
        render_footer()
        return

    date = pulse["date"]
    breadth = pulse.get("breadth", {})

    # ══════════════════════════════════════════════════════════════
    # HEADER
    # ══════════════════════════════════════════════════════════════
    h1, h2 = st.columns([4, 1])
    with h1:
        st.markdown("### Market Pulse")
        avg_chg = breadth.get("avg_chg", 0) or 0
        avg_color = _C["up"] if avg_chg > 0 else _C["dn"] if avg_chg < 0 else _C["neu"]
        st.markdown(
            f'<span style="color:{_C["muted"]};font-size:13px;">{date}</span> '
            f'<span style="color:{avg_color};font-size:13px;font-weight:600;">'
            f'Mkt Avg: {avg_chg:+.2f}%</span>',
            unsafe_allow_html=True,
        )
    with h2:
        if st.button("Refresh", key="pulse_refresh"):
            st.cache_data.clear()
            st.rerun()

    # ══════════════════════════════════════════════════════════════
    # ROW 1: BREADTH STRIP
    # ══════════════════════════════════════════════════════════════
    g = breadth.get("gainers", 0) or 0
    l = breadth.get("losers", 0) or 0
    u = breadth.get("unchanged", 0) or 0
    total_vol = breadth.get("total_vol", 0) or 0
    total_val = breadth.get("total_value", 0) or 0
    vol_str = f"{total_vol/1e6:.0f}M" if total_vol >= 1e6 else f"{total_vol:,.0f}"
    val_str = f"Rs.{total_val/1e9:.2f}B" if total_val >= 1e9 else f"Rs.{total_val/1e6:.0f}M" if total_val >= 1e6 else f"Rs.{total_val:,.0f}"

    total = g + l or 1
    g_pct = g / total * 100
    l_pct = l / total * 100

    st.markdown(f"""
    <div style="display:flex;align-items:center;gap:16px;padding:8px 12px;
                background:{_C["bg"]};border:1px solid {_C["border"]};border-radius:2px;
                font-family:ui-monospace,monospace;font-size:12px;margin-bottom:8px;">
      <span style="color:{_C["up"]};font-weight:600">{g} Adv</span>
      <span style="color:{_C["dn"]};font-weight:600">{l} Dec</span>
      <span style="color:{_C["neu"]}">{u} Unch</span>
      <div style="flex:1;display:flex;height:4px;border-radius:2px;overflow:hidden;margin:0 8px;">
        <div style="width:{g_pct:.0f}%;background:{_C["up"]}"></div>
        <div style="width:{l_pct:.0f}%;background:{_C["dn"]}"></div>
      </div>
      <span style="color:{_C["muted"]}">Vol</span>
      <span style="color:{_C["text"]};font-weight:600">{vol_str}</span>
      <span style="color:{_C["muted"]}">Val</span>
      <span style="color:{_C["text"]};font-weight:600">{val_str}</span>
    </div>
    """, unsafe_allow_html=True)

    # ══════════════════════════════════════════════════════════════
    # ROW 2: DONUT + HISTOGRAM + SECTOR BAR
    # ══════════════════════════════════════════════════════════════
    c1, c2, c3 = st.columns([1, 1.5, 2])

    with c1:
        st.plotly_chart(_breadth_donut(g, l, u), use_container_width=True)

    with c2:
        dist_df = pulse.get("change_dist", pd.DataFrame())
        if not dist_df.empty and "chg_pct" in dist_df.columns:
            st.plotly_chart(_change_histogram(dist_df), use_container_width=True)

    with c3:
        sect_df = pulse.get("sectors", pd.DataFrame())
        if not sect_df.empty:
            st.plotly_chart(_sector_bar(sect_df), use_container_width=True)

    # ══════════════════════════════════════════════════════════════
    # ROW 3: GAINERS | LOSERS
    # ══════════════════════════════════════════════════════════════
    t1, t2 = st.columns(2)

    with t1:
        st.markdown(
            f'<div style="color:{_C["up"]};font-size:11px;font-weight:600;'
            f'letter-spacing:0.05em;">TOP GAINERS</div>',
            unsafe_allow_html=True,
        )
        _styled_movers_table(
            pulse.get("gainers", pd.DataFrame()),
            {"symbol": "Symbol", "close": "Close", "prev_close": "Prev",
             "chg_pct": "Chg%", "volume": "Vol"},
        )

    with t2:
        st.markdown(
            f'<div style="color:{_C["dn"]};font-size:11px;font-weight:600;'
            f'letter-spacing:0.05em;">TOP LOSERS</div>',
            unsafe_allow_html=True,
        )
        _styled_movers_table(
            pulse.get("losers", pd.DataFrame()),
            {"symbol": "Symbol", "close": "Close", "prev_close": "Prev",
             "chg_pct": "Chg%", "volume": "Vol"},
        )

    # ══════════════════════════════════════════════════════════════
    # ROW 4: VOLUME LEADERS | VALUE LEADERS
    # ══════════════════════════════════════════════════════════════
    v1, v2 = st.columns(2)

    with v1:
        st.markdown(
            '<div style="color:#2F81F7;font-size:11px;font-weight:600;'
            'letter-spacing:0.05em;">VOLUME LEADERS</div>',
            unsafe_allow_html=True,
        )
        _styled_movers_table(
            pulse.get("vol_leaders", pd.DataFrame()),
            {"symbol": "Symbol", "close": "Close", "volume": "Volume",
             "chg_pct": "Chg%"},
        )

    with v2:
        st.markdown(
            '<div style="color:#FFB300;font-size:11px;font-weight:600;'
            'letter-spacing:0.05em;">VALUE LEADERS (TURNOVER)</div>',
            unsafe_allow_html=True,
        )
        val_df = pulse.get("val_leaders", pd.DataFrame())
        if not val_df.empty:
            _styled_movers_table(
                val_df,
                {"symbol": "Symbol", "close": "Close", "volume": "Vol",
                 "value": "Value", "chg_pct": "Chg%"},
            )

    # ══════════════════════════════════════════════════════════════
    # ROW 5: SECTOR TABLE (detailed)
    # ══════════════════════════════════════════════════════════════
    if not sect_df.empty:
        with st.expander("Sector Breakdown", expanded=False):
            _styled_movers_table(
                sect_df,
                {"sector": "Sector", "stocks": "Stocks", "avg_chg": "Avg Chg%",
                 "total_vol": "Volume", "up": "Adv", "down": "Dec"},
            )

    # ══════════════════════════════════════════════════════════════
    # ROW 6: ANNOUNCEMENTS
    # ══════════════════════════════════════════════════════════════
    ann_df = pulse.get("announcements", pd.DataFrame())
    if not ann_df.empty:
        with st.expander("Recent Announcements", expanded=False):
            st.dataframe(ann_df, use_container_width=True, hide_index=True, height=250)

    render_footer()
