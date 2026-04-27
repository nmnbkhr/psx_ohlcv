"""Market Pulse -- what moved today. Quant-worthy single-screen briefing."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.db.repositories.market_summary import (
    get_change_distribution,
    get_eod_breadth,
    get_latest_full_trading_day,
    get_recent_announcements,
    get_sector_performance,
    get_top_movers,
    get_value_leaders,
    get_volume_leaders,
)
from pakfindata.ui.components.helpers import get_connection, render_footer
from pakfindata.ui.themes import get_plotly_layout, get_chart_colors

# Bloomberg-style colors
_C = {
    "up": "#00C853", "dn": "#FF5252", "neu": "#6B7280",
    "bg": "#12161C", "border": "#1E2329", "text": "#EAECEF",
    "muted": "#6B7280", "accent": "#2F81F7",
}


# ── Cached queries (thin wrappers around market_summary repo) ─────────────

@st.cache_data(ttl=60)
def _load_pulse(_con):
    """Load all market pulse data in one pass via the market_summary repo."""
    date = get_latest_full_trading_day(_con, min_symbols=1)
    if not date:
        return None

    breadth = get_eod_breadth(_con, date=date) or {}
    # Remap canonical names to the renderer's expectations
    breadth_view = {
        "gainers": breadth.get("gainers"),
        "losers": breadth.get("losers"),
        "unchanged": breadth.get("unchanged"),
        "total": breadth.get("total"),
        "total_vol": breadth.get("total_volume"),
        "total_value": breadth.get("total_value"),
        "avg_chg": breadth.get("avg_change"),
    }

    # Movers return 'change_pct'; renderer expects 'chg_pct' for the styler
    gainers_df = get_top_movers(_con, direction="gainers", date=date, limit=10)
    losers_df = get_top_movers(_con, direction="losers", date=date, limit=10)
    vol_df = get_volume_leaders(_con, date=date, limit=10)
    val_df = get_value_leaders(_con, date=date, limit=10)
    for df in (gainers_df, losers_df, vol_df, val_df):
        if not df.empty and "change_pct" in df.columns and "chg_pct" not in df.columns:
            df.rename(columns={"change_pct": "chg_pct"}, inplace=True)

    return {
        "date": date,
        "breadth": breadth_view,
        "gainers": gainers_df,
        "losers": losers_df,
        "vol_leaders": vol_df,
        "val_leaders": val_df,
        "change_dist": get_change_distribution(_con, date=date),
        "sectors": get_sector_performance(_con, date=date, min_stocks=3),
        "announcements": get_recent_announcements(_con, limit=8),
    }


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
    st.dataframe(styled, width='stretch', hide_index=True, height=340)


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
        st.plotly_chart(_breadth_donut(g, l, u), width='stretch')

    with c2:
        dist_df = pulse.get("change_dist", pd.DataFrame())
        if not dist_df.empty and "chg_pct" in dist_df.columns:
            st.plotly_chart(_change_histogram(dist_df), width='stretch')

    with c3:
        sect_df = pulse.get("sectors", pd.DataFrame())
        if not sect_df.empty:
            st.plotly_chart(_sector_bar(sect_df), width='stretch')

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
            st.dataframe(ann_df, width='stretch', hide_index=True, height=250)

    render_footer()
