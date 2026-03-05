"""Intraday Trading Terminal — PSX tick-level analytics.

Tabs:
  Dashboard — Market KPIs, top movers, sector heatmap, breadth gauge
  Charts — Single-symbol candlestick, VWAP, volume profile, Bollinger bands
  Market Pulse — Advance/decline, tick distribution, intraday momentum
  Volume — Volume leaders, unusual activity, block trades, concentration
  Movers — Gainers, losers, most active with visual cards and scatter
  Sync — All bulk/single sync controls preserved
"""

import sqlite3
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from pakfindata.config import get_db_path
from pakfindata.query import (
    get_intraday_latest,
    get_intraday_stats,
    get_symbols_list,
)
from pakfindata.services import (
    is_service_running,
    read_status as read_service_status,
)
from pakfindata.sync import sync_intraday
from pakfindata.sync_timeseries import (
    is_intraday_sync_running,
    read_intraday_sync_progress,
    start_intraday_sync,
)
from pakfindata.ui.components.helpers import (
    EXPORTS_DIR,
    format_volume,
    get_connection,
    render_footer,
    render_market_status_badge,
)

INTRADAY_TEMP_DIR = Path("/mnt/e/psxdata/intradaytemp")


# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676",
    "down": "#FF5252",
    "neutral": "#78909C",
    "accent": "#00D4AA",
    "blue": "#42A5F5",
    "orange": "#FF9800",
    "purple": "#AB47BC",
    "gold": "#FFD700",
    "bg": "#0e1117",
    "card_bg": "#1a1a2e",
    "grid": "#2d2d3d",
    "text": "#e0e0e0",
    "text_dim": "#888888",
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11),
    margin=dict(l=10, r=10, t=40, b=10),
)

_AXIS_STYLE = dict(gridcolor=_COLORS["grid"], zeroline=False)


def _apply_layout(fig: go.Figure, **kwargs) -> go.Figure:
    """Apply _CHART_LAYOUT + axis styling + any overrides. Avoids duplicate kwarg errors."""
    fig.update_layout(**_CHART_LAYOUT, **kwargs)
    fig.update_xaxes(**_AXIS_STYLE)
    fig.update_yaxes(**_AXIS_STYLE)
    return fig


def _styled_fig(height: int = 400, **kwargs) -> go.Figure:
    layout = {**_CHART_LAYOUT, "height": height, **kwargs}
    fig = go.Figure(layout=layout)
    fig.update_xaxes(**_AXIS_STYLE)
    fig.update_yaxes(**_AXIS_STYLE)
    return fig


def _change_color(val):
    if val > 0:
        return _COLORS["up"]
    elif val < 0:
        return _COLORS["down"]
    return _COLORS["neutral"]


def _metric_card(label, value, delta=None, prefix="", suffix=""):
    card_bg = _COLORS["card_bg"]
    text_col = _COLORS["text"]
    dim = _COLORS["text_dim"]
    delta_html = ""
    if delta is not None:
        d_color = _change_color(delta)
        sign = "+" if delta > 0 else ""
        delta_html = (
            f'<div style="font-size:13px;color:{d_color};margin-top:2px">'
            f'{sign}{delta:.2f}%</div>'
        )
    return (
        f'<div style="background:{card_bg};border-radius:8px;padding:14px 16px;'
        f'text-align:center;border:1px solid {_COLORS["grid"]}">'
        f'<div style="font-size:11px;color:{dim};text-transform:uppercase;'
        f'letter-spacing:1px">{label}</div>'
        f'<div style="font-size:22px;font-weight:700;color:{text_col};'
        f'margin-top:4px">{prefix}{value}{suffix}</div>'
        f'{delta_html}</div>'
    )


def _ts_range(date_str: str) -> tuple[str, str]:
    """Return (start, end) ts strings for index-friendly BETWEEN filter."""
    return f"{date_str} 00:00:00", f"{date_str} 23:59:59"


def _load_today_summary(con: sqlite3.Connection, date_str: str) -> pd.DataFrame:
    """Load aggregated intraday summary for a given date."""
    ts_start, ts_end = _ts_range(date_str)
    return pd.read_sql_query(
        """
        WITH ranked AS (
            SELECT symbol, ts_epoch, close, volume,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts_epoch ASC) AS rn_first,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY ts_epoch DESC) AS rn_last
            FROM intraday_bars WHERE ts BETWEEN ? AND ?
        )
        SELECT
            symbol,
            COUNT(*) AS ticks,
            MAX(CASE WHEN rn_first=1 THEN close END) AS open,
            MAX(close) AS high,
            MIN(close) AS low,
            MAX(CASE WHEN rn_last=1 THEN close END) AS last_price,
            MAX(volume) AS total_vol,
            MAX(CASE WHEN rn_first=1 THEN ts_epoch END) AS first_epoch,
            MAX(CASE WHEN rn_last=1 THEN ts_epoch END) AS last_epoch
        FROM ranked
        GROUP BY symbol
        HAVING COUNT(*) >= 2
        ORDER BY total_vol DESC
        """,
        con,
        params=[ts_start, ts_end],
    )


def _add_sector_info(con: sqlite3.Connection, df: pd.DataFrame, date_str: str = "") -> pd.DataFrame:
    """Add sector_code, company_name, turnover, and pc_volume from post_close_turnover + eod_ohlcv."""
    if df.empty:
        df["sector_code"] = ""
        df["company_name"] = ""
        df["turnover"] = 0.0
        df["pc_volume"] = 0
        return df

    syms = df["symbol"].tolist()
    placeholders = ",".join("?" * len(syms))

    # 1. Get turnover + volume from post_close_turnover (authoritative source)
    pc_date = date_str if date_str else "(SELECT MAX(date) FROM post_close_turnover)"
    pc_date_filter = "date = ?" if date_str else f"date = {pc_date}"
    pc_params = syms + ([date_str] if date_str else [])
    try:
        pc_df = pd.read_sql_query(
            f"""SELECT symbol, turnover, volume AS pc_volume, company_name AS pc_company
                FROM post_close_turnover
                WHERE symbol IN ({placeholders})
                  AND {pc_date_filter}""",
            con,
            params=pc_params,
        )
    except Exception:
        pc_df = pd.DataFrame()

    # 2. Get sector_code and company_name from eod_ohlcv
    eod_date_filter = f"date = '{date_str}'" if date_str else "date = (SELECT MAX(date) FROM eod_ohlcv)"
    sector_df = pd.read_sql_query(
        f"""SELECT symbol, sector_code, company_name FROM eod_ohlcv
            WHERE symbol IN ({placeholders})
              AND {eod_date_filter}""",
        con,
        params=syms,
    )

    # Fallback to latest date if no match for specific date
    if sector_df.empty or "sector_code" not in sector_df.columns:
        sector_df = pd.read_sql_query(
            f"""SELECT symbol, sector_code, company_name FROM eod_ohlcv
                WHERE symbol IN ({placeholders})
                  AND date = (SELECT MAX(date) FROM eod_ohlcv)""",
            con,
            params=syms,
        )

    if sector_df.empty:
        sector_df = pd.read_sql_query(
            f"SELECT symbol, sector AS sector_code, name AS company_name FROM symbols WHERE symbol IN ({placeholders})",
            con,
            params=syms,
        )

    # Merge sector info
    if not sector_df.empty:
        sector_df = sector_df.drop_duplicates(subset="symbol")
        df = df.merge(sector_df, on="symbol", how="left")
    else:
        df["sector_code"] = ""
        df["company_name"] = ""

    # Merge post_close turnover + volume (overrides eod_ohlcv company_name if available)
    if not pc_df.empty:
        pc_df = pc_df.drop_duplicates(subset="symbol")
        df = df.merge(pc_df[["symbol", "turnover", "pc_volume"]], on="symbol", how="left")
        # Use pc_company as fallback for company_name
        if "pc_company" in pc_df.columns:
            pc_names = pc_df.set_index("symbol")["pc_company"].to_dict()
            mask = df["company_name"].isna() | (df["company_name"] == "")
            df.loc[mask, "company_name"] = df.loc[mask, "symbol"].map(pc_names)
    else:
        df["turnover"] = 0.0
        df["pc_volume"] = 0

    df["sector_code"] = df["sector_code"].fillna("")
    df["company_name"] = df["company_name"].fillna("")
    if "turnover" not in df.columns:
        df["turnover"] = 0.0
    df["turnover"] = pd.to_numeric(df["turnover"], errors="coerce").fillna(0.0)
    if "pc_volume" not in df.columns:
        df["pc_volume"] = 0
    df["pc_volume"] = pd.to_numeric(df["pc_volume"], errors="coerce").fillna(0).astype(int)
    return df


# ═════════════════════════════════════════════════════════════════════════════
# SECTOR CODE LABELS
# ═════════════════════════════════════════════════════════════════════════════

_SECTOR_LABELS = {
    "0101": "Banks", "0102": "Inv.Banks", "0103": "Modaraba", "0104": "Leasing",
    "0105": "Insurance", "0106": "Close-End Funds",
    "0201": "Textile Composite", "0202": "Textile Spinning", "0203": "Textile Weaving",
    "0301": "Sugar", "0302": "Food", "0303": "Tobacco",
    "0401": "Cement", "0402": "Glass", "0403": "Ceramics",
    "0501": "Chemical", "0502": "Pharma", "0503": "Fertilizer",
    "0601": "Engineering", "0602": "Auto Assembler", "0603": "Auto Parts",
    "0604": "Cable", "0605": "Transport", "0606": "Technology",
    "0701": "Paper", "0702": "Vanaspati", "0703": "Leather",
    "0801": "Refinery", "0802": "Power", "0803": "Oil & Gas Mktg",
    "0804": "Oil & Gas Expl", "0805": "Gas Distribution",
    "0807": "Real Estate",
    "0900": "Miscellaneous", "0901": "Misc",
}


# ═════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═════════════════════════════════════════════════════════════════════════════

def render_intraday():
    """Intraday Trading Terminal."""
    # Auto-refresh
    service_running, service_pid = is_service_running()
    service_status = read_service_status()
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        st_autorefresh(interval=60000, limit=None, key="intraday_autorefresh")

    # Header
    h1, h2, h3 = st.columns([2, 1, 1])
    with h1:
        st.markdown("## Intraday Trading Terminal")
        st.caption("PSX tick-level analytics, market breadth & momentum")
    with h2:
        render_market_status_badge()
    with h3:
        if service_running:
            st.success("Auto-Sync ON")
            if service_status.last_run_at:
                st.caption(f"Last: {service_status.last_run_at[:19]}")
        else:
            st.info("Auto-Sync OFF")

    # Date selector
    con = get_connection()
    today_str = date.today().isoformat()

    try:
        from pakfindata.db.repositories.intraday import get_intraday_dates
        avail_dates = get_intraday_dates(con)
    except Exception:
        avail_dates = [today_str]

    sel_date = st.selectbox(
        "Trading Date",
        avail_dates if avail_dates else [today_str],
        index=0,
        key="int_date_sel",
    )

    # Load data for selected date
    summary_df = _load_today_summary(con, sel_date)
    if not summary_df.empty:
        summary_df["change"] = summary_df["last_price"] - summary_df["open"]
        summary_df["change_pct"] = (
            summary_df["change"] / summary_df["open"] * 100
        ).replace([np.inf, -np.inf], 0).fillna(0)
        summary_df["range_pct"] = (
            (summary_df["high"] - summary_df["low"]) / summary_df["low"] * 100
        ).replace([np.inf, -np.inf], 0).fillna(0)
        summary_df = _add_sector_info(con, summary_df, sel_date)

    # Tabs
    tab_dash, tab_charts, tab_pulse, tab_vol, tab_movers, tab_sync = st.tabs(
        ["Dashboard", "Charts", "Market Pulse", "Volume", "Movers", "Sync"]
    )

    # ═════════════════════════════════════════════════════════════════════
    # TAB 1: DASHBOARD
    # ═════════════════════════════════════════════════════════════════════
    with tab_dash:
        try:
            _render_dashboard(con, summary_df, sel_date)
        except Exception as e:
            st.error(f"Dashboard error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 2: CHARTS
    # ═════════════════════════════════════════════════════════════════════
    with tab_charts:
        try:
            _render_charts(con, summary_df, sel_date)
        except Exception as e:
            st.error(f"Charts error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 3: MARKET PULSE
    # ═════════════════════════════════════════════════════════════════════
    with tab_pulse:
        try:
            _render_market_pulse(con, summary_df, sel_date)
        except Exception as e:
            st.error(f"Market Pulse error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 4: VOLUME
    # ═════════════════════════════════════════════════════════════════════
    with tab_vol:
        try:
            _render_volume(con, summary_df, sel_date)
        except Exception as e:
            st.error(f"Volume error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 5: MOVERS
    # ═════════════════════════════════════════════════════════════════════
    with tab_movers:
        try:
            _render_movers(con, summary_df, sel_date)
        except Exception as e:
            st.error(f"Movers error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 6: SYNC
    # ═════════════════════════════════════════════════════════════════════
    with tab_sync:
        try:
            _render_sync(con, sel_date)
        except Exception as e:
            st.error(f"Sync error: {e}")

    render_footer()


# ═════════════════════════════════════════════════════════════════════════════
# TAB: DASHBOARD
# ═════════════════════════════════════════════════════════════════════════════

def _render_dashboard(con, df, sel_date):
    if df.empty:
        st.info(f"No intraday data for {sel_date}. Use Sync tab to fetch.")
        return

    n_symbols = len(df)
    advancers = (df["change_pct"] > 0).sum()
    decliners = (df["change_pct"] < 0).sum()
    unchanged = n_symbols - advancers - decliners
    total_vol = df["total_vol"].sum()
    total_pc_vol = df["pc_volume"].sum() if "pc_volume" in df.columns else 0
    total_turnover = df["turnover"].sum() if "turnover" in df.columns else 0
    avg_change = df["change_pct"].mean()
    total_ticks = df["ticks"].sum()

    def _fmt_turnover(val):
        if val >= 1e9:
            return f"Rs.{val / 1e9:.2f}B"
        if val >= 1e6:
            return f"Rs.{val / 1e6:.1f}M"
        if val >= 1e3:
            return f"Rs.{val / 1e3:.0f}K"
        return f"Rs.{val:,.0f}"

    # Row 1: Market KPIs
    c1, c2, c3, c4, c5, c6, c7, c8 = st.columns(8)
    c1.markdown(_metric_card("Symbols", f"{n_symbols:,}"), unsafe_allow_html=True)
    c2.markdown(
        _metric_card("Advancers", f"{advancers}", delta=advancers / n_symbols * 100 if n_symbols else 0),
        unsafe_allow_html=True,
    )
    c3.markdown(
        _metric_card("Decliners", f"{decliners}", delta=-(decliners / n_symbols * 100) if n_symbols else 0),
        unsafe_allow_html=True,
    )
    c4.markdown(_metric_card("Unchanged", f"{unchanged}"), unsafe_allow_html=True)
    c5.markdown(_metric_card("Tick Volume", format_volume(total_vol)), unsafe_allow_html=True)
    c6.markdown(_metric_card("PSX Volume", format_volume(total_pc_vol) if total_pc_vol else "—"), unsafe_allow_html=True)
    c7.markdown(_metric_card("Turnover", _fmt_turnover(total_turnover)), unsafe_allow_html=True)
    c8.markdown(_metric_card("Total Ticks", f"{total_ticks:,}"), unsafe_allow_html=True)

    st.markdown("")

    # Row 2: Breadth + Avg Return
    c1, c2, c3, c4 = st.columns(4)
    ad_ratio = advancers / max(decliners, 1)
    c1.markdown(_metric_card("A/D Ratio", f"{ad_ratio:.2f}"), unsafe_allow_html=True)
    c2.markdown(_metric_card("Avg Change", f"{avg_change:.2f}%", delta=avg_change), unsafe_allow_html=True)
    median_chg = df["change_pct"].median()
    c3.markdown(_metric_card("Median Change", f"{median_chg:.2f}%", delta=median_chg), unsafe_allow_html=True)
    avg_range = df["range_pct"].mean()
    c4.markdown(_metric_card("Avg Range", f"{avg_range:.2f}%"), unsafe_allow_html=True)

    st.markdown("---")

    # Breadth gauge + Top movers side-by-side
    col_l, col_r = st.columns([1, 1])

    with col_l:
        st.markdown("**Market Breadth**")
        # Stacked bar showing advance/decline/unchanged
        fig_breadth = go.Figure()
        fig_breadth.add_trace(go.Bar(
            x=[advancers], y=["Breadth"], orientation="h",
            name="Advance", marker_color=_COLORS["up"], text=[advancers], textposition="inside",
        ))
        fig_breadth.add_trace(go.Bar(
            x=[unchanged], y=["Breadth"], orientation="h",
            name="Unchanged", marker_color=_COLORS["neutral"], text=[unchanged], textposition="inside",
        ))
        fig_breadth.add_trace(go.Bar(
            x=[decliners], y=["Breadth"], orientation="h",
            name="Decline", marker_color=_COLORS["down"], text=[decliners], textposition="inside",
        ))
        _apply_layout(
            fig_breadth, height=120, barmode="stack", showlegend=True,
            legend=dict(orientation="h", y=-0.3, bgcolor="rgba(0,0,0,0)"),
            yaxis=dict(visible=False, gridcolor=_COLORS["grid"]),
            xaxis=dict(visible=False, gridcolor=_COLORS["grid"]),
        )
        st.plotly_chart(fig_breadth, use_container_width=True)

        # Change distribution histogram
        st.markdown("**Return Distribution**")
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=df["change_pct"], nbinsx=50,
            marker_color=_COLORS["accent"], opacity=0.8,
        ))
        fig_hist.add_vline(x=0, line_dash="dash", line_color=_COLORS["text_dim"])
        _apply_layout(fig_hist, height=250, xaxis_title="Change %", yaxis_title="Count")
        st.plotly_chart(fig_hist, use_container_width=True)

    with col_r:
        # Top gainers and losers quick view
        st.markdown("**Top 10 Gainers**")
        top_gain = df.nlargest(10, "change_pct")[["symbol", "last_price", "change_pct", "total_vol"]]
        st.dataframe(
            top_gain.style.applymap(
                lambda v: f"color: {_COLORS['up']}" if isinstance(v, (int, float)) and v > 0
                else f"color: {_COLORS['down']}" if isinstance(v, (int, float)) and v < 0
                else "",
                subset=["change_pct"],
            ),
            use_container_width=True, hide_index=True, height=200,
        )

        st.markdown("**Top 10 Losers**")
        top_lose = df.nsmallest(10, "change_pct")[["symbol", "last_price", "change_pct", "total_vol"]]
        st.dataframe(
            top_lose.style.applymap(
                lambda v: f"color: {_COLORS['up']}" if isinstance(v, (int, float)) and v > 0
                else f"color: {_COLORS['down']}" if isinstance(v, (int, float)) and v < 0
                else "",
                subset=["change_pct"],
            ),
            use_container_width=True, hide_index=True, height=200,
        )

    st.markdown("---")

    # Sector heatmap
    st.markdown("**Sector Performance Heatmap**")
    sector_df = df[df["sector_code"] != ""].copy()
    if not sector_df.empty:
        sector_df["sector_name"] = sector_df["sector_code"].map(
            lambda c: _SECTOR_LABELS.get(c, c)
        )
        sector_agg = sector_df.groupby("sector_name").agg(
            avg_chg=("change_pct", "mean"),
            vol=("total_vol", "sum"),
            count=("symbol", "count"),
        ).reset_index()
        sector_agg = sector_agg.sort_values("avg_chg", ascending=False)

        if len(sector_agg) > 0:
            fig_sector = go.Figure(go.Treemap(
                ids=sector_agg["sector_name"],
                labels=sector_agg["sector_name"],
                parents=[""] * len(sector_agg),
                values=sector_agg["vol"].clip(lower=1),
                text=sector_agg.apply(
                    lambda r: f"{r['avg_chg']:+.2f}%<br>{r['count']} stocks", axis=1
                ),
                textinfo="label+text",
                marker=dict(
                    colors=sector_agg["avg_chg"],
                    colorscale=[[0, _COLORS["down"]], [0.5, _COLORS["neutral"]], [1, _COLORS["up"]]],
                    cmid=0,
                    colorbar=dict(title="Chg%", thickness=15),
                ),
            ))
            _apply_layout(
                fig_sector, height=450,
                title=dict(text="Sectors by Volume (colored by avg change)", font=dict(size=13)),
            )
            st.plotly_chart(fig_sector, use_container_width=True)
    else:
        st.caption("Sector data not available — run EOD sync to populate sector codes.")


# ═════════════════════════════════════════════════════════════════════════════
# TAB: CHARTS
# ═════════════════════════════════════════════════════════════════════════════

def _render_charts(con, summary_df, sel_date):
    if summary_df.empty:
        st.info(f"No intraday data for {sel_date}.")
        return

    symbols = get_symbols_list(con)
    top_active = summary_df.nlargest(20, "total_vol")["symbol"].tolist()

    c1, c2 = st.columns([2, 1])
    with c1:
        sel_sym = st.selectbox(
            "Symbol", top_active + [s for s in symbols if s not in top_active],
            index=0, key="int_chart_sym",
        )
    with c2:
        chart_type = st.radio("Chart", ["Candlestick", "Line"], horizontal=True, key="int_chart_type")

    # Load tick data for this symbol on this date
    ts_start, ts_end = _ts_range(sel_date)
    tick_df = pd.read_sql_query(
        """SELECT ts, ts_epoch, open, high, low, close, volume
           FROM intraday_bars WHERE symbol=? AND ts BETWEEN ? AND ?
           ORDER BY ts_epoch""",
        con,
        params=[sel_sym, ts_start, ts_end],
    )

    if tick_df.empty:
        st.info(f"No ticks for {sel_sym} on {sel_date}.")
        return

    tick_df["ts_dt"] = pd.to_datetime(tick_df["ts"])

    # Calculate indicators
    tick_df["vwap"] = (
        (tick_df["close"] * tick_df["volume"]).cumsum() /
        tick_df["volume"].cumsum().replace(0, np.nan)
    )
    tick_df["sma_20"] = tick_df["close"].rolling(20, min_periods=1).mean()
    tick_df["sma_50"] = tick_df["close"].rolling(50, min_periods=1).mean()
    bb_sma = tick_df["close"].rolling(20, min_periods=5).mean()
    bb_std = tick_df["close"].rolling(20, min_periods=5).std()
    tick_df["bb_upper"] = bb_sma + 2 * bb_std
    tick_df["bb_lower"] = bb_sma - 2 * bb_std

    # Session stats
    sess_open = tick_df["close"].iloc[0]
    sess_close = tick_df["close"].iloc[-1]
    sess_high = tick_df["high"].max()
    sess_low = tick_df["low"].min()
    sess_vol = tick_df["volume"].max()
    sess_change = sess_close - sess_open
    sess_chg_pct = (sess_change / sess_open * 100) if sess_open else 0
    current_vwap = tick_df["vwap"].iloc[-1] if not tick_df["vwap"].isna().all() else None

    # KPI cards
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.markdown(_metric_card("Last", f"{sess_close:.2f}", delta=sess_chg_pct, prefix="Rs."), unsafe_allow_html=True)
    c2.markdown(_metric_card("Open", f"{sess_open:.2f}", prefix="Rs."), unsafe_allow_html=True)
    c3.markdown(_metric_card("High", f"{sess_high:.2f}", prefix="Rs."), unsafe_allow_html=True)
    c4.markdown(_metric_card("Low", f"{sess_low:.2f}", prefix="Rs."), unsafe_allow_html=True)
    c5.markdown(_metric_card("VWAP", f"{current_vwap:.2f}" if current_vwap else "—", prefix="Rs."), unsafe_allow_html=True)
    c6.markdown(_metric_card("Volume", format_volume(sess_vol)), unsafe_allow_html=True)

    # VWAP context
    if current_vwap and sess_close:
        vwap_diff = sess_close - current_vwap
        vwap_pct = (vwap_diff / current_vwap * 100)
        bias = "Bullish" if vwap_diff > 0 else "Bearish"
        bias_color = _COLORS["up"] if vwap_diff > 0 else _COLORS["down"]
        st.markdown(
            f'<div style="text-align:center;padding:4px;color:{bias_color};font-size:12px">'
            f'Price {"above" if vwap_diff > 0 else "below"} VWAP by Rs.{abs(vwap_diff):.2f} '
            f'({vwap_pct:+.2f}%) — {bias} bias</div>',
            unsafe_allow_html=True,
        )

    st.markdown("")

    # Overlays
    show_vwap = st.checkbox("VWAP", value=True, key="int_show_vwap")
    show_bb = st.checkbox("Bollinger Bands", value=False, key="int_show_bb")
    show_sma = st.checkbox("SMA 20/50", value=False, key="int_show_sma")

    # Main chart
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05,
        row_heights=[0.75, 0.25],
        subplot_titles=[f"{sel_sym} — Intraday ({sel_date})", "Volume"],
    )

    if chart_type == "Candlestick":
        # Resample to ~1 min bars for cleaner candles
        tick_df_indexed = tick_df.set_index("ts_dt")
        ohlc = tick_df_indexed["close"].resample("1min").ohlc().dropna()
        vol_1m = tick_df_indexed["volume"].resample("1min").last().fillna(method="ffill").dropna()

        if not ohlc.empty:
            bar_colors = [_COLORS["up"] if c >= o else _COLORS["down"]
                          for o, c in zip(ohlc["open"], ohlc["close"])]
            fig.add_trace(go.Candlestick(
                x=ohlc.index, open=ohlc["open"], high=ohlc["high"],
                low=ohlc["low"], close=ohlc["close"],
                increasing_line_color=_COLORS["up"], decreasing_line_color=_COLORS["down"],
                name="Price",
            ), row=1, col=1)

            fig.add_trace(go.Bar(
                x=vol_1m.index, y=vol_1m.values,
                marker_color=[_COLORS["up"] if i % 2 == 0 else _COLORS["blue"]
                              for i in range(len(vol_1m))],
                opacity=0.5, name="Volume", showlegend=False,
            ), row=2, col=1)
    else:
        fig.add_trace(go.Scatter(
            x=tick_df["ts_dt"], y=tick_df["close"],
            mode="lines", name="Price", line=dict(color=_COLORS["blue"], width=2),
        ), row=1, col=1)

        vol_colors = [_COLORS["up"] if c >= o else _COLORS["down"]
                      for o, c in zip(tick_df["open"], tick_df["close"])]
        fig.add_trace(go.Bar(
            x=tick_df["ts_dt"], y=tick_df["volume"],
            marker_color=vol_colors, opacity=0.5, name="Volume", showlegend=False,
        ), row=2, col=1)

    # Overlays
    if show_vwap:
        fig.add_trace(go.Scatter(
            x=tick_df["ts_dt"], y=tick_df["vwap"],
            mode="lines", name="VWAP",
            line=dict(color=_COLORS["orange"], width=2, dash="dash"),
        ), row=1, col=1)

    if show_sma:
        fig.add_trace(go.Scatter(
            x=tick_df["ts_dt"], y=tick_df["sma_20"],
            mode="lines", name="SMA 20",
            line=dict(color=_COLORS["purple"], width=1),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=tick_df["ts_dt"], y=tick_df["sma_50"],
            mode="lines", name="SMA 50",
            line=dict(color=_COLORS["gold"], width=1),
        ), row=1, col=1)

    if show_bb:
        fig.add_trace(go.Scatter(
            x=tick_df["ts_dt"], y=tick_df["bb_upper"],
            mode="lines", name="BB Upper", line=dict(color=_COLORS["text_dim"], width=1, dash="dot"),
        ), row=1, col=1)
        fig.add_trace(go.Scatter(
            x=tick_df["ts_dt"], y=tick_df["bb_lower"],
            mode="lines", name="BB Lower", line=dict(color=_COLORS["text_dim"], width=1, dash="dot"),
            fill="tonexty", fillcolor="rgba(120,144,156,0.1)",
        ), row=1, col=1)

    _apply_layout(
        fig, height=650, xaxis_rangeslider_visible=False, hovermode="x unified",
        legend=dict(orientation="h", y=1.06, x=0, bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_yaxes(title_text="Price (Rs.)", row=1, col=1, gridcolor=_COLORS["grid"])
    fig.update_yaxes(title_text="Vol", row=2, col=1, gridcolor=_COLORS["grid"])
    st.plotly_chart(fig, use_container_width=True)

    # Tick-level data table
    with st.expander("Tick Data (last 100)", expanded=False):
        st.dataframe(
            tick_df.sort_values("ts_epoch", ascending=False).head(100)[
                ["ts", "open", "high", "low", "close", "volume"]
            ],
            use_container_width=True, hide_index=True,
        )


# ═════════════════════════════════════════════════════════════════════════════
# TAB: MARKET PULSE
# ═════════════════════════════════════════════════════════════════════════════

def _render_market_pulse(con, df, sel_date):
    if df.empty:
        st.info(f"No intraday data for {sel_date}.")
        return

    # Advance / Decline ratio over time (cumulative ticks)
    st.markdown("**Intraday Advance/Decline — Tick Timeline**")
    tick_timeline = pd.read_sql_query(
        """SELECT
             SUBSTR(ts, 1, 16) AS minute,
             COUNT(DISTINCT CASE WHEN close > open THEN symbol END) AS adv,
             COUNT(DISTINCT CASE WHEN close < open THEN symbol END) AS dec,
             COUNT(DISTINCT symbol) AS total
           FROM intraday_bars
           WHERE ts BETWEEN ? AND ?
           GROUP BY SUBSTR(ts, 1, 16)
           ORDER BY minute""",
        con,
        params=[*_ts_range(sel_date)],
    )

    if not tick_timeline.empty:
        tick_timeline["net"] = tick_timeline["adv"] - tick_timeline["dec"]
        tick_timeline["cumulative_net"] = tick_timeline["net"].cumsum()

        fig_ad = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.08,
                               row_heights=[0.6, 0.4])
        fig_ad.add_trace(go.Scatter(
            x=tick_timeline["minute"], y=tick_timeline["adv"],
            mode="lines", name="Advancing", line=dict(color=_COLORS["up"], width=1.5),
            fill="tozeroy", fillcolor="rgba(0,230,118,0.15)",
        ), row=1, col=1)
        fig_ad.add_trace(go.Scatter(
            x=tick_timeline["minute"], y=tick_timeline["dec"],
            mode="lines", name="Declining", line=dict(color=_COLORS["down"], width=1.5),
            fill="tozeroy", fillcolor="rgba(255,82,82,0.15)",
        ), row=1, col=1)
        fig_ad.add_trace(go.Bar(
            x=tick_timeline["minute"], y=tick_timeline["cumulative_net"],
            marker_color=[_COLORS["up"] if v >= 0 else _COLORS["down"]
                          for v in tick_timeline["cumulative_net"]],
            name="Cumulative Net", opacity=0.8,
        ), row=2, col=1)
        _apply_layout(
            fig_ad, height=450,
            legend=dict(orientation="h", y=1.05, x=0, bgcolor="rgba(0,0,0,0)"),
        )
        fig_ad.update_yaxes(title_text="Stocks", row=1, col=1, gridcolor=_COLORS["grid"])
        fig_ad.update_yaxes(title_text="Cum. Net", row=2, col=1, gridcolor=_COLORS["grid"])
        st.plotly_chart(fig_ad, use_container_width=True)

    st.markdown("---")

    # Tick distribution by hour
    st.markdown("**Tick Activity by Hour**")
    hourly = pd.read_sql_query(
        """SELECT
             CAST(SUBSTR(ts, 12, 2) AS INTEGER) AS hour,
             COUNT(*) AS ticks,
             COUNT(DISTINCT symbol) AS symbols,
             SUM(volume) AS volume
           FROM intraday_bars
           WHERE ts BETWEEN ? AND ?
           GROUP BY hour ORDER BY hour""",
        con,
        params=[*_ts_range(sel_date)],
    )

    if not hourly.empty:
        fig_hourly = make_subplots(specs=[[{"secondary_y": True}]])
        fig_hourly.add_trace(go.Bar(
            x=hourly["hour"].apply(lambda h: f"{h:02d}:00"),
            y=hourly["ticks"],
            name="Ticks", marker_color=_COLORS["accent"], opacity=0.8,
        ), secondary_y=False)
        fig_hourly.add_trace(go.Scatter(
            x=hourly["hour"].apply(lambda h: f"{h:02d}:00"),
            y=hourly["symbols"],
            mode="lines+markers", name="Active Symbols",
            line=dict(color=_COLORS["orange"], width=2),
        ), secondary_y=True)
        _apply_layout(
            fig_hourly, height=350,
            legend=dict(orientation="h", y=1.05, x=0, bgcolor="rgba(0,0,0,0)"),
        )
        fig_hourly.update_yaxes(title_text="Ticks", secondary_y=False, gridcolor=_COLORS["grid"])
        fig_hourly.update_yaxes(title_text="Symbols", secondary_y=True, gridcolor=_COLORS["grid"])
        fig_hourly.update_xaxes(gridcolor=_COLORS["grid"])
        st.plotly_chart(fig_hourly, use_container_width=True)

    st.markdown("---")

    # Price change distribution by range bucket
    st.markdown("**Intraday Range Distribution**")
    if not df.empty:
        bins = [-100, -5, -3, -1, 0, 1, 3, 5, 100]
        labels = ["<-5%", "-5 to -3%", "-3 to -1%", "-1 to 0%",
                  "0 to 1%", "1 to 3%", "3 to 5%", ">5%"]
        df["range_bucket"] = pd.cut(df["change_pct"], bins=bins, labels=labels, include_lowest=True)
        bucket_counts = df["range_bucket"].value_counts().reindex(labels).fillna(0)

        bar_colors = [_COLORS["down"]] * 4 + [_COLORS["up"]] * 4
        fig_buckets = go.Figure(go.Bar(
            x=bucket_counts.index, y=bucket_counts.values,
            marker_color=bar_colors, opacity=0.85,
            text=bucket_counts.values.astype(int), textposition="outside",
        ))
        _apply_layout(fig_buckets, height=300, xaxis_title="Change Range", yaxis_title="Stocks")
        st.plotly_chart(fig_buckets, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: VOLUME
# ═════════════════════════════════════════════════════════════════════════════

def _render_volume(con, df, sel_date):
    if df.empty:
        st.info(f"No intraday data for {sel_date}.")
        return

    # Volume leaders
    st.markdown("**Volume Leaders**")
    vol_top = df.nlargest(20, "total_vol")[["symbol", "last_price", "change_pct", "total_vol", "ticks"]].copy()

    fig_vol = go.Figure(go.Bar(
        x=vol_top["total_vol"],
        y=vol_top["symbol"],
        orientation="h",
        marker_color=[_change_color(c) for c in vol_top["change_pct"]],
        text=vol_top.apply(
            lambda r: f"Rs.{r['last_price']:.1f} ({r['change_pct']:+.1f}%)", axis=1
        ),
        textposition="outside",
    ))
    _apply_layout(
        fig_vol, height=max(400, len(vol_top) * 25),
        yaxis=dict(autorange="reversed", gridcolor=_COLORS["grid"]),
        xaxis_title="Volume",
    )
    st.plotly_chart(fig_vol, use_container_width=True)

    st.markdown("---")

    # Volume vs Change scatter
    st.markdown("**Volume vs Change Scatter**")
    scatter_df = df[df["total_vol"] > 0].copy()
    if not scatter_df.empty:
        scatter_df["log_vol"] = np.log10(scatter_df["total_vol"].clip(lower=1))
        fig_scatter = go.Figure(go.Scatter(
            x=scatter_df["change_pct"],
            y=scatter_df["log_vol"],
            mode="markers",
            marker=dict(
                color=scatter_df["change_pct"],
                colorscale=[[0, _COLORS["down"]], [0.5, _COLORS["neutral"]], [1, _COLORS["up"]]],
                cmid=0,
                size=6,
                colorbar=dict(title="Chg%", thickness=12),
            ),
            text=scatter_df["symbol"],
            hovertemplate="<b>%{text}</b><br>Change: %{x:.2f}%<br>Log Vol: %{y:.1f}<extra></extra>",
        ))
        fig_scatter.add_vline(x=0, line_dash="dash", line_color=_COLORS["text_dim"])
        _apply_layout(fig_scatter, height=450, xaxis_title="Change %", yaxis_title="Log10(Volume)")
        st.plotly_chart(fig_scatter, use_container_width=True)

    st.markdown("---")

    # Volume concentration (top N % of total)
    st.markdown("**Volume Concentration**")
    sorted_vol = df.sort_values("total_vol", ascending=False).copy()
    sorted_vol["cum_vol"] = sorted_vol["total_vol"].cumsum()
    total = sorted_vol["total_vol"].sum()
    sorted_vol["cum_pct"] = sorted_vol["cum_vol"] / total * 100

    top10_pct = sorted_vol.head(10)["total_vol"].sum() / total * 100
    top20_pct = sorted_vol.head(20)["total_vol"].sum() / total * 100
    top50_pct = sorted_vol.head(50)["total_vol"].sum() / total * 100

    c1, c2, c3 = st.columns(3)
    c1.markdown(_metric_card("Top 10 Stocks", f"{top10_pct:.1f}%", suffix=" of volume"), unsafe_allow_html=True)
    c2.markdown(_metric_card("Top 20 Stocks", f"{top20_pct:.1f}%", suffix=" of volume"), unsafe_allow_html=True)
    c3.markdown(_metric_card("Top 50 Stocks", f"{top50_pct:.1f}%", suffix=" of volume"), unsafe_allow_html=True)

    fig_conc = go.Figure(go.Scatter(
        x=list(range(1, len(sorted_vol) + 1)),
        y=sorted_vol["cum_pct"],
        mode="lines", fill="tozeroy",
        line=dict(color=_COLORS["accent"], width=2),
        fillcolor="rgba(0,212,170,0.15)",
    ))
    fig_conc.add_hline(y=80, line_dash="dash", line_color=_COLORS["text_dim"],
                       annotation_text="80%", annotation_position="right")
    _apply_layout(fig_conc, height=300, xaxis_title="Number of Stocks (ranked)", yaxis_title="Cumulative Volume %")
    st.plotly_chart(fig_conc, use_container_width=True)

    st.markdown("---")

    # Sector volume breakdown
    st.markdown("**Sector Volume Breakdown**")
    sector_vol = df[df["sector_code"] != ""].copy()
    if not sector_vol.empty:
        sector_vol["sector_name"] = sector_vol["sector_code"].map(
            lambda c: _SECTOR_LABELS.get(c, c)
        )
        sv_agg = sector_vol.groupby("sector_name").agg(
            vol=("total_vol", "sum"), count=("symbol", "count"),
        ).reset_index().sort_values("vol", ascending=False).head(15)

        fig_sv = go.Figure(go.Bar(
            x=sv_agg["sector_name"], y=sv_agg["vol"],
            marker_color=_COLORS["blue"], opacity=0.85,
            text=sv_agg["count"].apply(lambda c: f"{c} stocks"),
            textposition="outside",
        ))
        _apply_layout(fig_sv, height=350, xaxis_tickangle=-45)
        st.plotly_chart(fig_sv, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB: MOVERS
# ═════════════════════════════════════════════════════════════════════════════

def _render_movers(con, df, sel_date):
    if df.empty:
        st.info(f"No intraday data for {sel_date}.")
        return

    n_show = st.slider("Top N", 10, 50, 20, key="int_movers_n")

    col_g, col_l = st.columns(2)

    with col_g:
        st.markdown("**Top Gainers**")
        gainers = df.nlargest(n_show, "change_pct")
        fig_g = go.Figure(go.Bar(
            y=gainers["symbol"], x=gainers["change_pct"],
            orientation="h", marker_color=_COLORS["up"],
            text=gainers.apply(lambda r: f"Rs.{r['last_price']:.1f} ({r['change_pct']:+.1f}%)", axis=1),
            textposition="outside",
        ))
        _apply_layout(
            fig_g, height=max(400, n_show * 22),
            yaxis=dict(autorange="reversed", gridcolor=_COLORS["grid"]),
            xaxis_title="Change %",
        )
        st.plotly_chart(fig_g, use_container_width=True)

    with col_l:
        st.markdown("**Top Losers**")
        losers = df.nsmallest(n_show, "change_pct")
        fig_l = go.Figure(go.Bar(
            y=losers["symbol"], x=losers["change_pct"],
            orientation="h", marker_color=_COLORS["down"],
            text=losers.apply(lambda r: f"Rs.{r['last_price']:.1f} ({r['change_pct']:+.1f}%)", axis=1),
            textposition="outside",
        ))
        _apply_layout(
            fig_l, height=max(400, n_show * 22),
            yaxis=dict(autorange="reversed", gridcolor=_COLORS["grid"]),
            xaxis_title="Change %",
        )
        st.plotly_chart(fig_l, use_container_width=True)

    st.markdown("---")

    # Most active by ticks
    st.markdown("**Most Active (by Tick Count)**")
    active = df.nlargest(n_show, "ticks")[["symbol", "ticks", "last_price", "change_pct", "total_vol"]]

    fig_active = go.Figure(go.Bar(
        y=active["symbol"], x=active["ticks"],
        orientation="h",
        marker_color=[_change_color(c) for c in active["change_pct"]],
        text=active.apply(lambda r: f"{r['ticks']:,} ticks ({r['change_pct']:+.1f}%)", axis=1),
        textposition="outside",
    ))
    _apply_layout(
        fig_active, height=max(400, n_show * 22),
        yaxis=dict(autorange="reversed", gridcolor=_COLORS["grid"]),
        xaxis_title="Tick Count",
    )
    st.plotly_chart(fig_active, use_container_width=True)

    st.markdown("---")

    # Widest intraday range
    st.markdown("**Widest Intraday Range**")
    wide = df.nlargest(n_show, "range_pct")[["symbol", "open", "low", "high", "last_price", "range_pct", "change_pct"]]

    fig_range = go.Figure()
    for _, r in wide.iterrows():
        color = _change_color(r["change_pct"])
        fig_range.add_trace(go.Scatter(
            x=[r["low"], r["high"]],
            y=[r["symbol"], r["symbol"]],
            mode="lines+markers",
            line=dict(color=color, width=4),
            marker=dict(size=8, color=color),
            name=r["symbol"],
            showlegend=False,
            hovertemplate=(
                f"<b>{r['symbol']}</b><br>"
                f"Low: Rs.{r['low']:.2f}<br>High: Rs.{r['high']:.2f}<br>"
                f"Range: {r['range_pct']:.2f}%<extra></extra>"
            ),
        ))
        # Mark last price
        fig_range.add_trace(go.Scatter(
            x=[r["last_price"]], y=[r["symbol"]],
            mode="markers", marker=dict(size=10, color="white", symbol="diamond"),
            showlegend=False,
        ))

    _apply_layout(
        fig_range, height=max(400, n_show * 25),
        yaxis=dict(autorange="reversed", gridcolor=_COLORS["grid"]),
        xaxis_title="Price (Rs.)",
        title=dict(text="Low-High Range (diamond = last price)", font=dict(size=12)),
    )
    st.plotly_chart(fig_range, use_container_width=True)

    st.markdown("---")

    # Full data table
    st.markdown("**Full Session Data**")
    table_cols = ["symbol", "company_name", "sector_code", "open", "high", "low",
                   "last_price", "change", "change_pct", "total_vol", "pc_volume", "turnover", "ticks"]
    table_cols = [c for c in table_cols if c in df.columns]
    table_df = df[table_cols].copy()
    table_df = table_df.rename(columns={
        "company_name": "Name", "sector_code": "Sector",
        "last_price": "Close", "change_pct": "Chg%",
        "total_vol": "Tick Vol", "pc_volume": "PSX Vol", "turnover": "Turnover",
    })
    table_df["Sector"] = table_df["Sector"].map(lambda c: _SECTOR_LABELS.get(c, c))

    st.dataframe(
        table_df.sort_values("PSX Vol" if "PSX Vol" in table_df.columns else "Tick Vol", ascending=False),
        use_container_width=True, hide_index=True, height=500,
        column_config={
            "open": st.column_config.NumberColumn("Open", format="%.2f"),
            "high": st.column_config.NumberColumn("High", format="%.2f"),
            "low": st.column_config.NumberColumn("Low", format="%.2f"),
            "Close": st.column_config.NumberColumn("Close", format="%.2f"),
            "change": st.column_config.NumberColumn("Chg", format="%.2f"),
            "Chg%": st.column_config.NumberColumn("Chg%", format="%.2f"),
            "Tick Vol": st.column_config.NumberColumn("Tick Vol", format="%,.0f"),
            "PSX Vol": st.column_config.NumberColumn("PSX Vol", format="%,.0f"),
            "Turnover": st.column_config.NumberColumn("Turnover", format="%,.0f"),
            "ticks": st.column_config.NumberColumn("Ticks", format="%,d"),
        },
    )

    # CSV download
    st.download_button(
        "Download CSV",
        table_df.to_csv(index=False),
        f"psx_intraday_{sel_date}.csv",
        "text/csv",
    )


# ═════════════════════════════════════════════════════════════════════════════
# TAB: SYNC (preserved from original)
# ═════════════════════════════════════════════════════════════════════════════

def _render_sync(con, sel_date):
    today_str = date.today().isoformat()

    # ── Bulk sync ──
    st.markdown("### Bulk Intraday Sync — All Symbols")
    running = is_intraday_sync_running()
    progress = read_intraday_sync_progress()

    if running and progress:
        pct = progress["current"] / max(progress["total"], 1)
        st.progress(pct, text=f"{progress['current']}/{progress['total']} — {progress.get('current_symbol', '')}")
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("OK", progress["ok"])
        c2.metric("Failed", progress["failed"])
        c3.metric("Ticks", f"{progress['rows_total']:,}")
        c4.metric("JSON Saved", progress.get("json_saved", 0))
        if st.button("Refresh", key="int_bulk_refresh"):
            st.rerun()
    else:
        if progress and progress.get("status") == "completed":
            json_info = ""
            if progress.get("json_saved", 0) > 0:
                json_info = f", {progress['json_saved']} JSON files"
            trade_date = progress.get("trading_date", "")
            date_info = f" (trading date: {trade_date})" if trade_date else ""
            st.success(
                f"Last run: {progress['ok']}/{progress['total']} symbols, "
                f"{progress['rows_total']:,} ticks{json_info}{date_info} — {progress.get('finished_at', '')[:19]}"
            )
            if progress.get("json_dir"):
                st.caption(f"JSON files: {progress['json_dir']}")

        bulk_col1, bulk_col2, bulk_col3 = st.columns(3)
        with bulk_col1:
            save_json = st.checkbox(
                "Save JSON files", value=False, key="int_bulk_save_json",
                help="Save raw PSX responses to /mnt/e/psxdata/intraday/{date}/{SYMBOL}.json",
            )
            if st.button(
                "Fetch PSX Ticks -> intraday_bars", key="int_bulk_btn", type="primary",
                help="Fetches today's tick-level trades for all ~620 symbols.",
            ):
                started = start_intraday_sync(save_json=save_json)
                if started:
                    st.success("Fetching PSX ticks for all symbols")
                else:
                    st.warning("Already running.")
                st.rerun()

        with bulk_col2:
            if st.button(
                f"intraday_bars -> JSON Disk ({today_str})", key="int_bulk_export_btn",
                help="Exports DB tick data to per-symbol JSON files on disk.",
            ):
                try:
                    import json as _json
                    from collections import defaultdict
                    from pakfindata.config import DATA_ROOT

                    _ts_s, _ts_e = _ts_range(today_str)
                    rows = con.execute(
                        "SELECT symbol, ts_epoch, close, volume "
                        "FROM intraday_bars WHERE ts BETWEEN ? AND ? ORDER BY symbol, ts_epoch",
                        (_ts_s, _ts_e),
                    ).fetchall()
                    by_sym = defaultdict(list)
                    for r in rows:
                        by_sym[r["symbol"]].append([r["ts_epoch"], r["close"], r["volume"]])

                    _dir = DATA_ROOT / "intraday" / today_str
                    _dir.mkdir(parents=True, exist_ok=True)
                    for sym, data in by_sym.items():
                        (_dir / f"{sym}.json").write_text(_json.dumps(data, indent=2))

                    st.success(f"Exported {len(by_sym)} symbols ({len(rows):,} ticks) -> {_dir}")
                except Exception as e:
                    st.error(f"Export failed: {e}")

        with bulk_col3:
            if st.button(
                f"intraday_bars -> eod_ohlcv ({today_str})", key="int_bulk_promote_btn",
                help="Aggregates intraday_bars into eod_ohlcv for today.",
            ):
                try:
                    from pakfindata.db.repositories.intraday import promote_intraday_to_eod
                    eod_count = promote_intraday_to_eod(con, today_str)
                    st.success(f"Promoted {eod_count} symbols to eod_ohlcv for {today_str}")
                except Exception as e:
                    st.error(f"Promote failed: {e}")

    st.markdown("---")

    # ── Single symbol sync ──
    st.markdown("### Single Symbol Sync")
    symbols = get_symbols_list(con)
    if not symbols:
        st.warning("No symbols found.")
        return

    # Initialize session state
    if "intraday_sync_result" not in st.session_state:
        st.session_state.intraday_sync_result = None
    if "intraday_sync_running" not in st.session_state:
        st.session_state.intraday_sync_running = False

    c1, c2 = st.columns([2, 1])
    with c1:
        sym_input = st.text_input(
            "Symbol", value="OGDC", key="int_sync_sym",
        ).strip().upper()
    with c2:
        sym_list = st.selectbox("Or select", [""] + symbols, index=0, key="int_sync_sel")

    sel_sym = sym_list if sym_list else sym_input
    if not sel_sym:
        return

    btn_col1, btn_col2, btn_col3 = st.columns(3)
    with btn_col1:
        fetch_btn = st.button(
            f"PSX API -> Disk ({sel_sym})", type="primary", key="int_sync_fetch",
            disabled=st.session_state.intraday_sync_running,
        )
    with btn_col2:
        load_btn = st.button(
            f"Disk -> intraday_bars ({sel_sym})", key="int_sync_load",
            disabled=st.session_state.intraday_sync_running,
        )
    with btn_col3:
        promote_btn = st.button(
            f"intraday -> eod ({today_str})", key="int_sync_promote",
            disabled=st.session_state.intraday_sync_running,
        )

    # Action handlers
    if fetch_btn and not st.session_state.intraday_sync_running:
        st.session_state.intraday_sync_result = None
        st.session_state.intraday_sync_running = True
        with st.status(f"Fetching {sel_sym}...", expanded=True) as status:
            try:
                from pakfindata.sources.intraday import fetch_intraday_json, parse_intraday_payload
                from pakfindata.http import create_session as create_http_session
                import json

                session = create_http_session()
                payload = fetch_intraday_json(sel_sym, session)
                df_fetched = parse_intraday_payload(sel_sym, payload)

                if df_fetched.empty:
                    st.session_state.intraday_sync_result = {"action": "download", "success": True, "rows": 0}
                    status.update(label="No data", state="complete")
                else:
                    from pakfindata.config import DATA_ROOT
                    json_dir = DATA_ROOT / "intraday" / today_str
                    json_dir.mkdir(parents=True, exist_ok=True)
                    json_path = json_dir / f"{sel_sym}.json"
                    json_path.write_text(json.dumps(payload, indent=2))

                    INTRADAY_TEMP_DIR.mkdir(parents=True, exist_ok=True)
                    csv_path = INTRADAY_TEMP_DIR / f"{sel_sym}.csv"
                    df_fetched[["symbol", "ts", "open", "high", "low", "close", "volume"]].to_csv(csv_path, index=False)

                    st.session_state.intraday_sync_result = {
                        "action": "download", "success": True,
                        "rows": len(df_fetched), "json_path": str(json_path),
                    }
                    status.update(label=f"Downloaded {len(df_fetched)} ticks", state="complete")
            except Exception as e:
                st.session_state.intraday_sync_result = {"action": "download", "success": False, "error": str(e)}
                status.update(label="Failed!", state="error")
            finally:
                st.session_state.intraday_sync_running = False

    if load_btn and not st.session_state.intraday_sync_running:
        st.session_state.intraday_sync_result = None
        st.session_state.intraday_sync_running = True
        with st.status(f"Loading {sel_sym}...", expanded=True) as status:
            try:
                import json as _json
                from pakfindata.config import DATA_ROOT
                from pakfindata.sources.intraday import parse_intraday_payload
                from pakfindata.db.repositories.intraday import upsert_intraday

                json_path = DATA_ROOT / "intraday" / today_str / f"{sel_sym}.json"
                if not json_path.exists():
                    intraday_dir = DATA_ROOT / "intraday"
                    found = None
                    if intraday_dir.exists():
                        for d in sorted(intraday_dir.iterdir(), reverse=True):
                            candidate = d / f"{sel_sym}.json"
                            if candidate.exists():
                                found = candidate
                                break
                    if found:
                        json_path = found
                    else:
                        raise FileNotFoundError(f"No JSON for {sel_sym}. Download first.")

                payload = _json.loads(json_path.read_text())
                df_load = parse_intraday_payload(sel_sym, payload)
                if df_load.empty:
                    st.session_state.intraday_sync_result = {"action": "load", "success": True, "db_rows": 0}
                    status.update(label="No rows", state="complete")
                else:
                    db_rows = upsert_intraday(con, df_load)
                    st.session_state.intraday_sync_result = {
                        "action": "load", "success": True, "db_rows": db_rows,
                    }
                    status.update(label=f"Loaded {db_rows} rows", state="complete")
            except Exception as e:
                st.session_state.intraday_sync_result = {"action": "load", "success": False, "error": str(e)}
                status.update(label="Failed!", state="error")
            finally:
                st.session_state.intraday_sync_running = False

    if promote_btn and not st.session_state.intraday_sync_running:
        st.session_state.intraday_sync_result = None
        st.session_state.intraday_sync_running = True
        with st.status("Promoting...", expanded=True) as status:
            try:
                from pakfindata.db.repositories.intraday import promote_intraday_to_eod
                eod_count = promote_intraday_to_eod(con, today_str)
                st.session_state.intraday_sync_result = {
                    "action": "promote", "success": True, "eod_promoted": eod_count,
                }
                status.update(label=f"Promoted {eod_count} symbols", state="complete")
            except Exception as e:
                st.session_state.intraday_sync_result = {"action": "promote", "success": False, "error": str(e)}
                status.update(label="Failed!", state="error")
            finally:
                st.session_state.intraday_sync_running = False

    # Display result
    result = st.session_state.intraday_sync_result
    if result:
        if result.get("success"):
            action = result.get("action", "")
            if action == "download":
                rows = result.get("rows", 0)
                if rows > 0:
                    st.success(f"Downloaded {rows} ticks")
                    if result.get("json_path"):
                        st.caption(f"JSON: {result['json_path']}")
                else:
                    st.info("No data returned")
            elif action == "load":
                st.success(f"Loaded {result.get('db_rows', 0)} rows into intraday_bars")
            elif action == "promote":
                st.success(f"Promoted {result.get('eod_promoted', 0)} symbols to eod_ohlcv")
        else:
            st.error(f"Error: {result.get('error')}")
