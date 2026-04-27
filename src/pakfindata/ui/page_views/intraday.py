"""Intraday Trading Terminal — PSX tick-level analytics.

Tabs:
  Dashboard — Market KPIs, top movers, sector heatmap, breadth gauge
  Charts — Single-symbol candlestick, VWAP, volume profile, Bollinger bands
  Market Pulse — Advance/decline, tick distribution, intraday momentum
  Volume — Volume leaders, unusual activity, block trades, concentration
  Movers — Gainers, losers, most active with visual cards and scatter
  Index — IDX market ticks from JSONL tick_logs (KSE-100, KSE-30, KMI-30 etc.)
  Dedup — Scan & deduplicate JSONL tick_logs files
  Sync — All bulk/single sync controls preserved
"""

import json as _json
import sqlite3
from datetime import date, datetime, timedelta
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


def _ensure_intraday_indexes(con):
    """Create performance indexes on intraday_bars (idempotent, runs once)."""
    if getattr(_ensure_intraday_indexes, "_done", False):
        return
    try:
        con.execute(
            "CREATE INDEX IF NOT EXISTS idx_intraday_bars_ts_sym "
            "ON intraday_bars(ts, symbol, ts_epoch, close, volume)"
        )
        _ensure_intraday_indexes._done = True
    except Exception:
        pass


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


def _last_trading_day() -> date:
    """Return the most recent PSX trading day (Mon-Fri).

    If today is a weekday and market hours haven't started, return previous
    trading day.  On weekends, walk back to Friday.
    """
    from pakfindata.ui.components.helpers import MARKET_DAYS, MARKET_OPEN_HOUR

    d = date.today()
    now_hour = datetime.now().hour
    # Before market opens on a weekday -> previous trading day
    if d.weekday() in MARKET_DAYS and now_hour < MARKET_OPEN_HOUR:
        d -= timedelta(days=1)
    # Walk back over weekends
    while d.weekday() not in MARKET_DAYS:
        d -= timedelta(days=1)
    return d


@st.cache_data(ttl=120, show_spinner="Loading intraday data...")
def _load_today_summary(_con, date_str: str) -> pd.DataFrame:
    """Load aggregated intraday summary for a given date from intraday_daily_summary.

    Reads pre-aggregated rows written by intraday_summary.compute_all(). Returns
    empty DataFrame if the summary hasn't been built for this date yet.
    """
    df = pd.read_sql_query(
        """
        SELECT symbol,
               tick_count  AS ticks,
               day_open    AS open,
               day_high    AS high,
               day_low     AS low,
               day_close   AS last_price,
               day_volume  AS total_vol
        FROM intraday_daily_summary
        WHERE date = ? AND market = 'REG'
        ORDER BY day_volume DESC
        """,
        _con, params=[date_str],
    )
    return df


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

_SECTOR_LABELS_RAW = {
    "0801": "AUTOMOBILE ASSEMBLER", "0802": "AUTOMOBILE PARTS & ACCESSORIES",
    "0803": "CABLE & ELECTRICAL GOODS", "0804": "CEMENT", "0805": "CHEMICAL",
    "0806": "CLOSE - END MUTUAL FUND", "0807": "COMMERCIAL BANKS",
    "0808": "ENGINEERING", "0809": "FERTILIZER",
    "0810": "FOOD & PERSONAL CARE PRODUCTS", "0811": "GLASS & CERAMICS",
    "0812": "INV. BANKS / INV. COS. / SECURITIES COS.", "0813": "INSURANCE",
    "0814": "JUTE", "0815": "LEATHER & TANNERIES", "0816": "LEASING COMPANIES",
    "0817": "MISCELLANEOUS", "0818": "MODARABAS", "0819": "OIL & GAS EXPLORATION COMPANIES",
    "0820": "OIL & GAS MARKETING COMPANIES", "0821": "PAPER & BOARD",
    "0822": "PHARMACEUTICALS", "0823": "POWER GENERATION & DISTRIBUTION",
    "0824": "REFINERY", "0825": "SUGAR & ALLIED INDUSTRIES",
    "0826": "SYNTHETIC & RAYON", "0827": "TECHNOLOGY & COMMUNICATION",
    "0828": "TEXTILE COMPOSITE", "0829": "TEXTILE SPINNING",
    "0830": "TEXTILE WEAVING", "0831": "TOBACCO", "0832": "TRANSPORT",
    "0833": "VANASPATI & ALLIED INDUSTRIES", "0834": "WOOLLEN",
    "0835": "REAL ESTATE INVESTMENT TRUST", "0836": "EXCHANGE TRADED FUNDS",
    "0837": "PROPERTY",
}
# Build lookup that works with both "0807" and "807" formats
_SECTOR_LABELS = {}
for code, name in _SECTOR_LABELS_RAW.items():
    _SECTOR_LABELS[code] = name
    _SECTOR_LABELS[code.lstrip("0") or "0"] = name


def _get_intraday_dates_cached(_con) -> list[str]:
    """Dates that have a pre-aggregated summary row. Fast indexed read."""
    try:
        from pakfindata.db.repositories import intraday_summary as _isum
        _isum.ensure_tables(_con)
        return _isum.get_summary_dates(_con)
    except Exception:
        return []


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

    # Connection
    con = get_connection()
    _ensure_intraday_indexes(con)

    # Date selector — manifest only, defaults to latest
    avail_dates = _get_intraday_dates_cached(con)
    if not avail_dates:
        st.warning("No intraday data. Use Sync tab to fetch.")
        render_footer()
        return

    dc1, dc2 = st.columns([4, 1])
    with dc1:
        sel_date = st.selectbox("Trading Date", avail_dates, index=0, key="int_date_sel")
    with dc2:
        from pakfindata.ui.components.helpers import render_date_refresh_button
        render_date_refresh_button(["intraday_bars"], key="int_refresh_dates")

    # Tabs
    tab_dash, tab_charts, tab_pulse, tab_vol, tab_movers, tab_idx, tab_dedup, tab_sync = st.tabs(
        ["Dashboard", "Charts", "Market Pulse", "Volume", "Movers", "Index", "Dedup", "Sync"]
    )

    # Lazy-load summary data: only compute once per run, skip if only Sync needed
    _summary_cache = {}

    def _get_summary():
        if "df" not in _summary_cache:
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
            _summary_cache["df"] = summary_df
        return _summary_cache["df"]

    # ═════════════════════════════════════════════════════════════════════
    # TAB 1: DASHBOARD
    # ═════════════════════════════════════════════════════════════════════
    with tab_dash:
        try:
            _render_dashboard(con, _get_summary(), sel_date)
        except Exception as e:
            st.error(f"Dashboard error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 2: CHARTS
    # ═════════════════════════════════════════════════════════════════════
    with tab_charts:
        try:
            _render_charts(con, _get_summary(), sel_date)
        except Exception as e:
            st.error(f"Charts error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 3: MARKET PULSE
    # ═════════════════════════════════════════════════════════════════════
    with tab_pulse:
        try:
            _render_market_pulse(con, _get_summary(), sel_date)
        except Exception as e:
            st.error(f"Market Pulse error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 4: VOLUME
    # ═════════════════════════════════════════════════════════════════════
    with tab_vol:
        try:
            _render_volume(con, _get_summary(), sel_date)
        except Exception as e:
            st.error(f"Volume error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 5: MOVERS
    # ═════════════════════════════════════════════════════════════════════
    with tab_movers:
        try:
            _render_movers(con, _get_summary(), sel_date)
        except Exception as e:
            st.error(f"Movers error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 6: INDEX
    # ═════════════════════════════════════════════════════════════════════
    with tab_idx:
        try:
            _render_index_ticks(sel_date)
        except Exception as e:
            st.error(f"Index error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 7: DEDUP
    # ═════════════════════════════════════════════════════════════════════
    with tab_dedup:
        try:
            _render_dedup()
        except Exception as e:
            st.error(f"Dedup error: {e}")

    # ═════════════════════════════════════════════════════════════════════
    # TAB 8: SYNC
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
        st.plotly_chart(fig_breadth, width='stretch')

        # Change distribution histogram
        st.markdown("**Return Distribution**")
        fig_hist = go.Figure()
        fig_hist.add_trace(go.Histogram(
            x=df["change_pct"], nbinsx=50,
            marker_color=_COLORS["accent"], opacity=0.8,
        ))
        fig_hist.add_vline(x=0, line_dash="dash", line_color=_COLORS["text_dim"])
        _apply_layout(fig_hist, height=250, xaxis_title="Change %", yaxis_title="Count")
        st.plotly_chart(fig_hist, width='stretch')

    with col_r:
        # Top gainers and losers quick view
        st.markdown("**Top 10 Gainers**")
        top_gain = df.nlargest(10, "change_pct")[["symbol", "last_price", "change_pct", "total_vol"]]
        st.dataframe(
            top_gain.style.map(
                lambda v: f"color: {_COLORS['up']}" if isinstance(v, (int, float)) and v > 0
                else f"color: {_COLORS['down']}" if isinstance(v, (int, float)) and v < 0
                else "",
                subset=["change_pct"],
            ),
            width='stretch', hide_index=True, height=200,
        )

        st.markdown("**Top 10 Losers**")
        top_lose = df.nsmallest(10, "change_pct")[["symbol", "last_price", "change_pct", "total_vol"]]
        st.dataframe(
            top_lose.style.map(
                lambda v: f"color: {_COLORS['up']}" if isinstance(v, (int, float)) and v > 0
                else f"color: {_COLORS['down']}" if isinstance(v, (int, float)) and v < 0
                else "",
                subset=["change_pct"],
            ),
            width='stretch', hide_index=True, height=200,
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
            st.plotly_chart(fig_sector, width='stretch')
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
    tick_df = pd.read_sql_query(
        """SELECT ts, ts_epoch, open, high, low, close, volume
           FROM intraday_bars WHERE symbol=? AND date=?
           ORDER BY ts_epoch""",
        con,
        params=[sel_sym, sel_date],
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
        vol_1m = tick_df_indexed["volume"].resample("1min").last().ffill().dropna()

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
    st.plotly_chart(fig, width='stretch')

    # Tick-level data table
    with st.expander("Tick Data (last 100)", expanded=False):
        st.dataframe(
            tick_df.sort_values("ts_epoch", ascending=False).head(100)[
                ["ts", "open", "high", "low", "close", "volume"]
            ],
            width='stretch', hide_index=True,
        )


# ═════════════════════════════════════════════════════════════════════════════
# TAB: MARKET PULSE
# ═════════════════════════════════════════════════════════════════════════════

def _render_market_pulse(con, df, sel_date):
    if df.empty:
        st.info(f"No intraday data for {sel_date}.")
        return

    # ── Tick-to-Tick Delta A/D (LAG window function approach) ──
    # For each tick, compare price to previous tick for the same symbol.
    # Uptick (+1): price > prev_price, Downtick (-1): price < prev_price, Flat (0).
    # Per minute per symbol: net direction = sum of tick signs.
    # Advancing = symbols with net positive ticks, Declining = net negative.
    st.markdown("**Intraday Advance/Decline — Tick Timeline**")
    tick_timeline = pd.read_sql_query(
        """SELECT REPLACE(minute, 'T', ' ') AS minute,
                  advancing  AS adv,
                  declining  AS dec,
                  total_symbols AS total,
                  net_ticks
           FROM intraday_minute_breadth
           WHERE date = ? AND market = 'REG'
           ORDER BY minute""",
        con,
        params=[sel_date],
    )

    # ── Load KSE-100 intraday per-minute values from intraday_index_minute ──
    kse_df = pd.DataFrame()
    try:
        from pakfindata.db.repositories import intraday_summary as _isum
        kse_all = _isum.get_index_minute(
            con, sel_date, ["KSE100", "KSE-100", "KMIALL"]
        )
        if not kse_all.empty:
            # Collapse across symbol aliases: take one value per minute
            kse_df = (
                kse_all.groupby("minute", as_index=False)
                .agg(value=("value", "last"))
            )
    except Exception:
        pass  # KSE-100 overlay is optional — fail silently

    if not tick_timeline.empty:
        tick_timeline["net"] = tick_timeline["adv"] - tick_timeline["dec"]
        tick_timeline["cumulative_ad"] = tick_timeline["net"].cumsum()
        tick_timeline["cumulative_ticks"] = tick_timeline["net_ticks"].cumsum()

        has_kse = not kse_df.empty

        # ════════════════════════════════════════════════════════════════
        # PANE 1: THE PULSE — Advancing vs Declining + KSE-100 Overlay
        # ════════════════════════════════════════════════════════════════
        fig_pulse = make_subplots(specs=[[{"secondary_y": True}]])
        fig_pulse.add_trace(go.Scatter(
            x=tick_timeline["minute"], y=tick_timeline["adv"],
            mode="lines", name="Advancing",
            line=dict(color=_COLORS["up"], width=2),
            fill="tozeroy", fillcolor="rgba(0,230,118,0.12)",
        ), secondary_y=False)
        fig_pulse.add_trace(go.Scatter(
            x=tick_timeline["minute"], y=tick_timeline["dec"],
            mode="lines", name="Declining",
            line=dict(color=_COLORS["down"], width=2),
            fill="tozeroy", fillcolor="rgba(255,82,82,0.12)",
        ), secondary_y=False)
        # KSE-100 overlay on secondary y-axis
        if has_kse:
            fig_pulse.add_trace(go.Scatter(
                x=kse_df["minute"], y=kse_df["value"],
                mode="lines", name="KSE-100",
                line=dict(color=_COLORS["gold"], width=1.5),
                opacity=0.85,
            ), secondary_y=True)
        _apply_layout(
            fig_pulse, height=320,
            legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)"),
            title=dict(
                text="<b>THE PULSE</b>  <span style='font-size:11px;color:#888'>Advancing vs Declining Stocks"
                     + (" + KSE-100" if has_kse else "") + "</span>",
                font=dict(size=13),
            ),
            hovermode="x unified",
        )
        fig_pulse.update_yaxes(
            title_text="Stocks", secondary_y=False,
            gridcolor=_COLORS["grid"], showgrid=True,
        )
        if has_kse:
            fig_pulse.update_yaxes(
                title_text="KSE-100", secondary_y=True,
                gridcolor="rgba(0,0,0,0)", showgrid=False,
                tickfont=dict(color=_COLORS["gold"]),
                title_font=dict(color=_COLORS["gold"]),
            )
        st.plotly_chart(fig_pulse, width='stretch')

        # ════════════════════════════════════════════════════════════════
        # PANE 2: THE TREND — Cumulative A/D as conditional fill area
        # ════════════════════════════════════════════════════════════════
        cum_ad = tick_timeline["cumulative_ad"]
        # Split into positive and negative segments for conditional fill
        pos_y = cum_ad.where(cum_ad >= 0, 0)
        neg_y = cum_ad.where(cum_ad <= 0, 0)

        fig_trend = go.Figure()
        # Positive fill (green)
        fig_trend.add_trace(go.Scatter(
            x=tick_timeline["minute"], y=pos_y,
            mode="lines", name="Bullish",
            line=dict(color=_COLORS["up"], width=0),
            fill="tozeroy", fillcolor="rgba(0,230,118,0.25)",
            showlegend=True,
        ))
        # Negative fill (red)
        fig_trend.add_trace(go.Scatter(
            x=tick_timeline["minute"], y=neg_y,
            mode="lines", name="Bearish",
            line=dict(color=_COLORS["down"], width=0),
            fill="tozeroy", fillcolor="rgba(255,82,82,0.25)",
            showlegend=True,
        ))
        # The actual line on top
        fig_trend.add_trace(go.Scatter(
            x=tick_timeline["minute"], y=cum_ad,
            mode="lines", name="Cumulative A/D",
            line=dict(color="white", width=2),
        ))
        # Zero baseline
        fig_trend.add_hline(y=0, line_dash="solid", line_color=_COLORS["text_dim"], line_width=1)
        _apply_layout(
            fig_trend, height=260,
            legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)"),
            title=dict(
                text="<b>THE TREND</b>  <span style='font-size:11px;color:#888'>Cumulative Advance/Decline Line</span>",
                font=dict(size=13),
            ),
            hovermode="x unified",
        )
        fig_trend.update_yaxes(title_text="Cum. A/D", gridcolor=_COLORS["grid"])
        st.plotly_chart(fig_trend, width='stretch')

        # ════════════════════════════════════════════════════════════════
        # PANE 3: THE OSCILLATOR — Net Tick Momentum histogram
        # ════════════════════════════════════════════════════════════════
        net_ticks = tick_timeline["net_ticks"]
        fig_osc = go.Figure()
        fig_osc.add_trace(go.Bar(
            x=tick_timeline["minute"], y=net_ticks,
            marker_color=[_COLORS["up"] if v >= 0 else _COLORS["down"] for v in net_ticks],
            name="Net Tick Momentum", opacity=0.9,
        ))
        # Zero line
        fig_osc.add_hline(y=0, line_color=_COLORS["text_dim"], line_width=1)
        # Overbought / Oversold threshold lines
        abs_max = max(abs(net_ticks.max()), abs(net_ticks.min()), 1)
        # Dynamic thresholds at ~60% of max range
        threshold = int(abs_max * 0.6)
        if threshold > 5:
            fig_osc.add_hline(
                y=threshold, line_dash="dash", line_color="#B8860B", line_width=1,
                annotation_text=f"OB +{threshold}", annotation_position="right",
                annotation_font=dict(color="#B8860B", size=10),
            )
            fig_osc.add_hline(
                y=-threshold, line_dash="dash", line_color="#B8860B", line_width=1,
                annotation_text=f"OS -{threshold}", annotation_position="right",
                annotation_font=dict(color="#B8860B", size=10),
            )
        _apply_layout(
            fig_osc, height=240,
            legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)"),
            title=dict(
                text="<b>THE OSCILLATOR</b>  <span style='font-size:11px;color:#888'>Net Tick Momentum per Minute</span>",
                font=dict(size=13),
            ),
            hovermode="x unified",
        )
        fig_osc.update_yaxes(title_text="Net Ticks", gridcolor=_COLORS["grid"])
        st.plotly_chart(fig_osc, width='stretch')

        # ── Summary KPIs for the breadth panel ──
        last_ad = cum_ad.iloc[-1]
        last_mom = tick_timeline["cumulative_ticks"].iloc[-1]
        peak_adv = tick_timeline["adv"].max()
        peak_dec = tick_timeline["dec"].max()
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(_metric_card("Cum. A/D", f"{last_ad:+.0f}", delta=last_ad), unsafe_allow_html=True)
        c2.markdown(_metric_card("Cum. Momentum", f"{last_mom:+,.0f}", delta=last_mom), unsafe_allow_html=True)
        c3.markdown(_metric_card("Peak Advancers", f"{peak_adv:.0f}"), unsafe_allow_html=True)
        c4.markdown(_metric_card("Peak Decliners", f"{peak_dec:.0f}"), unsafe_allow_html=True)

    st.markdown("---")

    # Tick distribution by hour
    st.markdown("**Tick Activity by Hour**")
    hourly = pd.read_sql_query(
        """SELECT hour,
                  tick_count   AS ticks,
                  symbol_count AS symbols
           FROM intraday_hourly_summary
           WHERE date = ? AND market = 'REG'
           ORDER BY hour""",
        con,
        params=[sel_date],
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
        st.plotly_chart(fig_hourly, width='stretch')

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
        st.plotly_chart(fig_buckets, width='stretch')

    st.markdown("---")

    # ── Breadth Persistence & History ──────────────────────────────────
    st.markdown("**Breadth History — Persisted Daily Snapshots**")

    from pakfindata.db.repositories.breadth import (
        compute_and_persist_breadth,
        get_breadth_daily_summary,
        get_breadth_dates,
        get_breadth_for_date,
    )

    bcol1, bcol2 = st.columns([1, 3])
    with bcol1:
        if st.button(
            f"Save Breadth ({sel_date})", key="mp_save_breadth", type="primary",
            help="Compute tick-to-tick A/D from intraday_bars and persist minute-level breadth.",
        ):
            with st.spinner("Computing breadth..."):
                n = compute_and_persist_breadth(con, sel_date)
            if n > 0:
                st.success(f"Saved {n} minute rows for {sel_date}")
            else:
                st.warning("No intraday data to compute breadth.")
    with bcol2:
        persisted_dates = get_breadth_dates(con)
        if persisted_dates:
            st.caption(f"{len(persisted_dates)} dates persisted: {persisted_dates[0]} → {persisted_dates[-1]}")
        else:
            st.caption("No breadth data persisted yet.")

    # Daily summary table
    daily_df = get_breadth_daily_summary(con, limit=60)
    if not daily_df.empty:
        # Add derived columns for display
        daily_df["A/D Ratio"] = (daily_df["adv"] / daily_df["dec"].replace(0, 1)).round(2)
        daily_df["Net A/D"] = daily_df["adv"] - daily_df["dec"]
        daily_df["Breadth %"] = ((daily_df["adv"] - daily_df["dec"]) / daily_df["total"].replace(0, 1) * 100).round(1)

        st.dataframe(
            daily_df.rename(columns={
                "date": "Date", "adv": "Adv", "dec": "Dec", "total": "Symbols",
                "net_ticks": "Net Ticks", "cum_ad": "Cum A/D", "cum_ticks": "Cum Ticks",
                "ingested_at": "Saved At",
            }),
            width='stretch', hide_index=True, height=400,
            column_config={
                "Date": st.column_config.TextColumn("Date", width="small"),
                "Adv": st.column_config.NumberColumn("Adv", format="%d"),
                "Dec": st.column_config.NumberColumn("Dec", format="%d"),
                "Symbols": st.column_config.NumberColumn("Symbols", format="%d"),
                "Net A/D": st.column_config.NumberColumn("Net A/D", format="%+d"),
                "A/D Ratio": st.column_config.NumberColumn("A/D Ratio", format="%.2f"),
                "Breadth %": st.column_config.NumberColumn("Breadth %", format="%+.1f"),
                "Net Ticks": st.column_config.NumberColumn("Net Ticks", format="%+,d"),
                "Cum A/D": st.column_config.NumberColumn("Cum A/D", format="%+d"),
                "Cum Ticks": st.column_config.NumberColumn("Cum Ticks", format="%+,d"),
                "Saved At": st.column_config.TextColumn("Saved At", width="small"),
            },
        )

        # Multi-day cumulative A/D chart
        if len(daily_df) > 1:
            st.markdown("**Multi-Day Cumulative A/D**")
            hist = daily_df.sort_values("date")
            hist["net_ad"] = hist["adv"] - hist["dec"]
            hist["running_ad"] = hist["net_ad"].cumsum()

            fig_hist_ad = go.Figure()
            pos_run = hist["running_ad"].where(hist["running_ad"] >= 0, 0)
            neg_run = hist["running_ad"].where(hist["running_ad"] <= 0, 0)
            fig_hist_ad.add_trace(go.Scatter(
                x=hist["date"], y=pos_run,
                mode="lines", line=dict(color=_COLORS["up"], width=0),
                fill="tozeroy", fillcolor="rgba(0,230,118,0.2)",
                name="Bullish", showlegend=True,
            ))
            fig_hist_ad.add_trace(go.Scatter(
                x=hist["date"], y=neg_run,
                mode="lines", line=dict(color=_COLORS["down"], width=0),
                fill="tozeroy", fillcolor="rgba(255,82,82,0.2)",
                name="Bearish", showlegend=True,
            ))
            fig_hist_ad.add_trace(go.Scatter(
                x=hist["date"], y=hist["running_ad"],
                mode="lines+markers", name="Running A/D",
                line=dict(color="white", width=2),
                marker=dict(size=5),
            ))
            fig_hist_ad.add_hline(y=0, line_color=_COLORS["text_dim"], line_width=1)
            _apply_layout(
                fig_hist_ad, height=300,
                legend=dict(orientation="h", y=1.08, x=0, bgcolor="rgba(0,0,0,0)"),
                title=dict(
                    text="<b>MULTI-DAY BREADTH</b>  <span style='font-size:11px;color:#888'>Running Cumulative A/D</span>",
                    font=dict(size=13),
                ),
                hovermode="x unified",
            )
            fig_hist_ad.update_yaxes(title_text="Running A/D", gridcolor=_COLORS["grid"])
            st.plotly_chart(fig_hist_ad, width='stretch')

    # Drill-down: view persisted minute data for a specific date
    if persisted_dates:
        with st.expander("Drill-down: Minute-level Breadth Data", expanded=False):
            drill_date = st.selectbox("Select date", persisted_dates, key="mp_drill_date")
            drill_df = get_breadth_for_date(con, drill_date)
            if not drill_df.empty:
                drill_df["time"] = drill_df["minute"].str[11:]  # extract HH:MM
                drill_df["net"] = drill_df["adv"] - drill_df["dec"]
                st.dataframe(
                    drill_df[["time", "adv", "dec", "total", "net", "net_ticks", "cum_ad", "cum_ticks"]].rename(
                        columns={
                            "time": "Time", "adv": "Adv", "dec": "Dec", "total": "Total",
                            "net": "Net A/D", "net_ticks": "Net Ticks",
                            "cum_ad": "Cum A/D", "cum_ticks": "Cum Ticks",
                        }
                    ),
                    width='stretch', hide_index=True, height=400,
                )

                st.download_button(
                    "Download Minute CSV",
                    drill_df.to_csv(index=False),
                    f"breadth_{drill_date}.csv",
                    "text/csv",
                    key="mp_drill_csv",
                )


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
    st.plotly_chart(fig_vol, width='stretch')

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
        st.plotly_chart(fig_scatter, width='stretch')

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
    st.plotly_chart(fig_conc, width='stretch')

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
        st.plotly_chart(fig_sv, width='stretch')


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
        st.plotly_chart(fig_g, width='stretch')

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
        st.plotly_chart(fig_l, width='stretch')

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
    st.plotly_chart(fig_active, width='stretch')

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
    st.plotly_chart(fig_range, width='stretch')

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
        width='stretch', hide_index=True, height=500,
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
# TAB: INDEX — IDX market ticks from JSONL
# ═════════════════════════════════════════════════════════════════════════════

_TICK_LOGS_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")


def _list_jsonl_files():
    """Return sorted list of JSONL files in tick_logs (newest first)."""
    if not _TICK_LOGS_DIR.exists():
        return []
    return sorted(
        [f for f in _TICK_LOGS_DIR.glob("ticks_*.jsonl") if "deduped" not in f.name],
        reverse=True,
    )


@st.cache_data(ttl=300, show_spinner=False)
def _load_idx_from_duckdb(date_str: str) -> pd.DataFrame:
    """Load index data from DuckDB index_ohlcv_5s (fast, pre-aggregated 5s bars)."""
    try:
        from pakfindata.db.connections import duck
        df = duck(
            "SELECT symbol, ts, o AS open, h AS high, l AS low, c AS price, "
            "v AS volume, turnover "
            "FROM index_ohlcv_5s WHERE ts LIKE ? ORDER BY symbol, ts",
            [f"{date_str}%"],
        )
        if not df.empty:
            df["_ts"] = pd.to_datetime(df["ts"])
            # Compute change from first bar per symbol
            first = df.groupby("symbol")["price"].first()
            df["change"] = df.apply(lambda r: r["price"] - first[r["symbol"]], axis=1)
            df["changePercent"] = df.apply(
                lambda r: (r["change"] / first[r["symbol"]] * 100) if first[r["symbol"]] else 0, axis=1
            )
            return df
    except Exception:
        pass
    return pd.DataFrame()


def _load_idx_ticks(jsonl_path: Path) -> pd.DataFrame:
    """Fallback: Load only IDX market rows from a JSONL file."""
    rows = []
    with open(jsonl_path) as fh:
        for line in fh:
            row = _json.loads(line)
            if row.get("market") == "IDX":
                rows.append(row)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["_ts"] = pd.to_datetime(df["_ts"])
    return df


def _render_index_ticks(sel_date: str):
    st.markdown("### Index Ticks (IDX Market)")
    st.caption("Index data from DuckDB index_ohlcv_5s (5-second bars) — KSE-100, KSE-30, KMI-30, etc.")

    # DuckDB primary — fast, pre-aggregated
    df = _load_idx_from_duckdb(sel_date)

    # Fallback: JSONL files
    if df.empty:
        files = _list_jsonl_files()
        if not files:
            st.warning("No index data in DuckDB or JSONL files.")
            return

        date_file_map = {f.stem.replace("ticks_", ""): f for f in files}
        default_idx = 0
        if sel_date in date_file_map:
            default_idx = list(date_file_map.keys()).index(sel_date)

        sel_file = st.selectbox(
            "Tick log file (JSONL fallback)",
            files,
            index=default_idx,
            format_func=lambda f: f"{f.name}  ({f.stat().st_size / 1024 / 1024:.1f} MB)",
            key="idx_file_sel",
        )

        with st.spinner("Loading IDX ticks from JSONL..."):
            df = _load_idx_ticks(sel_file)

    if df.empty:
        st.info("No IDX ticks in this file.")
        return

    indices = sorted(df["symbol"].unique())
    st.markdown(f"**{len(df):,}** IDX ticks across **{len(indices)}** indices")

    # ── KPIs row ──
    latest = df.sort_values("_ts").groupby("symbol").last().reset_index()
    kpi_cols = st.columns(min(len(indices), 6))
    for i, idx_sym in enumerate(indices[:6]):
        row = latest[latest["symbol"] == idx_sym]
        if row.empty:
            continue
        r = row.iloc[0]
        chg_pct = r["changePercent"] * 100 if abs(r["changePercent"]) < 1 else r["changePercent"]
        color = "green" if r["change"] >= 0 else "red"
        with kpi_cols[i % len(kpi_cols)]:
            st.metric(
                idx_sym,
                f"{r['price']:,.2f}",
                delta=f"{r['change']:+,.2f} ({chg_pct:+.2f}%)",
            )

    # ── Index selector for charts ──
    sel_indices = st.multiselect(
        "Select indices to chart", indices,
        default=indices[:3],
        key="idx_chart_sel",
    )

    if not sel_indices:
        return

    df_sel = df[df["symbol"].isin(sel_indices)].copy()

    # ── Price chart ──
    fig_price = go.Figure()
    for sym in sel_indices:
        d = df_sel[df_sel["symbol"] == sym].sort_values("_ts")
        fig_price.add_trace(go.Scatter(
            x=d["_ts"], y=d["price"], mode="lines", name=sym,
            line=dict(width=1.5),
            hovertemplate="%{x|%H:%M:%S}<br>%{y:,.2f}<extra>%{fullData.name}</extra>",
        ))
    fig_price.update_layout(
        title="Index Price (Intraday)",
        height=420,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EAECEF", size=11),
        xaxis=dict(gridcolor="#1e2430"), yaxis=dict(gridcolor="#1e2430"),
        margin=dict(l=10, r=10, t=40, b=10),
        hovermode="x unified",
        legend=dict(orientation="h", y=1.12),
    )
    st.plotly_chart(fig_price, width='stretch')

    # ── Change % chart ──
    fig_chg = go.Figure()
    for sym in sel_indices:
        d = df_sel[df_sel["symbol"] == sym].sort_values("_ts")
        chg_pct = d["changePercent"] * 100 if d["changePercent"].abs().max() < 1 else d["changePercent"]
        fig_chg.add_trace(go.Scatter(
            x=d["_ts"], y=chg_pct, mode="lines", name=sym,
            line=dict(width=1.5),
        ))
    fig_chg.update_layout(
        title="Index Change %",
        height=320,
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EAECEF", size=11),
        xaxis=dict(gridcolor="#1e2430"), yaxis=dict(gridcolor="#1e2430", ticksuffix="%"),
        margin=dict(l=10, r=10, t=40, b=10),
        hovermode="x unified",
        legend=dict(orientation="h", y=1.12),
    )
    st.plotly_chart(fig_chg, width='stretch')

    # ── High/Low range bar ──
    st.markdown("#### Intraday Range")
    range_cols = ["symbol", "low", "high", "price"]
    if "previousClose" in latest.columns:
        range_cols.append("previousClose")
    range_data = latest[latest["symbol"].isin(sel_indices)][range_cols].copy()
    if "previousClose" not in range_data.columns:
        range_data["previousClose"] = range_data["price"] - latest.loc[range_data.index, "change"] if "change" in latest.columns else range_data["price"]
    range_data["range_pct"] = ((range_data["high"] - range_data["low"]) / range_data["low"].replace(0, 1) * 100).round(2)
    range_data = range_data.sort_values("range_pct", ascending=True)

    fig_range = go.Figure()
    for _, r in range_data.iterrows():
        color = "#00E676" if r["price"] >= r["previousClose"] else "#FF5252"
        fig_range.add_trace(go.Bar(
            y=[r["symbol"]], x=[r["range_pct"]], orientation="h",
            marker_color=color, name=r["symbol"],
            text=f"{r['low']:,.2f} — {r['high']:,.2f} ({r['range_pct']:.2f}%)",
            textposition="auto", showlegend=False,
        ))
    fig_range.update_layout(
        height=max(200, len(range_data) * 40),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#EAECEF", size=11),
        xaxis=dict(gridcolor="#1e2430", title="Range %"),
        yaxis=dict(gridcolor="#1e2430"),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    st.plotly_chart(fig_range, width='stretch')

    # ── Raw data table ──
    with st.expander("Raw IDX ticks"):
        show_cols = ["symbol", "price", "change", "changePercent", "high", "low",
                     "previousClose", "volume", "value", "trades", "_ts"]
        show_cols = [c for c in show_cols if c in df_sel.columns]
        st.dataframe(
            df_sel[show_cols].sort_values("_ts", ascending=False),
            width='stretch', hide_index=True, height=400,
        )


# ═════════════════════════════════════════════════════════════════════════════
# TAB: DEDUP — Scan & deduplicate JSONL tick_logs
# ═════════════════════════════════════════════════════════════════════════════

def _scan_file_dupes(path: Path) -> dict:
    """Scan a JSONL file and return dupe stats without writing."""
    seen = set()
    total = 0
    dupes = 0
    with open(path) as fh:
        for line in fh:
            total += 1
            row = _json.loads(line)
            key = (row.get("symbol"), row.get("timestamp"), row.get("price"), row.get("volume"))
            if key in seen:
                dupes += 1
            seen.add(key)
    return {"total": total, "unique": total - dupes, "dupes": dupes, "size_mb": path.stat().st_size / 1024 / 1024}


def _dedup_file(src: Path) -> dict:
    """Deduplicate a JSONL file → _deduped.jsonl in same folder. Returns stats."""
    seen = set()
    total = 0
    kept = 0
    dst = src.with_name(src.stem + "_deduped.jsonl")

    with open(src) as fin, open(dst, "w") as fout:
        for line in fin:
            total += 1
            row = _json.loads(line)
            key = (row.get("symbol"), row.get("timestamp"), row.get("price"), row.get("volume"))
            if key not in seen:
                seen.add(key)
                fout.write(line)
                kept += 1

    return {
        "total": total, "kept": kept, "removed": total - kept,
        "src_mb": src.stat().st_size / 1024 / 1024,
        "dst_mb": dst.stat().st_size / 1024 / 1024,
        "dst": dst,
    }


def _render_dedup():
    st.markdown("### Tick Log Deduplicator")
    st.caption(
        "Scans JSONL files in `/mnt/e/psxdata/tick_logs/` for duplicate rows "
        "(same symbol + exchange timestamp + price + volume). "
        "Creates a `_deduped.jsonl` file — originals are never modified."
    )

    files = _list_jsonl_files()
    if not files:
        st.warning("No JSONL files found.")
        return

    # File selector
    sel_files = st.multiselect(
        "Select files to process",
        files,
        default=files[:1],
        format_func=lambda f: f"{f.name}  ({f.stat().st_size / 1024 / 1024:.1f} MB)",
        key="dedup_file_sel",
    )

    if not sel_files:
        return

    c1, c2 = st.columns(2)
    scan_btn = c1.button("Scan for duplicates", key="dedup_scan_btn")
    dedup_btn = c2.button("Deduplicate selected", key="dedup_run_btn", type="primary")

    # ── Scan ──
    if scan_btn:
        results = []
        progress = st.progress(0, text="Scanning...")
        for i, f in enumerate(sel_files):
            progress.progress((i + 1) / len(sel_files), text=f"Scanning {f.name}...")
            stats = _scan_file_dupes(f)
            stats["file"] = f.name
            # Check if deduped version exists
            deduped = f.with_name(f.stem + "_deduped.jsonl")
            stats["has_deduped"] = deduped.exists()
            results.append(stats)
        progress.empty()

        rdf = pd.DataFrame(results)
        rdf["dupe_%"] = (rdf["dupes"] / rdf["total"] * 100).round(1)
        rdf["saved_mb"] = (rdf["size_mb"] * rdf["dupes"] / rdf["total"]).round(1)
        rdf = rdf[["file", "total", "unique", "dupes", "dupe_%", "size_mb", "saved_mb", "has_deduped"]]
        rdf.columns = ["File", "Total Rows", "Unique", "Duplicates", "Dupe %", "Size (MB)", "Saveable (MB)", "Deduped Exists"]

        # Color-code dupe % for visibility
        st.dataframe(
            rdf,
            width='stretch', hide_index=True,
            column_config={
                "Dupe %": st.column_config.ProgressColumn("Dupe %", min_value=0, max_value=100, format="%.1f%%"),
                "Size (MB)": st.column_config.NumberColumn("Size (MB)", format="%.1f"),
                "Saveable (MB)": st.column_config.NumberColumn("Saveable (MB)", format="%.1f"),
            },
        )

        total_dupes = rdf["Duplicates"].sum()
        total_save = rdf["Saveable (MB)"].sum()
        if total_dupes > 0:
            st.warning(f"**{total_dupes:,.0f}** duplicate rows found across {len(sel_files)} file(s). Potential savings: **{total_save:.1f} MB**")
        else:
            st.success("All files are clean — no duplicates found.")

    # ── Dedup ──
    if dedup_btn:
        results = []
        progress = st.progress(0, text="Deduplicating...")
        for i, f in enumerate(sel_files):
            progress.progress((i + 1) / len(sel_files), text=f"Deduplicating {f.name}...")
            stats = _dedup_file(f)
            stats["file"] = f.name
            results.append(stats)
        progress.empty()

        for r in results:
            if r["removed"] > 0:
                st.success(
                    f"**{r['file']}**: {r['total']:,} → {r['kept']:,} "
                    f"(removed {r['removed']:,} dupes) | "
                    f"{r['src_mb']:.1f} MB → {r['dst_mb']:.1f} MB | "
                    f"Saved to `{r['dst'].name}`"
                )
            else:
                st.info(f"**{r['file']}**: Clean — no duplicates. Deduped copy still written.")


# ═════════════════════════════════════════════════════════════════════════════
# TAB: SYNC (preserved from original)
# ═════════════════════════════════════════════════════════════════════════════

def _render_sync(con, sel_date):
    today_str = date.today().isoformat()
    last_td = _last_trading_day()
    last_td_str = last_td.isoformat()

    # Show which date the PSX API serves
    st.info(
        f"**PSX API serves last trading day's data:** "
        f"**{last_td_str}** ({last_td.strftime('%A')})"
        + (f"  \nToday is {today_str} ({date.today().strftime('%A')})"
           if today_str != last_td_str else "")
    )

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
        # Auto-refresh every 3s while sync is running (no st.rerun needed)
        if HAS_AUTOREFRESH and st_autorefresh:
            st_autorefresh(interval=3000, limit=None, key="int_bulk_sync_autorefresh")
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

        # ── Step 1: Fetch All → Disk (parallel sharded) ──
        st.markdown("#### Step 1: Fetch All Ticks → Disk")
        st.caption(
            "Fetch tick data from PSX API for all symbols using **3 parallel shards** "
            "(10 workers each) and save JSON files to disk. "
            "SUSPENDED/DELISTED (>30 days) skipped."
        )
        fetch_disk_date = st.date_input(
            "Date (output folder)",
            value=last_td,
            max_value=date.today(),
            key="int_fetch_disk_date",
        )
        if st.button(
            f"Fetch All Ticks → Disk ({fetch_disk_date.isoformat()})",
            key="int_fetch_disk_btn",
            type="primary",
        ):
            from concurrent.futures import ThreadPoolExecutor, as_completed
            from pakfindata.sync_timeseries import fetch_ticks_to_disk_parallel

            with st.spinner(f"Fetching ticks for {fetch_disk_date.isoformat()} (3 shards)..."):
                try:
                    result = fetch_ticks_to_disk_parallel(
                        con, fetch_disk_date.isoformat(), n_shards=3,
                    )
                    st.success(
                        f"Done! **{result['ok']}** symbols, "
                        f"**{result['ticks']:,}** ticks saved. "
                        f"{result['fail']} failed, {result['skipped']} skipped. "
                        f"→ `{result['json_dir']}`"
                    )
                except Exception as e:
                    st.error(f"Error: {e}")

        st.markdown("---")

        # ── Step 2: Load from Disk → SQLite ──
        st.markdown("#### Step 2: Disk → SQLite")
        _intraday_root = Path("/mnt/e/psxdata/intraday")
        _disk_dates = sorted(
            [d.name for d in _intraday_root.iterdir() if d.is_dir() and d.name[:4].isdigit()],
            reverse=True,
        ) if _intraday_root.is_dir() else []
        load_disk_date = st.selectbox(
            "Date (disk folder)", _disk_dates,
            index=_disk_dates.index(last_td_str) if last_td_str in _disk_dates else 0,
            key="int_load_disk_date",
        )
        bulk_col1, bulk_col2, bulk_col3 = st.columns(3)
        with bulk_col1:
            if st.button(
                f"Load Ticks from Disk ({load_disk_date})", key="int_bulk_btn", type="primary",
                help=f"Reads JSON files from /mnt/e/psxdata/intraday/{load_disk_date}/ and loads into intraday_bars + tick_data.",
            ):
                from pakfindata.sync_timeseries import load_ticks_from_disk
                with st.spinner(f"Loading ticks from disk for {load_disk_date}..."):
                    try:
                        result = load_ticks_from_disk(con, load_disk_date)
                        if result.get("error"):
                            st.error(result["error"])
                        else:
                            st.success(
                                f"Loaded {result['ok']}/{result['total_files']} files, "
                                f"{result['rows_total']:,} rows → intraday_bars + tick_data"
                            )
                            # Update manifest once after bulk load
                            try:
                                from pakfindata.db.date_manifest import add_date
                                add_date("intraday_bars", load_disk_date)
                            except Exception:
                                pass
                    except Exception as e:
                        st.error(f"Error: {e}")

                # Chained step: build summary tables from JSONL
                try:
                    from pakfindata.db.repositories import intraday_summary as _isum
                    if _isum.source_available(load_disk_date) == "jsonl":
                        with st.spinner(f"Building summaries for {load_disk_date}..."):
                            sres = _isum.compute_all(con, load_disk_date)
                        if "error" in sres:
                            st.warning(f"Summary build skipped: {sres['error']}")
                        else:
                            t = sres["timings"]
                            st.success(
                                f"Summaries built in {t['total_s']}s — "
                                f"daily: {sres['daily']}, minute: {sres['minute_breadth']}, hourly: {sres['hourly']}"
                            )
                    else:
                        st.info(
                            f"No JSONL for {load_disk_date} — run tick log sync to enable summary build."
                        )
                except Exception as e:
                    st.warning(f"Summary build failed: {e}")

        with bulk_col2:
            if st.button(
                f"intraday_bars -> JSON Disk ({last_td_str})", key="int_bulk_export_btn",
                help="Exports DB tick data to per-symbol JSON files on disk.",
            ):
                try:
                    import json as _json
                    from collections import defaultdict
                    from pakfindata.config import DATA_ROOT

                    rows = con.execute(
                        "SELECT symbol, ts_epoch, close, volume "
                        "FROM intraday_bars WHERE date = ? ORDER BY symbol, ts_epoch",
                        (last_td_str,),
                    ).fetchall()
                    by_sym = defaultdict(list)
                    for r in rows:
                        by_sym[r["symbol"]].append([r["ts_epoch"], r["close"], r["volume"]])

                    _dir = DATA_ROOT / "intraday" / last_td_str
                    _dir.mkdir(parents=True, exist_ok=True)
                    for sym, data in by_sym.items():
                        (_dir / f"{sym}.json").write_text(_json.dumps(data, indent=2))

                    st.success(f"Exported {len(by_sym)} symbols ({len(rows):,} ticks) -> {_dir}")
                except Exception as e:
                    st.error(f"Export failed: {e}")

        with bulk_col3:
            if st.button(
                f"intraday_bars -> eod_ohlcv ({last_td_str})", key="int_bulk_promote_btn",
                help=f"Aggregates intraday_bars into eod_ohlcv for {last_td_str}.",
            ):
                try:
                    from pakfindata.db.repositories.intraday import promote_intraday_to_eod
                    eod_count = promote_intraday_to_eod(con, last_td_str)
                    st.success(f"Promoted {eod_count} symbols to eod_ohlcv for {last_td_str}")
                except Exception as e:
                    st.error(f"Promote failed: {e}")

        # Row 2: Parquet export
        st.markdown("#### Parquet Export")
        from pathlib import Path as _Path
        _intraday_root = _Path("/mnt/e/psxdata/intraday")
        _avail_dates = sorted(
            [d.name for d in _intraday_root.iterdir() if d.is_dir() and d.name[:4].isdigit()],
            reverse=True,
        ) if _intraday_root.is_dir() else []
        _duck_date = st.selectbox(
            "Date (JSON folder)", _avail_dates,
            index=_avail_dates.index(last_td_str) if last_td_str in _avail_dates else 0,
            key="int_duck_date_select",
        )
        if st.button(
            f"Export intraday_bars → Parquet ({_duck_date})", key="int_duck_fetch_btn",
            type="primary",
            help=f"Exports intraday_bars for {_duck_date} from SQLite to Parquet.",
        ):
            try:
                from pakfindata.db.parquet_store import export_table
                with st.spinner(f"Exporting intraday_bars for {_duck_date}..."):
                    rows = export_table("intraday_bars", _duck_date)
                st.success(f"Exported intraday_bars: {rows:,} rows → Parquet/{_duck_date}.parquet")
            except Exception as e:
                import traceback
                st.error(f"Parquet export failed: {e}")
                st.code(traceback.format_exc(), language="python")

        # ── Build Summary Tables from JSONL ──
        st.markdown("#### Build Intraday Summary Tables")
        st.caption(
            "Aggregates `tick_logs_cloud/ticks_{date}.jsonl` into three summary tables "
            "(`intraday_daily_summary`, `intraday_minute_breadth`, `intraday_hourly_summary`). "
            "Parser runs via in-memory DuckDB — no persistent DuckDB file, no locks. "
            "Output is written to SQLite."
        )

        try:
            from pakfindata.db.repositories import intraday_summary as _isum
            _isum.ensure_tables(con)
            _stats = _isum.get_stats(con)
        except Exception as _e:
            _stats = {"total_rows": 0, "dates": 0, "first_date": None, "last_date": None}
            st.caption(f"(summary tables not yet created: {_e})")

        _sc1, _sc2, _sc3 = st.columns(3)
        _sc1.metric("Rows", f"{_stats['total_rows']:,}")
        _sc2.metric("Dates", _stats["dates"])
        _sc3.metric("Last", _stats["last_date"] or "—")

        _jl_root = Path("/mnt/e/psxdata/tick_logs_cloud")
        _jl_dates = sorted(
            [p.stem.replace("ticks_", "") for p in _jl_root.glob("ticks_*.jsonl")],
            reverse=True,
        ) if _jl_root.is_dir() else []
        _sum_date = st.selectbox(
            "Date (JSONL source)", _jl_dates,
            index=_jl_dates.index(last_td_str) if last_td_str in _jl_dates else 0,
            key="int_sum_date_select",
        ) if _jl_dates else None

        _sumb1, _sumb2 = st.columns(2)
        with _sumb1:
            if _sum_date and st.button(
                f"Build Summaries for {_sum_date}",
                key="int_sum_build_btn", type="primary",
                help="Runs daily / minute breadth / hourly aggregations for this date.",
            ):
                from pakfindata.db.repositories import intraday_summary as _isum
                with st.spinner(f"Aggregating {_sum_date}..."):
                    try:
                        result = _isum.compute_all(con, _sum_date)
                        if "error" in result:
                            st.error(result["error"])
                        else:
                            t = result["timings"]
                            st.success(
                                f"Done in {t['total_s']}s — "
                                f"daily: {result['daily']} rows ({t['daily_s']}s), "
                                f"minute: {result['minute_breadth']} rows ({t['minute_breadth_s']}s), "
                                f"hourly: {result['hourly']} rows ({t['hourly_s']}s)"
                            )
                    except Exception as e:
                        import traceback
                        st.error(f"Build failed: {e}")
                        st.code(traceback.format_exc(), language="python")

        with _sumb2:
            if st.button(
                "Build Missing (all JSONL dates)",
                key="int_sum_build_missing_btn",
                help="Runs aggregations for every JSONL date not yet in the summary table.",
            ):
                from pakfindata.db.repositories import intraday_summary as _isum
                _existing = set(_isum.get_summary_dates(con))
                _missing = [d for d in _jl_dates if d not in _existing]
                if not _missing:
                    st.info("No missing dates — all JSONL dates already summarized.")
                else:
                    _prog = st.progress(0, text=f"0/{len(_missing)}")
                    _ok = 0
                    for _i, _d in enumerate(_missing):
                        try:
                            _isum.compute_all(con, _d)
                            _ok += 1
                        except Exception as _e:
                            st.warning(f"{_d}: {_e}")
                        _prog.progress((_i + 1) / len(_missing), text=f"{_i+1}/{len(_missing)} ({_d})")
                    st.success(f"Built summaries for {_ok}/{len(_missing)} dates.")

    st.markdown("---")

    # ── Data Coverage / Gap Detector ──
    st.markdown("### Data Coverage — Gap Detector")
    if st.button("Scan All Dates", key="int_gap_scan"):
        with st.spinner("Scanning all data stores..."):
            from pathlib import Path as _P
            from pakfindata.db.date_manifest import get_dates

            # Use manifest for instant lookups
            _eod = set(d for d in get_dates("eod_ohlcv") if d >= "2026-02-01")
            _sq_dates = set(d for d in get_dates("intraday_bars") if d >= "2026-02-01")
            _tb = set(get_dates("ohlcv_5s"))
            _jl = set(get_dates("tick_jsonl"))
            _duck_dates = _sq_dates  # same source

            # Disk JSON dates (filesystem scan — fast)
            _disk_root = _P("/mnt/e/psxdata/intraday")
            _disk = set(
                p.name for p in _disk_root.iterdir()
                if p.is_dir() and p.name[:4].isdigit() and p.name >= "2026-02-01"
            ) if _disk_root.is_dir() else set()

            _all = sorted(_eod | _disk | _sq_dates | _tb | _jl | _duck_dates)
            rows_data = []
            for dt in _all:
                if dt < "2026-02-01":
                    continue
                in_eod = dt in _eod
                in_disk = dt in _disk
                in_sq = dt in _sq_dates
                in_tb = dt in _tb
                in_jl = dt in _jl
                in_duck = dt in _duck_dates

                disk_count = ""
                if in_disk:
                    disk_count = str(len(list((_disk_root / dt).glob("*.json"))))

                status = "OK" if (in_disk and in_sq and in_duck) else "GAP"
                rows_data.append({
                    "Date": dt,
                    "EOD": "Y" if in_eod else "-",
                    "Disk JSON": disk_count if in_disk else "MISS",
                    "SQLite": "Y" if in_sq else "MISS",
                    "DuckDB": "Y" if in_duck else "MISS",
                    "TickBar": "Y" if in_tb else "-",
                    "JSONL": "Y" if in_jl else "-",
                    "Status": status,
                })

            import pandas as _gap_pd
            gap_df = _gap_pd.DataFrame(rows_data)

            gaps = gap_df[gap_df["Status"] == "GAP"]
            if gaps.empty:
                st.success(f"No gaps found across {len(gap_df)} trading dates!")
            else:
                st.warning(f"{len(gaps)} dates have gaps out of {len(gap_df)} total")

            # Color the status column
            def _highlight_gaps(row):
                if row["Status"] == "GAP":
                    return ["background-color: #3a1c1c"] * len(row)
                return [""] * len(row)

            st.dataframe(
                gap_df.style.apply(_highlight_gaps, axis=1),
                width='stretch',
                height=min(len(gap_df) * 35 + 40, 600),
            )

            # Backfill suggestions
            fillable = []
            for _, r in gap_df.iterrows():
                if r["Disk JSON"] == "MISS" and r["JSONL"] == "Y":
                    fillable.append((r["Date"], "JSONL → Disk JSON"))
                elif r["Disk JSON"] == "MISS" and r["SQLite"] == "Y":
                    fillable.append((r["Date"], "SQLite → Disk JSON"))
                if r["SQLite"] == "MISS" and r["Disk JSON"] not in ("MISS", ""):
                    fillable.append((r["Date"], "Disk JSON → SQLite"))
                if r["DuckDB"] == "MISS" and r["Disk JSON"] not in ("MISS", ""):
                    fillable.append((r["Date"], "Disk JSON → DuckDB"))

            if fillable:
                st.markdown("**Auto-fixable gaps:**")
                for dt, action in fillable:
                    st.caption(f"  {dt}: {action}")

                if st.button("Fix All Gaps", key="int_fix_gaps", type="primary"):
                    import json as _json2
                    from collections import defaultdict as _dd
                    fixed = 0
                    for dt, action in fillable:
                        try:
                            if action == "JSONL → Disk JSON":
                                # Find JSONL file
                                jl_file = None
                                for _d2 in [_P("/mnt/e/psxdata/tick_logs_cloud"), _P("/mnt/e/psxdata/tick_logs_cloud")]:
                                    candidate = _d2 / f"ticks_{dt}.jsonl"
                                    if candidate.exists():
                                        jl_file = candidate
                                        break
                                if jl_file:
                                    out_dir = _disk_root / dt
                                    out_dir.mkdir(parents=True, exist_ok=True)
                                    syms = _dd(list)
                                    with open(jl_file) as _fh:
                                        for _line in _fh:
                                            _row = _json2.loads(_line)
                                            if _row.get("market") != "REG":
                                                continue
                                            syms[_row["symbol"]].append([_row["timestamp"], _row["price"], _row.get("volume", 0)])
                                    for sym, trades in syms.items():
                                        seen = set()
                                        unique = []
                                        for t in trades:
                                            k = (t[0], t[1], t[2])
                                            if k not in seen:
                                                seen.add(k)
                                                unique.append(t)
                                        unique.sort(key=lambda x: x[0], reverse=True)
                                        (out_dir / f"{sym}.json").write_text(
                                            _json2.dumps({"status": 1, "message": "", "data": unique}, indent=2)
                                        )
                                    st.toast(f"Fixed {dt}: {len(syms)} symbols from JSONL")
                                    fixed += 1

                            elif action == "SQLite → Disk JSON":
                                out_dir = _disk_root / dt
                                out_dir.mkdir(parents=True, exist_ok=True)
                                rows = con.execute(
                                    "SELECT symbol, ts_epoch, close, volume FROM intraday_bars "
                                    "WHERE ts BETWEEN ? AND ? ORDER BY symbol, ts_epoch",
                                    (f"{dt} 00:00:00", f"{dt} 23:59:59"),
                                ).fetchall()
                                by_sym = _dd(list)
                                for r in rows:
                                    by_sym[r["symbol"]].append([r["ts_epoch"], r["close"], r["volume"]])
                                for sym, data in by_sym.items():
                                    data.sort(key=lambda x: x[0], reverse=True)
                                    (out_dir / f"{sym}.json").write_text(
                                        _json2.dumps({"status": 1, "message": "", "data": data}, indent=2)
                                    )
                                st.toast(f"Fixed {dt}: {len(by_sym)} symbols from SQLite")
                                fixed += 1

                            elif action == "Disk JSON → SQLite":
                                json_dir = _disk_root / dt
                                batch = []
                                for jf in json_dir.glob("*.json"):
                                    sym = jf.stem
                                    payload = _json2.loads(jf.read_text())
                                    data = payload.get("data", payload) if isinstance(payload, dict) else payload
                                    if not isinstance(data, list):
                                        continue
                                    # Aggregate to 1s bars (v3 schema bar semantics)
                                    by_sec: dict[int, dict] = {}
                                    for item in data:
                                        if not isinstance(item, list) or len(item) < 2:
                                            continue
                                        ts_epoch = int(item[0])
                                        price = float(item[1])
                                        volume = float(item[2]) if len(item) >= 3 else 0
                                        b = by_sec.get(ts_epoch)
                                        if b is None:
                                            by_sec[ts_epoch] = {
                                                "o": price, "h": price, "l": price, "c": price,
                                                "v": volume, "tc": 1, "pv": price * volume,
                                            }
                                        else:
                                            b["h"] = max(b["h"], price)
                                            b["l"] = min(b["l"], price)
                                            b["c"] = price
                                            b["v"] += volume
                                            b["tc"] += 1
                                            b["pv"] += price * volume
                                    for ts_epoch, b in by_sec.items():
                                        try:
                                            ts_str = datetime.fromtimestamp(ts_epoch).strftime("%Y-%m-%d %H:%M:%S")
                                        except (ValueError, OSError):
                                            continue
                                        vwap = (b["pv"] / b["v"]) if b["v"] > 0 else b["c"]
                                        batch.append((sym, "REG", dt, ts_str, ts_epoch,
                                                      b["o"], b["h"], b["l"], b["c"],
                                                      b["v"], b["tc"], vwap))
                                con.executemany(
                                    "INSERT OR IGNORE INTO intraday_bars "
                                    "(symbol, market, date, ts, ts_epoch, interval, "
                                    " open, high, low, close, volume, trade_count, vwap, source) "
                                    "VALUES (?,?,?,?,?,'1s',?,?,?,?,?,?,?,'disk_json')",
                                    batch,
                                )
                                con.commit()
                                st.toast(f"Fixed {dt}: {len(batch):,} rows → SQLite")
                                fixed += 1

                            elif action == "Disk JSON → DuckDB":
                                # Export to Parquet instead of DuckDB
                                from pakfindata.db.parquet_store import export_table
                                rows = export_table("intraday_bars", dt)
                                st.toast(f"Fixed {dt}: {rows:,} rows → Parquet")
                                fixed += 1
                        except Exception as e:
                            st.error(f"Failed {dt}: {e}")

                    if fixed:
                        st.success(f"Fixed {fixed}/{len(fillable)} gaps")
                        st.rerun()

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
            f"intraday -> eod ({last_td_str})", key="int_sync_promote",
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
                    # Detect actual date from data timestamps
                    from pakfindata.config import DATA_ROOT
                    first_ts = df_fetched["ts"].iloc[0]
                    detected_date = str(first_ts)[:10] if first_ts else last_td_str
                    json_dir = DATA_ROOT / "intraday" / detected_date
                    json_dir.mkdir(parents=True, exist_ok=True)
                    json_path = json_dir / f"{sel_sym}.json"
                    json_path.write_text(json.dumps(payload, indent=2))

                    INTRADAY_TEMP_DIR.mkdir(parents=True, exist_ok=True)
                    csv_path = INTRADAY_TEMP_DIR / f"{sel_sym}.csv"
                    df_fetched[["symbol", "ts", "open", "high", "low", "close", "volume"]].to_csv(csv_path, index=False)

                    st.session_state.intraday_sync_result = {
                        "action": "download", "success": True,
                        "rows": len(df_fetched), "json_path": str(json_path),
                        "detected_date": detected_date,
                    }
                    status.update(label=f"Downloaded {len(df_fetched)} ticks for {detected_date}", state="complete")
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

                # Try last trading day first, then today, then scan
                json_path = DATA_ROOT / "intraday" / last_td_str / f"{sel_sym}.json"
                if not json_path.exists():
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
                eod_count = promote_intraday_to_eod(con, last_td_str)
                st.session_state.intraday_sync_result = {
                    "action": "promote", "success": True, "eod_promoted": eod_count,
                }
                status.update(label=f"Promoted {eod_count} symbols for {last_td_str}", state="complete")
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
                det_date = result.get("detected_date", "")
                if rows > 0:
                    st.success(f"Downloaded {rows} ticks" + (f" for **{det_date}**" if det_date else ""))
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
