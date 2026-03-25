"""Tick Analytics Terminal — Quant-grade market microstructure from tick_bars.db.

Data source: tick_bars.db (written by tick_service)
  ohlcv_5s        — 5-second OHLCV bars per symbol (REG/FUT/ODL)
  raw_ticks       — individual ticks with bid/ask
  index_ohlcv_5s  — 5-second bars for indices (KSE100, ALLSHR, etc.)
  index_raw_ticks — raw index ticks

Sections:
  Overview — Market-wide KPIs with cross-day history, regime indicators, daily KPI table
  Market Snapshot — Single-day KPIs, breadth, top movers
  Price & Returns — Intraday price chart, return distribution, autocorrelation
  Volume Profile — Time-of-day volume, VWAP, cumulative delta, block detection
  Spread & Liquidity — Bid-ask spread evolution, depth, liquidity score
  Volatility — Realized vol, Garman-Klass, Parkinson, vol smile by sector
  Correlation & Heatmap — Intraday return correlations across symbols
  Flow Analysis — Net buy/sell pressure, order imbalance, tick-rule classification
"""

from __future__ import annotations

import json
import sqlite3
import statistics
from pathlib import Path
from datetime import datetime

import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer
from pakfindata.db.repositories.tick_logs import (
    sync_latest_file,
    backfill_all_files,
    backfill_background,
    get_backfill_status,
    insert_ticks_from_file,
    ensure_tick_logs_table,
)
from pakfindata.db.repositories.tick_summary import (
    ensure_summary_table,
    get_available_dates as _summary_available_dates,
    get_summary_dates,
    compute_daily_summary,
    compute_missing_summaries,
    get_daily_summary,
    get_multi_day_summary,
    get_summary_stats,
)

# Heavy libs — lazy-loaded on first use via _lazy_imports()
np = pd = go = px = make_subplots = None  # type: ignore[assignment]

def _lazy_imports():
    """Load numpy/pandas/plotly on first call to an analytics tab (not Sync)."""
    global np, pd, go, px, make_subplots
    if np is not None:
        return
    import numpy as _np
    import pandas as _pd
    import plotly.graph_objects as _go
    import plotly.express as _px
    from plotly.subplots import make_subplots as _make_subplots
    np, pd, go, px, make_subplots = _np, _pd, _go, _px, _make_subplots

# ═══════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═══════════════════════════════════════════════════════════════════════════════

_C = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#2F81F7", "amber": "#FFB300", "cyan": "#00B8D4",
    "magenta": "#E040FB", "teal": "#00D4AA",
    "bg": "#0e1117", "card": "#1a1a2e", "grid": "#1E2329",
    "text": "#EAECEF", "dim": "#6B7280",
    "vol_buy": "#00E676", "vol_sell": "#FF5252",
}

_CHART = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_C["text"], size=11, family="JetBrains Mono, monospace"),
    xaxis=dict(gridcolor=_C["grid"], zeroline=False),
    yaxis=dict(gridcolor=_C["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
    margin=dict(l=10, r=10, t=40, b=10),
    hovermode="x unified",
)

TICK_LOG_DIR = Path("/mnt/e/psxdata/tick_logs")
TICK_BARS_DB = Path("/mnt/e/psxdata/tick_bars.db")


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING — tick_bars.db (primary) with JSONL fallback
# ═══════════════════════════════════════════════════════════════════════════════

def _tick_bars_con() -> sqlite3.Connection:
    """Open a read-only connection to tick_bars.db."""
    con = sqlite3.connect(f"file:{TICK_BARS_DB}?mode=ro", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA cache_size=-20000")  # 20 MB cache
    return con


@st.cache_data(ttl=60, show_spinner=False)
def _available_dates() -> list[str]:
    """List all dates with tick data from any source.

    Sources: tick_daily_summary, DuckDB tick_logs, JSONL files.
    """
    dates: set[str] = set()
    try:
        con = get_connection()
        ensure_summary_table(con)
        dates = set(get_summary_dates(con))
    except Exception:
        pass

    # DuckDB tick_logs dates
    try:
        from pakfindata.db.connections import has_duckdb
        if has_duckdb():
            import duckdb as _duckdb
            from pakfindata.db.duckdb_manager import DUCKDB_PATH
            dcon = _duckdb.connect(str(DUCKDB_PATH), read_only=True)
            rows = dcon.execute(
                "SELECT DISTINCT REPLACE(source_file, 'ticks_', '') "
                "FROM (SELECT REPLACE(source_file, '.jsonl', '') AS source_file FROM tick_logs)"
            ).fetchall()
            dcon.close()
            dates |= {r[0] for r in rows if r[0]}
    except Exception:
        pass

    # JSONL files on disk
    file_dates = {
        f.stem.replace("ticks_", "")
        for f in TICK_LOG_DIR.glob("ticks_*.jsonl")
    }
    return sorted(dates | file_dates, reverse=True)


def _enrich_bars_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed columns to a bars DataFrame from tick_bars.db.

    Derives fields expected by analytics tabs (changePercent, value, etc.)
    from the raw 5-second OHLCV data.
    """
    if df.empty:
        return df
    if "ts" in df.columns:
        dt = pd.to_datetime(df["ts"])
        if dt.dt.tz is not None:
            # Already tz-aware (ISO with +05:00) — convert to PKT directly
            df["_ts"] = dt.dt.tz_convert("Asia/Karachi")
        else:
            # Naive timestamps — assume PKT
            df["_ts"] = dt.dt.tz_localize("Asia/Karachi")
    # Rename ohlcv_5s columns to match analytics expectations
    rename = {"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    if "close" in df.columns:
        df["price"] = df["close"]

    # Compute per-symbol derived fields that analytics tabs expect
    # previousClose: first open of the day per symbol (proxy for prev close)
    if "open" in df.columns:
        first_open = df.groupby("symbol")["open"].transform("first")
        df["previousClose"] = first_open
        df["change"] = df["price"] - df["previousClose"]
        df["changePercent"] = (df["change"] / df["previousClose"].replace(0, float("nan")) * 100).fillna(0)
    else:
        df["previousClose"] = 0
        df["change"] = 0
        df["changePercent"] = 0

    # value: cumulative turnover proxy (price * volume)
    if "price" in df.columns and "volume" in df.columns:
        df["value"] = df["price"] * df["volume"]
    else:
        df["value"] = 0

    # bid/ask placeholders (not available in ohlcv_5s; use raw_ticks for spread analysis)
    for col in ("bid", "ask", "bidVol", "askVol"):
        if col not in df.columns:
            df[col] = 0.0
    df["spread"] = 0.0
    df["spread_bps"] = 0.0
    df["mid"] = df["price"]

    return df


def _enrich_ticks_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed columns to a raw_ticks DataFrame."""
    if df.empty:
        return df
    if "ts" in df.columns:
        df["_ts"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.tz_convert("Asia/Karachi")
    if "bid" in df.columns and "ask" in df.columns:
        df["spread"] = df["ask"] - df["bid"]
        df["spread_bps"] = np.where(
            df["price"] > 0, df["spread"] / df["price"] * 10_000, 0
        )
        df["mid"] = (df["bid"] + df["ask"]) / 2
    return df


@st.cache_data(ttl=120, show_spinner="Loading summary…")
def _load_ticks(date_str: str) -> pd.DataFrame:
    """Load precomputed daily summary from tick_daily_summary.

    Returns a DataFrame with one row per symbol — all daily metrics
    precomputed (OHLCV, change%, vol, turnover, etc.). Instant read.
    Auto-computes summary if missing but raw data exists.
    """
    try:
        con = get_connection()
        ensure_summary_table(con)
        df = get_daily_summary(con, date_str)
        if df.empty:
            # Auto-compute summary from raw tick data
            computed = compute_daily_summary(con, date_str)
            if computed > 0:
                df = get_daily_summary(con, date_str)
        if not df.empty:
            # Rename for compatibility with existing analytics code
            df["price"] = df["close"]
            df["changePercent"] = df["change_pct"]
            df["value"] = df["turnover"]
            df["previousClose"] = df["open"]  # proxy
            df["spread_bps"] = df["med_spread_bps"]
            # Placeholders for bid/ask (not in summary)
            for col in ("bid", "ask", "bidVol", "askVol", "mid"):
                if col not in df.columns:
                    df[col] = 0.0
            df["spread"] = 0.0
            return df
    except Exception:
        pass

    return pd.DataFrame()


@st.cache_data(ttl=120, show_spinner="Loading 5s bars…")
def _load_bars(date_str: str) -> pd.DataFrame:
    """Load 5-second OHLCV bars for a trading date (for charting/resampling).

    DuckDB first, SQLite fallback.
    """
    _lazy_imports()

    ts_start = f"{date_str}T00:00:00"
    ts_end = f"{date_str}T23:59:59+99:99"

    # DuckDB: fast columnar scan
    try:
        import duckdb as _duckdb
        from pakfindata.db.connections import has_duckdb
        from pakfindata.db.duckdb_manager import DUCKDB_PATH
        if has_duckdb():
            con = _duckdb.connect(str(DUCKDB_PATH), read_only=True)
            df = con.execute(
                "SELECT * FROM ohlcv_5s WHERE ts >= ? AND ts <= ? ORDER BY symbol, ts",
                [ts_start, ts_end],
            ).df()
            con.close()
            if not df.empty:
                return _enrich_bars_df(df)
    except Exception:
        pass

    # SQLite fallback — range query uses idx_bar_sym_ts efficiently
    if TICK_BARS_DB.exists():
        try:
            con = _tick_bars_con()
            df = pd.read_sql_query(
                "SELECT * FROM ohlcv_5s WHERE ts >= ? AND ts <= ? ORDER BY symbol, ts",
                con, params=[ts_start, ts_end],
            )
            con.close()
            if not df.empty:
                return _enrich_bars_df(df)
        except Exception:
            pass

    return pd.DataFrame()


@st.cache_data(ttl=120, show_spinner="Loading raw ticks…")
def _load_raw_ticks(date_str: str, symbol: str) -> pd.DataFrame:
    """Load raw ticks — DuckDB JSONL (fast) with SQLite fallback."""
    import duckdb as _duckdb
    from datetime import timezone, timedelta

    pkt = timezone(timedelta(hours=5))
    day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(
        hour=0, minute=0, second=0, tzinfo=pkt
    )
    day_end = day_start.replace(hour=23, minute=59, second=59)
    ts_start = day_start.timestamp()
    ts_end = day_end.timestamp()

    # Method 1: DuckDB direct JSONL query (50-100x faster than line-by-line)
    cloud_path = Path(f"/mnt/e/psxdata/tick_logs_cloud/ticks_{date_str}.jsonl")
    local_path = Path(f"/mnt/e/psxdata/tick_logs/ticks_{date_str}.jsonl")
    jsonl_path = cloud_path if cloud_path.exists() else local_path

    if jsonl_path.exists():
        try:
            con = _duckdb.connect()
            df = con.execute(f"""
                SELECT
                    symbol, market, timestamp AS ts, price, volume,
                    COALESCE(bid, 0) AS bid, COALESCE(ask, 0) AS ask,
                    COALESCE("bidVol", 0) AS bid_vol, COALESCE("askVol", 0) AS ask_vol
                FROM read_json_auto('{jsonl_path}',
                     format='newline_delimited',
                     maximum_object_size=10485760)
                WHERE symbol = '{symbol}' AND market != 'IDX'
                ORDER BY timestamp
            """).df()
            con.close()
            if not df.empty:
                return _enrich_ticks_df(df)
        except Exception:
            pass

    # Method 2: Fallback to tick_bars.db raw_ticks table (legacy data)
    if TICK_BARS_DB.exists():
        try:
            con = _tick_bars_con()
            tables = [r[0] for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_ticks'"
            ).fetchall()]
            if "raw_ticks" in tables:
                df = pd.read_sql_query(
                    """SELECT symbol, market, ts, price, volume, bid, ask, bid_vol, ask_vol
                       FROM raw_ticks
                       WHERE symbol = ? AND ts >= ? AND ts <= ?
                       ORDER BY ts""",
                    con,
                    params=(symbol, ts_start, ts_end),
                )
                con.close()
                if not df.empty:
                    return _enrich_ticks_df(df)
            else:
                con.close()
        except Exception:
            pass

    return pd.DataFrame()


def _last_tick_per_symbol(df: pd.DataFrame, market: str = "REG") -> pd.DataFrame:
    """Get the final bar for each symbol in a given market."""
    if "market" in df.columns:
        mdf = df[df["market"] == market]
    else:
        mdf = df
    return mdf.drop_duplicates("symbol", keep="last").copy()


def _build_ohlcv_bars(df: pd.DataFrame, symbol: str, freq: str = "1min") -> pd.DataFrame:
    """Resample 5-second bars into larger bars for a single symbol.

    Since tick_bars.db already has 5-second OHLCV, we just resample up.
    """
    if "market" in df.columns:
        sdf = df[(df["symbol"] == symbol) & (df["market"] == "REG")].copy()
    else:
        sdf = df[df["symbol"] == symbol].copy()
    if sdf.empty:
        return pd.DataFrame()
    sdf = sdf.set_index("_ts")
    bars = pd.DataFrame()
    bars["open"] = sdf["open"].resample(freq).first()
    bars["high"] = sdf["high"].resample(freq).max()
    bars["low"] = sdf["low"].resample(freq).min()
    bars["close"] = sdf["close"].resample(freq).last()
    bars["volume"] = sdf["volume"].resample(freq).sum()
    if "trades" in sdf.columns:
        bars["trades"] = sdf["trades"].resample(freq).sum()
    # VWAP approximation from bar data
    bars["vwap"] = np.where(
        bars["volume"] > 0,
        (sdf["close"] * sdf["volume"]).resample(freq).sum() / bars["volume"].replace(0, np.nan),
        bars["close"],
    )
    bars = bars.dropna(subset=["close"])
    return bars.reset_index()


# ═══════════════════════════════════════════════════════════════════════════════
# KPI CARD
# ═══════════════════════════════════════════════════════════════════════════════

def _kpi(label: str, value: str, delta: str = "", color: str = _C["accent"]):
    delta_html = ""
    if delta:
        dc = _C["up"] if delta.startswith("+") else _C["down"] if delta.startswith("-") else _C["dim"]
        delta_html = f'<div style="color:{dc};font-size:0.85em;">{delta}</div>'
    st.markdown(
        f'<div style="background:{_C["card"]};padding:14px 16px;border-radius:6px;'
        f'border-left:3px solid {color};font-family:monospace;">'
        f'<div style="color:{_C["dim"]};font-size:0.75em;text-transform:uppercase;">{label}</div>'
        f'<div style="color:{_C["text"]};font-size:1.4em;font-weight:700;">{value}</div>'
        f'{delta_html}</div>',
        unsafe_allow_html=True,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 1: MARKET OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

def _render_market_overview(df: pd.DataFrame):
    st.markdown("### Market Overview")

    last = _last_tick_per_symbol(df, "REG")
    active = last[last["volume"] > 0]

    # KPI row
    total_vol = active["volume"].sum()
    total_val = active["value"].sum()
    advancers = (active["changePercent"] > 0).sum()
    decliners = (active["changePercent"] < 0).sum()
    unchanged = (active["changePercent"] == 0).sum()
    total_ticks = len(df[df["market"] == "REG"])
    avg_spread = active["spread_bps"].median()

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        _kpi("Total Volume", f"{total_vol / 1e6:,.1f}M")
    with c2:
        _kpi("Turnover", f"PKR {total_val / 1e9:,.2f}B", color=_C["amber"])
    with c3:
        _kpi("Active Symbols", f"{len(active)}", color=_C["cyan"])
    with c4:
        ratio = advancers / max(decliners, 1)
        _kpi("A/D Ratio", f"{ratio:.2f}", f"{advancers}▲ {decliners}▼ {unchanged}—",
             color=_C["up"] if ratio > 1 else _C["down"])
    with c5:
        _kpi("Tick Count", f"{total_ticks:,}", color=_C["magenta"])
    with c6:
        _kpi("Med Spread", f"{avg_spread:.1f} bps", color=_C["teal"])

    # Breadth bar
    total = advancers + decliners + unchanged
    if total > 0:
        pct_a = advancers / total * 100
        pct_d = decliners / total * 100
        pct_u = 100 - pct_a - pct_d
        st.markdown(
            f'<div style="display:flex;height:8px;border-radius:4px;overflow:hidden;margin:8px 0 16px 0;">'
            f'<div style="width:{pct_a}%;background:{_C["up"]};"></div>'
            f'<div style="width:{pct_u}%;background:{_C["neutral"]};"></div>'
            f'<div style="width:{pct_d}%;background:{_C["down"]};"></div>'
            f'</div>',
            unsafe_allow_html=True,
        )

    # Top movers table
    col_g, col_l, col_v = st.columns(3)
    with col_g:
        st.markdown(f"**Top Gainers**")
        g = active.nlargest(10, "changePercent")[["symbol", "price", "changePercent", "volume"]]
        g.columns = ["Symbol", "Price", "Chg%", "Volume"]
        st.dataframe(g, hide_index=True, use_container_width=True)
    with col_l:
        st.markdown(f"**Top Losers**")
        l = active.nsmallest(10, "changePercent")[["symbol", "price", "changePercent", "volume"]]
        l.columns = ["Symbol", "Price", "Chg%", "Volume"]
        st.dataframe(l, hide_index=True, use_container_width=True)
    with col_v:
        st.markdown(f"**Volume Leaders**")
        v = active.nlargest(10, "value")[["symbol", "price", "changePercent", "value"]]
        v["value"] = (v["value"] / 1e6).round(1)
        v.columns = ["Symbol", "Price", "Chg%", "Val(M)"]
        st.dataframe(v, hide_index=True, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 2: SINGLE-SYMBOL PRICE & RETURNS
# ═══════════════════════════════════════════════════════════════════════════════

def _render_price_returns(df: pd.DataFrame, symbol: str, freq: str):
    bars = _build_ohlcv_bars(df, symbol, freq)
    if bars.empty:
        st.warning(f"No tick data for {symbol}")
        return

    st.markdown(f"### {symbol} — Price & Returns ({freq} bars)")

    # Candlestick + VWAP + Volume
    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03,
        row_heights=[0.7, 0.3],
    )

    fig.add_trace(go.Candlestick(
        x=bars["_ts"], open=bars["open"], high=bars["high"],
        low=bars["low"], close=bars["close"], name="OHLC",
        increasing_line_color=_C["up"], decreasing_line_color=_C["down"],
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=bars["_ts"], y=bars["vwap"], name="VWAP",
        line=dict(color=_C["amber"], width=1.5, dash="dot"),
    ), row=1, col=1)

    # Volume bars colored by direction
    colors = [_C["up"] if c >= o else _C["down"]
              for c, o in zip(bars["close"], bars["open"])]
    fig.add_trace(go.Bar(
        x=bars["_ts"], y=bars["volume"], name="Volume",
        marker_color=colors, opacity=0.6,
    ), row=2, col=1)

    fig.update_layout(**_CHART, height=520, showlegend=True,
                      title=dict(text=f"{symbol} Intraday", font=dict(size=14)))
    fig.update_xaxes(rangeslider_visible=False)
    fig.update_yaxes(title_text="Price", row=1, col=1, gridcolor=_C["grid"])
    fig.update_yaxes(title_text="Volume", row=2, col=1, gridcolor=_C["grid"])
    st.plotly_chart(fig, use_container_width=True)

    # Return distribution
    bars["ret"] = bars["close"].pct_change() * 100
    rets = bars["ret"].dropna()

    col_hist, col_qq = st.columns(2)
    with col_hist:
        fig2 = go.Figure()
        fig2.add_trace(go.Histogram(
            x=rets, nbinsx=60, name="Returns",
            marker_color=_C["accent"], opacity=0.7,
        ))
        skew = rets.skew()
        kurt = rets.kurtosis()
        fig2.update_layout(**_CHART, height=300,
                           title=dict(text=f"Return Distribution (skew={skew:.2f}, kurt={kurt:.2f})",
                                      font=dict(size=12)))
        fig2.update_xaxes(title_text="Return %")
        st.plotly_chart(fig2, use_container_width=True)

    with col_qq:
        # Autocorrelation
        max_lag = min(30, len(rets) - 1)
        if max_lag > 1:
            acf_vals = [rets.autocorr(lag=i) for i in range(1, max_lag + 1)]
            fig3 = go.Figure()
            fig3.add_trace(go.Bar(
                x=list(range(1, max_lag + 1)), y=acf_vals,
                marker_color=_C["cyan"], name="ACF",
            ))
            ci = 1.96 / np.sqrt(len(rets))
            fig3.add_hline(y=ci, line_dash="dash", line_color=_C["dim"])
            fig3.add_hline(y=-ci, line_dash="dash", line_color=_C["dim"])
            fig3.update_layout(**_CHART, height=300,
                               title=dict(text="Return Autocorrelation", font=dict(size=12)))
            fig3.update_xaxes(title_text="Lag")
            st.plotly_chart(fig3, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 3: VOLUME PROFILE & VWAP
# ═══════════════════════════════════════════════════════════════════════════════

def _render_volume_profile(df: pd.DataFrame, symbol: str):
    sdf = df[(df["symbol"] == symbol) & (df["market"] == "REG")].copy()
    if sdf.empty:
        return

    st.markdown(f"### {symbol} — Volume Profile & Flow")

    col_vp, col_tod = st.columns(2)

    with col_vp:
        # Price-based volume profile
        prices = sdf["price"]
        n_bins = 40
        bins = np.linspace(prices.min(), prices.max(), n_bins + 1)
        sdf["price_bin"] = pd.cut(prices, bins=bins, labels=False)
        vol_diff = sdf.groupby("price_bin")["volume"].last().diff().fillna(0).clip(lower=0)

        fig = go.Figure()
        fig.add_trace(go.Bar(
            y=[(bins[i] + bins[i + 1]) / 2 for i in range(n_bins)],
            x=vol_diff.reindex(range(n_bins), fill_value=0).values,
            orientation="h", name="Volume @ Price",
            marker_color=_C["accent"], opacity=0.7,
        ))
        fig.update_layout(**_CHART, height=400,
                          title=dict(text="Volume Profile (Price Levels)", font=dict(size=12)))
        fig.update_xaxes(title_text="Volume")
        fig.update_yaxes(title_text="Price")
        st.plotly_chart(fig, use_container_width=True)

    with col_tod:
        # Time-of-day volume
        sdf["hour"] = sdf["_ts"].dt.hour
        hourly = sdf.groupby("hour").agg(
            vol=("volume", lambda x: x.diff().fillna(0).clip(lower=0).sum()),
            ticks=("price", "count"),
        ).reset_index()

        fig2 = make_subplots(specs=[[{"secondary_y": True}]])
        fig2.add_trace(go.Bar(
            x=hourly["hour"], y=hourly["vol"], name="Volume",
            marker_color=_C["teal"], opacity=0.7,
        ), secondary_y=False)
        fig2.add_trace(go.Scatter(
            x=hourly["hour"], y=hourly["ticks"], name="Tick Count",
            line=dict(color=_C["amber"], width=2),
        ), secondary_y=True)
        fig2.update_layout(**_CHART, height=400,
                           title=dict(text="Time-of-Day Volume", font=dict(size=12)))
        fig2.update_xaxes(title_text="Hour (PKT)")
        st.plotly_chart(fig2, use_container_width=True)

    # Cumulative order imbalance (tick rule)
    sdf = sdf.sort_values("_ts")
    dp = sdf["price"].diff()
    sdf["tick_dir"] = np.where(dp > 0, 1, np.where(dp < 0, -1, 0))
    # Forward-fill zero ticks
    sdf["tick_dir"] = sdf["tick_dir"].replace(0, np.nan).ffill().fillna(0)
    vol_diff_ts = sdf["volume"].diff().fillna(0).clip(lower=0)
    sdf["signed_vol"] = sdf["tick_dir"] * vol_diff_ts
    sdf["cum_delta"] = sdf["signed_vol"].cumsum()

    fig3 = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                         row_heights=[0.5, 0.5])
    fig3.add_trace(go.Scatter(
        x=sdf["_ts"], y=sdf["price"], name="Price",
        line=dict(color=_C["text"], width=1.5),
    ), row=1, col=1)
    fig3.add_trace(go.Scatter(
        x=sdf["_ts"], y=sdf["cum_delta"], name="Cum Delta",
        fill="tozeroy", fillcolor="rgba(0,212,170,0.15)",
        line=dict(color=_C["teal"], width=2),
    ), row=2, col=1)
    fig3.update_layout(**_CHART, height=400,
                       title=dict(text="Price vs Cumulative Volume Delta (Tick Rule)",
                                  font=dict(size=12)))
    fig3.update_yaxes(title_text="Price", row=1, col=1, gridcolor=_C["grid"])
    fig3.update_yaxes(title_text="Cum Δ Vol", row=2, col=1, gridcolor=_C["grid"])
    st.plotly_chart(fig3, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 4: SPREAD & LIQUIDITY
# ═══════════════════════════════════════════════════════════════════════════════

def _render_spread_liquidity(df: pd.DataFrame, symbol: str):
    sdf = df[(df["symbol"] == symbol) & (df["market"] == "REG")].copy()
    if sdf.empty:
        return

    st.markdown(f"### {symbol} — Spread & Liquidity")

    col_sp, col_dep = st.columns(2)

    with col_sp:
        # Spread over time
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=sdf["_ts"], y=sdf["spread_bps"], name="Spread (bps)",
            line=dict(color=_C["amber"], width=1), mode="lines",
        ))
        # Rolling median
        if len(sdf) > 20:
            sdf["spread_ma"] = sdf["spread_bps"].rolling(20, min_periods=1).median()
            fig.add_trace(go.Scatter(
                x=sdf["_ts"], y=sdf["spread_ma"], name="20-tick Median",
                line=dict(color=_C["magenta"], width=2),
            ))
        fig.update_layout(**_CHART, height=350,
                          title=dict(text="Bid-Ask Spread Evolution", font=dict(size=12)))
        fig.update_yaxes(title_text="Spread (bps)")
        st.plotly_chart(fig, use_container_width=True)

    with col_dep:
        # Book depth: bid vs ask volume
        fig2 = go.Figure()
        fig2.add_trace(go.Scatter(
            x=sdf["_ts"], y=sdf["bidVol"], name="Bid Depth",
            line=dict(color=_C["up"], width=1), fill="tozeroy",
            fillcolor="rgba(0,230,118,0.1)",
        ))
        fig2.add_trace(go.Scatter(
            x=sdf["_ts"], y=sdf["askVol"], name="Ask Depth",
            line=dict(color=_C["down"], width=1), fill="tozeroy",
            fillcolor="rgba(255,82,82,0.1)",
        ))
        fig2.update_layout(**_CHART, height=350,
                           title=dict(text="Order Book Depth (Bid vs Ask Volume)",
                                      font=dict(size=12)))
        fig2.update_yaxes(title_text="Volume")
        st.plotly_chart(fig2, use_container_width=True)

    # Liquidity score card
    med_spread = sdf["spread_bps"].median()
    avg_depth = ((sdf["bidVol"] + sdf["askVol"]) / 2).median()
    tick_freq = len(sdf) / max((sdf["_ts"].max() - sdf["_ts"].min()).total_seconds() / 60, 1)

    # Amihud illiquidity = |return| / turnover
    sdf["ret_abs"] = sdf["price"].pct_change().abs()
    sdf["turnover"] = sdf["value"].diff().fillna(0).clip(lower=0)
    valid = sdf[sdf["turnover"] > 0]
    amihud = (valid["ret_abs"] / valid["turnover"] * 1e6).median() if len(valid) > 0 else 0

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi("Median Spread", f"{med_spread:.1f} bps", color=_C["amber"])
    with c2:
        _kpi("Avg Depth", f"{avg_depth:,.0f}", color=_C["teal"])
    with c3:
        _kpi("Tick Freq", f"{tick_freq:.1f}/min", color=_C["cyan"])
    with c4:
        _kpi("Amihud Illiq", f"{amihud:.4f}", color=_C["magenta"])


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 5: VOLATILITY
# ═══════════════════════════════════════════════════════════════════════════════

def _render_volatility(df: pd.DataFrame, symbol: str, freq: str):
    bars = _build_ohlcv_bars(df, symbol, freq)
    if bars.empty or len(bars) < 5:
        return

    st.markdown(f"### {symbol} — Volatility Estimators")

    bars["ret"] = np.log(bars["close"] / bars["close"].shift(1))
    bars["ret2"] = bars["ret"] ** 2

    # Rolling realized vol
    window = min(20, len(bars) - 1)
    bars["realized_vol"] = bars["ret"].rolling(window).std() * np.sqrt(252 * (390 / {"1min": 1, "5min": 5, "15min": 15, "30min": 30}.get(freq, 1)))

    # Garman-Klass
    bars["gk"] = (
        0.5 * (np.log(bars["high"] / bars["low"])) ** 2
        - (2 * np.log(2) - 1) * (np.log(bars["close"] / bars["open"])) ** 2
    )
    bars["gk_vol"] = bars["gk"].rolling(window).mean().apply(lambda x: np.sqrt(max(x, 0)) * np.sqrt(252))

    # Parkinson
    bars["park"] = (np.log(bars["high"] / bars["low"])) ** 2 / (4 * np.log(2))
    bars["park_vol"] = bars["park"].rolling(window).mean().apply(lambda x: np.sqrt(max(x, 0)) * np.sqrt(252))

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=bars["_ts"], y=bars["realized_vol"], name=f"Realized ({window}-bar)",
        line=dict(color=_C["accent"], width=2),
    ))
    fig.add_trace(go.Scatter(
        x=bars["_ts"], y=bars["gk_vol"], name="Garman-Klass",
        line=dict(color=_C["amber"], width=1.5, dash="dot"),
    ))
    fig.add_trace(go.Scatter(
        x=bars["_ts"], y=bars["park_vol"], name="Parkinson",
        line=dict(color=_C["magenta"], width=1.5, dash="dash"),
    ))
    fig.update_layout(**_CHART, height=380,
                      title=dict(text="Annualized Volatility Estimators", font=dict(size=13)))
    fig.update_yaxes(title_text="Vol (annualized)")
    st.plotly_chart(fig, use_container_width=True)

    # Vol stats
    c1, c2, c3, c4 = st.columns(4)
    rv = bars["realized_vol"].dropna()
    with c1:
        _kpi("Current RVol", f"{rv.iloc[-1]:.1%}" if len(rv) > 0 else "—")
    with c2:
        _kpi("Mean RVol", f"{rv.mean():.1%}" if len(rv) > 0 else "—", color=_C["amber"])
    with c3:
        hi_lo = bars["high"] / bars["low"] - 1
        _kpi("Avg Hi-Lo Range", f"{hi_lo.mean():.2%}", color=_C["cyan"])
    with c4:
        _kpi("Max Drawdown", f"{(bars['close'] / bars['close'].cummax() - 1).min():.2%}",
             color=_C["down"])


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 6: CROSS-SYMBOL CORRELATION
# ═══════════════════════════════════════════════════════════════════════════════

def _render_correlation(df: pd.DataFrame):
    st.markdown("### Intraday Return Correlation Heatmap")

    reg = df[df["market"] == "REG"]
    # Pick top N by tick count
    top_syms = reg["symbol"].value_counts().head(25).index.tolist()
    if len(top_syms) < 3:
        st.info("Not enough symbols for correlation.")
        return

    # Build 1-min return matrix
    returns = {}
    for sym in top_syms:
        sdf = reg[reg["symbol"] == sym].set_index("_ts")["price"]
        sdf = sdf.resample("1min").last().pct_change().dropna()
        if len(sdf) > 10:
            returns[sym] = sdf

    if len(returns) < 3:
        st.info("Not enough data for correlation matrix.")
        return

    ret_df = pd.DataFrame(returns).dropna(how="all")
    corr = ret_df.corr()

    fig = go.Figure(go.Heatmap(
        z=corr.values, x=corr.columns, y=corr.index,
        colorscale="RdBu_r", zmid=0, zmin=-1, zmax=1,
        text=corr.values.round(2), texttemplate="%{text}",
        textfont=dict(size=8),
    ))
    fig.update_layout(**_CHART, height=600,
                      title=dict(text="Top 25 Symbols — Intraday Return Correlation",
                                 font=dict(size=13)))
    st.plotly_chart(fig, use_container_width=True)

    # Highest correlations
    mask = np.triu(np.ones_like(corr, dtype=bool), k=1)
    pairs = corr.where(mask).stack().reset_index()
    pairs.columns = ["Sym A", "Sym B", "Corr"]
    pairs["AbsCorr"] = pairs["Corr"].abs()

    col_hi, col_lo = st.columns(2)
    with col_hi:
        st.markdown("**Most Correlated Pairs**")
        st.dataframe(pairs.nlargest(10, "AbsCorr")[["Sym A", "Sym B", "Corr"]],
                     hide_index=True, use_container_width=True)
    with col_lo:
        st.markdown("**Least Correlated Pairs**")
        st.dataframe(pairs.nsmallest(10, "AbsCorr")[["Sym A", "Sym B", "Corr"]],
                     hide_index=True, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 7: MARKET-WIDE FLOW & ORDER IMBALANCE
# ═══════════════════════════════════════════════════════════════════════════════

def _render_market_flow(df: pd.DataFrame):
    st.markdown("### Market-Wide Flow & Tick Frequency")

    reg = df[df["market"] == "REG"].copy()
    reg["minute"] = reg["_ts"].dt.floor("1min")

    # Ticks per minute
    tpm = reg.groupby("minute").size().reset_index(name="ticks")

    # Market-wide order imbalance by minute
    reg_sorted = reg.sort_values(["symbol", "_ts"])
    reg_sorted["dp"] = reg_sorted.groupby("symbol")["price"].diff()
    reg_sorted["dir"] = np.where(reg_sorted["dp"] > 0, 1, np.where(reg_sorted["dp"] < 0, -1, 0))
    imb = reg_sorted.groupby("minute")["dir"].mean().reset_index(name="imbalance")

    merged = tpm.merge(imb, on="minute", how="left")

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                        row_heights=[0.5, 0.5])

    fig.add_trace(go.Scatter(
        x=merged["minute"], y=merged["ticks"], name="Ticks/min",
        fill="tozeroy", fillcolor="rgba(47,129,247,0.15)",
        line=dict(color=_C["accent"], width=1.5),
    ), row=1, col=1)

    # Order imbalance as colored bars
    colors = [_C["up"] if v > 0 else _C["down"] for v in merged["imbalance"].fillna(0)]
    fig.add_trace(go.Bar(
        x=merged["minute"], y=merged["imbalance"], name="Order Imbalance",
        marker_color=colors, opacity=0.7,
    ), row=2, col=1)

    fig.update_layout(**_CHART, height=420,
                      title=dict(text="Tick Frequency & Market Order Imbalance", font=dict(size=13)))
    fig.update_yaxes(title_text="Ticks/min", row=1, col=1, gridcolor=_C["grid"])
    fig.update_yaxes(title_text="Imbalance (-1 to +1)", row=2, col=1, gridcolor=_C["grid"])
    st.plotly_chart(fig, use_container_width=True)

    # Sector flow
    last = _last_tick_per_symbol(df, "REG")
    if "sector" not in last.columns:
        # Try to get sectors from DB
        try:
            con = get_connection()
            sectors = pd.read_sql_query(
                "SELECT symbol, sector FROM symbols WHERE sector IS NOT NULL", con
            )
            last = last.merge(sectors, on="symbol", how="left")
        except Exception:
            pass

    if "sector" in last.columns and last["sector"].notna().any():
        sec_flow = last.groupby("sector").agg(
            net_chg=("changePercent", "mean"),
            volume=("volume", "sum"),
            count=("symbol", "count"),
        ).sort_values("net_chg", ascending=False).reset_index()

        fig2 = go.Figure()
        colors = [_C["up"] if v > 0 else _C["down"] for v in sec_flow["net_chg"]]
        fig2.add_trace(go.Bar(
            x=sec_flow["sector"], y=sec_flow["net_chg"],
            marker_color=colors, opacity=0.8,
            text=sec_flow["net_chg"].apply(lambda x: f"{x:+.2f}%"),
            textposition="outside",
        ))
        fig2.update_layout(**_CHART, height=400,
                           title=dict(text="Sector Average Change %", font=dict(size=13)))
        fig2.update_xaxes(tickangle=-45)
        st.plotly_chart(fig2, use_container_width=True)


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 8: FUTURES BASIS & SPREAD
# ═══════════════════════════════════════════════════════════════════════════════

def _render_futures_basis(df: pd.DataFrame):
    st.markdown("### Futures Basis Analysis")

    fut = df[df["market"] == "FUT"].drop_duplicates("symbol", keep="last")
    reg = df[df["market"] == "REG"].drop_duplicates("symbol", keep="last")

    if fut.empty or reg.empty:
        st.info("No futures data available.")
        return

    # Match futures to spot (e.g., LUCK-MAR → LUCK)
    fut = fut.copy()
    fut["spot_sym"] = fut["symbol"].str.replace(r"-(MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC|JAN|FEB)[A-Z]?$", "", regex=True)
    merged = fut.merge(reg[["symbol", "price"]], left_on="spot_sym", right_on="symbol",
                       suffixes=("_fut", "_spot"))
    merged = merged[merged["price_spot"] > 0]
    merged["basis_pct"] = (merged["price_fut"] / merged["price_spot"] - 1) * 100

    if merged.empty:
        st.info("No matching spot-futures pairs.")
        return

    merged = merged.sort_values("basis_pct")

    fig = go.Figure()
    colors = [_C["up"] if b > 0 else _C["down"] for b in merged["basis_pct"]]
    fig.add_trace(go.Bar(
        x=merged["symbol_fut"], y=merged["basis_pct"],
        marker_color=colors, opacity=0.8,
        text=merged["basis_pct"].apply(lambda x: f"{x:+.2f}%"),
        textposition="outside",
    ))
    fig.update_layout(**_CHART, height=450,
                      title=dict(text="Futures Premium/Discount vs Spot (%)", font=dict(size=13)))
    fig.update_xaxes(tickangle=-45)
    fig.update_yaxes(title_text="Basis %")
    st.plotly_chart(fig, use_container_width=True)

    # Stats
    c1, c2, c3 = st.columns(3)
    with c1:
        _kpi("Avg Basis", f"{merged['basis_pct'].mean():+.3f}%")
    with c2:
        _kpi("Max Premium", f"{merged['basis_pct'].max():+.3f}%",
             merged.loc[merged["basis_pct"].idxmax(), "symbol_fut"], color=_C["up"])
    with c3:
        _kpi("Max Discount", f"{merged['basis_pct'].min():+.3f}%",
             merged.loc[merged["basis_pct"].idxmin(), "symbol_fut"], color=_C["down"])


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 9: QUANT OVERVIEW (cross-day, market-wide)
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300, show_spinner="Building market overview…")
def _build_daily_stats() -> pd.DataFrame:
    """Per-date market-wide stats from tick_daily_summary (instant)."""
    try:
        con = get_connection()
        ensure_summary_table(con)
        df = pd.read_sql_query(
            """
            SELECT date,
                   SUM(bar_count) AS total_ticks,
                   COUNT(DISTINCT symbol) AS symbols,
                   SUM(CASE WHEN market='REG' THEN bar_count ELSE 0 END) AS reg_ticks,
                   COUNT(DISTINCT CASE WHEN market='FUT' THEN symbol END) AS fut_symbols,
                   1 AS files
            FROM tick_daily_summary
            GROUP BY date
            ORDER BY date
            """,
            con,
        )
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner=False)
def _build_daily_breadth() -> pd.DataFrame:
    """Per-date breadth / volume / spread stats from tick_daily_summary (instant)."""
    try:
        con = get_connection()
        ensure_summary_table(con)
        df = pd.read_sql_query(
            "SELECT * FROM tick_daily_summary WHERE market = 'REG'",
            con,
        )
        if df.empty:
            return pd.DataFrame()

        df["intraday_range_pct"] = np.where(
            df["low"] > 0, (df["high"] - df["low"]) / df["low"] * 100, 0
        )

        daily = df.groupby("date").agg(
            symbols=("symbol", "nunique"),
            advancers=("change_pct", lambda x: (x > 0).sum()),
            decliners=("change_pct", lambda x: (x < 0).sum()),
            unchanged=("change_pct", lambda x: (x == 0).sum()),
            total_volume=("volume", "sum"),
            total_turnover=("turnover", "sum"),
            total_trades=("trades", "sum"),
            median_spread_bps=("med_spread_bps", "median"),
            mean_spread_bps=("avg_spread_bps", "mean"),
            p90_spread_bps=("avg_spread_bps", lambda x: np.percentile(x, 90) if len(x) > 0 else 0),
            median_range_pct=("intraday_range_pct", "median"),
            mean_chg_pct=("change_pct", "mean"),
            std_chg_pct=("change_pct", "std"),
            max_gain=("change_pct", "max"),
            max_loss=("change_pct", "min"),
        ).reset_index()

        daily["ad_ratio"] = daily["advancers"] / daily["decliners"].replace(0, 1)
        daily["breadth_pct"] = (daily["advancers"] - daily["decliners"]) / daily["symbols"] * 100
        daily["avg_depth"] = 0  # Not available in summary

        return daily.sort_values("date")
    except Exception:
        return pd.DataFrame()


def _load_today_snapshot(source_file: str) -> dict:
    """Load today's overview KPIs from tick_daily_summary (instant)."""
    defaults = dict(
        total_ticks=0, symbols_traded=0, adv=0, dec=0, unch=0,
        total_vol=0, total_val=0, total_trades=0, fut_count=0,
        med_spread=0, p90_spread=0, mean_chg=0, cross_sec_vol=0,
        max_gain=0, max_loss=0, avg_depth=0, top10_conc=0,
        tick_rate=0,
    )
    try:
        # Extract date from source_file name
        date_str = source_file.replace("ticks_", "").replace(".jsonl", "")
        con = get_connection()
        ensure_summary_table(con)

        df = get_daily_summary(con, date_str)
        if df.empty:
            # Fallback: compute lightweight snapshot from DuckDB tick_logs
            try:
                from pakfindata.db.connections import duck
                tl = duck(
                    """SELECT symbol, market,
                              COUNT(*) AS bar_count,
                              MAX(volume) AS volume,
                              MAX(value) AS turnover,
                              MAX(trades) AS trades,
                              (MAX(price) - MIN(price)) / NULLIF(AVG(price), 0) * 10000 AS med_spread_bps,
                              ((LAST(price) - FIRST(price)) / NULLIF(FIRST(price), 0)) * 100 AS change_pct
                       FROM tick_logs
                       WHERE SUBSTR(_ts, 1, 10) = ? AND market IN ('REG', 'FUT')
                       GROUP BY symbol, market""",
                    [date_str],
                )
                if not tl.empty:
                    df = tl
                else:
                    return defaults
            except Exception:
                return defaults

        reg = df[df["market"] == "REG"] if "market" in df.columns else df
        active = reg[reg["volume"] > 0]
        if active.empty:
            return defaults

        total_ticks = int(active["bar_count"].sum())
        adv = int((active["change_pct"] > 0).sum())
        dec_ = int((active["change_pct"] < 0).sum())
        unch = int((active["change_pct"] == 0).sum())
        total_val = float(active["turnover"].sum())
        total_vol = float(active["volume"].sum())
        total_trades = int(active["trades"].sum())

        # Top-10 concentration
        top10_val = float(active.nlargest(10, "turnover")["turnover"].sum())

        sorted_spreads = sorted(active["med_spread_bps"].tolist())
        p90_idx = int(len(sorted_spreads) * 0.9)
        chg_pcts = active["change_pct"].tolist()

        # Futures count from summary
        fut_count = int((df["market"] == "FUT").sum()) if "market" in df.columns else 0

        # Duration estimate (first_ts to last_ts)
        duration_min = 330  # ~5.5h trading day default
        if "first_ts" in active.columns and "last_ts" in active.columns:
            try:
                ft = str(active["first_ts"].min())
                lt = str(active["last_ts"].max())
                if ft and lt and len(ft) > 15 and len(lt) > 15:
                    from datetime import datetime as _dt
                    t1 = _dt.fromisoformat(ft)
                    t2 = _dt.fromisoformat(lt)
                    duration_min = max((t2 - t1).total_seconds() / 60, 1)
            except Exception:
                pass

        return dict(
            total_ticks=total_ticks,
            symbols_traded=len(active),
            adv=adv, dec=dec_, unch=unch,
            total_vol=total_vol,
            total_val=total_val,
            total_trades=total_trades,
            fut_count=fut_count,
            med_spread=float(active["med_spread_bps"].median()) if len(active) else 0,
            p90_spread=sorted_spreads[p90_idx] if sorted_spreads else 0,
            mean_chg=statistics.mean(chg_pcts) if chg_pcts else 0,
            cross_sec_vol=statistics.stdev(chg_pcts) if len(chg_pcts) > 1 else 0,
            max_gain=max(chg_pcts) if chg_pcts else 0,
            max_loss=min(chg_pcts) if chg_pcts else 0,
            avg_depth=0,
            top10_conc=top10_val / max(total_val, 1) * 100,
            tick_rate=total_ticks / duration_min,
        )
    except Exception as e:
        import traceback
        print(f"[tick_analytics] _load_today_snapshot ERROR: {e}")
        traceback.print_exc()
        return defaults


def _render_quant_overview(latest_date: str):
    """Render market-wide quant overview with historical context."""
    st.markdown("### Quant Overview — Market-Wide Analytics")

    source_file = f"ticks_{latest_date}.jsonl"
    snap = _load_today_snapshot(source_file)

    daily_stats = _build_daily_stats()
    daily_breadth = _build_daily_breadth()

    has_history = len(daily_breadth) > 1

    total_ticks = snap["total_ticks"]
    symbols_traded = snap["symbols_traded"]
    adv = snap["adv"]
    dec = snap["dec"]
    unch = snap["unch"]
    ad_ratio = adv / max(dec, 1)
    breadth = (adv - dec) / max(symbols_traded, 1) * 100
    total_vol = snap["total_vol"]
    total_val = snap["total_val"]
    total_trades = snap["total_trades"]
    med_spread = snap["med_spread"]
    mean_chg = snap["mean_chg"]
    cross_sec_vol = snap["cross_sec_vol"]

    # Historical averages for delta comparison
    h_avg = {}
    if has_history:
        h_avg = {
            "ticks": daily_stats["total_ticks"].mean() if not daily_stats.empty else 0,
            "symbols": daily_breadth["symbols"].mean(),
            "ad_ratio": daily_breadth["ad_ratio"].mean(),
            "volume": daily_breadth["total_volume"].mean(),
            "turnover": daily_breadth["total_turnover"].mean(),
            "spread": daily_breadth["median_spread_bps"].mean(),
            "breadth": daily_breadth["breadth_pct"].mean(),
            "dispersion": daily_breadth["std_chg_pct"].mean(),
        }

    def _delta(val, avg_key, fmt=".1f", pct=False):
        if not h_avg or avg_key not in h_avg or h_avg[avg_key] == 0:
            return ""
        diff = (val / h_avg[avg_key] - 1) * 100
        s = f"{diff:+.0f}% vs avg"
        return s

    # Row 1: Activity KPIs
    st.markdown(f'<div style="color:{_C["dim"]};font-size:0.75em;text-transform:uppercase;'
                f'letter-spacing:1px;margin-bottom:4px;">Activity & Participation</div>',
                unsafe_allow_html=True)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        _kpi("Total Ticks", f"{total_ticks:,}", _delta(total_ticks, "ticks"))
    with c2:
        _kpi("Symbols Traded", f"{symbols_traded}", _delta(symbols_traded, "symbols"), color=_C["cyan"])
    with c3:
        _kpi("Total Volume", f"{total_vol / 1e6:,.1f}M", _delta(total_vol, "volume"), color=_C["teal"])
    with c4:
        _kpi("Turnover", f"PKR {total_val / 1e9:,.2f}B", _delta(total_val, "turnover"), color=_C["amber"])
    with c5:
        _kpi("Total Trades", f"{total_trades:,}", color=_C["magenta"])
    with c6:
        _kpi("Tick Rate", f"{snap['tick_rate']:.0f}/min", color=_C["accent"])

    st.markdown("")

    # Row 2: Breadth & Sentiment
    st.markdown(f'<div style="color:{_C["dim"]};font-size:0.75em;text-transform:uppercase;'
                f'letter-spacing:1px;margin-bottom:4px;">Breadth & Sentiment</div>',
                unsafe_allow_html=True)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        _kpi("A/D Ratio", f"{ad_ratio:.2f}", _delta(ad_ratio, "ad_ratio"),
             color=_C["up"] if ad_ratio > 1 else _C["down"])
    with c2:
        _kpi("Breadth %", f"{breadth:+.1f}%", _delta(breadth, "breadth") if breadth != 0 else "",
             color=_C["up"] if breadth > 0 else _C["down"])
    with c3:
        _kpi("Advancers", f"{adv}", color=_C["up"])
    with c4:
        _kpi("Decliners", f"{dec}", color=_C["down"])
    with c5:
        _kpi("Mean Chg%", f"{mean_chg:+.2f}%",
             color=_C["up"] if mean_chg > 0 else _C["down"])
    with c6:
        _kpi("Max Gain / Loss", f"+{snap['max_gain']:.1f}% / {snap['max_loss']:.1f}%",
             color=_C["neutral"])

    st.markdown("")

    # Row 3: Microstructure quality
    st.markdown(f'<div style="color:{_C["dim"]};font-size:0.75em;text-transform:uppercase;'
                f'letter-spacing:1px;margin-bottom:4px;">Microstructure & Liquidity</div>',
                unsafe_allow_html=True)
    c1, c2, c3, c4, c5, c6 = st.columns(6)
    with c1:
        _kpi("Med Spread", f"{med_spread:.1f} bps", _delta(med_spread, "spread"), color=_C["amber"])
    with c2:
        _kpi("P90 Spread", f"{snap['p90_spread']:.1f} bps", color=_C["amber"])
    with c3:
        _kpi("Cross-Sec Vol", f"{cross_sec_vol:.2f}%", _delta(cross_sec_vol, "dispersion"), color=_C["magenta"])
    with c4:
        _kpi("Med Book Depth", f"{snap['avg_depth']:,.0f}", color=_C["teal"])
    with c5:
        _kpi("Top-10 Conc%", f"{snap['top10_conc']:.1f}%", color=_C["cyan"])
    with c6:
        _kpi("Futures Active", f"{snap['fut_count']}", color=_C["neutral"])

    # ── Historical KPI Table ──
    if has_history:
        st.markdown("---")
        st.markdown(f'<div style="color:{_C["dim"]};font-size:0.75em;text-transform:uppercase;'
                    f'letter-spacing:1px;margin-bottom:8px;">Daily KPI History</div>',
                    unsafe_allow_html=True)

        # Merge stats + breadth
        if not daily_stats.empty and not daily_breadth.empty:
            hist = daily_breadth.merge(
                daily_stats[["date", "total_ticks", "reg_ticks", "fut_symbols"]],
                on="date", how="left",
            )
        elif not daily_breadth.empty:
            hist = daily_breadth.copy()
        else:
            hist = daily_stats.copy()

        # Display table
        display_cols = {
            "date": "Date",
            "total_ticks": "Ticks",
            "symbols": "Syms",
            "advancers": "Adv",
            "decliners": "Dec",
            "ad_ratio": "A/D",
            "breadth_pct": "Breadth%",
            "total_volume": "Volume",
            "total_turnover": "Turnover",
            "total_trades": "Trades",
            "median_spread_bps": "MedSprd(bps)",
            "mean_spread_bps": "AvgSprd(bps)",
            "std_chg_pct": "Dispersion%",
            "mean_chg_pct": "AvgChg%",
            "max_gain": "MaxGain%",
            "max_loss": "MaxLoss%",
        }
        available_cols = [c for c in display_cols if c in hist.columns]
        tbl = hist[available_cols].copy().sort_values("date", ascending=False)
        tbl.columns = [display_cols[c] for c in available_cols]

        # Format numerics
        for col in tbl.columns:
            if col == "Date":
                continue
            if col == "Volume":
                tbl[col] = (tbl[col] / 1e6).map(lambda x: f"{x:,.1f}M")
            elif col == "Turnover":
                tbl[col] = (tbl[col] / 1e9).map(lambda x: f"{x:,.2f}B")
            elif col in ("A/D",):
                tbl[col] = tbl[col].map(lambda x: f"{x:.2f}")
            elif col in ("Breadth%", "AvgChg%", "Dispersion%", "MaxGain%", "MaxLoss%"):
                tbl[col] = tbl[col].map(lambda x: f"{x:+.2f}" if pd.notna(x) else "—")
            elif col in ("MedSprd(bps)", "AvgSprd(bps)"):
                tbl[col] = tbl[col].map(lambda x: f"{x:.1f}")
            elif col in ("Ticks", "Trades", "Syms", "Adv", "Dec"):
                tbl[col] = tbl[col].map(lambda x: f"{int(x):,}" if pd.notna(x) else "—")

        st.dataframe(tbl, hide_index=True, use_container_width=True, height=min(400, 38 * len(tbl) + 38))

        # ── Historical Charts ──
        st.markdown("---")

        # Rebuild numeric hist for charting
        hist_c = daily_breadth.merge(
            daily_stats[["date", "total_ticks"]], on="date", how="left"
        ) if not daily_stats.empty and not daily_breadth.empty else daily_breadth.copy()
        hist_c["date"] = pd.to_datetime(hist_c["date"])
        hist_c = hist_c.sort_values("date")

        # Row of time-series charts
        col_a, col_b = st.columns(2)

        with col_a:
            # Breadth + A/D ratio
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            bar_colors = [_C["up"] if b > 0 else _C["down"] for b in hist_c["breadth_pct"]]
            fig.add_trace(go.Bar(
                x=hist_c["date"], y=hist_c["breadth_pct"], name="Breadth %",
                marker_color=bar_colors, opacity=0.6,
            ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=hist_c["date"], y=hist_c["ad_ratio"], name="A/D Ratio",
                line=dict(color=_C["accent"], width=2),
            ), secondary_y=True)
            fig.add_hline(y=1, line_dash="dash", line_color=_C["dim"], secondary_y=True)
            fig.update_layout(**_CHART, height=320,
                              title=dict(text="Market Breadth & A/D Ratio", font=dict(size=12)))
            fig.update_yaxes(title_text="Breadth %", secondary_y=False, gridcolor=_C["grid"])
            fig.update_yaxes(title_text="A/D Ratio", secondary_y=True, gridcolor=_C["grid"])
            st.plotly_chart(fig, use_container_width=True)

        with col_b:
            # Volume + Turnover
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(
                x=hist_c["date"], y=hist_c["total_volume"] / 1e6, name="Volume (M)",
                marker_color=_C["teal"], opacity=0.6,
            ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=hist_c["date"], y=hist_c["total_turnover"] / 1e9, name="Turnover (B)",
                line=dict(color=_C["amber"], width=2),
            ), secondary_y=True)
            fig.update_layout(**_CHART, height=320,
                              title=dict(text="Market Volume & Turnover", font=dict(size=12)))
            fig.update_yaxes(title_text="Volume (M)", secondary_y=False, gridcolor=_C["grid"])
            fig.update_yaxes(title_text="Turnover (PKR B)", secondary_y=True, gridcolor=_C["grid"])
            st.plotly_chart(fig, use_container_width=True)

        col_c, col_d = st.columns(2)

        with col_c:
            # Spread + Dispersion
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Scatter(
                x=hist_c["date"], y=hist_c["median_spread_bps"], name="Med Spread (bps)",
                line=dict(color=_C["amber"], width=2),
                fill="tozeroy", fillcolor="rgba(255,179,0,0.08)",
            ), secondary_y=False)
            if "p90_spread_bps" in hist_c.columns:
                fig.add_trace(go.Scatter(
                    x=hist_c["date"], y=hist_c["p90_spread_bps"], name="P90 Spread",
                    line=dict(color=_C["amber"], width=1, dash="dot"),
                ), secondary_y=False)
            fig.add_trace(go.Scatter(
                x=hist_c["date"], y=hist_c["std_chg_pct"], name="Cross-Sec Vol %",
                line=dict(color=_C["magenta"], width=2),
            ), secondary_y=True)
            fig.update_layout(**_CHART, height=320,
                              title=dict(text="Liquidity & Dispersion", font=dict(size=12)))
            fig.update_yaxes(title_text="Spread (bps)", secondary_y=False, gridcolor=_C["grid"])
            fig.update_yaxes(title_text="Dispersion %", secondary_y=True, gridcolor=_C["grid"])
            st.plotly_chart(fig, use_container_width=True)

        with col_d:
            # Tick count + symbols
            if "total_ticks" in hist_c.columns:
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(go.Scatter(
                    x=hist_c["date"], y=hist_c["total_ticks"], name="Total Ticks",
                    line=dict(color=_C["accent"], width=2),
                    fill="tozeroy", fillcolor="rgba(47,129,247,0.08)",
                ), secondary_y=False)
                fig.add_trace(go.Scatter(
                    x=hist_c["date"], y=hist_c["symbols"], name="Symbols Traded",
                    line=dict(color=_C["cyan"], width=2),
                ), secondary_y=True)
                fig.update_layout(**_CHART, height=320,
                                  title=dict(text="Data Coverage", font=dict(size=12)))
                fig.update_yaxes(title_text="Ticks", secondary_y=False, gridcolor=_C["grid"])
                fig.update_yaxes(title_text="Symbols", secondary_y=True, gridcolor=_C["grid"])
                st.plotly_chart(fig, use_container_width=True)

        # ── Regime Summary ──
        if len(hist_c) >= 5:
            st.markdown("---")
            st.markdown(f'<div style="color:{_C["dim"]};font-size:0.75em;text-transform:uppercase;'
                        f'letter-spacing:1px;margin-bottom:4px;">Regime Summary (last 5 days)</div>',
                        unsafe_allow_html=True)
            recent = hist_c.tail(5)
            avg_breadth = recent["breadth_pct"].mean()
            avg_disp = recent["std_chg_pct"].mean()
            avg_vol = recent["total_volume"].mean()
            vol_trend = "expanding" if recent["total_volume"].iloc[-1] > avg_vol else "contracting"
            breadth_regime = "bullish" if avg_breadth > 5 else "bearish" if avg_breadth < -5 else "neutral"
            vol_regime = "high" if avg_disp > 2 else "low" if avg_disp < 1 else "moderate"

            c1, c2, c3, c4 = st.columns(4)
            with c1:
                bc = _C["up"] if breadth_regime == "bullish" else _C["down"] if breadth_regime == "bearish" else _C["neutral"]
                _kpi("Breadth Regime", breadth_regime.upper(), f"avg {avg_breadth:+.1f}%", color=bc)
            with c2:
                vc = _C["down"] if vol_regime == "high" else _C["up"] if vol_regime == "low" else _C["amber"]
                _kpi("Volatility Regime", vol_regime.upper(), f"avg dispersion {avg_disp:.2f}%", color=vc)
            with c3:
                tc = _C["up"] if vol_trend == "expanding" else _C["down"]
                _kpi("Volume Trend", vol_trend.upper(), f"avg {avg_vol/1e6:.0f}M", color=tc)
            with c4:
                spread_trend = recent["median_spread_bps"].mean()
                _kpi("Avg Spread (5d)", f"{spread_trend:.1f} bps", color=_C["amber"])
    else:
        st.info("Sync multiple dates to see historical trends. Currently showing single-day snapshot only.")


# ═══════════════════════════════════════════════════════════════════════════════
# SECTION 10: TICK LOG SYNC
# ═══════════════════════════════════════════════════════════════════════════════

def _render_tick_sync():
    st.markdown("### Tick Data Sync")

    from pakfindata.db.connections import has_duckdb, duck_write
    from pakfindata.db.duckdb_manager import DUCKDB_PATH, JSONL_CLOUD_DIR, JSONL_LOCAL_DIR

    con = get_connection()
    ensure_summary_table(con)

    _use_duck = has_duckdb()

    # ── KPI row: DuckDB primary, SQLite fallback ──
    if _use_duck:
        import duckdb as _duckdb
        try:
            dcon = _duckdb.connect(str(DUCKDB_PATH), read_only=True)
            duck_tl_count = dcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
            duck_tl_dates = dcon.execute(
                "SELECT DISTINCT SUBSTR(_ts, 1, 10) AS d FROM tick_logs ORDER BY d DESC"
            ).fetchall()
            duck_first = duck_tl_dates[-1][0] if duck_tl_dates else "---"
            duck_last = duck_tl_dates[0][0] if duck_tl_dates else "---"
            duck_dates_set = {r[0] for r in duck_tl_dates}
            dcon.close()
        except Exception:
            duck_tl_count, duck_dates_set, duck_first, duck_last = 0, set(), "---", "---"

        st.caption("Primary: DuckDB  |  Fallback: SQLite")
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            _kpi("DuckDB tick_logs", f"{duck_tl_count:,}", color=_C["accent"])
        with c2:
            _kpi("Dates", f"{len(duck_dates_set)}", color=_C["cyan"])
        with c3:
            _kpi("First Date", str(duck_first), color=_C["amber"])
        with c4:
            _kpi("Latest Date", str(duck_last), color=_C["teal"])
    else:
        st.caption("DuckDB not available — using SQLite only")
        sum_stats = get_summary_stats(con)
        c1, c2, c3, c4 = st.columns(4)
        with c1:
            _kpi("Summary Dates", f"{sum_stats['dates']}", color=_C["accent"])
        with c2:
            _kpi("Summary Rows", f"{sum_stats['total_rows']:,}", color=_C["cyan"])
        with c3:
            _kpi("First Date", str(sum_stats.get("first_date") or "---"), color=_C["amber"])
        with c4:
            _kpi("Latest Date", str(sum_stats.get("last_date") or "---"), color=_C["teal"])

    # ── Daily Summary (SQLite — still needed for precomputed summaries) ──
    st.markdown("---")
    st.markdown("#### Daily Summary (tick_bars.db -> tick_daily_summary)")
    st.caption("Precomputes per-symbol daily metrics from tick_bars.db. ~25s per date, runs once.")

    avail = set(_summary_available_dates(con))
    existing = set(get_summary_dates(con))
    missing = sorted(avail - existing)

    if missing:
        st.info(f"**{len(missing)}** dates need summary computation: {', '.join(missing)}")
        if st.button(f"Compute {len(missing)} Missing Summaries", type="primary"):
            with st.spinner(f"Computing {len(missing)} date summaries..."):
                result = compute_missing_summaries(con)
                st.cache_data.clear()
            st.success(f"Computed {result['dates_computed']} dates, {result['symbols_total']} symbol-rows")
            st.rerun()
    else:
        st.success(f"All {len(existing)} dates have precomputed summaries.")

    st.markdown("---")

    # ═══════════════════════════════════════════════════════
    # SYNC tick_bars.db → DuckDB (ohlcv_5s, index tables)
    # ═══════════════════════════════════════════════════════
    st.markdown("#### Sync tick_bars.db → DuckDB (via /tmp/)")
    st.caption("Merges ohlcv_5s + index tables from tick_bars.db into DuckDB. ~10 seconds.")

    _sync_col1, _sync_col2 = st.columns([1, 1])

    with _sync_col1:
        if st.button("🔄 Sync tick_bars.db → DuckDB", type="primary", key="_sync_tickbars_duck"):
            with st.spinner("Copying to /tmp/ for fast sync..."):
                try:
                    from pakfindata.db.duckdb_manager import sync_sqlite_to_duckdb, DUCKDB_PATH as _DP
                    results = sync_sqlite_to_duckdb(
                        sqlite_path=str(TICK_BARS_DB),
                        duckdb_path=str(_DP),
                        tables=["ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"],
                    )
                    for table, added in results.items():
                        if added > 0:
                            st.success(f"✅ {table}: +{added:,} new rows")
                        else:
                            st.info(f"ℹ️ {table}: already up to date")
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    with _sync_col2:
        try:
            import duckdb as _ddb
            import sqlite3 as _sql
            from pakfindata.db.duckdb_manager import DUCKDB_PATH as _DP2
            _dc = _ddb.connect(str(_DP2), read_only=True)
            _sc = _sql.connect(str(TICK_BARS_DB))
            for t in ["ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"]:
                dc = _dc.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                sc = _sc.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                delta = sc - dc
                if delta > 0:
                    st.warning(f"{t}: DuckDB {dc:,} | SQLite {sc:,} (⚠️ {delta:,} behind)")
                else:
                    st.success(f"{t}: {dc:,} rows ✅")
            _dc.close()
            _sc.close()
        except Exception as e:
            st.caption(f"Could not compare: {e}")

    st.markdown("---")

    # ═══════════════════════════════════════════════════════
    # JSONL → DuckDB tick_logs
    # ═══════════════════════════════════════════════════════
    st.markdown("#### JSONL -> DuckDB tick_logs (primary)")
    st.caption("Import JSONL tick files directly into DuckDB — fast columnar import via read_json_auto.")

    if not _use_duck:
        st.warning("DuckDB not available. Using SQLite fallback below.")
    else:
        # Collect all JSONL files from cloud + local
        cloud_files = sorted(JSONL_CLOUD_DIR.glob("ticks_*.jsonl"), reverse=True) if JSONL_CLOUD_DIR.exists() else []
        local_files = sorted(JSONL_LOCAL_DIR.glob("ticks_*.jsonl"), reverse=True) if JSONL_LOCAL_DIR.exists() else []
        all_jsonl = {f.stem.replace("ticks_", ""): f for f in local_files}
        for f in cloud_files:
            d = f.stem.replace("ticks_", "")
            all_jsonl[d] = f  # cloud overrides local
        jsonl_dates = sorted(all_jsonl.keys(), reverse=True)

        # ── Single date import ──
        st.markdown("**Import single date**")
        if jsonl_dates:
            not_imported = [d for d in jsonl_dates if d not in duck_dates_set]
            import_choices = not_imported if not_imported else jsonl_dates
            sel_import_date = st.selectbox(
                "JSONL date", import_choices, index=0, key="_duck_import_date"
            )
            already = sel_import_date in duck_dates_set
            if already:
                st.info(f"{sel_import_date} already imported to DuckDB.")

            if st.button(
                f"Import {sel_import_date} -> DuckDB tick_logs",
                key="_duck_import_single", type="primary",
                disabled=already,
            ):
                jsonl_path = all_jsonl[sel_import_date]
                with st.spinner(f"Importing {jsonl_path.name} via read_json_auto..."):
                    try:
                        wcon = duck_write()
                        before_n = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
                        wcon.execute(f"""
                            INSERT OR IGNORE INTO tick_logs
                            SELECT
                                symbol, market, timestamp, "_ts",
                                price, "open", high, low, change,
                                "changePercent" AS change_pct,
                                CAST(volume AS BIGINT) AS volume,
                                value,
                                CAST(trades AS INTEGER) AS trades,
                                bid, ask,
                                CAST("bidVol" AS BIGINT) AS bid_vol,
                                CAST("askVol" AS BIGINT) AS ask_vol,
                                "previousClose" AS prev_close,
                                '{jsonl_path.name}' AS source_file,
                                '{datetime.now().isoformat()}' AS ingested_at
                            FROM read_json_auto('{jsonl_path}',
                                 format='newline_delimited',
                                 maximum_object_size=10485760)
                        """)
                        after_n = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
                        wcon.close()
                        st.success(f"Imported {after_n - before_n:,} ticks from {jsonl_path.name} (total: {after_n:,})")
                        st.cache_data.clear()
                        st.rerun()
                    except Exception as e:
                        st.error(f"Import failed: {e}")
        else:
            st.info("No JSONL files found in tick_logs_cloud or tick_logs directories.")

        # ── Bulk import all ──
        st.markdown("---")
        st.markdown("**Import ALL missing JSONL files**")
        missing_dates = sorted(set(jsonl_dates) - duck_dates_set)
        if not missing_dates:
            st.success(f"All {len(jsonl_dates)} JSONL dates already imported to DuckDB.")
        else:
            st.info(f"**{len(missing_dates)}** dates not yet in DuckDB: {', '.join(missing_dates[:5])}{'...' if len(missing_dates) > 5 else ''}")
            if st.button(
                f"Import {len(missing_dates)} Missing Dates -> DuckDB",
                key="_duck_import_all", type="primary",
            ):
                progress_bar = st.progress(0, text="Starting bulk import...")
                imported_total = 0
                for idx, d in enumerate(missing_dates):
                    jsonl_path = all_jsonl[d]
                    progress_bar.progress(
                        (idx + 1) / len(missing_dates),
                        text=f"Importing {d} ({idx + 1}/{len(missing_dates)})..."
                    )
                    try:
                        wcon = duck_write()
                        before_n = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
                        wcon.execute(f"""
                            INSERT OR IGNORE INTO tick_logs
                            SELECT
                                symbol, market, timestamp, "_ts",
                                price, "open", high, low, change,
                                "changePercent" AS change_pct,
                                CAST(volume AS BIGINT) AS volume,
                                value,
                                CAST(trades AS INTEGER) AS trades,
                                bid, ask,
                                CAST("bidVol" AS BIGINT) AS bid_vol,
                                CAST("askVol" AS BIGINT) AS ask_vol,
                                "previousClose" AS prev_close,
                                '{jsonl_path.name}' AS source_file,
                                '{datetime.now().isoformat()}' AS ingested_at
                            FROM read_json_auto('{jsonl_path}',
                                 format='newline_delimited',
                                 maximum_object_size=10485760)
                        """)
                        after_n = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
                        wcon.close()
                        imported_total += (after_n - before_n)
                    except Exception as e:
                        st.warning(f"Failed {d}: {e}")

                progress_bar.progress(1.0, text="Done!")
                st.success(f"Bulk import complete: {imported_total:,} ticks from {len(missing_dates)} dates")
                st.cache_data.clear()
                st.rerun()

    st.markdown("---")

    # ═══════════════════════════════════════════════════════
    # FULL NIGHTLY SYNC (one-click)
    # ═══════════════════════════════════════════════════════
    st.markdown("#### 🚀 Full Nightly Sync")
    st.caption("tick_bars.db → DuckDB + ALL JSONL → DuckDB in one click")

    if st.button("🚀 Run Full Nightly Sync", type="primary", key="_full_nightly_sync"):
        _nightly_progress = st.progress(0, text="Starting...")

        # Part 1: tick_bars.db → DuckDB
        _nightly_progress.progress(10, text="Syncing tick_bars.db → DuckDB (ohlcv_5s, index)...")
        try:
            from pakfindata.db.duckdb_manager import sync_sqlite_to_duckdb, DUCKDB_PATH as _NDP
            tb_results = sync_sqlite_to_duckdb(
                sqlite_path=str(TICK_BARS_DB),
                duckdb_path=str(_NDP),
                tables=["ohlcv_5s", "index_ohlcv_5s", "index_raw_ticks"],
            )
            for tbl, added in tb_results.items():
                st.write(f"  {tbl}: +{added:,}")
        except Exception as e:
            st.warning(f"tick_bars.db sync: {e}")

        # Part 2: JSONL → DuckDB tick_logs (import all missing)
        _nightly_progress.progress(40, text="Importing JSONL → DuckDB tick_logs...")
        try:
            from pakfindata.db.connections import has_duckdb as _hd
            from pakfindata.db.duckdb_manager import DUCKDB_PATH as _NDP2, JSONL_CLOUD_DIR as _JC, JSONL_LOCAL_DIR as _JL
            import duckdb as _duckdb
            if _hd():
                # Collect JSONL files
                cloud_files = sorted(_JC.glob("ticks_*.jsonl"), reverse=True) if _JC.exists() else []
                local_files = sorted(_JL.glob("ticks_*.jsonl"), reverse=True) if _JL.exists() else []
                all_jsonl = {f.stem.replace("ticks_", ""): f for f in local_files}
                for f in cloud_files:
                    d = f.stem.replace("ticks_", "")
                    all_jsonl[d] = f

                # Close cached read-only connection to allow read-write
                try:
                    from pakfindata.db.connections import _duck_con
                    _duck_con().close()
                    _duck_con.clear()
                except Exception:
                    pass
                wcon = _duckdb.connect(str(_NDP2))
                duck_dates = set()
                try:
                    duck_dates = set(
                        r[0]
                        for r in wcon.execute(
                            "SELECT DISTINCT SUBSTR(source_file, 7, 10) "
                            "FROM (SELECT REPLACE(source_file, '.jsonl', '') AS source_file FROM tick_logs)"
                        ).fetchall()
                    )
                except Exception:
                    pass

                missing = sorted(set(all_jsonl.keys()) - duck_dates)
                imported_n = 0
                for i, d in enumerate(missing):
                    pct = 40 + int(50 * (i + 1) / max(len(missing), 1))
                    _nightly_progress.progress(pct, text=f"Importing {d}...")
                    jpath = all_jsonl[d]
                    try:
                        before = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
                        wcon.execute(f"""
                            INSERT OR IGNORE INTO tick_logs
                            SELECT
                                symbol, market, timestamp, "_ts",
                                price, "open", high, low, change,
                                "changePercent" AS change_pct,
                                CAST(volume AS BIGINT) AS volume,
                                value,
                                CAST(trades AS INTEGER) AS trades,
                                bid, ask,
                                CAST("bidVol" AS BIGINT) AS bid_vol,
                                CAST("askVol" AS BIGINT) AS ask_vol,
                                "previousClose" AS prev_close,
                                '{jpath.name}' AS source_file,
                                '{datetime.now().isoformat()}' AS ingested_at
                            FROM read_json_auto('{jpath}',
                                 format='newline_delimited', maximum_object_size=10485760)
                        """)
                        after = wcon.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
                        imported_n += after - before
                    except Exception:
                        pass
                wcon.close()
                st.write(f"  tick_logs: +{imported_n:,} from {len(missing)} dates")
        except Exception as e:
            st.warning(f"JSONL import: {e}")

        _nightly_progress.progress(100, text="✅ Full nightly sync complete!")
        st.cache_data.clear()

    st.markdown("---")

    # ═══════════════════════════════════════════════════════
    # FALLBACK: SQLite tick_logs sync (legacy — disabled)
    # ═══════════════════════════════════════════════════════
    with st.expander("JSONL -> SQLite tick_logs (legacy — slow, use DuckDB above)", expanded=False):
        st.caption("SQLite tick_logs is deprecated. Use DuckDB import above for fast JSONL ingestion.")
        files_on_disk = sorted(TICK_LOG_DIR.glob("ticks_*.jsonl"), reverse=True)
        if not files_on_disk:
            st.warning("No JSONL files found in `/mnt/e/psxdata/tick_logs/`")
        else:
            st.markdown(f"**{len(files_on_disk)} file(s) on disk** — latest: `{files_on_disk[0].name}`")

            if st.button("Sync Latest File (SQLite)", type="secondary", use_container_width=True):
                with st.spinner(f"Syncing {files_on_disk[0].name}..."):
                    result = sync_latest_file(con)
                    st.cache_data.clear()
                if result["status"] == "ok":
                    st.success(f"Synced **{result['ticks_synced']:,}** ticks from `{result['file']}`")
                    st.rerun()
                else:
                    st.error(f"Sync failed: {result['status']}")


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═══════════════════════════════════════════════════════════════════════════════

def _render_intraday_controls_and_tabs(dates: list[str]):
    """Render date/symbol/freq controls and all intraday analysis tabs."""
    col_date, col_sym, col_freq = st.columns([1, 1, 1])

    with col_date:
        sel_date = st.selectbox("Trading Date", dates, index=0)

    df = _load_ticks(sel_date)
    if df.empty:
        st.error(f"No data in tick log for {sel_date}.")
        return

    if "market" in df.columns:
        reg_df = df[df["market"] == "REG"]
    else:
        reg_df = df
    reg_symbols = sorted(reg_df["symbol"].unique())
    # Default to highest volume symbol
    if len(reg_df) > 0 and "volume" in reg_df.columns:
        top_sym = reg_df.nlargest(1, "volume")["symbol"].iloc[0]
    else:
        top_sym = reg_symbols[0] if reg_symbols else ""

    with col_sym:
        sel_sym = st.selectbox("Symbol", reg_symbols,
                               index=reg_symbols.index(top_sym) if top_sym in reg_symbols else 0)

    with col_freq:
        sel_freq = st.selectbox("Bar Frequency", ["1min", "5min", "15min", "30min"], index=0)

    # Data info bar
    ts_range = ""
    if "first_ts" in df.columns and not df.empty:
        ft = str(df["first_ts"].iloc[0] or "")
        lt = str(df["last_ts"].iloc[0] or "")
        if ft and lt:
            ts_range = f" · {ft[11:16]} → {lt[11:16]} PKT"
    st.markdown(
        f'<div style="background:{_C["card"]};padding:8px 14px;border-radius:4px;'
        f'font-family:monospace;font-size:0.8em;color:{_C["dim"]};">'
        f'{len(df):,} symbols · src: tick_daily_summary'
        f'{ts_range}'
        f'</div>',
        unsafe_allow_html=True,
    )

    # Intraday sub-tabs — only render the active one
    SUB_TABS = [
        "Market Snapshot", "Price & Returns", "Volume Profile",
        "Spread & Liquidity", "Volatility", "Correlations",
        "Market Flow", "Futures Basis",
    ]
    active_sub = st.radio(
        "Analysis", SUB_TABS,
        index=SUB_TABS.index(st.session_state.get("_tick_sub", "Market Snapshot")),
        horizontal=True, label_visibility="collapsed",
        key="_tick_sub_radio",
    )
    st.session_state["_tick_sub"] = active_sub

    if active_sub == "Market Snapshot":
        _render_market_overview(df)
    else:
        # All other tabs need 5-second bar data (not the summary)
        bars_df = _load_bars(sel_date)
        if bars_df.empty:
            st.warning(f"No 5-second bar data for {sel_date}. Only Market Snapshot is available from the summary.")
            return
        if active_sub == "Price & Returns":
            _render_price_returns(bars_df, sel_sym, sel_freq)
        elif active_sub == "Volume Profile":
            _render_volume_profile(bars_df, sel_sym)
        elif active_sub == "Spread & Liquidity":
            _render_spread_liquidity(bars_df, sel_sym)
        elif active_sub == "Volatility":
            _render_volatility(bars_df, sel_sym, sel_freq)
        elif active_sub == "Correlations":
            _render_correlation(bars_df)
        elif active_sub == "Market Flow":
            _render_market_flow(bars_df)
        elif active_sub == "Futures Basis":
            _render_futures_basis(bars_df)


def render_tick_analytics():
    """Main entry point — only renders the active tab to keep the page fast."""
    st.markdown("## Tick Analytics Terminal")
    st.caption("Quant-grade intraday microstructure · Volume profile · Volatility · Flow analysis")

    TAB_NAMES = ["Overview", "Intraday Analytics", "Sync"]
    active = st.radio(
        "Section", TAB_NAMES,
        index=TAB_NAMES.index(st.session_state.get("_tick_tab", "Overview")),
        horizontal=True, label_visibility="collapsed",
        key="_tick_tab_radio",
    )
    st.session_state["_tick_tab"] = active
    st.markdown("---")

    if active == "Sync":
        _render_tick_sync()
    elif active == "Overview":
        _lazy_imports()
        dates = _available_dates()
        if not dates:
            st.info("No tick data. Use the **Sync** section to import JSONL files.")
        else:
            _render_quant_overview(dates[0])
    elif active == "Intraday Analytics":
        _lazy_imports()
        dates = _available_dates()
        if not dates:
            st.error("No tick data found in database or JSONL files.")
            st.info("Use the **Sync** section to import JSONL files, or start the tick service: `pakfindata tick start`")
        else:
            _render_intraday_controls_and_tabs(dates)

    render_footer()
