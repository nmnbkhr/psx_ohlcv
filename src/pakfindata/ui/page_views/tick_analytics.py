"""Tick Analytics Terminal — Quant-grade market microstructure from JSONL tick logs.

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
    get_synced_files,
    get_tick_logs_stats,
    ensure_tick_logs_table,
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


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING (DB-first, JSONL fallback)
# ═══════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=60, show_spinner=False)
def _available_dates() -> list[str]:
    """List available dates from tick_logs source_file index + JSONL files on disk."""
    db_dates: set[str] = set()
    try:
        con = get_connection()
        ensure_tick_logs_table(con)
        # Fast: uses idx_tick_logs_source index instead of scanning all rows
        rows = con.execute(
            "SELECT DISTINCT source_file FROM tick_logs"
        ).fetchall()
        db_dates = {
            r[0].replace("ticks_", "").replace(".jsonl", "")
            for r in rows if r[0]
        }
    except Exception:
        pass
    # Also include JSONL-only dates not yet synced
    file_dates = {
        f.stem.replace("ticks_", "")
        for f in TICK_LOG_DIR.glob("ticks_*.jsonl")
    }
    return sorted(db_dates | file_dates, reverse=True)


def _enrich_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add computed columns to a tick DataFrame."""
    if df.empty:
        return df
    if "_ts" in df.columns:
        df["_ts"] = pd.to_datetime(df["_ts"], utc=True).dt.tz_convert("Asia/Karachi")
    df["spread"] = df["ask"] - df["bid"]
    df["spread_bps"] = np.where(
        df["price"] > 0, df["spread"] / df["price"] * 10_000, 0
    )
    df["mid"] = (df["bid"] + df["ask"]) / 2
    return df


@st.cache_data(ttl=120, show_spinner="Loading tick data…")
def _load_ticks(date_str: str) -> pd.DataFrame:
    """Load ticks from DB first, falling back to JSONL file."""
    source_file = f"ticks_{date_str}.jsonl"

    # Try DB — query by indexed source_file column (fast)
    try:
        con = get_connection()
        ensure_tick_logs_table(con)
        df = pd.read_sql_query(
            """SELECT symbol, market, timestamp, _ts, price, open, high, low,
                      change, change_pct AS "changePercent",
                      volume, value, trades,
                      bid, ask, bid_vol AS "bidVol", ask_vol AS "askVol",
                      prev_close AS "previousClose"
               FROM tick_logs
               WHERE source_file = ?
               ORDER BY timestamp""",
            con,
            params=(source_file,),
        )
        if not df.empty:
            return _enrich_df(df)
    except Exception:
        pass

    # Fallback: JSONL file
    path = TICK_LOG_DIR / source_file
    if not path.exists():
        return pd.DataFrame()
    records = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    continue
    df = pd.DataFrame(records)
    return _enrich_df(df)


def _last_tick_per_symbol(df: pd.DataFrame, market: str = "REG") -> pd.DataFrame:
    """Get the final tick for each symbol in a given market."""
    mdf = df[df["market"] == market]
    return mdf.drop_duplicates("symbol", keep="last").copy()


def _build_ohlcv_bars(df: pd.DataFrame, symbol: str, freq: str = "1min") -> pd.DataFrame:
    """Build OHLCV bars for a single symbol from raw ticks."""
    sdf = df[(df["symbol"] == symbol) & (df["market"] == "REG")].copy()
    if sdf.empty:
        return pd.DataFrame()
    sdf = sdf.set_index("_ts")
    bars = sdf["price"].resample(freq).ohlc()
    bars.columns = ["open", "high", "low", "close"]
    bars["volume"] = sdf["volume"].resample(freq).last().diff().fillna(0).clip(lower=0)
    bars["trades"] = sdf["trades"].resample(freq).last().diff().fillna(0).clip(lower=0)
    bars["vwap"] = np.where(
        bars["volume"] > 0,
        (sdf["value"].resample(freq).last().diff().fillna(0)) / bars["volume"].replace(0, np.nan),
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
    """Compute per-date market-wide stats from tick_logs (all history)."""
    try:
        con = get_connection()
        ensure_tick_logs_table(con)
        df = pd.read_sql_query(
            """
            SELECT
                REPLACE(REPLACE(source_file, 'ticks_', ''), '.jsonl', '') AS date,
                COUNT(*)                          AS total_ticks,
                COUNT(DISTINCT symbol)            AS symbols,
                SUM(CASE WHEN market='REG' THEN 1 ELSE 0 END) AS reg_ticks,
                COUNT(DISTINCT CASE WHEN market='FUT' THEN symbol END) AS fut_symbols,
                1                                 AS files
            FROM tick_logs
            GROUP BY source_file
            ORDER BY date
            """,
            con,
        )
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=300, show_spinner="Loading daily snapshots…")
def _build_daily_breadth() -> pd.DataFrame:
    """Per-date breadth / volume / spread stats from last tick per symbol per day."""
    try:
        con = get_connection()
        ensure_tick_logs_table(con)
        # Get last tick per symbol per file (REG market only)
        # Uses source_file + (symbol, market, timestamp) indices
        df = pd.read_sql_query(
            """
            WITH ranked AS (
                SELECT *,
                       REPLACE(REPLACE(source_file, 'ticks_', ''), '.jsonl', '') AS date,
                       ROW_NUMBER() OVER (
                           PARTITION BY symbol, source_file
                           ORDER BY timestamp DESC
                       ) AS rn
                FROM tick_logs
                WHERE market = 'REG'
            )
            SELECT date, symbol, price, open, high, low,
                   change, change_pct, volume, value, trades,
                   bid, ask, bid_vol, ask_vol, prev_close
            FROM ranked WHERE rn = 1
            """,
            con,
        )
        if df.empty:
            return pd.DataFrame()

        df["spread_bps"] = np.where(
            df["price"] > 0, (df["ask"] - df["bid"]) / df["price"] * 10_000, 0
        )
        df["intraday_range_pct"] = np.where(
            df["low"] > 0, (df["high"] - df["low"]) / df["low"] * 100, 0
        )

        daily = df.groupby("date").agg(
            symbols=("symbol", "nunique"),
            advancers=("change_pct", lambda x: (x > 0).sum()),
            decliners=("change_pct", lambda x: (x < 0).sum()),
            unchanged=("change_pct", lambda x: (x == 0).sum()),
            total_volume=("volume", "sum"),
            total_turnover=("value", "sum"),
            total_trades=("trades", "sum"),
            median_spread_bps=("spread_bps", "median"),
            mean_spread_bps=("spread_bps", "mean"),
            p90_spread_bps=("spread_bps", lambda x: np.percentile(x, 90) if len(x) > 0 else 0),
            median_range_pct=("intraday_range_pct", "median"),
            mean_chg_pct=("change_pct", "mean"),
            std_chg_pct=("change_pct", "std"),
            max_gain=("change_pct", "max"),
            max_loss=("change_pct", "min"),
            avg_depth=("bid_vol", lambda x: ((x + df.loc[x.index, "ask_vol"]) / 2).median()),
        ).reset_index()

        daily["ad_ratio"] = daily["advancers"] / daily["decliners"].replace(0, 1)
        daily["breadth_pct"] = (daily["advancers"] - daily["decliners"]) / daily["symbols"] * 100

        return daily.sort_values("date")
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=120, show_spinner=False)
def _load_today_snapshot(source_file: str) -> dict:
    """Compute today's overview KPIs in SQL — no full tick load needed."""
    defaults = dict(
        total_ticks=0, symbols_traded=0, adv=0, dec=0, unch=0,
        total_vol=0, total_val=0, total_trades=0, fut_count=0,
        med_spread=0, p90_spread=0, mean_chg=0, cross_sec_vol=0,
        max_gain=0, max_loss=0, avg_depth=0, top10_conc=0,
        tick_rate=0,
    )
    try:
        con = get_connection()
        # Total ticks + time range for tick rate
        meta = con.execute(
            "SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM tick_logs WHERE source_file = ?",
            (source_file,),
        ).fetchone()
        total_ticks = meta[0]
        duration_min = max((meta[2] - meta[1]) / 60, 1) if meta[1] and meta[2] else 1

        # Last tick per symbol (REG only) — lightweight via MAX(timestamp)
        rows = con.execute(
            """SELECT t.symbol, t.price, t.change_pct, t.volume, t.value, t.trades,
                      t.bid, t.ask, t.bid_vol, t.ask_vol
               FROM tick_logs t
               INNER JOIN (
                   SELECT symbol, MAX(timestamp) as max_ts
                   FROM tick_logs
                   WHERE source_file = ? AND market = 'REG'
                   GROUP BY symbol
               ) latest ON t.symbol = latest.symbol AND t.timestamp = latest.max_ts
                       AND t.source_file = ? AND t.market = 'REG'""",
            (source_file, source_file),
        ).fetchall()

        if not rows:
            return defaults

        active = [r for r in rows if r[3] > 0]  # volume > 0
        if not active:
            return defaults

        chg_pcts = [r[2] for r in active]
        volumes = [r[3] for r in active]
        values = [r[4] for r in active]
        trades_list = [r[5] for r in active]
        spreads_bps = [
            (r[7] - r[6]) / r[1] * 10_000 if r[1] and r[1] > 0 else 0
            for r in active
        ]
        depths = [(r[8] + r[9]) / 2 for r in active]

        adv = sum(1 for c in chg_pcts if c > 0)
        dec_ = sum(1 for c in chg_pcts if c < 0)
        unch = sum(1 for c in chg_pcts if c == 0)
        total_val = sum(values)

        # Top-10 concentration
        top10_val = sum(sorted(values, reverse=True)[:10])

        sorted_spreads = sorted(spreads_bps)
        p90_idx = int(len(sorted_spreads) * 0.9)

        fut_count = con.execute(
            "SELECT COUNT(DISTINCT symbol) FROM tick_logs WHERE source_file = ? AND market = 'FUT'",
            (source_file,),
        ).fetchone()[0]

        return dict(
            total_ticks=total_ticks,
            symbols_traded=len(active),
            adv=adv, dec=dec_, unch=unch,
            total_vol=sum(volumes),
            total_val=total_val,
            total_trades=sum(trades_list),
            fut_count=fut_count,
            med_spread=statistics.median(spreads_bps) if spreads_bps else 0,
            p90_spread=sorted_spreads[p90_idx] if sorted_spreads else 0,
            mean_chg=statistics.mean(chg_pcts) if chg_pcts else 0,
            cross_sec_vol=statistics.stdev(chg_pcts) if len(chg_pcts) > 1 else 0,
            max_gain=max(chg_pcts) if chg_pcts else 0,
            max_loss=min(chg_pcts) if chg_pcts else 0,
            avg_depth=statistics.median(depths) if depths else 0,
            top10_conc=top10_val / max(total_val, 1) * 100,
            tick_rate=total_ticks / duration_min,
        )
    except Exception:
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
    st.markdown("### Tick Log Sync")
    st.caption("Sync JSONL tick files from `/mnt/e/psxdata/tick_logs/` into the database")

    con = get_connection()

    # Stats row
    stats = get_tick_logs_stats(con)
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        _kpi("Total Ticks", f"{stats['total_ticks']:,}", color=_C["accent"])
    with c2:
        _kpi("Symbols", f"{stats['symbols']:,}", color=_C["cyan"])
    with c3:
        _kpi("Files Synced", f"{stats['files']:,}", color=_C["amber"])
    with c4:
        last = stats.get("last_tick") or "—"
        _kpi("Latest Tick", str(last)[:19] if last != "—" else "—", color=_C["teal"])

    st.markdown("---")

    # Available files on disk
    files_on_disk = sorted(TICK_LOG_DIR.glob("ticks_*.jsonl"), reverse=True)
    if not files_on_disk:
        st.warning("No JSONL files found in `/mnt/e/psxdata/tick_logs/`")
        return

    st.markdown(f"**{len(files_on_disk)} file(s) on disk** — latest: `{files_on_disk[0].name}`")

    # Actions
    col_sync, col_backfill = st.columns(2)

    with col_sync:
        st.markdown(f'<div style="background:{_C["card"]};padding:16px;border-radius:6px;'
                    f'border-left:3px solid {_C["accent"]};font-family:monospace;">'
                    f'<div style="color:{_C["text"]};font-size:1.1em;font-weight:700;">Sync Latest</div>'
                    f'<div style="color:{_C["dim"]};font-size:0.85em;">Upsert the most recent file: '
                    f'<code>{files_on_disk[0].name}</code></div></div>',
                    unsafe_allow_html=True)
        if st.button("Sync Latest File", type="primary", use_container_width=True):
            with st.spinner(f"Syncing {files_on_disk[0].name}..."):
                result = sync_latest_file(con)
                st.cache_data.clear()
            if result["status"] == "ok":
                st.success(f"Synced **{result['ticks_synced']:,}** ticks from `{result['file']}`")
                st.rerun()
            else:
                st.error(f"Sync failed: {result['status']}")

    with col_backfill:
        # Determine unsynced files
        try:
            existing = set(
                row[0]
                for row in con.execute(
                    "SELECT DISTINCT source_file FROM tick_logs"
                ).fetchall()
            )
        except Exception:
            existing = set()

        unsynced = [f for f in files_on_disk if f.name not in existing]
        n_unsynced = len(unsynced)
        bf_status = get_backfill_status()

        st.markdown(f'<div style="background:{_C["card"]};padding:16px;border-radius:6px;'
                    f'border-left:3px solid {_C["amber"]};font-family:monospace;">'
                    f'<div style="color:{_C["text"]};font-size:1.1em;font-weight:700;">Backfill All</div>'
                    f'<div style="color:{_C["dim"]};font-size:0.85em;">'
                    f'{n_unsynced} unsynced / {len(files_on_disk)} total files '
                    f'(background · non-blocking)</div></div>',
                    unsafe_allow_html=True)

        if bf_status.get("running"):
            # Show live progress — auto-refresh every 2s
            done = bf_status.get("files_done", 0)
            total = bf_status.get("files_total", 1)
            cur = bf_status.get("current_file", "")
            ticks = bf_status.get("ticks_inserted", 0)
            st.progress(
                done / max(total, 1),
                text=f"Inserting {cur} ({done}/{total} files · {ticks:,} ticks)"
            )
            import time
            time.sleep(2)
            st.rerun()
        elif n_unsynced == 0:
            if bf_status.get("files_done", 0) > 0:
                st.success(
                    f"Backfill complete: {bf_status['files_done']} files, "
                    f"{bf_status.get('ticks_inserted', 0):,} ticks inserted"
                )
            else:
                st.info("All files already synced.")
        elif st.button(f"Backfill {n_unsynced} Unsynced Files", type="secondary", use_container_width=True):
            backfill_background(unsynced)
            st.rerun()

    # ── Clear table ──
    st.markdown("---")
    with st.expander("Danger Zone — Clear tick_logs table", expanded=False):
        st.warning("This will **DELETE ALL rows** from tick_logs. You can refill using Backfill above.")
        if st.button("Clear tick_logs table", type="secondary"):
            st.session_state["_tick_clear_confirm"] = True
        if st.session_state.get("_tick_clear_confirm"):
            st.error("Are you sure? This cannot be undone.")
            c_yes, c_no = st.columns(2)
            with c_yes:
                if st.button("Yes — DELETE ALL", type="primary"):
                    con.execute("DELETE FROM tick_logs")
                    con.commit()
                    st.session_state.pop("_tick_clear_confirm", None)
                    st.success("tick_logs table cleared.")
                    st.rerun()
            with c_no:
                if st.button("Cancel"):
                    st.session_state.pop("_tick_clear_confirm", None)
                    st.rerun()

    # Synced files table
    st.markdown("---")
    st.markdown("**Synced Files**")
    synced_df = get_synced_files(con)
    if synced_df.empty:
        st.info("No files synced yet. Click **Backfill** above to import all dates.")
    else:
        synced_df.columns = ["File", "Ticks", "Symbols", "First Tick", "Last Tick"]
        st.dataframe(synced_df, hide_index=True, use_container_width=True)


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

    reg_symbols = sorted(df[df["market"] == "REG"]["symbol"].unique())
    top_sym = df[df["market"] == "REG"]["symbol"].value_counts().index[0] if reg_symbols else ""

    with col_sym:
        sel_sym = st.selectbox("Symbol", reg_symbols,
                               index=reg_symbols.index(top_sym) if top_sym in reg_symbols else 0)

    with col_freq:
        sel_freq = st.selectbox("Bar Frequency", ["1min", "5min", "15min", "30min"], index=0)

    # Data info bar
    fpath = TICK_LOG_DIR / f"ticks_{sel_date}.jsonl"
    fsize_str = ""
    if fpath.exists():
        fsize_str = f" · {fpath.stat().st_size / (1024 * 1024):.1f} MB"
    source_tag = "DB" if "source_file" not in df.columns else "JSONL"
    ts_range = f"{df['_ts'].min().strftime('%H:%M')} → {df['_ts'].max().strftime('%H:%M')} PKT"
    st.markdown(
        f'<div style="background:{_C["card"]};padding:8px 14px;border-radius:4px;'
        f'font-family:monospace;font-size:0.8em;color:{_C["dim"]};">'
        f'{len(df):,} ticks · {df["symbol"].nunique()} symbols'
        f'{fsize_str} · {ts_range} · src: {source_tag}'
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
    elif active_sub == "Price & Returns":
        _render_price_returns(df, sel_sym, sel_freq)
    elif active_sub == "Volume Profile":
        _render_volume_profile(df, sel_sym)
    elif active_sub == "Spread & Liquidity":
        _render_spread_liquidity(df, sel_sym)
    elif active_sub == "Volatility":
        _render_volatility(df, sel_sym, sel_freq)
    elif active_sub == "Correlations":
        _render_correlation(df)
    elif active_sub == "Market Flow":
        _render_market_flow(df)
    elif active_sub == "Futures Basis":
        _render_futures_basis(df)


def render_tick_analytics():
    """Main entry point — only renders the active tab to keep the page fast."""
    st.markdown("## Tick Analytics Terminal")
    st.caption("Quant-grade intraday microstructure · Volume profile · Volatility · Flow analysis")

    TAB_NAMES = ["Overview", "Intraday Analytics", "Sync"]
    active = st.radio(
        "Section", TAB_NAMES,
        index=TAB_NAMES.index(st.session_state.get("_tick_tab", "Sync")),
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
