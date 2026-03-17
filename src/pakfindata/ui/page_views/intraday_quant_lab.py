"""Intraday Quant Lab — Alpha-grade analytics from CSV tick & OHLCV files.

Tabs
────
  Volume Profile    — TPO market-profile, POC, value-area, volume-at-price
  Order Flow        — Tick-rule cum-delta, buy/sell imbalance, block detection
  ORB Scanner       — Opening-range-breakout stats for all symbols
  Multi-TF Confluence — 5m/15m/1h/1d trend alignment heatmap
  VWAP Terminal     — Real-time VWAP ± sigma bands, deviation ranking
  Volatility Regime — Intraday vol patterns, regime classification

Data source: ~/psxdata/intraday/*.csv  (raw CSVs, no DB dependency)
"""

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, time as dtime

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.ui.components.helpers import render_footer
from pakfindata.engine.commentary import (
    get_volume_profile_commentary,
    get_order_flow_commentary,
    get_orb_commentary,
    get_vwap_commentary,
    get_vol_regime_commentary,
    get_mtf_confluence_commentary,
    OLLAMA_MODEL_FAST,
    OLLAMA_MODEL_DEEP,
)

# ═══════════════════════════════════════════════════════════════════════════════
# DESIGN TOKENS
# ═══════════════════════════════════════════════════════════════════════════════

_C = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#2F81F7", "accent2": "#00D4AA", "warning": "#FFD600",
    "poc": "#FFD600", "vah": "#FF9800", "val": "#29B6F6",
    "vwap": "#AB47BC", "buy": "#00E676", "sell": "#FF5252",
    "bg": "#0B0E11", "card": "#12161C", "grid": "#1e2430",
    "text": "#EAECEF", "dim": "#6B7280",
    "band1": "rgba(171,71,188,0.18)", "band2": "rgba(171,71,188,0.10)",
    "band3": "rgba(171,71,188,0.05)",
    "bull_trend": "#00E676", "bear_trend": "#FF5252", "flat_trend": "#78909C",
}

_FONT = "JetBrains Mono, ui-monospace, SFMono-Regular, monospace"

_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_C["text"], size=11, family=_FONT),
    xaxis=dict(gridcolor=_C["grid"], zeroline=False),
    yaxis=dict(gridcolor=_C["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)", font=dict(size=10)),
    margin=dict(l=10, r=10, t=40, b=10),
)

_PAGE_CSS = """
<style>
.quant-kpi {
    background: #12161C; border: 1px solid #1e2430; border-radius: 4px;
    padding: 12px 16px; text-align: center;
}
.quant-kpi .val { font-size: 1.6em; font-weight: 700; color: #EAECEF; }
.quant-kpi .lbl { font-size: 0.72em; color: #6B7280; letter-spacing: 1px; text-transform: uppercase; }
.trend-bull { color: #00E676; font-weight: 700; }
.trend-bear { color: #FF5252; font-weight: 700; }
.trend-flat { color: #78909C; font-weight: 700; }
.orb-hit { color: #00E676; }
.orb-miss { color: #FF5252; }
</style>
"""


def _fig(height=400, **kw):
    return go.Figure(layout={**_LAYOUT, "height": height, **kw})


def _subfig(rows, cols, shared_xaxes=True, vertical_spacing=0.06,
            row_heights=None, column_widths=None, **kw):
    fig = make_subplots(
        rows=rows, cols=cols, shared_xaxes=shared_xaxes,
        vertical_spacing=vertical_spacing, row_heights=row_heights,
        column_widths=column_widths,
    )
    fig.update_layout(**{**_LAYOUT, **kw})
    return fig


def _kpi_html(label: str, value: str, color: str = _C["text"]) -> str:
    return (
        f'<div class="quant-kpi">'
        f'<div class="val" style="color:{color}">{value}</div>'
        f'<div class="lbl">{label}</div></div>'
    )


def _render_ai_commentary(key: str, commentary_fn, **kwargs):
    """Render an AI commentary section with a generate button.

    Parameters
    ----------
    key : unique session state key for this commentary
    commentary_fn : callable from engine.commentary that returns str
    **kwargs : arguments to pass to commentary_fn
    """
    ss_key = f"quant_ai_{key}"
    selected_model = st.session_state.get("ql_selected_model", OLLAMA_MODEL_FAST)
    if st.button("Generate AI Commentary", key=f"btn_{key}",
                 type="secondary", use_container_width=False):
        with st.spinner(f"Querying {selected_model.split(':')[0]}..."):
            result = commentary_fn(model=selected_model, **kwargs)
            st.session_state[ss_key] = result

    if ss_key in st.session_state and st.session_state[ss_key]:
        txt = st.session_state[ss_key]
        if txt.startswith("Ollama Error"):
            st.warning(txt)
        else:
            st.markdown(
                f'<div style="background:#12161C;border-left:3px solid {_C["accent"]};'
                f'padding:12px 16px;border-radius:4px;margin:8px 0;'
                f'font-size:0.9em;color:{_C["text"]}">{txt}</div>',
                unsafe_allow_html=True,
            )


# ═══════════════════════════════════════════════════════════════════════════════
# DATA LOADING  (CSV → DataFrame, cached)
# ═══════════════════════════════════════════════════════════════════════════════

_DATA_DIR = Path(os.path.expanduser("~/psxdata/intraday"))


@st.cache_data(ttl=300, show_spinner=False)
def _load_ticks() -> pd.DataFrame:
    """Load latest DPS tick file."""
    files = sorted(_DATA_DIR.glob("dps_ticks_*.csv"), reverse=True)
    if not files:
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df


@st.cache_data(ttl=300, show_spinner=False)
def _load_ohlcv(timeframe: str) -> pd.DataFrame:
    """Load OHLCV bars for a given timeframe (1m, 5m, 15m, 1h, 1d, 1w)."""
    if timeframe == "1m":
        # Try today's file first, then the previous
        files = sorted(_DATA_DIR.glob("*_1m.csv"), reverse=True)
    elif timeframe == "1d":
        files = sorted(_DATA_DIR.glob("backfill_1d.csv"))
    else:
        files = sorted(_DATA_DIR.glob(f"psxt_backfill_{timeframe}.csv"))
    if not files:
        return pd.DataFrame()
    df = pd.read_csv(files[0])
    dt_col = "datetime" if "datetime" in df.columns else "date"
    df["dt"] = pd.to_datetime(df[dt_col])
    for c in ("open", "high", "low", "close", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


@st.cache_data(ttl=300, show_spinner=False)
def _load_daily_eod() -> pd.DataFrame:
    """Load DPS EOD daily history (long history, 5+ years)."""
    f = _DATA_DIR / "dps_eod_daily.csv"
    if not f.exists():
        return pd.DataFrame()
    df = pd.read_csv(f)
    df["date"] = pd.to_datetime(df["date"])
    for c in ("open", "high", "low", "close", "volume"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _get_symbols(df: pd.DataFrame) -> list[str]:
    """Get sorted unique symbols from a dataframe."""
    return sorted(df["symbol"].dropna().unique().tolist())


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 1: VOLUME PROFILE  (Market Profile / TPO)
# ═══════════════════════════════════════════════════════════════════════════════

def _render_volume_profile(ticks: pd.DataFrame, ohlcv_1m: pd.DataFrame):
    """TPO-style volume-at-price profile with POC, VAH, VAL."""
    st.markdown("### Volume Profile")
    st.caption("Volume-at-Price distribution — POC (yellow), Value Area High/Low (orange/blue)")

    # Pick data source — prefer ticks for granularity
    source = ticks if not ticks.empty else ohlcv_1m
    if source.empty:
        st.warning("No tick or 1m data available.")
        return

    symbols = _get_symbols(source)
    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        sym = st.selectbox("Symbol", symbols, key="vp_sym",
                           index=symbols.index("LUCK") if "LUCK" in symbols else 0)
    with c2:
        n_bins = st.slider("Price bins", 20, 100, 50, key="vp_bins")
    with c3:
        va_pct = st.slider("Value Area %", 50, 90, 70, key="vp_va")

    sdf = source[source["symbol"] == sym].copy()
    if sdf.empty:
        st.info(f"No data for {sym}")
        return

    # Build price/volume arrays
    if "price" in sdf.columns:
        prices = sdf["price"].values
        volumes = sdf["volume"].values
    else:
        prices = sdf["close"].values
        volumes = sdf["volume"].values

    mask = np.isfinite(prices) & np.isfinite(volumes)
    prices, volumes = prices[mask], volumes[mask]
    if len(prices) < 10:
        st.info("Insufficient data points")
        return

    # Histogram: volume at each price bin
    bin_edges = np.linspace(prices.min(), prices.max(), n_bins + 1)
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    vol_at_price = np.zeros(n_bins)
    bin_idx = np.digitize(prices, bin_edges) - 1
    bin_idx = np.clip(bin_idx, 0, n_bins - 1)
    for i, v in zip(bin_idx, volumes):
        vol_at_price[i] += v

    # POC = bin with max volume
    poc_idx = np.argmax(vol_at_price)
    poc_price = bin_centers[poc_idx]

    # Value Area — expand from POC until va_pct% of total volume captured
    total_vol = vol_at_price.sum()
    target_vol = total_vol * va_pct / 100
    va_vol = vol_at_price[poc_idx]
    lo_idx, hi_idx = poc_idx, poc_idx
    while va_vol < target_vol and (lo_idx > 0 or hi_idx < n_bins - 1):
        add_lo = vol_at_price[lo_idx - 1] if lo_idx > 0 else 0
        add_hi = vol_at_price[hi_idx + 1] if hi_idx < n_bins - 1 else 0
        if add_lo >= add_hi and lo_idx > 0:
            lo_idx -= 1
            va_vol += add_lo
        elif hi_idx < n_bins - 1:
            hi_idx += 1
            va_vol += add_hi
        else:
            lo_idx -= 1
            va_vol += add_lo
    val_price = bin_centers[lo_idx]
    vah_price = bin_centers[hi_idx]

    # KPIs
    cols = st.columns(4)
    with cols[0]:
        st.markdown(_kpi_html("POC", f"{poc_price:.2f}", _C["poc"]), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_kpi_html("Value Area High", f"{vah_price:.2f}", _C["vah"]), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(_kpi_html("Value Area Low", f"{val_price:.2f}", _C["val"]), unsafe_allow_html=True)
    with cols[3]:
        st.markdown(_kpi_html("Total Volume", f"{total_vol:,.0f}", _C["accent"]), unsafe_allow_html=True)

    # Chart: horizontal volume bars + price line
    fig = _subfig(1, 2, shared_xaxes=False, height=520,
                  column_widths=[0.3, 0.7])

    # Volume profile (horizontal bars)
    bar_colors = [
        _C["poc"] if i == poc_idx
        else (_C["vah"] if lo_idx <= i <= hi_idx else _C["dim"])
        for i in range(n_bins)
    ]
    fig.add_trace(go.Bar(
        y=bin_centers, x=vol_at_price, orientation="h",
        marker_color=bar_colors, name="Vol@Price",
        hovertemplate="Price: %{y:.2f}<br>Volume: %{x:,.0f}<extra></extra>",
    ), row=1, col=1)

    # Price line on right panel
    if "price" in sdf.columns:
        fig.add_trace(go.Scatter(
            x=sdf["datetime"], y=sdf["price"],
            mode="lines", line=dict(color=_C["accent"], width=1),
            name="Price",
        ), row=1, col=2)
    else:
        fig.add_trace(go.Candlestick(
            x=sdf["dt"], open=sdf["open"], high=sdf["high"],
            low=sdf["low"], close=sdf["close"], name="OHLC",
            increasing_line_color=_C["up"], decreasing_line_color=_C["down"],
        ), row=1, col=2)
        fig.update_layout(xaxis2_rangeslider_visible=False)

    # POC / VA lines on price chart
    for price, color, label in [
        (poc_price, _C["poc"], "POC"),
        (vah_price, _C["vah"], "VAH"),
        (val_price, _C["val"], "VAL"),
    ]:
        fig.add_hline(y=price, line_color=color, line_dash="dot",
                      annotation_text=label, annotation_font_color=color,
                      row=1, col=2)

    fig.update_layout(
        title=dict(text=f"{sym} — Volume Profile", font=dict(size=14)),
        yaxis=dict(title="Price"), xaxis=dict(title="Volume"),
        showlegend=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # AI Commentary
    _render_ai_commentary(
        f"vp_{sym}", get_volume_profile_commentary,
        symbol=sym, poc=poc_price, vah=vah_price, val=val_price,
        last_price=float(prices[-1]), total_volume=float(total_vol),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 2: ORDER FLOW  (cumulative delta, block detection)
# ═══════════════════════════════════════════════════════════════════════════════

def _classify_ticks(df: pd.DataFrame) -> pd.DataFrame:
    """Tick-rule classification: price up → buy, price down → sell."""
    df = df.sort_values("datetime").copy()
    df["price_diff"] = df["price"].diff()
    # Tick rule: if price goes up it's a buy, down is a sell, unchanged inherits previous
    df["side"] = np.where(df["price_diff"] > 0, 1, np.where(df["price_diff"] < 0, -1, 0))
    df["side"] = df["side"].replace(0, np.nan).ffill().fillna(1).astype(int)
    df["signed_vol"] = df["volume"] * df["side"]
    df["cum_delta"] = df["signed_vol"].cumsum()
    return df


def _render_order_flow(ticks: pd.DataFrame):
    """Cumulative delta, buy/sell imbalance, block trade detection."""
    st.markdown("### Order Flow Analysis")
    st.caption("Tick-rule classification — Cumulative Delta, Imbalance Ratio, Block Detection")

    if ticks.empty:
        st.warning("No tick data available. Requires dps_ticks_*.csv")
        return

    symbols = _get_symbols(ticks)
    c1, c2 = st.columns([3, 1])
    with c1:
        sym = st.selectbox("Symbol", symbols, key="of_sym",
                           index=symbols.index("HUBC") if "HUBC" in symbols else 0)
    with c2:
        block_threshold = st.number_input("Block size threshold", 5000, 500000, 50000,
                                          step=5000, key="of_block")

    sdf = ticks[ticks["symbol"] == sym].copy()
    if len(sdf) < 20:
        st.info(f"Insufficient ticks for {sym} ({len(sdf)} ticks)")
        return

    sdf = _classify_ticks(sdf)

    # KPIs
    buy_vol = sdf.loc[sdf["side"] == 1, "volume"].sum()
    sell_vol = sdf.loc[sdf["side"] == -1, "volume"].sum()
    total_vol = buy_vol + sell_vol
    imbalance = (buy_vol - sell_vol) / total_vol if total_vol > 0 else 0
    blocks = sdf[sdf["volume"] >= block_threshold]
    final_delta = sdf["cum_delta"].iloc[-1]

    cols = st.columns(5)
    with cols[0]:
        st.markdown(_kpi_html("Buy Volume", f"{buy_vol:,.0f}", _C["buy"]), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_kpi_html("Sell Volume", f"{sell_vol:,.0f}", _C["sell"]), unsafe_allow_html=True)
    with cols[2]:
        color = _C["buy"] if imbalance > 0 else _C["sell"]
        st.markdown(_kpi_html("Imbalance", f"{imbalance:+.1%}", color), unsafe_allow_html=True)
    with cols[3]:
        color = _C["buy"] if final_delta > 0 else _C["sell"]
        st.markdown(_kpi_html("Cum Delta", f"{final_delta:,.0f}", color), unsafe_allow_html=True)
    with cols[4]:
        st.markdown(_kpi_html("Block Trades", f"{len(blocks)}", _C["warning"]), unsafe_allow_html=True)

    # Chart: Price + Cum Delta + Block markers
    fig = _subfig(3, 1, row_heights=[0.4, 0.35, 0.25], height=620)

    # Price
    fig.add_trace(go.Scatter(
        x=sdf["datetime"], y=sdf["price"], mode="lines",
        line=dict(color=_C["accent"], width=1.2), name="Price",
    ), row=1, col=1)

    # Block trade markers
    if not blocks.empty:
        fig.add_trace(go.Scatter(
            x=blocks["datetime"], y=blocks["price"], mode="markers",
            marker=dict(
                size=np.clip(blocks["volume"] / block_threshold * 8, 6, 20),
                color=np.where(blocks["side"] == 1, _C["buy"], _C["sell"]),
                symbol="diamond", line=dict(width=1, color=_C["text"]),
            ),
            name="Blocks", hovertemplate="%{x}<br>Price: %{y:.2f}<br>Vol: %{text:,}<extra></extra>",
            text=blocks["volume"],
        ), row=1, col=1)

    # Cumulative delta
    delta_color = np.where(sdf["cum_delta"] >= 0, _C["buy"], _C["sell"])
    fig.add_trace(go.Scatter(
        x=sdf["datetime"], y=sdf["cum_delta"], mode="lines",
        line=dict(color=_C["accent2"], width=1.5), name="Cum Delta",
        fill="tozeroy", fillcolor="rgba(0,212,170,0.08)",
    ), row=2, col=1)
    fig.add_hline(y=0, line_color=_C["dim"], line_dash="dot", row=2, col=1)

    # Per-bar signed volume
    bar_colors = np.where(sdf["side"] == 1, _C["buy"], _C["sell"])
    fig.add_trace(go.Bar(
        x=sdf["datetime"], y=sdf["signed_vol"],
        marker_color=bar_colors.tolist(), name="Signed Vol", opacity=0.7,
    ), row=3, col=1)

    fig.update_layout(
        title=dict(text=f"{sym} — Order Flow ({len(sdf):,} ticks)", font=dict(size=14)),
        showlegend=True, legend=dict(orientation="h", y=1.02),
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Cum Delta", row=2, col=1)
    fig.update_yaxes(title_text="Signed Vol", row=3, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # Divergence callout
    price_trend = sdf["price"].iloc[-1] - sdf["price"].iloc[0]
    if (price_trend > 0 and final_delta < 0) or (price_trend < 0 and final_delta > 0):
        st.error(
            f"**DIVERGENCE DETECTED** — Price moved {'up' if price_trend > 0 else 'down'} "
            f"but cumulative delta is {'negative' if final_delta < 0 else 'positive'}. "
            "This often precedes a reversal."
        )
    elif abs(imbalance) > 0.3:
        side = "buying" if imbalance > 0 else "selling"
        st.success(f"**Strong {side} pressure** — Imbalance at {imbalance:+.1%}. Flow confirms direction.")

    # AI Commentary
    _render_ai_commentary(
        f"of_{sym}", get_order_flow_commentary,
        symbol=sym, buy_vol=float(buy_vol), sell_vol=float(sell_vol),
        imbalance=float(imbalance), cum_delta=float(final_delta),
        n_blocks=len(blocks), price_change=float(price_trend),
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 3: OPENING RANGE BREAKOUT SCANNER
# ═══════════════════════════════════════════════════════════════════════════════

def _render_orb_scanner(ohlcv_1m: pd.DataFrame):
    """Opening Range Breakout: first N minutes high/low as breakout levels."""
    st.markdown("### Opening Range Breakout Scanner")
    st.caption("First N minutes define the range — breakout triggers tracked for all symbols")

    if ohlcv_1m.empty:
        st.warning("No 1-minute data available.")
        return

    c1, c2, c3 = st.columns([1, 1, 1])
    with c1:
        orb_minutes = st.selectbox("Opening Range (min)", [5, 10, 15, 30], index=2, key="orb_min")
    with c2:
        min_volume = st.number_input("Min daily volume", 1000, 10_000_000, 50_000, step=10_000, key="orb_vol")
    with c3:
        market_open = st.time_input("Market open time", dtime(9, 15), key="orb_open")

    results = []
    open_dt = pd.Timestamp.now().normalize() + pd.Timedelta(hours=market_open.hour, minutes=market_open.minute)

    for sym, gdf in ohlcv_1m.groupby("symbol"):
        gdf = gdf.sort_values("dt")
        # Get today-like data (latest date in the file)
        latest_date = gdf["dt"].dt.date.max()
        day_df = gdf[gdf["dt"].dt.date == latest_date].copy()
        if day_df.empty or day_df["volume"].sum() < min_volume:
            continue

        # Opening range = first N minutes after market open
        day_start = day_df["dt"].min()
        orb_end = day_start + pd.Timedelta(minutes=orb_minutes)
        orb_df = day_df[day_df["dt"] <= orb_end]
        rest_df = day_df[day_df["dt"] > orb_end]

        if orb_df.empty or len(orb_df) < 2:
            continue

        orb_high = orb_df["high"].max()
        orb_low = orb_df["low"].min()
        orb_range = orb_high - orb_low
        if orb_range <= 0:
            continue

        day_high = day_df["high"].max()
        day_low = day_df["low"].min()
        day_close = day_df["close"].iloc[-1]
        day_open = day_df["open"].iloc[0]
        day_vol = day_df["volume"].sum()

        # Breakout detection
        broke_high = day_high > orb_high
        broke_low = day_low < orb_low
        if broke_high and not broke_low:
            direction = "BULL"
            breakout_move = (day_high - orb_high) / orb_high * 100
        elif broke_low and not broke_high:
            direction = "BEAR"
            breakout_move = (orb_low - day_low) / orb_low * 100
        elif broke_high and broke_low:
            direction = "CHOP"
            breakout_move = 0
        else:
            direction = "INSIDE"
            breakout_move = 0

        results.append({
            "Symbol": sym,
            "ORB High": orb_high,
            "ORB Low": orb_low,
            "ORB Range %": orb_range / orb_low * 100,
            "Day Close": day_close,
            "Day Chg %": (day_close - day_open) / day_open * 100 if day_open else 0,
            "Direction": direction,
            "Breakout Move %": breakout_move,
            "Volume": day_vol,
        })

    if not results:
        st.info("No symbols met the criteria.")
        return

    rdf = pd.DataFrame(results).sort_values("Breakout Move %", ascending=False)

    # KPIs
    bull_count = (rdf["Direction"] == "BULL").sum()
    bear_count = (rdf["Direction"] == "BEAR").sum()
    inside_count = (rdf["Direction"] == "INSIDE").sum()
    chop_count = (rdf["Direction"] == "CHOP").sum()

    cols = st.columns(4)
    with cols[0]:
        st.markdown(_kpi_html("Bull Breakouts", str(bull_count), _C["up"]), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_kpi_html("Bear Breakdowns", str(bear_count), _C["down"]), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(_kpi_html("Inside Range", str(inside_count), _C["neutral"]), unsafe_allow_html=True)
    with cols[3]:
        st.markdown(_kpi_html("Chop / Both", str(chop_count), _C["warning"]), unsafe_allow_html=True)

    # Color the direction column
    def _color_dir(val):
        m = {"BULL": "color:#00E676", "BEAR": "color:#FF5252",
             "INSIDE": "color:#78909C", "CHOP": "color:#FFD600"}
        return m.get(val, "")

    styled = rdf.style.format({
        "ORB High": "{:.2f}", "ORB Low": "{:.2f}", "ORB Range %": "{:.2f}%",
        "Day Close": "{:.2f}", "Day Chg %": "{:+.2f}%",
        "Breakout Move %": "{:+.2f}%", "Volume": "{:,.0f}",
    }).map(_color_dir, subset=["Direction"])

    st.dataframe(styled, use_container_width=True, height=450)

    # Top breakout chart
    top = rdf[rdf["Direction"].isin(["BULL", "BEAR"])].head(15)
    if not top.empty:
        colors = [_C["up"] if d == "BULL" else _C["down"] for d in top["Direction"]]
        fig = _fig(height=320, title=dict(text="Top Breakouts by Move %", font=dict(size=13)))
        fig.add_trace(go.Bar(
            x=top["Symbol"], y=top["Breakout Move %"],
            marker_color=colors,
            hovertemplate="%{x}: %{y:+.2f}%<extra></extra>",
        ))
        fig.update_yaxes(title_text="Breakout Move %")
        st.plotly_chart(fig, use_container_width=True)

    # AI Commentary
    top_for_ai = rdf[rdf["Direction"].isin(["BULL", "BEAR"])].head(5).to_dict("records")
    _render_ai_commentary(
        "orb_scan", get_orb_commentary,
        n_bull=bull_count, n_bear=bear_count, n_inside=inside_count,
        n_chop=chop_count, top_breakouts=top_for_ai, orb_minutes=orb_minutes,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 4: MULTI-TIMEFRAME CONFLUENCE
# ═══════════════════════════════════════════════════════════════════════════════

def _compute_trend(df: pd.DataFrame, sym: str) -> int:
    """Simple trend: +1 (bull), -1 (bear), 0 (flat) based on SMA crossover."""
    sdf = df[df["symbol"] == sym].sort_values("dt")
    if len(sdf) < 10:
        return 0
    closes = sdf["close"].dropna().values
    if len(closes) < 10:
        return 0
    fast = np.mean(closes[-5:])
    slow = np.mean(closes[-10:])
    if fast > slow * 1.005:
        return 1
    elif fast < slow * 0.995:
        return -1
    return 0


def _render_mtf_confluence(data_5m: pd.DataFrame, data_15m: pd.DataFrame,
                           data_1h: pd.DataFrame, data_1d: pd.DataFrame):
    """Multi-timeframe trend confluence heatmap."""
    st.markdown("### Multi-Timeframe Confluence")
    st.caption("Trend alignment across 5m / 15m / 1h / 1d — green = all bullish, red = all bearish")

    frames = {"5m": data_5m, "15m": data_15m, "1h": data_1h, "1d": data_1d}
    available = {k: v for k, v in frames.items() if not v.empty}

    if len(available) < 2:
        st.warning("Need at least 2 timeframes loaded. Check CSV files.")
        return

    # Get symbols common across all available timeframes
    sym_sets = [set(_get_symbols(v)) for v in available.values()]
    common_syms = sorted(set.intersection(*sym_sets))

    min_vol = st.slider("Min avg volume filter", 0, 500_000, 10_000, step=5_000, key="mtf_vol")

    # Filter by volume on the shortest timeframe available
    shortest_tf = list(available.keys())[0]
    shortest_df = available[shortest_tf]
    vol_by_sym = shortest_df.groupby("symbol")["volume"].mean()
    liquid_syms = [s for s in common_syms if vol_by_sym.get(s, 0) >= min_vol]

    if not liquid_syms:
        st.info("No symbols pass the volume filter.")
        return

    # Compute trend per symbol per timeframe
    rows = []
    for sym in liquid_syms:
        row = {"Symbol": sym}
        score = 0
        for tf_name, tf_df in available.items():
            t = _compute_trend(tf_df, sym)
            row[tf_name] = t
            score += t
        row["Score"] = score
        rows.append(row)

    cdf = pd.DataFrame(rows).sort_values("Score", ascending=False)

    # KPIs
    all_bull = (cdf["Score"] == len(available)).sum()
    all_bear = (cdf["Score"] == -len(available)).sum()
    mixed = len(cdf) - all_bull - all_bear

    cols = st.columns(4)
    with cols[0]:
        st.markdown(_kpi_html("Symbols Analyzed", str(len(cdf)), _C["accent"]), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_kpi_html("All Bullish", str(all_bull), _C["up"]), unsafe_allow_html=True)
    with cols[2]:
        st.markdown(_kpi_html("All Bearish", str(all_bear), _C["down"]), unsafe_allow_html=True)
    with cols[3]:
        st.markdown(_kpi_html("Mixed / Flat", str(mixed), _C["neutral"]), unsafe_allow_html=True)

    # Heatmap
    tf_cols = [c for c in cdf.columns if c not in ("Symbol", "Score")]
    z_data = cdf[tf_cols].values
    fig = _fig(
        height=max(300, len(cdf) * 18),
        title=dict(text="Trend Confluence Heatmap", font=dict(size=13)),
    )
    fig.add_trace(go.Heatmap(
        z=z_data, x=tf_cols, y=cdf["Symbol"].tolist(),
        colorscale=[[0, _C["down"]], [0.5, _C["grid"]], [1, _C["up"]]],
        zmin=-1, zmax=1,
        hovertemplate="Symbol: %{y}<br>TF: %{x}<br>Trend: %{z}<extra></extra>",
        showscale=False,
    ))
    fig.update_yaxes(autorange="reversed", dtick=1)
    st.plotly_chart(fig, use_container_width=True)

    # Tables: top bull & top bear
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**All-Timeframe Bullish**")
        bulls = cdf[cdf["Score"] == len(available)][["Symbol", "Score"]]
        if bulls.empty:
            st.caption("None")
        else:
            st.dataframe(bulls, use_container_width=True, hide_index=True)
    with c2:
        st.markdown("**All-Timeframe Bearish**")
        bears = cdf[cdf["Score"] == -len(available)][["Symbol", "Score"]]
        if bears.empty:
            st.caption("None")
        else:
            st.dataframe(bears, use_container_width=True, hide_index=True)

    # AI Commentary
    bull_syms = cdf[cdf["Score"] == len(available)]["Symbol"].tolist()
    bear_syms = cdf[cdf["Score"] == -len(available)]["Symbol"].tolist()
    _render_ai_commentary(
        "mtf_scan", get_mtf_confluence_commentary,
        n_all_bull=all_bull, n_all_bear=all_bear, n_total=len(cdf),
        top_bulls=bull_syms, top_bears=bear_syms,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 5: VWAP TERMINAL
# ═══════════════════════════════════════════════════════════════════════════════

def _render_vwap_terminal(ticks: pd.DataFrame, ohlcv_1m: pd.DataFrame):
    """VWAP with sigma bands + deviation ranking across all symbols."""
    st.markdown("### VWAP Terminal")
    st.caption("Volume-Weighted Average Price with 1/2/3 sigma bands — institutional anchor level")

    source = ticks if not ticks.empty else ohlcv_1m
    if source.empty:
        st.warning("No tick or 1m data.")
        return

    is_tick = "price" in source.columns
    symbols = _get_symbols(source)

    c1, c2 = st.columns([3, 1])
    with c1:
        sym = st.selectbox("Symbol", symbols, key="vwap_sym",
                           index=symbols.index("OGDC") if "OGDC" in symbols else 0)
    with c2:
        show_ranking = st.checkbox("Show deviation ranking", True, key="vwap_rank")

    sdf = source[source["symbol"] == sym].sort_values("datetime" if is_tick else "dt")
    if len(sdf) < 10:
        st.info(f"Insufficient data for {sym}")
        return

    # Compute VWAP
    if is_tick:
        p, v = sdf["price"].values, sdf["volume"].values
    else:
        p = ((sdf["high"] + sdf["low"] + sdf["close"]) / 3).values  # typical price
        v = sdf["volume"].values

    mask = np.isfinite(p) & np.isfinite(v) & (v > 0)
    p, v = p[mask], v[mask]
    ts = sdf["datetime" if is_tick else "dt"].values[mask]

    cum_pv = np.cumsum(p * v)
    cum_v = np.cumsum(v)
    vwap = cum_pv / cum_v

    # Rolling standard deviation of price from VWAP
    sq_diff = (p - vwap) ** 2
    cum_sq = np.cumsum(sq_diff * v)
    vwap_std = np.sqrt(cum_sq / cum_v)

    last_price = p[-1]
    last_vwap = vwap[-1]
    last_std = vwap_std[-1] if vwap_std[-1] > 0 else 0.01
    deviation_sigma = (last_price - last_vwap) / last_std

    # KPIs
    cols = st.columns(4)
    with cols[0]:
        st.markdown(_kpi_html("VWAP", f"{last_vwap:.2f}", _C["vwap"]), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_kpi_html("Last Price", f"{last_price:.2f}", _C["text"]), unsafe_allow_html=True)
    with cols[2]:
        dev_color = _C["up"] if last_price > last_vwap else _C["down"]
        st.markdown(_kpi_html("Deviation", f"{deviation_sigma:+.2f}σ", dev_color), unsafe_allow_html=True)
    with cols[3]:
        prem = (last_price - last_vwap) / last_vwap * 100
        st.markdown(_kpi_html("Premium/Disc", f"{prem:+.2f}%", dev_color), unsafe_allow_html=True)

    # Chart
    fig = _fig(height=480, title=dict(text=f"{sym} — VWAP ± Bands", font=dict(size=14)))

    # Price
    fig.add_trace(go.Scatter(x=ts, y=p, mode="lines",
                             line=dict(color=_C["accent"], width=1.2), name="Price"))

    # VWAP line
    fig.add_trace(go.Scatter(x=ts, y=vwap, mode="lines",
                             line=dict(color=_C["vwap"], width=2), name="VWAP"))

    # Bands
    for n, fill_color, label in [(1, _C["band1"], "±1σ"), (2, _C["band2"], "±2σ"), (3, _C["band3"], "±3σ")]:
        upper = vwap + n * vwap_std
        lower = vwap - n * vwap_std
        fig.add_trace(go.Scatter(x=ts, y=upper, mode="lines",
                                 line=dict(width=0), showlegend=False, hoverinfo="skip"))
        fig.add_trace(go.Scatter(x=ts, y=lower, mode="lines",
                                 line=dict(width=0), fill="tonexty", fillcolor=fill_color,
                                 name=label))

    fig.update_yaxes(title_text="Price")
    st.plotly_chart(fig, use_container_width=True)

    # AI Commentary
    _render_ai_commentary(
        f"vwap_{sym}", get_vwap_commentary,
        symbol=sym, vwap=float(last_vwap), last_price=float(last_price),
        deviation_sigma=float(deviation_sigma), premium_pct=float(prem),
    )

    # Deviation ranking across all symbols
    if show_ranking:
        st.markdown("#### VWAP Deviation Ranking")
        rankings = []
        for s in symbols:
            sdf2 = source[source["symbol"] == s]
            if len(sdf2) < 20:
                continue
            if is_tick:
                p2, v2 = sdf2["price"].values, sdf2["volume"].values
            else:
                p2 = ((sdf2["high"] + sdf2["low"] + sdf2["close"]) / 3).values
                v2 = sdf2["volume"].values
            m2 = np.isfinite(p2) & np.isfinite(v2) & (v2 > 0)
            if m2.sum() < 20:
                continue
            p2, v2 = p2[m2], v2[m2]
            cpv = np.cumsum(p2 * v2)
            cv = np.cumsum(v2)
            vw = cpv / cv
            sq = np.cumsum((p2 - vw) ** 2 * v2)
            vs = np.sqrt(sq / cv)
            std_val = vs[-1] if vs[-1] > 0 else 0.01
            dev = (p2[-1] - vw[-1]) / std_val
            rankings.append({
                "Symbol": s, "Price": p2[-1], "VWAP": vw[-1],
                "Deviation σ": dev, "Volume": v2.sum(),
            })

        if rankings:
            rk = pd.DataFrame(rankings).sort_values("Deviation σ", ascending=False)
            # Show extremes
            top_5 = rk.head(5)
            bot_5 = rk.tail(5)
            c1, c2 = st.columns(2)
            with c1:
                st.markdown("**Most Above VWAP (overbought)**")
                st.dataframe(top_5.style.format({
                    "Price": "{:.2f}", "VWAP": "{:.2f}",
                    "Deviation σ": "{:+.2f}", "Volume": "{:,.0f}",
                }), use_container_width=True, hide_index=True)
            with c2:
                st.markdown("**Most Below VWAP (oversold)**")
                st.dataframe(bot_5.style.format({
                    "Price": "{:.2f}", "VWAP": "{:.2f}",
                    "Deviation σ": "{:+.2f}", "Volume": "{:,.0f}",
                }), use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════════════════════
# TAB 6: VOLATILITY REGIME
# ═══════════════════════════════════════════════════════════════════════════════

def _render_volatility_regime(ohlcv_1h: pd.DataFrame, daily_eod: pd.DataFrame):
    """Intraday vol patterns, regime classification, vol compression/expansion."""
    st.markdown("### Volatility Regime Classifier")
    st.caption("Realized vol vs its own average — compression precedes breakouts")

    source = daily_eod if not daily_eod.empty else ohlcv_1h
    if source.empty:
        st.warning("No daily or hourly data available.")
        return

    dt_col = "date" if "date" in source.columns else "dt"
    symbols = _get_symbols(source)

    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        sym = st.selectbox("Symbol", symbols, key="vol_sym",
                           index=symbols.index("LUCK") if "LUCK" in symbols else 0)
    with c2:
        vol_window = st.slider("Vol window", 5, 60, 20, key="vol_win")
    with c3:
        avg_window = st.slider("Avg vol window", 20, 120, 60, key="vol_avg")

    sdf = source[source["symbol"] == sym].sort_values(dt_col).copy()
    if len(sdf) < avg_window + 5:
        st.info(f"Need at least {avg_window + 5} bars. Got {len(sdf)}.")
        return

    # Compute log returns and rolling realized vol
    sdf["ret"] = np.log(sdf["close"] / sdf["close"].shift(1))
    sdf["rvol"] = sdf["ret"].rolling(vol_window).std() * np.sqrt(252)  # annualized
    sdf["rvol_avg"] = sdf["rvol"].rolling(avg_window).mean()
    sdf = sdf.dropna(subset=["rvol", "rvol_avg"])

    if sdf.empty:
        st.info("Insufficient data after vol computation.")
        return

    # Regime: expansion if rvol > 1.2x avg, compression if < 0.8x
    sdf["regime"] = np.where(
        sdf["rvol"] > sdf["rvol_avg"] * 1.2, "EXPANSION",
        np.where(sdf["rvol"] < sdf["rvol_avg"] * 0.8, "COMPRESSION", "NORMAL")
    )

    latest = sdf.iloc[-1]
    regime = latest["regime"]
    regime_color = {"EXPANSION": _C["down"], "COMPRESSION": _C["warning"], "NORMAL": _C["neutral"]}

    cols = st.columns(4)
    with cols[0]:
        st.markdown(_kpi_html("Current Vol", f"{latest['rvol']:.1%}", _C["accent"]), unsafe_allow_html=True)
    with cols[1]:
        st.markdown(_kpi_html("Avg Vol", f"{latest['rvol_avg']:.1%}", _C["dim"]), unsafe_allow_html=True)
    with cols[2]:
        ratio = latest["rvol"] / latest["rvol_avg"] if latest["rvol_avg"] > 0 else 1
        st.markdown(_kpi_html("Vol Ratio", f"{ratio:.2f}x", regime_color[regime]), unsafe_allow_html=True)
    with cols[3]:
        st.markdown(_kpi_html("Regime", regime, regime_color[regime]), unsafe_allow_html=True)

    # Chart: Price + Vol + Regime bands
    fig = _subfig(2, 1, row_heights=[0.55, 0.45], height=520)

    fig.add_trace(go.Scatter(
        x=sdf[dt_col], y=sdf["close"], mode="lines",
        line=dict(color=_C["accent"], width=1.2), name="Price",
    ), row=1, col=1)

    # Color background by regime
    for regime_name, color in [("COMPRESSION", "rgba(255,214,0,0.08)"), ("EXPANSION", "rgba(255,82,82,0.08)")]:
        mask = sdf["regime"] == regime_name
        if mask.any():
            fig.add_trace(go.Scatter(
                x=sdf.loc[mask, dt_col], y=sdf.loc[mask, "close"],
                mode="markers", marker=dict(size=4, color=color.replace("0.08", "0.6")),
                name=regime_name, showlegend=True,
            ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=sdf[dt_col], y=sdf["rvol"], mode="lines",
        line=dict(color=_C["warning"], width=1.5), name=f"RVol({vol_window}d)",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=sdf[dt_col], y=sdf["rvol_avg"], mode="lines",
        line=dict(color=_C["dim"], width=1, dash="dash"), name=f"Avg({avg_window}d)",
    ), row=2, col=1)

    # Expansion/compression zones
    fig.add_trace(go.Scatter(
        x=sdf[dt_col], y=sdf["rvol_avg"] * 1.2, mode="lines",
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=sdf[dt_col], y=sdf["rvol_avg"] * 0.8, mode="lines",
        line=dict(width=0), fill="tonexty", fillcolor="rgba(120,144,156,0.1)",
        name="Normal Zone",
    ), row=2, col=1)

    fig.update_layout(title=dict(text=f"{sym} — Volatility Regime", font=dict(size=14)))
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Ann. Vol", tickformat=".0%", row=2, col=1)
    st.plotly_chart(fig, use_container_width=True)

    # Cross-sectional vol scanner
    n_comp, n_exp = 0, 0
    with st.expander("Cross-Sectional Vol Scanner", expanded=False):
        scan_results = []
        for s in symbols[:200]:  # cap for performance
            sdf2 = source[source["symbol"] == s].sort_values(dt_col)
            if len(sdf2) < avg_window + 5:
                continue
            ret2 = np.log(sdf2["close"] / sdf2["close"].shift(1)).dropna()
            if len(ret2) < avg_window:
                continue
            rv = ret2.rolling(vol_window).std().iloc[-1] * np.sqrt(252)
            rv_avg = ret2.rolling(vol_window).std().rolling(avg_window).mean().iloc[-1] * np.sqrt(252)
            if rv_avg > 0 and np.isfinite(rv) and np.isfinite(rv_avg):
                scan_results.append({
                    "Symbol": s,
                    "RVol": rv, "Avg Vol": rv_avg,
                    "Ratio": rv / rv_avg,
                    "Regime": "EXPANSION" if rv > rv_avg * 1.2 else ("COMPRESSION" if rv < rv_avg * 0.8 else "NORMAL"),
                })
        if scan_results:
            scan_df = pd.DataFrame(scan_results).sort_values("Ratio")
            st.markdown(f"**{(scan_df['Regime'] == 'COMPRESSION').sum()} compressed** / "
                        f"**{(scan_df['Regime'] == 'EXPANSION').sum()} expanding** / "
                        f"**{(scan_df['Regime'] == 'NORMAL').sum()} normal**")
            st.dataframe(scan_df.style.format({
                "RVol": "{:.1%}", "Avg Vol": "{:.1%}", "Ratio": "{:.2f}x",
            }), use_container_width=True, height=350, hide_index=True)
            n_comp = (scan_df["Regime"] == "COMPRESSION").sum()
            n_exp = (scan_df["Regime"] == "EXPANSION").sum()
            n_comp = (scan_df["Regime"] == "COMPRESSION").sum()
            n_exp = (scan_df["Regime"] == "EXPANSION").sum()

    # AI Commentary (DeepSeek for deeper reasoning)
    _render_ai_commentary(
        f"vol_{sym}", get_vol_regime_commentary,
        symbol=sym, current_vol=float(latest["rvol"]),
        avg_vol=float(latest["rvol_avg"]), vol_ratio=float(ratio),
        regime=regime, n_compressed=n_comp, n_expanding=n_exp,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════════

def render_intraday_quant_lab():
    """Main entry point for the Intraday Quant Lab page."""
    st.markdown(_PAGE_CSS, unsafe_allow_html=True)
    st.markdown("## Intraday Quant Lab")
    st.caption(
        "Volume Profile · Order Flow · ORB Scanner · "
        "Multi-TF Confluence · VWAP Terminal · Volatility Regime"
    )

    # Check data directory
    if not _DATA_DIR.exists():
        st.error(f"Data directory not found: `{_DATA_DIR}`")
        return

    csv_files = list(_DATA_DIR.glob("*.csv"))
    if not csv_files:
        st.error(f"No CSV files found in `{_DATA_DIR}`")
        return

    # LLM model selector
    _model_options = {
        "Llama 3.1 (fast)": OLLAMA_MODEL_FAST,
        "DeepSeek R1 (reasoning)": OLLAMA_MODEL_DEEP,
    }
    with st.expander("Settings", expanded=False):
        c1, c2 = st.columns(2)
        with c1:
            _sel_model = st.selectbox(
                "AI Commentary Model", list(_model_options.keys()), key="ql_model",
            )
        with c2:
            st.caption(f"Model ID: `{_model_options[_sel_model]}`")
    st.session_state["ql_selected_model"] = _model_options[_sel_model]

    # Data inventory bar
    with st.expander("Data Inventory", expanded=False):
        inv = []
        for f in sorted(csv_files):
            size_mb = f.stat().st_size / 1024 / 1024
            inv.append({"File": f.name, "Size (MB)": f"{size_mb:.1f}"})
        st.dataframe(pd.DataFrame(inv), use_container_width=True, hide_index=True)

    # Tabs
    tab_vp, tab_of, tab_orb, tab_mtf, tab_vwap, tab_vol = st.tabs([
        "Volume Profile", "Order Flow", "ORB Scanner",
        "Multi-TF Confluence", "VWAP Terminal", "Vol Regime",
    ])

    with tab_vp:
        ticks = _load_ticks()
        ohlcv_1m = _load_ohlcv("1m")
        _render_volume_profile(ticks, ohlcv_1m)

    with tab_of:
        ticks = _load_ticks()
        _render_order_flow(ticks)

    with tab_orb:
        ohlcv_1m = _load_ohlcv("1m")
        _render_orb_scanner(ohlcv_1m)

    with tab_mtf:
        _render_mtf_confluence(
            _load_ohlcv("5m"), _load_ohlcv("15m"),
            _load_ohlcv("1h"), _load_ohlcv("1d"),
        )

    with tab_vwap:
        ticks = _load_ticks()
        ohlcv_1m = _load_ohlcv("1m")
        _render_vwap_terminal(ticks, ohlcv_1m)

    with tab_vol:
        _render_volatility_regime(_load_ohlcv("1h"), _load_daily_eod())

    render_footer()
