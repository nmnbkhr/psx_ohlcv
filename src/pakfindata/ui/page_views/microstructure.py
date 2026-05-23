"""Market Microstructure & Risk — VPIN toxicity monitor & Game Theory payoff.

Tabs:
  Toxicity Monitor — VPIN gauge, volume bucket flow, payoff matrix
  Settings — Configure bucket size, window, spread/loss parameters
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.engine.microstructure import (
    compute_vpin,
    evaluate_payoff,
    build_payoff_table,
    generate_dummy_tick_data,
)
from pakfindata.engine.commentary import get_vpin_rules_commentary, get_vpin_ai_commentary
from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import render_footer

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "warning": "#FFD600", "toxic": "#FF5252",
    "safe": "#00C853", "vpin_line": "#FFD600",
    "buy": "#00E676", "sell": "#FF5252",
    "bg": "#0e1117", "card_bg": "#1a1a2e", "grid": "#2d2d3d",
    "text": "#e0e0e0", "text_dim": "#888888",
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11, family="JetBrains Mono, monospace"),
    xaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    yaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=10, r=10, t=40, b=10),
)


def _styled_fig(height=400, **kw):
    return go.Figure(layout={**_CHART_LAYOUT, "height": height, **kw})


# ═════════════════════════════════════════════════════════════════════════════
# VPIN GAUGE
# ═════════════════════════════════════════════════════════════════════════════

def _render_vpin_gauge(vpin_value: float):
    """Semi-circle gauge for VPIN toxicity (0 → 1)."""
    if vpin_value < 0.4:
        bar_color = _COLORS["safe"]
        label = "SAFE"
    elif vpin_value < 0.7:
        bar_color = _COLORS["warning"]
        label = "ELEVATED"
    else:
        bar_color = _COLORS["toxic"]
        label = "TOXIC"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=vpin_value,
        number=dict(
            font=dict(size=48, color=bar_color, family="JetBrains Mono, monospace"),
            valueformat=".3f",
        ),
        title=dict(
            text=f"Order Flow Toxicity — {label}",
            font=dict(size=16, color=_COLORS["text"]),
        ),
        gauge=dict(
            axis=dict(range=[0, 1], tickwidth=2, tickcolor=_COLORS["text_dim"],
                      dtick=0.1, tickfont=dict(size=10)),
            bar=dict(color=bar_color, thickness=0.3),
            bgcolor="rgba(0,0,0,0)",
            borderwidth=0,
            steps=[
                dict(range=[0, 0.4], color="rgba(0,200,83,0.15)"),
                dict(range=[0.4, 0.7], color="rgba(255,214,0,0.15)"),
                dict(range=[0.7, 1.0], color="rgba(255,23,68,0.15)"),
            ],
            threshold=dict(
                line=dict(color=_COLORS["text"], width=3),
                thickness=0.8,
                value=vpin_value,
            ),
        ),
    ))
    fig.update_layout(
        height=280,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=_COLORS["text"]),
        margin=dict(l=30, r=30, t=60, b=10),
    )
    st.plotly_chart(fig, width='stretch')


# ═════════════════════════════════════════════════════════════════════════════
# VOLUME BUCKET FLOW CHART
# ═════════════════════════════════════════════════════════════════════════════

def _render_bucket_flow(buckets: pd.DataFrame):
    """Stacked bar chart of V_buy/V_sell with VPIN line overlay."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    # Stacked bars: Buy (green) on bottom, Sell (red) on top
    fig.add_trace(
        go.Bar(
            x=buckets["bucket_id"],
            y=buckets["V_buy"],
            name="Buy Volume (V<sup>B</sup>)",
            marker_color=_COLORS["buy"],
            opacity=0.8,
        ),
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(
            x=buckets["bucket_id"],
            y=buckets["V_sell"],
            name="Sell Volume (V<sup>S</sup>)",
            marker_color=_COLORS["sell"],
            opacity=0.8,
        ),
        secondary_y=False,
    )

    # VPIN line overlay on secondary axis
    if "vpin" in buckets.columns:
        fig.add_trace(
            go.Scatter(
                x=buckets["bucket_id"],
                y=buckets["vpin"],
                name="VPIN",
                line=dict(color=_COLORS["vpin_line"], width=2.5),
                mode="lines",
            ),
            secondary_y=True,
        )

        # Toxicity threshold bands
        fig.add_hline(y=0.7, line_dash="dot", line_color=_COLORS["toxic"],
                      annotation_text="Toxic (0.7)", secondary_y=True,
                      annotation_font_color=_COLORS["toxic"])
        fig.add_hline(y=0.4, line_dash="dot", line_color=_COLORS["warning"],
                      annotation_text="Elevated (0.4)", secondary_y=True,
                      annotation_font_color=_COLORS["warning"])

    fig.update_layout(
        **_CHART_LAYOUT,
        height=420,
        barmode="stack",
        title=dict(text="Volume Bucket Flow — Buy vs Sell with VPIN Overlay",
                   font=dict(size=14)),
        hovermode="x unified",
    )
    fig.update_xaxes(title_text="Volume Bucket (τ)", gridcolor=_COLORS["grid"])
    fig.update_yaxes(title_text="Volume", gridcolor=_COLORS["grid"],
                     secondary_y=False)
    fig.update_yaxes(title_text="VPIN", range=[0, 1.05],
                     gridcolor=_COLORS["grid"], secondary_y=True)

    st.plotly_chart(fig, width='stretch')


# ═════════════════════════════════════════════════════════════════════════════
# VPIN TIME SERIES
# ═════════════════════════════════════════════════════════════════════════════

def _render_vpin_timeseries(buckets: pd.DataFrame):
    """VPIN evolution over volume-time with colored zones."""
    if "vpin" not in buckets.columns:
        return

    fig = _styled_fig(height=250, title=dict(text="VPIN Evolution", font=dict(size=14)))

    # Zone fills
    fig.add_hrect(y0=0, y1=0.4, fillcolor="rgba(0,200,83,0.06)", line_width=0)
    fig.add_hrect(y0=0.4, y1=0.7, fillcolor="rgba(255,214,0,0.06)", line_width=0)
    fig.add_hrect(y0=0.7, y1=1.0, fillcolor="rgba(255,23,68,0.06)", line_width=0)

    fig.add_trace(go.Scatter(
        x=buckets["bucket_id"], y=buckets["vpin"],
        mode="lines", line=dict(color=_COLORS["vpin_line"], width=2),
        fill="tozeroy", fillcolor="rgba(255,214,0,0.1)",
        name="VPIN",
    ))

    fig.update_yaxes(range=[0, 1.05], dtick=0.2)
    fig.update_xaxes(title_text="Volume Bucket (τ)")
    st.plotly_chart(fig, width='stretch')


# ═════════════════════════════════════════════════════════════════════════════
# PAYOFF MATRIX TABLE
# ═════════════════════════════════════════════════════════════════════════════

def _render_payoff_matrix(vpin: float, half_spread: float, adverse_loss: float):
    """Game Theory payoff matrix with conditional formatting via st.dataframe."""
    table = build_payoff_table(vpin, half_spread, adverse_loss)
    payoff = evaluate_payoff(vpin, half_spread, adverse_loss)

    # KPI cards for current state
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("VPIN (π)", f"{payoff.vpin:.3f}")
    with c2:
        st.metric("Half-Spread (s)", f"{payoff.half_spread:.2f}")
    with c3:
        st.metric("Adverse Loss (L)", f"{payoff.adverse_loss:.2f}")
    with c4:
        ev_delta = "positive" if payoff.ev_make > 0 else "negative"
        st.metric("EV_make", f"{payoff.ev_make:.4f}",
                  delta=ev_delta,
                  delta_color="normal" if payoff.ev_make > 0 else "inverse")

    # Pandas styled dataframe — matches app-wide pattern (fund_explorer, live_ticker)
    def _color_ev(val):
        """Color EV_make: green if positive, red if negative, yellow if zero."""
        if isinstance(val, (int, float)):
            if val > 0:
                return f"color: {_COLORS['safe']}; font-weight: 700"
            elif val < 0:
                return f"color: {_COLORS['toxic']}; font-weight: 700"
            else:
                return f"color: {_COLORS['warning']}; font-weight: 700"
        return ""

    def _color_strategy(val):
        """Color strategy cell based on recommendation."""
        if isinstance(val, str):
            if "MAKER" in val:
                return f"color: {_COLORS['safe']}; font-weight: 600"
            elif "NEUTRAL" in val:
                return f"color: {_COLORS['warning']}; font-weight: 600"
            elif "TAKER" in val:
                return f"color: {_COLORS['toxic']}; font-weight: 600"
        return ""

    def _highlight_current(row):
        """Highlight the ▶ Current row with a blue background."""
        if "Current" in str(row["Market State"]):
            return ["background-color: rgba(47,129,247,0.15)"] * len(row)
        return [""] * len(row)

    styled = (
        table.style
        .apply(_highlight_current, axis=1)
        .map(_color_ev, subset=["EV_make"])
        .map(_color_strategy, subset=["Optimal Strategy"])
        .format({"VPIN (π)": "{:.3f}", "EV_make": "{:+.4f}"})
    )

    st.dataframe(styled, width='stretch', hide_index=True, height=210)


# ═════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═════════════════════════════════════════════════════════════════════════════

def _get_intraday_dates() -> list[str]:
    """Dates with a pre-aggregated summary row. Fast indexed read."""
    dates = api_client.get_intraday_dates() or []
    return dates[:30]


def _get_master_symbols() -> list[str]:
    """Load all active symbols from the symbols master."""
    rows = api_client.get_symbols(active_only=True) or []
    return [r["symbol"] for r in rows]


def _load_intraday_ticks(symbol: str, date_str: str) -> pd.DataFrame:
    """Load tick-level data for a symbol on a date."""
    rows = api_client.get_intraday_bars(
        symbol=symbol, date=date_str, interval="1s", limit=50000
    ) or []
    if not rows:
        return pd.DataFrame(columns=["datetime", "ts_epoch", "close", "volume"])
    df = pd.DataFrame(rows)[["ts", "ts_epoch", "close", "volume"]].rename(
        columns={"ts": "datetime"}
    )
    df["datetime"] = pd.to_datetime(df["datetime"])
    # Drop rows with zero volume (no trade)
    df = df[df["volume"] > 0].reset_index(drop=True)
    return df


def _load_eod_data(symbol: str, limit: int = 200) -> pd.DataFrame:
    """Load EOD daily bars for a symbol (interday VPIN).

    Calls /v1/eod/{symbol} with a wide-enough window to cover ``limit``
    trading days (~365 calendar days for 200 trading days), filters
    volume > 0, then takes the most recent ``limit`` rows.
    """
    from datetime import date as _date, timedelta as _td

    calendar_days = max(int(limit * 1.8), 60)
    from_date = (_date.today() - _td(days=calendar_days)).isoformat()
    rows = api_client.get_symbol_history(symbol=symbol, from_date=from_date) or []
    if not rows:
        return pd.DataFrame(columns=["datetime", "close", "volume"])
    df = pd.DataFrame(rows)[["date", "close", "volume"]].rename(
        columns={"date": "datetime"}
    )
    df = df[df["volume"] > 0]
    df = df.sort_values("datetime").tail(limit).reset_index(drop=True)
    df["datetime"] = pd.to_datetime(df["datetime"])
    return df




_PLAYBOOK_MD = """
### STEP 1: Find the Macro Bias (Daily FFT)
*Question: Are we structurally overbought or oversold?*

Look at the **Macro Cycles** page. Identify the Dominant Cycle length and compare the Current Price to the smooth **IFFT Signal Line**.

* :chart_with_downwards_trend: **Oversold (Bull Bias):** Price is significantly **BELOW** the IFFT line.
* :chart_with_upwards_trend: **Overbought (Bear Bias):** Price is significantly **ABOVE** the IFFT line. Mean-reversion is due.
* :heavy_minus_sign: **White Noise:** Power spectrum is flat. Cycles are dead.

---

### STEP 2: Read the Microstructure (Intraday VPIN)
*Question: Who is aggressively controlling the tape right now?*

Switch to the **Microstructure** page. Check the **VPIN Gauge** (Order Flow Toxicity).

* :green_circle: **VPIN < 0.4 (Low Toxicity):** Retail noise. Balanced buying and selling. Safe environment.
* :yellow_circle: **VPIN 0.4 - 0.7 (Elevated):** Imbalance is building. Institutions are accumulating/distributing.
* :red_circle: **VPIN > 0.7 (High Toxicity):** Toxic order flow. Algorithms are sweeping the book.

---

### STEP 3: The Game Theory Execution
*Question: How do I route my order for the best fill?*

Combine Step 1 and Step 2 into the **Expected Value (EV) Matrix**:

| Macro Bias (FFT) | Micro Toxicity (VPIN) | EV | Execution Strategy |
| :--- | :--- | :--- | :--- |
| **BULLISH** | :green_circle: **LOW** (Safe) | **+** | **MAKER:** Post a Limit Buy Order on the Bid. |
| **BULLISH** | :red_circle: **HIGH** (Buy Vol) | **-** | **TAKER:** Cross the spread! Market Buy immediately. |
| **BEARISH** | :green_circle: **LOW** (Safe) | **+** | **MAKER:** Post a Limit Sell Order on the Ask. |
| **BEARISH** | :red_circle: **HIGH** (Sell Vol)| **-** | **TAKER:** Cross the spread! Market Sell immediately. |
| **NEUTRAL** | :red_circle: **HIGH** (Mixed) | **-** | **AVOID:** Toxic chop. Step away from the terminal. |

*Pro Tip: Never act as a Market Maker (post limit orders) when VPIN is in the Red Zone.*
"""


# ═════════════════════════════════════════════════════════════════════════════
# VOLUME PROFILE (Price vs Volume horizontal bars)
# ═════════════════════════════════════════════════════════════════════════════

def _render_volume_profile(df: pd.DataFrame):
    """Horizontal bar chart — volume at each price level."""
    prices = df["price"].dropna()
    volumes = df["volume"].dropna()
    if prices.empty or len(prices) < 10:
        st.info("Not enough price data for volume profile.")
        return

    # Compute per-tick volume (diff cumulative)
    tick_vol = volumes.diff().fillna(0).clip(lower=0)

    # Bin prices into levels
    price_range = prices.max() - prices.min()
    if price_range <= 0:
        st.info("No price variation for volume profile.")
        return

    n_bins = min(50, max(15, int(price_range / 0.05)))
    bins = pd.cut(prices, bins=n_bins)
    profile = pd.DataFrame({"price_bin": bins, "vol": tick_vol})
    profile = profile.groupby("price_bin", observed=True)["vol"].sum().reset_index()
    profile = profile[profile["vol"] > 0].copy()

    if profile.empty:
        st.info("No volume data for profile.")
        return

    profile["mid"] = profile["price_bin"].apply(lambda x: x.mid)
    profile = profile.sort_values("mid")

    # Point of Control (POC) — price with max volume
    poc_idx = profile["vol"].idxmax()
    poc_price = profile.loc[poc_idx, "mid"]

    # Value Area — 70% of total volume around POC
    total_vol = profile["vol"].sum()
    sorted_by_vol = profile.sort_values("vol", ascending=False)
    cum = 0
    va_prices = set()
    for _, row in sorted_by_vol.iterrows():
        cum += row["vol"]
        va_prices.add(row["mid"])
        if cum >= total_vol * 0.7:
            break

    colors = [
        _COLORS["accent"] if mid in va_prices else _COLORS["text_dim"]
        for mid in profile["mid"]
    ]

    fig = go.Figure(go.Bar(
        y=profile["mid"],
        x=profile["vol"],
        orientation="h",
        marker_color=colors,
        hovertemplate="Price: %{y:.2f}<br>Volume: %{x:,.0f}<extra></extra>",
    ))
    fig.add_hline(
        y=poc_price,
        line_dash="dash", line_color=_COLORS["warning"], line_width=2,
        annotation_text=f"POC: {poc_price:.2f}",
        annotation_font_color=_COLORS["warning"],
    )
    fig.update_layout(
        **_CHART_LAYOUT, height=500,
        title="Volume Profile (70% Value Area highlighted)",
        xaxis_title="Volume", yaxis_title="Price",
        yaxis_tickformat=".2f",
    )
    st.plotly_chart(fig, width='stretch')

    c1, c2, c3 = st.columns(3)
    c1.metric("POC (Point of Control)", f"{poc_price:.2f}")
    c2.metric("Value Area High", f"{max(va_prices):.2f}")
    c3.metric("Value Area Low", f"{min(va_prices):.2f}")


# ═════════════════════════════════════════════════════════════════════════════
# TRADE SIZE DISTRIBUTION
# ═════════════════════════════════════════════════════════════════════════════

def _render_trade_size_distribution(df: pd.DataFrame):
    """Histogram of per-tick trade sizes with retail vs institutional breakdown."""
    volumes = df["volume"].dropna()
    if volumes.empty:
        st.info("No volume data available.")
        return

    # Compute per-tick volume (diff cumulative)
    tick_vol = volumes.diff().fillna(0).clip(lower=0)
    tick_vol = tick_vol[tick_vol > 0]

    if tick_vol.empty or len(tick_vol) < 5:
        st.info("Not enough trade data for size distribution.")
        return

    # Buckets
    bins = [0, 100, 500, 1000, 5000, 50000, float("inf")]
    labels = ["1–100", "101–500", "501–1K", "1K–5K", "5K–50K", "50K+"]
    cats = pd.cut(tick_vol, bins=bins, labels=labels)
    dist = cats.value_counts().reindex(labels, fill_value=0)

    fig = go.Figure(go.Bar(
        x=dist.index.tolist(),
        y=dist.values,
        marker_color=[_COLORS["safe"], _COLORS["accent"], _COLORS["vpin_line"],
                      _COLORS["warning"], _COLORS["toxic"], _COLORS["down"]],
        text=[f"{v:,}" for v in dist.values],
        textposition="outside",
    ))
    fig.update_layout(
        **_CHART_LAYOUT, height=350,
        title="Trade Size Distribution",
        xaxis_title="Trade Size (shares)", yaxis_title="Count",
    )
    st.plotly_chart(fig, width='stretch')

    # Retail vs institutional
    total_trades = len(tick_vol)
    retail = (tick_vol <= 500).sum()
    institutional = (tick_vol > 5000).sum()
    mid = total_trades - retail - institutional

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Trades", f"{total_trades:,}")
    c2.metric("Retail (≤500)", f"{retail/total_trades*100:.1f}%")
    c3.metric("Mid (500–5K)", f"{mid/total_trades*100:.1f}%")
    c4.metric("Institutional (>5K)", f"{institutional/total_trades*100:.1f}%")


# ═════════════════════════════════════════════════════════════════════════════
# TICK-BY-TICK TABLE
# ═════════════════════════════════════════════════════════════════════════════

def _render_tick_table(df: pd.DataFrame):
    """Last N trades with side classification (Lee-Ready tick rule)."""
    show_n = st.slider("Show last N ticks", 20, 500, 100, step=20, key="tick_table_n")

    tdf = df.tail(show_n).copy()
    tdf = tdf.reset_index(drop=True)

    # Per-tick volume
    if "volume" in tdf.columns:
        tdf["tick_vol"] = tdf["volume"].diff().fillna(0).clip(lower=0).astype(int)
    else:
        tdf["tick_vol"] = 0

    # Tick rule: classify buy/sell
    tdf["side"] = "—"
    if "price" in tdf.columns:
        price_diff = tdf["price"].diff()
        last_side = "—"
        sides = []
        for d in price_diff:
            if pd.isna(d) or d == 0:
                sides.append(last_side)
            elif d > 0:
                last_side = "BUY"
                sides.append("BUY")
            else:
                last_side = "SELL"
                sides.append("SELL")
        tdf["side"] = sides

    # Spread
    if "bid" in tdf.columns and "ask" in tdf.columns:
        tdf["spread"] = (tdf["ask"] - tdf["bid"]).round(2)
    else:
        tdf["spread"] = 0.0

    # Time column
    time_col = None
    for c in ["datetime", "_ts", "ts"]:
        if c in tdf.columns:
            time_col = c
            break

    cols_display = []
    if time_col:
        tdf["Time"] = tdf[time_col].astype(str).str[-8:]
        cols_display.append("Time")

    rename = {}
    if "price" in tdf.columns:
        rename["price"] = "Price"
    if "tick_vol" in tdf.columns:
        rename["tick_vol"] = "Vol"
    if "bid" in tdf.columns:
        rename["bid"] = "Bid"
    if "ask" in tdf.columns:
        rename["ask"] = "Ask"
    rename["spread"] = "Spread"
    rename["side"] = "Side"

    tdf = tdf.rename(columns=rename)
    cols_display += [v for v in rename.values() if v in tdf.columns]

    display_df = tdf[cols_display].iloc[::-1]  # newest first

    def _color_side(val):
        if val == "BUY":
            return f"color: {_COLORS['buy']}"
        elif val == "SELL":
            return f"color: {_COLORS['down']}"
        return ""

    styled = display_df.style.map(_color_side, subset=["Side"])
    st.dataframe(styled, width='stretch', height=400)

    # Summary
    buys = (display_df["Side"] == "BUY").sum()
    sells = (display_df["Side"] == "SELL").sum()
    total = buys + sells
    if total > 0:
        c1, c2, c3 = st.columns(3)
        c1.metric("Buy Trades", f"{buys} ({buys/total*100:.0f}%)")
        c2.metric("Sell Trades", f"{sells} ({sells/total*100:.0f}%)")
        c3.metric("Net Pressure", f"{'BUY' if buys > sells else 'SELL'} +{abs(buys-sells)}")


def render_microstructure():
    """Main entry point for the Market Microstructure & Risk page."""
    if api_client.render_api_status_banner_if_down():
        return

    st.markdown("## Market Microstructure & Risk")
    st.caption("Order Flow Toxicity (VPIN) · Maker-Taker Game Theory · Volume-Synchronized Analysis")

    with st.expander("How to Read This Analysis (Execution Playbook)", expanded=False):
        st.markdown(_PLAYBOOK_MD)

    # ── Top bar: Data source, date, symbol selectors (on page, not sidebar) ──
    col_src, col_date, col_sym = st.columns([1, 1, 1])

    with col_src:
        data_source = st.selectbox(
            "Data Source",
            ["Intraday Ticks", "EOD Daily Bars", "Demo Data"],
            index=0,
        )

    sel_symbol = None
    sel_date = None
    n_bars = 1000

    # Load master symbol list once (cached)
    all_symbols = _get_master_symbols()

    if data_source == "Intraday Ticks":
        avail_dates = _get_intraday_dates()
        with col_date:
            if avail_dates:
                sel_date = st.selectbox("Trading Date", avail_dates)
            else:
                st.warning("No intraday data in DB.")
        with col_sym:
            sel_symbol = st.selectbox(
                "Symbol", all_symbols,
                help=f"{len(all_symbols)} active symbols",
            )

    elif data_source == "EOD Daily Bars":
        with col_date:
            eod_days = st.slider("Lookback (trading days)", 50, 500, 200, step=10)
        with col_sym:
            sel_symbol = st.selectbox(
                "Symbol", all_symbols,
                help=f"{len(all_symbols)} active symbols",
            )

    else:  # Demo Data
        with col_date:
            n_bars = st.slider("Bars (demo)", 200, 5000, 1000, step=100)
        with col_sym:
            st.info("Synthetic data")

    # ── Sidebar: VPIN & Game Theory parameters ────────────────────────────
    with st.sidebar:
        st.markdown("### VPIN Parameters")
        bucket_size_mode = st.radio("Bucket Size", ["Auto", "Manual"],
                                    index=0, horizontal=True)
        manual_bucket = None
        if bucket_size_mode == "Manual":
            manual_bucket = st.number_input("Volume per Bucket", 1000, 1000000,
                                            50000, step=5000)
        vpin_window = st.slider("VPIN Window (n buckets)", 10, 100, 50)
        st.markdown("---")
        st.markdown("### Game Theory")
        half_spread = st.number_input("Half-Spread (s)", 0.01, 5.0, 0.50,
                                      step=0.05, format="%.2f")
        adverse_loss = st.number_input("Adverse Loss (L)", 0.1, 20.0, 2.0,
                                       step=0.1, format="%.1f")

    # ── Load data ─────────────────────────────────────────────────────────
    tick_df = None

    if data_source == "Intraday Ticks" and sel_symbol and sel_date:
        tick_df = _load_intraday_ticks(sel_symbol, sel_date)
        if tick_df.empty:
            st.warning(f"No tick data for **{sel_symbol}** on {sel_date}.")
            tick_df = None
        else:
            st.success(
                f"**{sel_symbol}** — {len(tick_df):,} ticks on {sel_date} "
                f"· Total vol: {tick_df['volume'].sum():,.0f}",
                icon="📡",
            )

    elif data_source == "EOD Daily Bars" and sel_symbol:
        tick_df = _load_eod_data(sel_symbol, limit=eod_days)
        if tick_df.empty:
            st.warning(f"No EOD data for **{sel_symbol}**.")
            tick_df = None
        else:
            st.success(
                f"**{sel_symbol}** — {len(tick_df)} daily bars "
                f"· Total vol: {tick_df['volume'].sum():,.0f}",
                icon="📊",
            )

    if tick_df is None:
        tick_df = generate_dummy_tick_data(n_bars=n_bars)
        if data_source != "Demo Data":
            st.info("Falling back to demo data.", icon="🔬")
        else:
            st.info(f"Demo: {n_bars} simulated 1-min bars with "
                    "informed-trading burst at 80%.", icon="🔬")

    # ── Compute VPIN ──────────────────────────────────────────────────────
    result = compute_vpin(
        tick_df,
        bucket_size=manual_bucket,
        window=vpin_window,
    )

    # ── SECTION 1: Toxicity Monitor (VPIN Gauge) ─────────────────────────
    st.markdown("### Toxicity Monitor")
    col_gauge, col_stats = st.columns([2, 1])

    with col_gauge:
        _render_vpin_gauge(result.current_vpin)

    with col_stats:
        vpin_s = result.vpin_series
        card_bg = _COLORS["card_bg"]
        dim = _COLORS["text_dim"]
        stats_html = (
            f'<div style="background:{card_bg};padding:20px;border-radius:8px;'
            f'font-family:monospace;line-height:2.2;">'
            f'<div style="color:{dim};font-size:0.8em;margin-bottom:8px;">VPIN STATISTICS</div>'
            f'<div>Current: <b style="color:{_COLORS["vpin_line"]}">{result.current_vpin:.4f}</b></div>'
            f'<div>Mean: <b>{vpin_s.mean():.4f}</b></div>'
            f'<div>Max: <b style="color:{_COLORS["toxic"]}">{vpin_s.max():.4f}</b></div>'
            f'<div>Min: <b style="color:{_COLORS["safe"]}">{vpin_s.min():.4f}</b></div>'
            f'<div>Std Dev: <b>{vpin_s.std():.4f}</b></div>'
            f'<div style="margin-top:8px;color:{dim};font-size:0.75em;">'
            f'Buckets: {len(result.buckets)} &middot; Size: {result.dominant_bucket_size:,}</div>'
            f'</div>'
        )
        st.markdown(stats_html, unsafe_allow_html=True)

    # ── SECTION 2: Volume Bucket Flow ─────────────────────────────────────
    st.markdown("### Volume Bucket Flow")
    _render_bucket_flow(result.buckets)

    # VPIN time series below
    _render_vpin_timeseries(result.buckets)

    # ── SECTION 3: Game Theory Payoff Matrix ──────────────────────────────
    st.markdown("### Game Theory — Maker-Taker Payoff Matrix")
    st.caption("EV_make = (1 - π) × s  −  π × L")
    _render_payoff_matrix(result.current_vpin, half_spread, adverse_loss)

    # ── SECTION 4: Quant Analyst Commentary ───────────────────────────────
    payoff = evaluate_payoff(result.current_vpin, half_spread, adverse_loss)
    symbol_label = sel_symbol or "DEMO"

    with st.expander("Quant Analyst Commentary", expanded=True):
        use_ai = st.toggle("Enable Deep LLM Analysis (Ollama)", value=False)

        if not use_ai:
            commentary = get_vpin_rules_commentary(
                result.current_vpin, payoff.ev_make, half_spread,
            )
            st.markdown(commentary)
        else:
            with st.spinner("Generating institutional analysis..."):
                ai_text = get_vpin_ai_commentary(
                    result.current_vpin, payoff.ev_make,
                    symbol_label, half_spread,
                )
                if ai_text is None:
                    st.warning(
                        "Ollama not running. Start with: `sudo systemctl start ollama`"
                    )
                    commentary = get_vpin_rules_commentary(
                        result.current_vpin, payoff.ev_make, half_spread,
                    )
                    st.markdown(commentary)
                else:
                    from pakfindata.ui.components.commentary_renderer import render_styled_commentary
                    render_styled_commentary(ai_text, "Microstructure Analysis")

    # Normalize: ensure "price" column exists (some sources use "close")
    if tick_df is not None and "price" not in tick_df.columns and "close" in tick_df.columns:
        tick_df = tick_df.copy()
        tick_df["price"] = tick_df["close"]

    # ── SECTION 5: Volume Profile ──────────────────────────────────────────
    if tick_df is not None and "price" in tick_df.columns and len(tick_df) > 10:
        st.markdown("### Volume Profile")
        _render_volume_profile(tick_df)

    # ── SECTION 6: Trade Size Distribution ─────────────────────────────────
    if tick_df is not None and "volume" in tick_df.columns and len(tick_df) > 10:
        st.markdown("### Trade Size Distribution")
        _render_trade_size_distribution(tick_df)

    # ── SECTION 7: Tick-by-Tick Table ──────────────────────────────────────
    if tick_df is not None and "price" in tick_df.columns and len(tick_df) > 5:
        st.markdown("### Tick-by-Tick Table")
        _render_tick_table(tick_df)

    render_footer()
