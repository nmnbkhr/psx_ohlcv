"""PMEX Commodities Terminal — Professional trading analytics.

Tabs:
  1. Market Overview — quote board, volume treemap, session comparison
  2. Price Charts   — Candlestick + volume, multi-symbol overlay
  3. Gold Terminal   — Tola/oz pricing, cross-rate matrix, premium tracker
  4. Returns         — Heatmap, performance cards, drawdown analysis
  5. Margins         — Risk dashboard, margin-to-price ratio, limit bands
  6. Data Sync       — OHLC/Margins fetch, backfill, DB browser

Data stored in separate DB: /mnt/e/psxdata/commod/commod.db
"""

import sqlite3
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Design system
# ─────────────────────────────────────────────────────────────────────────────

_COLORS = {
    "gold": "#FFD700",
    "gold_dark": "#B8860B",
    "crude": "#FF6B35",
    "silver": "#C0C0C0",
    "fx": "#45B7D1",
    "index": "#9B59B6",
    "copper": "#B87333",
    "up": "#00C853",
    "down": "#FF1744",
    "neutral": "#78909C",
    "bg": "#0E1117",
    "card": "#1E1E2E",
    "text": "#E0E0E0",
    "accent": "#4ECDC4",
}

_CLASS_COLORS = {
    "Gold": _COLORS["gold"],
    "Crude Oil": _COLORS["crude"],
    "Silver": _COLORS["silver"],
    "FX": _COLORS["fx"],
    "Indices": _COLORS["index"],
    "Copper": _COLORS["copper"],
    "Platinum": "#E5E4E2",
    "Other": _COLORS["neutral"],
}

_CHART_LAYOUT = dict(
    plot_bgcolor="rgba(0,0,0,0)",
    paper_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11),
    margin=dict(l=10, r=10, t=35, b=10),
    xaxis=dict(gridcolor="rgba(255,255,255,0.05)", zeroline=False),
    yaxis=dict(gridcolor="rgba(255,255,255,0.08)", zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
)


def _styled_fig(height=380, **kwargs) -> go.Figure:
    """Create a pre-styled figure with dark theme."""
    fig = go.Figure()
    layout = {**_CHART_LAYOUT, "height": height, **kwargs}
    fig.update_layout(**layout)
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Constants & Schema
# ─────────────────────────────────────────────────────────────────────────────

COMMOD_DB_PATH = Path("/mnt/e/psxdata/commod/commod.db")
COMMOD_DATA_ROOT = Path("/mnt/e/psxdata/commod")
PMEX_OHLC_DIR = COMMOD_DATA_ROOT / "pmex_ohlc"
PMEX_MARGINS_DIR = COMMOD_DATA_ROOT / "pmex_margins"

COMMOD_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pmex_ohlc (
    trading_date     DATE NOT NULL,
    symbol           TEXT NOT NULL,
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    traded_volume    INTEGER DEFAULT 0,
    settlement_price REAL,
    fx_rate          REAL,
    fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trading_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_pmex_ohlc_sym  ON pmex_ohlc(symbol);
CREATE INDEX IF NOT EXISTS idx_pmex_ohlc_date ON pmex_ohlc(trading_date);

CREATE TABLE IF NOT EXISTS pmex_margins (
    report_date          DATE NOT NULL,
    sheet_name           TEXT NOT NULL,
    product_group        TEXT,
    contract_code        TEXT NOT NULL,
    reference_price      REAL,
    initial_margin_pct   REAL,
    initial_margin_value REAL,
    wcm                  REAL,
    maintenance_margin   REAL,
    lower_limit          REAL,
    upper_limit          REAL,
    fx_rate              REAL,
    is_active            BOOLEAN DEFAULT 1,
    fetched_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, contract_code)
);
CREATE INDEX IF NOT EXISTS idx_pmex_margins_date ON pmex_margins(report_date);
CREATE INDEX IF NOT EXISTS idx_pmex_margins_code ON pmex_margins(contract_code);
"""

# Symbol classification
_GOLD_PREFIXES = ("GOMOZ", "GO1OZ", "GO10OZ", "GO100OZ", "TOLAGOLD", "MTOLAGOLD")
_CRUDE_PREFIXES = ("CRUDE", "BRENT")
_SILVER_PREFIXES = ("SL100OZ", "SL1000OZ")
_INDEX_KEYWORDS = ("NSDQ", "SP500", "DJ30")
_FX_KEYWORDS = ("USD", "GBP", "EUR", "JPY", "CHF", "CAD", "AUD")


def _classify_symbol(sym: str) -> str:
    s = sym.upper().split("-")[0]
    # Gold oz contracts
    for p in _GOLD_PREFIXES:
        if s.startswith(p):
            return "Gold"
    # Gold cross-rate FX pairs (GOLDUSDJPY, GOLDGBPUSD, etc.)
    if s.startswith("GOLD"):
        return "FX"
    for p in _CRUDE_PREFIXES:
        if s.startswith(p):
            return "Crude Oil"
    for p in _SILVER_PREFIXES:
        if s.startswith(p):
            return "Silver"
    for k in _INDEX_KEYWORDS:
        if k in s:
            return "Indices"
    if s.startswith("DJ"):
        return "Indices"
    if "COPPER" in s:
        return "Copper"
    if "PLAT" in s:
        return "Platinum"
    if "ALUMINUM" in s:
        return "Metals"
    return "Other"


def _base_product(sym: str) -> str:
    """Extract base product from PMEX symbol (strip month code)."""
    parts = sym.rsplit("-", 1)
    return parts[0] if len(parts) == 2 else sym


def _chg_color(val: float) -> str:
    if val > 0:
        return _COLORS["up"]
    elif val < 0:
        return _COLORS["down"]
    return _COLORS["neutral"]


def _fmt_price(val, decimals=2) -> str:
    if val is None or pd.isna(val):
        return "—"
    if abs(val) >= 10000:
        return f"{val:,.0f}"
    return f"{val:,.{decimals}f}"


# ─────────────────────────────────────────────────────────────────────────────
# DB Connection
# ─────────────────────────────────────────────────────────────────────────────


def _get_commod_con() -> sqlite3.Connection | None:
    try:
        for d in [COMMOD_DATA_ROOT, PMEX_OHLC_DIR, PMEX_MARGINS_DIR]:
            d.mkdir(parents=True, exist_ok=True)
        con = sqlite3.connect(str(COMMOD_DB_PATH), check_same_thread=False, timeout=30)
        con.row_factory = sqlite3.Row
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA busy_timeout=30000")
        con.executescript(COMMOD_SCHEMA_SQL)
        con.commit()
        return con
    except Exception as e:
        st.error(f"Failed to connect to commod.db: {e}")
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main Render
# ─────────────────────────────────────────────────────────────────────────────


def render_pmex():
    st.markdown("## PMEX Commodities Terminal")

    con = _get_commod_con()
    if con is None:
        return

    # Top quote strip
    _render_quote_strip(con)

    tab_overview, tab_charts, tab_gold, tab_returns, tab_margins, tab_sync = st.tabs([
        "Market Overview", "Price Charts", "Gold Terminal",
        "Returns", "Margin Monitor", "Data Sync",
    ])

    with tab_overview:
        try:
            _render_market_overview(con)
        except Exception as e:
            st.error(f"Error: {e}")
    with tab_charts:
        try:
            _render_price_charts(con)
        except Exception as e:
            st.error(f"Error: {e}")
    with tab_gold:
        try:
            _render_gold_terminal(con)
        except Exception as e:
            st.error(f"Error: {e}")
    with tab_returns:
        try:
            _render_returns(con)
        except Exception as e:
            st.error(f"Error: {e}")
    with tab_margins:
        try:
            _render_margin_monitor(con)
        except Exception as e:
            st.error(f"Error: {e}")
    with tab_sync:
        try:
            _render_data_sync(con)
        except Exception as e:
            st.error(f"Error: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# QUOTE STRIP — Live-style ticker at top
# ═════════════════════════════════════════════════════════════════════════════


def _render_quote_strip(con: sqlite3.Connection):
    """Bloomberg-style quote strip: key benchmarks across the top."""
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT trading_date FROM pmex_ohlc ORDER BY trading_date DESC LIMIT 2"
    ).fetchall()]
    if not dates:
        return

    latest_dt, prev_dt = dates[0], dates[1] if len(dates) > 1 else dates[0]

    # Key benchmark symbols — pick the most liquid per asset class
    benchmarks = [
        ("GOMOZ", "Gold USD/oz", "$"),
        ("TOLAGOLD", "Tola Gold PKR", "Rs "),
        ("CRUDE10", "WTI Crude", "$"),
        ("BRENT10", "Brent Crude", "$"),
        ("SP500", "S&P 500", ""),
        ("NSDQ100", "Nasdaq 100", ""),
        ("SL100OZ", "Silver USD/oz", "$"),
    ]

    quotes = []
    for prefix, label, curr in benchmarks:
        row = con.execute(
            """SELECT symbol, close, traded_volume FROM pmex_ohlc
               WHERE symbol LIKE ? AND trading_date = ? AND traded_volume > 0
               ORDER BY traded_volume DESC LIMIT 1""",
            (f"{prefix}%", latest_dt),
        ).fetchone()
        if not row:
            continue

        prev_row = con.execute(
            """SELECT close FROM pmex_ohlc
               WHERE symbol = ? AND trading_date = ? AND traded_volume > 0""",
            (row["symbol"], prev_dt),
        ).fetchone()

        price = row["close"]
        prev_price = prev_row["close"] if prev_row else price
        chg = price - prev_price
        chg_pct = (chg / prev_price * 100) if prev_price else 0
        quotes.append((label, curr, price, chg, chg_pct))

    if not quotes:
        return

    cols = st.columns(len(quotes))
    for i, (label, curr, price, chg, chg_pct) in enumerate(quotes):
        with cols[i]:
            delta_str = f"{chg:+,.2f} ({chg_pct:+.1f}%)"
            st.metric(label, f"{curr}{_fmt_price(price)}", delta_str)

    st.caption(f"Session: **{latest_dt}** vs {prev_dt}")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1: MARKET OVERVIEW
# ═════════════════════════════════════════════════════════════════════════════


def _render_market_overview(con: sqlite3.Connection):
    latest_date = con.execute("SELECT MAX(trading_date) FROM pmex_ohlc").fetchone()[0]
    if not latest_date:
        st.info("No OHLC data. Go to **Data Sync** tab to fetch data.")
        return

    # Load latest + previous session
    prev_date = con.execute(
        "SELECT MAX(trading_date) FROM pmex_ohlc WHERE trading_date < ?", (latest_date,)
    ).fetchone()[0]

    df = pd.read_sql_query(
        "SELECT * FROM pmex_ohlc WHERE trading_date = ?", con, params=(latest_date,),
    )
    df["asset_class"] = df["symbol"].apply(_classify_symbol)
    df["base"] = df["symbol"].apply(_base_product)
    active = df[df["traded_volume"] > 0].copy()

    if prev_date:
        prev_df = pd.read_sql_query(
            "SELECT symbol, close, traded_volume FROM pmex_ohlc WHERE trading_date = ?",
            con, params=(prev_date,),
        )
        prev_map = dict(zip(prev_df["symbol"], prev_df["close"]))
        active["prev_close"] = active["symbol"].map(prev_map)
        active["chg"] = active["close"] - active["prev_close"]
        active["chg_pct"] = (active["chg"] / active["prev_close"] * 100).round(2)
    else:
        active["prev_close"] = active["close"]
        active["chg"] = 0
        active["chg_pct"] = 0

    # ── Volume treemap — what a trader scans first ──
    st.markdown("#### Volume Treemap")
    st.caption(f"Session: {latest_date} | {len(active)} active contracts | "
               f"Total volume: {active['traded_volume'].sum():,}")

    if not active.empty:
        tree_df = active[["symbol", "asset_class", "traded_volume", "chg_pct", "close"]].copy()
        tree_df = tree_df[tree_df["traded_volume"] > 0]

        # Build treemap using ids/parents (more reliable than labels/parents)
        ids = []
        labels = []
        parents = []
        values = []
        marker_colors = []

        # Parent nodes (asset class groups) — value will be filled after leaves
        cls_list = list(tree_df["asset_class"].unique())
        cls_vol = tree_df.groupby("asset_class")["traded_volume"].sum()
        for cls in cls_list:
            ids.append(f"cls_{cls}")
            labels.append(cls)
            parents.append("")
            values.append(int(cls_vol.get(cls, 0)))
            marker_colors.append(_CLASS_COLORS.get(cls, _COLORS["neutral"]))

        # Leaf nodes
        for _, r in tree_df.iterrows():
            ids.append(f"sym_{r['symbol']}")
            chg = r["chg_pct"] if pd.notna(r["chg_pct"]) else 0
            chg_str = f"+{chg:.1f}%" if chg >= 0 else f"{chg:.1f}%"
            labels.append(f"{r['symbol']}\n{_fmt_price(r['close'])}\nVol:{r['traded_volume']:,}\n{chg_str}")
            parents.append(f"cls_{r['asset_class']}")
            values.append(int(r["traded_volume"]))
            # Color: green for up, red for down
            if chg > 0.5:
                marker_colors.append("#1B5E20")
            elif chg < -0.5:
                marker_colors.append("#B71C1C")
            else:
                marker_colors.append("#37474F")

        fig = go.Figure(go.Treemap(
            ids=ids,
            labels=labels,
            parents=parents,
            values=values,
            marker=dict(colors=marker_colors, line=dict(width=1, color="#1a1a2e")),
            textinfo="label",
            textfont=dict(color="white", size=11),
            hovertemplate="<b>%{label}</b><br>Volume: %{value:,}<extra></extra>",
            branchvalues="total",
            pathbar=dict(visible=True),
        ))
        fig.update_layout(
            height=500, margin=dict(l=2, r=2, t=30, b=2),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Session movers: gainers & losers ──
    if "chg_pct" in active.columns and active["chg_pct"].notna().any():
        col_gain, col_lose = st.columns(2)

        with col_gain:
            st.markdown("#### Top Gainers")
            gainers = active.nlargest(8, "chg_pct")[["symbol", "asset_class", "close", "chg", "chg_pct", "traded_volume"]]
            for _, r in gainers.iterrows():
                st.markdown(
                    f"<div style='padding:4px 8px;margin:2px 0;border-left:3px solid {_COLORS['up']};'>"
                    f"<b>{r['symbol']}</b> &nbsp; {_fmt_price(r['close'])} &nbsp;"
                    f"<span style='color:{_COLORS['up']}'>+{r['chg_pct']:.1f}%</span> &nbsp;"
                    f"<span style='color:#888'>Vol: {r['traded_volume']:,}</span></div>",
                    unsafe_allow_html=True,
                )

        with col_lose:
            st.markdown("#### Top Losers")
            losers = active.nsmallest(8, "chg_pct")[["symbol", "asset_class", "close", "chg", "chg_pct", "traded_volume"]]
            for _, r in losers.iterrows():
                st.markdown(
                    f"<div style='padding:4px 8px;margin:2px 0;border-left:3px solid {_COLORS['down']};'>"
                    f"<b>{r['symbol']}</b> &nbsp; {_fmt_price(r['close'])} &nbsp;"
                    f"<span style='color:{_COLORS['down']}'>{r['chg_pct']:.1f}%</span> &nbsp;"
                    f"<span style='color:#888'>Vol: {r['traded_volume']:,}</span></div>",
                    unsafe_allow_html=True,
                )

    # ── Volume by asset class (horizontal stacked bar — cleaner than pie) ──
    st.markdown("#### Volume Breakdown")
    vol_by_class = active.groupby("asset_class")["traded_volume"].sum().sort_values(ascending=True)
    if not vol_by_class.empty:
        fig = _styled_fig(height=280)
        fig.add_trace(go.Bar(
            y=vol_by_class.index, x=vol_by_class.values,
            orientation="h",
            marker_color=[_CLASS_COLORS.get(c, _COLORS["neutral"]) for c in vol_by_class.index],
            text=[f"{v:,}" for v in vol_by_class.values],
            textposition="outside",
            textfont=dict(size=11),
        ))
        fig.update_layout(xaxis_title="Contracts Traded", showlegend=False)
        st.plotly_chart(fig, use_container_width=True)

    # ── Full quote board ──
    with st.expander("Full Quote Board", expanded=False):
        display = active[["symbol", "asset_class", "open", "high", "low", "close",
                          "chg_pct", "traded_volume", "settlement_price"]].copy()
        display.columns = ["Symbol", "Class", "Open", "High", "Low", "Close",
                           "Chg %", "Volume", "Settlement"]
        display = display.sort_values("Volume", ascending=False)
        st.dataframe(display, use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2: PRICE CHARTS
# ═════════════════════════════════════════════════════════════════════════════


def _render_price_charts(con: sqlite3.Connection):
    symbols = [r[0] for r in con.execute(
        """SELECT symbol, SUM(traded_volume) as vol FROM pmex_ohlc
           WHERE traded_volume > 0 GROUP BY symbol ORDER BY vol DESC"""
    ).fetchall()]
    if not symbols:
        st.info("No active OHLC data. Fetch data first.")
        return

    c1, c2, c3 = st.columns([3, 1, 1])
    with c1:
        sel_sym = st.selectbox("Symbol", symbols, key="pmex_chart_sym")
    with c2:
        chart_type = st.radio("Style", ["Candle", "Line"], horizontal=True, key="pmex_ctype")
    with c3:
        show_sma = st.checkbox("Show SMA", value=False, key="pmex_sma")

    df = pd.read_sql_query(
        """SELECT trading_date, open, high, low, close, traded_volume, settlement_price
           FROM pmex_ohlc WHERE symbol = ? AND traded_volume > 0 ORDER BY trading_date""",
        con, params=(sel_sym,),
    )

    if df.empty:
        st.info(f"No active trading data for {sel_sym}")
        return

    asset_class = _classify_symbol(sel_sym)
    color = _CLASS_COLORS.get(asset_class, _COLORS["accent"])

    # ── Price metrics strip ──
    latest = df.iloc[-1]
    prev = df.iloc[-2] if len(df) > 1 else latest
    chg = latest["close"] - prev["close"]
    pct = (chg / prev["close"] * 100) if prev["close"] else 0

    mc1, mc2, mc3, mc4, mc5, mc6 = st.columns(6)
    mc1.metric("Close", _fmt_price(latest["close"]), f"{chg:+,.2f} ({pct:+.1f}%)")
    mc2.metric("Open", _fmt_price(latest["open"]))
    mc3.metric("High", _fmt_price(latest["high"]))
    mc4.metric("Low", _fmt_price(latest["low"]))
    mc5.metric("Volume", f"{latest['traded_volume']:,}")
    mc6.metric("Settlement", _fmt_price(latest["settlement_price"]))

    # ── Candlestick / Line chart with volume ──
    from plotly.subplots import make_subplots

    fig = make_subplots(
        rows=2, cols=1, shared_xaxes=True,
        vertical_spacing=0.02, row_heights=[0.78, 0.22],
    )

    if chart_type == "Candle":
        fig.add_trace(go.Candlestick(
            x=df["trading_date"],
            open=df["open"], high=df["high"], low=df["low"], close=df["close"],
            name="OHLC",
            increasing=dict(line_color=_COLORS["up"], fillcolor="rgba(0,200,83,0.3)"),
            decreasing=dict(line_color=_COLORS["down"], fillcolor="rgba(255,23,68,0.3)"),
        ), row=1, col=1)
    else:
        fig.add_trace(go.Scatter(
            x=df["trading_date"], y=df["close"],
            mode="lines", name="Close",
            line=dict(color=color, width=2.5),
            fill="tozeroy", fillcolor=f"rgba({','.join(str(int(color.lstrip('#')[i:i+2], 16)) for i in (0, 2, 4))},0.08)",
        ), row=1, col=1)

    # Settlement overlay
    if df["settlement_price"].notna().any():
        fig.add_trace(go.Scatter(
            x=df["trading_date"], y=df["settlement_price"],
            mode="markers+lines", name="Settlement",
            line=dict(color="rgba(255,255,255,0.3)", width=1, dash="dot"),
            marker=dict(size=4, color="rgba(255,255,255,0.5)"),
        ), row=1, col=1)

    # SMA overlay
    if show_sma and len(df) >= 3:
        for period, clr in [(3, "rgba(255,215,0,0.6)"), (5, "rgba(78,205,196,0.6)")]:
            if len(df) >= period:
                sma = df["close"].rolling(period).mean()
                fig.add_trace(go.Scatter(
                    x=df["trading_date"], y=sma,
                    mode="lines", name=f"SMA-{period}",
                    line=dict(color=clr, width=1.2),
                ), row=1, col=1)

    # Volume bars
    vol_colors = [_COLORS["up"] if c >= o else _COLORS["down"]
                  for c, o in zip(df["close"], df["open"])]
    fig.add_trace(go.Bar(
        x=df["trading_date"], y=df["traded_volume"],
        name="Volume", marker_color=vol_colors, opacity=0.6,
        showlegend=False,
    ), row=2, col=1)

    fig.update_layout(
        **{**_CHART_LAYOUT, "legend": dict(orientation="h", y=1.05, x=0, bgcolor="rgba(0,0,0,0)")},
        height=520,
        xaxis_rangeslider_visible=False,
        title=dict(text=f"{sel_sym} ({asset_class})", font=dict(size=14)),
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    fig.update_yaxes(title_text="Vol", row=2, col=1)

    st.plotly_chart(fig, use_container_width=True)

    # ── Range analysis cards ──
    if len(df) > 1:
        rc1, rc2, rc3, rc4 = st.columns(4)
        period_high = df["high"].max()
        period_low = df["low"].min()
        range_pct = ((period_high - period_low) / period_low * 100) if period_low > 0 else 0
        avg_vol = df["traded_volume"].mean()
        rc1.metric("Period High", _fmt_price(period_high))
        rc2.metric("Period Low", _fmt_price(period_low))
        rc3.metric("Range", f"{range_pct:.1f}%")
        rc4.metric("Avg Volume", f"{avg_vol:,.0f}")

    # ── Multi-symbol comparison ──
    with st.expander("Compare Multiple Symbols"):
        compare_syms = st.multiselect("Select symbols", symbols[:30], max_selections=5,
                                       key="pmex_compare_syms")
        if len(compare_syms) >= 2:
            fig = _styled_fig(height=380, title=dict(text="Normalized Price Comparison (Base=100)"))
            palette = [_COLORS["gold"], _COLORS["crude"], _COLORS["accent"],
                       _COLORS["fx"], _COLORS["index"]]
            for i, sym in enumerate(compare_syms):
                sdf = pd.read_sql_query(
                    """SELECT trading_date, close FROM pmex_ohlc
                       WHERE symbol = ? AND traded_volume > 0 ORDER BY trading_date""",
                    con, params=(sym,),
                )
                if sdf.empty or sdf.iloc[0]["close"] <= 0:
                    continue
                sdf["norm"] = sdf["close"] / sdf.iloc[0]["close"] * 100
                fig.add_trace(go.Scatter(
                    x=sdf["trading_date"], y=sdf["norm"],
                    mode="lines+markers", name=sym,
                    line=dict(color=palette[i % len(palette)], width=2),
                    marker=dict(size=5),
                ))
            fig.add_hline(y=100, line_dash="dot", line_color="rgba(255,255,255,0.2)")
            st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3: GOLD TERMINAL
# ═════════════════════════════════════════════════════════════════════════════


def _render_gold_terminal(con: sqlite3.Connection):
    gold_df = pd.read_sql_query(
        """SELECT trading_date, symbol, open, high, low, close, traded_volume,
                  settlement_price, fx_rate
           FROM pmex_ohlc
           WHERE traded_volume > 0
             AND (symbol LIKE 'GOMOZ%' OR symbol LIKE 'GO1OZ%' OR symbol LIKE 'GO10OZ%'
                  OR symbol LIKE 'GO100OZ%'
                  OR symbol LIKE 'TOLAGOLD%' OR symbol LIKE 'MTOLAGOLD%'
                  OR symbol LIKE 'SL100OZ%' OR symbol LIKE 'SL1000OZ%')
           ORDER BY trading_date""",
        con,
    )

    if gold_df.empty:
        st.info("No gold/silver trading data. Fetch OHLC data first.")
        return

    latest_date = gold_df["trading_date"].max()
    latest = gold_df[gold_df["trading_date"] == latest_date]

    # ── Gold price strip ──
    usd_gold = latest[latest["symbol"].str.startswith("GOMOZ")]
    tola_gold = latest[latest["symbol"].str.startswith("TOLAGOLD")]
    mtola_gold = latest[latest["symbol"].str.startswith("MTOLAGOLD")]
    silver = latest[latest["symbol"].str.startswith("SL100OZ")]

    st.markdown(
        f"<div style='text-align:center;padding:6px;'>"
        f"<span style='font-size:11px;color:#888'>Session: {latest_date}</span></div>",
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4, c5 = st.columns(5)

    # Gold USD/oz
    with c1:
        if not usd_gold.empty:
            p = usd_gold.iloc[0]["close"]
            st.metric("Gold (USD/oz)", f"${p:,.2f}",
                      help=f"GOMOZ | Vol: {usd_gold.iloc[0]['traded_volume']:,}")
        else:
            st.metric("Gold (USD/oz)", "—")

    # Tola Gold PKR
    with c2:
        if not tola_gold.empty:
            p = tola_gold.iloc[0]["close"]
            st.metric("Tola Gold (PKR)", f"Rs {p:,.0f}",
                      help=f"TOLAGOLD | Vol: {tola_gold.iloc[0]['traded_volume']:,}")
        else:
            st.metric("Tola Gold (PKR)", "—")

    # Mini Tola (grams)
    with c3:
        if not mtola_gold.empty:
            p = mtola_gold.iloc[0]["close"]
            st.metric("Mini Tola Gold", f"Rs {p:,.0f}",
                      help=f"MTOLAGOLD | Vol: {mtola_gold.iloc[0]['traded_volume']:,}")
        else:
            st.metric("Mini Tola Gold", "—")

    # Silver
    with c4:
        if not silver.empty:
            p = silver.iloc[0]["close"]
            st.metric("Silver (USD/oz)", f"${p:,.2f}",
                      help=f"SL100OZ | Vol: {silver.iloc[0]['traded_volume']:,}")
        else:
            st.metric("Silver (USD/oz)", "—")

    # Gold/Silver ratio
    with c5:
        if not usd_gold.empty and not silver.empty:
            g = usd_gold.iloc[0]["close"]
            s = silver.iloc[0]["close"]
            ratio = g / s if s > 0 else 0
            st.metric("Gold/Silver Ratio", f"{ratio:.1f}x",
                      help="How many oz of silver per oz of gold")
        else:
            st.metric("Gold/Silver Ratio", "—")

    # ── Tola premium calculator ──
    fx = latest["fx_rate"].dropna()
    fx = fx[fx > 1]
    fx_rate = fx.mean() if not fx.empty else 0

    if not usd_gold.empty and not tola_gold.empty and fx_rate > 0:
        intl_usd = usd_gold.iloc[0]["close"]
        tola_pkr = tola_gold.iloc[0]["close"]
        # 1 tola = 11.6638g, 1 troy oz = 31.1035g => 1 tola = 0.375117 oz
        theoretical_tola = intl_usd * 0.375117 * fx_rate
        premium = tola_pkr - theoretical_tola
        premium_pct = (premium / theoretical_tola * 100) if theoretical_tola > 0 else 0

        st.markdown("---")
        pc1, pc2, pc3, pc4 = st.columns(4)
        pc1.metric("PKR/USD Rate", f"{fx_rate:,.2f}")
        pc2.metric("Theoretical Tola", f"Rs {theoretical_tola:,.0f}",
                   help="Intl gold * 0.375 oz/tola * FX rate")
        pc3.metric("Actual Tola", f"Rs {tola_pkr:,.0f}")
        pc4.metric("Local Premium", f"{premium_pct:+.1f}%",
                   f"Rs {premium:+,.0f}",
                   delta_color="inverse" if premium > 0 else "normal")

    # ── Gold price charts side by side ──
    st.markdown("---")
    col_usd, col_pkr = st.columns(2)

    with col_usd:
        usd_syms = gold_df[gold_df["symbol"].str.startswith("GOMOZ")]["symbol"].value_counts()
        if not usd_syms.empty:
            top_sym = usd_syms.index[0]
            hist = gold_df[gold_df["symbol"] == top_sym].sort_values("trading_date")
            fig = _styled_fig(height=320)
            fig.add_trace(go.Candlestick(
                x=hist["trading_date"],
                open=hist["open"], high=hist["high"], low=hist["low"], close=hist["close"],
                increasing=dict(line_color=_COLORS["gold"], fillcolor="rgba(255,215,0,0.3)"),
                decreasing=dict(line_color=_COLORS["down"], fillcolor="rgba(255,23,68,0.3)"),
                name=top_sym,
            ))
            fig.update_layout(
                title=dict(text=f"{top_sym} — Gold USD/oz", font=dict(size=12)),
                xaxis_rangeslider_visible=False,
            )
            st.plotly_chart(fig, use_container_width=True)

    with col_pkr:
        tola_syms = gold_df[gold_df["symbol"].str.startswith("TOLAGOLD")]["symbol"].value_counts()
        if not tola_syms.empty:
            top_sym = tola_syms.index[0]
            hist = gold_df[gold_df["symbol"] == top_sym].sort_values("trading_date")
            fig = _styled_fig(height=320)
            fig.add_trace(go.Candlestick(
                x=hist["trading_date"],
                open=hist["open"], high=hist["high"], low=hist["low"], close=hist["close"],
                increasing=dict(line_color=_COLORS["gold"], fillcolor="rgba(255,215,0,0.3)"),
                decreasing=dict(line_color=_COLORS["down"], fillcolor="rgba(255,23,68,0.3)"),
                name=top_sym,
            ))
            fig.update_layout(
                title=dict(text=f"{top_sym} — Tola Gold PKR", font=dict(size=12)),
                xaxis_rangeslider_visible=False,
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── Gold cross-rate matrix ──
    fx_gold = latest[latest["symbol"].str.startswith("GOLD") & latest["symbol"].str.contains("USD|GBP|EUR|JPY|CHF|CAD|AUD")]
    if not fx_gold.empty and len(fx_gold) >= 3:
        st.markdown("#### Gold Cross-Rate Contracts")
        display = fx_gold[["symbol", "close", "traded_volume", "settlement_price"]].copy()
        display.columns = ["Contract", "Price", "Volume", "Settlement"]
        display = display.sort_values("Volume", ascending=False)
        st.dataframe(display, use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 4: RETURNS
# ═════════════════════════════════════════════════════════════════════════════


def _render_returns(con: sqlite3.Connection):
    # Get symbols with at least 2 active days
    symbols_df = pd.read_sql_query(
        """SELECT symbol, COUNT(*) as days FROM pmex_ohlc
           WHERE traded_volume > 0 GROUP BY symbol HAVING days >= 2
           ORDER BY days DESC""",
        con,
    )
    if symbols_df.empty:
        st.info("Need at least 2 trading sessions for return calculations.")
        return

    returns_data = []
    for _, row in symbols_df.iterrows():
        sym = row["symbol"]
        prices = pd.read_sql_query(
            """SELECT trading_date, close, traded_volume FROM pmex_ohlc
               WHERE symbol = ? AND traded_volume > 0 ORDER BY trading_date""",
            con, params=(sym,),
        )
        if len(prices) < 2 or prices.iloc[0]["close"] <= 0:
            continue

        first_c, last_c = prices.iloc[0]["close"], prices.iloc[-1]["close"]
        ret = ((last_c - first_c) / first_c) * 100
        total_vol = prices["traded_volume"].sum()

        # Session-over-session return (latest)
        sos_ret = ((prices.iloc[-1]["close"] - prices.iloc[-2]["close"])
                   / prices.iloc[-2]["close"] * 100) if len(prices) > 1 else 0

        # Volatility
        prices["ret"] = prices["close"].pct_change()
        vol = prices["ret"].std() * (252 ** 0.5) * 100 if len(prices) > 2 else 0

        # Max drawdown
        cummax = prices["close"].cummax()
        dd = ((prices["close"] / cummax) - 1) * 100
        max_dd = dd.min()

        returns_data.append({
            "Symbol": sym, "Class": _classify_symbol(sym),
            "Sessions": int(row["days"]),
            "Last": last_c, "Return %": round(ret, 2),
            "Session Chg %": round(sos_ret, 2),
            "Vol %": round(vol, 1), "Max DD %": round(max_dd, 1),
            "Total Vol": total_vol,
        })

    if not returns_data:
        st.info("Insufficient data for return calculations")
        return

    ret_df = pd.DataFrame(returns_data)

    # Filters
    c1, c2 = st.columns(2)
    with c1:
        classes = ["All"] + sorted(ret_df["Class"].unique())
        sel_class = st.selectbox("Asset Class", classes, key="pmex_ret_class")
    with c2:
        sort_by = st.selectbox("Sort By",
                               ["Return %", "Session Chg %", "Vol %", "Total Vol", "Max DD %"],
                               key="pmex_ret_sort")

    if sel_class != "All":
        ret_df = ret_df[ret_df["Class"] == sel_class]

    ascending = sort_by == "Max DD %"
    ret_df = ret_df.sort_values(sort_by, ascending=ascending)

    st.caption(f"{len(ret_df)} symbols")

    # ── Returns heatmap by asset class ──
    if len(ret_df) >= 3:
        st.markdown("#### Returns Heatmap")

        # Build a pivot-like structure: class x symbol → return %
        hm_df = ret_df.nlargest(30, "Total Vol")[["Symbol", "Class", "Return %"]].copy()
        hm_classes = hm_df["Class"].unique()

        fig = _styled_fig(height=max(200, len(hm_df) * 22))
        fig.add_trace(go.Bar(
            y=hm_df["Symbol"], x=hm_df["Return %"],
            orientation="h",
            marker=dict(
                color=hm_df["Return %"],
                colorscale=[[0, _COLORS["down"]], [0.5, "#2D2D3D"], [1, _COLORS["up"]]],
                cmid=0,
                colorbar=dict(title="Return %", thickness=12),
            ),
            text=[f"{r:+.1f}%" for r in hm_df["Return %"]],
            textposition="outside",
            textfont=dict(size=10),
        ))
        fig.update_layout(
            title=dict(text="Period Returns (top 30 by volume)", font=dict(size=13)),
            xaxis_title="Return %", yaxis=dict(autorange="reversed"),
        )
        fig.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.2)")
        st.plotly_chart(fig, use_container_width=True)

    # ── Risk-return scatter ──
    if len(ret_df) >= 3:
        st.markdown("#### Risk-Return Map")
        fig = _styled_fig(height=420)

        # Size based on volume (log scale for visual clarity)
        vol_log = np.log1p(ret_df["Total Vol"])
        sizes = (vol_log / vol_log.max() * 25).clip(lower=6)

        fig.add_trace(go.Scatter(
            x=ret_df["Vol %"], y=ret_df["Return %"],
            mode="markers+text",
            text=ret_df["Symbol"].str[:12],
            textposition="top center",
            textfont=dict(size=8, color="rgba(255,255,255,0.6)"),
            marker=dict(
                size=sizes,
                color=[_CLASS_COLORS.get(c, _COLORS["neutral"]) for c in ret_df["Class"]],
                line=dict(width=1, color="rgba(255,255,255,0.2)"),
            ),
            hovertemplate="<b>%{text}</b><br>Return: %{y:.1f}%<br>Vol: %{x:.1f}%<extra></extra>",
        ))
        fig.update_layout(
            xaxis_title="Annualized Volatility %", yaxis_title="Return %",
            title=dict(text="Risk-Return (size = volume, color = asset class)", font=dict(size=13)),
        )
        fig.add_hline(y=0, line_dash="dot", line_color="rgba(255,255,255,0.15)")
        fig.add_vline(x=ret_df["Vol %"].median(), line_dash="dot",
                      line_color="rgba(255,255,255,0.1)")
        st.plotly_chart(fig, use_container_width=True)

    # ── Full table ──
    with st.expander("Full Returns Table", expanded=True):
        st.dataframe(ret_df, use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 5: MARGIN MONITOR
# ═════════════════════════════════════════════════════════════════════════════


def _render_margin_monitor(con: sqlite3.Connection):
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT report_date FROM pmex_margins ORDER BY report_date DESC LIMIT 30"
    ).fetchall()]
    if not dates:
        st.info("No margins data. Go to **Data Sync** tab.")
        return

    latest_date = dates[0]
    prev_date = dates[1] if len(dates) > 1 else None

    latest_df = pd.read_sql_query(
        """SELECT contract_code, product_group, reference_price,
                  initial_margin_pct, initial_margin_value, wcm,
                  maintenance_margin, lower_limit, upper_limit, fx_rate, is_active
           FROM pmex_margins WHERE report_date = ? AND is_active = 1
           ORDER BY product_group, contract_code""",
        con, params=(latest_date,),
    )

    if latest_df.empty:
        st.info(f"No active margins for {latest_date}")
        return

    # Clean up: fill missing product_group
    latest_df["product_group"] = latest_df["product_group"].fillna("Unclassified")

    # Compute margin-to-price ratio (margin value / (ref_price * fx_rate))
    denom = latest_df["reference_price"] * latest_df["fx_rate"].fillna(1)
    latest_df["margin_ratio_pct"] = (
        latest_df["initial_margin_value"] / denom.replace(0, np.nan) * 100
    ).round(2)

    st.caption(f"Report: **{latest_date}** | {len(latest_df)} active contracts")

    # ── Key metrics ──
    mc1, mc2, mc3, mc4, mc5 = st.columns(5)
    mc1.metric("Active Contracts", len(latest_df))
    mc2.metric("Product Groups", latest_df["product_group"].nunique())
    avg_margin = latest_df["initial_margin_value"].dropna().mean()
    mc3.metric("Avg Margin", f"{avg_margin:,.0f}" if avg_margin else "—")
    fx = latest_df["fx_rate"].dropna()
    fx = fx[fx > 1]
    mc4.metric("FX Rate", f"{fx.mean():,.2f}" if not fx.empty else "—")
    median_ratio = latest_df["margin_ratio_pct"].dropna().median()
    mc5.metric("Median Margin/Price", f"{median_ratio:.1f}%" if median_ratio else "—")

    # ── Margin treemap by product group ──
    st.markdown("#### Margin Requirements by Product")
    tree = latest_df.dropna(subset=["initial_margin_value"]).copy()
    if not tree.empty:
        tree["label"] = tree.apply(
            lambda r: f"{r['contract_code']}<br>"
                      f"Margin: {r['initial_margin_value']:,.0f}<br>"
                      f"Ref: {_fmt_price(r['reference_price'])}",
            axis=1,
        )

        # Build treemap with explicit parent nodes
        _grp_colors = {
            "Metals": "#FFD700", "ENERGY": "#FF6B35", "Currency Pairs": "#45B7D1",
            "Indices": "#9B59B6", "Financial": "#4ECDC4", "Unclassified": "#78909C",
        }
        m_ids = []
        m_labels = []
        m_parents = []
        m_values = []
        marker_colors = []

        grp_totals = tree.groupby("product_group")["initial_margin_value"].sum()
        for grp in tree["product_group"].unique():
            m_ids.append(f"grp_{grp}")
            m_labels.append(grp)
            m_parents.append("")
            m_values.append(float(grp_totals.get(grp, 0)))
            marker_colors.append(_grp_colors.get(grp, "#78909C"))

        for _, r in tree.iterrows():
            m_ids.append(f"c_{r['contract_code']}")
            m_labels.append(r["label"])
            m_parents.append(f"grp_{r['product_group']}")
            m_values.append(r["initial_margin_value"])
            # Vary by margin ratio — higher ratio = more red tint
            base = _grp_colors.get(r["product_group"], "#78909C").lstrip("#")
            rgb = tuple(int(base[i:i+2], 16) for i in (0, 2, 4))
            ratio = r["margin_ratio_pct"] if pd.notna(r["margin_ratio_pct"]) else 0
            factor = max(0.4, min(0.9, 0.7 - ratio / 100))
            adj = tuple(int(c * factor) for c in rgb)
            marker_colors.append(f"rgb({adj[0]},{adj[1]},{adj[2]})")

        fig = go.Figure(go.Treemap(
            ids=m_ids,
            labels=m_labels,
            parents=m_parents,
            values=m_values,
            marker=dict(colors=marker_colors),
            textinfo="label",
            textfont=dict(color="white", size=10),
            hovertemplate="<b>%{label}</b><br>Margin: %{value:,.0f}<extra></extra>",
            branchvalues="total",
        ))
        fig.update_layout(
            height=450, margin=dict(l=5, r=5, t=5, b=5),
            paper_bgcolor="rgba(0,0,0,0)",
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Summary by group ──
    group_summary = latest_df.groupby("product_group").agg(
        contracts=("contract_code", "count"),
        avg_margin=("initial_margin_value", "mean"),
        max_margin=("initial_margin_value", "max"),
        avg_ref=("reference_price", "mean"),
        avg_ratio=("margin_ratio_pct", "mean"),
    ).round(1).reset_index()
    group_summary.columns = ["Product Group", "Contracts", "Avg Margin",
                             "Max Margin", "Avg Ref Price", "Avg Margin/Price %"]
    st.dataframe(group_summary, use_container_width=True, hide_index=True)

    # ── Margin changes ──
    if prev_date:
        st.markdown(f"#### Margin Changes: {prev_date} vs {latest_date}")
        prev_df = pd.read_sql_query(
            """SELECT contract_code, initial_margin_value as prev_margin,
                      reference_price as prev_price
               FROM pmex_margins WHERE report_date = ? AND is_active = 1""",
            con, params=(prev_date,),
        )

        if not prev_df.empty:
            merged = latest_df.merge(prev_df, on="contract_code", how="inner")
            merged["margin_chg"] = merged["initial_margin_value"] - merged["prev_margin"]
            merged["margin_chg_pct"] = (
                merged["margin_chg"] / merged["prev_margin"].replace(0, np.nan) * 100
            ).round(2)
            merged["price_chg_pct"] = (
                (merged["reference_price"] - merged["prev_price"])
                / merged["prev_price"].replace(0, np.nan) * 100
            ).round(2)

            changed = merged[merged["margin_chg"].abs() > 0.01].sort_values(
                "margin_chg", ascending=False
            )

            if not changed.empty:
                tightened = len(changed[changed["margin_chg"] > 0])
                eased = len(changed[changed["margin_chg"] < 0])

                tc1, tc2, tc3 = st.columns(3)
                tc1.metric("Changes", len(changed))
                tc2.metric("Tightened", tightened,
                           delta=f"+{tightened}", delta_color="inverse")
                tc3.metric("Eased", eased,
                           delta=f"-{eased}", delta_color="normal")

                # Margin change waterfall
                top_chg = changed.head(20)
                fig = _styled_fig(height=380)
                fig.add_trace(go.Bar(
                    x=top_chg["contract_code"], y=top_chg["margin_chg"],
                    marker_color=[_COLORS["down"] if c > 0 else _COLORS["up"]
                                  for c in top_chg["margin_chg"]],
                    text=[f"{c:+,.0f}" for c in top_chg["margin_chg"]],
                    textposition="outside", textfont=dict(size=9),
                ))
                fig.update_layout(
                    title=dict(text="Margin Changes (red = tightened, green = eased)",
                               font=dict(size=12)),
                    yaxis_title="Change",
                    xaxis_tickangle=-45,
                )
                fig.add_hline(y=0, line_dash="dash", line_color="rgba(255,255,255,0.15)")
                st.plotly_chart(fig, use_container_width=True)

                with st.expander("Change Details"):
                    display = changed[["contract_code", "product_group", "reference_price",
                                       "prev_price", "price_chg_pct",
                                       "initial_margin_value", "prev_margin",
                                       "margin_chg", "margin_chg_pct"]].copy()
                    display.columns = ["Contract", "Group", "Price", "Prev Price", "Price Chg %",
                                       "Margin", "Prev Margin", "Margin Chg", "Margin Chg %"]
                    st.dataframe(display, use_container_width=True, hide_index=True)
            else:
                st.success(f"No margin changes between sessions")

    # ── Price limit bands (visual range chart) ──
    st.markdown("#### Price Limit Bands")
    limits = latest_df.dropna(subset=["lower_limit", "upper_limit"]).copy()
    if not limits.empty:
        limits["band_pct"] = (
            (limits["upper_limit"] - limits["lower_limit"])
            / limits["reference_price"].replace(0, np.nan) * 100
        ).round(1)

        # Show as range bars
        top_limits = limits.nlargest(20, "band_pct")
        fig = _styled_fig(height=max(250, len(top_limits) * 22))
        fig.add_trace(go.Bar(
            y=top_limits["contract_code"],
            x=top_limits["band_pct"],
            orientation="h",
            marker=dict(
                color=top_limits["band_pct"],
                colorscale=[[0, _COLORS["accent"]], [1, _COLORS["crude"]]],
            ),
            text=[f"{b:.1f}%" for b in top_limits["band_pct"]],
            textposition="outside", textfont=dict(size=10),
        ))
        fig.update_layout(
            title=dict(text="Price Limit Band Width (% of ref price)", font=dict(size=12)),
            xaxis_title="Band Width %",
            yaxis=dict(autorange="reversed"),
        )
        st.plotly_chart(fig, use_container_width=True)

    # ── Full table ──
    with st.expander("All Active Contracts"):
        display = latest_df[["contract_code", "product_group", "reference_price",
                              "initial_margin_value", "margin_ratio_pct", "wcm",
                              "lower_limit", "upper_limit", "fx_rate"]].copy()
        display.columns = ["Contract", "Group", "Ref Price", "Margin",
                           "Margin/Price %", "WCM", "Lower", "Upper", "FX"]
        st.dataframe(display.sort_values("Margin", ascending=False),
                     use_container_width=True, hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 6: DATA SYNC (preserved from original)
# ═════════════════════════════════════════════════════════════════════════════


def _render_data_sync(con: sqlite3.Connection):
    _render_ohlc_section(con)
    st.divider()
    _render_margins_section(con)
    st.divider()
    with st.expander("Backfill", expanded=False):
        _render_backfill(con)
    with st.expander("View Database", expanded=False):
        _render_view_db(con)


def _render_ohlc_section(con: sqlite3.Connection):
    st.markdown("### OHLC Data")
    col_from, col_to = st.columns(2)
    with col_from:
        ohlc_from = st.date_input("From", value=date.today() - timedelta(days=30), key="ohlc_from_date")
    with col_to:
        ohlc_to = st.date_input("To", value=date.today(), key="ohlc_to_date")

    active_only = st.checkbox("Active only (volume > 0)", key="ohlc_active_only")
    has_data = "pmex_ohlc_df" in st.session_state and st.session_state["pmex_ohlc_df"] is not None

    btn1, btn2, btn3 = st.columns(3)
    with btn1:
        if st.button("Get OHLC Data", type="primary", use_container_width=True):
            _do_ohlc_fetch(ohlc_from, ohlc_to)
    with btn2:
        if st.button("Save to Disk", disabled=not has_data, use_container_width=True):
            _do_ohlc_save(ohlc_from, ohlc_to)
    with btn3:
        if st.button("Populate to DB", disabled=not has_data, use_container_width=True):
            _do_ohlc_populate(con)

    if has_data:
        df = st.session_state["pmex_ohlc_df"]
        if df is not None and not df.empty:
            display_df = df.copy()
            if active_only:
                display_df = display_df[display_df["traded_volume"] > 0]
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Rows", len(display_df))
            mc2.metric("Active", len(display_df[display_df["traded_volume"] > 0]))
            mc3.metric("Symbols", display_df["symbol"].nunique())
            st.dataframe(display_df, use_container_width=True, hide_index=True)


def _do_ohlc_fetch(from_date, to_date):
    with st.spinner("Fetching OHLC..."):
        try:
            from pakfindata.commodities.fetcher_pmex_ohlc import fetch_ohlc
            df = fetch_ohlc(from_date, to_date)
            if df.empty:
                st.warning("No data returned.")
                st.session_state["pmex_ohlc_df"] = None
                st.session_state["pmex_ohlc_raw"] = None
            else:
                df["trading_date"] = df["trading_date"].dt.strftime("%Y-%m-%d")
                st.session_state["pmex_ohlc_df"] = df
                st.session_state["pmex_ohlc_raw"] = df.to_dict("records")
                st.success(f"{len(df)} rows, {df['symbol'].nunique()} symbols")
                st.rerun()
        except Exception as e:
            st.error(f"Fetch failed: {e}")


def _do_ohlc_save(from_date, to_date):
    raw = st.session_state.get("pmex_ohlc_raw")
    if not raw:
        st.warning("No data. Fetch first.")
        return
    try:
        import json
        PMEX_OHLC_DIR.mkdir(parents=True, exist_ok=True)
        fpath = PMEX_OHLC_DIR / f"ohlc_{from_date.isoformat()}_{to_date.isoformat()}.json"
        with open(fpath, "w") as f:
            json.dump(raw, f, indent=2, default=str)
        st.success(f"Saved: {fpath} ({fpath.stat().st_size / 1024:.1f} KB)")
    except Exception as e:
        st.error(f"Save failed: {e}")


def _do_ohlc_populate(con):
    df = st.session_state.get("pmex_ohlc_df")
    if df is None or df.empty:
        st.warning("No data. Fetch first.")
        return
    try:
        con.executemany(
            """INSERT OR REPLACE INTO pmex_ohlc
               (trading_date, symbol, open, high, low, close,
                traded_volume, settlement_price, fx_rate)
               VALUES (:trading_date, :symbol, :open, :high, :low, :close,
                        :traded_volume, :settlement_price, :fx_rate)""",
            df.to_dict("records"),
        )
        con.commit()
        st.success(f"Populated {len(df)} rows")
        st.rerun()
    except Exception as e:
        st.error(f"Failed: {e}")


def _render_margins_section(con):
    st.markdown("### Margins Data")
    margins_date = st.date_input("Report Date", value=date.today(), key="margins_report_date")
    active_only = st.checkbox("Active only (is_active = 1)", key="margins_active_only")
    has_data = "pmex_margins_df" in st.session_state and st.session_state["pmex_margins_df"] is not None

    btn4, btn5, btn6 = st.columns(3)
    with btn4:
        if st.button("Get Margins", type="primary", use_container_width=True):
            _do_margins_fetch(margins_date)
    with btn5:
        if st.button("Save to Disk", disabled=not has_data, use_container_width=True, key="margins_save_btn"):
            _do_margins_save()
    with btn6:
        if st.button("Populate to DB", disabled=not has_data, use_container_width=True, key="margins_pop_btn"):
            _do_margins_populate(con)

    from pakfindata.commodities.fetcher_pmex_margins import margins_url
    st.caption("Auto-download uses Chrome (Cloudflare bypass). Manual fallback below.")
    st.code(margins_url(margins_date), language=None)

    uploaded = st.file_uploader("Upload Margins Excel (.xlsx)", type=["xlsx"], key="margins_upload")
    if uploaded is not None:
        _do_margins_upload(uploaded, margins_date)

    if has_data:
        df = st.session_state["pmex_margins_df"]
        if df is not None and not df.empty:
            display_df = df if not active_only else df[df["is_active"] == True]  # noqa: E712
            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("Contracts", len(display_df))
            mc2.metric("Active", int(display_df["is_active"].sum()))
            mc3.metric("Groups", display_df["product_group"].dropna().nunique())
            st.dataframe(display_df, use_container_width=True, hide_index=True)


def _do_margins_fetch(target_date):
    with st.spinner("Downloading margins..."):
        try:
            from pakfindata.commodities.fetcher_pmex_margins import fetch_margins_file, parse_margins_excel
            raw_bytes, actual_date = fetch_margins_file(target_date, walk_back_days=5)
            if raw_bytes is None:
                st.warning("No margins file found within 5 days.")
                st.session_state["pmex_margins_df"] = None
                return
            df = parse_margins_excel(raw_bytes, actual_date)
            if df.empty:
                st.warning(f"File for {actual_date} parsed but no data.")
                return
            st.session_state["pmex_margins_df"] = df
            st.session_state["pmex_margins_bytes"] = raw_bytes
            st.session_state["pmex_margins_date"] = actual_date
            st.success(f"Fetched {actual_date}: {len(df)} contracts ({int(df['is_active'].sum())} active)")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


def _do_margins_save():
    raw = st.session_state.get("pmex_margins_bytes")
    dt = st.session_state.get("pmex_margins_date")
    if not raw or not dt:
        st.warning("No data. Fetch first.")
        return
    try:
        PMEX_MARGINS_DIR.mkdir(parents=True, exist_ok=True)
        fpath = PMEX_MARGINS_DIR / f"Margins-{dt.strftime('%d-%m-%Y')}.xlsx"
        with open(fpath, "wb") as f:
            f.write(raw)
        st.success(f"Saved: {fpath} ({fpath.stat().st_size / 1024:.1f} KB)")
    except Exception as e:
        st.error(f"Failed: {e}")


def _do_margins_upload(uploaded_file, report_date):
    try:
        from pakfindata.commodities.fetcher_pmex_margins import parse_margins_excel
        raw_bytes = uploaded_file.read()
        df = parse_margins_excel(raw_bytes, report_date)
        if df.empty:
            st.warning("Could not parse uploaded file.")
            return
        st.session_state["pmex_margins_df"] = df
        st.session_state["pmex_margins_bytes"] = raw_bytes
        st.session_state["pmex_margins_date"] = report_date
        st.success(f"Parsed: {len(df)} contracts ({int(df['is_active'].sum())} active)")
        st.rerun()
    except Exception as e:
        st.error(f"Failed: {e}")


def _do_margins_populate(con):
    df = st.session_state.get("pmex_margins_df")
    if df is None or df.empty:
        st.warning("No data. Fetch first.")
        return
    try:
        con.executemany(
            """INSERT OR REPLACE INTO pmex_margins
               (report_date, sheet_name, product_group, contract_code,
                reference_price, initial_margin_pct, initial_margin_value,
                wcm, maintenance_margin, lower_limit, upper_limit,
                fx_rate, is_active)
               VALUES (:report_date, :sheet_name, :product_group, :contract_code,
                        :reference_price, :initial_margin_pct, :initial_margin_value,
                        :wcm, :maintenance_margin, :lower_limit, :upper_limit,
                        :fx_rate, :is_active)""",
            df.to_dict("records"),
        )
        con.commit()
        st.success(f"Populated {len(df)} rows")
        st.rerun()
    except Exception as e:
        st.error(f"Failed: {e}")


# ─── Backfill & View DB ──────────────────────────────────────────────────────


def _render_backfill(con):
    tab_ohlc, tab_margins = st.tabs(["OHLC Backfill", "Margins Backfill"])
    with tab_ohlc:
        _render_ohlc_backfill(con)
    with tab_margins:
        _render_margins_backfill(con)


def _render_ohlc_backfill(con):
    c1, c2 = st.columns(2)
    with c1:
        bf_from = st.date_input("Start", value=date(2024, 1, 1), key="ohlc_bf_from")
    with c2:
        bf_to = st.date_input("End", value=date.today(), key="ohlc_bf_to")
    bf_active = st.checkbox("Active only", key="ohlc_bf_active")
    bf_save = st.checkbox("Save JSON files", key="ohlc_bf_save")

    if st.button("Start OHLC Backfill", type="primary", key="ohlc_bf_start"):
        from pakfindata.commodities.fetcher_pmex_ohlc import fetch_ohlc
        import requests as _requests, time, json

        progress = st.progress(0, text="Starting...")
        session = _requests.Session()
        all_chunks = []
        cur = bf_from
        total_chunks = max(1, ((bf_to - bf_from).days + 89) // 90)
        chunk_num = 0

        try:
            while cur < bf_to:
                chunk_end = min(cur + timedelta(days=89), bf_to)
                chunk_num += 1
                progress.progress(chunk_num / total_chunks, text=f"Chunk {chunk_num}/{total_chunks}")
                df = fetch_ohlc(cur, chunk_end, session)
                if not df.empty:
                    if bf_active:
                        df = df[df["traded_volume"] > 0]
                    df["trading_date"] = df["trading_date"].dt.strftime("%Y-%m-%d")
                    if bf_save:
                        PMEX_OHLC_DIR.mkdir(parents=True, exist_ok=True)
                        with open(PMEX_OHLC_DIR / f"ohlc_{cur}_{chunk_end}.json", "w") as f:
                            json.dump(df.to_dict("records"), f, indent=2, default=str)
                    con.executemany(
                        """INSERT OR REPLACE INTO pmex_ohlc
                           (trading_date, symbol, open, high, low, close,
                            traded_volume, settlement_price, fx_rate)
                           VALUES (:trading_date, :symbol, :open, :high, :low, :close,
                                    :traded_volume, :settlement_price, :fx_rate)""",
                        df.to_dict("records"),
                    )
                    con.commit()
                    all_chunks.append(df)
                cur = chunk_end + timedelta(days=1)
                if cur < bf_to:
                    time.sleep(2.0)
            progress.progress(1.0, text="Done!")
            st.success(f"Backfill: {sum(len(c) for c in all_chunks)} rows from {chunk_num} chunks")
            st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


def _render_margins_backfill(con):
    c1, c2 = st.columns(2)
    with c1:
        bf_from = st.date_input("Start", value=date.today() - timedelta(days=30), key="margins_bf_from")
    with c2:
        bf_to = st.date_input("End", value=date.today(), key="margins_bf_to")

    if st.button("Start Margins Backfill", type="primary", key="margins_bf_start"):
        from pakfindata.commodities.fetcher_pmex_margins import backfill_margins
        progress = st.progress(0, text="Launching Chrome...")
        try:
            def _prog(cur, total, label):
                progress.progress(min(cur / total, 1.0) if total else 0, text=label)
            df = backfill_margins(start_date=bf_from, end_date=bf_to, progress_callback=_prog)
            if df.empty:
                st.warning("No data found.")
            else:
                con.executemany(
                    """INSERT OR REPLACE INTO pmex_margins
                       (report_date, sheet_name, product_group, contract_code,
                        reference_price, initial_margin_pct, initial_margin_value,
                        wcm, maintenance_margin, lower_limit, upper_limit,
                        fx_rate, is_active)
                       VALUES (:report_date, :sheet_name, :product_group, :contract_code,
                                :reference_price, :initial_margin_pct, :initial_margin_value,
                                :wcm, :maintenance_margin, :lower_limit, :upper_limit,
                                :fx_rate, :is_active)""",
                    df.to_dict("records"),
                )
                con.commit()
                st.success(f"Done: {len(df)} rows from {df['report_date'].nunique()} dates")
                st.rerun()
        except Exception as e:
            st.error(f"Failed: {e}")


def _render_view_db(con):
    tab_o, tab_m = st.tabs(["OHLC", "Margins"])
    with tab_o:
        _render_view_ohlc(con)
    with tab_m:
        _render_view_margins(con)


def _render_view_ohlc(con):
    symbols = [r[0] for r in con.execute("SELECT DISTINCT symbol FROM pmex_ohlc ORDER BY symbol").fetchall()]
    if not symbols:
        st.info("No data.")
        return
    c1, c2, c3 = st.columns(3)
    with c1:
        sel = st.selectbox("Symbol", ["All"] + symbols, key="view_ohlc_sym")
    with c2:
        active = st.checkbox("Active only", key="view_ohlc_active")
    with c3:
        limit = st.number_input("Limit", 50, 5000, 500, key="view_ohlc_limit")

    conds, params = [], []
    if sel != "All":
        conds.append("symbol = ?"); params.append(sel)
    if active:
        conds.append("traded_volume > 0")
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    params.append(limit)
    rows = con.execute(f"SELECT * FROM pmex_ohlc {where} ORDER BY trading_date DESC, symbol LIMIT ?", params).fetchall()
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("CSV", df.to_csv(index=False), "pmex_ohlc.csv", "text/csv")


def _render_view_margins(con):
    dates = [r[0] for r in con.execute(
        "SELECT DISTINCT report_date FROM pmex_margins ORDER BY report_date DESC LIMIT 30"
    ).fetchall()]
    if not dates:
        st.info("No data.")
        return
    c1, c2 = st.columns(2)
    with c1:
        sel_dt = st.selectbox("Date", dates, key="view_margins_date")
    with c2:
        active = st.checkbox("Active only", key="view_margins_active")

    conds = ["report_date = ?"]
    params = [sel_dt]
    if active:
        conds.append("is_active = 1")
    rows = con.execute(
        f"SELECT * FROM pmex_margins WHERE {' AND '.join(conds)} ORDER BY product_group, contract_code",
        params,
    ).fetchall()
    if rows:
        df = pd.DataFrame([dict(r) for r in rows])
        st.dataframe(df, use_container_width=True, hide_index=True)
        st.download_button("CSV", df.to_csv(index=False), f"margins_{sel_dt}.csv", "text/csv")
