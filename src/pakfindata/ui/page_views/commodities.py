"""Commodities Dashboard — Global commodity prices with Pakistan context.

Tabs:
  Dashboard — KPIs: Gold PKR/Tola, Brent, Cotton, USD/PKR + sparklines
  Charts — Interactive candlestick/line with volume subplot, SMA overlays
  Categories — Browse by category, daily change heatmap, correlation matrix
  Pakistan View — PKR prices, gold premium analysis, local unit charts
  Local Markets — khistocks.com Pakistan data with visual price cards
  PMEX Portal — direct PMEX market watch data
  Export — CSV download

Phase 1.7.G.3.1 — all reads go through ``/v1/commodities``,
``/v1/khistocks``, ``/v1/pmex-portal``. Sync buttons remain — they call
``pakfindata.commodities.sync.*`` directly (engine domain, Phase 1.6
pattern; the sync functions open their own write connections and run
``init_commodity_schema`` internally so the page does not need a DB
handle).
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from pakfindata.ui.api import client as api_client


# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM — consistent dark theme
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676",
    "down": "#FF5252",
    "neutral": "#78909C",
    "accent": "#00D4AA",
    "gold": "#FFD700",
    "energy": "#FF6B35",
    "agri": "#66BB6A",
    "metals": "#42A5F5",
    "fx": "#AB47BC",
    "bg": "#0e1117",
    "card_bg": "#1a1a2e",
    "grid": "#2d2d3d",
    "text": "#e0e0e0",
    "text_dim": "#888888",
}

_CAT_COLORS = {
    "metals": _COLORS["metals"],
    "energy": _COLORS["energy"],
    "agriculture": _COLORS["agri"],
    "fx": _COLORS["fx"],
    "livestock": "#8D6E63",
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11),
    xaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    yaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    legend=dict(bgcolor="rgba(0,0,0,0)"),
    margin=dict(l=10, r=10, t=40, b=10),
)


def _styled_fig(height: int = 400, **kwargs) -> go.Figure:
    layout = {**_CHART_LAYOUT, "height": height, **kwargs}
    return go.Figure(layout=layout)


def _fmt_price(val, decimals=2):
    if val is None or pd.isna(val):
        return "—"
    if abs(val) >= 1000:
        return f"{val:,.{decimals}f}"
    return f"{val:.{decimals}f}"


def _change_color(val):
    if val > 0:
        return _COLORS["up"]
    elif val < 0:
        return _COLORS["down"]
    return _COLORS["neutral"]


def _metric_card(label, value, delta=None, prefix="", suffix=""):
    """Render a styled metric card via HTML."""
    delta_html = ""
    if delta is not None and not pd.isna(delta):
        color = _change_color(delta)
        sign = "+" if delta > 0 else ""
        delta_html = (
            f"<span style='color:{color};font-size:0.85em;'>"
            f"{sign}{delta:.2f}%</span>"
        )
    card_bg = _COLORS["card_bg"]
    accent = _COLORS["accent"]
    text_dim = _COLORS["text_dim"]
    st.markdown(
        f"<div style='background:{card_bg};border-radius:8px;"
        f"padding:12px 16px;border-left:3px solid {accent};'>"
        f"<div style='color:{text_dim};font-size:0.75em;'>{label}</div>"
        f"<div style='font-size:1.3em;font-weight:600;'>{prefix}{value}{suffix}</div>"
        f"{delta_html}</div>",
        unsafe_allow_html=True,
    )


# ═════════════════════════════════════════════════════════════════════════════
# RENDER ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def render_commodities():
    st.markdown("## Commodities Terminal")

    api_client.render_api_status_banner_if_down()

    has_data = api_client.get_commodity_has_data() or {
        "commodity_eod": 0,
        "khistocks_prices": 0,
        "pmex_market_watch": 0,
        "has_any": False,
    }

    if not has_data.get("has_any"):
        _render_empty_state()
        return

    tab_dash, tab_charts, tab_categories, tab_pk, tab_local, tab_pmex, tab_export = st.tabs([
        "Dashboard", "Charts", "Categories", "Pakistan View",
        "Local Markets", "PMEX Portal", "Export",
    ])

    for tab, renderer in [
        (tab_dash, _render_dashboard),
        (tab_charts, _render_charts),
        (tab_categories, _render_categories),
        (tab_pk, _render_pakistan_view),
        (tab_local, lambda: _render_local_markets(has_data)),
        (tab_pmex, lambda: _render_pmex_portal(has_data)),
        (tab_export, _render_export),
    ]:
        with tab:
            try:
                renderer()
            except Exception as e:
                st.error(f"Error: {e}")

    st.divider()
    _render_sync_controls()


def _render_empty_state():
    st.info(
        "No commodity data found. Run the initial sync to populate data.\n\n"
        "**CLI:** `pfsync commodity sync --all`\n\n"
        "Or use the sync button below."
    )
    if st.button("Seed Commodity Universe & Sync (yfinance)", type="primary"):
        with st.spinner("Seeding commodity universe and syncing from yfinance..."):
            try:
                from pakfindata.commodities.sync import seed_commodity_universe, sync_yfinance
                seed_commodity_universe()
                summary = sync_yfinance(incremental=False, period="1y")
                st.success(
                    f"Synced {summary.symbols_ok}/{summary.symbols_total} commodities, "
                    f"{summary.rows_upserted} rows upserted."
                )
                st.rerun()
            except Exception as e:
                st.error(f"Sync failed: {e}")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1: DASHBOARD — KPIs + sparklines + sector performance
# ═════════════════════════════════════════════════════════════════════════════

def _render_dashboard():
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    key_symbols = ["GOLD", "BRENT", "COTTON", "WHEAT", "NATURAL_GAS", "USD_PKR", "SUGAR", "COPPER"]

    latest_rows = api_client.get_commodity_latest(key_symbols) or []
    latest = {r["symbol"]: r for r in latest_rows}

    if not latest:
        st.warning("No recent commodity data. Run a sync first.")
        return

    # ── KPI cards — 4 per row ──
    st.markdown("### Market Snapshot")
    row1 = st.columns(4)
    row2 = st.columns(4)
    all_cols = row1 + row2

    for i, sym in enumerate(key_symbols):
        if sym not in latest:
            continue
        data = latest[sym]
        cdef = COMMODITY_UNIVERSE.get(sym)
        name = cdef.name if cdef else sym
        unit = cdef.unit if cdef else ""
        price = data.get("close")
        prev = data.get("prev_close")
        delta = ((price - prev) / prev * 100) if price and prev and prev != 0 else None
        with all_cols[i]:
            _metric_card(f"{name} ({unit})", _fmt_price(price), delta)

    # ── Sparklines — 4 key commodities ──
    st.markdown("### 30-Day Trends")
    spark_cols = st.columns(4)
    for i, sym in enumerate(key_symbols[:4]):
        with spark_cols[i]:
            _render_sparkline(sym)

    # ── Sector performance bar ──
    st.markdown("### Sector Performance (Latest Session)")
    sector_data = api_client.get_commodity_sector_performance() or []

    if sector_data:
        cats = [r["category"] for r in sector_data]
        chgs = [r["avg_chg"] for r in sector_data]
        bar_colors = [_COLORS["up"] if c >= 0 else _COLORS["down"] for c in chgs]
        fig = _styled_fig(height=220)
        fig.add_trace(go.Bar(
            y=cats, x=chgs, orientation="h",
            marker_color=bar_colors,
            text=[f"{c:+.2f}%" for c in chgs],
            textposition="outside", textfont=dict(size=11),
        ))
        fig.update_layout(xaxis_title="Avg Daily Change %", showlegend=False)
        st.plotly_chart(fig, width='stretch')

    # ── PKR prices summary ──
    pkr_rows = api_client.get_commodity_pkr_latest() or []

    if pkr_rows:
        st.markdown("### Pakistan Prices (PKR)")
        pkr_df = pd.DataFrame(pkr_rows)
        pkr_df["name"] = pkr_df["symbol"].map(
            lambda s: COMMODITY_UNIVERSE[s].name if s in COMMODITY_UNIVERSE else s
        )
        pkr_df = pkr_df[["name", "symbol", "pkr_price", "pk_unit", "date", "usd_pkr"]]
        pkr_df.columns = ["Commodity", "Symbol", "PKR Price", "Unit", "Date", "USD/PKR"]
        st.dataframe(pkr_df, width='stretch', hide_index=True)


def _render_sparkline(symbol: str):
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    rows = api_client.get_commodity_eod(symbol, limit=30) or []
    rows = [r for r in rows if r.get("close") is not None]
    if not rows:
        return

    df = pd.DataFrame(rows).sort_values("date")
    cdef = COMMODITY_UNIVERSE.get(symbol)
    name = cdef.name if cdef else symbol

    first, last = df["close"].iloc[0], df["close"].iloc[-1]
    trend_color = _COLORS["up"] if last >= first else _COLORS["down"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["close"],
        mode="lines", fill="tozeroy",
        line=dict(width=1.5, color=trend_color),
        fillcolor=trend_color.replace(")", ",0.08)").replace("rgb", "rgba").replace("#", "rgba(")
        if trend_color.startswith("rgb") else f"rgba({int(trend_color[1:3],16)},{int(trend_color[3:5],16)},{int(trend_color[5:7],16)},0.08)",
    ))
    fig.update_layout(
        title=dict(text=f"{name}  {_fmt_price(last)}", font=dict(size=11)),
        height=110, margin=dict(l=0, r=0, t=25, b=0),
        xaxis=dict(visible=False), yaxis=dict(visible=False),
        showlegend=False, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    st.plotly_chart(fig, width='stretch')


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2: CHARTS — Candlestick/line with volume, SMA, dark theme
# ═════════════════════════════════════════════════════════════════════════════

def _render_charts():
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    st.markdown("### Commodity Price Charts")

    all_symbols = api_client.get_commodity_symbols() or []
    fx_symbols = api_client.get_commodity_fx_pairs() or []
    all_available = sorted(set(all_symbols + fx_symbols))
    if not all_available:
        st.info("No commodity data available. Run a sync first.")
        return

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    with col1:
        selected = st.selectbox(
            "Commodity", all_available,
            format_func=lambda s: f"{COMMODITY_UNIVERSE[s].name} ({s})" if s in COMMODITY_UNIVERSE else s,
        )
    with col2:
        chart_type = st.radio("Chart Type", ["Candlestick", "Line"], horizontal=True)
    with col3:
        period = st.selectbox("Period", ["30d", "90d", "180d", "1y", "All"], index=3)
    with col4:
        show_sma = st.multiselect("SMA", [10, 20, 50], default=[20])

    if not selected:
        return

    limit_map = {"30d": 30, "90d": 90, "180d": 180, "1y": 365, "All": 10000}
    limit = limit_map.get(period, 365)

    rows = api_client.get_commodity_eod(selected, limit=limit) or []
    if not rows:
        st.info(f"No data for {selected}")
        return

    df = pd.DataFrame(rows).sort_values("date")
    cdef = COMMODITY_UNIVERSE.get(selected)
    title = f"{cdef.name} ({cdef.unit})" if cdef else selected

    has_volume = "volume" in df.columns and df["volume"].notna().any() and df["volume"].sum() > 0
    row_heights = [0.75, 0.25] if has_volume else [1.0]
    fig = make_subplots(
        rows=2 if has_volume else 1, cols=1,
        shared_xaxes=True, vertical_spacing=0.03,
        row_heights=row_heights,
    )

    if chart_type == "Candlestick" and all(c in df.columns for c in ["open", "high", "low", "close"]):
        fig.add_trace(go.Candlestick(
            x=df["date"], open=df["open"], high=df["high"],
            low=df["low"], close=df["close"], name="OHLC",
            increasing_line_color=_COLORS["up"],
            decreasing_line_color=_COLORS["down"],
        ), row=1, col=1)
    else:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["close"], mode="lines",
            name="Close", line=dict(width=2, color=_COLORS["accent"]),
        ), row=1, col=1)

    # SMA overlays
    sma_colors = {10: "#FF9800", 20: "#2196F3", 50: "#9C27B0"}
    for window in show_sma:
        if len(df) >= window:
            sma = df["close"].rolling(window).mean()
            fig.add_trace(go.Scatter(
                x=df["date"], y=sma, mode="lines",
                name=f"SMA {window}",
                line=dict(width=1.2, color=sma_colors.get(window, "#888"), dash="dot"),
            ), row=1, col=1)

    # Volume bars
    if has_volume:
        vol_colors = [_COLORS["up"] if c >= o else _COLORS["down"]
                      for c, o in zip(df["close"].fillna(0), df["open"].fillna(0))]
        fig.add_trace(go.Bar(
            x=df["date"], y=df["volume"], name="Volume",
            marker_color=vol_colors, opacity=0.6, showlegend=False,
        ), row=2, col=1)

    fig.update_layout(
        **{**_CHART_LAYOUT, "legend": dict(orientation="h", y=1.05, x=0, bgcolor="rgba(0,0,0,0)")},
        height=520, xaxis_rangeslider_visible=False,
        title=dict(text=title, font=dict(size=14)),
    )
    fig.update_yaxes(title_text="Price", row=1, col=1)
    if has_volume:
        fig.update_yaxes(title_text="Vol", row=2, col=1)
    st.plotly_chart(fig, width='stretch')

    # Range analysis cards
    if len(df) > 1:
        rc1, rc2, rc3, rc4 = st.columns(4)
        period_high = df["high"].max() if "high" in df.columns else df["close"].max()
        period_low = df["low"].min() if "low" in df.columns else df["close"].min()
        range_pct = ((period_high - period_low) / period_low * 100) if period_low and period_low > 0 else 0
        avg_vol = df["volume"].mean() if has_volume else 0
        with rc1:
            _metric_card("Period High", _fmt_price(period_high))
        with rc2:
            _metric_card("Period Low", _fmt_price(period_low))
        with rc3:
            _metric_card("Range", f"{range_pct:.1f}%")
        with rc4:
            _metric_card("Avg Volume", f"{avg_vol:,.0f}" if avg_vol else "—")

    with st.expander("Raw Data"):
        st.dataframe(df.sort_values("date", ascending=False), width='stretch', hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3: CATEGORIES — Browse + heatmap + correlation
# ═════════════════════════════════════════════════════════════════════════════

def _render_categories():
    from pakfindata.commodities.config import COMMODITY_UNIVERSE, CATEGORIES

    st.markdown("### Browse by Category")

    selected_cat = st.selectbox("Category", ["All"] + CATEGORIES)

    rows = api_client.get_commodity_categories_latest() or []
    if not rows:
        st.info("No commodity data. Run a sync first.")
        return

    df = pd.DataFrame(rows)
    if selected_cat != "All":
        df = df[df["category"] == selected_cat]
    if df.empty:
        st.info(f"No data for category: {selected_cat}")
        return

    df["change_pct"] = ((df["close"] - df["open"]) / df["open"] * 100).round(2).fillna(0)

    # ── Change heatmap bar ──
    st.markdown("#### Daily Change")
    sorted_df = df.sort_values("change_pct")
    fig = _styled_fig(height=max(280, len(sorted_df) * 22))
    fig.add_trace(go.Bar(
        y=sorted_df["name"].fillna(sorted_df["symbol"]),
        x=sorted_df["change_pct"],
        orientation="h",
        marker_color=[_change_color(v) for v in sorted_df["change_pct"]],
        text=[f"{v:+.2f}%" for v in sorted_df["change_pct"]],
        textposition="outside", textfont=dict(size=10),
    ))
    fig.update_layout(xaxis_title="Daily Change %", showlegend=False)
    st.plotly_chart(fig, width='stretch')

    # ── Data table ──
    display_df = df[["name", "symbol", "category", "close", "change_pct", "unit", "pk_relevance", "date"]].copy()
    display_df.columns = ["Commodity", "Symbol", "Category", "Price", "Change %", "Unit", "PK Relevance", "Date"]
    st.dataframe(display_df, width='stretch', hide_index=True)

    # ── Correlation matrix (30-day) ──
    if selected_cat == "All" and len(df) >= 5:
        st.markdown("#### 30-Day Price Correlation")
        top_symbols = df.nlargest(10, "volume" if "volume" in df.columns else "close")["symbol"].tolist()
        if len(top_symbols) >= 3:
            price_data = {}
            for sym in top_symbols:
                hist = api_client.get_commodity_eod(sym, limit=30) or []
                hist = [h for h in hist if h.get("close") is not None]
                if hist and len(hist) >= 10:
                    s = pd.Series(
                        [r["close"] for r in hist],
                        index=[r["date"] for r in hist],
                    )
                    cdef = COMMODITY_UNIVERSE.get(sym)
                    label = cdef.name if cdef else sym
                    price_data[label] = s.pct_change().dropna()

            if len(price_data) >= 3:
                corr_df = pd.DataFrame(price_data).corr()
                fig = go.Figure(go.Heatmap(
                    z=corr_df.values, x=corr_df.columns, y=corr_df.index,
                    colorscale=[[0, _COLORS["down"]], [0.5, _COLORS["grid"]], [1, _COLORS["up"]]],
                    zmin=-1, zmax=1,
                    text=corr_df.round(2).values,
                    texttemplate="%{text}",
                    textfont=dict(size=10, color="white"),
                ))
                fig.update_layout(
                    **_CHART_LAYOUT, height=400,
                    title=dict(text="Return Correlation (30d)", font=dict(size=13)),
                )
                st.plotly_chart(fig, width='stretch')


# ═════════════════════════════════════════════════════════════════════════════
# TAB 4: PAKISTAN VIEW — PKR prices + gold premium
# ═════════════════════════════════════════════════════════════════════════════

def _render_pakistan_view():
    from pakfindata.commodities.config import COMMODITY_UNIVERSE

    st.markdown("### Pakistan Commodity Prices")
    st.caption("Prices converted to PKR using the latest USD/PKR exchange rate")

    pkr_rows = api_client.get_commodity_pkr_latest() or []

    if not pkr_rows:
        st.info(
            "No PKR prices computed yet. These are generated after syncing commodity "
            "and FX data.\n\n**CLI:** `pfsync commodity sync --all`"
        )
        return

    df = pd.DataFrame(pkr_rows)
    df["name"] = df["symbol"].map(
        lambda s: COMMODITY_UNIVERSE[s].name if s in COMMODITY_UNIVERSE else s
    )

    # ── Precious metals KPI row ──
    gold_row = df[df["symbol"] == "GOLD"]
    silver_row = df[df["symbol"] == "SILVER"]

    if not gold_row.empty or not silver_row.empty:
        st.markdown("### Precious Metals")
        pm_cols = st.columns(4)
        if not gold_row.empty:
            g = gold_row.iloc[0]
            with pm_cols[0]:
                _metric_card("Gold (per Tola)", f"PKR {g['pkr_price']:,.0f}", prefix="")
            with pm_cols[1]:
                _metric_card("Gold (USD/oz)", f"${g['usd_price']:,.2f}")
        if not silver_row.empty:
            s = silver_row.iloc[0]
            with pm_cols[2]:
                _metric_card("Silver (per Tola)", f"PKR {s['pkr_price']:,.0f}")
            with pm_cols[3]:
                _metric_card("Silver (USD/oz)", f"${s['usd_price']:,.2f}")

    # ── Gold premium analysis ──
    if not gold_row.empty:
        st.markdown("### Gold Premium Analysis")
        st.caption("Tola gold premium = Local PKR/Tola price vs (Intl USD/oz x 0.375117 x FX rate)")
        g = gold_row.iloc[0]
        intl_tola = g["usd_price"] * 0.375117 * g["usd_pkr"]
        premium = g["pkr_price"] - intl_tola
        premium_pct = (premium / intl_tola * 100) if intl_tola else 0

        pc1, pc2, pc3 = st.columns(3)
        with pc1:
            _metric_card("Local PKR/Tola", f"{g['pkr_price']:,.0f}")
        with pc2:
            _metric_card("Intl Equivalent", f"{intl_tola:,.0f}")
        with pc3:
            _metric_card("Premium", f"{premium:+,.0f}", premium_pct, suffix=f" ({premium_pct:+.1f}%)")

    # ── PKR price history chart ──
    st.markdown("### PKR Price Trends")
    pkr_symbols = df["symbol"].tolist()
    sel_pkr = st.multiselect("Select Commodities", pkr_symbols, default=pkr_symbols[:3])

    if sel_pkr:
        fig = _styled_fig(height=400)
        for sym in sel_pkr:
            hist = api_client.get_commodity_pkr_history(sym, limit=90) or []
            if hist:
                hdf = pd.DataFrame(hist).sort_values("date")
                cdef = COMMODITY_UNIVERSE.get(sym)
                label = f"{cdef.name} ({cdef.pk_unit})" if cdef and cdef.pk_unit else sym
                fig.add_trace(go.Scatter(
                    x=hdf["date"], y=hdf["pkr_price"],
                    mode="lines", name=label,
                    line=dict(width=2),
                ))
        fig.update_layout(
            title=dict(text="PKR Prices (90d)", font=dict(size=13)),
            yaxis_title="PKR",
        )
        st.plotly_chart(fig, width='stretch')

    # ── Full table ──
    display = df[["name", "symbol", "pkr_price", "pk_unit", "usd_price", "usd_pkr", "date"]].copy()
    display.columns = ["Commodity", "Symbol", "PKR Price", "Unit", "USD Price", "USD/PKR", "Date"]
    display["PKR Price"] = display["PKR Price"].apply(lambda x: f"{x:,.0f}" if x else "N/A")
    st.dataframe(display, width='stretch', hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 5: LOCAL MARKETS — khistocks.com
# ═════════════════════════════════════════════════════════════════════════════

_FEED_LABELS = {
    "khistocks_pmex": "PMEX Commodity Exchange",
    "khistocks_sarafa": "Karachi Sarafa Bazaar",
    "khistocks_intl_bullion": "International Bullion",
    "khistocks_mandi": "Lahore Akbari Mandi",
    "khistocks_lme": "London Metal Exchange (LME)",
}


def _render_local_markets(has_data: dict):
    st.markdown("### Pakistan Local Markets")
    st.caption("Data from khistocks.com — PMEX, Karachi Sarafa, Akbari Mandi, LME")

    if has_data.get("khistocks_prices", 0) == 0:
        st.info(
            "No local market data found. Sync from khistocks.com first.\n\n"
            "**CLI:** `pfsync commodity sync --source khistocks`"
        )
        return

    feed_list = api_client.get_khistocks_feeds() or []

    selected_feed = st.selectbox(
        "Market Feed", ["All Feeds"] + feed_list,
        format_func=lambda f: _FEED_LABELS.get(f, f) if f != "All Feeds" else "All Feeds",
        key="khi_feed_select",
    )

    if selected_feed == "All Feeds":
        rows = api_client.get_khistocks_latest() or []
    else:
        rows = api_client.get_khistocks_latest(feed=selected_feed) or []

    if not rows:
        st.info("No data for selected feed.")
        return

    df = pd.DataFrame(rows)

    # ── Visual price cards by feed ──
    for feed_name, group in df.groupby("feed"):
        label = _FEED_LABELS.get(feed_name, feed_name)
        st.markdown(f"#### {label}")

        # Change bar chart for this feed
        if "close" in group.columns and "open" in group.columns:
            g = group.dropna(subset=["close", "open"]).copy()
            if not g.empty:
                g["chg_pct"] = ((g["close"] - g["open"]) / g["open"] * 100).round(2).fillna(0)
                g_sorted = g.sort_values("chg_pct")
                display_name = g_sorted["name"].fillna(g_sorted["symbol"])

                fig = _styled_fig(height=max(200, len(g_sorted) * 22))
                fig.add_trace(go.Bar(
                    y=display_name, x=g_sorted["chg_pct"], orientation="h",
                    marker_color=[_change_color(v) for v in g_sorted["chg_pct"]],
                    text=[f"{v:+.2f}%" for v in g_sorted["chg_pct"]],
                    textposition="outside", textfont=dict(size=10),
                    hovertemplate="<b>%{y}</b><br>Change: %{x:.2f}%<extra></extra>",
                ))
                fig.update_layout(xaxis_title="Change %", showlegend=False)
                st.plotly_chart(fig, width='stretch')

        # Table
        if "lme" in feed_name:
            cols = ["symbol", "name", "date", "cash_buyer", "cash_seller",
                    "three_month_buyer", "three_month_seller", "net_change", "change_pct"]
        elif "sarafa" in feed_name or "bullion" in feed_name:
            cols = ["symbol", "name", "date", "open", "high", "low", "close",
                    "net_change", "change_pct"]
        elif "mandi" in feed_name:
            cols = ["symbol", "name", "date", "rate", "quotation", "net_change", "change_pct"]
        else:
            cols = ["symbol", "name", "date", "open", "high", "low", "close",
                    "quotation", "net_change", "change_pct"]

        available = [c for c in cols if c in group.columns]
        display = group[available].copy().dropna(axis=1, how="all")
        st.dataframe(display, width='stretch', hide_index=True)

    # ── History drill-down ──
    st.markdown("---")
    st.markdown("#### Price History")
    all_symbols = sorted(df["symbol"].unique())
    selected_sym = st.selectbox("Select Symbol", all_symbols, key="khi_sym_history")

    if selected_sym:
        history = api_client.get_khistocks_history(selected_sym, limit=90) or []
        if history:
            hist_df = pd.DataFrame(history).sort_values("date")
            price_col = "close" if hist_df["close"].notna().any() else "rate"
            if hist_df[price_col].notna().any():
                fig = _styled_fig(height=350)
                fig.add_trace(go.Scatter(
                    x=hist_df["date"], y=hist_df[price_col],
                    mode="lines+markers",
                    line=dict(width=2, color=_COLORS["accent"]),
                    name=selected_sym,
                ))
                fig.update_layout(
                    title=dict(text=f"{selected_sym} — Price History", font=dict(size=13)),
                    yaxis_title="Price",
                )
                st.plotly_chart(fig, width='stretch')

            with st.expander("Raw Data"):
                st.dataframe(hist_df.sort_values("date", ascending=False),
                             width='stretch', hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 6: PMEX PORTAL — direct PMEX market watch data
# ═════════════════════════════════════════════════════════════════════════════

_PMEX_CATEGORY_LABELS = {
    "Indices": "PMEX Indices",
    "Metals": "Precious & Base Metals",
    "Oil": "Crude Oil & Petroleum",
    "Cots": "Cotton Contracts",
    "Energy": "Energy",
    "Agri": "Agriculture Futures",
    "Phy_Agri": "Physical Agriculture",
    "Phy_Gold": "Physical Gold",
    "Financials": "Financial Futures",
}

_PMEX_CAT_COLORS = {
    "Metals": _COLORS["gold"],
    "Oil": _COLORS["energy"],
    "Energy": "#FF9800",
    "Cots": "#8D6E63",
    "Agri": _COLORS["agri"],
    "Phy_Agri": "#43A047",
    "Phy_Gold": "#FFC107",
    "Indices": _COLORS["fx"],
    "Financials": _COLORS["metals"],
}


def _render_pmex_portal(has_data: dict):
    st.markdown("### PMEX Market Watch")
    st.caption("Direct from PMEX Portal — 134 instruments across 9 categories")

    if has_data.get("pmex_market_watch", 0) == 0:
        st.info(
            "No PMEX data found. Sync from the PMEX portal first.\n\n"
            "**CLI:** `pfsync commodity sync --source pmex_portal`"
        )
        return

    cat_list = api_client.get_pmex_portal_categories() or []

    selected_cat = st.selectbox(
        "Category", ["All Categories"] + cat_list,
        format_func=lambda c: _PMEX_CATEGORY_LABELS.get(c, c) if c != "All Categories" else "All Categories",
        key="pmex_cat_select",
    )

    if selected_cat == "All Categories":
        rows = api_client.get_pmex_portal_latest() or []
    else:
        rows = api_client.get_pmex_portal_latest(category=selected_cat) or []

    if not rows:
        st.info("No data for selected category.")
        return

    df = pd.DataFrame(rows)

    # ── KPI row ──
    kc1, kc2, kc3, kc4 = st.columns(4)
    with kc1:
        _metric_card("Contracts", str(len(df)))
    with kc2:
        _metric_card("Categories", str(df["category"].nunique()))
    with kc3:
        active = len(df[df["total_vol"].fillna(0) > 0]) if "total_vol" in df.columns else 0
        _metric_card("With Volume", str(active))
    with kc4:
        total_vol = df["total_vol"].fillna(0).sum() if "total_vol" in df.columns else 0
        _metric_card("Total Volume", f"{total_vol:,.0f}")

    # ── Volume by category donut ──
    if "total_vol" in df.columns and selected_cat == "All Categories":
        vol_by_cat = df.groupby("category")["total_vol"].sum().sort_values(ascending=False)
        vol_by_cat = vol_by_cat[vol_by_cat > 0]
        if not vol_by_cat.empty:
            st.markdown("#### Volume by Category")
            fig = go.Figure(go.Pie(
                labels=[_PMEX_CATEGORY_LABELS.get(c, c) for c in vol_by_cat.index],
                values=vol_by_cat.values,
                hole=0.45,
                marker=dict(colors=[_PMEX_CAT_COLORS.get(c, _COLORS["neutral"]) for c in vol_by_cat.index]),
                textinfo="label+percent",
                textfont=dict(color="white", size=10),
            ))
            fig.update_layout(
                **_CHART_LAYOUT, height=350, showlegend=False,
            )
            st.plotly_chart(fig, width='stretch')

    # ── Change distribution scatter ──
    if "change_pct" in df.columns and df["change_pct"].notna().any():
        st.markdown("#### Price Change Distribution")
        scatter_df = df.dropna(subset=["change_pct"]).copy()
        if not scatter_df.empty and "total_vol" in scatter_df.columns:
            fig = _styled_fig(height=350)
            for cat in scatter_df["category"].unique():
                cat_data = scatter_df[scatter_df["category"] == cat]
                fig.add_trace(go.Scatter(
                    x=cat_data["change_pct"], y=cat_data["total_vol"].fillna(0),
                    mode="markers+text",
                    marker=dict(
                        size=10,
                        color=_PMEX_CAT_COLORS.get(cat, _COLORS["neutral"]),
                        opacity=0.8,
                    ),
                    text=cat_data["contract"],
                    textposition="top center",
                    textfont=dict(size=8),
                    name=_PMEX_CATEGORY_LABELS.get(cat, cat),
                    hovertemplate="<b>%{text}</b><br>Change: %{x:.2f}%<br>Volume: %{y:,}<extra></extra>",
                ))
            fig.update_layout(
                xaxis_title="Change %", yaxis_title="Volume",
                legend=dict(orientation="h", y=-0.15, bgcolor="rgba(0,0,0,0)"),
            )
            # Add zero line
            fig.add_vline(x=0, line_dash="dash", line_color=_COLORS["text_dim"], opacity=0.5)
            st.plotly_chart(fig, width='stretch')

    # ── Tables by category ──
    for cat_name, group in df.groupby("category"):
        label = _PMEX_CATEGORY_LABELS.get(cat_name, cat_name)
        st.markdown(f"#### {label} ({len(group)} contracts)")

        cols = ["contract", "snapshot_date", "bid", "ask", "last_price",
                "open", "close", "high", "low", "change", "change_pct",
                "total_vol", "state"]
        available = [c for c in cols if c in group.columns]
        display = group[available].copy().dropna(axis=1, how="all")
        display = display.rename(columns={
            "contract": "Contract", "snapshot_date": "Date",
            "bid": "Bid", "ask": "Ask", "last_price": "Last",
            "open": "Open", "close": "Close", "high": "High", "low": "Low",
            "change": "Change", "change_pct": "Chg%",
            "total_vol": "Volume", "state": "State",
        })
        st.dataframe(display, width='stretch', hide_index=True)

    # ── Contract history ──
    st.markdown("---")
    st.markdown("#### Contract History")
    all_contracts = sorted(df["contract"].unique())
    selected_contract = st.selectbox("Select Contract", all_contracts, key="pmex_contract_history")

    if selected_contract:
        history = api_client.get_pmex_portal_history(selected_contract, limit=90) or []
        if history:
            hist_df = pd.DataFrame(history).sort_values("snapshot_date")
            price_col = "last_price" if hist_df["last_price"].notna().any() else "close"
            if hist_df[price_col].notna().any():
                fig = _styled_fig(height=350)
                fig.add_trace(go.Scatter(
                    x=hist_df["snapshot_date"], y=hist_df[price_col],
                    mode="lines+markers",
                    line=dict(width=2, color=_COLORS["energy"]),
                    name=selected_contract,
                ))
                if hist_df["bid"].notna().any() and hist_df["ask"].notna().any():
                    fig.add_trace(go.Scatter(
                        x=hist_df["snapshot_date"], y=hist_df["bid"],
                        mode="lines", line=dict(width=1, dash="dash", color=_COLORS["up"]),
                        name="Bid",
                    ))
                    fig.add_trace(go.Scatter(
                        x=hist_df["snapshot_date"], y=hist_df["ask"],
                        mode="lines", line=dict(width=1, dash="dash", color=_COLORS["down"]),
                        name="Ask",
                    ))
                fig.update_layout(
                    title=dict(text=f"{selected_contract} — Price History", font=dict(size=13)),
                    yaxis_title="Price",
                )
                st.plotly_chart(fig, width='stretch')

            with st.expander("Raw Data"):
                st.dataframe(hist_df.sort_values("snapshot_date", ascending=False),
                             width='stretch', hide_index=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 7: EXPORT
# ═════════════════════════════════════════════════════════════════════════════

_EXPORT_OPTIONS: dict[str, str] = {
    "Daily OHLCV (yfinance)": "eod",
    "Monthly Benchmarks (FRED/WorldBank)": "monthly",
    "PKR Prices": "pkr",
    "FX Rates": "fx",
    "Local Markets (khistocks)": "khistocks",
    "PMEX Market Watch": "pmex_market_watch",
}


def _render_export():
    st.markdown("### Export Commodity Data")
    st.caption(
        "Bulk export is capped at 50,000 rows per request to avoid huge payloads. "
        "For full-history exports (e.g. 1.8M FX rows), use the CLI:"
        " `pfsync export commodity-fx`."
    )

    export_label = st.selectbox("Data Set", list(_EXPORT_OPTIONS.keys()))
    dataset = _EXPORT_OPTIONS[export_label]

    limit = st.slider(
        "Row limit", min_value=100, max_value=50000, value=5000, step=500,
        help="Higher limits = bigger CSV downloads",
    )

    rows = api_client.get_commodity_export(dataset, limit=limit) or []
    if not rows:
        st.info("No data available for this export.")
        return

    df = pd.DataFrame(rows)

    st.text(f"{len(df)} rows")
    st.dataframe(df.head(100), width='stretch', hide_index=True)

    csv = df.to_csv(index=False)
    st.download_button(
        "Download CSV", csv,
        file_name=f"pakfindata_commodity_{export_label.split('(')[0].strip().lower().replace(' ', '_')}.csv",
        mime="text/csv",
    )


# ═════════════════════════════════════════════════════════════════════════════
# SYNC CONTROLS
# ═════════════════════════════════════════════════════════════════════════════

def _render_sync_controls():
    """Sync buttons — call engine sync functions directly.

    Per Phase 1.6 pattern, these are engine-domain calls (not /v1 write
    endpoints, not worker jobs). Each sync function opens its own write
    connection and runs ``init_commodity_schema`` internally.
    """
    with st.expander("Sync Commodity Data"):
        st.caption("Fetch latest commodity prices from free data sources.")

        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            if st.button("Sync yfinance (Daily)", type="primary", key="cmd_sync_yf"):
                with st.spinner("Syncing from yfinance..."):
                    try:
                        from pakfindata.commodities.sync import sync_yfinance
                        summary = sync_yfinance(incremental=True)
                        st.success(
                            f"yfinance: {summary.symbols_ok}/{summary.symbols_total} symbols, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"yfinance sync failed: {e}")

        with col2:
            if st.button("Sync FRED (Monthly)", key="cmd_sync_fred"):
                with st.spinner("Syncing from FRED..."):
                    try:
                        from pakfindata.commodities.sync import sync_fred
                        summary = sync_fred()
                        st.success(
                            f"FRED: {summary.symbols_ok}/{summary.symbols_total} series, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                    except ImportError:
                        st.warning("fredapi not installed. Run: `pip install fredapi`")
                    except ValueError as e:
                        st.warning(str(e))
                    except Exception as e:
                        st.error(f"FRED sync failed: {e}")

        with col3:
            if st.button("Sync World Bank", key="cmd_sync_wb"):
                with st.spinner("Downloading World Bank Pink Sheet..."):
                    try:
                        from pakfindata.commodities.sync import sync_worldbank
                        summary = sync_worldbank()
                        st.success(
                            f"World Bank: {summary.symbols_ok} commodities, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"World Bank sync failed: {e}")

        with col4:
            if st.button("Sync khistocks (PK)", key="cmd_sync_khi"):
                with st.spinner("Syncing from khistocks.com..."):
                    try:
                        from pakfindata.commodities.sync import sync_khistocks
                        summary = sync_khistocks()
                        st.success(
                            f"khistocks: {summary.symbols_ok}/{summary.symbols_total} symbols, "
                            f"{summary.rows_upserted} rows"
                        )
                        if summary.errors:
                            with st.expander("Errors"):
                                for sym, err in summary.errors:
                                    st.text(f"{sym}: {err}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"khistocks sync failed: {e}")

        with col5:
            if st.button("Sync PMEX Portal", key="cmd_sync_pmex"):
                with st.spinner("Fetching PMEX market data..."):
                    try:
                        from pakfindata.commodities.sync import sync_pmex
                        summary = sync_pmex()
                        st.success(
                            f"PMEX: {summary.symbols_ok}/{summary.symbols_total} contracts, "
                            f"{summary.rows_upserted} rows"
                        )
                        if summary.errors:
                            with st.expander("Errors"):
                                for sym, err in summary.errors:
                                    st.text(f"{sym}: {err}")
                        st.rerun()
                    except Exception as e:
                        st.error(f"PMEX sync failed: {e}")

        if st.button("Compute PKR Prices", key="cmd_pkr"):
            with st.spinner("Computing PKR prices..."):
                try:
                    from pakfindata.commodities.sync import compute_pkr_prices
                    n = compute_pkr_prices()
                    st.success(f"Computed {n} PKR price rows")
                    st.rerun()
                except Exception as e:
                    st.error(f"PKR computation failed: {e}")

        sync_rows = api_client.get_commodity_sync_runs(limit=10) or []
        if sync_rows:
            st.markdown("#### Recent Sync Runs")
            sync_df = pd.DataFrame(sync_rows)
            st.dataframe(sync_df, width='stretch', hide_index=True)
