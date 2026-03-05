"""FX Rates Terminal — interbank, open market, kerb, spreads, volatility, carry.

Tabs:
  Overview — KPI cards, rate cards across sources, kerb premium
  Charts — Multi-source history, comparison overlays, candlestick for fx_ohlcv
  Spreads — Interbank vs kerb spread heatmap, premium analysis
  Volatility — Rolling vol, drawdown, regime detection
  Carry — Carry trade calculator, KIBOR vs global rates, NPC certificates
  FX Signals — Microservice-sourced regime, intervention, carry signals
  Sync — All sync/backfill controls
"""

import sqlite3
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from pakfindata.ui.components.helpers import get_connection, render_ai_commentary, render_footer
from pakfindata.sources.sbp_fx import SBPFXScraper
from pakfindata.sources.forex_scraper import ForexPKScraper
from pakfindata.sources.fx_client import FXClient

_fx = FXClient()

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "interbank": "#FF6B35", "open_mkt": "#4ECDC4",
    "kerb": "#45B7D1", "policy": "#E74C3C", "kibor": "#9B59B6",
    "bg": "#0e1117", "card_bg": "#1a1a2e", "grid": "#2d2d3d",
    "text": "#e0e0e0", "text_dim": "#888888",
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11),
    margin=dict(l=10, r=10, t=40, b=10),
)

_KEY_CURRENCIES = ["USD", "EUR", "GBP", "SAR", "AED"]

_FX_TABLES = {
    "Interbank": "sbp_fx_interbank",
    "Open Market": "sbp_fx_open_market",
    "Kerb": "forex_kerb",
}

_SRC_COLORS = {
    "Interbank": _COLORS["interbank"],
    "Open Market": _COLORS["open_mkt"],
    "Kerb": _COLORS["kerb"],
}


_AXIS_STYLE = dict(gridcolor=_COLORS["grid"], zeroline=False)


def _styled_fig(height=400, **kw):
    fig = go.Figure(layout={**_CHART_LAYOUT, "height": height, **kw})
    fig.update_xaxes(**_AXIS_STYLE)
    fig.update_yaxes(**_AXIS_STYLE)
    return fig


def _card(label, value, delta=None, color=None):
    card_bg = _COLORS["card_bg"]
    border = color or _COLORS["accent"]
    dim = _COLORS["text_dim"]
    delta_html = ""
    if delta is not None and not pd.isna(delta):
        dc = _COLORS["up"] if delta > 0 else _COLORS["down"] if delta < 0 else _COLORS["neutral"]
        sign = "+" if delta > 0 else ""
        delta_html = f"<span style='color:{dc};font-size:0.85em;'>{sign}{delta:.2f}</span>"
    st.markdown(
        f"<div style='background:{card_bg};border-radius:8px;padding:12px 16px;"
        f"border-left:3px solid {border};'>"
        f"<div style='color:{dim};font-size:0.75em;'>{label}</div>"
        f"<div style='font-size:1.3em;font-weight:600;'>{value}</div>"
        f"{delta_html}</div>",
        unsafe_allow_html=True,
    )


def _get_latest_rate(con, table, currency):
    try:
        row = con.execute(
            f"SELECT date, buying, selling FROM {table}"
            " WHERE UPPER(currency) = ? ORDER BY date DESC LIMIT 1",
            (currency.upper(),),
        ).fetchone()
        return dict(row) if row else None
    except Exception:
        return None


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def render_fx_dashboard():
    st.markdown("## FX Rates Terminal")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    tabs = st.tabs(["Overview", "Charts", "Spreads", "Volatility", "Carry", "FX Signals", "Sync"])

    renderers = [
        _render_overview, _render_charts, _render_spreads,
        _render_volatility, _render_carry, _render_fx_signals, _render_sync,
    ]

    for tab, renderer in zip(tabs, renderers):
        with tab:
            try:
                renderer(con)
            except Exception as e:
                st.error(f"Error: {e}")

    render_footer()


# ═════════════════════════════════════════════════════════════════════════════
# TAB 1: OVERVIEW
# ═════════════════════════════════════════════════════════════════════════════

def _render_overview(con):
    # ── USD/PKR headline ──
    usd_ib = _get_latest_rate(con, "sbp_fx_interbank", "USD")
    usd_om = _get_latest_rate(con, "sbp_fx_open_market", "USD")
    usd_kerb = _get_latest_rate(con, "forex_kerb", "USD")

    # Previous day for delta
    prev_usd = con.execute(
        "SELECT selling FROM sbp_fx_interbank WHERE UPPER(currency)='USD'"
        " ORDER BY date DESC LIMIT 1 OFFSET 1"
    ).fetchone()

    mc = st.columns(5)
    with mc[0]:
        val = f"{usd_ib['selling']:.2f}" if usd_ib else "N/A"
        delta = usd_ib["selling"] - prev_usd["selling"] if usd_ib and prev_usd else None
        _card("USD/PKR Interbank", val, delta, _COLORS["interbank"])
    with mc[1]:
        val = f"{usd_om['selling']:.2f}" if usd_om else "N/A"
        _card("USD/PKR Open Mkt", val, color=_COLORS["open_mkt"])
    with mc[2]:
        val = f"{usd_kerb['selling']:.2f}" if usd_kerb else "N/A"
        _card("USD/PKR Kerb", val, color=_COLORS["kerb"])
    with mc[3]:
        if usd_ib and usd_kerb and usd_ib["selling"] and usd_kerb["selling"]:
            spread = usd_kerb["selling"] - usd_ib["selling"]
            _card("Kerb Premium", f"{spread:+.2f}", color="#FFD700")
        else:
            _card("Kerb Premium", "N/A")
    with mc[4]:
        # DXY from commodity_fx_rates
        dxy = con.execute(
            "SELECT close FROM commodity_fx_rates WHERE pair='DXY' ORDER BY date DESC LIMIT 1"
        ).fetchone()
        _card("DXY Index", f"{dxy['close']:.2f}" if dxy else "N/A", color="#AB47BC")

    st.markdown("")

    # ── All currency rate cards ──
    st.markdown("### Currency Rates Comparison")
    for currency in _KEY_CURRENCIES:
        cols = st.columns([1, 2, 2, 2, 1])
        cols[0].markdown(f"**{currency}/PKR**")
        for i, (src_name, table) in enumerate(_FX_TABLES.items()):
            rate = _get_latest_rate(con, table, currency)
            with cols[i + 1]:
                if rate:
                    st.metric(
                        src_name,
                        f"{rate['buying']:.2f} / {rate['selling']:.2f}",
                        help=f"Buy / Sell as of {rate['date']}",
                    )
                else:
                    st.metric(src_name, "N/A")
        ib = _get_latest_rate(con, "sbp_fx_interbank", currency)
        kerb = _get_latest_rate(con, "forex_kerb", currency)
        with cols[4]:
            if ib and kerb and ib["selling"] and kerb["selling"]:
                spread = kerb["selling"] - ib["selling"]
                st.metric("Spread", f"{spread:+.2f}")
            else:
                st.metric("Spread", "N/A")

    # ── All currencies table ──
    st.markdown("### All Currency Rates")
    for src_name, table in _FX_TABLES.items():
        try:
            df = pd.read_sql_query(
                f"""SELECT t.currency, t.date, t.buying, t.selling,
                           ROUND(t.selling - t.buying, 4) as spread
                    FROM {table} t
                    INNER JOIN (SELECT currency, MAX(date) as max_date FROM {table} GROUP BY currency)
                         mx ON t.currency=mx.currency AND t.date=mx.max_date
                    ORDER BY t.currency""",
                con,
            )
            if not df.empty:
                st.markdown(f"**{src_name}** ({df['date'].iloc[0]})")
                st.dataframe(df.rename(columns={
                    "currency": "Currency", "date": "Date",
                    "buying": "Buying", "selling": "Selling", "spread": "Spread",
                }), use_container_width=True, hide_index=True)
        except Exception:
            pass


# ═════════════════════════════════════════════════════════════════════════════
# TAB 2: CHARTS
# ═════════════════════════════════════════════════════════════════════════════

def _render_charts(con):
    st.markdown("### Rate History")

    c1, c2, c3 = st.columns([2, 1, 1])
    with c1:
        currency = st.selectbox("Currency", _KEY_CURRENCIES, key="fx_chart_ccy")
    with c2:
        period = st.selectbox("Period", ["90d", "180d", "1y", "All"], index=2, key="fx_chart_period")
    with c3:
        chart_mode = st.radio("Mode", ["Overlay", "Candlestick"], horizontal=True, key="fx_chart_mode")

    limit = {"90d": 90, "180d": 180, "1y": 365, "All": 9999}[period]

    if chart_mode == "Overlay":
        fig = _styled_fig(height=450)
        for src_name, table in _FX_TABLES.items():
            df = pd.read_sql_query(
                f"SELECT date, buying, selling FROM {table}"
                " WHERE UPPER(currency)=? ORDER BY date DESC LIMIT ?",
                con, params=(currency.upper(), limit),
            )
            if not df.empty:
                df = df.sort_values("date")
                fig.add_trace(go.Scatter(
                    x=df["date"], y=df["selling"], mode="lines",
                    name=f"{src_name} (Sell)",
                    line=dict(width=2, color=_SRC_COLORS.get(src_name, "#999")),
                ))
                fig.add_trace(go.Scatter(
                    x=df["date"], y=df["buying"], mode="lines",
                    name=f"{src_name} (Buy)",
                    line=dict(width=1, dash="dot", color=_SRC_COLORS.get(src_name, "#999")),
                    showlegend=False,
                ))
        fig.update_layout(
            title=dict(text=f"{currency}/PKR — Multi-Source", font=dict(size=13)),
            yaxis_title=f"{currency}/PKR",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        # Candlestick from fx_ohlcv
        pair = f"{currency}/PKR"
        df = pd.read_sql_query(
            "SELECT date, open, high, low, close FROM fx_ohlcv"
            " WHERE pair=? ORDER BY date DESC LIMIT ?",
            con, params=(pair, limit),
        )
        if df.empty:
            st.info(f"No OHLCV data for {pair}. Use FX microservice sync.")
        else:
            df = df.sort_values("date")
            fig = _styled_fig(height=450)
            fig.add_trace(go.Candlestick(
                x=df["date"], open=df["open"], high=df["high"],
                low=df["low"], close=df["close"], name=pair,
                increasing_line_color=_COLORS["up"],
                decreasing_line_color=_COLORS["down"],
            ))
            fig.update_layout(
                title=dict(text=f"{pair} OHLC", font=dict(size=13)),
                xaxis_rangeslider_visible=False,
                yaxis_title="PKR",
            )
            st.plotly_chart(fig, use_container_width=True)

    # ── DXY + major pairs ──
    st.markdown("### Global FX (yfinance)")
    global_pairs = con.execute(
        "SELECT DISTINCT pair FROM commodity_fx_rates ORDER BY pair"
    ).fetchall()
    pair_list = [r["pair"] for r in global_pairs]
    if pair_list:
        sel_pairs = st.multiselect("Pairs", pair_list, default=["DXY", "USD_PKR"][:len(pair_list)], key="fx_global_pairs")
        if sel_pairs:
            fig = _styled_fig(height=380)
            for pair in sel_pairs:
                df = pd.read_sql_query(
                    "SELECT date, close FROM commodity_fx_rates"
                    " WHERE pair=? ORDER BY date DESC LIMIT ?",
                    con, params=(pair, limit),
                )
                if not df.empty:
                    df = df.sort_values("date")
                    fig.add_trace(go.Scatter(
                        x=df["date"], y=df["close"], mode="lines",
                        name=pair, line=dict(width=2),
                    ))
            fig.update_layout(
                title=dict(text="Global FX Pairs", font=dict(size=13)),
                yaxis_title="Rate",
                legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
            )
            st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 3: SPREADS
# ═════════════════════════════════════════════════════════════════════════════

def _render_spreads(con):
    # ── Bar chart: spread per currency ──
    st.markdown("### Interbank vs Kerb Spread")
    spreads = []
    for ccy in _KEY_CURRENCIES:
        ib = _get_latest_rate(con, "sbp_fx_interbank", ccy)
        kerb = _get_latest_rate(con, "forex_kerb", ccy)
        if ib and kerb and ib["selling"] and kerb["selling"]:
            spreads.append({
                "Currency": ccy,
                "Interbank": ib["selling"],
                "Kerb": kerb["selling"],
                "Spread": round(kerb["selling"] - ib["selling"], 2),
            })
    if spreads:
        sdf = pd.DataFrame(spreads)
        fig = _styled_fig(height=320)
        fig.add_trace(go.Bar(
            x=sdf["Currency"], y=sdf["Interbank"],
            name="Interbank", marker_color=_COLORS["interbank"],
        ))
        fig.add_trace(go.Bar(
            x=sdf["Currency"], y=sdf["Kerb"],
            name="Kerb", marker_color=_COLORS["kerb"],
        ))
        fig.update_layout(
            barmode="group", yaxis_title="PKR (Selling)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)

        # Spread bar
        fig2 = _styled_fig(height=220)
        bar_colors = [_COLORS["up"] if s > 0 else _COLORS["down"] for s in sdf["Spread"]]
        fig2.add_trace(go.Bar(
            x=sdf["Currency"], y=sdf["Spread"],
            marker_color=bar_colors,
            text=[f"{s:+.2f}" for s in sdf["Spread"]],
            textposition="outside",
        ))
        fig2.update_layout(yaxis_title="Kerb Premium (PKR)", showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No spread data — sync both interbank and kerb rates first")

    # ── Spread heatmap over time ──
    st.markdown("### Spread Heatmap (History)")
    df = pd.read_sql_query(
        """SELECT i.currency, i.date, ROUND(k.selling - i.selling, 2) as spread
           FROM sbp_fx_interbank i
           INNER JOIN forex_kerb k ON i.currency=k.currency AND i.date=k.date
           WHERE i.currency IN ('USD','EUR','GBP','SAR','AED')
           ORDER BY i.date DESC LIMIT 150""",
        con,
    )
    if not df.empty:
        pivot = df.pivot_table(index="date", columns="currency", values="spread").sort_index()
        if not pivot.empty:
            fig = go.Figure(go.Heatmap(
                z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
                colorscale=[[0, _COLORS["up"]], [0.5, "#FFEB3B"], [1, _COLORS["down"]]],
                colorbar=dict(title="Spread"),
                hovertemplate="Ccy: %{x}<br>Date: %{y}<br>Spread: %{z:.2f}<extra></extra>",
            ))
            fig.update_layout(**_CHART_LAYOUT, height=400, yaxis=dict(autorange="reversed", gridcolor=_COLORS["grid"]))
            fig.update_xaxes(**_AXIS_STYLE)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Need overlapping interbank + kerb dates for heatmap")

    # ── Buy-sell spread by source ──
    st.markdown("### Buy/Sell Spread by Source")
    spread_data = []
    for src_name, table in _FX_TABLES.items():
        for ccy in _KEY_CURRENCIES:
            rate = _get_latest_rate(con, table, ccy)
            if rate and rate["buying"] and rate["selling"]:
                spread_data.append({
                    "Source": src_name, "Currency": ccy,
                    "Spread": round(rate["selling"] - rate["buying"], 4),
                })
    if spread_data:
        bs_df = pd.DataFrame(spread_data)
        fig = _styled_fig(height=280)
        for src in bs_df["Source"].unique():
            sd = bs_df[bs_df["Source"] == src]
            fig.add_trace(go.Bar(
                x=sd["Currency"], y=sd["Spread"], name=src,
                marker_color=_SRC_COLORS.get(src, _COLORS["neutral"]),
            ))
        fig.update_layout(
            barmode="group", yaxis_title="Buy/Sell Spread (PKR)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 4: VOLATILITY
# ═════════════════════════════════════════════════════════════════════════════

def _render_volatility(con):
    st.markdown("### USD/PKR Volatility Analysis")

    # Try fx_ohlcv first, then sbp_fx_interbank
    df = pd.read_sql_query(
        "SELECT date, close FROM fx_ohlcv WHERE pair='USD/PKR' ORDER BY date", con,
    )
    source_label = "FX OHLCV"
    if len(df) < 10:
        df = pd.read_sql_query(
            "SELECT date, selling as close FROM sbp_fx_interbank"
            " WHERE UPPER(currency)='USD' ORDER BY date", con,
        )
        source_label = "SBP Interbank"

    if len(df) < 10:
        st.info("Need at least 10 data points for volatility analysis")
        return

    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    df["return"] = df["close"].pct_change()
    df["vol_10d"] = df["return"].rolling(10, min_periods=5).std() * (252 ** 0.5) * 100
    df["vol_30d"] = df["return"].rolling(30, min_periods=10).std() * (252 ** 0.5) * 100

    # ── Price + Vol dual axis ──
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.05,
                        row_heights=[0.6, 0.4])
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["close"], mode="lines", name="USD/PKR",
        line=dict(width=2, color=_COLORS["accent"]),
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=df["date"], y=df["vol_10d"], mode="lines", name="10D Vol",
        line=dict(width=1.5, color="#FF9800"),
    ), row=2, col=1)
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["vol_30d"], mode="lines", name="30D Vol",
        line=dict(width=2, color=_COLORS["down"]),
        fill="tozeroy", fillcolor="rgba(255,82,82,0.08)",
    ), row=2, col=1)

    fig.update_layout(
        **_CHART_LAYOUT, height=520,
        legend=dict(orientation="h", y=1.05, bgcolor="rgba(0,0,0,0)"),
    )
    fig.update_xaxes(**_AXIS_STYLE)
    fig.update_yaxes(title_text="USD/PKR", row=1, col=1, **_AXIS_STYLE)
    fig.update_yaxes(title_text="Ann. Vol %", row=2, col=1, **_AXIS_STYLE)
    st.plotly_chart(fig, use_container_width=True)
    st.caption(f"Source: {source_label} | {len(df)} data points")

    # ── Vol metrics ──
    latest_vol = df["vol_30d"].dropna()
    if not latest_vol.empty:
        vc1, vc2, vc3, vc4 = st.columns(4)
        with vc1:
            _card("Current 30D Vol", f"{latest_vol.iloc[-1]:.1f}%", color=_COLORS["down"])
        with vc2:
            _card("Avg 30D Vol", f"{latest_vol.mean():.1f}%")
        with vc3:
            _card("Max 30D Vol", f"{latest_vol.max():.1f}%")
        with vc4:
            # Drawdown
            peak = df["close"].cummax()
            dd = ((df["close"] - peak) / peak * 100)
            _card("Max Drawdown", f"{dd.min():.2f}%", color=_COLORS["down"])

    # ── Daily returns distribution ──
    st.markdown("### Daily Returns Distribution")
    returns = df["return"].dropna()
    if len(returns) > 10:
        fig = _styled_fig(height=280)
        fig.add_trace(go.Histogram(
            x=returns * 100, nbinsx=50,
            marker_color=_COLORS["accent"], opacity=0.7,
            name="Daily Returns",
        ))
        fig.add_vline(x=0, line_dash="dash", line_color=_COLORS["text_dim"])
        fig.update_layout(
            xaxis_title="Daily Return %", yaxis_title="Frequency",
            showlegend=False,
        )
        st.plotly_chart(fig, use_container_width=True)

        rc1, rc2, rc3 = st.columns(3)
        with rc1:
            _card("Mean Return", f"{returns.mean()*100:.4f}%")
        with rc2:
            _card("Skewness", f"{returns.skew():.2f}")
        with rc3:
            _card("Kurtosis", f"{returns.kurtosis():.2f}")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 5: CARRY
# ═════════════════════════════════════════════════════════════════════════════

def _render_carry(con):
    st.markdown("### Carry Trade Analysis")

    # KIBOR as PKR rate
    kibor = con.execute(
        "SELECT date, offer FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1"
    ).fetchone()

    policy = con.execute(
        "SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1"
    ).fetchone()

    kc1, kc2, kc3 = st.columns(3)
    with kc1:
        val = f"{policy['policy_rate']:.1f}%" if policy else "N/A"
        _card("SBP Policy Rate", val, color=_COLORS["policy"])
    with kc2:
        val = f"{kibor['offer']:.2f}%" if kibor else "N/A"
        _card("KIBOR 3M (Offer)", val, color=_COLORS["kibor"])
    with kc3:
        konia = con.execute("SELECT rate_pct FROM konia_daily ORDER BY date DESC LIMIT 1").fetchone()
        _card("KONIA (O/N)", f"{konia['rate_pct']:.2f}%" if konia else "N/A")

    if not kibor:
        st.info("No KIBOR data for carry calculation")
        return

    pkr_rate = kibor["offer"]

    # Global reference rates
    global_rates = {}
    try:
        rows = con.execute(
            "SELECT rate_name, rate FROM global_reference_rates"
            " WHERE rate_name IN ('SOFR','SONIA','EUSTR','TONA')"
            " ORDER BY date DESC"
        ).fetchall()
        for r in rows:
            if r["rate_name"] not in global_rates:
                global_rates[r["rate_name"]] = r["rate"]
    except Exception:
        pass

    rate_map = {
        "USD (SOFR)": global_rates.get("SOFR", 4.30),
        "GBP (SONIA)": global_rates.get("SONIA", 4.45),
        "EUR (EUSTR)": global_rates.get("EUSTR", 2.65),
        "JPY (TONA)": global_rates.get("TONA", 0.23),
    }

    st.markdown("### Carry Differential")
    carry_data = []
    for label, foreign_rate in rate_map.items():
        carry = pkr_rate - foreign_rate
        carry_data.append({
            "Currency": label, "Foreign Rate": foreign_rate,
            "PKR Rate": pkr_rate, "Carry": carry,
        })

    cdf = pd.DataFrame(carry_data)

    fig = _styled_fig(height=300)
    bar_colors = [_COLORS["up"] if c > 0 else _COLORS["down"] for c in cdf["Carry"]]
    fig.add_trace(go.Bar(
        x=cdf["Currency"], y=cdf["Carry"],
        marker_color=bar_colors,
        text=[f"{c:+.2f}%" for c in cdf["Carry"]],
        textposition="outside",
    ))
    fig.add_hline(y=0, line_dash="dash", line_color=_COLORS["text_dim"])
    fig.update_layout(yaxis_title="Carry Spread (%)", showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

    st.dataframe(
        cdf.rename(columns={
            "Foreign Rate": "Foreign (%)", "PKR Rate": "PKR (%)", "Carry": "Carry (%)",
        }),
        use_container_width=True, hide_index=True,
    )

    # ── NPC Certificate Rates ──
    npc = pd.read_sql_query(
        "SELECT * FROM npc_rates ORDER BY date DESC, tenor", con,
    )
    if not npc.empty:
        st.markdown("### NPC Certificate Rates")
        st.caption("National Prize Certificate / PKR deposit alternatives for NRPs")
        st.dataframe(npc, use_container_width=True, hide_index=True)

    # ── KIBOR History chart ──
    st.markdown("### KIBOR Term Structure History")
    kdf = pd.read_sql_query(
        "SELECT date, tenor, offer FROM kibor_daily"
        " WHERE tenor IN ('1M','3M','6M','1Y') AND offer IS NOT NULL ORDER BY date",
        con,
    )
    if not kdf.empty:
        fig = _styled_fig(height=350)
        kibor_colors = {"1M": "#FF6B35", "3M": "#4ECDC4", "6M": "#45B7D1", "1Y": "#96CEB4"}
        for tenor in ["1M", "3M", "6M", "1Y"]:
            tdf = kdf[kdf["tenor"] == tenor]
            if not tdf.empty:
                fig.add_trace(go.Scatter(
                    x=tdf["date"], y=tdf["offer"], mode="lines",
                    name=f"KIBOR {tenor}",
                    line=dict(width=2, color=kibor_colors.get(tenor, "#999")),
                ))
        fig.update_layout(
            yaxis_title="Offer Rate (%)",
            legend=dict(orientation="h", y=-0.12, bgcolor="rgba(0,0,0,0)"),
        )
        st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# TAB 6: FX SIGNALS (microservice)
# ═════════════════════════════════════════════════════════════════════════════

def _render_fx_signals(con):
    if not _fx.is_healthy():
        st.info(
            "FX microservice not running — showing DB-sourced rates only. "
            "Start it: `uvicorn api.service:app --port 8100`"
        )
        return

    st.markdown("### FX Trading Signals")

    # KIBOR Live
    data = _fx.get_kibor()
    if data:
        rates = data.get("rates", data.get("kibor", []))
        if rates:
            st.markdown("#### KIBOR Rates (Live)")
            rows = [{"Tenor": r.get("tenor"), "Bid": r.get("bid"),
                      "Offer": r.get("offer"), "Mid": r.get("mid")}
                    for r in rates if isinstance(r, dict)]
            if rows:
                st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Regime
    regime_data = _fx.get_regime()
    if regime_data:
        st.markdown("#### FX-Equity Regime")
        regime = regime_data.get("regime", "unknown")
        regime_colors = {"pkr_weakening": "red", "pkr_strengthening": "green", "stable": "blue"}
        color = regime_colors.get(regime, "gray")
        st.markdown(f"**Regime:** :{color}[{regime.replace('_',' ').title()}]")

        rc = st.columns(3)
        rc[0].metric("Equity Signal", regime_data.get("equity_signal", "N/A"))
        rc[1].metric("Sector Bias", regime_data.get("sector_bias", "N/A"))
        metrics = regime_data.get("metrics", {})
        if metrics:
            rc[2].metric("USD/PKR", f"{metrics.get('last_close', 0):.2f}")

    # Carry + Premium (side by side)
    report = _fx.get_signal_report()
    if report:
        sc1, sc2 = st.columns(2)
        with sc1:
            carry = report.get("carry_trade", report.get("carry", {}))
            if carry:
                st.markdown("#### Carry Trade")
                best = carry.get("best_carry", carry.get("signal", {}))
                if isinstance(best, dict):
                    st.metric("Best Carry", best.get("pair", "N/A"),
                              delta=f"{best.get('differential', 0):.1f}%")
                signals = carry.get("signals", carry.get("pairs", []))
                if signals and isinstance(signals, list):
                    rows = [{"Pair": s.get("pair"), "Differential": s.get("differential"),
                             "Signal": s.get("signal")}
                            for s in signals if isinstance(s, dict)]
                    if rows:
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        with sc2:
            prem = report.get("premium_spread", report.get("premium", {}))
            if prem:
                st.markdown("#### Premium Spread")
                stress = prem.get("stress_level", prem.get("signal", ""))
                if stress:
                    sc_colors = {"low": "green", "moderate": "orange", "high": "red", "elevated": "orange"}
                    st.markdown(f"**Stress:** :{sc_colors.get(stress.lower(), 'gray')}[{stress.title()}]")
                pairs = prem.get("pairs", prem.get("spreads", []))
                if pairs and isinstance(pairs, list):
                    rows = [{"Pair": p.get("pair"),
                             "Interbank": p.get("interbank", p.get("official")),
                             "Open Mkt": p.get("open_market", p.get("kerb")),
                             "Gap %": p.get("gap_pct", p.get("premium_pct"))}
                            for p in pairs if isinstance(p, dict)]
                    if rows:
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

    # Intervention
    intv_data = _fx.get_intervention()
    if intv_data:
        st.markdown("#### SBP Intervention Detection")
        signal = intv_data.get("signal", intv_data)
        if isinstance(signal, dict):
            ic = st.columns(3)
            likely = signal.get("likely", signal.get("intervention_likely", False))
            conf = signal.get("confidence", 0)
            direction = signal.get("direction", signal.get("stance", "N/A"))
            ic[0].metric("Likely", "Yes" if likely else "No")
            ic[1].metric("Confidence", f"{conf:.0%}" if isinstance(conf, float) and conf <= 1 else str(conf))
            ic[2].metric("Direction", str(direction).replace("_", " ").title())

    # AI Commentary
    st.divider()
    render_ai_commentary(con, "FX")


# ═════════════════════════════════════════════════════════════════════════════
# TAB 7: SYNC
# ═════════════════════════════════════════════════════════════════════════════

def _render_sync(con):
    st.markdown("### Sync FX Data")

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Sync SBP Interbank", type="primary", key="fx_sync_interbank"):
            with st.spinner("Syncing SBP interbank rates..."):
                try:
                    result = SBPFXScraper().sync_interbank(con)
                    st.success(f"Interbank: {result.get('ok', 0)} rates synced")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")
    with col2:
        if st.button("Sync Kerb (forex.pk)", key="fx_sync_kerb"):
            with st.spinner("Syncing kerb rates from forex.pk..."):
                try:
                    result = ForexPKScraper().sync_kerb(con)
                    st.success(f"Kerb: {result.get('ok', 0)} rates synced")
                    st.rerun()
                except Exception as e:
                    st.error(f"Sync failed: {e}")

    if _fx.is_healthy():
        col3, col4 = st.columns(2)
        with col3:
            if st.button("Sync from FX Microservice", key="fx_sync_micro"):
                with st.spinner("Syncing rates from FX microservice..."):
                    try:
                        from pakfindata.sources.fx_sync import sync_fx_rates
                        result = sync_fx_rates(con)
                        st.success(
                            f"FX micro: {result.get('rates_stored', 0)} rates, "
                            f"{result.get('kibor_stored', 0)} KIBOR tenors"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"FX micro sync failed: {e}")
        with col4:
            if st.button("Backfill FX History", key="fx_backfill"):
                with st.spinner("Backfilling FX history from microservice..."):
                    try:
                        from pakfindata.sources.fx_sync import backfill_fx_history
                        result = backfill_fx_history(con)
                        st.success(
                            f"Backfill: {result.get('inserted', 0)} inserted, "
                            f"{result.get('skipped', 0)} skipped"
                        )
                        st.rerun()
                    except Exception as e:
                        st.error(f"Backfill failed: {e}")

    # Sync runs table
    try:
        runs = pd.read_sql_query(
            "SELECT * FROM fx_sync_runs ORDER BY started_at DESC LIMIT 10", con,
        )
        if not runs.empty:
            st.markdown("#### Recent Sync Runs")
            st.dataframe(runs, use_container_width=True, hide_index=True)
    except Exception:
        pass
