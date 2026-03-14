"""Sector & Breadth Analysis — Combined-Symbol FFT and Aggregate VPIN.

Tabs:
  1) Aggregate Macro (FFT) — synthetic basket index + IFFT trendline
  2) Systemic Risk (VPIN)  — aggregate toxicity gauge + combined volume buckets
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.engine.fft_cycles import compute_fft_cycles
from pakfindata.engine.microstructure import (
    compute_vpin,
    evaluate_payoff,
    build_payoff_table,
)
from pakfindata.engine.commentary import (
    get_fft_rules_commentary,
    get_fft_ai_commentary,
    get_vpin_rules_commentary,
    get_vpin_ai_commentary,
)
from pakfindata.ui.components.helpers import get_connection, render_footer


# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "warning": "#FFD600", "toxic": "#FF1744",
    "safe": "#00C853", "vpin_line": "#FFD600",
    "buy": "#00E676", "sell": "#FF5252",
    "cycle_line": "#00B0FF", "ifft_line": "#FFD600",
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


# ═════════════════════════════════════════════════════════════════════════════
# DATA LOADERS
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def _get_master_symbols(_con) -> list[str]:
    rows = _con.execute(
        "SELECT symbol FROM symbols WHERE is_active=1 ORDER BY symbol"
    ).fetchall()
    return [r[0] for r in rows]


@st.cache_data(ttl=300)
def _get_sectors(_con) -> dict[str, str]:
    """Return {sector_code: sector_name} mapping."""
    rows = _con.execute(
        "SELECT sector_code, sector_name FROM sectors ORDER BY sector_name"
    ).fetchall()
    return {r[0]: r[1] for r in rows}


@st.cache_data(ttl=300)
def _get_symbols_by_sector(_con, sector_code: str) -> list[str]:
    """Return symbols belonging to a sector (from eod_ohlcv latest date)."""
    rows = _con.execute(
        "SELECT DISTINCT symbol FROM eod_ohlcv "
        "WHERE sector_code=? AND date=(SELECT MAX(date) FROM eod_ohlcv) "
        "ORDER BY symbol",
        (sector_code,),
    ).fetchall()
    return [r[0] for r in rows]


def _load_eod_basket(con, symbols: list[str], limit: int = 500) -> pd.DataFrame:
    """Load EOD data for multiple symbols, return combined synthetic basket.

    Aggregation: group by date, mean(close) → synthetic index price.
    """
    placeholders = ",".join("?" * len(symbols))
    df = pd.read_sql_query(
        f"SELECT date, symbol, open, high, low, close, volume FROM eod_ohlcv "
        f"WHERE symbol IN ({placeholders}) AND volume > 0 "
        f"ORDER BY date DESC LIMIT ?",
        con,
        params=(*symbols, limit * len(symbols)),
    )
    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])

    # Aggregate: mean price, sum volume per date
    agg = df.groupby("date").agg(
        open=("open", "mean"),
        high=("high", "mean"),
        low=("low", "mean"),
        close=("close", "mean"),
        volume=("volume", "sum"),
        n_symbols=("symbol", "nunique"),
    ).reset_index().sort_values("date").reset_index(drop=True)

    agg.rename(columns={"date": "datetime"}, inplace=True)
    return agg


def _get_intraday_dates(con) -> list[str]:
    """Get available trading dates from intraday_bars."""
    dates: list[str] = []
    row = con.execute("SELECT MAX(ts) FROM intraday_bars").fetchone()
    if not row or not row[0]:
        return dates
    cur = row[0][:10]
    for _ in range(30):
        dates.append(cur)
        prev = con.execute(
            "SELECT MAX(ts) FROM intraday_bars WHERE ts < ?", (cur,)
        ).fetchone()
        if not prev or not prev[0]:
            break
        cur = prev[0][:10]
    return dates


def _load_intraday_basket(
    con, symbols: list[str], date_str: str,
) -> pd.DataFrame:
    """Load intraday bars for multiple symbols, merge chronologically.

    Returns a single DataFrame sorted by ts_epoch with combined volume.
    """
    placeholders = ",".join("?" * len(symbols))
    df = pd.read_sql_query(
        f"SELECT symbol, ts AS datetime, ts_epoch, close, volume "
        f"FROM intraday_bars "
        f"WHERE symbol IN ({placeholders}) AND ts BETWEEN ? AND ? "
        f"ORDER BY ts_epoch",
        con,
        params=(*symbols, f"{date_str} 00:00:00", f"{date_str} 23:59:59"),
    )
    if df.empty:
        return pd.DataFrame()

    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df[df["volume"] > 0].copy()

    # Aggregate: group by ts_epoch (same-second trades across symbols)
    agg = df.groupby("ts_epoch").agg(
        datetime=("datetime", "first"),
        close=("close", "mean"),
        volume=("volume", "sum"),
    ).reset_index().sort_values("ts_epoch").reset_index(drop=True)

    return agg


# ═════════════════════════════════════════════════════════════════════════════
# FFT TAB CHARTS
# ═════════════════════════════════════════════════════════════════════════════

def _render_fft_spectrum(spectrum: pd.DataFrame, dominant_cycles: list[dict]):
    """Power spectrum bar chart with dominant spikes highlighted."""
    top_spec = spectrum.head(30).sort_values("period")
    dom_periods = {round(c["period"], 1) for c in dominant_cycles[:3]}

    colors = [
        _COLORS["warning"] if round(row["period"], 1) in dom_periods
        else _COLORS["cycle_line"]
        for _, row in top_spec.iterrows()
    ]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=top_spec["period"], y=top_spec["amplitude"],
        marker_color=colors, opacity=0.85,
        hovertemplate="Period: %{x:.0f} days<br>Amplitude: %{y:.4f}<extra></extra>",
    ))

    for i, c in enumerate(dominant_cycles[:3]):
        fig.add_annotation(
            x=c["period"], y=c["amplitude"],
            text=f"{c['period']:.0f}d",
            showarrow=True, arrowhead=2,
            font=dict(color=_COLORS["warning"], size=12),
            arrowcolor=_COLORS["warning"], ax=0, ay=-30 - i * 15,
        )

    fig.update_layout(
        **_CHART_LAYOUT, height=380,
        title=dict(text="Aggregate Power Spectrum — Sector Cycle Frequencies",
                   font=dict(size=14)),
        xaxis_title="Period (days)", yaxis_title="Amplitude",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_fft_overlay(df: pd.DataFrame, ifft_signal, dates):
    """Synthetic index price + IFFT trendline overlay."""
    has_ohlc = all(c in df.columns for c in ["open", "high", "low", "close"])

    fig = go.Figure()
    if has_ohlc:
        fig.add_trace(go.Candlestick(
            x=dates, open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            increasing_line_color=_COLORS["up"],
            decreasing_line_color=_COLORS["down"],
            name="Synthetic Index",
        ))
    else:
        fig.add_trace(go.Scatter(
            x=dates, y=df["close"], mode="lines",
            line=dict(color=_COLORS["neutral"], width=1.5),
            name="Synthetic Close",
        ))

    fig.add_trace(go.Scatter(
        x=dates, y=ifft_signal, mode="lines",
        line=dict(color=_COLORS["ifft_line"], width=2.5),
        name="IFFT Trendline (zero-lag)",
    ))

    fig.update_layout(
        **_CHART_LAYOUT, height=480,
        title=dict(text="Synthetic Sector Index with FFT Cycle Trendline",
                   font=dict(size=14)),
        xaxis_rangeslider_visible=False, hovermode="x unified",
    )
    fig.update_yaxes(title_text="Price (avg)")
    st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# VPIN TAB CHARTS
# ═════════════════════════════════════════════════════════════════════════════

def _render_systemic_gauge(vpin_value: float):
    """Systemic toxicity gauge (0 → 1)."""
    if vpin_value < 0.4:
        bar_color, label = _COLORS["safe"], "SAFE"
    elif vpin_value < 0.7:
        bar_color, label = _COLORS["warning"], "ELEVATED"
    else:
        bar_color, label = _COLORS["toxic"], "TOXIC"

    fig = go.Figure(go.Indicator(
        mode="gauge+number",
        value=vpin_value,
        number=dict(
            font=dict(size=48, color=bar_color, family="JetBrains Mono"),
            valueformat=".3f",
        ),
        title=dict(
            text=f"Systemic Order Flow Toxicity — {label}",
            font=dict(size=16, color=_COLORS["text"]),
        ),
        gauge=dict(
            axis=dict(range=[0, 1], tickwidth=2, tickcolor=_COLORS["text_dim"],
                      dtick=0.1, tickfont=dict(size=10)),
            bar=dict(color=bar_color, thickness=0.3),
            bgcolor="rgba(0,0,0,0)", borderwidth=0,
            steps=[
                dict(range=[0, 0.4], color="rgba(0,200,83,0.15)"),
                dict(range=[0.4, 0.7], color="rgba(255,214,0,0.15)"),
                dict(range=[0.7, 1.0], color="rgba(255,23,68,0.15)"),
            ],
            threshold=dict(
                line=dict(color=_COLORS["text"], width=3),
                thickness=0.8, value=vpin_value,
            ),
        ),
    ))
    fig.update_layout(
        height=280, paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color=_COLORS["text"]),
        margin=dict(l=30, r=30, t=60, b=10),
    )
    st.plotly_chart(fig, use_container_width=True)


def _render_aggregate_bucket_flow(buckets: pd.DataFrame):
    """Stacked bar chart of aggregate V_buy/V_sell with VPIN overlay."""
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Bar(x=buckets["bucket_id"], y=buckets["V_buy"],
               name="Aggregate Buy Vol", marker_color=_COLORS["buy"], opacity=0.8),
        secondary_y=False,
    )
    fig.add_trace(
        go.Bar(x=buckets["bucket_id"], y=buckets["V_sell"],
               name="Aggregate Sell Vol", marker_color=_COLORS["sell"], opacity=0.8),
        secondary_y=False,
    )

    if "vpin" in buckets.columns:
        fig.add_trace(
            go.Scatter(x=buckets["bucket_id"], y=buckets["vpin"],
                       name="Aggregate VPIN",
                       line=dict(color=_COLORS["vpin_line"], width=2.5),
                       mode="lines"),
            secondary_y=True,
        )
        fig.add_hline(y=0.7, line_dash="dot", line_color=_COLORS["toxic"],
                      annotation_text="Toxic (0.7)", secondary_y=True,
                      annotation_font_color=_COLORS["toxic"])
        fig.add_hline(y=0.4, line_dash="dot", line_color=_COLORS["warning"],
                      annotation_text="Elevated (0.4)", secondary_y=True,
                      annotation_font_color=_COLORS["warning"])

    fig.update_layout(
        **_CHART_LAYOUT, height=420, barmode="stack",
        title=dict(text="Aggregate Volume Bucket Flow — Sector Basket",
                   font=dict(size=14)),
        hovermode="x unified",
    )
    fig.update_xaxes(title_text="Volume Bucket", gridcolor=_COLORS["grid"])
    fig.update_yaxes(title_text="Volume", gridcolor=_COLORS["grid"], secondary_y=False)
    fig.update_yaxes(title_text="Agg. VPIN", range=[0, 1.05],
                     gridcolor=_COLORS["grid"], secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)


def _render_payoff_matrix(vpin: float, half_spread: float, adverse_loss: float):
    """Game Theory payoff matrix with conditional formatting."""
    table = build_payoff_table(vpin, half_spread, adverse_loss)
    payoff = evaluate_payoff(vpin, half_spread, adverse_loss)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Agg. VPIN (π)", f"{payoff.vpin:.3f}")
    with c2:
        st.metric("Half-Spread (s)", f"{payoff.half_spread:.2f}")
    with c3:
        st.metric("Adverse Loss (L)", f"{payoff.adverse_loss:.2f}")
    with c4:
        ev_delta = "positive" if payoff.ev_make > 0 else "negative"
        st.metric("EV_make", f"{payoff.ev_make:.4f}",
                  delta=ev_delta,
                  delta_color="normal" if payoff.ev_make > 0 else "inverse")

    def _color_ev(val):
        if isinstance(val, (int, float)):
            if val > 0:
                return f"color: {_COLORS['safe']}; font-weight: 700"
            elif val < 0:
                return f"color: {_COLORS['toxic']}; font-weight: 700"
            return f"color: {_COLORS['warning']}; font-weight: 700"
        return ""

    def _color_strategy(val):
        if isinstance(val, str):
            if "MAKER" in val:
                return f"color: {_COLORS['safe']}; font-weight: 600"
            elif "TAKER" in val:
                return f"color: {_COLORS['toxic']}; font-weight: 600"
            elif "NEUTRAL" in val:
                return f"color: {_COLORS['warning']}; font-weight: 600"
        return ""

    def _highlight_current(row):
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
    st.dataframe(styled, use_container_width=True, hide_index=True, height=210)


# ═════════════════════════════════════════════════════════════════════════════
# PLAYBOOK
# ═════════════════════════════════════════════════════════════════════════════

_PLAYBOOK_MD = """
### STEP 1: Find the Macro Bias (Daily FFT)
*Question: Are we structurally overbought or oversold?*

Look at the **Aggregate Macro (FFT)** tab. Identify the Dominant Cycle length and compare the Synthetic Index Price to the smooth **IFFT Signal Line**.

* :chart_with_downwards_trend: **Oversold (Bull Bias):** Price is significantly **BELOW** the IFFT line.
* :chart_with_upwards_trend: **Overbought (Bear Bias):** Price is significantly **ABOVE** the IFFT line. Mean-reversion is due.
* :heavy_minus_sign: **White Noise:** Power spectrum is flat. Cycles are dead.

---

### STEP 2: Read the Systemic Microstructure (Aggregate VPIN)
*Question: Is there sector-wide informed trading?*

Switch to the **Systemic Risk (VPIN)** tab. Check the **Systemic Toxicity Gauge**.

* :green_circle: **Agg. VPIN < 0.4:** Balanced flow across the basket. Noise-dominated.
* :yellow_circle: **Agg. VPIN 0.4 - 0.7:** Sector-level imbalance building. Institutions accumulating/distributing across multiple names.
* :red_circle: **Agg. VPIN > 0.7:** **SECTOR-WIDE TOXICITY.** Pull all limit orders across these symbols.

---

### STEP 3: The Game Theory Execution

| Macro Bias (FFT) | Systemic Toxicity (VPIN) | EV | Execution Strategy |
| :--- | :--- | :--- | :--- |
| **BULLISH** | :green_circle: **LOW** | **+** | **MAKER:** Post Limit Buys across the basket. |
| **BULLISH** | :red_circle: **HIGH** | **-** | **TAKER:** Market Buy immediately — institutions are sweeping. |
| **BEARISH** | :green_circle: **LOW** | **+** | **MAKER:** Post Limit Sells across the basket. |
| **BEARISH** | :red_circle: **HIGH** | **-** | **TAKER:** Market Sell immediately. |
| **NEUTRAL** | :red_circle: **HIGH** | **-** | **AVOID:** Systemic toxic chop. Step away. |
"""


# ═════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═════════════════════════════════════════════════════════════════════════════

def render_sector_breadth():
    """Main entry point for the Sector & Breadth Analysis page."""
    st.markdown("## Sector & Breadth Analysis")
    st.caption(
        "Combined-Symbol FFT Cycles · Aggregate VPIN · Systemic Liquidity Risk"
    )

    with st.expander("How to Read This Analysis (Execution Playbook)", expanded=False):
        st.markdown(_PLAYBOOK_MD)

    con = get_connection()

    # ── Symbol selection ───────────────────────────────────────────────────
    all_symbols = _get_master_symbols(con)

    # Sector quick-fill
    sectors = _get_sectors(con)
    col_sector, col_multi = st.columns([1, 3])

    with col_sector:
        sector_options = ["-- Manual --"] + [
            f"{code} — {name}" for code, name in sectors.items()
        ]
        sector_pick = st.selectbox("Quick-Fill by Sector", sector_options,
                                   key="sb_sector")

    default_symbols: list[str] = []
    if sector_pick != "-- Manual --":
        sector_code = sector_pick.split(" — ")[0]
        default_symbols = _get_symbols_by_sector(con, sector_code)

    with col_multi:
        selected_symbols = st.multiselect(
            "Select Basket Symbols",
            all_symbols,
            default=default_symbols,
            key="sb_symbols",
            help="Pick 2+ symbols to build a synthetic basket",
        )

    if len(selected_symbols) < 2:
        st.info("Select at least **2 symbols** to build a sector basket.")
        render_footer()
        return

    st.success(
        f"Basket: **{len(selected_symbols)} symbols** — "
        + ", ".join(selected_symbols[:10])
        + ("..." if len(selected_symbols) > 10 else "")
    )

    # ── Tabs ───────────────────────────────────────────────────────────────
    tab_fft, tab_vpin = st.tabs(["Aggregate Macro (FFT)", "Systemic Risk (VPIN)"])

    # ══════════════════════════════════════════════════════════════════════
    # TAB 1: AGGREGATE MACRO (FFT)
    # ══════════════════════════════════════════════════════════════════════
    with tab_fft:
        st.markdown("### Aggregate FFT — Sector Cycle Detection")

        col_lb, col_topn, col_lp = st.columns(3)
        with col_lb:
            eod_days = st.slider("Lookback (trading days)", 60, 1000, 500,
                                 step=10, key="sb_lookback")
        with col_topn:
            top_n = st.slider("Dominant Cycles (top-N)", 3, 10, 5, key="sb_topn")
        with col_lp:
            low_pass = st.slider("IFFT Components", 2, 15, 5, key="sb_lp")

        basket_eod = _load_eod_basket(con, selected_symbols, limit=eod_days)

        if basket_eod.empty or len(basket_eod) < 20:
            st.warning("Not enough EOD data for the selected basket.")
        else:
            st.caption(
                f"{len(basket_eod)} daily bars · "
                f"Avg symbols/day: {basket_eod['n_symbols'].mean():.1f}"
            )

            fft_result = compute_fft_cycles(
                basket_eod, top_n=top_n, low_pass_periods=low_pass,
            )

            # Dominant cycles KPI
            if fft_result.dominant_cycles:
                cols = st.columns(min(len(fft_result.dominant_cycles), 5))
                for i, c in enumerate(fft_result.dominant_cycles[:5]):
                    with cols[i]:
                        st.metric(f"Cycle #{i+1}",
                                  f"{c['period']:.0f} days",
                                  f"Amp: {c['amplitude']:.4f}")

            _render_fft_spectrum(fft_result.spectrum, fft_result.dominant_cycles)
            _render_fft_overlay(basket_eod, fft_result.ifft_signal, fft_result.dates)

            # Phase analysis
            if fft_result.dominant_cycles:
                cycle_days = fft_result.dominant_cycles[0]["period"]
                current_price = float(basket_eod["close"].iloc[-1])
                ifft_price = float(fft_result.ifft_signal[-1])
                diff_pct = ((current_price - ifft_price) / ifft_price) * 100 if ifft_price else 0

                st.markdown("### Sector Cycle Phase")
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.metric("Synthetic Index", f"{current_price:,.2f}")
                with c2:
                    st.metric("IFFT Trendline", f"{ifft_price:,.2f}")
                with c3:
                    phase = "Expansion" if diff_pct > 0 else "Contraction"
                    st.metric("Deviation", f"{diff_pct:+.2f}%",
                              delta=phase,
                              delta_color="normal" if diff_pct > 0 else "inverse")

                # Commentary
                basket_label = ", ".join(selected_symbols[:3])
                if len(selected_symbols) > 3:
                    basket_label += f" +{len(selected_symbols)-3}"

                with st.expander("Sector Cycle Commentary", expanded=True):
                    use_ai = st.toggle("Enable LLM Analysis", value=False,
                                       key="sb_fft_ai")
                    if not use_ai:
                        st.markdown(get_fft_rules_commentary(
                            cycle_days, current_price, ifft_price))
                    else:
                        with st.spinner("Generating sector cycle analysis..."):
                            ai = get_fft_ai_commentary(
                                cycle_days, current_price, ifft_price, basket_label)
                            if ai is None:
                                st.warning("Set `OPENAI_API_KEY` in `.env` for LLM.")
                                st.markdown(get_fft_rules_commentary(
                                    cycle_days, current_price, ifft_price))
                            else:
                                from pakfindata.ui.components.commentary_renderer import render_styled_commentary
                                render_styled_commentary(ai, "Sector Cycle Analysis")

    # ══════════════════════════════════════════════════════════════════════
    # TAB 2: SYSTEMIC RISK (AGGREGATE VPIN)
    # ══════════════════════════════════════════════════════════════════════
    with tab_vpin:
        st.markdown("### Systemic Risk — Aggregate VPIN")

        col_dsrc, col_dt = st.columns([1, 1])
        with col_dsrc:
            vpin_source = st.selectbox(
                "Data Source",
                ["Intraday Bars (Combined)", "EOD Daily Bars (Combined)"],
                key="sb_vpin_src",
            )
        with col_dt:
            if vpin_source.startswith("Intraday"):
                avail_dates = _get_intraday_dates(con)
                if avail_dates:
                    sel_date = st.selectbox("Trading Date", avail_dates,
                                           key="sb_vpin_date")
                else:
                    st.warning("No intraday data.")
                    sel_date = None
            else:
                vpin_eod_days = st.slider("Lookback (days)", 50, 500, 200,
                                          step=10, key="sb_vpin_lb")

        # Sidebar: VPIN + Game Theory params
        with st.sidebar:
            st.markdown("### Aggregate VPIN Parameters")
            bucket_mode = st.radio("Bucket Size", ["Auto", "Manual"],
                                    index=0, horizontal=True, key="sb_bk_mode")
            manual_bucket = None
            if bucket_mode == "Manual":
                manual_bucket = st.number_input("Volume per Bucket", 1000, 5000000,
                                                200000, step=10000, key="sb_bk_val")
            vpin_window = st.slider("VPIN Window (n)", 10, 100, 50, key="sb_vpin_w")
            st.markdown("---")
            st.markdown("### Game Theory")
            half_spread = st.number_input("Half-Spread (s)", 0.01, 5.0, 0.50,
                                          step=0.05, format="%.2f", key="sb_hs")
            adverse_loss = st.number_input("Adverse Loss (L)", 0.1, 20.0, 2.0,
                                            step=0.1, format="%.1f", key="sb_al")

        # Load combined data
        combined_df = None

        if vpin_source.startswith("Intraday") and sel_date:
            combined_df = _load_intraday_basket(con, selected_symbols, sel_date)
            if combined_df.empty:
                st.warning(f"No intraday data for basket on {sel_date}.")
                combined_df = None
            else:
                st.success(
                    f"**{len(combined_df):,}** combined bars on {sel_date} · "
                    f"Total vol: {combined_df['volume'].sum():,.0f}",
                    icon="📡",
                )
        elif vpin_source.startswith("EOD"):
            eod_combined = _load_eod_basket(con, selected_symbols, limit=vpin_eod_days)
            if eod_combined.empty:
                st.warning("No EOD data for the selected basket.")
            else:
                combined_df = eod_combined
                st.success(
                    f"**{len(combined_df)}** combined daily bars · "
                    f"Total vol: {combined_df['volume'].sum():,.0f}",
                    icon="📊",
                )

        if combined_df is None or combined_df.empty:
            st.stop()
            return

        # Compute aggregate VPIN
        vpin_result = compute_vpin(
            combined_df,
            bucket_size=manual_bucket,
            window=vpin_window,
        )

        # ── Systemic Toxicity Gauge ────────────────────────────────────────
        col_gauge, col_stats = st.columns([2, 1])

        with col_gauge:
            _render_systemic_gauge(vpin_result.current_vpin)

        with col_stats:
            vpin_s = vpin_result.vpin_series
            card_bg = _COLORS["card_bg"]
            dim = _COLORS["text_dim"]
            stats_html = (
                f'<div style="background:{card_bg};padding:20px;border-radius:8px;'
                f'font-family:monospace;line-height:2.2;">'
                f'<div style="color:{dim};font-size:0.8em;margin-bottom:8px;">'
                f'AGGREGATE VPIN STATISTICS</div>'
                f'<div>Current: <b style="color:{_COLORS["vpin_line"]}">'
                f'{vpin_result.current_vpin:.4f}</b></div>'
                f'<div>Mean: <b>{vpin_s.mean():.4f}</b></div>'
                f'<div>Max: <b style="color:{_COLORS["toxic"]}">'
                f'{vpin_s.max():.4f}</b></div>'
                f'<div>Min: <b style="color:{_COLORS["safe"]}">'
                f'{vpin_s.min():.4f}</b></div>'
                f'<div>Std Dev: <b>{vpin_s.std():.4f}</b></div>'
                f'<div style="margin-top:8px;color:{dim};font-size:0.75em;">'
                f'Buckets: {len(vpin_result.buckets)} · '
                f'Size: {vpin_result.dominant_bucket_size:,}</div>'
                f'</div>'
            )
            st.markdown(stats_html, unsafe_allow_html=True)

        # Systemic warning
        if vpin_result.current_vpin > 0.7:
            st.error(
                "**SECTOR-WIDE TOXICITY DETECTED:** Aggregate VPIN > 0.7. "
                "Pull all limit orders across these symbols. "
                "Informed algorithms are sweeping the entire basket.",
                icon="🚨",
            )

        # ── Aggregate Volume Bucket Flow ───────────────────────────────────
        st.markdown("### Aggregate Volume Bucket Flow")
        _render_aggregate_bucket_flow(vpin_result.buckets)

        # ── Game Theory Payoff Matrix ──────────────────────────────────────
        st.markdown("### Game Theory — Sector Basket Payoff Matrix")
        st.caption("EV_make = (1 - π) × s  −  π × L")
        _render_payoff_matrix(vpin_result.current_vpin, half_spread, adverse_loss)

        # ── Commentary ─────────────────────────────────────────────────────
        payoff = evaluate_payoff(vpin_result.current_vpin, half_spread, adverse_loss)
        basket_label = ", ".join(selected_symbols[:3])
        if len(selected_symbols) > 3:
            basket_label += f" +{len(selected_symbols)-3}"

        with st.expander("Systemic Risk Commentary", expanded=True):
            use_ai = st.toggle("Enable LLM Analysis", value=False,
                               key="sb_vpin_ai")
            if not use_ai:
                st.markdown(get_vpin_rules_commentary(
                    vpin_result.current_vpin, payoff.ev_make, half_spread))
            else:
                with st.spinner("Generating systemic risk analysis..."):
                    ai = get_vpin_ai_commentary(
                        vpin_result.current_vpin, payoff.ev_make,
                        f"Basket({basket_label})", half_spread)
                    if ai is None:
                        st.warning("Set `OPENAI_API_KEY` in `.env` for LLM.")
                        st.markdown(get_vpin_rules_commentary(
                            vpin_result.current_vpin, payoff.ev_make, half_spread))
                    else:
                        from pakfindata.ui.components.commentary_renderer import render_styled_commentary
                        render_styled_commentary(ai, "Systemic Risk Analysis")

    render_footer()
