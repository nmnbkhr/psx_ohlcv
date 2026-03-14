"""Macro Cycles — FFT spectral analysis & zero-lag trendline overlay.

Charts:
  A) Power Spectrum — bar chart of amplitude vs period (days)
  B) Candlestick + IFFT trendline overlay (time domain)
  C) Quant Analyst Commentary (rule-based + optional OpenAI)
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.engine.fft_cycles import compute_fft_cycles
from pakfindata.engine.commentary import get_fft_rules_commentary, get_fft_ai_commentary
from pakfindata.ui.components.helpers import get_connection, render_footer

# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM (matches microstructure page)
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "up": "#00E676", "down": "#FF5252", "neutral": "#78909C",
    "accent": "#00D4AA", "warning": "#FFD600",
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
# CHART A: POWER SPECTRUM
# ═════════════════════════════════════════════════════════════════════════════

def _render_power_spectrum(spectrum: pd.DataFrame, dominant_cycles: list[dict]):
    """Bar chart: Amplitude vs Period (days) with dominant spikes highlighted."""
    # Show top 30 periods by power for readability
    top_spec = spectrum.head(30).sort_values("period")

    dom_periods = {round(c["period"], 1) for c in dominant_cycles[:3]}

    colors = []
    for _, row in top_spec.iterrows():
        if round(row["period"], 1) in dom_periods:
            colors.append(_COLORS["warning"])
        else:
            colors.append(_COLORS["cycle_line"])

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=top_spec["period"],
        y=top_spec["amplitude"],
        marker_color=colors,
        opacity=0.85,
        hovertemplate="Period: %{x:.0f} days<br>Amplitude: %{y:.4f}<extra></extra>",
    ))

    # Annotate dominant cycles
    for i, c in enumerate(dominant_cycles[:3]):
        fig.add_annotation(
            x=c["period"], y=c["amplitude"],
            text=f"{c['period']:.0f}d",
            showarrow=True, arrowhead=2,
            font=dict(color=_COLORS["warning"], size=12, family="JetBrains Mono"),
            arrowcolor=_COLORS["warning"],
            ax=0, ay=-30 - i * 15,
        )

    fig.update_layout(
        **_CHART_LAYOUT,
        height=380,
        title=dict(text="Power Spectrum — Dominant Cycle Frequencies",
                   font=dict(size=14)),
        xaxis_title="Period (days)",
        yaxis_title="Amplitude",
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# CHART B: CANDLESTICK + IFFT OVERLAY
# ═════════════════════════════════════════════════════════════════════════════

def _render_price_with_ifft(df: pd.DataFrame, ifft_signal, dates):
    """Price line chart overlaid with IFFT zero-lag trendline."""
    has_ohlc = all(c in df.columns for c in ["open", "high", "low", "close"])

    fig = go.Figure()

    if has_ohlc:
        fig.add_trace(go.Candlestick(
            x=dates,
            open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            increasing_line_color=_COLORS["up"],
            decreasing_line_color=_COLORS["down"],
            name="Price",
        ))
    else:
        fig.add_trace(go.Scatter(
            x=dates, y=df["close"],
            mode="lines",
            line=dict(color=_COLORS["neutral"], width=1.5),
            name="Close Price",
        ))

    # IFFT zero-lag trendline
    fig.add_trace(go.Scatter(
        x=dates, y=ifft_signal,
        mode="lines",
        line=dict(color=_COLORS["ifft_line"], width=2.5, dash="solid"),
        name="IFFT Trendline (zero-lag)",
    ))

    fig.update_layout(
        **_CHART_LAYOUT,
        height=480,
        title=dict(text="Price with FFT Cycle Trendline Overlay",
                   font=dict(size=14)),
        xaxis_rangeslider_visible=False,
        hovermode="x unified",
    )
    fig.update_yaxes(title_text="Price")
    st.plotly_chart(fig, use_container_width=True)


# ═════════════════════════════════════════════════════════════════════════════
# DATA LOADERS (reuse pattern from microstructure page)
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_data(ttl=300)
def _get_master_symbols(_con) -> list[str]:
    rows = _con.execute(
        "SELECT symbol FROM symbols WHERE is_active=1 ORDER BY symbol"
    ).fetchall()
    return [r[0] for r in rows]


def _get_intraday_dates(con) -> list[str]:
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


def _load_eod_ohlcv(con, symbol: str, limit: int = 500) -> pd.DataFrame:
    """Load EOD daily OHLCV for a symbol."""
    df = pd.read_sql_query(
        "SELECT date AS datetime, open, high, low, close, volume FROM eod_ohlcv "
        "WHERE symbol=? AND volume > 0 ORDER BY date DESC LIMIT ?",
        con, params=(symbol, limit),
    )
    if not df.empty:
        df = df.iloc[::-1].reset_index(drop=True)
        df["datetime"] = pd.to_datetime(df["datetime"])
    return df


def _load_intraday_bars(con, symbol: str, date_str: str) -> pd.DataFrame:
    """Load intraday bars for a symbol on a date."""
    df = pd.read_sql_query(
        "SELECT ts AS datetime, close, volume "
        "FROM intraday_bars WHERE symbol=? AND ts BETWEEN ? AND ? "
        "ORDER BY ts_epoch",
        con,
        params=(symbol, f"{date_str} 00:00:00", f"{date_str} 23:59:59"),
    )
    if not df.empty:
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df[df["volume"] > 0].reset_index(drop=True)
    return df


# ═════════════════════════════════════════════════════════════════════════════
# DOMINANT CYCLES TABLE
# ═════════════════════════════════════════════════════════════════════════════

def _render_cycles_table(dominant_cycles: list[dict]):
    """Show dominant cycles as a styled metrics row."""
    if not dominant_cycles:
        return
    cols = st.columns(min(len(dominant_cycles), 5))
    for i, c in enumerate(dominant_cycles[:5]):
        with cols[i]:
            st.metric(
                f"Cycle #{i+1}",
                f"{c['period']:.0f} days",
                f"Amp: {c['amplitude']:.4f}",
            )


# ═════════════════════════════════════════════════════════════════════════════
# MAIN RENDER
# ═════════════════════════════════════════════════════════════════════════════

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


def render_macro_cycles():
    """Main entry point for the Macro Cycles / FFT page."""
    st.markdown("## Macro Cycles — FFT Spectral Analysis")
    st.caption("Power Spectrum · Zero-Lag Trendline (IFFT) · Cycle-Based Commentary")

    with st.expander("How to Read This Analysis (Execution Playbook)", expanded=False):
        st.markdown(_PLAYBOOK_MD)

    con = get_connection()

    # ── Top bar: Data source, date/lookback, symbol ────────────────────────
    col_src, col_date, col_sym = st.columns([1, 1, 1])

    with col_src:
        data_source = st.selectbox(
            "Data Source",
            ["EOD Daily Bars", "Intraday Bars"],
            index=0,
            key="fft_src",
        )

    all_symbols = _get_master_symbols(con)
    sel_symbol = None
    sel_date = None

    if data_source == "EOD Daily Bars":
        with col_date:
            eod_days = st.slider("Lookback (trading days)", 60, 1000, 500,
                                 step=10, key="fft_lookback")
        with col_sym:
            sel_symbol = st.selectbox(
                "Symbol", all_symbols,
                help=f"{len(all_symbols)} active symbols",
                key="fft_sym_eod",
            )
    else:  # Intraday
        avail_dates = _get_intraday_dates(con)
        with col_date:
            if avail_dates:
                sel_date = st.selectbox("Trading Date", avail_dates, key="fft_date")
            else:
                st.warning("No intraday data in DB.")
        with col_sym:
            sel_symbol = st.selectbox(
                "Symbol", all_symbols,
                help=f"{len(all_symbols)} active symbols",
                key="fft_sym_intra",
            )

    # ── Sidebar: FFT parameters ───────────────────────────────────────────
    with st.sidebar:
        st.markdown("### FFT Parameters")
        top_n = st.slider("Dominant Cycles (top-N)", 3, 10, 5, key="fft_topn")
        low_pass = st.slider("IFFT Trendline Components", 2, 15, 5, key="fft_lp",
                             help="Number of strongest frequencies kept for zero-lag reconstruction")

    # ── Load data ──────────────────────────────────────────────────────────
    price_df = None

    if data_source == "EOD Daily Bars" and sel_symbol:
        price_df = _load_eod_ohlcv(con, sel_symbol, limit=eod_days)
        if price_df.empty:
            st.warning(f"No EOD data for **{sel_symbol}**.")
            price_df = None
        else:
            st.success(
                f"**{sel_symbol}** — {len(price_df)} daily bars · "
                f"Range: {price_df['datetime'].iloc[0].date()} → "
                f"{price_df['datetime'].iloc[-1].date()}",
                icon="📊",
            )

    elif data_source == "Intraday Bars" and sel_symbol and sel_date:
        price_df = _load_intraday_bars(con, sel_symbol, sel_date)
        if price_df.empty:
            st.warning(f"No intraday data for **{sel_symbol}** on {sel_date}.")
            price_df = None
        else:
            st.success(
                f"**{sel_symbol}** — {len(price_df):,} bars on {sel_date}",
                icon="📡",
            )

    if price_df is None or len(price_df) < 20:
        if price_df is not None and len(price_df) < 20:
            st.warning("Need at least 20 data points for FFT analysis.")
        st.stop()
        return

    # ── Compute FFT ────────────────────────────────────────────────────────
    result = compute_fft_cycles(price_df, top_n=top_n, low_pass_periods=low_pass)

    # ── SECTION 1: Dominant Cycles KPI ─────────────────────────────────────
    st.markdown("### Dominant Cycles")
    _render_cycles_table(result.dominant_cycles)

    # ── SECTION 2: Power Spectrum ──────────────────────────────────────────
    st.markdown("### Power Spectrum")
    _render_power_spectrum(result.spectrum, result.dominant_cycles)

    # ── SECTION 3: Price + IFFT Trendline ──────────────────────────────────
    st.markdown("### Price with Zero-Lag Trendline")
    _render_price_with_ifft(price_df, result.ifft_signal, result.dates)

    # ── SECTION 4: Cycle Phase Analysis ────────────────────────────────────
    if result.dominant_cycles:
        cycle_days = result.dominant_cycles[0]["period"]
        current_price = float(price_df["close"].iloc[-1])
        ifft_price = float(result.ifft_signal[-1])
        diff_pct = ((current_price - ifft_price) / ifft_price) * 100 if ifft_price else 0

        # Phase KPI cards
        st.markdown("### Cycle Phase")
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Current Price", f"{current_price:,.2f}")
        with c2:
            st.metric("IFFT Trendline", f"{ifft_price:,.2f}")
        with c3:
            phase = "Expansion" if current_price > ifft_price else "Contraction"
            st.metric(
                "Deviation from Trend",
                f"{diff_pct:+.2f}%",
                delta=phase,
                delta_color="normal" if diff_pct > 0 else "inverse",
            )

    # ── SECTION 5: Quant Analyst Commentary ────────────────────────────────
    symbol_label = sel_symbol or "N/A"

    if result.dominant_cycles:
        with st.expander("Quant Analyst Commentary", expanded=True):
            use_ai = st.toggle("Enable Deep LLM Analysis (OpenAI)", value=False,
                               key="fft_ai_toggle")

            if not use_ai:
                commentary = get_fft_rules_commentary(
                    cycle_days, current_price, ifft_price,
                )
                st.markdown(commentary)
            else:
                with st.spinner("Generating cycle analysis..."):
                    ai_text = get_fft_ai_commentary(
                        cycle_days, current_price, ifft_price, symbol_label,
                    )
                    if ai_text is None:
                        st.warning(
                            "OpenAI API key not found. Set `OPENAI_API_KEY` in your "
                            "`.env` file to enable LLM analysis."
                        )
                        commentary = get_fft_rules_commentary(
                            cycle_days, current_price, ifft_price,
                        )
                        st.markdown(commentary)
                    else:
                        from pakfindata.ui.components.commentary_renderer import render_styled_commentary
                        render_styled_commentary(ai_text, "Macro Cycle Analysis")

    render_footer()
