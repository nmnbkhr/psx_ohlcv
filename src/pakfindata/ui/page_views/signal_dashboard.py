"""Signal Analysis — Unified 3-Layer Top-Down Microstructure Dashboard.

Combines:
  Layer 1: Macro Regime (daily OHLCV) - Hurst, Volatility, SMA, Circuit Breakers
  Layer 2: Intraday Anchor (intraday bars) - VWAP+bands, Volume Profile POC, ER
  Layer 3: Execution DNA (tick logs) - CVD, OFI, Block Trades, VPIN
  -> Composite Signal Score (1-100)
"""

from __future__ import annotations

import datetime
from typing import Optional

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from pakfindata.engine.macro_regime import compute_macro_regime, MacroRegime
from pakfindata.engine.signal_score import (
    IntradayAnchor,
    ExecutionDNA,
    SignalReport,
    compute_intraday_anchor,
    compute_execution_dna,
    compute_signal_score,
    interpret_score,
    score_color,
    batch_score_symbols,
    batch_results_to_dataframe,
    BatchScanResult,
)
from pakfindata.ui.components.helpers import get_connection, render_footer

# Commentary engine — import _llm_call for AI analysis
try:
    from pakfindata.engine.commentary import _llm_call
    _HAS_LLM = True
except ImportError:
    _HAS_LLM = False


# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM (matches existing Bloomberg dark theme)
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "accent": "#C8A96E",
    "up": "#00E676",
    "down": "#FF5252",
    "neutral": "#78909C",
    "bg": "#0B0E11",
    "card_bg": "#12151A",
    "card_border": "#2A2D35",
    "grid": "#1A1D23",
    "text": "#e0e0e0",
    "text_dim": "#8B8D93",
    "vwap_line": "#C8A96E",
    "band_1": "rgba(200,169,110,0.3)",
    "band_2": "rgba(200,169,110,0.15)",
    "poc_line": "#FFD600",
    "va_fill": "rgba(255,214,0,0.1)",
    "buy": "#00E676",
    "sell": "#FF5252",
    "block": "#FF9800",
}

_CHART_LAYOUT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font=dict(color=_COLORS["text"], size=11, family="JetBrains Mono, monospace"),
    xaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    yaxis=dict(gridcolor=_COLORS["grid"], zeroline=False),
    margin=dict(l=10, r=10, t=40, b=10),
)

_PAGE_CSS = """
<style>
.signal-score-box {
    text-align: center;
    padding: 24px 16px;
    border-radius: 12px;
    background: linear-gradient(135deg, #12151A 0%, #1A1D23 100%);
    border: 1px solid #2A2D35;
    margin-bottom: 16px;
}
.signal-score-value {
    font-size: 4rem;
    font-weight: bold;
    line-height: 1;
}
.signal-score-label {
    font-size: 1rem;
    color: #8B8D93;
    margin-top: 4px;
}
.signal-breakdown {
    font-size: 0.9rem;
    color: #8B8D93;
    margin-top: 8px;
}
.metric-card {
    background: linear-gradient(135deg, #12151A 0%, #1A1D23 100%);
    border: 1px solid #2A2D35;
    border-radius: 8px;
    padding: 14px;
    text-align: center;
}
.metric-value {
    font-size: 1.6rem;
    font-weight: bold;
    color: #C8A96E;
}
.metric-label {
    font-size: 0.8rem;
    color: #8B8D93;
    margin-top: 2px;
}
.metric-sub {
    font-size: 0.75rem;
    color: #5A5D63;
    margin-top: 2px;
}
.layer-header {
    font-size: 1.1rem;
    font-weight: bold;
    color: #C8A96E;
    border-bottom: 1px solid #2A2D35;
    padding-bottom: 8px;
    margin-top: 24px;
    margin-bottom: 12px;
    letter-spacing: 1px;
}
</style>
"""


def _styled_fig(height: int = 400, **kw) -> go.Figure:
    return go.Figure(layout={**_CHART_LAYOUT, "height": height, **kw})


def _metric_card(label: str, value: str, sub: str = "") -> str:
    sub_html = f'<div class="metric-sub">{sub}</div>' if sub else ""
    return (
        f'<div class="metric-card">'
        f'<div class="metric-value">{value}</div>'
        f'<div class="metric-label">{label}</div>'
        f"{sub_html}</div>"
    )


# ═════════════════════════════════════════════════════════════════════════════
# DATA LOADING
# ═════════════════════════════════════════════════════════════════════════════


def _get_symbols(con) -> list[str]:
    """Get active symbols from symbols table."""
    try:
        rows = con.execute(
            "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
        ).fetchall()
        return [r["symbol"] for r in rows]
    except Exception:
        return []


def _get_symbol_info(con, symbol: str) -> dict:
    """Get sector info for a symbol."""
    try:
        row = con.execute(
            "SELECT sector, sector_name FROM symbols WHERE symbol = ?", (symbol,)
        ).fetchone()
        if row:
            return {"sector": row["sector"], "sector_name": row["sector_name"]}
    except Exception:
        pass
    return {"sector": None, "sector_name": None}


def _get_eod_data(con, symbol: str, limit: int = 600) -> pd.DataFrame:
    """Query EOD OHLCV from eod_ohlcv table."""
    try:
        return pd.read_sql_query(
            """SELECT date, open, high, low, close, volume
               FROM eod_ohlcv
               WHERE symbol = ?
               ORDER BY date DESC LIMIT ?""",
            con,
            params=(symbol, limit),
        ).sort_values("date").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _get_index_data(con, limit: int = 600) -> pd.DataFrame:
    """Query KSE-100 daily data for relative strength."""
    # Try common index symbols
    for idx_sym in ("KSE100", "KSE-100", "KSEALL"):
        try:
            df = pd.read_sql_query(
                """SELECT date, close FROM eod_ohlcv
                   WHERE symbol = ? ORDER BY date DESC LIMIT ?""",
                con,
                params=(idx_sym, limit),
            )
            if not df.empty:
                return df.sort_values("date").reset_index(drop=True)
        except Exception:
            continue
    return pd.DataFrame()


def _get_intraday_data(con, symbol: str, limit: int = 5000) -> pd.DataFrame:
    """Query intraday bars from intraday_bars table."""
    try:
        return pd.read_sql_query(
            """SELECT ts, ts_epoch, open, high, low, close, volume
               FROM intraday_bars
               WHERE symbol = ?
               ORDER BY ts_epoch DESC LIMIT ?""",
            con,
            params=(symbol, limit),
        ).sort_values("ts_epoch").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def _get_tick_data(
    con, symbol: str, market: str = "REG", days: int = 3
) -> pd.DataFrame:
    """Query tick logs from tick_logs table."""
    try:
        cutoff = (
            datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5)))
            - datetime.timedelta(days=days)
        )
        cutoff_ts = cutoff.timestamp()

        return pd.read_sql_query(
            """SELECT symbol, market, timestamp, _ts, price, volume,
                      change, high, low, open, prev_close,
                      bid, ask, bid_vol, ask_vol, trades
               FROM tick_logs
               WHERE symbol = ? AND market = ? AND timestamp >= ?
               ORDER BY timestamp ASC""",
            con,
            params=(symbol, market, cutoff_ts),
        )
    except Exception:
        return pd.DataFrame()


def _check_tick_table_exists(con) -> bool:
    """Check if tick_logs table exists and has data."""
    try:
        row = con.execute(
            "SELECT COUNT(*) as cnt FROM tick_logs LIMIT 1"
        ).fetchone()
        return row["cnt"] > 0 if row else False
    except Exception:
        return False


def _is_market_open() -> bool:
    """Check if PSX market is currently open."""
    now_pkt = datetime.datetime.now(
        datetime.timezone(datetime.timedelta(hours=5))
    )
    is_weekday = now_pkt.weekday() < 5
    is_hours = datetime.time(9, 15) <= now_pkt.time() <= datetime.time(15, 30)
    return is_weekday and is_hours


# ═════════════════════════════════════════════════════════════════════════════
# RENDERERS
# ═════════════════════════════════════════════════════════════════════════════


def _render_signal_score(report: SignalReport):
    """Render the big signal score gauge at the top."""
    s = report.signal_score
    color = score_color(s)
    interp = report.interpretation

    macro_s = report.macro.score if report.macro else 0
    intra_s = report.intraday.score if report.intraday else 0
    exec_s = report.execution.score if report.execution else 0

    st.markdown(
        f"""<div class="signal-score-box">
        <div class="signal-score-value" style="color:{color}">{s}</div>
        <div class="signal-score-label">{interp}</div>
        <div class="signal-breakdown">
            Macro: {macro_s}/33 &nbsp;|&nbsp;
            Intraday: {intra_s}/33 &nbsp;|&nbsp;
            Execution: {exec_s}/33
        </div>
        </div>""",
        unsafe_allow_html=True,
    )


def _render_layer1(macro: MacroRegime):
    """Render Layer 1: Macro Regime section."""
    st.markdown('<div class="layer-header">LAYER 1: MACRO REGIME</div>', unsafe_allow_html=True)

    # Metric cards row
    cols = st.columns(4)
    with cols[0]:
        regime_color = {"TRENDING": _COLORS["up"], "MEAN_REVERTING": _COLORS["down"]}.get(
            macro.regime, _COLORS["neutral"]
        )
        st.markdown(
            _metric_card(
                "Hurst Exponent",
                f"{macro.hurst_exponent:.3f}",
                f'<span style="color:{regime_color}">{macro.regime}</span>',
            ),
            unsafe_allow_html=True,
        )
    with cols[1]:
        vol_label = "HIGH" if macro.ann_volatility > 40 else ("LOW" if macro.ann_volatility < 15 else "MODERATE")
        st.markdown(
            _metric_card("Ann. Volatility", f"{macro.ann_volatility}%", vol_label),
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            _metric_card(
                f"SMA-{macro.sma_200_actual_window}",
                f"PKR {macro.sma_200:,.2f}",
                f"Window: {macro.sma_200_actual_window}d",
            ),
            unsafe_allow_html=True,
        )
    with cols[3]:
        dist_color = _COLORS["up"] if macro.sma_distance_pct > 0 else _COLORS["down"]
        sign = "+" if macro.sma_distance_pct > 0 else ""
        st.markdown(
            _metric_card(
                "Price vs SMA",
                f'{sign}{macro.sma_distance_pct}%',
                f'<span style="color:{dist_color}">{"ABOVE" if macro.sma_distance_pct > 0 else "BELOW"}</span>',
            ),
            unsafe_allow_html=True,
        )

    # Extra info row
    info_cols = st.columns(3)
    with info_cols[0]:
        if macro.sector_name:
            st.caption(f"Sector: {macro.sector_name}")
        elif macro.sector:
            st.caption(f"Sector Code: {macro.sector}")
    with info_cols[1]:
        if macro.beta_20d is not None:
            st.caption(f"Beta (20d): {macro.beta_20d} | Alpha: {macro.alpha_20d}%")
    with info_cols[2]:
        if macro.circuit_breaker_dates:
            st.warning(f"Circuit Breaker: {', '.join(macro.circuit_breaker_dates)}")
        else:
            st.caption("Circuit Breaker (5d): None")

    if macro.fake_hl_warning:
        st.warning(
            "High/Low values appear derived (max/min of Open,Close). "
            "ATR and volatility calculations may be inaccurate."
        )

    # --- 2-Year Price Chart + SMA-200 ---
    if macro.daily_df is not None and not macro.daily_df.empty:
        df = macro.daily_df

        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["close"],
            name="Close", line=dict(color=_COLORS["text"], width=1.5),
        ))

        sma_col = df["close"].rolling(macro.sma_200_actual_window).mean()
        fig.add_trace(go.Scatter(
            x=df["date"], y=sma_col,
            name=f"SMA-{macro.sma_200_actual_window}",
            line=dict(color=_COLORS["accent"], width=1.5, dash="dot"),
        ))

        fig.update_layout(
            **_CHART_LAYOUT,
            height=320,
            title=dict(text="Price & SMA", font=dict(size=13)),
            yaxis_title="PKR",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)

        # --- Rolling Hurst Chart ---
        if macro.hurst_rolling is not None and len(macro.hurst_rolling) > 10:
            fig_h = go.Figure()
            # Get dates aligned to rolling hurst index
            dates = df["date"].iloc[macro.hurst_rolling.index]

            fig_h.add_trace(go.Scatter(
                x=dates, y=macro.hurst_rolling,
                name="Rolling Hurst", line=dict(color=_COLORS["accent"], width=1.5),
            ))
            # Reference lines
            fig_h.add_hline(y=0.5, line_dash="dash", line_color=_COLORS["text_dim"],
                            annotation_text="Random Walk (0.5)")
            fig_h.add_hline(y=0.55, line_dash="dot", line_color=_COLORS["up"],
                            annotation_text="Trending (0.55)")
            fig_h.add_hline(y=0.45, line_dash="dot", line_color=_COLORS["down"],
                            annotation_text="Mean-Reverting (0.45)")

            fig_h.update_layout(
                **_CHART_LAYOUT,
                height=250,
                title=dict(text="Rolling Hurst Exponent (200-bar window)", font=dict(size=13)),
                showlegend=False,
            )
            fig_h.update_yaxes(range=[0.2, 0.9], gridcolor=_COLORS["grid"])
            st.plotly_chart(fig_h, use_container_width=True)


def _render_layer2(intraday: IntradayAnchor):
    """Render Layer 2: Intraday Anchor section."""
    st.markdown('<div class="layer-header">LAYER 2: INTRADAY ANCHOR</div>', unsafe_allow_html=True)

    if intraday.data_source == "none":
        st.info("No intraday data available. Score defaults to 16/33.")
        return

    # Metric summary
    cols = st.columns(4)
    with cols[0]:
        vwap_d = f"{intraday.vwap_distance_std:.2f}σ" if intraday.vwap_distance_std is not None else "N/A"
        st.markdown(_metric_card("VWAP Distance", vwap_d), unsafe_allow_html=True)
    with cols[1]:
        poc_str = f"PKR {intraday.poc_price:,.2f}" if intraday.poc_price else "N/A"
        st.markdown(_metric_card("POC", poc_str), unsafe_allow_html=True)
    with cols[2]:
        va_str = (
            f"PKR {intraday.va_low:,.0f} - {intraday.va_high:,.0f}"
            if intraday.va_low and intraday.va_high
            else "N/A"
        )
        st.markdown(_metric_card("Value Area", va_str), unsafe_allow_html=True)
    with cols[3]:
        er_label = "SPIKE" if intraday.er_spike_active else "Clean"
        er_color = _COLORS["down"] if intraday.er_spike_active else _COLORS["up"]
        st.markdown(
            _metric_card("ER Status", f'<span style="color:{er_color}">{er_label}</span>'),
            unsafe_allow_html=True,
        )

    # --- VWAP + Bands chart ---
    if intraday.vwap_df is not None and not intraday.vwap_df.empty:
        vdf = intraday.vwap_df
        dt_col = "datetime" if "datetime" in vdf.columns else "ts"

        fig = go.Figure()

        # Price line
        fig.add_trace(go.Scatter(
            x=vdf[dt_col], y=vdf["close"],
            name="Price", line=dict(color=_COLORS["text"], width=1.2),
        ))

        # VWAP line
        if "vwap" in vdf.columns:
            fig.add_trace(go.Scatter(
                x=vdf[dt_col], y=vdf["vwap"],
                name="VWAP", line=dict(color=_COLORS["vwap_line"], width=2),
            ))

            # +/-1 sigma bands
            if "vwap_upper_1" in vdf.columns:
                fig.add_trace(go.Scatter(
                    x=vdf[dt_col], y=vdf["vwap_upper_1"],
                    name="+1σ", line=dict(color=_COLORS["accent"], width=0.8, dash="dot"),
                    showlegend=False,
                ))
                fig.add_trace(go.Scatter(
                    x=vdf[dt_col], y=vdf["vwap_lower_1"],
                    name="-1σ", line=dict(color=_COLORS["accent"], width=0.8, dash="dot"),
                    fill="tonexty", fillcolor=_COLORS["band_1"],
                    showlegend=False,
                ))

            # +/-2 sigma bands
            if "vwap_upper_2" in vdf.columns:
                fig.add_trace(go.Scatter(
                    x=vdf[dt_col], y=vdf["vwap_upper_2"],
                    name="+2σ", line=dict(color=_COLORS["accent"], width=0.5, dash="dash"),
                    showlegend=False,
                ))
                fig.add_trace(go.Scatter(
                    x=vdf[dt_col], y=vdf["vwap_lower_2"],
                    name="-2σ", line=dict(color=_COLORS["accent"], width=0.5, dash="dash"),
                    fill="tonexty", fillcolor=_COLORS["band_2"],
                    showlegend=False,
                ))

        # POC + Value Area
        if intraday.poc_price:
            fig.add_hline(
                y=intraday.poc_price, line_dash="dash",
                line_color=_COLORS["poc_line"],
                annotation_text=f"POC: {intraday.poc_price:,.2f}",
            )
        if intraday.va_low and intraday.va_high:
            fig.add_hrect(
                y0=intraday.va_low, y1=intraday.va_high,
                fillcolor=_COLORS["va_fill"], line_width=0,
                annotation_text="Value Area",
            )

        fig.update_layout(
            **_CHART_LAYOUT,
            height=380,
            title=dict(text="VWAP + Bands + Volume Profile", font=dict(size=13)),
            yaxis_title="PKR",
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        st.plotly_chart(fig, use_container_width=True)

    # --- Volume Profile Histogram ---
    if intraday.profile_data and intraday.profile_data.get("levels"):
        prof = intraday.profile_data
        fig_vp = go.Figure()
        fig_vp.add_trace(go.Bar(
            x=prof["profile"], y=prof["levels"],
            orientation="h",
            name="Volume at Price",
            marker_color=_COLORS["accent"],
            opacity=0.7,
        ))

        if intraday.poc_price:
            fig_vp.add_hline(
                y=intraday.poc_price, line_dash="solid",
                line_color=_COLORS["poc_line"], line_width=2,
                annotation_text=f"POC: {intraday.poc_price:,.2f}",
            )

        fig_vp.update_layout(
            **_CHART_LAYOUT,
            height=300,
            title=dict(text="Volume Profile", font=dict(size=13)),
            xaxis_title="Volume",
            yaxis_title="Price (PKR)",
        )
        st.plotly_chart(fig_vp, use_container_width=True)


def _render_layer3(execution: ExecutionDNA):
    """Render Layer 3: Execution DNA section."""
    st.markdown('<div class="layer-header">LAYER 3: EXECUTION DNA</div>', unsafe_allow_html=True)

    if not execution.has_tick_data:
        st.info(
            "Layer 3 requires tick data. Sync tick logs from the Tick Analytics page, "
            "or start the tick collector during market hours."
        )
        return

    # Metric cards
    cols = st.columns(5)
    with cols[0]:
        st.markdown(
            _metric_card("Ticks", f"{execution.tick_count:,}", f"{execution.days_available}d"),
            unsafe_allow_html=True,
        )
    with cols[1]:
        buy_color = _COLORS["up"] if execution.buy_pct > 50 else _COLORS["down"]
        st.markdown(
            _metric_card(
                "Buy / Sell",
                f'<span style="color:{buy_color}">{execution.buy_pct}%</span> / {execution.sell_pct}%',
            ),
            unsafe_allow_html=True,
        )
    with cols[2]:
        slope_color = _COLORS["up"] if execution.cvd_slope > 0 else _COLORS["down"]
        st.markdown(
            _metric_card(
                "CVD Slope",
                f'<span style="color:{slope_color}">{execution.cvd_slope:+.4f}</span>',
            ),
            unsafe_allow_html=True,
        )
    with cols[3]:
        ofi_color = _COLORS["up"] if execution.recent_ofi > 0 else _COLORS["down"]
        st.markdown(
            _metric_card(
                "Recent OFI (15m)",
                f'<span style="color:{ofi_color}">{execution.recent_ofi:+.3f}</span>',
            ),
            unsafe_allow_html=True,
        )
    with cols[4]:
        bias_label = {1: "BUY", -1: "SELL", 0: "NEUTRAL"}.get(execution.block_bias, "N/A")
        bias_color = {1: _COLORS["up"], -1: _COLORS["down"]}.get(
            execution.block_bias, _COLORS["neutral"]
        )
        st.markdown(
            _metric_card(
                "Blocks",
                f"{execution.block_count}",
                f'<span style="color:{bias_color}">Bias: {bias_label}</span>',
            ),
            unsafe_allow_html=True,
        )

    # Extra metrics row
    if execution.vpin_value is not None:
        tox_color = {"LOW": _COLORS["up"], "MODERATE": "#EF9F27", "TOXIC": _COLORS["down"]}.get(
            execution.vpin_toxicity or "", _COLORS["neutral"]
        )
        st.markdown(
            f'VPIN: **{execution.vpin_value:.3f}** — '
            f'<span style="color:{tox_color}">{execution.vpin_toxicity}</span>',
            unsafe_allow_html=True,
        )

    if execution.cross_market_divergence:
        st.warning(
            f"Cross-Market Divergence detected: "
            f"REG CVD = {execution.reg_cvd:,.0f}, FUT CVD = {execution.fut_cvd:,.0f}"
        )

    # --- CVD Chart ---
    if execution.cvd_df is not None and not execution.cvd_df.empty:
        cvd = execution.cvd_df
        dt_col = "_ts" if "_ts" in cvd.columns else "timestamp"

        fig = make_subplots(
            rows=2, cols=1, shared_xaxes=True,
            row_heights=[0.6, 0.4], vertical_spacing=0.08,
        )

        # Price line
        fig.add_trace(go.Scatter(
            x=cvd[dt_col], y=cvd["price"],
            name="Price", line=dict(color=_COLORS["text"], width=1),
        ), row=1, col=1)

        # CVD
        cvd_color = _COLORS["up"] if execution.cvd_slope > 0 else _COLORS["down"]
        fig.add_trace(go.Scatter(
            x=cvd[dt_col], y=cvd["cvd"],
            name="CVD", line=dict(color=cvd_color, width=1.5),
            fill="tozeroy",
            fillcolor=f"rgba({','.join(str(int(cvd_color[i:i+2], 16)) for i in (1,3,5))},0.15)"
            if cvd_color.startswith("#") and len(cvd_color) == 7 else "rgba(0,230,118,0.15)",
        ), row=2, col=1)

        # Block trade markers
        if execution.block_trades is not None and not execution.block_trades.empty:
            blocks = execution.block_trades
            fig.add_trace(go.Scatter(
                x=blocks[dt_col] if dt_col in blocks.columns else blocks.index,
                y=blocks["price"],
                mode="markers",
                name="Block Trades",
                marker=dict(
                    color=_COLORS["block"], size=10, symbol="diamond",
                    line=dict(width=1, color=_COLORS["text"]),
                ),
            ), row=1, col=1)

        fig.update_layout(
            **_CHART_LAYOUT,
            height=400,
            title=dict(text="CVD & Block Trades", font=dict(size=13)),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
        )
        fig.update_yaxes(title_text="Price (PKR)", row=1, col=1, gridcolor=_COLORS["grid"])
        fig.update_yaxes(title_text="CVD", row=2, col=1, gridcolor=_COLORS["grid"])
        st.plotly_chart(fig, use_container_width=True)

    # --- OFI Heatmap ---
    if execution.ofi_df is not None and not execution.ofi_df.empty:
        ofi = execution.ofi_df.copy()
        ofi["time"] = ofi.index
        if not isinstance(ofi["time"].iloc[0], str):
            ofi["time"] = pd.to_datetime(ofi["time"]).dt.strftime("%H:%M")

        fig_ofi = go.Figure()
        fig_ofi.add_trace(go.Bar(
            x=ofi["time"], y=ofi["ofi"],
            marker_color=[
                _COLORS["up"] if v > 0 else _COLORS["down"] for v in ofi["ofi"]
            ],
            name="OFI",
        ))

        fig_ofi.update_layout(
            **_CHART_LAYOUT,
            height=250,
            title=dict(text="Order Flow Imbalance (per minute)", font=dict(size=13)),
            showlegend=False,
        )
        fig_ofi.update_yaxes(range=[-1.1, 1.1], gridcolor=_COLORS["grid"])
        fig_ofi.update_xaxes(gridcolor=_COLORS["grid"])
        st.plotly_chart(fig_ofi, use_container_width=True)

    # --- Session Segmentation ---
    if execution.session_ofi:
        st.caption("Session OFI Breakdown:")
        seg_cols = st.columns(4)
        labels = {"pre_open": "Pre-Open", "morning": "Morning", "afternoon": "Afternoon", "close": "Close"}
        for i, (key, label) in enumerate(labels.items()):
            val = execution.session_ofi.get(key, 0.0)
            color = _COLORS["up"] if val > 0 else (_COLORS["down"] if val < 0 else _COLORS["neutral"])
            with seg_cols[i]:
                st.markdown(
                    _metric_card(label, f'<span style="color:{color}">{val:+.3f}</span>'),
                    unsafe_allow_html=True,
                )


# ═════════════════════════════════════════════════════════════════════════════
# COMMENTARY (Single Symbol)
# ═════════════════════════════════════════════════════════════════════════════


def _signal_rules_commentary(report: SignalReport) -> str:
    """Instant rules-based commentary for a single symbol signal analysis."""
    s = report.signal_score
    macro = report.macro
    intraday = report.intraday
    execution = report.execution

    lines: list[str] = []

    # Overall verdict
    if s >= 71:
        lines.append(
            f"**Strong Signal** — Composite score of {s}/100 indicates high confluence "
            f"across macro, intraday, and execution layers. "
            f"Interpretation: {report.interpretation}."
        )
    elif s >= 51:
        lines.append(
            f"**Moderate Signal** — Composite score of {s}/100 suggests a developing setup. "
            f"Interpretation: {report.interpretation}. "
            f"Not all layers are aligned — exercise selectivity."
        )
    elif s >= 31:
        lines.append(
            f"**Neutral** — Score of {s}/100. {report.interpretation}. "
            f"No clear directional edge at this time."
        )
    else:
        lines.append(
            f"**Weak Setup** — Score of {s}/100 ({report.interpretation}). "
            f"Insufficient confluence for directional conviction."
        )

    # Layer 1 - Macro
    if macro:
        regime_desc = {
            "TRENDING": "a trending regime (Hurst > 0.55) — momentum strategies favored",
            "MEAN_REVERTING": "a mean-reverting regime (Hurst < 0.45) — contrarian strategies favored",
            "RANDOM_WALK": "a random walk regime — no statistical edge from trend or reversion",
        }.get(macro.regime, "an unclassified regime")
        lines.append(
            f"**Macro Regime ({macro.score}/33):** Hurst = {macro.hurst_exponent:.3f}, indicating "
            f"{regime_desc}. Price is {macro.sma_distance_pct:+.1f}% from SMA-{macro.sma_200_actual_window}. "
            f"Volatility: {macro.ann_volatility}% annualized."
        )
        if macro.circuit_breaker_dates:
            lines.append(
                f"⚠ Circuit breakers triggered on {', '.join(macro.circuit_breaker_dates)} — "
                f"elevated tail risk."
            )
        mom = getattr(macro, "momentum_signal", None)
        if mom and mom not in ("N/A", "NEUTRAL"):
            lines.append(f"Momentum: **{mom}** (20/60 SMA crossover).")

    # Layer 2 - Intraday
    if intraday and intraday.data_source != "none":
        vwap_d = intraday.vwap_distance_std
        if vwap_d is not None:
            if abs(vwap_d) > 2:
                lines.append(
                    f"**Intraday ({intraday.score}/33):** Price is {vwap_d:.2f}σ from VWAP — "
                    f"{'overextended above' if vwap_d > 0 else 'deeply below'}, "
                    f"mean-reversion likely."
                )
            else:
                lines.append(
                    f"**Intraday ({intraday.score}/33):** Price is {vwap_d:.2f}σ from VWAP — "
                    f"within normal range."
                )
        if intraday.er_spike_active:
            lines.append("⚠ Efficiency Ratio spike detected — large move on thin volume.")

    # Layer 3 - Execution
    if execution and execution.has_tick_data:
        flow = "buying pressure" if execution.buy_pct > 55 else (
            "selling pressure" if execution.sell_pct > 55 else "balanced flow"
        )
        lines.append(
            f"**Execution ({execution.score}/33):** {flow} "
            f"(Buy {execution.buy_pct:.0f}% / Sell {execution.sell_pct:.0f}%). "
            f"CVD slope: {execution.cvd_slope:+.4f}. OFI: {execution.recent_ofi:+.3f}."
        )
        if execution.vpin_value is not None:
            lines.append(
                f"VPIN: {execution.vpin_value:.3f} ({execution.vpin_toxicity}). "
                f"{'High toxicity — informed traders active.' if execution.vpin_toxicity == 'TOXIC' else ''}"
            )
        if execution.block_count > 0:
            bias = {1: "buy-biased", -1: "sell-biased", 0: "neutral"}.get(execution.block_bias, "")
            lines.append(f"Block trades: {execution.block_count} detected ({bias}).")

    return "\n\n".join(lines)


def _signal_ai_commentary(report: SignalReport, symbol: str) -> Optional[str]:
    """LLM commentary for single symbol signal analysis."""
    if not _HAS_LLM:
        return None

    macro = report.macro
    intraday = report.intraday
    execution = report.execution

    system = (
        "You are a senior market strategist at a Pakistani brokerage. "
        "Write a 4-6 sentence tactical analysis of this stock's signal score. "
        "Structure: (1) Overall verdict — is this a buy/hold/avoid and why, "
        "(2) Which layer is strongest/weakest and what it means, "
        "(3) Key risk or opportunity the data reveals, "
        "(4) Specific actionable recommendation. "
        "Be direct, reference specific numbers. "
        "PSX context: Pakistan Stock Exchange, PKR currency, ±7.5% circuit limits."
    )

    # Build context
    parts = [
        f"Signal Analysis for **{symbol}** on PSX:",
        f"- Composite Score: {report.signal_score}/100 ({report.interpretation})",
    ]

    if macro:
        parts.append(
            f"- Layer 1 Macro ({macro.score}/33): Hurst={macro.hurst_exponent:.3f} "
            f"({macro.regime}), Vol={macro.ann_volatility}%, "
            f"SMA dist={macro.sma_distance_pct:+.1f}%"
        )
        mom = getattr(macro, "momentum_signal", "N/A")
        if mom != "N/A":
            parts.append(f"  Momentum: {mom}")
        if macro.circuit_breaker_dates:
            parts.append(f"  Circuit breakers: {', '.join(macro.circuit_breaker_dates)}")

    if intraday and intraday.data_source != "none":
        vd = intraday.vwap_distance_std
        parts.append(
            f"- Layer 2 Intraday ({intraday.score}/33): VWAP dist={vd:.2f}σ, "
            f"POC={intraday.poc_price}, ER spike={'YES' if intraday.er_spike_active else 'No'}"
        )

    if execution and execution.has_tick_data:
        parts.append(
            f"- Layer 3 Execution ({execution.score}/33): "
            f"Buy/Sell={execution.buy_pct:.0f}/{execution.sell_pct:.0f}%, "
            f"CVD slope={execution.cvd_slope:+.4f}, OFI={execution.recent_ofi:+.3f}"
        )
        if execution.vpin_value is not None:
            parts.append(f"  VPIN={execution.vpin_value:.3f} ({execution.vpin_toxicity})")
        if execution.block_count > 0:
            parts.append(f"  Blocks={execution.block_count}")
    else:
        parts.append("- Layer 3 Execution: No tick data available")

    parts.append("Provide your tactical analysis.")
    user = "\n".join(parts)
    return _llm_call(system, user, max_tokens=500)


def _render_signal_commentary(report: SignalReport, symbol: str):
    """Render the AI/rules-based commentary expander for single symbol."""
    with st.expander("Quant Analyst Commentary", expanded=True):
        use_ai = st.toggle("Enable Deep LLM Analysis (OpenAI)", value=False, key="signal_ai_toggle")

        if not use_ai:
            commentary = _signal_rules_commentary(report)
            st.markdown(commentary)
        else:
            with st.spinner("Generating institutional analysis..."):
                ai_text = _signal_ai_commentary(report, symbol)
                if ai_text is None:
                    st.warning(
                        "OpenAI API key not found. Set `OPENAI_API_KEY` in your "
                        "`.env` file to enable LLM analysis."
                    )
                    commentary = _signal_rules_commentary(report)
                    st.markdown(commentary)
                elif ai_text.startswith("LLM Error"):
                    st.warning(f"LLM call failed: {ai_text}")
                    commentary = _signal_rules_commentary(report)
                    st.markdown(commentary)
                else:
                    from pakfindata.ui.components.commentary_renderer import render_styled_commentary
                    render_styled_commentary(ai_text, "Signal Analysis")


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PAGE
# ═════════════════════════════════════════════════════════════════════════════


_ALL_MARKET = "── ALL (Batch Scanner) ──"


def _get_sectors(con) -> list[str]:
    """Get distinct sector names."""
    try:
        rows = con.execute(
            "SELECT DISTINCT sector_name FROM symbols WHERE is_active = 1 AND sector_name IS NOT NULL ORDER BY sector_name"
        ).fetchall()
        return [r["sector_name"] for r in rows if r["sector_name"]]
    except Exception:
        return []


def _run_batch_scan(
    con, symbols: list[str], min_volume: int, sector_filter: str
) -> list[BatchScanResult]:
    """Run batch scanner with progress bar."""
    progress = st.progress(0, text="Scanning market...")

    def update_progress(current: int, total: int):
        if total > 0:
            progress.progress(current / total, text=f"Scoring {current}/{total}...")

    results = batch_score_symbols(
        con,
        symbols,
        progress_callback=update_progress,
        min_volume=min_volume,
        sector_filter=sector_filter,
    )
    progress.empty()
    return results


def _render_batch_scanner(results: list[BatchScanResult], top_n: int):
    """Render the full batch scanner UI: summary, charts, table, download."""
    if not results:
        st.warning("No symbols with sufficient data.")
        return

    df = batch_results_to_dataframe(results)

    # ── Summary cards ──
    st.markdown(
        '<div class="layer-header">BATCH SIGNAL SCANNER</div>',
        unsafe_allow_html=True,
    )

    trending = sum(1 for r in results if r.regime == "TRENDING")
    mean_rev = sum(1 for r in results if r.regime == "MEAN_REVERTING")
    above_sma = sum(1 for r in results if r.sma_distance_pct > 0)
    avg_score = np.mean([r.signal_score for r in results])

    cols = st.columns(5)
    with cols[0]:
        st.markdown(
            _metric_card("Scanned", str(len(results))),
            unsafe_allow_html=True,
        )
    with cols[1]:
        st.markdown(
            _metric_card("Avg Score", f"{avg_score:.0f}", interpret_score(int(avg_score))),
            unsafe_allow_html=True,
        )
    with cols[2]:
        st.markdown(
            _metric_card("Trending", str(trending), f"Mean-Rev: {mean_rev}"),
            unsafe_allow_html=True,
        )
    with cols[3]:
        st.markdown(
            _metric_card("Above SMA", str(above_sma), f"Below: {len(results) - above_sma}"),
            unsafe_allow_html=True,
        )
    with cols[4]:
        avg_vol = np.mean([r.ann_volatility for r in results])
        st.markdown(
            _metric_card("Avg Volatility", f"{avg_vol:.1f}%"),
            unsafe_allow_html=True,
        )

    # ── Market Commentary (LLM with template fallback) ──
    from pakfindata.engine.batch_commentary import build_batch_narrative, render_batch_commentary

    narrative = build_batch_narrative(results)

    with st.expander("Quant Analyst Commentary", expanded=True):
        use_ai = st.toggle("Enable Deep LLM Analysis (OpenAI)", value=False, key="batch_ai_toggle")

        if not use_ai:
            from pakfindata.engine.batch_commentary import generate_batch_summary_text
            st.markdown(generate_batch_summary_text(narrative))
        else:
            with st.spinner("Generating market analysis..."):
                main_text, raw_stats = render_batch_commentary(narrative)
                if raw_stats:
                    # LLM succeeded: main_text = AI, raw_stats = template
                    from pakfindata.ui.components.commentary_renderer import render_styled_commentary
                    render_styled_commentary(main_text, "Market Signal Scan")
                    with st.expander("Raw Stats", expanded=False):
                        st.caption(raw_stats)
                else:
                    # LLM failed: main_text = template, raw_stats = None
                    st.warning(
                        "LLM unavailable — showing rules-based summary. "
                        "Set `OPENAI_API_KEY` in `.env` to enable AI commentary."
                    )
                    st.markdown(main_text)

    # ── Charts: Sector Treemap + Regime Donut (side by side) ──
    chart_cols = st.columns(2)

    # Sector heatmap (treemap)
    with chart_cols[0]:
        sector_scores: dict[str, list[int]] = {}
        for r in results:
            key = r.sector_name or "Unknown"
            sector_scores.setdefault(key, []).append(r.signal_score)

        sec_labels = []
        sec_avgs = []
        sec_counts = []
        for sec, scores in sorted(sector_scores.items()):
            sec_labels.append(sec)
            sec_avgs.append(np.mean(scores))
            sec_counts.append(len(scores))

        fig_tree = go.Figure(
            go.Treemap(
                labels=[f"{l}<br>Avg:{a:.0f} ({c})" for l, a, c in zip(sec_labels, sec_avgs, sec_counts)],
                parents=[""] * len(sec_labels),
                values=sec_counts,
                marker=dict(
                    colors=sec_avgs,
                    colorscale=[[0, _COLORS["down"]], [0.5, _COLORS["neutral"]], [1, _COLORS["up"]]],
                    cmin=20,
                    cmax=80,
                ),
                textinfo="label",
            )
        )
        fig_tree.update_layout(
            **_CHART_LAYOUT,
            height=300,
            title=dict(text="Sector Heatmap (Avg Score)", font=dict(size=13)),
        )
        st.plotly_chart(fig_tree, use_container_width=True)

    # Regime distribution (donut)
    with chart_cols[1]:
        random_walk = sum(1 for r in results if r.regime == "RANDOM_WALK")
        other = len(results) - trending - mean_rev - random_walk

        labels_d = ["TRENDING", "MEAN_REVERTING", "RANDOM_WALK"]
        values_d = [trending, mean_rev, random_walk]
        if other > 0:
            labels_d.append("OTHER")
            values_d.append(other)
        colors_d = [_COLORS["up"], _COLORS["down"], _COLORS["neutral"], _COLORS["text_dim"]]

        fig_donut = go.Figure(
            go.Pie(
                labels=labels_d,
                values=values_d,
                hole=0.55,
                marker=dict(colors=colors_d[: len(labels_d)]),
                textinfo="label+percent",
                textfont=dict(size=11),
            )
        )
        fig_donut.update_layout(
            **_CHART_LAYOUT,
            height=300,
            title=dict(text="Regime Distribution", font=dict(size=13)),
            showlegend=False,
        )
        st.plotly_chart(fig_donut, use_container_width=True)

    # ── Top N Table ──
    st.markdown(
        f'<div class="layer-header">TOP {top_n} SIGNALS</div>',
        unsafe_allow_html=True,
    )
    top_df = df.head(top_n).copy()
    top_df.index = range(1, len(top_df) + 1)
    top_df.index.name = "Rank"

    st.dataframe(
        top_df,
        use_container_width=True,
        height=min(40 * len(top_df) + 40, 500),
        column_config={
            "Price": st.column_config.NumberColumn(format="%.2f"),
            "Score": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%d"),
            "Macro": st.column_config.ProgressColumn(min_value=0, max_value=33, format="%d"),
            "Hurst": st.column_config.NumberColumn(format="%.3f"),
            "SMA %": st.column_config.NumberColumn(format="%.2f"),
            "Vol %": st.column_config.NumberColumn(format="%.1f"),
        },
    )

    # ── Full Results + CSV Download ──
    with st.expander(f"Full Results ({len(df)} symbols)"):
        st.dataframe(
            df,
            use_container_width=True,
            height=600,
            column_config={
                "Price": st.column_config.NumberColumn(format="%.2f"),
                "Score": st.column_config.ProgressColumn(min_value=0, max_value=100, format="%d"),
                "Macro": st.column_config.ProgressColumn(min_value=0, max_value=33, format="%d"),
                "Hurst": st.column_config.NumberColumn(format="%.3f"),
                "SMA %": st.column_config.NumberColumn(format="%.2f"),
                "Vol %": st.column_config.NumberColumn(format="%.1f"),
            },
        )
        csv = df.to_csv(index=False)
        st.download_button(
            "Download CSV",
            csv,
            file_name="signal_scan.csv",
            mime="text/csv",
        )

    st.caption("Select a specific symbol from the dropdown for full 3-layer deep analysis.")


def render_signal_dashboard():
    """Main entry point for the Signal Analysis page."""
    st.markdown(_PAGE_CSS, unsafe_allow_html=True)

    con = get_connection()
    symbols = _get_symbols(con)

    if not symbols:
        st.error("No symbols found. Sync data first.")
        return

    # ── Top Bar: Symbol selector + filters on main page ──
    options = [_ALL_MARKET] + symbols
    sectors = _get_sectors(con)

    row1 = st.columns([3, 1, 1])
    with row1[0]:
        default_idx = 0  # Start on ALL (Batch Scanner)
        selection = st.selectbox(
            "Symbol",
            options,
            index=default_idx,
            key="signal_symbol",
            label_visibility="collapsed",
            placeholder="Select symbol or ALL for batch scanner...",
        )
    with row1[1]:
        run_btn = st.button("Run Analysis", type="primary", use_container_width=True)
    with row1[2]:
        if _is_market_open():
            st.markdown(":red_circle: **LIVE**")
        else:
            st.markdown(":black_circle: **Closed**")

    is_screener = selection == _ALL_MARKET

    # ═══ Batch Scanner Mode ═══
    if is_screener:
        # Filters row
        filter_cols = st.columns([2, 1, 1])
        with filter_cols[0]:
            sector_filter = st.selectbox(
                "Sector",
                ["All Sectors"] + sectors,
                key="signal_sector_filter",
                label_visibility="collapsed",
            )
        with filter_cols[1]:
            min_volume = st.number_input(
                "Min Avg Volume (20d)",
                min_value=0,
                value=100_000,
                step=50_000,
                key="signal_min_vol",
                label_visibility="collapsed",
                help="Minimum 20-day average volume",
            )
        with filter_cols[2]:
            top_n = st.selectbox(
                "Show Top",
                [10, 20, 50, 100],
                index=0,
                key="signal_top_n",
                label_visibility="collapsed",
                help="Top N results to highlight",
            )

        sec_val = "" if sector_filter == "All Sectors" else sector_filter

        last_mode = st.session_state.get("signal_mode_last")
        cached_results: list[BatchScanResult] | None = st.session_state.get(
            "signal_batch_results"
        )
        cached_params = st.session_state.get("signal_batch_params")
        current_params = (sec_val, min_volume)

        need_run = (
            run_btn
            or last_mode != "batch"
            or cached_results is None
            or cached_params != current_params
        )

        if need_run:
            results = _run_batch_scan(con, symbols, min_volume, sec_val)
            st.session_state["signal_batch_results"] = results
            st.session_state["signal_batch_params"] = current_params
            st.session_state["signal_mode_last"] = "batch"
        else:
            results = cached_results

        _render_batch_scanner(results, top_n)
        render_footer()
        return

    # ═══ Single Symbol Mode ═══
    symbol = selection

    # Show symbol name prominently
    info = _get_symbol_info(con, symbol)
    sector_name = info.get("sector_name") or ""
    sector_label = f" — {sector_name}" if sector_name else ""
    st.markdown(
        f'<div style="font-size:1.8rem;font-weight:bold;color:{_COLORS["accent"]};'
        f'margin-bottom:4px">{symbol}{sector_label}</div>',
        unsafe_allow_html=True,
    )

    # Determine whether to run analysis
    need_run = run_btn
    cached_report: SignalReport | None = st.session_state.get("signal_report")
    last_symbol = st.session_state.get("signal_symbol_last")
    last_mode = st.session_state.get("signal_mode_last")

    if last_mode != "single" or (cached_report is not None and last_symbol != symbol):
        need_run = True

    if need_run:
        with st.spinner(f"Analyzing {symbol}..."):
            report = _run_full_analysis(con, symbol)
            st.session_state["signal_report"] = report
            st.session_state["signal_symbol_last"] = symbol
            st.session_state["signal_mode_last"] = "single"
    else:
        report = cached_report

    if report is None:
        with st.spinner(f"Analyzing {symbol}..."):
            report = _run_full_analysis(con, symbol)
            st.session_state["signal_report"] = report
            st.session_state["signal_symbol_last"] = symbol
            st.session_state["signal_mode_last"] = "single"

    # ── Data Availability (inline) ──
    avail_parts = [f"EOD: {report.eod_days}d", f"Intraday: {report.intraday_bars} bars"]
    if report.tick_count > 0:
        avail_parts.append(f"Ticks: {report.tick_count:,}")
    else:
        avail_parts.append("Ticks: —")
    st.caption(" · ".join(avail_parts))

    # ── Signal Score ──
    _render_signal_score(report)

    # ── Layer 1 ──
    if report.macro:
        _render_layer1(report.macro)

    # ── Layer 2 ──
    if report.intraday:
        _render_layer2(report.intraday)

    # ── Layer 3 ──
    if report.execution:
        _render_layer3(report.execution)

    # ── AI Commentary ──
    _render_signal_commentary(report, symbol)

    # ── Methodology ──
    with st.expander("Methodology"):
        st.markdown("""
**Signal Score (1-100)** combines three layers, each contributing 0-33 points.

**Layer 1 - Macro Regime** (Daily OHLCV)
- Hurst Exponent (R/S method): H>0.55=Trending, H<0.45=Mean-Reverting
- 200-Day SMA distance: Price position relative to long-term trend
- Annualized Volatility: Log-return volatility scaled to 245 PSX trading days
- Circuit Breaker detection: Flags +/-7% daily moves

**Layer 2 - Intraday Anchor** (Intraday Bars)
- Anchored VWAP with +/-1,2 sigma bands (session-reset daily)
- Volume Profile POC (Point of Control) + 70% Value Area
- Efficiency Ratio spike detection (large price move on thin volume)

**Layer 3 - Execution DNA** (Tick Logs)
- Lee-Ready trade classification (tick rule with forward-fill)
- Cumulative Volume Delta (CVD) + slope regression
- Order Flow Imbalance per minute (normalized to +/-1.0)
- Block trade detection (volume > 5x median)
- Cross-market REG vs FUT CVD divergence
- Session segmentation OFI (Pre-Open / Morning / Afternoon / Close)
- VPIN integration (from existing VPIN engine)

**Score Interpretation:**
- 86-100: Exceptional Confluence
- 71-85: Strong Buy Setup
- 51-70: Moderate Buy Setup
- 31-50: Neutral - Wait
- 0-30: Weak - Avoid
        """)

    render_footer()


def _run_full_analysis(con, symbol: str) -> SignalReport:
    """Execute the full 3-layer analysis pipeline."""
    report = SignalReport(
        symbol=symbol,
        timestamp=datetime.datetime.now().isoformat(),
        market_is_open=_is_market_open(),
    )

    # --- Symbol info ---
    info = _get_symbol_info(con, symbol)

    # --- Layer 1: Macro Regime ---
    eod_df = _get_eod_data(con, symbol)
    report.eod_days = len(eod_df)

    index_df = _get_index_data(con)

    if not eod_df.empty:
        if len(eod_df) < 200:
            st.warning(f"Only {len(eod_df)} days of EOD data. SMA uses {len(eod_df)}-day window.")
        if len(eod_df) < 30:
            st.warning("Insufficient data for Hurst Exponent (need 30+ days).")

        report.macro = compute_macro_regime(
            eod_df,
            symbol,
            sector=info.get("sector"),
            sector_name=info.get("sector_name"),
            index_df=index_df if not index_df.empty else None,
        )
    else:
        st.error(f"No EOD data found for {symbol}. Check the Symbols page.")

    # --- Layer 2: Intraday Anchor ---
    intraday_df = _get_intraday_data(con, symbol)
    report.intraday_bars = len(intraday_df)

    if not intraday_df.empty:
        report.intraday = compute_intraday_anchor(
            intraday_df, data_source="intraday_bars", dt_col="ts"
        )
    else:
        report.intraday = IntradayAnchor(data_source="none")

    # --- Layer 3: Execution DNA ---
    has_ticks = _check_tick_table_exists(con)

    if has_ticks:
        reg_ticks = _get_tick_data(con, symbol, market="REG")
        fut_ticks = _get_tick_data(con, symbol, market="FUT")
        report.tick_count = len(reg_ticks)

        if not reg_ticks.empty:
            report.execution = compute_execution_dna(
                reg_ticks,
                fut_tick_df=fut_ticks if not fut_ticks.empty else None,
            )
        else:
            report.execution = ExecutionDNA()
    else:
        report.execution = ExecutionDNA()

    # --- Composite Score ---
    report.signal_score = compute_signal_score(
        report.macro, report.intraday, report.execution
    )
    report.interpretation = interpret_score(report.signal_score)

    return report
