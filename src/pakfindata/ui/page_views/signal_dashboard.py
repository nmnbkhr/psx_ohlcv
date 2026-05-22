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
from pakfindata.ui.api import client as api_client
from pakfindata.ui.components.helpers import get_connection, render_footer

from pakfindata.services.llm_client import llm


# ═════════════════════════════════════════════════════════════════════════════
# DESIGN SYSTEM (matches existing Bloomberg dark theme)
# ═════════════════════════════════════════════════════════════════════════════

_COLORS = {
    "accent": "#2F81F7",
    "up": "#22c55e",
    "down": "#FF5252",
    "neutral": "#78909C",
    "bg": "#0B0E11",
    "card_bg": "rgba(0,0,0,0)",
    "card_border": "#30363D",
    "grid": "rgba(0,0,0,0)",
    "text": "#e0e0e0",
    "text_dim": "#6B7280",
    "vwap_line": "#2F81F7",
    "band_1": "rgba(47,129,247,0.3)",
    "band_2": "rgba(47,129,247,0.15)",
    "poc_line": "#FFD600",
    "va_fill": "rgba(255,214,0,0.1)",
    "buy": "#22c55e",
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
    border: 1px solid #30363D;
    margin-bottom: 16px;
}
.signal-score-value {
    font-size: 4rem;
    font-weight: bold;
    line-height: 1;
}
.signal-score-label {
    font-size: 1rem;
    color: #6B7280;
    margin-top: 4px;
}
.signal-breakdown {
    font-size: 0.9rem;
    color: #6B7280;
    margin-top: 8px;
}
.metric-value {
    font-size: 1.6rem;
    font-weight: bold;
    color: #2F81F7;
}
.metric-label {
    font-size: 0.8rem;
    color: #6B7280;
    margin-top: 2px;
}
.metric-sub {
    font-size: 0.75rem;
    color: #6B7280;
    margin-top: 2px;
}
.layer-header {
    font-size: 1.1rem;
    font-weight: bold;
    color: #2F81F7;
    border-bottom: 1px solid #30363D;
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
# DATA LOADING (cached)
# ═════════════════════════════════════════════════════════════════════════════


def _eod_from_date(limit: int) -> str:
    """Resolve a `from_date` string that covers approximately `limit`
    trading days (using a 1.8x calendar-day buffer for weekends/holidays)."""
    cal_days = max(int(limit * 1.8), 60)
    return (
        datetime.date.today() - datetime.timedelta(days=cal_days)
    ).isoformat()


def _get_symbols() -> list[str]:
    """Get active symbols list via /v1/symbols (cached at client)."""
    rows = api_client.get_symbols(active_only=True) or []
    return [r["symbol"] for r in rows]


def _get_symbol_info(symbol: str) -> dict:
    """Get sector info for a symbol — filter the cached /v1/symbols payload."""
    rows = api_client.get_symbols(active_only=True) or []
    for r in rows:
        if r.get("symbol") == symbol:
            return {"sector": r.get("sector"), "sector_name": r.get("sector_name")}
    return {"sector": None, "sector_name": None}


def _get_eod_data(symbol: str, limit: int = 600) -> pd.DataFrame:
    """Query EOD OHLCV via /v1/eod/{symbol} (cached at client).

    Client requests a wide-enough window via from_date and trims to the
    last ``limit`` rows here.
    """
    rows = api_client.get_symbol_history(
        symbol=symbol, from_date=_eod_from_date(limit)
    ) or []
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    keep = [c for c in ["date", "open", "high", "low", "close", "volume"] if c in df.columns]
    df = df[keep].sort_values("date").tail(limit).reset_index(drop=True)
    return df


def _get_index_data(limit: int = 600) -> pd.DataFrame:
    """Query KSE-100 daily data via /v1/eod/{symbol} — tries known aliases."""
    for idx_sym in ("KSE100", "KSE-100", "KSEALL"):
        rows = api_client.get_symbol_history(
            symbol=idx_sym, from_date=_eod_from_date(limit)
        ) or []
        if rows:
            df = pd.DataFrame(rows)
            keep = [c for c in ["date", "close"] if c in df.columns]
            return df[keep].sort_values("date").tail(limit).reset_index(drop=True)
    return pd.DataFrame()


@st.cache_data(ttl=86400, show_spinner=False, hash_funcs={pd.DataFrame: lambda _: None})
def _cached_macro_regime(
    symbol: str,
    eod_as_of: str,
    index_as_of: str,
    eod_df: pd.DataFrame,
    sector: str,
    sector_name: str,
    index_df: pd.DataFrame,
) -> MacroRegime:
    """Cache `compute_macro_regime` by (symbol, eod_as_of, index_as_of).
    EOD is write-once per day, so result is stable for the whole day.
    DataFrame args are excluded from the hash key (passed for the computation only).
    """
    return compute_macro_regime(
        eod_df,
        symbol,
        sector=sector or None,
        sector_name=sector_name or None,
        index_df=index_df if not index_df.empty else None,
    )


@st.cache_data(ttl=1800, show_spinner=False)
def _get_intraday_data(symbol: str, limit: int = 5000) -> pd.DataFrame:
    """Intraday bars for the latest date — reads directly from the JSONL tick
    log via in-memory DuckDB. Avoids the 24M-row `intraday_bars` scan entirely
    (which is unreliable on FUSE filesystems).
    """
    from pathlib import Path
    import duckdb

    dates = api_client.get_intraday_dates() or []
    latest_date = dates[0] if dates else None
    if not latest_date:
        return pd.DataFrame()

    jf = Path(f"/mnt/e/psxdata/tick_logs_cloud/ticks_{latest_date}.jsonl")
    if not jf.exists():
        return pd.DataFrame()

    dcon = duckdb.connect(":memory:")
    try:
        df = dcon.execute(
            f"""SELECT _ts AS ts,
                       CAST(timestamp AS BIGINT) AS ts_epoch,
                       open, high, low,
                       price AS close,
                       volume
                FROM read_json_auto('{jf}', ignore_errors=true)
                WHERE symbol = ? AND market = 'REG'
                ORDER BY timestamp ASC
                LIMIT {int(limit)}""",
            [symbol],
        ).df()
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        dcon.close()


@st.cache_data(ttl=1800, show_spinner=False)
def _get_tick_data(
    symbol: str, market: str = "REG", days: int = 3
) -> pd.DataFrame:
    """Read ticks for a symbol+market from the JSONL file for the latest date
    on which this symbol actually has data.

    Uses `intraday_daily_summary` as a cheap index — looks up MAX(date) for
    the (symbol, market) pair, then opens that ONE JSONL file with in-memory
    DuckDB. Returns empty fast if the symbol/market combination has no data.
    """
    from pathlib import Path
    import duckdb

    # Latest summary date — page falls through gracefully if this symbol+market
    # combo has no rows in that file (DuckDB returns empty df).
    dates = api_client.get_intraday_dates() or []
    latest_date = dates[0] if dates else None
    if not latest_date:
        return pd.DataFrame()

    jf = Path(f"/mnt/e/psxdata/tick_logs_cloud/ticks_{latest_date}.jsonl")
    if not jf.exists():
        return pd.DataFrame()

    dcon = duckdb.connect(":memory:")
    try:
        df = dcon.execute(
            f"""SELECT symbol, market, timestamp, _ts, price, volume,
                       change, high, low, open,
                       previousClose AS prev_close,
                       bid, ask,
                       bidVol AS bid_vol, askVol AS ask_vol,
                       trades
                FROM read_json_auto('{jf}', ignore_errors=true)
                WHERE symbol = ? AND market = ?
                ORDER BY timestamp ASC""",
            [symbol, market],
        ).df()
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        dcon.close()


@st.cache_data(ttl=1800, show_spinner=False)
def _check_tick_table_exists() -> bool:
    """True when at least one JSONL tick file exists on disk."""
    from pathlib import Path
    return any(Path("/mnt/e/psxdata/tick_logs_cloud").glob("ticks_*.jsonl"))


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
        st.plotly_chart(fig, width='stretch')

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
            st.plotly_chart(fig_h, width='stretch')


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
        st.plotly_chart(fig, width='stretch')

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
        st.plotly_chart(fig_vp, width='stretch')


def _render_layer3(execution: ExecutionDNA):
    """Render Layer 3: Execution DNA section."""
    st.markdown('<div class="layer-header">LAYER 3: EXECUTION DNA</div>', unsafe_allow_html=True)

    if not execution.has_tick_data:
        st.info(
            "📊 No tick data available for this symbol yet. "
            "Layer 3 scores will update after market sync."
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
        st.plotly_chart(fig, width='stretch')

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
        st.plotly_chart(fig_ofi, width='stretch')

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
    if not llm.is_running():
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
    return llm.complete_chat_text(system, user, max_tokens=500, use_case="commentary")


def _render_signal_commentary(report: SignalReport, symbol: str):
    """Render the AI/rules-based commentary expander for single symbol."""
    with st.expander("Quant Analyst Commentary", expanded=True):
        use_ai = st.toggle("Enable Deep LLM Analysis (Ollama)", value=False, key="signal_ai_toggle")

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


def _get_sectors() -> list[str]:
    """Get distinct sector names via /v1/symbols/sectors."""
    return api_client.get_symbol_sectors() or []


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
        st.plotly_chart(fig_tree, width='stretch')

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
        st.plotly_chart(fig_donut, width='stretch')

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
        width='stretch',
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
            width='stretch',
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


def _render_intelligence_brief(symbol: str):
    """Cross-data intelligence brief — combines all data sources for a symbol."""
    from datetime import datetime, timedelta, timezone

    pkt = timezone(timedelta(hours=5))
    today = datetime.now(pkt).strftime("%Y-%m-%d")

    # Price action — latest EOD row via /v1/eod/{symbol}; client returns
    # the full default 90-day window, we take the last row.
    eod_rows = api_client.get_symbol_history(symbol=symbol) or []
    eod = eod_rows[-1] if eod_rows else None

    col_price, col_micro = st.columns(2)

    with col_price:
        st.markdown("**Price Action**")
        if eod:
            close = eod.get("close")
            prev = eod.get("prev_close")
            vol = eod.get("volume")
            high = eod.get("high")
            low = eod.get("low")
            chg = (close - prev) if (close is not None and prev) else 0
            chg_pct = (chg / prev * 100) if prev else 0
            st.markdown(
                f"Close: **{close:.2f}** ({chg:+.2f}, {chg_pct:+.2f}%)  \n"
                f"Range: {low:.2f} — {high:.2f}  \n"
                f"Volume: {vol:,}"
            )
        else:
            st.caption("No EOD data")

    with col_micro:
        st.markdown("**Microstructure**")
        try:
            from pakfindata.engine.block_trades import load_off_market, get_available_dates
            block_dates = get_available_dates()
            if block_dates:
                bdf = load_off_market(block_dates[0])
                sym_blocks = bdf[bdf["symbol"] == symbol] if not bdf.empty else pd.DataFrame()
                if not sym_blocks.empty:
                    block_vol = sym_blocks["turnover"].sum()
                    block_val = sym_blocks["value"].sum()
                    st.markdown(
                        f"Block trades: **{len(sym_blocks)}** ({block_vol:,} shares)  \n"
                        f"Block value: PKR {block_val/1e6:.1f}M"
                    )
                else:
                    st.caption("No block trades")
            else:
                st.caption("No off-market data")
        except Exception:
            st.caption("Block trade data unavailable")

    col_deriv, col_inst = st.columns(2)

    with col_deriv:
        st.markdown("**Derivatives**")
        try:
            fut = api_client.get_latest_futures(base_symbol=symbol)
            if fut and eod:
                fut_close = fut.get("close")
                fut_vol = fut.get("volume")
                month = fut.get("contract_month")
                eod_close = eod.get("close")
                basis = fut_close - eod_close if eod_close else 0
                basis_pct = (basis / eod_close * 100) if eod_close else 0
                signal = "Premium (Bullish)" if basis > 0 else "Discount (Bearish)"
                st.markdown(
                    f"Futures: **{fut_close:.2f}** ({month})  \n"
                    f"Basis: {basis:+.2f} ({basis_pct:+.3f}%) — {signal}  \n"
                    f"Fut Volume: {fut_vol:,}"
                )
            else:
                st.caption("No futures data")
        except Exception:
            st.caption("Futures data unavailable")

    with col_inst:
        st.markdown("**Institutional**")
        try:
            # Index weight
            import glob
            from pathlib import Path
            weight_files = sorted(glob.glob("/mnt/e/psxdata/downloads/daily/*/indices/constituent_data_*.xls"))
            if weight_files:
                import pandas as _pd
                wdf = _pd.read_excel(weight_files[-1], engine="xlrd")
                sym_row = wdf[wdf["SYMBOL"] == symbol]
                if not sym_row.empty:
                    wt = sym_row["IDX WT %"].iloc[0]
                    st.markdown(f"Index weight: **{wt:.3f}%**")
                else:
                    st.caption("Not in KSE-100")
            else:
                st.caption("No index weight data")
        except Exception:
            st.caption("Index weight unavailable")


def render_signal_dashboard():
    """Main entry point for the Signal Analysis page."""
    if api_client.render_api_status_banner_if_down():
        return

    st.markdown(_PAGE_CSS, unsafe_allow_html=True)

    # Engine batch scanner reads DB directly per CLAUDE.md exception —
    # connection retained for that call path only.
    con = get_connection()
    symbols = _get_symbols()

    if not symbols:
        st.error("No symbols found. Sync data first.")
        return

    # ── Top Bar: Symbol selector + filters on main page ──
    options = [_ALL_MARKET] + symbols
    sectors = _get_sectors()

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
        run_btn = st.button("Run Analysis", type="primary", width='stretch')
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

        cached_results: list[BatchScanResult] | None = st.session_state.get(
            "signal_batch_results"
        )
        cached_params = st.session_state.get("signal_batch_params")
        current_params = (sec_val, min_volume)

        if run_btn:
            # User clicked Run Analysis — compute fresh
            results = _run_batch_scan(con, symbols, min_volume, sec_val)
            st.session_state["signal_batch_results"] = results
            st.session_state["signal_batch_params"] = current_params
            st.session_state["signal_mode_last"] = "batch"
        elif cached_results is not None and cached_params == current_params:
            # Show cached results
            results = cached_results
        else:
            # First visit or params changed — show prompt instead of auto-running
            st.info("Click **Run Analysis** to scan all symbols. Previous results will be shown if available.")
            render_footer()
            return

        _render_batch_scanner(results, top_n)
        render_footer()
        return

    # ═══ Single Symbol Mode ═══
    symbol = selection

    # Show symbol name prominently
    info = _get_symbol_info(symbol)
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

    # ── Intelligence Brief ──
    with st.expander("Intelligence Brief — Cross-Data Summary", expanded=False):
        _render_intelligence_brief(symbol)

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
    info = _get_symbol_info(symbol)

    # --- Layer 1: Macro Regime ---
    eod_df = _get_eod_data(symbol)
    report.eod_days = len(eod_df)

    index_df = _get_index_data()

    if not eod_df.empty:
        if len(eod_df) < 200:
            st.warning(f"Only {len(eod_df)} days of EOD data. SMA uses {len(eod_df)}-day window.")
        if len(eod_df) < 30:
            st.warning("Insufficient data for Hurst Exponent (need 30+ days).")

        report.macro = _cached_macro_regime(
            symbol,
            str(eod_df["date"].iloc[-1]),
            str(index_df["date"].iloc[-1]) if not index_df.empty else "",
            eod_df,
            info.get("sector") or "",
            info.get("sector_name") or "",
            index_df,
        )
    else:
        st.error(f"No EOD data found for {symbol}. Check the Symbols page.")

    # --- Layer 2: Intraday Anchor ---
    intraday_df = _get_intraday_data(symbol)
    report.intraday_bars = len(intraday_df)

    if not intraday_df.empty:
        report.intraday = compute_intraday_anchor(
            intraday_df, data_source="intraday_bars", dt_col="ts"
        )
    else:
        report.intraday = IntradayAnchor(data_source="none")

    # --- Layer 3: Execution DNA ---
    has_ticks = _check_tick_table_exists()

    if has_ticks:
        reg_ticks = _get_tick_data(symbol, market="REG")
        fut_ticks = _get_tick_data(symbol, market="FUT")
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
