"""FX Rate History — interactive charts for any currency pair."""

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from pakfindata.ui.components.helpers import get_connection, render_footer


def render_fx_history():
    """Render the FX Rate History page — interactive pair charts."""
    st.markdown("## FX Rate History")
    st.caption("Interactive charts for any currency pair over time")

    con = get_connection()
    if con is None:
        st.error("Database connection not available")
        return

    # Get available pairs
    pairs = _get_available_pairs(con)
    if not pairs:
        st.info(
            "No FX data available. Run `pfsync fx seed` then `pfsync fx sync` to fetch data."
        )
        render_footer()
        return

    # ── Controls ─────────────────────────────────────────────────
    col1, col2 = st.columns([2, 1])

    with col1:
        default_idx = pairs.index("USD/PKR") if "USD/PKR" in pairs else 0
        selected_pair = st.selectbox("Currency Pair", pairs, index=default_idx, key="fxh_pair")

    with col2:
        period = st.selectbox(
            "Time Range",
            ["30 Days", "90 Days", "180 Days", "1 Year", "All"],
            index=2,
            key="fxh_period",
        )

    days_map = {"30 Days": 30, "90 Days": 90, "180 Days": 180, "1 Year": 365, "All": 9999}
    limit = days_map[period]

    # ── Rate Chart ───────────────────────────────────────────────
    _render_rate_chart(con, selected_pair, limit)

    st.divider()

    # ── Key Metrics ──────────────────────────────────────────────
    _render_rate_metrics(con, selected_pair)

    st.divider()

    # ── Multi-Pair Comparison ────────────────────────────────────
    _render_multi_pair_comparison(con, pairs)

    # ── Sync Section ─────────────────────────────────────────────
    st.divider()
    with st.expander("Sync FX Data"):
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Sync FX Rates", type="primary", key="fxh_sync"):
                with st.spinner("Syncing FX data..."):
                    try:
                        from pakfindata.config import get_db_path
                        from pakfindata.sync_fx import sync_fx_pairs
                        result = sync_fx_pairs(db_path=get_db_path())
                        st.success(f"Sync: {result.ok} OK, {result.rows_upserted} rows")
                    except Exception as e:
                        st.error(f"Sync failed: {e}")
        with col2:
            if st.button("Seed FX Pairs", key="fxh_seed"):
                try:
                    from pakfindata.config import get_db_path
                    from pakfindata.sync_fx import seed_fx_pairs
                    result = seed_fx_pairs(db_path=get_db_path())
                    st.success(f"Seeded {result.get('inserted', 0)} pairs")
                except Exception as e:
                    st.error(f"Seed failed: {e}")

    render_footer()


def _get_available_pairs(con) -> list[str]:
    """Get list of available FX pairs."""
    try:
        from pakfindata.db import get_fx_pairs
        pairs_data = get_fx_pairs(con, active_only=True)
        return [p["pair"] for p in pairs_data] if pairs_data else []
    except Exception:
        return []


def _render_rate_chart(con, pair: str, limit: int):
    """Interactive rate chart for selected pair."""
    try:
        from pakfindata.db import get_fx_ohlcv
        df = get_fx_ohlcv(con, pair, limit=limit)
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        st.info(f"No chart data for {pair}.")
        return

    df = df.sort_values("date")

    fig = go.Figure()

    if len(df) <= 60:
        fig.add_trace(go.Candlestick(
            x=df["date"],
            open=df["open"], high=df["high"],
            low=df["low"], close=df["close"],
            name=pair,
        ))
    else:
        fig.add_trace(go.Scatter(
            x=df["date"], y=df["close"],
            mode="lines", name=pair,
            line=dict(color="#4DA8DA", width=2),
        ))
        # 50-day MA
        if len(df) >= 50:
            df["ma50"] = df["close"].rolling(window=50).mean()
            fig.add_trace(go.Scatter(
                x=df["date"], y=df["ma50"],
                mode="lines", name="50D MA",
                line=dict(color="#FFC107", width=1, dash="dash"),
            ))

    fig.update_layout(
        title=f"{pair} Rate",
        xaxis_title="Date", yaxis_title="Rate",
        height=450,
        xaxis_rangeslider_visible=False,
    )
    st.plotly_chart(fig, use_container_width=True)

    # Data table
    with st.expander("View Data"):
        st.dataframe(df[["date", "open", "high", "low", "close"]].tail(30),
                      use_container_width=True, hide_index=True)
        csv = df.to_csv(index=False)
        st.download_button("Export CSV", csv, f"fx_{pair.replace('/', '_')}.csv",
                           "text/csv", key="fxh_export")


def _render_rate_metrics(con, pair: str):
    """Key metrics for selected pair."""
    st.subheader("Rate Metrics")

    try:
        from pakfindata.analytics_fx import get_fx_analytics
        analytics = get_fx_analytics(con, pair)
    except Exception:
        analytics = {}

    if analytics.get("error"):
        st.info(f"No analytics for {pair}.")
        return

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Latest", f"{analytics.get('latest_close', 0):.4f}")

    ret_1w = analytics.get("return_1W", 0) or 0
    c2.metric("1 Week", f"{ret_1w * 100:+.2f}%",
              delta=f"{ret_1w * 100:+.2f}%", delta_color="inverse")

    ret_1m = analytics.get("return_1M", 0) or 0
    c3.metric("1 Month", f"{ret_1m * 100:+.2f}%",
              delta=f"{ret_1m * 100:+.2f}%", delta_color="inverse")

    vol = analytics.get("vol_1M", 0) or 0
    c4.metric("30D Volatility", f"{vol * 100:.2f}%")


def _render_multi_pair_comparison(con, pairs: list[str]):
    """Normalized performance comparison across pairs."""
    st.subheader("Multi-Pair Comparison")

    if len(pairs) < 2:
        st.info("Need at least 2 pairs for comparison.")
        return

    selected = st.multiselect(
        "Select pairs to compare",
        pairs,
        default=pairs[:min(3, len(pairs))],
        max_selections=5,
        key="fxh_compare",
    )

    if not selected:
        return

    try:
        from pakfindata.analytics_fx import get_normalized_fx_performance
        perf_df = get_normalized_fx_performance(con, selected)
    except Exception:
        perf_df = pd.DataFrame()

    if perf_df.empty:
        st.info("Not enough data for comparison.")
        return

    fig = go.Figure()
    for pair in selected:
        if pair in perf_df.columns:
            fig.add_trace(go.Scatter(
                x=perf_df.index, y=perf_df[pair],
                mode="lines", name=pair,
            ))

    fig.update_layout(
        title="Normalized Performance (Base = 100)",
        xaxis_title="Date", yaxis_title="Value",
        height=400,
        legend=dict(orientation="h", y=-0.15),
    )
    st.plotly_chart(fig, use_container_width=True)
