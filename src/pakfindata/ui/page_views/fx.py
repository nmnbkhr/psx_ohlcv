"""FX overview and impact pages."""

import pandas as pd
import streamlit as st

from pakfindata.analytics_fx import (
    compute_and_store_fx_adjusted_metrics,
    get_fx_analytics,
    get_normalized_fx_performance,
)
from pakfindata.config import get_db_path
from pakfindata.db import (
    get_fx_adjusted_metrics,
    get_fx_ohlcv,
    get_fx_pairs,
)
from pakfindata.sync_fx import (
    seed_fx_pairs,
    sync_fx_pairs,
)
from pakfindata.ui.components.helpers import (
    get_connection,
    render_footer,
)


def render_fx_overview():
    """FX Overview - Macro context for currency analysis."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🌍 FX Overview")
        st.caption("Foreign Exchange Analytics - Macro Context (Read-Only)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📊 Macro Context Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # Get FX pairs
    fx_pairs = get_fx_pairs(con, active_only=True)

    if not fx_pairs:
        st.warning(
            "No FX pairs found. Run `pfsync fx seed` to seed FX pairs, "
            "then `pfsync fx sync` to fetch data."
        )
        render_footer()
        return

    # Pair selector
    col1, col2 = st.columns([2, 4])

    with col1:
        pair_names = [p["pair"] for p in fx_pairs]
        selected_pair = st.selectbox(
            "Select Currency Pair",
            pair_names,
            index=0 if "USD/PKR" in pair_names else 0,
        )

    # Get analytics for selected pair
    analytics = get_fx_analytics(con, selected_pair)

    if analytics.get("error"):
        st.info(
            f"No data available for {selected_pair}. "
            "Run `pfsync fx sync` to fetch data."
        )
        render_footer()
        return

    # Key metrics
    st.markdown("---")
    st.subheader("Current Rate & Returns")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        latest_rate = analytics.get("latest_close", 0)
        st.metric("Latest Rate", f"{latest_rate:.4f}")

    with col2:
        ret_1w = analytics.get("return_1W", 0) or 0
        st.metric(
            "1 Week",
            f"{ret_1w * 100:+.2f}%",
            delta=f"{ret_1w * 100:+.2f}%",
            delta_color="inverse"  # Red for appreciation (bad for PKR)
        )

    with col3:
        ret_1m = analytics.get("return_1M", 0) or 0
        st.metric(
            "1 Month",
            f"{ret_1m * 100:+.2f}%",
            delta=f"{ret_1m * 100:+.2f}%",
            delta_color="inverse"
        )

    with col4:
        ret_3m = analytics.get("return_3M", 0) or 0
        st.metric(
            "3 Month",
            f"{ret_3m * 100:+.2f}%",
            delta=f"{ret_3m * 100:+.2f}%",
            delta_color="inverse"
        )

    # Trend info
    trend = analytics.get("trend", {})
    if trend:
        col1, col2 = st.columns(2)
        with col1:
            direction = trend.get("trend_direction", "N/A").upper()
            strength = trend.get("trend_strength", "N/A")
            if direction == "UP":
                st.warning(f"📈 Trend: {direction} ({strength}) - PKR Depreciating")
            else:
                st.success(f"📉 Trend: {direction} ({strength}) - PKR Appreciating")

        with col2:
            vol_1m = analytics.get("vol_1M", 0) or 0
            st.metric("30D Volatility", f"{vol_1m * 100:.2f}%")

    # FX Chart
    st.markdown("---")
    st.subheader("FX Rate Chart")

    # Date range selector
    date_range = st.selectbox(
        "Time Range",
        ["30 Days", "90 Days", "180 Days", "1 Year"],
        index=1,
    )

    days_map = {"30 Days": 30, "90 Days": 90, "180 Days": 180, "1 Year": 365}
    days = days_map[date_range]

    # Get OHLCV data
    df = get_fx_ohlcv(con, selected_pair, limit=days)

    if not df.empty:
        df = df.sort_values("date")

        import plotly.graph_objects as go

        # Create candlestick chart
        fig = go.Figure()

        if len(df) <= 60:
            # Candlestick for shorter periods
            fig.add_trace(go.Candlestick(
                x=df["date"],
                open=df["open"],
                high=df["high"],
                low=df["low"],
                close=df["close"],
                name=selected_pair,
            ))
        else:
            # Line chart for longer periods
            fig.add_trace(go.Scatter(
                x=df["date"],
                y=df["close"],
                mode="lines",
                name=selected_pair,
                line=dict(color="#2196F3", width=2),
            ))

            # Add 50-day MA
            if len(df) >= 50:
                df["ma50"] = df["close"].rolling(window=50).mean()
                fig.add_trace(go.Scatter(
                    x=df["date"],
                    y=df["ma50"],
                    mode="lines",
                    name="50D MA",
                    line=dict(color="#FFC107", width=1, dash="dash"),
                ))

        fig.update_layout(
            title=f"{selected_pair} - {date_range}",
            xaxis_title="Date",
            yaxis_title="Rate",
            height=400,
            xaxis_rangeslider_visible=False,
        )

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No chart data available.")

    # Multi-pair comparison
    st.markdown("---")
    st.subheader("Multi-Pair Comparison")

    if len(pair_names) >= 2:
        compare_pairs = st.multiselect(
            "Select pairs to compare",
            pair_names,
            default=pair_names[:3] if len(pair_names) >= 3 else pair_names,
            max_selections=5,
        )

        if compare_pairs:
            perf_df = get_normalized_fx_performance(con, compare_pairs)

            if not perf_df.empty:
                import plotly.graph_objects as go

                fig = go.Figure()

                for pair in compare_pairs:
                    if pair in perf_df.columns:
                        fig.add_trace(go.Scatter(
                            x=perf_df.index,
                            y=perf_df[pair],
                            mode="lines",
                            name=pair,
                        ))

                fig.update_layout(
                    title="Normalized Performance (Base = 100)",
                    xaxis_title="Date",
                    yaxis_title="Value",
                    height=350,
                    legend=dict(orientation="h", yanchor="bottom", y=1.02),
                )

                st.plotly_chart(fig, use_container_width=True)

    # Sync section
    st.markdown("---")
    st.subheader("Sync FX Data")

    col1, col2 = st.columns(2)

    with col1:
        if st.button("Sync FX Rates", type="primary"):
            with st.spinner("Syncing FX data..."):
                summary = sync_fx_pairs(db_path=get_db_path())
                st.success(
                    f"Sync complete: {summary.ok} OK, "
                    f"{summary.rows_upserted} rows"
                )

    with col2:
        if st.button("Seed FX Pairs"):
            result = seed_fx_pairs(db_path=get_db_path())
            st.success(f"Seeded {result.get('inserted', 0)} pairs")

    render_footer()


def render_fx_impact():
    """FX Impact - FX-adjusted equity performance analysis."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 FX Impact")
        st.caption("FX-Adjusted Equity Performance (Read-Only Analytics)")
    with header_col2:
        st.markdown(
            '<div class="data-info">📈 Analytics Only</div>',
            unsafe_allow_html=True
        )

    con = get_connection()

    # Get FX pairs
    fx_pairs = get_fx_pairs(con, active_only=True)

    if not fx_pairs:
        st.warning("No FX pairs found. Run `pfsync fx seed` first.")
        render_footer()
        return

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        pair_names = [p["pair"] for p in fx_pairs]
        default_idx = pair_names.index("USD/PKR") if "USD/PKR" in pair_names else 0
        selected_pair = st.selectbox(
            "FX Pair for Adjustment",
            pair_names,
            index=default_idx,
        )

    with col2:
        period = st.selectbox(
            "Return Period",
            ["1W", "1M", "3M"],
            index=1,
        )

    with col3:
        top_n = st.slider("Top N Stocks", min_value=10, max_value=50, value=20)

    # Get FX analytics for context
    fx_analytics = get_fx_analytics(con, selected_pair)

    # Show FX context
    st.markdown("---")
    st.subheader(f"{selected_pair} Context")

    col1, col2, col3 = st.columns(3)

    with col1:
        fx_return = fx_analytics.get(f"return_{period}", 0) or 0
        st.metric(
            f"FX Return ({period})",
            f"{fx_return * 100:+.2f}%",
            help="Positive = PKR depreciation"
        )

    with col2:
        latest = fx_analytics.get("latest_close", 0)
        st.metric("Latest Rate", f"{latest:.2f}")

    with col3:
        vol = fx_analytics.get("vol_1M", 0) or 0
        st.metric("FX Volatility", f"{vol * 100:.1f}%")

    # Explanation
    st.markdown("""
    **How FX-Adjusted Returns Work:**
    - FX-Adjusted Return = Equity Return - FX Return
    - If PKR depreciates by 2% and stock rises 5%, the USD-adjusted return is 3%
    - This helps compare PSX returns with global benchmarks
    """)

    # Get FX-adjusted metrics
    st.markdown("---")
    st.subheader("FX-Adjusted Performance")

    metrics = get_fx_adjusted_metrics(
        con,
        fx_pair=selected_pair,
        period=period,
        limit=top_n,
    )

    if not metrics:
        st.info(
            "No FX-adjusted metrics available. "
            "Run `pfsync fx compute-adjusted` to compute."
        )

        if st.button("Compute FX-Adjusted Metrics", type="primary"):
            with st.spinner("Computing metrics..."):
                result = compute_and_store_fx_adjusted_metrics(
                    con,
                    fx_pair=selected_pair,
                )
                if result.get("success"):
                    st.success(f"Computed {result.get('metrics_stored', 0)} metrics")
                    st.rerun()
                else:
                    st.error(f"Error: {result.get('error')}")
    else:
        # Convert to DataFrame
        df = pd.DataFrame(metrics)

        # Format for display
        display_df = df[["symbol", "equity_return", "fx_return", "fx_adjusted_return"]].copy()
        display_df["equity_return"] = display_df["equity_return"].apply(
            lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
        )
        display_df["fx_return"] = display_df["fx_return"].apply(
            lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
        )
        display_df["fx_adjusted_return"] = display_df["fx_adjusted_return"].apply(
            lambda x: f"{x * 100:.2f}%" if pd.notna(x) else "N/A"
        )

        display_df.columns = ["Symbol", f"Equity ({period})", f"FX ({period})", "Adjusted"]

        st.dataframe(display_df, use_container_width=True, hide_index=True)

        # Visualization
        st.markdown("---")
        st.subheader("Visual Comparison")

        # Select stocks to visualize
        symbols = df["symbol"].tolist()
        selected_symbols = st.multiselect(
            "Select stocks to compare",
            symbols,
            default=symbols[:5] if len(symbols) >= 5 else symbols,
            max_selections=10,
        )

        if selected_symbols:
            import plotly.graph_objects as go

            filtered_df = df[df["symbol"].isin(selected_symbols)]

            fig = go.Figure()

            # Equity returns
            fig.add_trace(go.Bar(
                name=f"Equity Return ({period})",
                x=filtered_df["symbol"],
                y=filtered_df["equity_return"] * 100,
                marker_color="#2196F3",
            ))

            # FX-adjusted returns
            fig.add_trace(go.Bar(
                name="FX-Adjusted Return",
                x=filtered_df["symbol"],
                y=filtered_df["fx_adjusted_return"] * 100,
                marker_color="#4CAF50",
            ))

            fig.update_layout(
                title=f"Equity vs FX-Adjusted Returns ({period})",
                xaxis_title="Symbol",
                yaxis_title="Return (%)",
                barmode="group",
                height=400,
            )

            st.plotly_chart(fig, use_container_width=True)

        # Summary stats
        st.markdown("---")
        st.subheader("Summary Statistics")

        col1, col2, col3 = st.columns(3)

        valid_adj = df["fx_adjusted_return"].dropna()

        if len(valid_adj) > 0:
            with col1:
                best = df.loc[df["fx_adjusted_return"].idxmax()]
                st.metric(
                    "Best FX-Adjusted",
                    best["symbol"],
                    f"{best['fx_adjusted_return'] * 100:.1f}%"
                )

            with col2:
                worst = df.loc[df["fx_adjusted_return"].idxmin()]
                st.metric(
                    "Worst FX-Adjusted",
                    worst["symbol"],
                    f"{worst['fx_adjusted_return'] * 100:.1f}%"
                )

            with col3:
                avg = valid_adj.mean() * 100
                st.metric("Average Adjusted", f"{avg:.1f}%")

    # Sync Section
    st.markdown("---")
    with st.expander("Sync FX Data & Compute Metrics", expanded=False):
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Seed FX Pairs", key="fxi_seed"):
                result = seed_fx_pairs(db_path=get_db_path())
                st.success(f"Seeded {result.get('inserted', 0)} pairs")
                st.rerun()

        with col2:
            if st.button("Sync FX Rates", type="primary", key="fxi_sync"):
                with st.spinner("Syncing FX data..."):
                    summary = sync_fx_pairs(db_path=get_db_path())
                    st.success(
                        f"Sync: {summary.ok} OK, {summary.rows_upserted} rows"
                    )
                    st.rerun()

        with col3:
            if st.button("Compute FX-Adjusted Metrics", key="fxi_compute"):
                with st.spinner("Computing FX-adjusted metrics..."):
                    result = compute_and_store_fx_adjusted_metrics(
                        con, fx_pair=selected_pair
                    )
                    if result.get("success"):
                        st.success(
                            f"Computed metrics for {result.get('symbols_processed', 0)} symbols"
                        )
                        st.rerun()
                    else:
                        st.error(f"Error: {result.get('error', 'Unknown')}")

    render_footer()
