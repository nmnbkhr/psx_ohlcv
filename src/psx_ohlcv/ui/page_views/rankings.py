"""Stock rankings page."""

import pandas as pd
import streamlit as st

from psx_ohlcv.analytics_phase1 import (
    compute_rankings,
    get_normalized_performance,
    get_rankings,
)
from psx_ohlcv.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_rankings():
    """View and compare instrument performance rankings."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🏆 Instrument Rankings")
        st.caption("Performance comparison for ETFs, REITs, and Indexes")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Filter options
    col1, col2, col3 = st.columns([2, 2, 2])

    with col1:
        types_filter = st.multiselect(
            "Instrument Types",
            ["ETF", "REIT", "INDEX"],
            default=["ETF", "REIT", "INDEX"]
        )

    with col2:
        top_n = st.slider("Top N", min_value=5, max_value=50, value=10)

    with col3:
        compute_btn = st.button("Refresh Rankings", type="primary")

    if compute_btn and types_filter:
        with st.spinner("Computing rankings..."):
            result = compute_rankings(
                con,
                as_of_date=None,  # Today
                instrument_types=types_filter,
                top_n=top_n,
            )
            if result.get("success"):
                st.success(f"Computed rankings for {result.get('instruments_ranked', 0)} instruments")
            else:
                st.error(f"Error: {result.get('error', 'Unknown error')}")

    st.markdown("---")

    # Get rankings
    rankings = get_rankings(
        con,
        as_of_date=None,  # Most recent
        instrument_types=types_filter if types_filter else None,
        top_n=top_n,
    )

    if not rankings:
        st.info(
            "No rankings found. Click 'Refresh Rankings' to compute, "
            "or ensure instrument data is synced first."
        )
        render_footer()
        return

    # Rankings Table
    st.subheader("Performance Rankings")

    # Convert to DataFrame
    rankings_df = pd.DataFrame(rankings)

    # Format for display
    display_cols = ["symbol", "name", "instrument_type", "return_1m", "return_3m", "return_6m", "return_1y", "volatility_30d"]
    available_cols = [c for c in display_cols if c in rankings_df.columns]
    display_df = rankings_df[available_cols].copy()

    # Add rank column
    display_df.insert(0, "Rank", range(1, len(display_df) + 1))

    # Format percentages
    pct_cols = ["return_1m", "return_3m", "return_6m", "return_1y", "volatility_30d"]
    for col in pct_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].apply(
                lambda x: f"{x * 100:.1f}%" if pd.notna(x) else "N/A"
            )

    # Rename columns
    col_names = {
        "symbol": "Symbol",
        "name": "Name",
        "instrument_type": "Type",
        "return_1m": "1M",
        "return_3m": "3M",
        "return_6m": "6M",
        "return_1y": "1Y",
        "volatility_30d": "Vol 30D",
    }
    display_df.rename(columns=col_names, inplace=True)

    st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Performance Comparison Chart
    st.subheader("Performance Comparison (Normalized)")

    if len(rankings) >= 2:
        # Let user select instruments to compare
        symbols = [r.get("symbol") for r in rankings if r.get("symbol")]

        if symbols:
            selected_symbols = st.multiselect(
                "Select instruments to compare (up to 5)",
                symbols,
                default=symbols[:3] if len(symbols) >= 3 else symbols,
                max_selections=5,
            )

            if selected_symbols:
                # Get instrument IDs for selected symbols
                selected_ids = [
                    r.get("instrument_id")
                    for r in rankings
                    if r.get("symbol") in selected_symbols
                ]

                # Get normalized performance
                perf_df = get_normalized_performance(con, selected_ids)

                if not perf_df.empty:
                    import plotly.graph_objects as go

                    fig = go.Figure()

                    # Map instrument IDs to symbols for legend
                    id_to_symbol = {
                        r.get("instrument_id"): r.get("symbol")
                        for r in rankings
                    }

                    for col in perf_df.columns:
                        symbol = id_to_symbol.get(col, col)
                        fig.add_trace(go.Scatter(
                            x=perf_df.index,
                            y=perf_df[col],
                            mode="lines",
                            name=symbol,
                            hovertemplate=f"{symbol}: %{{y:.1f}}<extra></extra>"
                        ))

                    fig.update_layout(
                        title="Normalized Performance (Base = 100)",
                        xaxis_title="Date",
                        yaxis_title="Value",
                        legend=dict(orientation="h", yanchor="bottom", y=1.02),
                        height=400,
                    )

                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No historical data available for selected instruments.")

    # Summary Statistics
    st.markdown("---")
    st.subheader("Summary Statistics")

    col1, col2, col3 = st.columns(3)

    # Calculate summary stats
    if "return_1m" in rankings_df.columns:
        valid_returns = rankings_df["return_1m"].dropna()
        if len(valid_returns) > 0:
            with col1:
                best = rankings_df.loc[rankings_df["return_1m"].idxmax()]
                st.metric(
                    "Best 1M Return",
                    f"{best.get('symbol', 'N/A')}",
                    f"{best.get('return_1m', 0) * 100:.1f}%"
                )

            with col2:
                worst = rankings_df.loc[rankings_df["return_1m"].idxmin()]
                st.metric(
                    "Worst 1M Return",
                    f"{worst.get('symbol', 'N/A')}",
                    f"{worst.get('return_1m', 0) * 100:.1f}%"
                )

            with col3:
                avg_return = valid_returns.mean() * 100
                st.metric("Avg 1M Return", f"{avg_return:.1f}%")

    # Info about syncing
    st.markdown("---")
    st.info("💡 To seed or sync instrument data, use the **📦 Instruments** page.")

    render_footer()
