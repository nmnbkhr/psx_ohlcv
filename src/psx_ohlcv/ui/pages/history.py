"""Historical data explorer page."""

from datetime import datetime, timedelta
import streamlit as st

from psx_ohlcv.query import (
    get_ohlcv_range,
    get_symbols_list,
)
from psx_ohlcv.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_history():
    """Display historical OHLCV data and trends."""
    import plotly.graph_objects as go

    from psx_ohlcv.query import (
        get_ohlcv_market_daily,
        get_ohlcv_range,
        get_ohlcv_stats,
        get_ohlcv_symbol_stats,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📚 Historical Data")
        st.caption("End-of-day OHLCV data synced from PSX")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Check data availability
    ohlcv_stats = get_ohlcv_stats(con)

    if ohlcv_stats["total_rows"] == 0:
        st.warning(
            "No OHLCV history data available yet. To populate history:\n\n"
            "1. Run `psxsync sync --all` to fetch historical EOD data\n"
            "2. Or use `psxsync sync SYMBOL` for specific symbols"
        )
        render_footer()
        return

    st.info(
        f"OHLCV data: **{ohlcv_stats['total_rows']:,}** records for "
        f"**{ohlcv_stats['unique_symbols']}** symbols from "
        f"**{ohlcv_stats['min_date']}** to **{ohlcv_stats['max_date']}**"
    )

    # Tabs for different history views
    tab_market, tab_symbol = st.tabs(
        ["📊 Market Daily", "📈 Symbol OHLCV"]
    )

    # =========================================================================
    # Tab 1: Market Daily Aggregates
    # =========================================================================
    with tab_market:
        st.subheader("Market Daily Aggregates")

        st.markdown("""
        Daily market-wide statistics computed from OHLCV data.
        **Gainers** = symbols where close > open for that day.
        """)

        # Date range selector
        col1, col2 = st.columns(2)
        with col1:
            days_back = st.selectbox(
                "Date Range",
                options=["Last 30 days", "Last 90 days", "Last 180 days", "All data"],
                index=0,
                key="market_daily_range",
            )
        with col2:
            pass  # Reserved for future filters

        # Calculate date range
        from datetime import date as date_type
        from datetime import timedelta as td

        today = date_type.today()
        if days_back == "Last 30 days":
            start_date = (today - td(days=30)).isoformat()
        elif days_back == "Last 90 days":
            start_date = (today - td(days=90)).isoformat()
        elif days_back == "Last 180 days":
            start_date = (today - td(days=180)).isoformat()
        else:
            start_date = None

        # Fetch market daily data
        df = get_ohlcv_market_daily(con, start_date=start_date, limit=500)

        if df.empty:
            st.info("No market daily data available for selected range.")
        else:
            # Sort by date ascending for charts
            df = df.sort_values("date", ascending=True)
            st.caption(f"Showing {len(df)} trading days")

            # Chart 1: Market Breadth Over Time
            st.markdown("#### Daily Gainers vs Losers")
            fig_breadth = go.Figure()
            fig_breadth.add_trace(go.Scatter(
                x=df["date"],
                y=df["gainers"],
                mode="lines",
                name="Gainers",
                line={"color": "#00C853", "width": 2},
                fill="tozeroy",
                fillcolor="rgba(0, 200, 83, 0.1)",
            ))
            fig_breadth.add_trace(go.Scatter(
                x=df["date"],
                y=df["losers"],
                mode="lines",
                name="Losers",
                line={"color": "#FF1744", "width": 2},
            ))
            fig_breadth.add_trace(go.Scatter(
                x=df["date"],
                y=df["unchanged"],
                mode="lines",
                name="Unchanged",
                line={"color": "#9E9E9E", "width": 1, "dash": "dot"},
            ))
            fig_breadth.update_layout(
                title="Daily Market Breadth (Gainers vs Losers)",
                xaxis_title="Date",
                yaxis_title="Number of Symbols",
                height=450,
                hovermode="x unified",
                legend={"orientation": "h", "yanchor": "bottom", "y": 1.02},
            )
            st.plotly_chart(fig_breadth, use_container_width=True)

            # Chart 2: Total Volume Over Time
            st.markdown("#### Daily Total Volume")
            fig_volume = go.Figure()
            fig_volume.add_trace(go.Bar(
                x=df["date"],
                y=df["total_volume"],
                name="Total Volume",
                marker_color="#2196F3",
            ))
            fig_volume.update_layout(
                title="Daily Market Volume",
                xaxis_title="Date",
                yaxis_title="Volume",
                height=450,
                hovermode="x unified",
            )
            st.plotly_chart(fig_volume, use_container_width=True)

            # Chart 3: Average Change %
            st.markdown("#### Daily Average Change %")
            fig_chg = go.Figure()
            colors = [
                "#00C853" if v >= 0 else "#FF1744"
                for v in df["avg_change_pct"]
            ]
            fig_chg.add_trace(go.Bar(
                x=df["date"],
                y=df["avg_change_pct"],
                name="Avg Change %",
                marker_color=colors,
            ))
            fig_chg.add_hline(y=0, line_dash="dash", line_color="gray")
            fig_chg.update_layout(
                title="Daily Average Change % (across all symbols)",
                xaxis_title="Date",
                yaxis_title="Avg Change %",
                height=450,
                hovermode="x unified",
            )
            st.plotly_chart(fig_chg, use_container_width=True)

            # Table: Recent daily data
            st.markdown("#### Daily Summary Table")
            display_df = df.tail(30).sort_values("date", ascending=False)
            st.dataframe(
                display_df,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "date": st.column_config.TextColumn("Date"),
                    "total_symbols": st.column_config.NumberColumn("Symbols"),
                    "gainers": st.column_config.NumberColumn("Gainers"),
                    "losers": st.column_config.NumberColumn("Losers"),
                    "unchanged": st.column_config.NumberColumn("Unchanged"),
                    "total_volume": st.column_config.NumberColumn(
                        "Volume", format="%,.0f"
                    ),
                    "avg_change_pct": st.column_config.NumberColumn(
                        "Avg Chg %", format="%.2f"
                    ),
                },
            )

    # =========================================================================
    # Tab 2: Symbol OHLCV History
    # =========================================================================
    with tab_symbol:
        st.subheader("Symbol OHLCV History")

        # Symbol input with suggestions
        symbols_list = get_symbols_list(con, is_active_only=True)
        col1, col2 = st.columns([2, 1])

        with col1:
            symbol_input = st.selectbox(
                "Select Symbol",
                options=[""] + symbols_list,
                index=0,
                key="history_ohlcv_symbol",
                help="Select a symbol to view its OHLCV history",
            )

        with col2:
            # Date range selector
            sym_range_options = [
                "Last 30 days",
                "Last 90 days",
                "Last 180 days",
                "Last 1 year",
                "All data",
            ]
            sym_selected_range = st.selectbox(
                "Date Range",
                options=sym_range_options,
                index=1,  # Default to 90 days
                key="symbol_ohlcv_range",
            )

        if symbol_input:
            # Get symbol stats
            sym_stats = get_ohlcv_symbol_stats(con, symbol_input)

            if sym_stats["total_rows"] == 0:
                st.info(f"No OHLCV history for {symbol_input}.")
            else:
                st.caption(
                    f"{symbol_input}: **{sym_stats['total_rows']}** days from "
                    f"**{sym_stats['min_date']}** to **{sym_stats['max_date']}** | "
                    f"Avg Volume: **{sym_stats['avg_volume']:,.0f}**"
                )

                # Calculate date range
                from datetime import date as date_type
                from datetime import timedelta as td

                today = date_type.today()
                if sym_selected_range == "Last 30 days":
                    start_date = (today - td(days=30)).isoformat()
                elif sym_selected_range == "Last 90 days":
                    start_date = (today - td(days=90)).isoformat()
                elif sym_selected_range == "Last 180 days":
                    start_date = (today - td(days=180)).isoformat()
                elif sym_selected_range == "Last 1 year":
                    start_date = (today - td(days=365)).isoformat()
                else:
                    start_date = None

                # Fetch symbol OHLCV history
                sym_df = get_ohlcv_range(con, symbol_input, start_date=start_date)

                if sym_df.empty:
                    st.info(f"No OHLCV data for {symbol_input} in selected range.")
                else:
                    st.caption(f"Showing {len(sym_df)} trading days")

                    # Chart type toggle
                    chart_type = st.radio(
                        "Chart Type",
                        options=["Candlestick", "Line"],
                        horizontal=True,
                        key="ohlcv_chart_type",
                    )

                    # Chart 1: Price OHLCV
                    st.markdown(f"#### {symbol_input} Price History")

                    if chart_type == "Candlestick":
                        fig_price = go.Figure(data=[go.Candlestick(
                            x=sym_df["date"],
                            open=sym_df["open"],
                            high=sym_df["high"],
                            low=sym_df["low"],
                            close=sym_df["close"],
                            name=symbol_input,
                        )])
                        fig_price.update_layout(
                            title=f"{symbol_input} OHLC",
                            xaxis_title="Date",
                            yaxis_title="Price (Rs.)",
                            height=500,
                            xaxis_rangeslider_visible=False,
                        )
                    else:
                        fig_price = go.Figure()
                        fig_price.add_trace(go.Scatter(
                            x=sym_df["date"],
                            y=sym_df["close"],
                            mode="lines",
                            name="Close",
                            line={"color": "#2196F3", "width": 2},
                        ))
                        fig_price.add_trace(go.Scatter(
                            x=sym_df["date"],
                            y=sym_df["open"],
                            mode="lines",
                            name="Open",
                            line={"color": "#9E9E9E", "width": 1, "dash": "dot"},
                        ))
                        fig_price.update_layout(
                            title=f"{symbol_input} Close Price",
                            xaxis_title="Date",
                            yaxis_title="Price (Rs.)",
                            height=500,
                            hovermode="x unified",
                        )
                    st.plotly_chart(fig_price, use_container_width=True)

                    # Chart 2: Volume
                    st.markdown(f"#### {symbol_input} Volume")
                    # Color bars by price direction
                    vol_colors = [
                        "#00C853" if c >= o else "#FF1744"
                        for o, c in zip(sym_df["open"], sym_df["close"])
                    ]
                    fig_vol = go.Figure()
                    fig_vol.add_trace(go.Bar(
                        x=sym_df["date"],
                        y=sym_df["volume"],
                        name="Volume",
                        marker_color=vol_colors,
                    ))
                    fig_vol.update_layout(
                        title=f"{symbol_input} Daily Volume",
                        xaxis_title="Date",
                        yaxis_title="Volume",
                        height=350,
                    )
                    st.plotly_chart(fig_vol, use_container_width=True)

                    # Table: OHLCV Data
                    st.markdown(f"#### {symbol_input} OHLCV Data")
                    with st.expander("View Data Table", expanded=False):
                        display_sym_df = sym_df.tail(100).sort_values(
                            "date", ascending=False
                        )
                        st.dataframe(
                            display_sym_df,
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "symbol": st.column_config.TextColumn("Symbol"),
                                "date": st.column_config.TextColumn("Date"),
                                "open": st.column_config.NumberColumn(
                                    "Open", format="%.2f"
                                ),
                                "high": st.column_config.NumberColumn(
                                    "High", format="%.2f"
                                ),
                                "low": st.column_config.NumberColumn(
                                    "Low", format="%.2f"
                                ),
                                "close": st.column_config.NumberColumn(
                                    "Close", format="%.2f"
                                ),
                                "volume": st.column_config.NumberColumn(
                                    "Volume", format="%,.0f"
                                ),
                            },
                        )
        else:
            st.info("Select a symbol to view its OHLCV history.")

    render_footer()
