"""Intraday trend analysis page."""

import streamlit as st

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
    st_autorefresh = None

from psx_ohlcv.config import get_db_path
from psx_ohlcv.query import (
    get_intraday_latest,
    get_intraday_stats,
    get_symbols_list,
)
from psx_ohlcv.services import (
    is_service_running,
    read_status as read_service_status,
)
from psx_ohlcv.sync import sync_intraday
from psx_ohlcv.ui.charts import (
    make_intraday_chart,
    make_volume_chart,
)
from psx_ohlcv.ui.components.helpers import (
    EXPORTS_DIR,
    format_volume,
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_intraday():
    """Intraday price trend visualization and sync."""
    # =================================================================
    # AUTO-REFRESH WHEN SERVICE IS RUNNING
    # =================================================================
    service_running, service_pid = is_service_running()
    service_status = read_service_status()

    # Auto-refresh every 60 seconds if service is running and autorefresh is available
    if service_running and HAS_AUTOREFRESH and st_autorefresh:
        # Refresh every 60 seconds (60000 ms)
        count = st_autorefresh(interval=60000, limit=None, key="intraday_autorefresh")

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2, header_col3 = st.columns([2, 1, 1])
    with header_col1:
        st.markdown("## ⏱ Intraday Trend")
        st.caption("Live intraday price movements and volume throughout the trading day")
    with header_col2:
        render_market_status_badge()
    with header_col3:
        # Show service status
        if service_running:
            st.success("🟢 Auto-Sync ON")
            if service_status.last_run_at:
                last_sync = service_status.last_run_at[:19]
                st.caption(f"Last: {last_sync}")
        else:
            st.info("🔴 Auto-Sync OFF")
            st.caption("Start service on Data Sync page")

    # Initialize session state for intraday sync
    if "intraday_sync_result" not in st.session_state:
        st.session_state.intraday_sync_result = None
    if "intraday_sync_running" not in st.session_state:
        st.session_state.intraday_sync_running = False

    try:
        con = get_connection()

        # Load symbols for suggestions
        symbols = get_symbols_list(con)

        if not symbols:
            st.warning("No symbols found. Run `psxsync symbols refresh` first.")
            render_footer()
            return

        st.markdown("---")

        # Symbol selection
        col1, col2 = st.columns([2, 1])

        with col1:
            symbol_input = st.text_input(
                "Enter Symbol",
                value="OGDC",
                placeholder="e.g., HBL, OGDC, MCB",
                help="Enter a stock symbol to view intraday data"
            ).strip().upper()

        with col2:
            selected_from_list = st.selectbox(
                "Or select from list",
                [""] + symbols,
                index=0,
                help="Select a symbol from the dropdown"
            )

        selected_symbol = selected_from_list if selected_from_list else symbol_input

        if not selected_symbol:
            st.info("Enter or select a symbol to view intraday data.")
            render_footer()
            return

        if selected_symbol not in symbols:
            st.warning(
                f"Symbol '{selected_symbol}' not found in database. "
                "It may be invalid or you need to refresh symbols."
            )

        st.markdown("---")

        # Sync controls
        st.subheader("Fetch / Refresh Data")

        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            incremental_mode = st.checkbox(
                "Incremental",
                value=True,
                help="Only fetch new data since last sync",
                disabled=st.session_state.intraday_sync_running
            )

        with col2:
            max_rows = st.number_input(
                "Max Rows",
                min_value=100,
                max_value=5000,
                value=2000,
                step=100,
                help="Maximum rows to fetch from API",
                disabled=st.session_state.intraday_sync_running
            )

        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            fetch_btn = st.button(
                "🔄 Fetch / Refresh Intraday"
                if not st.session_state.intraday_sync_running
                else "⏳ Fetching...",
                type="primary",
                disabled=st.session_state.intraday_sync_running,
                help=f"Fetch intraday data for {selected_symbol}"
            )

        with col2:
            if st.session_state.intraday_sync_running:
                st.warning("Fetching...")

        # Execute intraday sync
        if fetch_btn and not st.session_state.intraday_sync_running:
            st.session_state.intraday_sync_result = None
            st.session_state.intraday_sync_running = True

            with st.status(
                f"Fetching intraday data for {selected_symbol}...",
                expanded=True
            ) as status:
                st.write(f"🔄 Fetching intraday data for {selected_symbol}...")

                try:
                    summary = sync_intraday(
                        db_path=get_db_path(),
                        symbol=selected_symbol,
                        incremental=incremental_mode,
                        max_rows=max_rows,
                    )

                    st.session_state.intraday_sync_result = {
                        "success": summary.error is None,
                        "summary": summary,
                    }

                    if summary.error:
                        status.update(
                            label=f"❌ Failed: {summary.error}", state="error"
                        )
                    else:
                        status.update(
                            label=f"✅ Fetched {summary.rows_upserted} rows",
                            state="complete"
                        )

                except Exception as e:
                    st.session_state.intraday_sync_result = {
                        "success": False,
                        "error": str(e),
                    }
                    status.update(label="❌ Fetch failed!", state="error")

                finally:
                    st.session_state.intraday_sync_running = False

        # Display sync result
        if st.session_state.intraday_sync_result is not None:
            result = st.session_state.intraday_sync_result
            if result["success"]:
                summary = result["summary"]
                st.success(
                    f"✅ Fetched {summary.rows_upserted} rows for {summary.symbol}"
                )
                if summary.newest_ts:
                    st.caption(f"Latest timestamp: {summary.newest_ts}")
            else:
                error_msg = result.get("error") or result.get("summary", {}).error
                st.error(f"❌ Error: {error_msg}")

        st.markdown("---")

        # Display controls
        col1, col2 = st.columns(2)
        with col1:
            limit = st.slider(
                "Display Limit",
                min_value=200,
                max_value=5000,
                value=500,
                step=100,
                help="Number of rows to display (most recent)"
            )

        with col2:
            stats = get_intraday_stats(con, selected_symbol)
            if stats["row_count"] > 0:
                st.metric(
                    "Total Rows",
                    f"{stats['row_count']:,}",
                    help="Total intraday records for this symbol"
                )
                st.caption(f"Range: {stats['min_ts']} to {stats['max_ts']}")
            else:
                st.info("No intraday data yet. Click 'Fetch / Refresh Intraday'.")

        st.markdown("---")

        # Fetch and display intraday data
        df = get_intraday_latest(con, selected_symbol, limit=limit)

        if df.empty:
            st.info(
                f"No intraday data for {selected_symbol}. "
                "Click 'Fetch / Refresh Intraday' to fetch data."
            )
            render_footer()
            return

        # Latest price stats
        st.subheader(f"{selected_symbol} - Intraday Stats")

        latest = df.iloc[-1]
        first = df.iloc[0]
        change = latest["close"] - first["open"] if first["open"] else 0
        change_pct = (change / first["open"]) * 100 if first["open"] else 0

        # Calculate VWAP (Volume Weighted Average Price)
        # VWAP = Σ(Typical Price × Volume) / Σ(Volume)
        df["typical_price"] = (df["high"] + df["low"] + df["close"]) / 3
        df["tp_volume"] = df["typical_price"] * df["volume"]
        cumulative_tp_volume = df["tp_volume"].cumsum()
        cumulative_volume = df["volume"].cumsum()
        df["vwap"] = cumulative_tp_volume / cumulative_volume
        vwap = df["vwap"].iloc[-1] if not df["vwap"].empty else None

        # Session stats
        session_high = df["high"].max()
        session_low = df["low"].min()
        total_volume = df["volume"].sum()

        # First row: Price metrics
        col1, col2, col3, col4, col5, col6 = st.columns(6)
        change_str = f"{change:+.2f} ({change_pct:+.1f}%)"
        col1.metric(
            "Latest Close",
            f"PKR {latest['close']:.2f}" if latest["close"] else "N/A",
            change_str if first["open"] else None,
            help="Most recent close price"
        )
        col2.metric(
            "Session Open",
            f"PKR {first['open']:.2f}" if first["open"] else "N/A",
            help="Session opening price"
        )
        col3.metric(
            "Session High",
            f"PKR {session_high:.2f}" if session_high else "N/A",
            help="Highest price in session"
        )
        col4.metric(
            "Session Low",
            f"PKR {session_low:.2f}" if session_low else "N/A",
            help="Lowest price in session"
        )
        col5.metric(
            "📊 VWAP",
            f"PKR {vwap:.2f}" if vwap else "N/A",
            help="Volume Weighted Average Price - institutional benchmark"
        )
        col6.metric(
            "Total Volume",
            format_volume(total_volume) if total_volume else "N/A",
            help="Total session volume"
        )

        # VWAP context
        if vwap and latest["close"]:
            vwap_diff = latest["close"] - vwap
            vwap_pct = (vwap_diff / vwap) * 100
            if vwap_diff > 0:
                st.caption(f"📍 Latest: {latest['ts']} | Price **above** VWAP by Rs.{vwap_diff:.2f} ({vwap_pct:+.2f}%) - Bullish bias")
            else:
                st.caption(f"📍 Latest: {latest['ts']} | Price **below** VWAP by Rs.{abs(vwap_diff):.2f} ({vwap_pct:+.2f}%) - Bearish bias")
        else:
            st.caption(f"📍 Latest: {latest['ts']}")

        st.markdown("---")

        # Intraday chart using the helper
        fig = make_intraday_chart(
            df,
            title=f"{selected_symbol} - Intraday",
            ts_col="ts",
            height=650,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Close Price Trend with VWAP overlay
        st.subheader("📈 Price & VWAP")
        import plotly.graph_objects as go

        chart_df = df.sort_values("ts", ascending=True)
        fig_price = go.Figure()

        # Close price line
        fig_price.add_trace(go.Scatter(
            x=chart_df["ts"],
            y=chart_df["close"],
            mode="lines",
            name="Close",
            line={"color": "#2196F3", "width": 2},
        ))

        # VWAP line
        fig_price.add_trace(go.Scatter(
            x=chart_df["ts"],
            y=chart_df["vwap"],
            mode="lines",
            name="VWAP",
            line={"color": "#FF9800", "width": 2, "dash": "dash"},
        ))

        # Add horizontal line at current VWAP
        if vwap:
            fig_price.add_hline(
                y=vwap,
                line_dash="dot",
                line_color="rgba(255,152,0,0.5)",
                annotation_text=f"VWAP: {vwap:.2f}",
                annotation_position="right"
            )

        fig_price.update_layout(
            title=f"{selected_symbol} - Price vs VWAP",
            xaxis_title="Time",
            yaxis_title="Price (PKR)",
            height=400,
            hovermode="x unified",
            legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "xanchor": "right", "x": 1},
            margin={"l": 60, "r": 20, "t": 60, "b": 60},
        )
        st.plotly_chart(fig_price, use_container_width=True)

        st.caption("**VWAP** (Volume Weighted Average Price) = institutional benchmark. "
                   "Price above VWAP suggests bullish bias; below suggests bearish bias.")

        # Volume chart
        st.subheader("📊 Volume")
        fig_vol = make_volume_chart(df, date_col="ts", height=250)
        st.plotly_chart(fig_vol, use_container_width=True)

        st.markdown("---")

        # Data table
        st.subheader(f"Data Preview (last {min(50, len(df))} rows)")

        preview_df = df.sort_values("ts", ascending=False).head(50)
        st.dataframe(
            preview_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "symbol": st.column_config.TextColumn("Symbol"),
                "ts": st.column_config.TextColumn("Timestamp"),
                "open": st.column_config.NumberColumn("Open", format="%.2f"),
                "high": st.column_config.NumberColumn("High", format="%.2f"),
                "low": st.column_config.NumberColumn("Low", format="%.2f"),
                "close": st.column_config.NumberColumn("Close", format="%.2f"),
                "volume": st.column_config.NumberColumn("Volume", format="%d"),
            }
        )

        st.markdown("---")

        # Export options
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                "⬇️ Download CSV",
                df.to_csv(index=False),
                f"{selected_symbol}_intraday.csv",
                "text/csv",
                help="Download intraday data to your computer"
            )

        with col2:
            if st.button(
                f"💾 Export to /exports/{selected_symbol}_intraday.csv",
                help="Save to server exports directory"
            ):
                EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
                export_path = EXPORTS_DIR / f"{selected_symbol}_intraday.csv"
                df.to_csv(export_path, index=False)
                st.success(f"Exported to: {export_path}")

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()
