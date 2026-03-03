"""Candlestick chart explorer page."""

from datetime import datetime, timedelta
import streamlit as st

from pakfindata.api_client import get_client
from pakfindata.ui.charts import make_candlestick
from pakfindata.ui.components.helpers import (
    EXPORTS_DIR,
    OHLCV_TOOLTIPS,
    render_footer,
    render_market_status_badge,
)


def render_candlestick():
    """Candlestick chart explorer with SMA overlays."""

    try:
        client = get_client()

        # =================================================================
        # HEADER
        # =================================================================
        header_col1, header_col2 = st.columns([3, 1])
        with header_col1:
            st.markdown("## 📈 Candlestick Explorer")
            st.caption("Technical analysis with OHLCV charts and moving averages")
        with header_col2:
            render_market_status_badge()

        # Load symbols
        symbols = client.get_symbols(active_only=False)
        if not symbols:
            st.warning("No symbols found. Run `pfsync symbols refresh` first.")
            render_footer()
            return

        st.markdown("---")

        # =================================================================
        # CONTROLS - Compact toolbar style
        # =================================================================
        ctrl_col1, ctrl_col2, ctrl_col3, ctrl_col4 = st.columns([3, 1, 1, 1])

        with ctrl_col1:
            selected = st.selectbox(
                "Symbol",
                symbols,
                index=0,
                label_visibility="collapsed",
                help="Choose a symbol to explore"
            )

        with ctrl_col2:
            range_options = {"1M": 30, "3M": 90, "6M": 180, "1Y": 365, "All": None}
            range_choice = st.selectbox(
                "Range",
                list(range_options.keys()),
                index=3,
                label_visibility="collapsed"
            )

        with ctrl_col3:
            show_sma = st.checkbox("SMA", value=True, help="Show SMA(20) and SMA(50)")

        with ctrl_col4:
            days_old, latest_date = client.get_data_freshness()
            if latest_date:
                st.caption(f"📅 {latest_date}")

        # Calculate date range
        end_date = datetime.now().strftime("%Y-%m-%d")
        days = range_options[range_choice]
        if days:
            start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        else:
            start_date = None

        # Fetch OHLCV data
        df = client.get_ohlcv_range(selected, start_date=start_date, end_date=end_date)

        if df.empty:
            st.warning(
                f"No data for {selected}. Run `pfsync sync --symbols {selected}`."
            )
            render_footer()
            return

        # Price stats
        st.markdown("---")
        latest = df.iloc[-1]
        prev = df.iloc[-2] if len(df) > 1 else latest
        change = latest["close"] - prev["close"]
        change_pct = (change / prev["close"]) * 100 if prev["close"] else 0

        col1, col2, col3, col4, col5 = st.columns(5)
        change_str = f"{change:+.2f} ({change_pct:+.1f}%)"
        col1.metric(
            "Close",
            f"PKR {latest['close']:.2f}",
            change_str,
            help=OHLCV_TOOLTIPS["close"]
        )
        col2.metric("Open", f"PKR {latest['open']:.2f}", help=OHLCV_TOOLTIPS["open"])
        col3.metric("High", f"PKR {latest['high']:.2f}", help=OHLCV_TOOLTIPS["high"])
        col4.metric("Low", f"PKR {latest['low']:.2f}", help=OHLCV_TOOLTIPS["low"])
        col5.metric(
            "Volume",
            f"{int(latest['volume']):,}",
            help=OHLCV_TOOLTIPS["volume"]
        )

        st.caption(f"📍 Last close: **PKR {latest['close']:.2f}** on {latest['date']}")

        st.markdown("---")

        # Candlestick chart using the helper
        fig = make_candlestick(
            df,
            title=f"{selected} - OHLC ({range_choice})",
            date_col="date",
            show_sma=show_sma,
            height=650,
        )
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("---")

        # Data preview
        st.subheader("Data Preview (last 20 rows)")
        preview_df = df.sort_values("date", ascending=False).head(20)
        st.dataframe(
            preview_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "date": st.column_config.DateColumn("Date", format="YYYY-MM-DD"),
                "open": st.column_config.NumberColumn(
                    "Open", format="%.2f", help=OHLCV_TOOLTIPS["open"]
                ),
                "high": st.column_config.NumberColumn(
                    "High", format="%.2f", help=OHLCV_TOOLTIPS["high"]
                ),
                "low": st.column_config.NumberColumn(
                    "Low", format="%.2f", help=OHLCV_TOOLTIPS["low"]
                ),
                "close": st.column_config.NumberColumn(
                    "Close", format="%.2f", help=OHLCV_TOOLTIPS["close"]
                ),
                "volume": st.column_config.NumberColumn(
                    "Volume", format="%d", help=OHLCV_TOOLTIPS["volume"]
                ),
            }
        )

        # Export buttons
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                "⬇️ Download CSV",
                df.to_csv(index=False),
                f"{selected}_ohlcv.csv",
                "text/csv",
                help="Download data to your computer"
            )

        with col2:
            if st.button(
                f"💾 Export to /exports/{selected}_ohlcv.csv",
                help="Save to server exports directory"
            ):
                EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
                export_path = EXPORTS_DIR / f"{selected}_ohlcv.csv"
                df.to_csv(export_path, index=False)
                st.success(f"Exported to: {export_path}")

    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()
