"""Index analytics page."""

import pandas as pd
import streamlit as st

from pakfindata.analytics_phase1 import (
    compute_all_metrics,
    get_normalized_performance,
)
from pakfindata.config import get_db_path
from pakfindata.db import (
    get_index_constituents,
    get_instruments,
    get_ohlcv_instrument,
    sync_index_membership,
)
from pakfindata.sync_instruments import sync_instruments_eod
from pakfindata.ui.charts import (
    make_candlestick,
    make_price_line,
)
from pakfindata.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_indices():
    """Comprehensive Index Analytics - All PSX indices with KPIs."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Index Analytics")
        st.caption("PSX Market Indices - Performance & Trends")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Get all index instruments
    indices = get_instruments(con, instrument_type="INDEX", active_only=True)

    if not indices:
        st.warning("No indices found. Run `pfsync universe seed-phase1` to seed indices.")
        render_footer()
        return

    # =================================================================
    # INDEX OVERVIEW TABLE
    # =================================================================
    st.markdown("---")
    st.subheader("📈 All Indices Performance")

    # Compute metrics for all indices
    all_metrics = []
    for idx in indices:
        metrics = compute_all_metrics(con, idx["instrument_id"])
        if "error" not in metrics:
            metrics["symbol"] = idx["symbol"]
            metrics["name"] = idx.get("name", idx["symbol"])
            metrics["instrument_id"] = idx["instrument_id"]
            all_metrics.append(metrics)

    if all_metrics:
        metrics_df = pd.DataFrame(all_metrics)

        # Get latest close prices
        for i, row in metrics_df.iterrows():
            ohlcv = get_ohlcv_instrument(con, row["instrument_id"], limit=2)
            if not ohlcv.empty:
                latest = ohlcv.sort_values("date", ascending=False).iloc[0]
                metrics_df.at[i, "close"] = latest.get("close", 0)
                if len(ohlcv) > 1:
                    prev = ohlcv.sort_values("date", ascending=False).iloc[1]
                    prev_close = prev.get("close", 0)
                    if prev_close:
                        change_pct = ((latest.get("close", 0) - prev_close) / prev_close) * 100
                        metrics_df.at[i, "change_1d"] = change_pct

        # Display columns
        display_cols = ["symbol", "name", "close", "change_1d", "return_1w", "return_1m", "return_3m", "vol_1m"]
        available_cols = [c for c in display_cols if c in metrics_df.columns]
        display_df = metrics_df[available_cols].copy()

        # Format for display
        if "close" in display_df.columns:
            display_df["close"] = display_df["close"].apply(
                lambda x: f"{x:,.2f}" if pd.notna(x) and x else "N/A"
            )

        pct_cols = ["change_1d", "return_1w", "return_1m", "return_3m", "vol_1m"]
        for col in pct_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].apply(
                    lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A"
                )

        col_names = {
            "symbol": "Symbol",
            "name": "Name",
            "close": "Last",
            "change_1d": "1D %",
            "return_1w": "1W %",
            "return_1m": "1M %",
            "return_3m": "3M %",
            "vol_1m": "Vol 30D",
        }
        display_df.rename(columns=col_names, inplace=True)

        st.dataframe(display_df, use_container_width=True, hide_index=True)

    # =================================================================
    # KPI SUMMARY
    # =================================================================
    st.markdown("---")
    st.subheader("📊 Market Summary")

    if all_metrics:
        # Calculate summary stats
        valid_1m = [m.get("return_1m") for m in all_metrics if m.get("return_1m") is not None]
        valid_1d = [m.get("change_1d") for m in all_metrics if m.get("change_1d") is not None] if "change_1d" in metrics_df.columns else []

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            gainers = len([r for r in valid_1d if r > 0]) if valid_1d else 0
            st.metric("📈 Gainers (1D)", gainers)

        with col2:
            losers = len([r for r in valid_1d if r < 0]) if valid_1d else 0
            st.metric("📉 Losers (1D)", losers)

        with col3:
            if valid_1m:
                avg_1m = sum(valid_1m) / len(valid_1m)
                st.metric("Avg 1M Return", f"{avg_1m:+.2f}%")
            else:
                st.metric("Avg 1M Return", "N/A")

        with col4:
            st.metric("Total Indices", len(indices))

    # =================================================================
    # INDIVIDUAL INDEX DETAIL
    # =================================================================
    st.markdown("---")
    st.subheader("🔍 Index Detail")

    symbol_list = [idx["symbol"] for idx in indices]
    name_map = {idx["symbol"]: idx.get("name", idx["symbol"]) for idx in indices}
    id_map = {idx["symbol"]: idx.get("instrument_id") for idx in indices}

    col1, col2 = st.columns([2, 2])

    with col1:
        selected_symbol = st.selectbox(
            "Select Index",
            symbol_list,
            format_func=lambda x: f"{x} - {name_map.get(x, x)}"
        )

    with col2:
        date_range = st.selectbox(
            "Time Range",
            ["30 Days", "90 Days", "180 Days", "1 Year"],
            index=1
        )

    # Map range to limit
    range_map = {"30 Days": 30, "90 Days": 90, "180 Days": 180, "1 Year": 365}
    limit = range_map.get(date_range, 90)

    if selected_symbol:
        instrument_id = id_map.get(selected_symbol)

        # Get metrics for selected index
        metrics = compute_all_metrics(con, instrument_id)

        # KPI row for selected index
        st.markdown("#### Performance Metrics")
        col1, col2, col3, col4, col5 = st.columns(5)

        with col1:
            ohlcv = get_ohlcv_instrument(con, instrument_id, limit=2)
            if not ohlcv.empty:
                latest = ohlcv.sort_values("date", ascending=False).iloc[0]
                st.metric("Current Value", f"{latest.get('close', 0):,.2f}")
            else:
                st.metric("Current Value", "N/A")

        with col2:
            if not ohlcv.empty and len(ohlcv) > 1:
                prev = ohlcv.sort_values("date", ascending=False).iloc[1]
                change = latest.get("close", 0) - prev.get("close", 0)
                pct = (change / prev.get("close", 1)) * 100 if prev.get("close") else 0
                st.metric("1D Change", f"{change:+,.2f}", f"{pct:+.2f}%")
            else:
                st.metric("1D Change", "N/A")

        with col3:
            ret_1w = metrics.get("return_1w")
            if ret_1w is not None:
                st.metric("1W Return", f"{ret_1w:+.2f}%")
            else:
                st.metric("1W Return", "N/A")

        with col4:
            ret_1m = metrics.get("return_1m")
            if ret_1m is not None:
                st.metric("1M Return", f"{ret_1m:+.2f}%")
            else:
                st.metric("1M Return", "N/A")

        with col5:
            vol = metrics.get("vol_1m")
            if vol is not None:
                st.metric("30D Volatility", f"{vol:.2f}%")
            else:
                st.metric("30D Volatility", "N/A")

        # Chart
        st.markdown("#### Price Chart")
        ohlcv_df = get_ohlcv_instrument(con, instrument_id, limit=limit)

        if not ohlcv_df.empty:
            ohlcv_df = ohlcv_df.sort_values("date")

            # Use candlestick if all OHLC columns exist, else line chart
            has_ohlc = all(c in ohlcv_df.columns for c in ["open", "high", "low", "close"])

            if has_ohlc and len(ohlcv_df) >= 5:
                fig = make_candlestick(
                    ohlcv_df,
                    title=f"{selected_symbol} - {name_map.get(selected_symbol, '')}",
                    date_col="date",
                    show_sma=True,
                )
            else:
                fig = make_price_line(
                    ohlcv_df,
                    title=f"{selected_symbol} - {name_map.get(selected_symbol, '')}",
                    date_col="date",
                    price_col="close",
                    height=400,
                )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No OHLCV data available. Run `pfsync instruments sync-eod` to sync index data.")

        # Constituents table
        constituents = get_index_constituents(con, selected_symbol)
        if constituents:
            st.markdown("#### Constituents ({} symbols)".format(len(constituents)))
            const_df = pd.DataFrame(constituents)[["symbol", "name"]]
            const_df.rename(columns={"symbol": "Symbol", "name": "Name"}, inplace=True)
            st.dataframe(const_df, use_container_width=True, hide_index=True)
        else:
            st.caption(
                "No constituents loaded. Use **Download Index Data → Sync Index Membership** below."
            )

    # =================================================================
    # MULTI-INDEX COMPARISON
    # =================================================================
    st.markdown("---")
    st.subheader("📊 Index Comparison")

    compare_symbols = st.multiselect(
        "Select indices to compare (up to 6)",
        symbol_list,
        default=symbol_list[:4] if len(symbol_list) >= 4 else symbol_list,
        max_selections=6,
    )

    if compare_symbols and len(compare_symbols) >= 2:
        # Get normalized performance
        compare_ids = [id_map.get(s) for s in compare_symbols if id_map.get(s)]

        perf_df = get_normalized_performance(con, compare_ids)

        if not perf_df.empty:
            import plotly.graph_objects as go

            fig = go.Figure()

            # Map IDs back to symbols
            id_to_symbol = {v: k for k, v in id_map.items()}

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

            # Apply theme
            from pakfindata.ui.charts import apply_bloomberg_layout
            apply_bloomberg_layout(fig)

            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No historical data available for comparison.")

    # =================================================================
    # CATEGORY VIEW
    # =================================================================
    st.markdown("---")
    st.subheader("📋 Indices by Category")

    # Group indices by category
    categories = {
        "Main Indices": ["KSE100", "ALLSHR", "KSE30", "KMI30"],
        "Islamic Indices": ["KMIALLSHR", "MII30"],
        "Sector Indices": ["BKTI", "OGTI"],
        "Thematic Indices": ["PSXDIV20", "UPP9", "KSE100PR"],
        "ETF Tracking Indices": ["NITPGI", "NBPPGI", "MZNPI", "JSMFI", "ACI", "JSGBKTI", "HBLTTI"],
    }

    for cat_name, cat_symbols in categories.items():
        # Get indices in this category
        cat_indices = [m for m in all_metrics if m.get("symbol") in cat_symbols]

        if cat_indices:
            with st.expander(f"{cat_name} ({len(cat_indices)} indices)", expanded=False):
                cat_df = pd.DataFrame(cat_indices)
                display_cols = ["symbol", "name", "return_1w", "return_1m", "return_3m", "vol_1m"]
                available_cols = [c for c in display_cols if c in cat_df.columns]
                display_df = cat_df[available_cols].copy()

                pct_cols = ["return_1w", "return_1m", "return_3m", "vol_1m"]
                for col in pct_cols:
                    if col in display_df.columns:
                        display_df[col] = display_df[col].apply(
                            lambda x: f"{x:+.2f}%" if pd.notna(x) else "N/A"
                        )

                display_df.rename(columns={
                    "symbol": "Symbol",
                    "name": "Name",
                    "return_1w": "1W",
                    "return_1m": "1M",
                    "return_3m": "3M",
                    "vol_1m": "Vol",
                }, inplace=True)

                st.dataframe(display_df, use_container_width=True, hide_index=True)

    # =================================================================
    # SYNC SECTION
    # =================================================================
    st.markdown("---")
    with st.expander("Download Index Data", expanded=False):
        col1, col2, col3 = st.columns([2, 2, 2])

        with col1:
            if st.button("Download Index OHLCV → ohlcv_instruments", type="primary", key="idx_sync"):
                with st.spinner("Downloading index OHLCV from PSX..."):
                    summary = sync_instruments_eod(
                        db_path=get_db_path(),
                        instrument_types=["INDEX"],
                        incremental=True,
                    )
                    st.success(
                        f"Downloaded: {summary.ok} indices, {summary.rows_upserted} rows → ohlcv_instruments"
                    )
                    st.rerun()

        with col2:
            if st.button("Sync Index Membership → instrument_membership", key="idx_membership"):
                with st.spinner("Parsing listed_in → instrument_membership..."):
                    result = sync_index_membership(con)
                    st.success(
                        "{} indices, {} memberships synced, {} skipped → instrument_membership".format(
                            result["indices"], result["memberships"], result["skipped"]
                        )
                    )
                    st.rerun()

        with col3:
            st.caption("To seed instruments, use the **Instruments** page.")

    render_footer()
