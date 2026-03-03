"""Instruments listing page."""

import pandas as pd
import streamlit as st

from pakfindata.analytics_phase1 import compute_all_metrics
from pakfindata.config import get_db_path
from pakfindata.db import (
    get_eod_ohlcv,
    get_instruments,
    get_ohlcv_instrument,
)
from pakfindata.sync_instruments import sync_instruments_eod
from pakfindata.ui.charts import make_price_line
from pakfindata.ui.components.helpers import (
    get_cached_connection,
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_instruments():
    """Browse and explore ETFs, REITs, and Indexes."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📦 Instruments Browser")
        st.caption("ETFs, REITs, and Indexes - Phase 1 Universe")
    with header_col2:
        render_market_status_badge()

    con = get_connection()

    # Instrument Type Filter
    col1, col2, col3 = st.columns([2, 2, 4])

    with col1:
        inst_type = st.selectbox(
            "Instrument Type",
            ["ALL", "ETF", "REIT", "INDEX"],
            index=0,
        )

    with col2:
        active_only = st.checkbox("Active Only", value=True)

    # Get instruments
    type_filter = None if inst_type == "ALL" else inst_type
    instruments = get_instruments(con, instrument_type=type_filter, active_only=active_only)

    if not instruments:
        st.warning("No instruments found. Run `pfsync universe seed-phase1` to seed the instrument universe.")
        render_footer()
        return

    # Summary metrics
    etf_count = len([i for i in instruments if i.get("instrument_type") == "ETF"])
    reit_count = len([i for i in instruments if i.get("instrument_type") == "REIT"])
    index_count = len([i for i in instruments if i.get("instrument_type") == "INDEX"])

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Instruments", len(instruments))
    with col2:
        st.metric("ETFs", etf_count)
    with col3:
        st.metric("REITs", reit_count)
    with col4:
        st.metric("Indexes", index_count)

    st.markdown("---")

    # Instrument Table
    st.subheader("Instrument List")

    # Convert to DataFrame for display
    df = pd.DataFrame(instruments)
    display_cols = ["symbol", "name", "instrument_type", "source", "is_active"]
    if all(col in df.columns for col in display_cols):
        display_df = df[display_cols].copy()
        display_df.columns = ["Symbol", "Name", "Type", "Source", "Active"]
        display_df["Active"] = display_df["Active"].apply(lambda x: "✓" if x else "✗")
        st.dataframe(display_df, use_container_width=True, hide_index=True)

    st.markdown("---")

    # Individual Instrument Viewer
    st.subheader("Instrument Detail")

    symbol_list = [inst["symbol"] for inst in instruments]
    selected_symbol = st.selectbox("Select Instrument", symbol_list)

    if selected_symbol:
        # Find the instrument
        selected_inst = next((i for i in instruments if i["symbol"] == selected_symbol), None)

        if selected_inst:
            col1, col2 = st.columns([1, 2])

            with col1:
                st.markdown(f"**Symbol:** {selected_inst['symbol']}")
                st.markdown(f"**Name:** {selected_inst.get('name', 'N/A')}")
                st.markdown(f"**Type:** {selected_inst.get('instrument_type', 'N/A')}")
                st.markdown(f"**Source:** {selected_inst.get('source', 'N/A')}")

                # Compute metrics if available
                instrument_id = selected_inst.get("instrument_id")
                if instrument_id:
                    metrics = compute_all_metrics(con, instrument_id)
                    if "error" not in metrics:
                        st.markdown("**Performance Metrics:**")
                        if metrics.get("return_1m"):
                            ret_color = "green" if metrics["return_1m"] > 0 else "red"
                            st.markdown(f"- 1M Return: :{ret_color}[{metrics['return_1m']:.2f}%]")
                        if metrics.get("return_3m"):
                            ret_color = "green" if metrics["return_3m"] > 0 else "red"
                            st.markdown(f"- 3M Return: :{ret_color}[{metrics['return_3m']:.2f}%]")
                        if metrics.get("vol_1m"):
                            st.markdown(f"- 30D Volatility: {metrics['vol_1m']:.2f}%")

            with col2:
                # Get OHLCV data for chart
                # Try eod_ohlcv first, fall back to ohlcv_instruments
                symbol = selected_inst.get("symbol")
                instrument_id = selected_inst.get("instrument_id")

                ohlcv_df = None
                # Try eod_ohlcv first (equities, ETFs, REITs via pfsync eod)
                if symbol:
                    ohlcv_df = get_eod_ohlcv(con, symbol=symbol, limit=90)

                # Fall back to ohlcv_instruments (indices, legacy sync)
                if (ohlcv_df is None or ohlcv_df.empty) and instrument_id:
                    ohlcv_df = get_ohlcv_instrument(con, instrument_id, limit=90)

                if ohlcv_df is not None and not ohlcv_df.empty:
                    ohlcv_df = ohlcv_df.sort_values("date")
                    fig = make_price_line(
                        ohlcv_df,
                        date_col="date",
                        price_col="close",
                        title=f"{selected_symbol} - Last 90 Days"
                    )
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info(f"No OHLCV data available. Run `pfsync eod {symbol}` to sync data.")

    # Sync Section
    st.markdown("---")
    with st.expander("Sync Instrument Data", expanded=False):
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            if st.button("Seed Universe", type="secondary", key="inst_seed"):
                with st.spinner("Seeding instrument universe..."):
                    from pakfindata.sources.instrument_universe import seed_universe
                    result = seed_universe(get_cached_connection())
                    totals = result.get('totals', {})
                    st.success(
                        f"Seeded {totals.get('inserted', 0)} instruments "
                        f"(Failed: {totals.get('failed', 0)})"
                    )
                    st.rerun()

        with col2:
            sync_types = st.multiselect(
                "Types to Sync",
                ["ETF", "REIT", "INDEX"],
                default=["ETF", "REIT", "INDEX"],
                key="inst_sync_types"
            )

        with col3:
            incremental = st.checkbox("Incremental", value=True, key="inst_incr")

        with col4:
            if st.button("Sync OHLCV", type="primary", key="inst_sync"):
                if sync_types:
                    with st.spinner(f"Syncing {', '.join(sync_types)}..."):
                        summary = sync_instruments_eod(
                            db_path=get_db_path(),
                            instrument_types=sync_types,
                            incremental=incremental,
                        )
                        st.success(
                            f"Sync complete: {summary.ok} OK, "
                            f"{summary.failed} failed, "
                            f"{summary.rows_upserted} rows"
                        )
                        st.rerun()
                else:
                    st.warning("Select at least one instrument type to sync.")

    render_footer()
