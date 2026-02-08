"""Regular market (live trading) page."""

import streamlit as st

from psx_ohlcv.analytics import (
    get_current_market_with_sectors,
)
from psx_ohlcv.api_client import get_client
from psx_ohlcv.ui.charts import (
    make_market_breadth_chart,
    make_top_movers_chart,
)
from psx_ohlcv.ui.components.helpers import (
    EXPORTS_DIR,
    render_footer,
    render_market_status_badge,
)


def render_regular_market():
    """Regular market watch - live market data display."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📊 Regular Market Watch")
        st.caption("Live market data • Prices, changes, and volume for all symbols")
    with header_col2:
        render_market_status_badge()

    try:
        from psx_ohlcv.analytics import compute_all_analytics
        from psx_ohlcv.sources.regular_market import (
            fetch_regular_market,
            get_all_current_hashes,
            get_current_market,
            init_regular_market_schema,
            insert_snapshots,
            upsert_current,
        )

        client = get_client()
        con = client.connection  # For write operations (fetch, upsert, snapshots)
        init_regular_market_schema(con)
        client.init_analytics()

        # Initialize session state
        if "rm_fetch_result" not in st.session_state:
            st.session_state.rm_fetch_result = None
        if "rm_fetch_running" not in st.session_state:
            st.session_state.rm_fetch_running = False

        st.markdown("---")

        # Fetch controls
        st.subheader("Fetch / Refresh Data")

        col1, col2, col3 = st.columns([1, 1, 2])

        with col1:
            save_unchanged = st.checkbox(
                "Save all rows",
                value=False,
                help="Save all rows to snapshots (even if unchanged)",
                disabled=st.session_state.rm_fetch_running
            )

        with col2:
            fetch_btn = st.button(
                "🔄 Fetch Market Data"
                if not st.session_state.rm_fetch_running
                else "⏳ Fetching...",
                type="primary",
                disabled=st.session_state.rm_fetch_running,
                help="Fetch latest market data from PSX"
            )

        # Execute fetch
        if fetch_btn and not st.session_state.rm_fetch_running:
            st.session_state.rm_fetch_result = None
            st.session_state.rm_fetch_running = True

            with st.status("Fetching market data...", expanded=True) as status:
                st.write("🔄 Fetching from PSX market-watch...")

                try:
                    df = fetch_regular_market()

                    if df.empty:
                        st.session_state.rm_fetch_result = {
                            "success": False,
                            "error": "No data returned from PSX",
                        }
                        status.update(label="❌ No data returned", state="error")
                    else:
                        # CRITICAL: Load previous hashes BEFORE upsert
                        prev_hashes = get_all_current_hashes(con)

                        # Insert snapshots first (using pre-loaded hashes)
                        snapshots_saved = insert_snapshots(
                            con, df,
                            save_unchanged=save_unchanged,
                            prev_hashes=prev_hashes,
                        )

                        # Then upsert current data
                        rows_upserted = upsert_current(con, df)

                        # Compute analytics
                        ts = df["ts"].iloc[0] if not df.empty else None
                        if ts:
                            compute_all_analytics(con, ts)

                        st.session_state.rm_fetch_result = {
                            "success": True,
                            "symbols": len(df),
                            "upserted": rows_upserted,
                            "snapshots": snapshots_saved,
                        }
                        status.update(
                            label=f"✅ Fetched {len(df)} symbols",
                            state="complete"
                        )

                except Exception as e:
                    st.session_state.rm_fetch_result = {
                        "success": False,
                        "error": str(e),
                    }
                    status.update(label="❌ Fetch failed!", state="error")

                finally:
                    st.session_state.rm_fetch_running = False

        # Display fetch result
        if st.session_state.rm_fetch_result is not None:
            result = st.session_state.rm_fetch_result
            if result["success"]:
                st.success(
                    f"✅ Fetched {result['symbols']} symbols, "
                    f"{result['upserted']} upserted, "
                    f"{result['snapshots']} snapshots saved"
                )
            else:
                st.error(f"❌ Error: {result.get('error', 'Unknown error')}")

        st.markdown("---")

        # Load current market data from database with sector names joined
        df = get_current_market_with_sectors(con)

        if df.empty:
            # Fallback to raw data
            df = get_current_market(con)

        if df.empty:
            st.info(
                "No market data available. Click 'Fetch Market Data' to get "
                "the latest data from PSX."
            )
            render_footer()
            return

        # Market overview using pre-computed analytics
        st.subheader("📈 Market Overview")

        market_analytics = client.get_latest_market_analytics()
        if market_analytics:
            total_symbols = market_analytics.get("total_symbols", len(df))
            gainers = market_analytics.get("gainers_count", 0)
            losers = market_analytics.get("losers_count", 0)
            unchanged = market_analytics.get("unchanged_count", 0)
        else:
            # Calculate from data if analytics not available
            total_symbols = len(df)
            gainers = len(df[df["change_pct"] > 0]) if "change_pct" in df.columns else 0
            losers = len(df[df["change_pct"] < 0]) if "change_pct" in df.columns else 0
            unchanged = (
                len(df[df["change_pct"] == 0]) if "change_pct" in df.columns else 0
            )

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Symbols", total_symbols)
        col2.metric("Gainers", gainers, delta=f"+{gainers}")
        col3.metric("Losers", losers, delta=f"-{losers}", delta_color="inverse")
        col4.metric("Unchanged", unchanged)

        # Market breadth chart and top movers
        if "change_pct" in df.columns:
            col1, col2, col3 = st.columns([1, 1, 1])

            with col1:
                breadth_fig = make_market_breadth_chart(
                    gainers=gainers,
                    losers=losers,
                    unchanged=unchanged,
                    height=300,
                )
                st.plotly_chart(breadth_fig, use_container_width=True)

            with col2:
                # Use analytics table for top gainers
                top_gainers_df = client.get_top_list("gainers", limit=5)
                if top_gainers_df.empty:
                    top_gainers_df = df.nlargest(5, "change_pct")[
                        ["symbol", "change_pct"]
                    ]
                gainers_fig = make_top_movers_chart(
                    top_gainers_df[["symbol", "change_pct"]],
                    title="Top 5 Gainers",
                    chart_type="gainers",
                    height=300,
                )
                st.plotly_chart(gainers_fig, use_container_width=True)
                # Quick links to company analytics
                gainer_symbols = top_gainers_df["symbol"].tolist()[:3]
                gcols = st.columns(len(gainer_symbols))
                for i, sym in enumerate(gainer_symbols):
                    with gcols[i]:
                        if st.button(f"📈 {sym}", key=f"rm_gainer_{sym}"):
                            st.session_state.company_symbol = sym
                            st.session_state.nav_to = "🏢 Company Analytics"
                            st.rerun()

            with col3:
                # Use analytics table for top losers
                top_losers_df = client.get_top_list("losers", limit=5)
                if top_losers_df.empty:
                    top_losers_df = df.nsmallest(5, "change_pct")[
                        ["symbol", "change_pct"]
                    ]
                losers_fig = make_top_movers_chart(
                    top_losers_df[["symbol", "change_pct"]],
                    title="Top 5 Losers",
                    chart_type="losers",
                    height=300,
                )
                st.plotly_chart(losers_fig, use_container_width=True)
                # Quick links to company analytics
                loser_symbols = top_losers_df["symbol"].tolist()[:3]
                lcols = st.columns(len(loser_symbols))
                for i, sym in enumerate(loser_symbols):
                    with lcols[i]:
                        if st.button(f"📉 {sym}", key=f"rm_loser_{sym}"):
                            st.session_state.company_symbol = sym
                            st.session_state.nav_to = "🏢 Company Analytics"
                            st.rerun()

        st.markdown("---")

        # Filters
        st.subheader("🔍 Filter Market Data")

        col1, col2, col3 = st.columns([2, 1, 1])

        with col1:
            search = st.text_input(
                "Search Symbol",
                placeholder="e.g., HBL, OGDC",
                help="Filter by symbol"
            )

        with col2:
            # Use sector_name for filter if available, otherwise sector_code
            if "sector_name" in df.columns and df["sector_name"].notna().any():
                sector_options = sorted(
                    df["sector_name"].dropna().unique().tolist()
                )
                sector_options = [s for s in sector_options if s]  # Remove empty
            elif "sector_code" in df.columns:
                sector_options = sorted(
                    df["sector_code"].dropna().unique().tolist()
                )
            else:
                sector_options = []
            sector_filter = st.selectbox(
                "Sector",
                ["All"] + sector_options,
                help="Filter by sector"
            )

        with col3:
            change_filter = st.selectbox(
                "Change",
                ["All", "Gainers", "Losers", "Unchanged"],
                help="Filter by price change"
            )

        # Apply filters
        filtered_df = df.copy()

        if search:
            filtered_df = filtered_df[
                filtered_df["symbol"].str.contains(search.upper(), na=False)
            ]

        if sector_filter != "All":
            if "sector_name" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["sector_name"] == sector_filter]
            elif "sector_code" in filtered_df.columns:
                filtered_df = filtered_df[filtered_df["sector_code"] == sector_filter]

        if change_filter != "All" and "change_pct" in filtered_df.columns:
            if change_filter == "Gainers":
                filtered_df = filtered_df[filtered_df["change_pct"] > 0]
            elif change_filter == "Losers":
                filtered_df = filtered_df[filtered_df["change_pct"] < 0]
            elif change_filter == "Unchanged":
                filtered_df = filtered_df[filtered_df["change_pct"] == 0]

        st.caption(f"Showing {len(filtered_df)} of {len(df)} symbols")

        st.markdown("---")

        # Display table
        st.subheader("📋 Market Data")

        # Select columns to display - use sector_name only (not sector_code)
        display_cols = [
            "symbol", "status", "sector_name", "listed_in",
            "ldcp", "open", "high", "low", "current",
            "change", "change_pct", "volume", "ts"
        ]
        display_cols = [c for c in display_cols if c in filtered_df.columns]

        st.dataframe(
            filtered_df[display_cols],
            use_container_width=True,
            hide_index=True,
            column_config={
                "symbol": st.column_config.TextColumn("Symbol", width="small"),
                "status": st.column_config.TextColumn("Status", width="small"),
                "sector_name": st.column_config.TextColumn("Sector", width="medium"),
                "listed_in": st.column_config.TextColumn("Index", width="small"),
                "ldcp": st.column_config.NumberColumn("LDCP", format="%.2f"),
                "open": st.column_config.NumberColumn("Open", format="%.2f"),
                "high": st.column_config.NumberColumn("High", format="%.2f"),
                "low": st.column_config.NumberColumn("Low", format="%.2f"),
                "current": st.column_config.NumberColumn("Current", format="%.2f"),
                "change": st.column_config.NumberColumn("Change", format="%.2f"),
                "change_pct": st.column_config.NumberColumn("Change %", format="%.2f"),
                "volume": st.column_config.NumberColumn("Volume", format="%d"),
                "ts": st.column_config.TextColumn("Timestamp"),
            }
        )

        st.markdown("---")

        # Export options
        col1, col2 = st.columns(2)

        with col1:
            st.download_button(
                "⬇️ Download CSV",
                filtered_df.to_csv(index=False),
                "regular_market.csv",
                "text/csv",
                help="Download market data to your computer"
            )

        with col2:
            if st.button(
                "💾 Export to /exports/regular_market.csv",
                help="Save to server exports directory"
            ):
                EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
                export_path = EXPORTS_DIR / "regular_market.csv"
                filtered_df.to_csv(export_path, index=False)
                st.success(f"Exported to: {export_path}")

    except ImportError:
        st.error(
            "Regular market module not found. "
            "Make sure psx_ohlcv.sources.regular_market is installed."
        )
    except Exception as e:
        st.error(f"Error: {e}")

    render_footer()
