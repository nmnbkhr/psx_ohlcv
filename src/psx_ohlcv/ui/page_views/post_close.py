"""Post-close turnover page — download and manage turnover data."""

import streamlit as st

from psx_ohlcv.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_post_close():
    """Standalone page for post-close turnover data."""
    from datetime import date as date_type
    from datetime import timedelta as td

    from psx_ohlcv.db.repositories.post_close import (
        get_dates_missing_turnover,
        get_post_close,
        get_post_close_dates,
        get_post_close_stats,
    )
    from psx_ohlcv.sources.market_summary import fetch_post_close

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 💰 Post Close Turnover")
        st.caption("Download daily turnover data from PSX DPS post_close files")
    with header_col2:
        render_market_status_badge()

    with st.expander("ℹ️ About Post Close Files"):
        st.markdown("""
        Download daily turnover (PKR traded value) per symbol from PSX DPS.
        This data supplements the market summary OHLCV files with traded value.

        **Source:** `https://dps.psx.com.pk/download/post_close/{date}.Z`
        **Format:** ZIP file containing pipe-delimited text: `symbol|company_name|volume|turnover|*`

        Data is stored in the `post_close_turnover` table and also synced to
        `eod_ohlcv.turnover` and `futures_eod.turnover` for convenience.
        """)

    con = get_connection()

    # Stats
    stats = get_post_close_stats(con)
    missing_dates = get_dates_missing_turnover(con)

    st.subheader("Statistics")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Records", f"{stats['total_rows']:,}")
    with col2:
        st.metric("Dates Loaded", stats["total_dates"])
    with col3:
        st.metric("Unique Symbols", stats["unique_symbols"])
    with col4:
        st.metric("Dates Missing", len(missing_dates))
    with col5:
        if stats["min_date"] and stats["max_date"]:
            st.metric("Latest", stats["max_date"])
        else:
            st.metric("Latest", "—")

    if stats["min_date"] and stats["max_date"]:
        st.caption(f"Date range: {stats['min_date']} to {stats['max_date']}")

    st.markdown("---")

    # Tabs
    tab_single, tab_range, tab_backfill, tab_history = st.tabs([
        "📅 Single Day", "📆 Date Range", "🔄 Backfill", "📋 History"
    ])

    # =========================================================================
    # Tab 1: Single Day
    # =========================================================================
    with tab_single:
        st.subheader("Download Single Day")

        col1, col2 = st.columns([2, 1])
        with col1:
            single_date = st.date_input(
                "Select date",
                value=date_type.today() - td(days=1),
                max_value=date_type.today(),
                key="pc_single_date",
            )
        with col2:
            single_force = st.checkbox(
                "Force re-download",
                value=False,
                key="pc_single_force",
                help="Re-download even if already exists in table",
            )

        if st.button("Download Turnover", key="pc_single_download", type="primary"):
            date_str = str(single_date)

            # Check if already exists (unless force)
            if not single_force:
                existing = get_post_close_dates(con)
                if date_str in existing:
                    st.info(f"Turnover for {date_str} already loaded. Use force to re-download.")
                    st.stop()

            with st.spinner(f"Downloading turnover for {single_date}..."):
                try:
                    result = fetch_post_close(date_str, con)
                    if result["status"] == "ok":
                        st.success(
                            f"Downloaded {result['date']}: "
                            f"{result['total_records']} symbols, "
                            f"{result['stored']} stored, "
                            f"synced eod={result['eod_updated']} futures={result['futures_updated']}"
                        )
                    elif result["status"] == "missing":
                        st.warning(f"No post_close file for {result['date']} (404)")
                    elif result["status"] == "empty":
                        st.warning(f"Post_close file for {result['date']} was empty")
                    else:
                        st.error(f"Failed: {result['status']}")
                except Exception as e:
                    st.error(f"Error: {e}")

    # =========================================================================
    # Tab 2: Date Range
    # =========================================================================
    with tab_range:
        st.subheader("Download Date Range")

        col1, col2 = st.columns(2)
        with col1:
            range_start = st.date_input(
                "Start date",
                value=date_type.today() - td(days=30),
                max_value=date_type.today(),
                key="pc_range_start",
            )
        with col2:
            range_end = st.date_input(
                "End date",
                value=date_type.today() - td(days=1),
                max_value=date_type.today(),
                key="pc_range_end",
            )

        col1, col2 = st.columns(2)
        with col1:
            range_skip_weekends = st.checkbox(
                "Skip weekends",
                value=True,
                key="pc_range_skip_weekends",
            )
        with col2:
            range_force = st.checkbox(
                "Force re-download",
                value=False,
                key="pc_range_force",
            )

        from psx_ohlcv.range_utils import iter_dates
        expected_dates = list(iter_dates(
            range_start, range_end, skip_weekends=range_skip_weekends
        ))

        # Filter out already-loaded dates unless force
        if not range_force:
            existing = set(get_post_close_dates(con))
            to_process = [d for d in expected_dates if str(d) not in existing]
        else:
            to_process = expected_dates

        st.caption(
            f"{len(expected_dates)} dates in range, "
            f"{len(to_process)} to download"
        )

        if st.button("Download Range", key="pc_range_download", type="primary"):
            if range_start > range_end:
                st.error("Start date must be before end date")
            elif not to_process:
                st.info("All dates in range already loaded.")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                ok_count = 0
                miss_count = 0
                fail_count = 0

                for i, d in enumerate(to_process):
                    progress_bar.progress((i + 1) / len(to_process))
                    try:
                        result = fetch_post_close(str(d), con)
                        if result["status"] == "ok":
                            ok_count += 1
                        elif result["status"] == "missing":
                            miss_count += 1
                        else:
                            fail_count += 1
                        status_text.text(
                            f"{result['date']}: {result['status']} | "
                            f"OK: {ok_count}, Missing: {miss_count}, Failed: {fail_count}"
                        )
                    except Exception as e:
                        fail_count += 1
                        status_text.text(f"{d}: error — {e}")

                st.success(
                    f"Completed: {ok_count} OK, "
                    f"{miss_count} missing (404), {fail_count} failed"
                )

    # =========================================================================
    # Tab 3: Backfill
    # =========================================================================
    with tab_backfill:
        st.subheader("Backfill Missing Dates")
        st.caption(
            "Find dates that have EOD data but no post-close turnover, "
            "and download turnover for them."
        )

        if missing_dates:
            st.info(
                f"{len(missing_dates)} dates with EOD data but no turnover: "
                f"{missing_dates[-1]} to {missing_dates[0]}"
            )

            backfill_limit = st.number_input(
                "Max dates to process",
                min_value=1,
                max_value=len(missing_dates),
                value=min(30, len(missing_dates)),
                key="pc_backfill_limit",
                help="Process the most recent dates first",
            )

            if st.button(
                f"Backfill {backfill_limit} dates",
                key="pc_backfill_run",
                type="primary",
            ):
                to_process = missing_dates[:backfill_limit]
                progress_bar = st.progress(0)
                status_text = st.empty()
                ok_count = 0
                miss_count = 0
                fail_count = 0

                for i, date_str in enumerate(to_process):
                    progress_bar.progress((i + 1) / len(to_process))
                    try:
                        result = fetch_post_close(date_str, con)
                        if result["status"] == "ok":
                            ok_count += 1
                        elif result["status"] == "missing":
                            miss_count += 1
                        else:
                            fail_count += 1
                        status_text.text(
                            f"{date_str}: {result['status']} | "
                            f"OK: {ok_count}, Missing: {miss_count}, Failed: {fail_count}"
                        )
                    except Exception as e:
                        fail_count += 1
                        status_text.text(f"{date_str}: error — {e}")

                st.success(
                    f"Backfill complete: {ok_count} OK, "
                    f"{miss_count} missing (404), {fail_count} failed"
                )
        else:
            st.success("All dates with EOD data have turnover loaded.")

    # =========================================================================
    # Tab 4: History
    # =========================================================================
    with tab_history:
        st.subheader("Turnover Data")

        dates = get_post_close_dates(con)
        if dates:
            selected_date = st.selectbox(
                "Select date",
                options=dates,
                key="pc_history_date",
            )

            df = get_post_close(con, date=selected_date, limit=500)
            if not df.empty:
                import pandas as pd

                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Symbols", len(df))
                with col2:
                    total_turnover = df["turnover"].sum()
                    if total_turnover >= 1e9:
                        st.metric("Total Turnover", f"Rs. {total_turnover / 1e9:.2f}B")
                    else:
                        st.metric("Total Turnover", f"Rs. {total_turnover / 1e6:.1f}M")
                with col3:
                    st.metric("Total Volume", f"{df['volume'].sum():,.0f}")

                # Display table sorted by turnover descending
                display_cols = ["symbol", "company_name", "volume", "turnover"]
                df_display = df[display_cols].copy()
                df_display.columns = ["Symbol", "Company", "Volume", "Turnover (PKR)"]
                df_display["Turnover (PKR)"] = df_display["Turnover (PKR)"].apply(
                    lambda x: f"{x:,.0f}" if x else "0"
                )
                df_display["Volume"] = df_display["Volume"].apply(
                    lambda x: f"{x:,.0f}" if x else "0"
                )
                st.dataframe(df_display, use_container_width=True, height=400)
            else:
                st.info("No data for selected date.")
        else:
            st.info("No turnover data loaded yet. Use the tabs above to download.")

    render_footer()
