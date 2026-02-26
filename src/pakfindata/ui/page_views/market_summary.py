"""Market summary page."""

from datetime import datetime, timedelta
import pandas as pd
import streamlit as st

from pakfindata.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_market_summary():
    """Download and manage market summary history files."""
    from datetime import date as date_type
    from datetime import timedelta as td

    from pakfindata.sources.market_summary import (
        fetch_day,
        fetch_day_with_tracking,
        fetch_range_with_tracking,
        get_all_tracking_records,
        get_failed_dates,
        get_missing_dates,
        get_tracking_stats,
        init_market_summary_tracking,
    )

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📥 Market Summary")
        st.caption("Download daily OHLCV files from PSX DPS")
    with header_col2:
        render_market_status_badge()

    with st.expander("ℹ️ About Market Summary Files"):
        st.markdown("""
        Download daily market summary files from PSX DPS. These files contain
        complete market data (OHLCV + company info) for all traded symbols in a
        single compressed file per day.

        **Source:** `https://dps.psx.com.pk/download/mkt_summary/{date}.Z`
        """)

    con = get_connection()

    # Initialize tracking table
    init_market_summary_tracking(con)

    # Session state for download progress
    if "ms_download_progress" not in st.session_state:
        st.session_state.ms_download_progress = None
    if "ms_download_results" not in st.session_state:
        st.session_state.ms_download_results = []

    # Get stats
    stats = get_tracking_stats(con)

    # Stats row
    st.subheader("Download Statistics")
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Tracked", stats["total"])
    with col2:
        st.metric("OK", stats["ok"], delta_color="normal")
    with col3:
        st.metric("Missing (404)", stats["missing"], delta_color="off")
    with col4:
        st.metric("Failed", stats["failed"], delta_color="inverse")
    with col5:
        st.metric("Total Rows", f"{stats['total_rows']:,}")

    if stats["min_date"] and stats["max_date"]:
        st.caption(f"Date range: {stats['min_date']} to {stats['max_date']}")

    st.markdown("---")

    # Tabs for different actions
    tab_single, tab_range, tab_retry, tab_history = st.tabs([
        "📅 Single Day", "📆 Date Range", "🔄 Retry Failed", "📋 History",
    ])

    # =========================================================================
    # Tab 1: Single Day Download
    # =========================================================================
    with tab_single:
        st.subheader("Download Single Day")

        col1, col2 = st.columns([2, 1])
        with col1:
            single_date = st.date_input(
                "Select date",
                value=date_type.today() - td(days=1),
                max_value=date_type.today(),
                key="ms_single_date",
            )
        with col2:
            single_force = st.checkbox(
                "Force re-download",
                value=False,
                key="ms_single_force",
                help="Re-download even if already exists",
            )
            single_keep_raw = st.checkbox(
                "Keep raw files",
                value=False,
                key="ms_single_keep_raw",
                help="Keep the extracted .txt file",
            )
            single_disk_only = st.checkbox(
                "Download only (no DB write)",
                value=False,
                key="ms_single_disk_only",
                help="Save CSV and raw files to disk without writing to database",
            )

        if st.button("Download", key="ms_single_download", type="primary"):
            with st.spinner(f"Downloading {single_date}..."):
                try:
                    if single_disk_only:
                        result = fetch_day(
                            single_date,
                            force=single_force,
                            keep_raw=single_keep_raw,
                        )
                    else:
                        result = fetch_day_with_tracking(
                            con,
                            single_date,
                            force=single_force,
                            keep_raw=single_keep_raw,
                        )
                    if result["status"] == "ok":
                        st.success(
                            f"Downloaded {result['date']}: "
                            f"{result['row_count']} records"
                        )
                    elif result["status"] == "skipped":
                        msg = result.get('message', '')
                        st.info(f"Skipped {result['date']}: {msg}")
                    elif result["status"] == "missing":
                        st.warning(f"No data for {result['date']} (404)")
                    else:
                        st.error(
                            f"Failed {result['date']}: {result.get('message', '')}"
                        )
                except Exception as e:
                    st.error(f"Error: {e}")

    # =========================================================================
    # Tab 2: Date Range Download
    # =========================================================================
    with tab_range:
        st.subheader("Download Date Range")

        col1, col2 = st.columns(2)
        with col1:
            range_start = st.date_input(
                "Start date",
                value=date_type.today() - td(days=30),
                max_value=date_type.today(),
                key="ms_range_start",
            )
        with col2:
            range_end = st.date_input(
                "End date",
                value=date_type.today() - td(days=1),
                max_value=date_type.today(),
                key="ms_range_end",
            )

        col1, col2, col3 = st.columns(3)
        with col1:
            range_skip_weekends = st.checkbox(
                "Skip weekends",
                value=True,
                key="ms_range_skip_weekends",
                help="Skip Saturday and Sunday",
            )
        with col2:
            range_force = st.checkbox(
                "Force re-download",
                value=False,
                key="ms_range_force",
            )
        with col3:
            range_keep_raw = st.checkbox(
                "Keep raw files",
                value=False,
                key="ms_range_keep_raw",
            )

        range_disk_only = st.checkbox(
            "Download only (no DB write)",
            value=False,
            key="ms_range_disk_only",
            help="Save CSV and raw files to disk without writing to database",
        )

        # Calculate expected dates
        from pakfindata.range_utils import iter_dates
        expected_dates = list(iter_dates(
            range_start, range_end, skip_weekends=range_skip_weekends
        ))
        st.caption(f"Will process {len(expected_dates)} dates")

        if st.button("Download Range", key="ms_range_download", type="primary"):
            if range_start > range_end:
                st.error("Start date must be before end date")
            else:
                progress_bar = st.progress(0)
                status_text = st.empty()
                results_container = st.container()

                ok_count = 0
                skip_count = 0
                missing_count = 0
                fail_count = 0

                if range_disk_only:
                    # Disk-only: iterate dates and call fetch_day() directly
                    _range_iter = (
                        fetch_day(d, force=range_force, keep_raw=range_keep_raw)
                        for d in expected_dates
                    )
                else:
                    _range_iter = fetch_range_with_tracking(
                        con,
                        range_start,
                        range_end,
                        skip_weekends=range_skip_weekends,
                        force=range_force,
                        keep_raw=range_keep_raw,
                    )

                for i, result in enumerate(_range_iter):
                    progress = (i + 1) / len(expected_dates)
                    progress_bar.progress(progress)

                    status = result["status"]
                    if status == "ok":
                        ok_count += 1
                    elif status == "skipped":
                        skip_count += 1
                    elif status == "missing":
                        missing_count += 1
                    else:
                        fail_count += 1

                    status_text.text(
                        f"Processing {result['date']}: {status} | "
                        f"OK: {ok_count}, Skip: {skip_count}, "
                        f"Missing: {missing_count}, Failed: {fail_count}"
                    )

                with results_container:
                    st.success(
                        f"Completed! OK: {ok_count}, Skipped: {skip_count}, "
                        f"Missing: {missing_count}, Failed: {fail_count}"
                    )

    # =========================================================================
    # Tab 3: Retry Failed/Missing
    # =========================================================================
    with tab_retry:
        st.subheader("Retry Failed Downloads")

        failed_dates = get_failed_dates(con)
        missing_dates = get_missing_dates(con)

        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**Failed dates:** {len(failed_dates)}")
            if failed_dates:
                with st.expander("View failed dates"):
                    for d in failed_dates[:50]:
                        st.text(d)
                    if len(failed_dates) > 50:
                        st.caption(f"...and {len(failed_dates) - 50} more")

        with col2:
            st.markdown(f"**Missing dates (404):** {len(missing_dates)}")
            if missing_dates:
                with st.expander("View missing dates"):
                    for d in missing_dates[:50]:
                        st.text(d)
                    if len(missing_dates) > 50:
                        st.caption(f"...and {len(missing_dates) - 50} more")

        col1, col2 = st.columns(2)
        with col1:
            if st.button(
                "Retry Failed",
                key="ms_retry_failed",
                disabled=len(failed_dates) == 0,
            ):
                progress_bar = st.progress(0)
                status_text = st.empty()
                ok_count = 0
                still_fail = 0

                for i, date_str in enumerate(failed_dates):
                    progress_bar.progress((i + 1) / len(failed_dates))
                    result = fetch_day_with_tracking(
                        con, date_str, force=True, retry_failed=True
                    )
                    if result["status"] == "ok":
                        ok_count += 1
                    else:
                        still_fail += 1
                    status_text.text(f"Retrying {date_str}: {result['status']}")

                st.success(
                    f"Retried {len(failed_dates)}: "
                    f"{ok_count} OK, {still_fail} still failed"
                )

        with col2:
            if st.button(
                "Retry Missing",
                key="ms_retry_missing",
                disabled=len(missing_dates) == 0,
                help="Retry dates that returned 404 (data may now be available)",
            ):
                progress_bar = st.progress(0)
                status_text = st.empty()
                ok_count = 0
                still_missing = 0

                for i, date_str in enumerate(missing_dates):
                    progress_bar.progress((i + 1) / len(missing_dates))
                    result = fetch_day_with_tracking(
                        con, date_str, force=True, retry_missing=True
                    )
                    if result["status"] == "ok":
                        ok_count += 1
                    else:
                        still_missing += 1
                    status_text.text(f"Retrying {date_str}: {result['status']}")

                st.success(
                    f"Retried {len(missing_dates)}: "
                    f"{ok_count} OK, {still_missing} still missing"
                )

    # =========================================================================
    # Tab 4: History
    # =========================================================================
    with tab_history:
        st.subheader("Download History")

        # Filter by status
        status_filter = st.multiselect(
            "Filter by status",
            options=["ok", "missing", "failed"],
            default=["ok", "missing", "failed"],
            key="ms_history_filter",
        )

        records = get_all_tracking_records(con, limit=500)

        if status_filter:
            records = [r for r in records if r["status"] in status_filter]

        if records:
            import pandas as pd
            df = pd.DataFrame(records)
            # Format for display
            cols = ["date", "status", "row_count", "message", "updated_at"]
            df_display = df[cols].copy()
            df_display.columns = ["Date", "Status", "Rows", "Message", "Updated"]

            # Color code status
            def style_status(val):
                if val == "ok":
                    return "background-color: #d4edda"
                elif val == "missing":
                    return "background-color: #fff3cd"
                elif val == "failed":
                    return "background-color: #f8d7da"
                return ""

            styled_df = df_display.style.map(style_status, subset=["Status"])
            st.dataframe(styled_df, use_container_width=True, height=400)

            st.caption(f"Showing {len(records)} records")
        else:
            st.info("No download history found. Start by downloading some dates.")

    render_footer()
