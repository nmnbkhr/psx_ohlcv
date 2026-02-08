"""Data acquisition and sync page."""

from datetime import datetime, timedelta
import json
import pandas as pd
import streamlit as st
import time

from psx_ohlcv.db import (
    get_company_snapshot,
    get_corporate_announcements,
    get_trading_sessions,
)
from psx_ohlcv.sources.deep_scraper import (
    deep_scrape_batch,
    deep_scrape_symbol,
)
from psx_ohlcv.ui.session_tracker import (
    track_button_click,
    track_page_visit,
    track_symbol_search,
)
from psx_ohlcv.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_data_acquisition():
    """Bulk data acquisition and scraping page."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📥 Data Acquisition")
        st.caption("Bulk data collection from PSX • Company profiles, financials & announcements")
    with header_col2:
        render_market_status_badge()

    con = get_connection()
    track_page_visit(con, "Data Acquisition")

    # Tabs for different sections
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔍 Scrape Company",
        "📊 Company Snapshot",
        "📈 Trading Sessions",
        "📣 Announcements"
    ])

    # -------------------------------------------------------------------------
    # Tab 1: Scrape Company
    # -------------------------------------------------------------------------
    with tab1:
        st.subheader("Deep Scrape Company Data")
        st.markdown("""
        Extract **all available data** from PSX company pages including:
        - Trading data (REG/FUT/CSF/ODL markets)
        - Equity structure (market cap, shares, free float)
        - Financial statements & ratios
        - Corporate announcements
        - Key people & company profile
        """)

        col1, col2 = st.columns([2, 1])

        with col1:
            # Single symbol scrape
            symbol_input = st.text_input(
                "Enter Symbol",
                placeholder="e.g., OGDC, HBL, ENGRO",
                help="Enter a PSX stock symbol to deep scrape"
            ).upper().strip()

        with col2:
            save_html = st.checkbox("Save Raw HTML", value=False,
                help="Store raw HTML for reprocessing (increases storage)")

        if st.button("🔬 Deep Scrape", type="primary", disabled=not symbol_input):
            with st.spinner(f"Deep scraping {symbol_input}..."):
                track_button_click(con, "Deep Scrape", "Data Acquisition", symbol_input)
                result = deep_scrape_symbol(con, symbol_input, save_raw_html=save_html)

            if result.get("success"):
                st.success(f"Successfully scraped {symbol_input}!")
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Snapshot", "Saved" if result.get("snapshot_saved") else "Failed")
                col2.metric("Trading Sessions", result.get("trading_sessions_saved", 0))
                col3.metric("Announcements", result.get("announcements_saved", 0))
                col4.metric("Equity Data", "Saved" if result.get("equity_saved") else "N/A")
            else:
                st.error(f"Failed to scrape {symbol_input}: {result.get('error')}")

        st.divider()

        # Batch scrape section
        st.subheader("Batch Scrape Multiple Symbols")

        batch_input = st.text_area(
            "Enter Symbols (one per line or comma-separated)",
            placeholder="OGDC\nHBL\nENGRO\nPPL",
            height=100
        )

        col1, col2 = st.columns([1, 1])
        with col1:
            delay = st.slider("Delay between requests (seconds)", 0.5, 5.0, 1.0, 0.5)
        with col2:
            batch_save_html = st.checkbox("Save Raw HTML (Batch)", value=False)

        if st.button("🚀 Batch Scrape", disabled=not batch_input.strip()):
            # Parse symbols
            symbols = []
            for line in batch_input.strip().split("\n"):
                for sym in line.split(","):
                    sym = sym.strip().upper()
                    if sym:
                        symbols.append(sym)

            if symbols:
                st.info(f"Scraping {len(symbols)} symbols: {', '.join(symbols[:10])}{'...' if len(symbols) > 10 else ''}")

                progress_bar = st.progress(0)
                status_text = st.empty()

                def update_progress(current, total, symbol, result):
                    progress_bar.progress(current / total)
                    status = "✅" if result.get("success") else "❌"
                    status_text.text(f"{status} [{current}/{total}] {symbol}")

                with st.spinner("Batch scraping in progress..."):
                    track_button_click(con, "Batch Scrape", "Data Acquisition", metadata={"count": len(symbols)})
                    summary = deep_scrape_batch(
                        con, symbols,
                        delay=delay,
                        save_raw_html=batch_save_html,
                        progress_callback=update_progress
                    )

                progress_bar.progress(1.0)
                status_text.empty()

                # Show summary
                col1, col2, col3 = st.columns(3)
                col1.metric("Total", summary["total"])
                col2.metric("Completed", summary["completed"], delta_color="normal")
                col3.metric("Failed", summary["failed"], delta_color="inverse")

                if summary.get("errors"):
                    with st.expander("View Errors"):
                        for err in summary["errors"]:
                            st.error(f"{err['symbol']}: {err['error']}")

        st.divider()

        # ---------------------------------------------------------------------
        # Background Bulk Fetch - Runs in separate process
        # ---------------------------------------------------------------------
        st.subheader("Background Bulk Fetch")
        st.markdown("""
        Fetch deep data for **all active symbols** in the background.
        The job runs in a separate process - you can navigate away and come back.
        """)

        # Import background job functions
        from psx_ohlcv.db import (
            create_background_job,
            get_running_jobs,
            get_recent_jobs,
            request_job_stop,
            get_unread_notifications,
            mark_notification_read,
            mark_all_notifications_read,
        )

        # Show notifications
        notifications = get_unread_notifications(con)
        if notifications:
            st.markdown("#### Notifications")
            for notif in notifications:
                notif_type = notif.get("notification_type", "info")
                if notif_type == "completed":
                    st.success(f"**{notif['title']}**\n\n{notif.get('message', '')}")
                elif notif_type == "failed":
                    st.error(f"**{notif['title']}**\n\n{notif.get('message', '')}")
                elif notif_type == "stopped":
                    st.warning(f"**{notif['title']}**\n\n{notif.get('message', '')}")
                else:
                    st.info(f"**{notif['title']}**\n\n{notif.get('message', '')}")

            if st.button("Clear All Notifications", key="clear_notifs"):
                mark_all_notifications_read(con)
                st.rerun()

            st.divider()

        # Check for running jobs
        running_jobs = get_running_jobs(con)

        if running_jobs:
            st.markdown("#### Running Jobs")
            for job in running_jobs:
                job_id = job["job_id"]
                status = job["status"]
                completed = job.get("symbols_completed", 0)
                total = job.get("symbols_requested", 0)
                failed = job.get("symbols_failed", 0)
                current_symbol = job.get("current_symbol", "")
                current_batch = job.get("current_batch", 0)
                total_batches = job.get("total_batches", 0)

                progress = completed / total if total > 0 else 0

                with st.container():
                    st.markdown(f"**Job `{job_id}`** - {status.upper()}")

                    col1, col2, col3, col4 = st.columns(4)
                    col1.metric("Progress", f"{completed}/{total}")
                    col2.metric("Failed", failed)
                    col3.metric("Batch", f"{current_batch}/{total_batches}")
                    col4.metric("Current", current_symbol or "-")

                    st.progress(progress)

                    col1, col2 = st.columns([1, 3])
                    with col1:
                        if st.button("Stop Job", key=f"stop_{job_id}", type="secondary"):
                            request_job_stop(con, job_id)
                            st.warning(f"Stop requested for job {job_id}")
                            st.rerun()
                    with col2:
                        if st.button("Refresh", key=f"refresh_{job_id}"):
                            st.rerun()

                st.divider()
        else:
            # No running jobs - show start new job form
            # Get count of active symbols
            active_count = con.execute(
                "SELECT COUNT(*) FROM symbols WHERE is_active = 1"
            ).fetchone()[0]

            # Get count of symbols already scraped today
            today_str = datetime.now().strftime("%Y-%m-%d")
            scraped_today = con.execute(
                "SELECT COUNT(DISTINCT symbol) FROM company_snapshots WHERE snapshot_date = ?",
                (today_str,)
            ).fetchone()[0]

            col1, col2, col3 = st.columns(3)
            col1.metric("Active Symbols", active_count)
            col2.metric("Scraped Today", scraped_today)
            col3.metric("Remaining", active_count - scraped_today)

            # Job configuration
            st.markdown("#### Job Configuration")

            col1, col2 = st.columns(2)
            with col1:
                batch_size = st.number_input("Batch Size", min_value=10, max_value=200,
                    value=50, step=10, help="Symbols per batch")
                request_delay = st.slider("Request Delay (sec)", 0.5, 5.0, 1.5, 0.5,
                    help="Delay between requests")
            with col2:
                batch_pause = st.number_input("Batch Pause (sec)", min_value=10, max_value=120,
                    value=30, step=10, help="Pause between batches to avoid rate limiting")
                skip_scraped = st.checkbox("Skip Already Scraped Today", value=True)

            col1, col2 = st.columns(2)
            with col1:
                use_limit = st.checkbox("Limit Symbols", value=False,
                    help="Limit total symbols (useful for testing)")
            with col2:
                if use_limit:
                    symbol_limit = st.number_input("Max Symbols", min_value=10,
                        max_value=active_count, value=min(100, active_count), step=10)
                else:
                    symbol_limit = active_count

            # Start job button
            if st.button("Start Background Job", type="primary", key="start_bg_job"):
                # Get symbols to scrape
                if skip_scraped:
                    query = """
                        SELECT s.symbol FROM symbols s
                        WHERE s.is_active = 1
                        AND s.symbol NOT IN (
                            SELECT DISTINCT symbol FROM company_snapshots
                            WHERE snapshot_date = ?
                        )
                        ORDER BY s.symbol
                    """
                    symbols_to_scrape = [r[0] for r in con.execute(query, (today_str,)).fetchall()]
                else:
                    query = "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
                    symbols_to_scrape = [r[0] for r in con.execute(query).fetchall()]

                # Apply limit
                if use_limit:
                    symbols_to_scrape = symbols_to_scrape[:symbol_limit]

                if not symbols_to_scrape:
                    st.warning("No symbols to scrape. All active symbols may already be scraped today.")
                else:
                    # Create job
                    job_id = create_background_job(
                        con,
                        job_type="bulk_deep_scrape",
                        symbols=symbols_to_scrape,
                        batch_size=batch_size,
                        batch_pause_sec=batch_pause,
                        config={
                            "request_delay": request_delay,
                            "save_raw_html": False,
                            "skip_scraped": skip_scraped,
                            "date": today_str,
                        },
                    )

                    # Start worker process
                    import subprocess
                    import sys

                    worker_cmd = [
                        sys.executable, "-m", "psx_ohlcv.worker", job_id
                    ]

                    # Start in background (detached)
                    subprocess.Popen(
                        worker_cmd,
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL,
                        start_new_session=True,
                    )

                    st.success(f"Started background job `{job_id}` for {len(symbols_to_scrape)} symbols")
                    track_button_click(con, "Start Background Job", "Data Acquisition",
                        metadata={"job_id": job_id, "count": len(symbols_to_scrape)})
                    time.sleep(1)  # Give worker time to start
                    st.rerun()

        # Show recent jobs history
        st.markdown("#### Recent Jobs")
        recent_jobs = get_recent_jobs(con, limit=5)

        if recent_jobs:
            job_data = []
            for job in recent_jobs:
                job_data.append({
                    "Job ID": job["job_id"],
                    "Status": job["status"],
                    "Completed": f"{job.get('symbols_completed', 0)}/{job.get('symbols_requested', 0)}",
                    "Failed": job.get("symbols_failed", 0),
                    "Started": job.get("started_at", "")[:16] if job.get("started_at") else "",
                    "Ended": job.get("ended_at", "")[:16] if job.get("ended_at") else "-",
                })
            st.dataframe(job_data, use_container_width=True, hide_index=True)
        else:
            st.info("No jobs yet. Start a background job above.")

    # -------------------------------------------------------------------------
    # Tab 2: Company Snapshot Viewer
    # -------------------------------------------------------------------------
    with tab2:
        st.subheader("View Company Snapshot")

        # Get list of symbols with snapshots
        snapshot_symbols = con.execute(
            "SELECT DISTINCT symbol FROM company_snapshots ORDER BY symbol"
        ).fetchall()
        symbol_list = [r[0] for r in snapshot_symbols]

        if not symbol_list:
            st.info("No snapshots available yet. Use the 'Scrape Company' tab to capture data.")
        else:
            selected_symbol = st.selectbox("Select Symbol", symbol_list)

            if selected_symbol:
                track_symbol_search(con, selected_symbol, "Data Acquisition - Snapshot")
                snapshot = get_company_snapshot(con, selected_symbol)

                if snapshot:
                    # Header info
                    st.markdown(f"### {snapshot.get('company_name', selected_symbol)}")
                    st.caption(f"Sector: {snapshot.get('sector_name', 'N/A')} | Scraped: {snapshot.get('scraped_at', 'N/A')}")

                    # Trading Data
                    st.markdown("#### Trading Data")
                    trading_data = snapshot.get("trading_data", {})

                    if trading_data:
                        tabs = st.tabs(list(trading_data.keys()))
                        for i, (market, stats) in enumerate(trading_data.items()):
                            with tabs[i]:
                                if stats:
                                    # Create metrics grid
                                    cols = st.columns(4)
                                    metrics = [
                                        ("Open", stats.get("open")),
                                        ("High", stats.get("high")),
                                        ("Low", stats.get("low")),
                                        ("Close", stats.get("close")),
                                        ("Volume", stats.get("volume")),
                                        ("LDCP", stats.get("ldcp")),
                                        ("P/E (TTM)", stats.get("pe_ratio_ttm")),
                                        ("YTD %", stats.get("ytd_change")),
                                    ]
                                    for j, (label, value) in enumerate(metrics):
                                        with cols[j % 4]:
                                            if value is not None:
                                                if "%" in label:
                                                    st.metric(label, f"{value:.2f}%")
                                                elif isinstance(value, float) and value > 1000:
                                                    st.metric(label, f"{value:,.0f}")
                                                else:
                                                    st.metric(label, f"{value:,.2f}" if isinstance(value, float) else value)

                                    # Show ranges
                                    st.markdown("**Ranges**")
                                    range_cols = st.columns(3)
                                    with range_cols[0]:
                                        day_low = stats.get("day_range_low")
                                        day_high = stats.get("day_range_high")
                                        if day_low and day_high:
                                            st.caption(f"Day Range: {day_low:,.2f} - {day_high:,.2f}")
                                    with range_cols[1]:
                                        circuit_low = stats.get("circuit_low")
                                        circuit_high = stats.get("circuit_high")
                                        if circuit_low and circuit_high:
                                            st.caption(f"Circuit: {circuit_low:,.2f} - {circuit_high:,.2f}")
                                    with range_cols[2]:
                                        w52_low = stats.get("week_52_low")
                                        w52_high = stats.get("week_52_high")
                                        if w52_low and w52_high:
                                            st.caption(f"52-Week: {w52_low:,.2f} - {w52_high:,.2f}")

                    # Equity Structure
                    equity = snapshot.get("equity_data", {})
                    if equity:
                        st.markdown("#### Equity Structure")
                        eq_cols = st.columns(4)
                        eq_cols[0].metric("Market Cap", f"{equity.get('market_cap', 0):,.0f}")
                        eq_cols[1].metric("Shares", f"{equity.get('outstanding_shares', 0):,.0f}")
                        eq_cols[2].metric("Free Float", f"{equity.get('free_float_shares', 0):,.0f}")
                        eq_cols[3].metric("Float %", f"{equity.get('free_float_percent', 0):.1f}%")

                    # Financials Summary
                    financials = snapshot.get("financials_data", {})
                    if financials:
                        st.markdown("#### Financials Summary")
                        annual = financials.get("annual", [])
                        if annual:
                            fin_df = pd.DataFrame(annual)
                            if not fin_df.empty:
                                # Reorder columns
                                display_cols = ["period_end", "sales", "profit_after_tax", "eps"]
                                display_cols = [c for c in display_cols if c in fin_df.columns]
                                st.dataframe(fin_df[display_cols], use_container_width=True, hide_index=True)

                    # Ratios Summary
                    ratios = snapshot.get("ratios_data", {})
                    if ratios:
                        st.markdown("#### Ratios Summary")
                        annual_ratios = ratios.get("annual", [])
                        if annual_ratios:
                            ratio_df = pd.DataFrame(annual_ratios)
                            if not ratio_df.empty:
                                display_cols = ["period_end", "gross_profit_margin", "net_profit_margin", "eps_growth", "peg_ratio"]
                                display_cols = [c for c in display_cols if c in ratio_df.columns]
                                st.dataframe(ratio_df[display_cols], use_container_width=True, hide_index=True)

                    # Raw JSON viewer
                    with st.expander("View Raw JSON Data"):
                        # Remove raw_html from display (too large)
                        display_snapshot = {k: v for k, v in snapshot.items() if k != "raw_html"}
                        st.json(display_snapshot)

    # -------------------------------------------------------------------------
    # Tab 3: Trading Sessions
    # -------------------------------------------------------------------------
    with tab3:
        st.subheader("Trading Sessions Database")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            ts_symbols = con.execute(
                "SELECT DISTINCT symbol FROM trading_sessions ORDER BY symbol"
            ).fetchall()
            ts_symbol_list = ["All"] + [r[0] for r in ts_symbols]
            filter_symbol = st.selectbox("Symbol", ts_symbol_list, key="ts_symbol")

        with col2:
            filter_market = st.selectbox("Market Type", ["All", "REG", "FUT", "CSF", "ODL"])

        with col3:
            filter_limit = st.number_input("Limit", min_value=10, max_value=1000, value=100)

        # Query
        symbol_filter = filter_symbol if filter_symbol != "All" else None
        market_filter = filter_market if filter_market != "All" else None

        df = get_trading_sessions(
            con,
            symbol=symbol_filter,
            market_type=market_filter,
            limit=filter_limit
        )

        if not df.empty:
            st.markdown(f"**{len(df)} records found**")

            # Display columns
            display_cols = [
                "symbol", "session_date", "market_type",
                "open", "high", "low", "close", "volume",
                "ldcp", "change_percent", "pe_ratio_ttm"
            ]
            display_cols = [c for c in display_cols if c in df.columns]

            st.dataframe(
                df[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "open": st.column_config.NumberColumn(format="%.2f"),
                    "high": st.column_config.NumberColumn(format="%.2f"),
                    "low": st.column_config.NumberColumn(format="%.2f"),
                    "close": st.column_config.NumberColumn(format="%.2f"),
                    "volume": st.column_config.NumberColumn(format="%d"),
                    "ldcp": st.column_config.NumberColumn(format="%.2f"),
                    "change_percent": st.column_config.NumberColumn(format="%.2f%%"),
                    "pe_ratio_ttm": st.column_config.NumberColumn(format="%.2f"),
                }
            )

            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                csv,
                "trading_sessions.csv",
                "text/csv"
            )
        else:
            st.info("No trading sessions found. Use 'Scrape Company' to capture data.")

    # -------------------------------------------------------------------------
    # Tab 4: Corporate Announcements
    # -------------------------------------------------------------------------
    with tab4:
        st.subheader("Corporate Announcements")

        # Filters
        col1, col2, col3 = st.columns(3)

        with col1:
            ann_symbols = con.execute(
                "SELECT DISTINCT symbol FROM corporate_announcements ORDER BY symbol"
            ).fetchall()
            ann_symbol_list = ["All"] + [r[0] for r in ann_symbols]
            ann_filter_symbol = st.selectbox("Symbol", ann_symbol_list, key="ann_symbol")

        with col2:
            ann_types = con.execute(
                "SELECT DISTINCT announcement_type FROM corporate_announcements"
            ).fetchall()
            ann_type_list = ["All"] + [r[0] for r in ann_types]
            ann_filter_type = st.selectbox("Type", ann_type_list)

        with col3:
            ann_limit = st.number_input("Limit", min_value=10, max_value=500, value=50, key="ann_limit")

        # Query
        ann_symbol = ann_filter_symbol if ann_filter_symbol != "All" else None
        ann_type = ann_filter_type if ann_filter_type != "All" else None

        ann_df = get_corporate_announcements(
            con,
            symbol=ann_symbol,
            announcement_type=ann_type,
            limit=ann_limit
        )

        if not ann_df.empty:
            st.markdown(f"**{len(ann_df)} announcements found**")

            # Format and display
            display_cols = ["symbol", "announcement_date", "announcement_type", "title"]
            display_cols = [c for c in display_cols if c in ann_df.columns]

            st.dataframe(
                ann_df[display_cols],
                use_container_width=True,
                hide_index=True,
                column_config={
                    "title": st.column_config.TextColumn(width="large"),
                }
            )
        else:
            st.info("No announcements found. Use 'Scrape Company' to capture data.")

    render_footer()
