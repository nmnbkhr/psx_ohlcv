"""Sync monitor page."""

from datetime import datetime, timedelta
import pandas as pd
import streamlit as st
import time

from psx_ohlcv.config import get_db_path
from psx_ohlcv.services import (
    is_service_running,
    read_status as read_service_status,
    start_service_background,
    stop_service,
)
from psx_ohlcv.services.announcements_service import (
    is_service_running as is_announcements_running,
    read_status as read_announcements_status,
    start_service_background as start_announcements_service,
    stop_service as stop_announcements_service,
)
from psx_ohlcv.services.eod_sync_service import (
    is_eod_sync_running,
    read_eod_status,
    start_eod_sync_background,
    stop_eod_sync,
)
from psx_ohlcv.sources.announcements import (
    fetch_announcements,
    fetch_company_payouts,
    fetch_corporate_events,
    save_announcement,
    save_corporate_event,
    save_dividend_payout,
)
from psx_ohlcv.sync import sync_intraday_bulk
from psx_ohlcv.ui.components.helpers import (
    get_connection,
    get_data_freshness,
    get_freshness_badge,
    render_footer,
    render_market_status_badge,
)


def render_sync_monitor():
    """Monitor sync operations and run sync from UI."""
    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 🔄 Sync Monitor")
        st.caption("Data synchronization control center")
    with header_col2:
        render_market_status_badge()

    # =========================================================================
    # EOD SYNC SECTION (Background Service)
    # =========================================================================
    st.subheader("📊 EOD Data Sync")

    # Check if EOD sync is running
    eod_running, eod_pid = is_eod_sync_running()
    eod_status = read_eod_status()

    # Show current status
    if eod_running:
        # Sync is running - show progress
        st.info(f"🔄 **EOD Sync Running** (PID: {eod_pid})")

        # Progress bar
        progress_pct = eod_status.progress / 100.0
        st.progress(progress_pct, text=eod_status.progress_message or "Syncing...")

        # Current symbol
        if eod_status.current_symbol:
            st.caption(f"Currently syncing: **{eod_status.current_symbol}**")

        # Stop button
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("🛑 Stop Sync", type="secondary", key="btn_stop_eod_sync"):
                success, msg = stop_eod_sync()
                if success:
                    st.success(msg)
                else:
                    st.error(msg)
                time.sleep(0.5)
                st.rerun()

        # Auto-refresh to show progress updates
        time.sleep(2)
        st.rerun()

    else:
        # Show last sync result if available
        if eod_status.completed_at:
            result_col1, result_col2, result_col3, result_col4 = st.columns(4)
            with result_col1:
                if eod_status.result == "success":
                    st.success("✅ Last sync: Success")
                elif eod_status.result == "partial":
                    st.warning("⚠️ Last sync: Partial")
                elif eod_status.result == "error":
                    st.error("❌ Last sync: Error")
                elif eod_status.result == "cancelled":
                    st.info("🚫 Last sync: Cancelled")
                else:
                    st.info("ℹ️ Last sync: Unknown")

            with result_col2:
                st.metric("Symbols OK", eod_status.symbols_ok)
            with result_col3:
                st.metric("Failed", eod_status.symbols_failed)
            with result_col4:
                st.metric("Rows", f"{eod_status.rows_upserted:,}")

            if eod_status.completed_at:
                st.caption(f"Completed: {eod_status.completed_at[:19].replace('T', ' ')}")
            if eod_status.error_message:
                st.error(f"Error: {eod_status.error_message}")

        # Sync options
        st.markdown("---")
        col1, col2 = st.columns([1, 1])

        with col1:
            refresh_symbols = st.checkbox(
                "Refresh symbols before sync",
                value=False,
                help="Fetch latest symbols from PSX market-watch before syncing",
            )
            incremental_mode = st.checkbox(
                "Incremental mode",
                value=True,
                help="Only fetch data newer than existing records (faster)",
            )

        with col2:
            cli_flags = "--all"
            if refresh_symbols:
                cli_flags += " --refresh-symbols"
            if incremental_mode:
                cli_flags += " --incremental"
            st.caption("Equivalent CLI command:")
            st.code(f"psxsync sync {cli_flags}", language="bash")

        # Run Sync Button
        col1, col2, col3 = st.columns([1, 1, 2])
        with col1:
            if st.button("▶️ Run Full Sync", type="primary", help="Start syncing EOD data for all symbols (runs in background)"):
                success, msg = start_eod_sync_background(
                    incremental=incremental_mode,
                    refresh_symbols=refresh_symbols,
                )
                if success:
                    st.success(f"🚀 {msg}")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error(msg)

        with col2:
            st.caption("Sync runs in background - you can navigate away")

    # =========================================================================
    # BULK INTRADAY SYNC SECTION
    # =========================================================================
    st.markdown("---")
    st.subheader("📈 Bulk Intraday Sync")

    # Initialize session state for intraday bulk sync
    if "intraday_bulk_result" not in st.session_state:
        st.session_state.intraday_bulk_result = None
    if "intraday_bulk_running" not in st.session_state:
        st.session_state.intraday_bulk_running = False
    if "sync_running" not in st.session_state:
        st.session_state.sync_running = False
    if "sync_result" not in st.session_state:
        st.session_state.sync_result = None

    # -------------------------------------------------------------------------
    # BACKGROUND SERVICE STATUS
    # -------------------------------------------------------------------------
    service_running, service_pid = is_service_running()
    service_status = read_service_status()

    # Service status display
    st.markdown("#### Background Service")
    if service_running:
        st.success(f"🟢 **Service Running** (PID: {service_pid})")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Mode", service_status.mode.capitalize())
        col2.metric("Interval", f"{service_status.interval_seconds}s")
        col3.metric("Total Runs", service_status.total_runs)
        col4.metric("Rows Synced", f"{service_status.rows_upserted:,}")

        # Progress info
        if service_status.current_symbol:
            st.info(f"📊 Currently syncing: **{service_status.current_symbol}** ({service_status.progress:.1f}%)")
        elif service_status.next_run_at:
            st.info(f"⏰ Next run at: {service_status.next_run_at}")

        # Last run result
        if service_status.last_run_at:
            result_icon = "✅" if service_status.last_run_result == "success" else "⚠️" if service_status.last_run_result == "partial" else "❌"
            st.caption(f"Last run: {service_status.last_run_at} - {result_icon} {service_status.symbols_synced} OK, {service_status.symbols_failed} failed")

        # Stop button
        if st.button("🛑 Stop Service", type="primary", key="btn_stop_service"):
            success, msg = stop_service()
            if success:
                st.success(msg)
                st.rerun()
            else:
                st.error(msg)
    else:
        st.warning("🔴 **Service Stopped**")

        # Service configuration
        col1, col2 = st.columns(2)
        with col1:
            service_mode = st.selectbox(
                "Sync Mode",
                options=["incremental", "full"],
                index=0,
                help="Incremental: only new data. Full: refresh all.",
                key="service_mode"
            )
            service_interval = st.number_input(
                "Interval (seconds)",
                min_value=60,
                max_value=3600,
                value=300,
                step=60,
                help="Time between sync runs (default: 300 = 5 minutes)",
                key="service_interval"
            )

        with col2:
            st.caption("CLI equivalent:")
            st.code(
                f"python -m psx_ohlcv.services.intraday_service start "
                f"--mode {service_mode} --interval {service_interval}",
                language="bash"
            )
            st.caption("Cron example (every 5 min during market hours):")
            st.code("*/5 9-15 * * 1-5 psxsync intraday sync-all -q", language="bash")

        # Start button
        if st.button("▶️ Start Background Service", type="primary", key="btn_start_service"):
            success, msg = start_service_background(
                mode=service_mode,
                interval_seconds=service_interval,
            )
            if success:
                st.success(msg)
                time.sleep(1)
                st.rerun()
            else:
                st.error(msg)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # ONE-TIME SYNC (runs in Streamlit, not as background service)
    # -------------------------------------------------------------------------
    st.markdown("#### One-Time Sync")
    st.caption("Run a single sync operation (blocks UI until complete)")

    col1, col2 = st.columns([1, 1])

    with col1:
        intraday_incremental = st.checkbox(
            "Incremental mode (only new data)",
            value=True,
            help="Only fetch data newer than last sync (faster)",
            disabled=st.session_state.intraday_bulk_running,
            key="intraday_bulk_incremental"
        )
        intraday_limit = st.number_input(
            "Limit symbols (0 = all)",
            min_value=0,
            max_value=500,
            value=0,
            help="Limit number of symbols to sync (0 for all)",
            disabled=st.session_state.intraday_bulk_running,
            key="intraday_bulk_limit"
        )

    with col2:
        cli_flags = ""
        if not intraday_incremental:
            cli_flags += " --no-incremental"
        if intraday_limit > 0:
            cli_flags += f" --limit {intraday_limit}"
        st.caption("Equivalent CLI command:")
        st.code(f"psxsync intraday sync-all{cli_flags}", language="bash")

    # Bulk Intraday Sync Buttons
    col1, col2, col3 = st.columns([1, 1, 1])
    with col1:
        run_intraday_full = st.button(
            "🔄 Full Sync" if not st.session_state.intraday_bulk_running else "⏳ Running...",
            disabled=st.session_state.intraday_bulk_running or st.session_state.sync_running,
            help="Sync all intraday data (full refresh)",
            key="btn_intraday_full"
        )
    with col2:
        run_intraday_incr = st.button(
            "⚡ Incremental" if not st.session_state.intraday_bulk_running else "⏳ Running...",
            disabled=st.session_state.intraday_bulk_running or st.session_state.sync_running,
            help="Only fetch new intraday data since last sync",
            key="btn_intraday_incr"
        )
    with col3:
        if st.session_state.intraday_bulk_running:
            st.warning("Sync in progress...")

    # Execute bulk intraday sync
    run_bulk_intraday = run_intraday_full or run_intraday_incr
    use_incremental = intraday_incremental if run_intraday_incr else False

    if run_bulk_intraday and not st.session_state.intraday_bulk_running:
        st.session_state.intraday_bulk_result = None
        st.session_state.intraday_bulk_running = True

        with st.status("Running bulk intraday sync...", expanded=True) as status:
            st.write("🔄 Initializing bulk intraday sync...")

            try:
                limit = intraday_limit if intraday_limit > 0 else None
                mode_str = "incremental" if use_incremental else "full"
                st.write(f"📊 Fetching intraday data ({mode_str} mode)...")

                # Progress container
                progress_bar = st.progress(0)
                progress_text = st.empty()

                def update_progress(current, total, symbol, result):
                    progress = current / total
                    progress_bar.progress(progress)
                    status_icon = "✅" if not result.error else "❌"
                    progress_text.text(f"{status_icon} {symbol}: {result.rows_upserted} rows ({current}/{total})")

                summary = sync_intraday_bulk(
                    db_path=get_db_path(),
                    incremental=use_incremental,
                    limit_symbols=limit,
                    progress_callback=update_progress,
                )

                st.session_state.intraday_bulk_result = {
                    "success": True,
                    "summary": summary,
                }

                if summary.symbols_failed == 0:
                    status.update(
                        label="✅ Bulk intraday sync completed!", state="complete"
                    )
                else:
                    status.update(
                        label=f"⚠️ Completed with {summary.symbols_failed} failures",
                        state="complete"
                    )

            except Exception as e:
                st.session_state.intraday_bulk_result = {
                    "success": False,
                    "error": str(e),
                }
                status.update(label="❌ Bulk intraday sync failed!", state="error")

            finally:
                st.session_state.intraday_bulk_running = False

    # Display bulk intraday sync result
    if st.session_state.intraday_bulk_result is not None:
        result = st.session_state.intraday_bulk_result

        if result["success"]:
            summary = result["summary"]

            if summary.symbols_failed == 0:
                st.success(
                    f"✅ Intraday sync completed: {summary.symbols_ok} symbols, "
                    f"{summary.rows_upserted:,} rows upserted"
                )
            else:
                st.warning(
                    f"⚠️ Intraday sync completed with issues: {summary.symbols_ok} OK, "
                    f"{summary.symbols_failed} failed"
                )

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Symbols", summary.symbols_total)
            col2.metric("Symbols OK", summary.symbols_ok)
            col3.metric("Symbols Failed", summary.symbols_failed)
            col4.metric("Rows Upserted", f"{summary.rows_upserted:,}")

            # Show failed symbols if any
            failed_results = [r for r in summary.results if r.error]
            if failed_results:
                with st.expander(f"🔍 View {len(failed_results)} failures"):
                    for r in failed_results[:20]:  # Limit display
                        st.text(f"{r.symbol}: {r.error}")
        else:
            st.error(f"❌ Intraday sync failed: {result['error']}")

    # =========================================================================
    # ANNOUNCEMENTS SYNC SECTION
    # =========================================================================
    st.markdown("---")
    st.subheader("📣 Announcements Sync")
    st.caption("Sync company announcements, AGM/EOGM calendar, and dividend payouts from PSX DPS")

    # Initialize session state
    if "announcements_sync_running" not in st.session_state:
        st.session_state.announcements_sync_running = False
    if "announcements_sync_result" not in st.session_state:
        st.session_state.announcements_sync_result = None

    # -------------------------------------------------------------------------
    # ANNOUNCEMENTS BACKGROUND SERVICE STATUS
    # -------------------------------------------------------------------------
    ann_running, ann_pid = is_announcements_running()
    ann_status = read_announcements_status()

    st.markdown("#### Background Service")
    if ann_running:
        st.success(f"🟢 **Announcements Service Running** (PID: {ann_pid})")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Interval", f"{ann_status.interval_seconds}s")
        col2.metric("Total Runs", ann_status.total_runs)
        col3.metric("Announcements", ann_status.announcements_synced)
        col4.metric("Dividends", ann_status.dividends_synced)

        if ann_status.current_task:
            st.info(f"🔄 Currently: {ann_status.current_task} - {ann_status.current_symbol or ''} ({ann_status.progress:.0f}%)")

        if ann_status.last_run_at:
            st.caption(f"Last run: {ann_status.last_run_at[:19]} | Result: {ann_status.last_run_result or 'N/A'}")

        if ann_status.next_run_at:
            st.caption(f"Next run: {ann_status.next_run_at[:19]}")

        # Stop button
        if st.button("⏹️ Stop Announcements Service", type="secondary", key="btn_stop_ann_service"):
            success, msg = stop_announcements_service()
            if success:
                st.success(msg)
                time.sleep(1)
                st.rerun()
            else:
                st.error(msg)
    else:
        st.info("🔴 Announcements service not running")

        col1, col2 = st.columns(2)
        with col1:
            ann_interval = st.number_input(
                "Interval (seconds)",
                min_value=300,
                max_value=7200,
                value=3600,
                step=300,
                help="Time between sync runs (default: 3600 = 1 hour)",
                key="ann_service_interval"
            )

        with col2:
            st.caption("CLI equivalent:")
            st.code(
                f"psxsync announcements service start --interval {ann_interval}",
                language="bash"
            )

        if st.button("▶️ Start Announcements Service", type="primary", key="btn_start_ann_service"):
            success, msg = start_announcements_service(interval_seconds=ann_interval)
            if success:
                st.success(msg)
                time.sleep(1)
                st.rerun()
            else:
                st.error(msg)

    st.markdown("---")

    # -------------------------------------------------------------------------
    # ONE-TIME ANNOUNCEMENTS SYNC
    # -------------------------------------------------------------------------
    st.markdown("#### One-Time Sync")
    st.caption("Run a single announcements sync (blocks UI until complete)")

    col1, col2, col3 = st.columns(3)
    with col1:
        sync_announcements_flag = st.checkbox("Company Announcements", value=True, key="sync_ann_flag")
    with col2:
        sync_events_flag = st.checkbox("Corporate Events (AGM/EOGM)", value=True, key="sync_events_flag")
    with col3:
        sync_dividends_flag = st.checkbox("Dividend Payouts", value=True, key="sync_dividends_flag")

    if st.button(
        "🔄 Sync Announcements Now" if not st.session_state.announcements_sync_running else "⏳ Syncing...",
        disabled=st.session_state.announcements_sync_running,
        type="primary",
        key="btn_sync_announcements"
    ):
        st.session_state.announcements_sync_running = True
        st.session_state.announcements_sync_result = None

        with st.status("Syncing announcements...", expanded=True) as status:
            try:
                from datetime import timedelta

                con = get_connection()
                stats = {"announcements": 0, "events": 0, "dividends": 0}

                # Sync announcements
                if sync_announcements_flag:
                    st.write("📣 Fetching company announcements...")
                    offset = 0
                    while True:
                        records, total = fetch_announcements(announcement_type="C", offset=offset)
                        if not records:
                            break
                        for record in records:
                            if save_announcement(con, record):
                                stats["announcements"] += 1
                        offset += len(records)
                        if offset >= total or len(records) < 20:
                            break
                    st.write(f"   ✅ {stats['announcements']} announcements saved")

                # Sync corporate events
                if sync_events_flag:
                    st.write("📅 Fetching corporate events...")
                    from_date = datetime.now().strftime("%Y-%m-%d")
                    to_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")
                    events = fetch_corporate_events(from_date, to_date)
                    for event in events:
                        if save_corporate_event(con, event):
                            stats["events"] += 1
                    st.write(f"   ✅ {stats['events']} events saved")

                # Sync dividends
                if sync_dividends_flag:
                    st.write("💰 Fetching dividend payouts...")
                    cur = con.execute("SELECT symbol FROM symbols WHERE is_active = 1")
                    symbols = [row[0] for row in cur.fetchall()]
                    progress_bar = st.progress(0)
                    for i, symbol in enumerate(symbols):
                        try:
                            payouts = fetch_company_payouts(symbol)
                            for payout in payouts:
                                if save_dividend_payout(con, payout):
                                    stats["dividends"] += 1
                        except Exception:
                            pass
                        progress_bar.progress((i + 1) / len(symbols))
                    st.write(f"   ✅ {stats['dividends']} payouts saved from {len(symbols)} symbols")

                st.session_state.announcements_sync_result = {"success": True, "stats": stats}
                status.update(label="✅ Announcements sync completed!", state="complete")

            except Exception as e:
                st.session_state.announcements_sync_result = {"success": False, "error": str(e)}
                status.update(label="❌ Sync failed!", state="error")

            finally:
                st.session_state.announcements_sync_running = False

    # Display sync result
    if st.session_state.announcements_sync_result is not None:
        result = st.session_state.announcements_sync_result
        if result["success"]:
            stats = result["stats"]
            col1, col2, col3 = st.columns(3)
            col1.metric("Announcements", stats["announcements"])
            col2.metric("Events", stats["events"])
            col3.metric("Dividends", stats["dividends"])
        else:
            st.error(f"❌ Sync failed: {result['error']}")

    # Display sync result
    if st.session_state.sync_result is not None:
        result = st.session_state.sync_result

        st.markdown("---")
        st.subheader("Sync Result")

        if result["success"]:
            summary = result["summary"]

            if summary.symbols_failed == 0:
                st.success(
                    f"✅ Sync completed: {summary.symbols_ok} symbols, "
                    f"{summary.rows_upserted:,} rows upserted"
                )
            else:
                st.warning(
                    f"⚠️ Sync completed with issues: {summary.symbols_ok} OK, "
                    f"{summary.symbols_failed} failed"
                )

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Symbols", summary.symbols_total)
            col2.metric("Symbols OK", summary.symbols_ok)
            failed_delta = (
                None if summary.symbols_failed == 0
                else f"-{summary.symbols_failed}"
            )
            col3.metric(
                "Symbols Failed",
                summary.symbols_failed,
                delta=failed_delta,
                delta_color="inverse"
            )
            col4.metric("Rows Upserted", f"{summary.rows_upserted:,}")

            if summary.failures:
                with st.expander(
                    f"🔍 View {len(summary.failures)} failures",
                    expanded=summary.symbols_failed <= 10
                ):
                    failures_df = pd.DataFrame(summary.failures)
                    failures_df.columns = ["Symbol", "Error Type", "Error Message"]
                    st.dataframe(failures_df, use_container_width=True, hide_index=True)

            st.caption(f"Run ID: `{summary.run_id}`")

        else:
            st.error(f"❌ Sync failed: {result['error']}")

    st.markdown("---")

    # Last Sync Summary
    try:
        con = get_connection()

        days_old, latest_date = get_data_freshness(con)
        badge_color, badge_text = get_freshness_badge(days_old)

        col1, col2 = st.columns([2, 1])
        with col1:
            st.subheader("Last Sync Summary")
        with col2:
            if badge_color == "green":
                st.success(f"📅 {badge_text}")
            elif badge_color == "orange":
                st.warning(f"📅 {badge_text}")
            elif badge_color == "red":
                st.error(f"📅 {badge_text}")

        last_run = pd.read_sql_query(
            """
            SELECT * FROM sync_runs
            WHERE ended_at IS NOT NULL
            ORDER BY ended_at DESC LIMIT 1
            """,
            con,
        )

        if last_run.empty:
            st.info("No sync runs recorded yet.")
        else:
            run = last_run.iloc[0]
            col1, col2, col3, col4 = st.columns(4)
            started = str(run["started_at"])[:16] if run["started_at"] else "N/A"
            col1.metric("Started", started)
            col2.metric("Symbols OK", run["symbols_ok"])
            col3.metric("Symbols Failed", run["symbols_failed"])
            col4.metric("Rows Upserted", f"{run['rows_upserted']:,}")

        st.markdown("---")

        # Recent Failures
        st.subheader("Recent Failures")
        failures_df = pd.read_sql_query(
            """
            SELECT symbol, error_type, error_message, created_at
            FROM sync_failures
            ORDER BY created_at DESC
            LIMIT 50
            """,
            con,
        )

        if failures_df.empty:
            st.success("✅ No failures recorded!")
        else:
            failures_df.columns = ["Symbol", "Error Type", "Message", "Time"]
            st.dataframe(failures_df, use_container_width=True, hide_index=True)

        st.markdown("---")

        # Sync History
        st.subheader("Sync History")
        history_df = pd.read_sql_query(
            """
            SELECT run_id, started_at, mode, symbols_ok, symbols_failed, rows_upserted
            FROM sync_runs
            ORDER BY started_at DESC
            LIMIT 20
            """,
            con,
        )

        if not history_df.empty:
            history_df.columns = ["Run ID", "Started", "Mode", "OK", "Failed", "Rows"]
            st.dataframe(history_df, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Database error: {e}")

    render_footer()
