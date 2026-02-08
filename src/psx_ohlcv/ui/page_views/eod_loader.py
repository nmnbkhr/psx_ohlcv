"""EOD data loader page."""

from datetime import datetime, timedelta
from pathlib import Path
import json
import pandas as pd
import streamlit as st
import time

from psx_ohlcv.config import DATA_ROOT
from psx_ohlcv.ui.components.helpers import (
    get_connection,
    render_footer,
    render_market_status_badge,
)


def render_eod_loader():
    """Load EOD data from CSV/PDF files into eod_ohlcv table.

    This page uses the API backend when available for lighter operation.
    Falls back to direct DB access if API is not running.
    """
    try:
        _eod_data_loader_page_impl()
    except Exception as e:
        st.error(f"Error loading EOD Data Loader page: {e}")
        import traceback
        with st.expander("Error Details"):
            st.code(traceback.format_exc())


def _eod_data_loader_page_impl():
    """Implementation of EOD Data Loader page."""
    from datetime import date as date_type
    from datetime import timedelta as td

    # Try to use API client
    try:
        from psx_ohlcv.api.client import get_client, is_api_available, APIError
        api_available = is_api_available()
    except ImportError:
        api_available = False

    # =================================================================
    # HEADER
    # =================================================================
    header_col1, header_col2 = st.columns([3, 1])
    with header_col1:
        st.markdown("## 📂 EOD Data Loader")
        st.caption("Download and load EOD data into eod_ohlcv table")
    with header_col2:
        render_market_status_badge()

    # API status indicator
    if api_available:
        st.success("🔌 API Backend Connected", icon="✅")
    else:
        st.warning("⚠️ API Backend not available - using direct DB access (slower)")

    with st.expander("ℹ️ About EOD Data Loader"):
        st.markdown("""
        This page allows you to **download** market summary files and **load** them into the **eod_ohlcv** table.

        **Data Sources:**
        - `market_summary` (CSV from .Z files) → processname = `eodfile`
        - `closing_rates_pdf` (PDF fallback) → processname = `eodfile`
        - `per_symbol_api` (JSON API) → processname = `per_symbol_api`

        **Important:** CSV/PDF data (eodfile) will **overwrite** existing data for the same date.
        API data (per_symbol_api) will only insert if no data exists (no overwrite).

        **Performance Tip:** Start the API backend for faster page loading:
        ```bash
        uvicorn psx_ohlcv.api.main:app --port 8000
        ```
        """)

    # =================================================================
    # TABLE STATISTICS (via API or direct DB)
    # =================================================================
    st.subheader("EOD OHLCV Table Statistics")

    if api_available:
        # Use API for stats
        try:
            client = get_client()
            stats = client.get_eod_stats()

            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Rows", f"{stats.total_rows:,}")
            with col2:
                st.metric("Dates", stats.total_dates)
            with col3:
                st.metric("Symbols", stats.total_symbols)
            with col4:
                if stats.min_date and stats.max_date:
                    st.metric("Date Range", f"{stats.min_date[:10]} to {stats.max_date[:10]}")
                else:
                    st.metric("Date Range", "N/A")

            # Source breakdown
            st.markdown("#### By Source & Process")
            if stats.by_source:
                source_data = []
                for source, count in stats.by_source.items():
                    processname = stats.by_processname.get(
                        "per_symbol_api" if source == "per_symbol_api" else "eodfile",
                        "N/A"
                    )
                    source_data.append({"Source": source, "Rows": count})
                source_df = pd.DataFrame(source_data)
                st.dataframe(source_df, use_container_width=True, hide_index=True)
            else:
                st.info("No data in eod_ohlcv table yet.")

            # Get CSV files info
            files_info = client.list_csv_files(limit=500)
            total_csv = files_info.get("total_csv_files", 0)
            in_db = files_info.get("total_in_db", 0)
            not_loaded_count = files_info.get("total_not_loaded", 0)
            files_list = files_info.get("files", [])
            not_loaded = [f["date"] for f in files_list if not f.get("in_db")]

        except APIError as e:
            st.error(f"API Error: {e}")
            api_available = False  # Fall back to direct DB

    if not api_available:
        # Direct DB access (fallback)
        con = get_connection()

        cursor = con.execute("SELECT COUNT(*) FROM eod_ohlcv")
        total_rows = cursor.fetchone()[0]

        cursor = con.execute("SELECT COUNT(DISTINCT date) FROM eod_ohlcv")
        total_dates = cursor.fetchone()[0]

        cursor = con.execute("SELECT COUNT(DISTINCT symbol) FROM eod_ohlcv")
        total_symbols = cursor.fetchone()[0]

        cursor = con.execute("SELECT MIN(date), MAX(date) FROM eod_ohlcv")
        date_range = cursor.fetchone()

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Rows", f"{total_rows:,}")
        with col2:
            st.metric("Dates", total_dates)
        with col3:
            st.metric("Symbols", total_symbols)
        with col4:
            if date_range[0] and date_range[1]:
                st.metric("Date Range", f"{date_range[0][:10]} to {date_range[1][:10]}")
            else:
                st.metric("Date Range", "N/A")

        # Source breakdown
        st.markdown("#### By Source & Process")
        cursor = con.execute("""
            SELECT source, processname, COUNT(*) as count
            FROM eod_ohlcv
            GROUP BY source, processname
            ORDER BY source
        """)
        source_data = cursor.fetchall()

        if source_data:
            source_df = pd.DataFrame(source_data, columns=["Source", "Process Name", "Rows"])
            st.dataframe(source_df, use_container_width=True, hide_index=True)
        else:
            st.info("No data in eod_ohlcv table yet.")

        # Get CSV files info directly
        from psx_ohlcv.config import DATA_ROOT
        csv_dir = DATA_ROOT / "market_summary" / "csv"
        pdf_csv_dir = DATA_ROOT / "closing_rates" / "csv"

        csv_files = sorted(csv_dir.glob("*.csv")) if csv_dir.exists() else []
        pdf_csv_files = sorted(pdf_csv_dir.glob("*.csv")) if pdf_csv_dir.exists() else []

        all_csv_dates = set()
        for f in csv_files:
            all_csv_dates.add(f.stem)
        for f in pdf_csv_files:
            all_csv_dates.add(f.stem)

        cursor = con.execute("SELECT DISTINCT date FROM eod_ohlcv")
        db_dates = set(row[0] for row in cursor.fetchall())

        not_loaded = sorted(all_csv_dates - db_dates, reverse=True)
        total_csv = len(all_csv_dates)
        in_db = len(db_dates & all_csv_dates)
        not_loaded_count = len(not_loaded)

    st.markdown("---")

    # =================================================================
    # AVAILABLE CSV FILES
    # =================================================================
    st.subheader("Load Data from CSV Files")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("CSV Files Available", total_csv)
    with col2:
        st.metric("Already in DB", in_db)
    with col3:
        st.metric("Not Yet Loaded", not_loaded_count)

    # Tabs for different loading options
    tab_download, tab_select, tab_recent, tab_all, tab_tasks = st.tabs([
        "📥 Download & Load", "📅 Select Dates", "🕐 Recent Dates", "📋 Load All Missing", "📊 Task Monitor"
    ])

    # =========================================================================
    # Tab 0: Download & Load (via API background task or direct)
    # =========================================================================
    with tab_download:
        st.markdown("#### Download & Load Date Range")
        st.markdown("Download market summary files from PSX and automatically load into database.")

        col1, col2 = st.columns(2)
        with col1:
            dl_start = st.date_input(
                "Start date",
                value=date_type.today() - td(days=7),
                max_value=date_type.today(),
                key="eod_dl_start",
            )
        with col2:
            dl_end = st.date_input(
                "End date",
                value=date_type.today() - td(days=1),
                max_value=date_type.today(),
                key="eod_dl_end",
            )

        col1, col2, col3 = st.columns(3)
        with col1:
            dl_skip_weekends = st.checkbox(
                "Skip weekends",
                value=True,
                key="eod_dl_skip_weekends",
            )
        with col2:
            dl_force = st.checkbox(
                "Force re-download",
                value=False,
                key="eod_dl_force",
            )
        with col3:
            dl_auto_ingest = st.checkbox(
                "Auto-load to DB",
                value=True,
                key="eod_dl_auto_ingest",
                help="Automatically load downloaded data into eod_ohlcv table",
            )

        # Calculate expected dates
        from psx_ohlcv.range_utils import iter_dates
        if dl_start <= dl_end:
            expected_dates = list(iter_dates(dl_start, dl_end, skip_weekends=dl_skip_weekends))
            st.caption(f"Will process {len(expected_dates)} dates")
        else:
            expected_dates = []
            st.error("Start date must be before end date")

        if api_available:
            # Use API background task
            if st.button("Start Background Task", key="eod_dl_run", type="primary", disabled=not expected_dates):
                try:
                    client = get_client()
                    result = client.start_load_task(
                        start_date=dl_start.strftime("%Y-%m-%d"),
                        end_date=dl_end.strftime("%Y-%m-%d"),
                        skip_weekends=dl_skip_weekends,
                        force=dl_force,
                        auto_download=dl_auto_ingest,
                    )
                    st.success(f"Task started: {result.get('task_id')}")
                    st.info("Switch to 'Task Monitor' tab to track progress")
                    st.session_state["eod_active_task"] = result.get("task_id")
                except APIError as e:
                    st.error(f"Failed to start task: {e}")
        else:
            # Direct execution (blocking)
            if st.button("Download & Load", key="eod_dl_run", type="primary", disabled=not expected_dates):
                from pathlib import Path
                from psx_ohlcv.config import DATA_ROOT
                from psx_ohlcv.db import ingest_market_summary_csv, check_eod_date_exists
                from psx_ohlcv.sources.market_summary import fetch_day_with_tracking, init_market_summary_tracking

                con = get_connection()
                init_market_summary_tracking(con)

                progress_bar = st.progress(0)
                status_text = st.empty()

                ok_count = 0
                skip_count = 0
                missing_count = 0
                fail_count = 0
                total_rows_loaded = 0

                for i, d in enumerate(expected_dates):
                    date_str = d.strftime("%Y-%m-%d") if hasattr(d, 'strftime') else str(d)
                    progress_bar.progress((i + 1) / len(expected_dates))
                    status_text.text(f"Processing {date_str} ({i+1}/{len(expected_dates)})...")

                    try:
                        result = fetch_day_with_tracking(con, d, force=dl_force)
                        status = result["status"]

                        if status == "ok":
                            ok_count += 1
                            if dl_auto_ingest and result.get("csv_path"):
                                try:
                                    ingest_result = ingest_market_summary_csv(
                                        con, result["csv_path"],
                                        skip_existing=False, source="market_summary"
                                    )
                                    if ingest_result["status"] == "ok":
                                        total_rows_loaded += ingest_result.get("rows_inserted", 0)
                                except Exception as e:
                                    st.warning(f"Ingest error for {date_str}: {e}")
                        elif status == "skipped":
                            skip_count += 1
                            if dl_auto_ingest and result.get("csv_path"):
                                csv_path = Path(result["csv_path"])
                                if csv_path.exists() and not check_eod_date_exists(con, date_str):
                                    try:
                                        ingest_result = ingest_market_summary_csv(
                                            con, str(csv_path),
                                            skip_existing=False, source="market_summary"
                                        )
                                        if ingest_result["status"] == "ok":
                                            total_rows_loaded += ingest_result.get("rows_inserted", 0)
                                    except Exception:
                                        pass
                        elif status == "missing":
                            missing_count += 1
                        else:
                            fail_count += 1
                    except Exception as e:
                        fail_count += 1
                        st.warning(f"Error for {date_str}: {e}")

                st.success(
                    f"**Completed!** OK: {ok_count}, Skipped: {skip_count}, "
                    f"Missing: {missing_count}, Failed: {fail_count}"
                )
                if dl_auto_ingest:
                    st.info(f"📊 Loaded **{total_rows_loaded:,}** rows into eod_ohlcv")
                st.rerun()

    # =========================================================================
    # Tab 1: Select specific dates (via API or direct)
    # =========================================================================
    with tab_select:
        st.markdown("#### Select Dates to Load")

        if not not_loaded:
            st.success("✓ All available CSV files are already loaded!")
        else:
            st.markdown(f"**{len(not_loaded)} dates available to load:**")

            selected_dates = st.multiselect(
                "Select dates to load",
                options=not_loaded[:100],  # Limit for performance
                default=not_loaded[:2] if len(not_loaded) >= 2 else not_loaded,
                key="eod_select_dates",
            )

            force_reload = st.checkbox(
                "Force reload (overwrite existing)",
                value=False,
                key="eod_force_reload",
            )

            if st.button("Load Selected Dates", key="eod_load_selected", type="primary", disabled=not selected_dates):
                if api_available:
                    try:
                        client = get_client()
                        result = client.load_dates(selected_dates, force=force_reload)
                        st.success(f"Loaded {result.get('ok_count', 0)}/{len(selected_dates)} dates ({result.get('total_rows', 0):,} rows)")
                        if result.get("results"):
                            results_df = pd.DataFrame(result["results"])
                            st.dataframe(results_df, use_container_width=True, hide_index=True)
                        st.rerun()
                    except APIError as e:
                        st.error(f"API Error: {e}")
                else:
                    # Direct DB access
                    from psx_ohlcv.config import DATA_ROOT
                    from psx_ohlcv.db import ingest_market_summary_csv

                    con = get_connection()
                    csv_dir = DATA_ROOT / "market_summary" / "csv"
                    pdf_csv_dir = DATA_ROOT / "closing_rates" / "csv"

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    results = []

                    for i, date_str in enumerate(selected_dates):
                        progress_bar.progress((i + 1) / len(selected_dates))
                        status_text.text(f"Loading {date_str}...")

                        csv_path = csv_dir / f"{date_str}.csv"
                        source = "market_summary"
                        if not csv_path.exists():
                            csv_path = pdf_csv_dir / f"{date_str}.csv"
                            source = "closing_rates_pdf"

                        if csv_path.exists():
                            try:
                                result = ingest_market_summary_csv(
                                    con, str(csv_path),
                                    skip_existing=not force_reload,
                                    source=source
                                )
                                results.append({
                                    "date": date_str,
                                    "status": result["status"],
                                    "rows": result.get("rows_inserted", 0),
                                    "source": source,
                                })
                            except Exception as e:
                                results.append({
                                    "date": date_str,
                                    "status": "error",
                                    "rows": 0,
                                    "source": source,
                                    "error": str(e),
                                })
                        else:
                            results.append({
                                "date": date_str,
                                "status": "not_found",
                                "rows": 0,
                                "source": "N/A",
                            })

                    ok_count = sum(1 for r in results if r["status"] == "ok")
                    total_rows_loaded = sum(r["rows"] for r in results)
                    st.success(f"Loaded {ok_count}/{len(selected_dates)} dates ({total_rows_loaded:,} rows)")
                    results_df = pd.DataFrame(results)
                    st.dataframe(results_df, use_container_width=True, hide_index=True)
                    st.rerun()

    # =========================================================================
    # Tab 2: Recent dates
    # =========================================================================
    with tab_recent:
        st.markdown("#### Load Recent Dates")
        recent_dates = not_loaded[:10] if not_loaded else []

        if not recent_dates:
            st.success("✓ All recent CSV files are already loaded!")
        else:
            st.markdown("**Recent dates not yet loaded:**")

            if api_available:
                # Quick load via API
                for date_str in recent_dates:
                    col1, col2, col3 = st.columns([3, 1, 1])
                    with col1:
                        st.markdown(f"📅 {date_str}")
                    with col2:
                        try:
                            client = get_client()
                            info = client.get_date_info(date_str)
                            st.caption(info.get("csv_source", "N/A"))
                        except:
                            st.caption("...")
                    with col3:
                        if st.button("Load", key=f"load_{date_str}"):
                            try:
                                client = get_client()
                                result = client.load_dates([date_str], force=False)
                                if result.get("ok_count", 0) > 0:
                                    st.success(f"Loaded {result.get('total_rows', 0)} rows")
                                else:
                                    st.warning("No rows loaded")
                                st.rerun()
                            except APIError as e:
                                st.error(f"Error: {e}")
            else:
                # Direct DB access
                from psx_ohlcv.config import DATA_ROOT
                from psx_ohlcv.db import ingest_market_summary_csv

                con = get_connection()
                csv_dir = DATA_ROOT / "market_summary" / "csv"
                pdf_csv_dir = DATA_ROOT / "closing_rates" / "csv"

                for date_str in recent_dates:
                    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

                    csv_path = csv_dir / f"{date_str}.csv"
                    source = "market_summary"
                    if not csv_path.exists():
                        csv_path = pdf_csv_dir / f"{date_str}.csv"
                        source = "closing_rates_pdf"

                    with col1:
                        st.markdown(f"📅 {date_str}")
                    with col2:
                        st.caption(source)
                    with col3:
                        exists = csv_path.exists()
                        st.caption("✓ CSV" if exists else "✗ No CSV")
                    with col4:
                        if st.button("Load", key=f"load_{date_str}", disabled=not exists):
                            with st.spinner(f"Loading {date_str}..."):
                                try:
                                    result = ingest_market_summary_csv(
                                        con, str(csv_path),
                                        skip_existing=False,
                                        source=source
                                    )
                                    if result["status"] == "ok":
                                        st.success(f"Loaded {result.get('rows_inserted', 0)} rows")
                                    else:
                                        st.warning(f"Status: {result['status']}")
                                    st.rerun()
                                except Exception as e:
                                    st.error(f"Error: {e}")

    # =========================================================================
    # Tab 3: Load all missing
    # =========================================================================
    with tab_all:
        st.markdown("#### Load All Missing Dates")

        if not not_loaded:
            st.success("✓ All available CSV files are already loaded!")
        else:
            st.warning(f"**{len(not_loaded)} dates** are available but not loaded.")

            if api_available and len(not_loaded) > 10:
                st.info("💡 Use background task for large batches (recommended)")

                if st.button("Start Background Load", key="eod_load_all_bg", type="primary"):
                    try:
                        client = get_client()
                        # Find date range
                        sorted_dates = sorted(not_loaded)
                        result = client.start_load_task(
                            start_date=sorted_dates[0],
                            end_date=sorted_dates[-1],
                            skip_weekends=True,
                            force=False,
                            auto_download=False,  # Just load existing files
                        )
                        st.success(f"Background task started: {result.get('task_id')}")
                        st.session_state["eod_active_task"] = result.get("task_id")
                    except APIError as e:
                        st.error(f"Error: {e}")

            if st.button("Load All Missing (Direct)", key="eod_load_all", type="secondary" if api_available else "primary"):
                if api_available:
                    try:
                        client = get_client()
                        result = client.load_dates(sorted(not_loaded), force=False)
                        st.success(f"Loaded {result.get('ok_count', 0)}/{len(not_loaded)} dates ({result.get('total_rows', 0):,} rows)")
                        st.rerun()
                    except APIError as e:
                        st.error(f"Error: {e}")
                else:
                    # Direct DB access
                    from psx_ohlcv.config import DATA_ROOT
                    from psx_ohlcv.db import ingest_market_summary_csv

                    con = get_connection()
                    csv_dir = DATA_ROOT / "market_summary" / "csv"
                    pdf_csv_dir = DATA_ROOT / "closing_rates" / "csv"

                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    ok_count = 0
                    total_rows_loaded = 0
                    errors = []

                    for i, date_str in enumerate(sorted(not_loaded)):
                        progress_bar.progress((i + 1) / len(not_loaded))
                        status_text.text(f"Loading {date_str} ({i+1}/{len(not_loaded)})...")

                        csv_path = csv_dir / f"{date_str}.csv"
                        source = "market_summary"
                        if not csv_path.exists():
                            csv_path = pdf_csv_dir / f"{date_str}.csv"
                            source = "closing_rates_pdf"

                        if csv_path.exists():
                            try:
                                result = ingest_market_summary_csv(
                                    con, str(csv_path),
                                    skip_existing=False,
                                    source=source
                                )
                                if result["status"] == "ok":
                                    ok_count += 1
                                    total_rows_loaded += result.get("rows_inserted", 0)
                            except Exception as e:
                                errors.append(f"{date_str}: {e}")

                    st.success(f"Loaded {ok_count}/{len(not_loaded)} dates ({total_rows_loaded:,} rows)")
                    if errors:
                        with st.expander(f"⚠️ {len(errors)} errors"):
                            for err in errors:
                                st.text(err)
                    st.rerun()

    # =========================================================================
    # Tab 4: Task Monitor (API only)
    # =========================================================================
    with tab_tasks:
        st.markdown("#### Background Task Monitor")

        if not api_available:
            st.info("API backend required for background tasks. Start with:")
            st.code("uvicorn psx_ohlcv.api.main:app --port 8000")
        else:
            try:
                client = get_client()

                # Show active task if any
                active_task_id = st.session_state.get("eod_active_task")
                if active_task_id:
                    st.markdown("##### Active Task")
                    try:
                        status = client.get_task_status(active_task_id)
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Status", status.status.upper())
                        with col2:
                            st.metric("Progress", f"{status.progress:.1f}%")
                        with col3:
                            if status.status == "running":
                                if st.button("Stop Task", key="stop_active"):
                                    client.stop_task(active_task_id)
                                    st.rerun()

                        if status.progress_message:
                            st.caption(status.progress_message)

                        if status.status == "running":
                            st.progress(status.progress / 100)
                            # Auto-refresh
                            time.sleep(2)
                            st.rerun()
                        elif status.status == "completed":
                            st.success("Task completed!")
                            if status.result:
                                st.json(status.result)
                            st.session_state["eod_active_task"] = None
                        elif status.status == "failed":
                            st.error(f"Task failed: {status.error}")
                            st.session_state["eod_active_task"] = None
                    except APIError:
                        st.warning(f"Could not get status for task: {active_task_id}")
                        st.session_state["eod_active_task"] = None

                # List recent tasks
                st.markdown("##### Recent Tasks")
                tasks = client.list_tasks(limit=10)
                if tasks:
                    tasks_df = pd.DataFrame(tasks)
                    st.dataframe(tasks_df, use_container_width=True, hide_index=True)
                else:
                    st.info("No tasks found")

                if st.button("Refresh", key="refresh_tasks"):
                    st.rerun()

            except APIError as e:
                st.error(f"API Error: {e}")

    render_footer()
