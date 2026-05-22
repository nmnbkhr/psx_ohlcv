"""PSX Company Scraper admin page — manage deep scraping, global announcements & payouts."""

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PSX_SQLITE = Path("/home/smnb/psxdata_rescue/psx.sqlite")


def _get_sqlite_con() -> sqlite3.Connection:
    con = sqlite3.connect(str(PSX_SQLITE))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _table_count(con: sqlite3.Connection, table: str) -> int:
    """Direct fallback for browse-data tabs that need a quick count.

    Admin-meta dashboards now use /v1/admin/tables; this helper remains
    for the per-symbol detail tabs (browse-data) where the read is
    tightly coupled to the SQL detail query.
    """
    try:
        return con.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    except Exception:
        return 0


def render_psx_scraper():
    from pakfindata.ui.api import client as _api_client

    st.markdown("## PSX Company Scraper")
    st.caption(
        "Manage deep scraping from dps.psx.com.pk | "
        "Profiles, financials, announcements, payouts, equity structure"
    )

    con = _get_sqlite_con()

    # ── top metrics (admin-meta — via /v1/admin/tables) ─────────
    _tables_payload = _api_client.get_admin_tables(include_counts=True) or []
    _counts_by_name = {t["name"]: t["row_count"] or 0 for t in _tables_payload}
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Profiles", f"{_counts_by_name.get('company_profile', 0):,}")
    c2.metric("Financials", f"{_counts_by_name.get('company_financials', 0):,}")
    c3.metric("Ratios", f"{_counts_by_name.get('company_ratios', 0):,}")
    c4.metric("Announcements", f"{_counts_by_name.get('corporate_announcements', 0):,}")
    c5.metric("Payouts", f"{_counts_by_name.get('dividend_payouts', 0):,}")

    tabs = st.tabs([
        "Deep Scrape",
        "Global Announcements",
        "Global Payouts",
        "Listing Status",
        "Data Status",
        "Browse Data",
    ])

    # ═════════════════════════════════════════════════════════════
    # TAB 1 — Deep Scrape (per-company, all 8 tabs)
    # ═════════════════════════════════════════════════════════════
    with tabs[0]:
        _render_deep_scrape(con)

    # ═════════════════════════════════════════════════════════════
    # TAB 2 — Global Announcements
    # ═════════════════════════════════════════════════════════════
    with tabs[1]:
        _render_global_announcements(con)

    # ═════════════════════════════════════════════════════════════
    # TAB 3 — Global Payouts
    # ═════════════════════════════════════════════════════════════
    with tabs[2]:
        _render_global_payouts(con)

    # ═════════════════════════════════════════════════════════════
    # TAB 4 — Listing Status
    # ═════════════════════════════════════════════════════════════
    with tabs[3]:
        _render_listing_status(con)

    # ═════════════════════════════════════════════════════════════
    # TAB 5 — Data Status
    # ═════════════════════════════════════════════════════════════
    with tabs[4]:
        _render_data_status(con)

    # ═════════════════════════════════════════════════════════════
    # TAB 6 — Browse Data
    # ═════════════════════════════════════════════════════════════
    with tabs[5]:
        _render_browse_data(con)

    con.close()


# ─── Deep Scrape ────────────────────────────────────────────────────────


def _render_deep_scrape(con: sqlite3.Connection):
    st.subheader("Per-Company Deep Scrape")
    st.markdown(
        "Scrapes `/company/{SYMBOL}` — all 8 tabs (quote, profile, equity, "
        "financials, ratios, payouts, announcements, reports) in one request."
    )

    from pakfindata.sources.deep_scraper import (
        is_deep_scrape_running,
        read_deep_scrape_progress,
        start_deep_scrape_background,
        stop_deep_scrape,
    )

    running = is_deep_scrape_running()
    progress = read_deep_scrape_progress()

    if running:
        st.warning("Deep scrape is running in the background.")
        if progress:
            pct = progress["current"] / max(progress["total"], 1)
            st.progress(pct, text=f"[{progress['current']}/{progress['total']}] {progress.get('current_symbol', '')}")

            mc1, mc2, mc3 = st.columns(3)
            mc1.metric("OK", progress.get("ok", 0))
            mc2.metric("Failed", progress.get("failed", 0))
            mc3.metric("Remaining", progress["total"] - progress["current"])

            if progress.get("errors"):
                with st.expander(f"Errors ({len(progress['errors'])})"):
                    for err in progress["errors"][:30]:
                        st.text(err)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Refresh", key="ds_refresh"):
                st.rerun()
        with col2:
            if st.button("Stop Scrape", key="ds_stop"):
                stop_deep_scrape()
                st.rerun()
        return

    # Not running
    if progress and progress.get("status") == "completed":
        st.success(
            f"Last run: {progress.get('ok', 0)}/{progress['total']} OK, "
            f"{progress.get('failed', 0)} failed"
        )

    # Scrape controls
    mode = st.radio("Symbols", ["All active", "Custom list", "Single symbol"], horizontal=True, key="ds_mode")

    if mode == "Single symbol":
        single = st.text_input("Symbol", placeholder="OGDC").upper().strip()
        delay = st.slider("Delay (sec)", 0.5, 5.0, 1.0, 0.5, key="ds_delay_s")
        if st.button("Scrape", type="primary", key="ds_single"):
            if not single:
                st.warning("Enter a symbol.")
                return
            with st.spinner(f"Deep scraping {single}..."):
                from pakfindata.sources.deep_scraper import deep_scrape_symbol
                # Need the main app connection for deep_scrape_symbol
                from pakfindata.ui.components.helpers import get_connection
                app_con = get_connection()
                result = deep_scrape_symbol(app_con, single)
                if result.get("success"):
                    parts = []
                    if result.get("snapshot_saved"):
                        parts.append("Quote")
                    if result.get("announcements_saved", 0):
                        parts.append(f"{result['announcements_saved']} announcements")
                    if result.get("financials_saved", 0):
                        parts.append(f"{result['financials_saved']} financials")
                    if result.get("payouts_saved", 0):
                        parts.append(f"{result['payouts_saved']} payouts")
                    st.success(f"Done: {', '.join(parts)}")
                else:
                    st.error(f"Failed: {result.get('error', 'Unknown')}")
    else:
        if mode == "Custom list":
            custom = st.text_input("Symbols (comma-separated)", placeholder="OGDC, HBL, ENGRO")
            symbols = [s.strip().upper() for s in custom.split(",") if s.strip()]
        else:
            from pakfindata.db.repositories.symbols import get_scrapable_symbols
            from pakfindata.ui.components.helpers import get_connection
            app_con = get_connection()
            symbols = get_scrapable_symbols(app_con)

        delay = st.slider("Delay (sec)", 0.5, 5.0, 1.0, 0.5, key="ds_delay_b")
        save_html = st.checkbox("Save raw HTML", value=False)
        st.caption(f"**{len(symbols)}** symbols to scrape")

        if st.button("Start Deep Scrape", type="primary", key="ds_batch"):
            if not symbols:
                st.warning("No symbols to scrape.")
                return
            started = start_deep_scrape_background(
                symbols=symbols, delay=delay, save_raw_html=save_html
            )
            if started:
                st.success(f"Started deep scrape for {len(symbols)} symbols.")
            else:
                st.warning("A deep scrape is already running.")
            st.rerun()


# ─── Global Announcements ──────────────────────────────────────────────


def _render_global_announcements(con: sqlite3.Connection):
    st.subheader("Global Announcements Scraper")
    st.markdown(
        "Scrapes `dps.psx.com.pk/announcements/companies` — all company "
        "announcements at once (50 per page). Runs in **background**."
    )

    from pakfindata.engine.background_jobs import (
        JOB_PSX_ANNOUNCEMENTS, is_running, read_state,
        start_psx_announcements, stop_job,
    )

    # Current counts (admin-meta via /v1/admin)
    from pakfindata.ui.api import client as _api_client
    _ann_payload = _api_client.get_admin_table_latest_date(
        "corporate_announcements", col="scraped_at"
    )
    latest = _ann_payload.get("latest_date") if _ann_payload else None
    _ann_total = _api_client.get_admin_table_distinct_count(
        "corporate_announcements", "id"
    )
    _ann_count = _ann_total.get("distinct_count", 0) if _ann_total else 0

    c1, c2 = st.columns(2)
    c1.metric("Total Announcements", f"{_ann_count:,}")
    c2.metric("Last Scraped", latest[:19] if latest else "Never")

    # Background job status
    running = is_running(JOB_PSX_ANNOUNCEMENTS)
    state = read_state(JOB_PSX_ANNOUNCEMENTS)

    if running and state:
        st.warning("Announcement scrape running in background.")
        st.markdown(
            f"Page {state.get('current_page', 0)} | "
            f"Scraped: {state.get('total_scraped', 0)} | "
            f"Saved: {state.get('total_saved', 0)}"
        )
        if st.button("Stop", key="ann_stop"):
            stop_job(JOB_PSX_ANNOUNCEMENTS)
            st.rerun()
        if st.button("Refresh", key="ann_refresh"):
            st.rerun()
        return

    if state and state.get("status") == "completed":
        st.success(
            f"Last run: {state.get('total_scraped', 0)} scraped, "
            f"{state.get('total_saved', 0)} saved — "
            f"{(state.get('finished_at') or '')[:19]}"
        )

    ann_type = st.selectbox(
        "Announcement Type",
        ["companies", "cdc", "secp", "nccpl", "psx"],
        format_func=lambda x: {
            "companies": "Companies (most useful)",
            "cdc": "CDC", "secp": "SECP", "nccpl": "NCCPL", "psx": "PSX"
        }.get(x, x),
    )

    max_pages = st.slider("Max pages (50 per page)", 1, 100, 20, key="ann_pages")

    if st.button("Start Background Scrape", type="primary", key="ann_scrape"):
        started = start_psx_announcements(ann_type=ann_type, max_pages=max_pages)
        if started:
            st.success("Announcement scrape started in background.")
        else:
            st.warning("Already running.")
        st.rerun()

    # Show recent announcements from DB
    with st.expander("Recent Announcements in DB"):
        try:
            df = pd.read_sql_query(
                "SELECT symbol, announcement_date, title, document_url "
                "FROM corporate_announcements ORDER BY announcement_date DESC LIMIT 50",
                con,
            )
            if not df.empty:
                st.dataframe(df, width='stretch')
            else:
                st.info("No announcements in DB yet.")
        except Exception as e:
            st.error(str(e))


# ─── Global Payouts ────────────────────────────────────────────────────


def _render_global_payouts(con: sqlite3.Connection):
    st.subheader("Global Payouts Scraper")
    st.markdown(
        "Scrapes `dps.psx.com.pk/payouts` — all dividend declarations across "
        "all companies (25 per page). Runs in **background**."
    )

    from pakfindata.engine.background_jobs import (
        JOB_PSX_PAYOUTS, is_running, read_state,
        start_psx_payouts, stop_job,
    )

    from pakfindata.ui.api import client as _api_client
    _pay_payload = _api_client.get_admin_table_latest_date(
        "dividend_payouts", col="scraped_at"
    )
    latest = _pay_payload.get("latest_date") if _pay_payload else None
    _pay_total = _api_client.get_admin_tables(include_counts=True) or []
    _pay_count = next(
        (t["row_count"] for t in _pay_total if t["name"] == "dividend_payouts"), 0
    ) or 0

    c1, c2 = st.columns(2)
    c1.metric("Total Payouts", f"{_pay_count:,}")
    c2.metric("Last Scraped", latest[:19] if latest else "Never")

    running = is_running(JOB_PSX_PAYOUTS)
    state = read_state(JOB_PSX_PAYOUTS)

    if running and state:
        st.warning("Payouts scrape running in background.")
        st.markdown(f"Scraped: {state.get('total_scraped', 0)} | Saved: {state.get('total_saved', 0)}")
        if st.button("Stop", key="pay_stop"):
            stop_job(JOB_PSX_PAYOUTS)
            st.rerun()
        if st.button("Refresh", key="pay_refresh"):
            st.rerun()
        return

    if state and state.get("status") == "completed":
        st.success(
            f"Last run: {state.get('total_scraped', 0)} scraped, "
            f"{state.get('total_saved', 0)} saved — "
            f"{(state.get('finished_at') or '')[:19]}"
        )

    max_pages = st.slider("Max pages (25 per page)", 1, 50, 20, key="pay_pages")

    if st.button("Start Background Scrape", type="primary", key="pay_scrape"):
        started = start_psx_payouts(max_pages=max_pages)
        if started:
            st.success("Payouts scrape started in background.")
        else:
            st.warning("Already running.")
        st.rerun()

    # Show recent payouts from DB
    with st.expander("Recent Payouts in DB"):
        try:
            df = pd.read_sql_query(
                "SELECT symbol, announcement_date, dividend_percent, dividend_type, "
                "dividend_number, book_closure_from, book_closure_to "
                "FROM dividend_payouts ORDER BY announcement_date DESC LIMIT 50",
                con,
            )
            if not df.empty:
                st.dataframe(df, width='stretch')
            else:
                st.info("No payouts in DB yet.")
        except Exception as e:
            st.error(str(e))


# ─── Listing Status ────────────────────────────────────────────────────


def _render_listing_status(con: sqlite3.Connection):
    """Show & manage company listing statuses (SUSPENDED, WINDING-UP, etc.)."""
    from pakfindata.sources.deep_scraper import (
        is_listing_status_check_running,
        read_listing_status_progress,
        start_listing_status_check_background,
    )

    st.subheader("Company Listing Status")
    st.caption(
        "Tracks company-level statuses from PSX (SUSPENDED, WINDING-UP, DEFAULTER, DELISTED). "
        "Separate from trading statuses (XD/XB/XR)."
    )

    # ── Current statuses from DB ──
    try:
        df_status = pd.read_sql_query(
            """SELECT cls.symbol, cls.status, cls.first_seen, cls.last_seen,
                      cp.company_name, cp.sector_name
               FROM company_listing_status cls
               LEFT JOIN company_profile cp ON cls.symbol = cp.symbol
               WHERE cls.is_current = 1
               ORDER BY cls.status, cls.symbol""",
            con,
        )
        if not df_status.empty:
            # Summary metrics
            status_counts = df_status["status"].value_counts()
            cols = st.columns(len(status_counts) + 1)
            cols[0].metric("Total Flagged", len(df_status))
            for i, (status, count) in enumerate(status_counts.items(), 1):
                cols[i].metric(status, count)

            st.dataframe(df_status, width='stretch', hide_index=True)
        else:
            st.info("No company listing statuses found. Run a scan to check all symbols.")
    except Exception:
        st.info("No listing status data yet. Run a scan below.")

    st.divider()

    # ── Background scan controls ──
    running = is_listing_status_check_running()
    progress = read_listing_status_progress()

    if running:
        st.warning("Listing status scan is running...")
        if progress:
            p = progress.get("processed", 0)
            t = progress.get("total", 1)
            st.progress(p / t if t else 0, text=f"{p}/{t} symbols checked")
            found = progress.get("found", {})
            if found:
                st.write(f"Found so far: {len(found)} symbols with statuses")
                st.json(found)
        if st.button("Refresh"):
            st.rerun()
    else:
        if progress and progress.get("status") == "completed":
            st.success(
                f"Last scan completed at {progress.get('completed_at', '?')} | "
                f"{progress.get('processed', 0)} symbols checked"
            )
            found = progress.get("found", {})
            if found:
                st.write(f"**{len(found)} symbols with listing statuses:**")
                st.json(found)

        if st.button("Scan All Symbols for Listing Status", type="primary"):
            started = start_listing_status_check_background(str(PSX_SQLITE))
            if started:
                st.success("Scan started in background! Refresh to see progress.")
            else:
                st.warning("Scan already running.")
            time.sleep(1)
            st.rerun()

    # ── Check single symbol ──
    st.divider()
    col1, col2 = st.columns([3, 1])
    with col1:
        sym_check = st.text_input("Check single symbol", placeholder="e.g. AAL")
    with col2:
        st.write("")
        st.write("")
        check_btn = st.button("Check")

    if check_btn and sym_check:
        from pakfindata.sources.deep_scraper import check_listing_status_single

        tags = check_listing_status_single(sym_check.strip().upper())
        if tags:
            st.error(f"**{sym_check.upper()}**: {', '.join(tags)}")
        else:
            st.success(f"**{sym_check.upper()}**: Normal (no listing status flags)")


# ─── Data Status ───────────────────────────────────────────────────────


def _render_data_status(con: sqlite3.Connection):
    st.subheader("Company Data Coverage")

    tables = {
        "company_profile": "Company Profiles",
        "company_key_people": "Key People",
        "company_financials": "Financials",
        "company_ratios": "Ratios",
        "company_payouts": "Payouts (per-company)",
        "dividend_payouts": "Payouts (global)",
        "corporate_announcements": "Announcements",
        "company_announcements": "Announcements (old)",
        "equity_structure": "Equity Structure",
        "company_snapshots": "Snapshots",
        "company_quote_snapshots": "Quote Snapshots",
    }

    from pakfindata.ui.api import client as _api_client
    _all_tables = _api_client.get_admin_tables(include_counts=True) or []
    _counts = {t["name"]: t["row_count"] or 0 for t in _all_tables}

    rows = []
    for table, label in tables.items():
        # Get latest date by trying common columns (returns None if all fail)
        latest = ""
        for date_col in [
            "updated_at", "scraped_at", "announcement_date",
            "snapshot_date", "as_of_date",
        ]:
            payload = _api_client.get_admin_table_latest_date(table, col=date_col)
            if payload and payload.get("latest_date"):
                latest = str(payload["latest_date"])[:19]
                break

        rows.append({
            "Table": table,
            "Description": label,
            "Rows": _counts.get(table, 0),
            "Latest": latest,
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, width='stretch', hide_index=True)

    # Symbols with profiles vs without
    st.markdown("---")
    st.markdown("#### Profile Coverage")

    try:
        _ts = _api_client.get_admin_table_distinct_count(
            "company_snapshots", "symbol"
        )
        total_symbols = _ts.get("distinct_count", 0) if _ts else 0
        # "with_profiles" was COUNT WHERE description IS NOT NULL — the API
        # doesn't expose conditional counts; use total profile count as proxy.
        with_profiles = _counts.get("company_profile", 0)
        _wf = _api_client.get_admin_table_distinct_count(
            "company_financials", "symbol"
        )
        with_financials = _wf.get("distinct_count", 0) if _wf else 0

        pc1, pc2, pc3 = st.columns(3)
        pc1.metric("Total Symbols", total_symbols)
        pc2.metric("With Profiles", with_profiles)
        pc3.metric("With Financials", with_financials)

        if total_symbols > 0:
            st.progress(with_profiles / total_symbols, text=f"Profile coverage: {with_profiles}/{total_symbols}")
    except Exception:
        pass

    # Stale data check
    st.markdown("---")
    st.markdown("#### Stale Data")
    try:
        stale = pd.read_sql_query("""
            SELECT symbol, company_name, updated_at
            FROM company_profile
            WHERE updated_at < date('now', '-30 days')
            ORDER BY updated_at ASC
            LIMIT 20
        """, con)
        if not stale.empty:
            st.dataframe(stale, width='stretch', hide_index=True)
        else:
            st.success("No stale profiles (all updated within 30 days).")
    except Exception:
        st.info("Could not check for stale data.")


# ─── Browse Data ───────────────────────────────────────────────────────


def _render_browse_data(con: sqlite3.Connection):
    st.subheader("Browse Company Data")

    symbol = st.text_input("Symbol", placeholder="OGDC", key="browse_sym").upper().strip()
    if not symbol:
        st.info("Enter a symbol to browse its data.")
        return

    data_tabs = st.tabs(["Profile", "Financials", "Ratios", "Payouts", "Announcements", "Equity"])

    with data_tabs[0]:
        try:
            df = pd.read_sql_query(
                "SELECT * FROM company_profile WHERE symbol = ?", con, params=(symbol,)
            )
            if not df.empty:
                # Show listing status prominently if present
                ls = df.iloc[0].get("listing_status")
                if ls and str(ls).strip():
                    st.error(f"Listing Status: **{ls}**")

                for col in df.columns:
                    val = df.iloc[0][col]
                    if val and str(val).strip():
                        st.markdown(f"**{col}:** {val}")
            else:
                st.info(f"No profile for {symbol}.")

            # Also show listing status history
            try:
                df_ls = pd.read_sql_query(
                    """SELECT status, is_current, first_seen, last_seen, removed_at
                       FROM company_listing_status WHERE symbol = ? ORDER BY first_seen DESC""",
                    con, params=(symbol,)
                )
                if not df_ls.empty:
                    st.markdown("**Listing Status History:**")
                    st.dataframe(df_ls, width='stretch', hide_index=True)
            except Exception:
                pass
        except Exception as e:
            st.error(str(e))

    with data_tabs[1]:
        try:
            df = pd.read_sql_query(
                "SELECT * FROM company_financials WHERE symbol = ? ORDER BY period_end DESC",
                con, params=(symbol,)
            )
            if not df.empty:
                st.dataframe(df, width='stretch', hide_index=True)
            else:
                st.info(f"No financials for {symbol}.")
        except Exception as e:
            st.error(str(e))

    with data_tabs[2]:
        try:
            df = pd.read_sql_query(
                "SELECT * FROM company_ratios WHERE symbol = ? ORDER BY period_end DESC",
                con, params=(symbol,)
            )
            if not df.empty:
                st.dataframe(df, width='stretch', hide_index=True)
            else:
                st.info(f"No ratios for {symbol}.")
        except Exception as e:
            st.error(str(e))

    with data_tabs[3]:
        try:
            # Try both payout tables
            df1 = pd.read_sql_query(
                "SELECT * FROM company_payouts WHERE symbol = ? ORDER BY ex_date DESC",
                con, params=(symbol,)
            )
            df2 = pd.read_sql_query(
                "SELECT * FROM dividend_payouts WHERE symbol = ? ORDER BY announcement_date DESC",
                con, params=(symbol,)
            )
            if not df1.empty:
                st.markdown("**Per-company payouts:**")
                st.dataframe(df1, width='stretch', hide_index=True)
            if not df2.empty:
                st.markdown("**Global payouts:**")
                st.dataframe(df2, width='stretch', hide_index=True)
            if df1.empty and df2.empty:
                st.info(f"No payouts for {symbol}.")
        except Exception as e:
            st.error(str(e))

    with data_tabs[4]:
        try:
            df = pd.read_sql_query(
                "SELECT announcement_date, title, document_url "
                "FROM corporate_announcements WHERE symbol = ? "
                "ORDER BY announcement_date DESC LIMIT 50",
                con, params=(symbol,)
            )
            if not df.empty:
                st.dataframe(df, width='stretch', hide_index=True)
            else:
                st.info(f"No announcements for {symbol}.")
        except Exception as e:
            st.error(str(e))

    with data_tabs[5]:
        try:
            df = pd.read_sql_query(
                "SELECT * FROM equity_structure WHERE symbol = ? ORDER BY as_of_date DESC",
                con, params=(symbol,)
            )
            if not df.empty:
                st.dataframe(df, width='stretch', hide_index=True)
            else:
                st.info(f"No equity structure for {symbol}.")
        except Exception as e:
            st.error(str(e))
