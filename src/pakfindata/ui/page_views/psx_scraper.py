"""PSX Company Scraper admin page — manage deep scraping, global announcements & payouts."""

import json
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

PSX_SQLITE = Path("/mnt/e/psxdata/psx.sqlite")


def _get_sqlite_con() -> sqlite3.Connection:
    con = sqlite3.connect(str(PSX_SQLITE))
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def _table_count(con: sqlite3.Connection, table: str) -> int:
    try:
        return con.execute(f"SELECT COUNT(*) FROM [{table}]").fetchone()[0]
    except Exception:
        return 0


def render_psx_scraper():
    st.markdown("## PSX Company Scraper")
    st.caption(
        "Manage deep scraping from dps.psx.com.pk | "
        "Profiles, financials, announcements, payouts, equity structure"
    )

    con = _get_sqlite_con()

    # ── top metrics ─────────────────────────────────────────────
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Profiles", f"{_table_count(con, 'company_profile'):,}")
    c2.metric("Financials", f"{_table_count(con, 'company_financials'):,}")
    c3.metric("Ratios", f"{_table_count(con, 'company_ratios'):,}")
    c4.metric("Announcements", f"{_table_count(con, 'corporate_announcements'):,}")
    c5.metric("Payouts", f"{_table_count(con, 'dividend_payouts'):,}")

    tabs = st.tabs([
        "Deep Scrape",
        "Global Announcements",
        "Global Payouts",
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
    # TAB 4 — Data Status
    # ═════════════════════════════════════════════════════════════
    with tabs[3]:
        _render_data_status(con)

    # ═════════════════════════════════════════════════════════════
    # TAB 5 — Browse Data
    # ═════════════════════════════════════════════════════════════
    with tabs[4]:
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

    # Current counts
    try:
        latest = con.execute(
            "SELECT MAX(scraped_at) FROM corporate_announcements"
        ).fetchone()[0]
    except Exception:
        latest = None

    c1, c2 = st.columns(2)
    c1.metric("Total Announcements", f"{_table_count(con, 'corporate_announcements'):,}")
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
                st.dataframe(df, use_container_width=True)
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

    try:
        latest = con.execute(
            "SELECT MAX(scraped_at) FROM dividend_payouts"
        ).fetchone()[0]
    except Exception:
        latest = None

    c1, c2 = st.columns(2)
    c1.metric("Total Payouts", f"{_table_count(con, 'dividend_payouts'):,}")
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
                st.dataframe(df, use_container_width=True)
            else:
                st.info("No payouts in DB yet.")
        except Exception as e:
            st.error(str(e))


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

    rows = []
    for table, label in tables.items():
        count = _table_count(con, table)
        # Get latest date if possible
        latest = ""
        try:
            for date_col in ["updated_at", "scraped_at", "announcement_date", "snapshot_date", "as_of_date"]:
                r = con.execute(f"SELECT MAX([{date_col}]) FROM [{table}]").fetchone()
                if r and r[0]:
                    latest = str(r[0])[:19]
                    break
        except Exception:
            pass

        rows.append({
            "Table": table,
            "Description": label,
            "Rows": count,
            "Latest": latest,
        })

    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Symbols with profiles vs without
    st.markdown("---")
    st.markdown("#### Profile Coverage")

    try:
        total_symbols = con.execute(
            "SELECT COUNT(DISTINCT symbol) FROM company_snapshots"
        ).fetchone()[0]
        with_profiles = con.execute(
            "SELECT COUNT(*) FROM company_profile WHERE description IS NOT NULL AND description != ''"
        ).fetchone()[0]
        with_financials = con.execute(
            "SELECT COUNT(DISTINCT symbol) FROM company_financials"
        ).fetchone()[0]

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
            st.dataframe(stale, use_container_width=True, hide_index=True)
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
                for col in df.columns:
                    val = df.iloc[0][col]
                    if val and str(val).strip():
                        st.markdown(f"**{col}:** {val}")
            else:
                st.info(f"No profile for {symbol}.")
        except Exception as e:
            st.error(str(e))

    with data_tabs[1]:
        try:
            df = pd.read_sql_query(
                "SELECT * FROM company_financials WHERE symbol = ? ORDER BY period_end DESC",
                con, params=(symbol,)
            )
            if not df.empty:
                st.dataframe(df, use_container_width=True, hide_index=True)
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
                st.dataframe(df, use_container_width=True, hide_index=True)
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
                st.dataframe(df1, use_container_width=True, hide_index=True)
            if not df2.empty:
                st.markdown("**Global payouts:**")
                st.dataframe(df2, use_container_width=True, hide_index=True)
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
                st.dataframe(df, use_container_width=True, hide_index=True)
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
                st.dataframe(df, use_container_width=True, hide_index=True)
            else:
                st.info(f"No equity structure for {symbol}.")
        except Exception as e:
            st.error(str(e))
