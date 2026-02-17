"""Website Financial Statement Scanner — discover which PSX companies
host financial reports on their corporate websites."""

import json

import pandas as pd
import streamlit as st

from psx_ohlcv.api_client import get_client
from psx_ohlcv.ui.components.helpers import render_footer


def render_website_scan():
    """Website scan dashboard: run scans and view results."""

    st.markdown("## 🔗 Website Financial Statement Scanner")
    st.caption("Discover which PSX companies host financial reports on their websites")

    client = get_client()
    con = client.connection

    if con is None:
        st.error("No database connection available.")
        render_footer()
        return

    # Ensure schema exists
    from psx_ohlcv.db.repositories.website_scan import (
        init_website_scan_schema,
        get_scan_summary,
        get_website_scans,
    )

    init_website_scan_schema(con)

    st.markdown("---")

    # =================================================================
    # SCAN CONTROLS
    # =================================================================
    st.subheader("Run Scan")
    col1, col2, col3 = st.columns([1, 1, 2])

    with col1:
        limit_choice = st.selectbox("Symbols to scan", [10, 25, 50, 100, 250, 0], format_func=lambda x: "All" if x == 0 else str(x))
    with col2:
        st.write("")  # spacer
        st.write("")
        run_btn = st.button("Run Website Scan", type="primary")

    if run_btn:
        from psx_ohlcv.sources.website_scanner import run_website_scan

        limit_val = limit_choice if limit_choice > 0 else None
        progress_bar = st.progress(0)
        status_text = st.empty()

        def _progress(done: int, total: int, symbol: str):
            pct = done / total if total else 0
            progress_bar.progress(pct)
            status_text.text(f"Scanning {symbol}... ({done}/{total})")

        with st.spinner("Scanning company websites..."):
            summary = run_website_scan(con, limit=limit_val, progress_cb=_progress)

        progress_bar.progress(1.0)
        status_text.text("Scan complete!")
        st.success(
            f"Scanned **{summary['scanned']}** websites: "
            f"**{summary['reachable']}** reachable, "
            f"**{summary['has_financial']}** have financial pages, "
            f"**{summary['errors']}** errors"
        )
        st.rerun()

    # =================================================================
    # SUMMARY METRICS
    # =================================================================
    summary = get_scan_summary(con)

    if summary["total"] == 0:
        st.info("No scan results yet. Click **Run Website Scan** above to start.")
        render_footer()
        return

    st.subheader("Summary")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Total Scanned", summary["total"])
    m2.metric("Have Website", summary["have_website"])
    m3.metric("Reachable", summary["reachable"])
    m4.metric("Has Financials", summary["has_financial"])
    m5.metric("Errors", summary["errors"])

    st.markdown("---")

    # =================================================================
    # RESULTS TABLE
    # =================================================================
    st.subheader("Scan Results")

    tab_all, tab_fin, tab_no_fin, tab_err = st.tabs(
        ["All", "Has Financials", "No Financials", "Errors"]
    )

    with tab_all:
        df = get_website_scans(con)
        _render_results_table(df)

    with tab_fin:
        df = get_website_scans(con, has_financial=True)
        if df.empty:
            st.info("No companies with financial pages found yet.")
        else:
            _render_results_table(df)

    with tab_no_fin:
        df = get_website_scans(con, has_financial=False, reachable=True)
        if df.empty:
            st.info("No reachable companies without financial pages.")
        else:
            _render_results_table(df)

    with tab_err:
        df = get_website_scans(con)
        df = df[df["error_message"].notna() & (df["error_message"] != "")]
        if df.empty:
            st.info("No errors recorded.")
        else:
            _render_results_table(df)

    # =================================================================
    # PDF DOWNLOAD
    # =================================================================
    st.markdown("---")
    st.subheader("Download Financial PDFs")
    st.caption("Download financial statement PDFs from company websites into /mnt/e/psxsymbolfin/{SYMBOL}/")

    fin_df = get_website_scans(con, has_financial=True)
    if fin_df.empty:
        st.info("No companies with financial pages to download from.")
    else:
        from datetime import datetime

        current_year = datetime.now().year
        dc1, dc2, dc3, dc4 = st.columns([2, 1, 1, 1])
        with dc1:
            fin_symbols = sorted(fin_df["symbol"].tolist())
            dl_choice = st.multiselect("Symbols", fin_symbols, default=fin_symbols[:5] if len(fin_symbols) > 5 else fin_symbols)
        with dc2:
            year_from = st.number_input("From year", min_value=2015, max_value=current_year, value=current_year - 5)
        with dc3:
            year_to = st.number_input("To year", min_value=2015, max_value=current_year, value=current_year)
        with dc4:
            st.write("")
            st.write("")
            dl_btn = st.button("Download PDFs", type="primary")

        if dl_btn and dl_choice:
            from psx_ohlcv.sources.fin_downloader import download_financials

            dl_progress = st.progress(0)
            dl_status = st.empty()

            def _dl_progress(done: int, total: int, symbol: str):
                pct = done / total if total else 0
                dl_progress.progress(pct)
                dl_status.text(f"Downloading {symbol}... ({done}/{total})")

            with st.spinner("Downloading financial PDFs..."):
                dl_result = download_financials(
                    con, symbols=dl_choice,
                    year_from=int(year_from), year_to=int(year_to),
                    progress_cb=_dl_progress,
                )

            dl_progress.progress(1.0)
            dl_status.text("Download complete!")
            st.success(
                f"**{dl_result['total_symbols']}** symbols: "
                f"**{dl_result['pdfs_found']}** PDFs found, "
                f"**{dl_result['downloaded']}** downloaded, "
                f"**{dl_result['skipped_existing']}** already existed, "
                f"**{dl_result['errors']}** errors"
            )

            # Show per-symbol detail
            for detail in dl_result.get("details", []):
                if detail["files"]:
                    with st.expander(f"{detail['symbol']} — {detail['downloaded']} downloaded, {detail['skipped']} existing"):
                        for f in detail["files"]:
                            icon = "ok" if f["status"] == "ok" else ("skip" if f["status"] == "exists" else "err")
                            st.text(f"  [{icon}] {f['file']}")
                else:
                    st.text(f"  {detail['symbol']}: no PDFs found for target years")

    # =================================================================
    # CSV EXPORT
    # =================================================================
    st.markdown("---")
    full_df = get_website_scans(con)
    if not full_df.empty:
        csv = full_df.to_csv(index=False)
        st.download_button("Download CSV", csv, "website_scan_results.csv", "text/csv")

    render_footer()


def _render_results_table(df: pd.DataFrame):
    """Render a scan results dataframe with formatting."""
    if df.empty:
        st.info("No results to display.")
        return

    display_cols = ["symbol", "dps_website_url", "website_reachable", "has_financial_page", "financial_urls", "error_message", "scan_duration_ms", "checked_at"]
    available = [c for c in display_cols if c in df.columns]
    show = df[available].copy()

    # Format booleans
    if "website_reachable" in show.columns:
        show["website_reachable"] = show["website_reachable"].map({1: "Yes", 0: "No", True: "Yes", False: "No"})
    if "has_financial_page" in show.columns:
        show["has_financial_page"] = show["has_financial_page"].map({1: "Yes", 0: "No", True: "Yes", False: "No"})

    # Parse financial_urls JSON for display
    if "financial_urls" in show.columns:
        def _format_urls(val):
            if not val or val == "[]":
                return ""
            try:
                urls = json.loads(val) if isinstance(val, str) else val
                return f"{len(urls)} links"
            except Exception:
                return str(val)[:50]
        show["financial_urls"] = show["financial_urls"].apply(_format_urls)

    show.columns = [c.replace("_", " ").title() for c in show.columns]
    st.dataframe(show, use_container_width=True, hide_index=True)
