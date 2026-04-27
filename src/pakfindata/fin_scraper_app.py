"""Standalone Financial Scraper — Independent Streamlit App.

Runs the full pipeline independently from the main pakfindata dashboard:
  0. Deep Scrape   — scrape dps.psx.com.pk for company profiles + financials
  1. Website Scan  — discover which PSX companies host financial reports
  2. DPS Announcements — download financial PDFs from PSX announcements
  3. PDF Download  — download financial statement PDFs to /mnt/e/psxsymbolfin/
  4. PDF Import    — parse PDFs and import P&L + Balance Sheet to DB
  5. Browse Files  — view what's already downloaded per symbol

Run:
    streamlit run src/pakfindata/fin_scraper_app.py
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

# Add src to path so pakfindata modules can be imported directly
_src = Path(__file__).resolve().parents[1]
if str(_src) not in sys.path:
    sys.path.insert(0, str(_src))

from dotenv import load_dotenv
load_dotenv()

import json

import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="PSX Financial Scraper",
    page_icon="📑",
    layout="wide",
)

BASE_DIR = Path("/mnt/e/psxsymbolfin")
CURRENT_YEAR = datetime.now().year

# ---------------------------------------------------------------------------
# DB connection (cached)
# ---------------------------------------------------------------------------

@st.cache_resource
def _get_connection():
    from pakfindata.db.connection import connect, init_schema
    con = connect()
    init_schema(con)
    return con


def _symbol_list(con) -> list[str]:
    """Get deduplicated base symbols suitable for scraping.

    Uses get_scrapable_symbols() which normalizes XD/XB/XR/XA/XI/XW/NC
    suffixes back to base symbols and skips winding-up (WU) counters.
    """
    try:
        from pakfindata.db.repositories.symbols import get_scrapable_symbols
        return get_scrapable_symbols(con)
    except Exception:
        # Fallback if the repo function isn't available
        try:
            cur = con.execute(
                "SELECT symbol FROM symbols WHERE is_active=1 ORDER BY symbol"
            )
            return [r[0] for r in cur.fetchall()]
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Tab 0 — Deep Scrape (the seed)
# ---------------------------------------------------------------------------

def _render_deep_scrape(con):
    st.header("0 · Deep Scrape — PSX Company Profiles")
    st.caption(
        "Scrapes dps.psx.com.pk/company/{symbol} for profiles, financials, "
        "ratios, payouts, announcements — and populates company_profile.website "
        "which the Website Scan tab needs"
    )

    all_symbols = _symbol_list(con)

    # Show current state
    try:
        r = con.execute(
            "SELECT COUNT(*), "
            "COUNT(CASE WHEN website IS NOT NULL AND website != '' THEN 1 END) "
            "FROM company_profile"
        ).fetchone()
        profiles_total, profiles_with_web = r[0] or 0, r[1] or 0
    except Exception:
        profiles_total, profiles_with_web = 0, 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Scrapable symbols", len(all_symbols))
    c2.metric("Profiles scraped", profiles_total)
    c3.metric("With website URL", profiles_with_web)
    with c4:
        st.caption("Use sidebar to refresh symbols")

    if profiles_with_web == 0:
        st.warning(
            "No company profiles with website URLs yet. "
            "Run Deep Scrape first — this is Step 0 that feeds all downstream tabs."
        )

    st.markdown("---")

    # Controls
    mode = st.radio(
        "Scrape mode",
        ["Selected symbols", "Missing profiles only", "All symbols"],
        horizontal=True, key="ds_mode",
    )

    selected: list[str] = []
    if mode == "Selected symbols":
        selected = st.multiselect(
            "Symbols", all_symbols,
            default=all_symbols[:10] if len(all_symbols) >= 10 else all_symbols,
            key="ds_symbols",
        )
    elif mode == "Missing profiles only":
        try:
            existing = {
                r[0] for r in con.execute("SELECT symbol FROM company_profile").fetchall()
            }
            selected = [s for s in all_symbols if s not in existing]
            st.info(f"{len(selected)} symbols without profiles")
        except Exception:
            selected = all_symbols
    else:
        selected = all_symbols

    col1, col2 = st.columns([1, 3])
    with col1:
        delay = st.number_input("Delay between requests (sec)", 0.5, 5.0, 1.0, 0.5, key="ds_delay")
    with col2:
        st.write("")
        st.write("")
        run_btn = st.button("Run Deep Scrape", type="primary", key="ds_run")

    if run_btn and selected:
        from pakfindata.sources.deep_scraper import deep_scrape_batch

        bar = st.progress(0)
        status = st.empty()
        log_area = st.empty()
        logs: list[str] = []

        total = len(selected)

        def _prog(current, total_count, symbol, result):
            bar.progress(current / total_count if total_count else 0)
            ok = result.get("success", False) if isinstance(result, dict) else False
            tag = "OK" if ok else "FAIL"
            logs.append(f"[{tag}] {symbol}")
            if len(logs) > 25:
                logs.pop(0)
            status.text(f"Scraping {symbol}… ({current}/{total_count})")
            log_area.code("\n".join(logs), language="text")

        with st.spinner(f"Deep scraping {len(selected)} symbols…"):
            result = deep_scrape_batch(
                con, symbols=selected, delay=delay, progress_callback=_prog,
            )

        bar.progress(1.0)
        status.text("Deep scrape complete!")
        st.success(
            f"**{result.get('completed', 0)}** succeeded, "
            f"**{result.get('failed', 0)}** failed out of "
            f"**{result.get('total', 0)}** symbols"
        )
        if result.get("errors"):
            with st.expander(f"Errors ({len(result['errors'])})"):
                for e in result["errors"]:
                    st.text(e if isinstance(e, str) else str(e))


# ---------------------------------------------------------------------------
# Tab 1 — Website Scan
# ---------------------------------------------------------------------------

def _render_website_scan(con):
    st.header("1 · Website Scan")
    st.caption("Discover which PSX companies host financial reports on their IR websites")

    from pakfindata.db.repositories.website_scan import (
        init_website_scan_schema,
        get_scan_summary,
        get_website_scans,
        upsert_website_scan,
    )
    init_website_scan_schema(con)

    col1, col2 = st.columns([1, 3])
    with col1:
        limit = st.selectbox(
            "Symbols to scan",
            [10, 25, 50, 100, 250, 0],
            format_func=lambda x: "All" if x == 0 else str(x),
            key="ws_limit",
        )
    with col2:
        st.write("")
        st.write("")
        run_btn = st.button("Run Website Scan", type="primary", key="ws_run")

    if run_btn:
        from pakfindata.sources.website_scanner import run_website_scan
        limit_val = limit if limit > 0 else None
        bar = st.progress(0)
        status = st.empty()

        def _prog(done, total, symbol):
            bar.progress(done / total if total else 0)
            status.text(f"Scanning {symbol}… ({done}/{total})")

        with st.spinner("Scanning websites…"):
            run_website_scan(con, limit=limit_val, progress_cb=_prog)
        bar.progress(1.0)
        status.text("Scan complete!")

    # Results
    summary = get_scan_summary(con)
    if summary:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total scanned", summary.get("total", 0))
        c2.metric("Has financials", summary.get("has_financial", 0))
        c3.metric("Errors", summary.get("errors", 0))

    df = get_website_scans(con)
    if not df.empty:
        st.dataframe(df, width='stretch', height=400)

    # --- CSV Import / Export ---
    st.markdown("---")
    csv_col1, csv_col2 = st.columns(2)

    with csv_col1:
        st.subheader("Import from CSV")
        csv_path = BASE_DIR / "financial_links.csv"
        uploaded = st.file_uploader(
            "Upload website scan CSV", type="csv", key="ws_csv_upload",
            help=f"Or auto-detect {csv_path}",
        )
        load_existing = st.button(
            f"Load {csv_path.name}" if csv_path.exists() else "No financial_links.csv found",
            disabled=not csv_path.exists(),
            key="ws_csv_load",
        )

        if uploaded or load_existing:
            try:
                csv_df = pd.read_csv(uploaded if uploaded else csv_path)
                imported = 0
                for _, row in csv_df.iterrows():
                    sym = row.get("symbol")
                    if not sym:
                        continue
                    # Parse financial_urls — may be JSON string or plain
                    fin_urls = row.get("financial_urls")
                    if isinstance(fin_urls, str):
                        try:
                            fin_urls = json.loads(fin_urls)
                        except (json.JSONDecodeError, ValueError):
                            fin_urls = [fin_urls] if fin_urls else []
                    fin_kw = row.get("financial_keywords")
                    if isinstance(fin_kw, str):
                        try:
                            fin_kw = json.loads(fin_kw)
                        except (json.JSONDecodeError, ValueError):
                            fin_kw = [fin_kw] if fin_kw else []
                    upsert_website_scan(con, {
                        "symbol": str(sym).upper(),
                        "dps_website_url": row.get("dps_website_url"),
                        "website_reachable": int(row.get("website_reachable", 0)),
                        "http_status": row.get("http_status"),
                        "has_financial_page": int(row.get("has_financial_page", 0)),
                        "financial_urls": fin_urls,
                        "financial_keywords": fin_kw,
                        "error_message": row.get("error_message") if pd.notna(row.get("error_message")) else None,
                        "scan_duration_ms": row.get("scan_duration_ms"),
                    })
                    imported += 1
                st.success(f"Imported **{imported}** symbols from CSV into DB")
            except Exception as e:
                st.error(f"CSV import failed: {e}")

    with csv_col2:
        st.subheader("Export to CSV")
        export_df = get_website_scans(con)
        if not export_df.empty:
            csv_data = export_df.to_csv(index=False)
            st.download_button(
                "Download website_scan_results.csv",
                csv_data, "website_scan_results.csv", "text/csv",
                key="ws_csv_export",
            )
            save_btn = st.button(f"Save to {csv_path}", key="ws_csv_save")
            if save_btn:
                csv_path.parent.mkdir(parents=True, exist_ok=True)
                export_df.to_csv(csv_path, index=False)
                st.success(f"Saved {len(export_df)} rows to {csv_path}")
        else:
            st.info("No scan data to export.")


# ---------------------------------------------------------------------------
# Tab 2 — DPS Announcements Download
# ---------------------------------------------------------------------------

def _render_dps_download(con):
    st.header("2 · DPS Announcements Download")
    st.caption("Download financial result PDFs from PSX DPS announcements page")

    from pakfindata.sources.dps_announcements import (
        download_dps_financials_batch,
        get_stale_symbols,
    )

    all_symbols = _symbol_list(con)

    # Mode selector
    mode = st.radio(
        "Mode",
        ["Smart (stale/new only)", "Selected symbols", "All symbols"],
        horizontal=True, key="dps_mode",
    )

    symbols_to_run: list[str] = []
    stale_days = 7

    if mode == "Smart (stale/new only)":
        stale_days = st.slider("Re-check after N days", 1, 30, 7, key="dps_stale")
        stale_syms, metrics = get_stale_symbols(con, stale_days)
        symbols_to_run = stale_syms

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total", metrics["total"])
        c2.metric("Never checked", metrics["never_checked"])
        c3.metric("Stale (>{} days)".format(stale_days), metrics["stale"])
        c4.metric("Up to date", metrics["up_to_date"])

        if symbols_to_run:
            with st.expander(f"Queue: {len(symbols_to_run)} symbols"):
                st.text(", ".join(symbols_to_run[:50]))
                if len(symbols_to_run) > 50:
                    st.caption(f"… and {len(symbols_to_run) - 50} more")
        else:
            st.success("All symbols are up to date!")

    elif mode == "Selected symbols":
        symbols_to_run = st.multiselect(
            "Symbols", all_symbols,
            default=all_symbols[:5] if len(all_symbols) >= 5 else all_symbols,
            key="dps_symbols",
        )

    else:  # All symbols
        symbols_to_run = all_symbols
        st.info(f"Will check all **{len(all_symbols)}** symbols")

    run_btn = st.button(
        f"Download from DPS ({len(symbols_to_run)} symbols)",
        type="primary", key="dps_run",
        disabled=len(symbols_to_run) == 0,
    )

    if run_btn and symbols_to_run:
        bar = st.progress(0)
        status = st.empty()

        def _prog(done, total, symbol, found_count=0):
            bar.progress(done / total if total else 0)
            status.text(f"Fetching {symbol}… ({done}/{total}) — {found_count} PDFs found")

        with st.spinner("Downloading from DPS…"):
            result = download_dps_financials_batch(
                con, symbols=symbols_to_run, stale_days=stale_days, progress_cb=_prog,
            )

        bar.progress(1.0)
        status.text("Done!")
        st.success(
            f"**{result.get('total_symbols', 0)}** symbols processed — "
            f"**{result.get('pdfs_found', result.get('pdfs_downloaded', 0))}** PDFs found, "
            f"**{result.get('downloaded', 0)}** downloaded, "
            f"**{result.get('skipped_existing', 0)}** already existed"
        )
        if result.get("errors"):
            with st.expander("Errors"):
                st.text(str(result["errors"]))


# ---------------------------------------------------------------------------
# Tab 3 — PDF Download from IR websites
# ---------------------------------------------------------------------------

def _render_pdf_download(con):
    st.header("3 · Download PDFs from IR Websites")
    st.caption(f"Download financial statement PDFs into {BASE_DIR}/{{SYMBOL}}/")

    from pakfindata.db.repositories.website_scan import (
        init_website_scan_schema,
        get_website_scans,
    )
    init_website_scan_schema(con)
    fin_df = get_website_scans(con, has_financial=True)

    if fin_df.empty:
        st.info("No companies with financial pages found. Run Website Scan first.")
        return

    fin_symbols = sorted(fin_df["symbol"].tolist())

    c1, c2, c3, c4 = st.columns([3, 1, 1, 1])
    with c1:
        dl_symbols = st.multiselect(
            "Symbols", fin_symbols,
            default=fin_symbols[:5] if len(fin_symbols) > 5 else fin_symbols,
            key="dl_symbols",
        )
    with c2:
        year_from = st.number_input("From year", 2015, CURRENT_YEAR, CURRENT_YEAR - 5, key="dl_yf")
    with c3:
        year_to = st.number_input("To year", 2015, CURRENT_YEAR, CURRENT_YEAR, key="dl_yt")
    with c4:
        st.write("")
        st.write("")
        dl_btn = st.button("Download PDFs", type="primary", key="dl_run")

    if dl_btn and dl_symbols:
        from pakfindata.sources.fin_downloader import download_financials

        bar = st.progress(0)
        status = st.empty()

        def _prog(done, total, symbol):
            bar.progress(done / total if total else 0)
            status.text(f"Downloading {symbol}… ({done}/{total})")

        with st.spinner("Downloading…"):
            result = download_financials(
                con, symbols=dl_symbols,
                year_from=int(year_from), year_to=int(year_to),
                progress_cb=_prog,
            )

        bar.progress(1.0)
        status.text("Download complete!")
        st.success(
            f"**{result['total_symbols']}** symbols: "
            f"**{result['pdfs_found']}** found, "
            f"**{result['downloaded']}** downloaded, "
            f"**{result['skipped_existing']}** already existed"
        )

        for detail in result.get("details", []):
            if detail.get("files"):
                with st.expander(f"{detail['symbol']} — {detail['downloaded']} new"):
                    for f in detail["files"]:
                        icon = "✓" if f["status"] == "ok" else "—"
                        st.text(f"  {icon} {f['file']}")


# ---------------------------------------------------------------------------
# Tab 4 — PDF Import (parse → DB)
# ---------------------------------------------------------------------------

def _render_pdf_import(con):
    st.header("4 · Import PDFs → Database")
    st.caption("Parse downloaded PDFs and import P&L + Balance Sheet into the database")

    if not BASE_DIR.exists():
        st.warning(f"Directory not found: {BASE_DIR}")
        return

    available_dirs = sorted([d.name for d in BASE_DIR.iterdir() if d.is_dir()])
    if not available_dirs:
        st.info("No symbol folders found. Download PDFs first.")
        return

    mode = st.radio("Import mode", ["Selected symbols", "All symbols"], horizontal=True, key="imp_mode")

    selected = []
    if mode == "Selected symbols":
        selected = st.multiselect("Symbols", available_dirs, default=available_dirs[:5], key="imp_symbols")
    else:
        selected = available_dirs

    c1, c2 = st.columns([1, 3])
    with c1:
        dry_run = st.checkbox("Dry run (preview only)", key="imp_dry")
    with c2:
        st.write("")
        import_btn = st.button("Import", type="primary", key="imp_run")

    if import_btn and selected:
        from pakfindata.sources.fin_downloader import import_all_pdfs

        bar = st.progress(0)
        status = st.empty()
        log_area = st.empty()
        logs = []

        total = len(selected)

        def _prog(symbol, filename, stat, detail):
            logs.append(f"[{symbol}] {filename}: {stat}")
            if len(logs) > 20:
                logs.pop(0)
            log_area.code("\n".join(logs), language="text")

        idx_holder = {"i": 0}
        orig_prog = _prog

        def _prog_with_bar(symbol, filename, stat, detail):
            idx_holder["i"] += 1
            bar.progress(min(idx_holder["i"] / max(total * 3, 1), 1.0))
            status.text(f"Parsing {symbol}/{filename}…")
            orig_prog(symbol, filename, stat, detail)

        with st.spinner("Importing…"):
            result = import_all_pdfs(
                con, base_dir=BASE_DIR,
                symbols=selected,
                dry_run=dry_run,
                progress_callback=_prog_with_bar,
            )

        bar.progress(1.0)
        status.text("Import complete!")
        st.success(
            f"**{result['symbols_processed']}** symbols — "
            f"**{result['total_parsed']}** parsed, "
            f"**{result['total_upserted']}** upserted, "
            f"**{result['total_errors']}** errors"
        )


# ---------------------------------------------------------------------------
# Tab 5 — Browse downloaded files
# ---------------------------------------------------------------------------

def _render_browse():
    st.header("5 · Browse Downloaded Files")
    st.caption(f"View contents of {BASE_DIR}")

    if not BASE_DIR.exists():
        st.warning(f"Directory not found: {BASE_DIR}")
        return

    dirs = sorted([d for d in BASE_DIR.iterdir() if d.is_dir()])
    if not dirs:
        st.info("No symbol folders found.")
        return

    # Summary table
    rows = []
    for d in dirs:
        pdfs = list(d.glob("*.pdf"))
        total_kb = sum(f.stat().st_size for f in pdfs) // 1024
        rows.append({
            "Symbol": d.name,
            "PDFs": len(pdfs),
            "Size (KB)": total_kb,
        })

    df = pd.DataFrame(rows)
    m1, m2 = st.columns(2)
    m1.metric("Total symbols", len(df))
    m2.metric("Total PDFs", int(df["PDFs"].sum()))

    st.dataframe(df, width='stretch', height=400)

    # CSV export — file inventory
    file_rows = []
    for d in dirs:
        for f in sorted(d.glob("*.pdf")):
            file_rows.append({
                "Symbol": d.name,
                "File": f.name,
                "Size_KB": f.stat().st_size // 1024,
            })
    if file_rows:
        inv_df = pd.DataFrame(file_rows)
        csv_inv = inv_df.to_csv(index=False)
        st.download_button(
            "Export file inventory CSV", csv_inv,
            "psxsymbolfin_inventory.csv", "text/csv",
            key="browse_csv",
        )

    # Drill-down
    selected = st.selectbox("Select symbol to view files", [d.name for d in dirs], key="browse_sym")
    if selected:
        sym_dir = BASE_DIR / selected
        files = sorted(sym_dir.glob("*.pdf"), key=lambda f: f.name)
        if files:
            for f in files:
                sz = f.stat().st_size / 1024
                st.text(f"  {f.name}  ({sz:.0f} KB)")
        else:
            st.info("No PDF files in this folder.")


# ---------------------------------------------------------------------------
# Tab 6 — Corporate Announcements Download
# ---------------------------------------------------------------------------

# Announcement categories with folder names and title patterns
_ANNOUNCEMENT_CATEGORIES = {
    "Dividend / Payout": {
        "folder": "dividends",
        "patterns": [
            "CASH DIVIDEND", "INTERIM CASH DIVIDEND", "FINAL CASH DIVIDEND",
            "BONUS SHARE", "RIGHT SHARE", "CREDIT OF",
            "BOOK CLOSURE", "PAYOUT",
        ],
    },
    "Board Meeting": {
        "folder": "board_meetings",
        "patterns": [
            "BOARD MEETING",
        ],
    },
    "Annual / Quarterly Report": {
        "folder": "reports",
        "patterns": [
            "TRANSMISSION OF ANNUAL REPORT", "TRANSMISSION OF QUARTERLY REPORT",
            "TRANSMISSION OF HALF YEARLY REPORT", "ANNUAL GENERAL MEETING",
        ],
    },
    "Corporate Action": {
        "folder": "corporate_actions",
        "patterns": [
            "CORPORATE BRIEFING", "APPOINTMENT", "REAPPOINTMENT",
            "MATERIAL INFORMATION", "NOTICE OF",
        ],
    },
}


def _match_category(title: str) -> str | None:
    """Match announcement title to a category."""
    upper = title.upper()
    # Skip financial results — already handled in Tab 2
    if "FINANCIAL RESULTS FOR" in upper or "ANNOUNCEMENT OF FINANCIAL RESULTS" in upper:
        return None
    # Skip disclosure of interest — noise
    if "DISCLOSURE OF INTEREST" in upper:
        return None
    for cat, cfg in _ANNOUNCEMENT_CATEGORIES.items():
        for pattern in cfg["patterns"]:
            if pattern in upper:
                return cat
    return None


def _render_announcements_download(con):
    st.header("6 · Corporate Announcements Download")
    st.caption(
        "Download dividends, board meetings, reports, corporate actions from DPS. "
        "Saved into subfolders per category inside /mnt/e/psxsymbolfin/{SYMBOL}/"
    )

    import asyncio
    import aiohttp
    from lxml import html as lhtml

    DPS_URL = "https://dps.psx.com.pk"

    all_symbols = _symbol_list(con)

    # Controls
    col1, col2, col3 = st.columns([3, 1, 1])
    with col1:
        selected = st.multiselect(
            "Symbols", all_symbols,
            default=all_symbols[:10] if len(all_symbols) >= 10 else all_symbols,
            key="ann_symbols",
        )
    with col2:
        st.write("")
        cats = st.multiselect(
            "Categories",
            list(_ANNOUNCEMENT_CATEGORIES.keys()),
            default=list(_ANNOUNCEMENT_CATEGORIES.keys()),
            key="ann_cats",
        )
    with col3:
        st.write("")
        st.write("")
        run_btn = st.button(
            f"Download ({len(selected)} symbols)",
            type="primary", key="ann_run",
            disabled=not selected or not cats,
        )

    # Category legend
    with st.expander("Category patterns"):
        for cat, cfg in _ANNOUNCEMENT_CATEGORIES.items():
            if cat in cats:
                st.caption(f"**{cat}** → `{cfg['folder']}/` — {', '.join(cfg['patterns'][:4])}")

    if run_btn and selected and cats:
        active_cats = {c: _ANNOUNCEMENT_CATEGORIES[c] for c in cats}

        bar = st.progress(0)
        status = st.empty()
        log_area = st.empty()
        logs: list[str] = []
        totals = {"found": 0, "downloaded": 0, "skipped": 0, "errors": 0}

        async def _fetch_and_download(symbols: list[str]):
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "X-Requested-With": "XMLHttpRequest",
                "Referer": f"{DPS_URL}/announcements/companies",
            }
            connector = aiohttp.TCPConnector(ssl=False, limit=5)
            async with aiohttp.ClientSession(connector=connector, headers=headers) as session:
                for idx, symbol in enumerate(symbols):
                    bar.progress((idx + 1) / len(symbols))
                    status.text(f"Processing {symbol}… ({idx + 1}/{len(symbols)})")

                    # Paginate announcements — only fetch recent (2025 Q4 + 2026)
                    offset = 0
                    while offset < 200:  # safety cap
                        form = {
                            "type": "C", "symbol": symbol, "query": "",
                            "count": "50", "offset": str(offset),
                            "date_from": "", "date_to": "", "page": "annc",
                        }
                        try:
                            async with session.post(
                                f"{DPS_URL}/announcements", data=form,
                                timeout=aiohttp.ClientTimeout(total=30),
                            ) as resp:
                                if resp.status != 200:
                                    break
                                body = await resp.text(errors="replace")
                        except Exception:
                            break

                        import re as _re
                        m = _re.search(r"of (\d+) entries", body)
                        total_entries = int(m.group(1)) if m else 0
                        if total_entries == 0:
                            break

                        try:
                            tree = lhtml.fromstring(body)
                        except Exception:
                            break

                        rows = tree.xpath("//tbody/tr")
                        if not rows:
                            break

                        hit_old = False
                        for row in rows:
                            cells = row.xpath("td")
                            if len(cells) < 6:
                                continue
                            date_str = cells[0].text_content().strip()
                            title = cells[4].text_content().strip()

                            # Date filter: only 2025 Q4 + 2026
                            from datetime import datetime as _dt
                            try:
                                ann_date = _dt.strptime(date_str, "%b %d, %Y")
                            except ValueError:
                                continue
                            if ann_date.year < 2025 or (ann_date.year == 2025 and ann_date.month < 10):
                                hit_old = True
                                continue

                            cat = _match_category(title)
                            if not cat or cat not in active_cats:
                                continue

                            pdf_links = row.xpath('.//a[contains(@href, ".pdf")]/@href')
                            if not pdf_links:
                                continue

                            pdf_url = pdf_links[0]
                            if not pdf_url.startswith("http"):
                                pdf_url = DPS_URL + pdf_url

                            folder = active_cats[cat]["folder"]
                            dest_dir = BASE_DIR / symbol / folder
                            dest_dir.mkdir(parents=True, exist_ok=True)

                            # Filename: date_docid.pdf
                            doc_id = pdf_url.split("/")[-1]
                            date_clean = date_str.replace(",", "").replace(" ", "-")
                            fname = f"{date_clean}_{doc_id}"
                            if not fname.endswith(".pdf"):
                                fname += ".pdf"
                            dest = dest_dir / fname

                            totals["found"] += 1

                            if dest.exists() and dest.stat().st_size > 500:
                                totals["skipped"] += 1
                                continue

                            try:
                                async with session.get(
                                    pdf_url, timeout=aiohttp.ClientTimeout(total=60),
                                ) as dl_resp:
                                    if dl_resp.status == 200:
                                        data = await dl_resp.read()
                                        dest.write_bytes(data)
                                        totals["downloaded"] += 1
                                        logs.append(f"[OK] {symbol}/{folder}/{fname}")
                                    else:
                                        totals["errors"] += 1
                            except Exception:
                                totals["errors"] += 1

                            await asyncio.sleep(0.3)

                        if hit_old:
                            break  # stop paginating — we've gone past Oct 2025

                        offset += 50
                        if offset >= total_entries:
                            break
                        await asyncio.sleep(0.3)

                    # Update log display
                    if logs:
                        log_area.code("\n".join(logs[-20:]), language="text")

        with st.spinner(f"Downloading announcements for {len(selected)} symbols…"):
            asyncio.run(_fetch_and_download(selected))

        bar.progress(1.0)
        status.text("Done!")
        st.success(
            f"**{totals['found']}** announcements found, "
            f"**{totals['downloaded']}** downloaded, "
            f"**{totals['skipped']}** already existed, "
            f"**{totals['errors']}** errors"
        )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _render_sidebar(con):
    """Sidebar: symbol management, refresh, and SCD2 status overview."""
    with st.sidebar:
        st.header("Symbol Management")

        # Current counts
        all_symbols = _symbol_list(con)
        try:
            total = con.execute("SELECT COUNT(*) FROM symbols WHERE is_active=1").fetchone()[0]
        except Exception:
            total = 0
        st.caption(f"**{total}** active symbols, **{len(all_symbols)}** scrapable (base)")

        # Refresh from PSX
        st.subheader("Refresh from PSX")
        if st.button("Fetch Listed Companies", type="primary", key="sb_refresh"):
            with st.spinner("Downloading master file from PSX..."):
                try:
                    from pakfindata.sources.listed_companies import refresh_listed_companies
                    result = refresh_listed_companies(con)
                    if result["success"]:
                        st.success(
                            f"**{result['symbols_found']}** symbols found, "
                            f"**{result['inserted']}** new, "
                            f"**{result['updated']}** updated, "
                            f"**{result['sectors_upserted']}** sectors"
                        )
                    else:
                        st.error(result.get("error", "Unknown error"))
                except Exception as e:
                    st.error(f"Refresh failed: {e}")

        if st.button("Refresh from Market Watch", key="sb_mw_refresh"):
            with st.spinner("Fetching market watch..."):
                try:
                    from pakfindata.sources.market_watch import refresh_symbols
                    result = refresh_symbols()
                    st.success(
                        f"**{result.symbols_found}** symbols found, "
                        f"**{result.symbols_upserted}** upserted"
                    )
                except Exception as e:
                    st.error(f"Refresh failed: {e}")

        # SCD2 Status Overview
        st.markdown("---")
        st.subheader("Symbol Status (SCD2)")
        try:
            from pakfindata.db.repositories.symbols import (
                get_symbol_status_history,
                get_symbol_status_at,
                get_status_definitions,
                upsert_status_definition,
            )

            # Status definitions with labels + descriptions
            defs = get_status_definitions(con)

            df = get_symbol_status_history(con, current_only=True)
            if not df.empty:
                counts = df.groupby("status").size()
                for status, cnt in counts.items():
                    if status != "NORMAL":
                        d = defs.get(status, {})
                        label = d.get("label", status)
                        st.caption(f"**{status}** ({label}): {cnt}")
                normal = counts.get("NORMAL", 0)
                st.caption(f"**NORMAL**: {normal}")

                # Lookup specific symbol
                lookup = st.text_input("Check symbol status", placeholder="HBL", key="sb_lookup")
                if lookup:
                    s = get_symbol_status_at(con, lookup.upper())
                    if s:
                        st.info(f"**{lookup.upper()}**: {s['status']} ({s['label']}) since {s['start_date']}")
                    else:
                        st.warning(f"{lookup.upper()}: not found")

                # Show non-NORMAL symbols
                non_normal = df[df["status"] != "NORMAL"][["symbol", "status", "start_date"]].reset_index(drop=True)
                if not non_normal.empty:
                    with st.expander(f"Active statuses ({len(non_normal)})"):
                        st.dataframe(non_normal, width='stretch', hide_index=True)
            else:
                st.info("No status data yet. Run a market watch refresh.")

            # Status definitions management
            st.markdown("---")
            st.subheader("Status Definitions")
            with st.expander("View / Edit definitions"):
                for code, d in defs.items():
                    st.caption(f"**{code}** — {d['label']}")
                    if d.get("description"):
                        st.caption(f"  _{d['description']}_")

                st.markdown("**Add / Update Status**")
                nc1, nc2 = st.columns(2)
                with nc1:
                    new_code = st.text_input("Code", placeholder="PP", key="sb_new_code")
                with nc2:
                    new_label = st.text_input("Label", placeholder="Pre-IPO", key="sb_new_label")
                new_desc = st.text_input("Description", placeholder="Stock in pre-IPO trading phase", key="sb_new_desc")
                new_is_suffix = st.checkbox("Appears as symbol suffix on market watch", value=True, key="sb_new_suffix")
                if st.button("Save", key="sb_save_def") and new_code and new_label:
                    upsert_status_definition(con, new_code.upper(), new_label, new_desc, new_is_suffix)
                    st.success(f"Saved: {new_code.upper()} — {new_label}")

        except Exception:
            st.caption("Status table not yet initialized.")

        # Instrument Registry
        st.markdown("---")
        st.subheader("Instrument Registry")
        try:
            from pakfindata.db.repositories.instrument_registry import (
                get_registry_stats,
                search_registry,
                sync_all as sync_registry,
            )

            stats = get_registry_stats(con)
            if not stats.empty:
                for _, r in stats.groupby("asset_class")["count"].sum().items():
                    st.caption(f"**{_}**: {r}")
                total_reg = int(stats["count"].sum())
                st.caption(f"**Total**: {total_reg}")
            else:
                st.info("Registry empty.")

            if st.button("Sync Registry", key="sb_sync_reg"):
                with st.spinner("Syncing..."):
                    result = sync_registry(con)
                total_synced = sum(v for k, v in result.items() if not k.endswith("_error") and isinstance(v, int))
                st.success(f"Synced {total_synced} instruments")

            reg_query = st.text_input("Search all instruments", placeholder="HBL, Gold, PIB...", key="sb_reg_search")
            if reg_query:
                df = search_registry(con, reg_query, limit=20)
                if not df.empty:
                    st.dataframe(
                        df[["symbol", "asset_class", "instrument_type", "name"]],
                        width='stretch', hide_index=True,
                    )
                else:
                    st.caption("No results.")
        except Exception:
            st.caption("Registry not yet initialized.")


_TAB_NAMES = [
    "0 · Deep Scrape",
    "1 · Website Scan",
    "2 · DPS Announcements",
    "3 · PDF Download",
    "4 · PDF Import",
    "5 · Browse Files",
    "6 · Corp Announcements",
]


def main():
    st.title("📑 PSX Financial Scraper")
    st.caption("Standalone pipeline — scan → download → parse → import")

    con = _get_connection()

    _render_sidebar(con)

    # Radio persists in session_state across reruns — stays on current tab
    selected = st.radio(
        "Pipeline Step",
        _TAB_NAMES,
        horizontal=True,
        key="active_tab",
        label_visibility="collapsed",
    )

    st.markdown("---")

    if selected == _TAB_NAMES[0]:
        _render_deep_scrape(con)
    elif selected == _TAB_NAMES[1]:
        _render_website_scan(con)
    elif selected == _TAB_NAMES[2]:
        _render_dps_download(con)
    elif selected == _TAB_NAMES[3]:
        _render_pdf_download(con)
    elif selected == _TAB_NAMES[4]:
        _render_pdf_import(con)
    elif selected == _TAB_NAMES[5]:
        _render_browse()
    elif selected == _TAB_NAMES[6]:
        _render_announcements_download(con)


if __name__ == "__main__":
    main()
