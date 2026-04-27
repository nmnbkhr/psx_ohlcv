"""Download financial results PDFs from DPS PSX announcements.

For companies that don't host financial statements on their own website,
PSX DPS announcements page (https://dps.psx.com.pk/announcements/companies)
has "FINANCIAL RESULTS FOR..." entries with attached PDF documents.

This module:
1. Queries DPS announcements API (POST /announcements) with symbol filter
2. Paginates through all announcements for that symbol
3. Filters entries with "FINANCIAL RESULTS FOR" in the title
4. Downloads the attached PDFs into /mnt/e/psxsymbolfin/{SYMBOL}/

Usage:
    from pakfindata.sources.dps_announcements import download_dps_financials_batch
    results = download_dps_financials_batch(con, symbols=["ATRL", "INDU"])
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import Any, Callable

import aiohttp
from lxml import html as lhtml

logger = logging.getLogger("pakfindata")

BASE_URL = "https://dps.psx.com.pk"
BASE_DIR = Path("/mnt/e/psxsymbolfin")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": f"{BASE_URL}/announcements/companies",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

TITLE_FILTER = "FINANCIAL RESULTS FOR"


# ---------------------------------------------------------------------------
# Step 1: Collect PDF links from DPS announcements
# ---------------------------------------------------------------------------

async def _collect_financial_pdfs(
    session: aiohttp.ClientSession,
    symbol: str,
    page_size: int = 50,
    rate_limit: float = 0.3,
) -> list[dict[str, str]]:
    """Paginate through DPS announcements for a symbol and collect financial result PDFs.

    Returns list of dicts: {date, title, url}
    """
    all_pdfs: list[dict[str, str]] = []
    offset = 0
    total = None

    while True:
        form = {
            "type": "C",
            "symbol": symbol,
            "query": "",
            "count": str(page_size),
            "offset": str(offset),
            "date_from": "",
            "date_to": "",
            "page": "annc",
        }

        try:
            async with session.post(
                f"{BASE_URL}/announcements",
                data=form,
                ssl=False,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status != 200:
                    logger.warning("DPS announcements %s offset=%d HTTP %d", symbol, offset, resp.status)
                    break
                body = await resp.text(errors="replace")
        except Exception as e:
            logger.warning("DPS announcements %s offset=%d error: %s", symbol, offset, e)
            break

        if total is None:
            m = re.search(r"of (\d+) entries", body)
            total = int(m.group(1)) if m else 0
            if total == 0:
                break

        try:
            tree = lhtml.fromstring(body)
        except Exception:
            break

        rows = tree.xpath("//tbody/tr")
        if not rows:
            break

        for row in rows:
            cells = row.xpath("td")
            if len(cells) < 6:
                continue
            date = cells[0].text_content().strip()
            title = cells[4].text_content().strip()

            if TITLE_FILTER not in title.upper():
                continue

            pdf_link = row.xpath('.//a[contains(@href, ".pdf")]/@href')
            if pdf_link:
                pdf_url = pdf_link[0]
                if not pdf_url.startswith("http"):
                    pdf_url = BASE_URL + pdf_url
                all_pdfs.append({"date": date, "title": title, "url": pdf_url})

        offset += page_size
        if offset >= total:
            break
        await asyncio.sleep(rate_limit)

    return all_pdfs


# ---------------------------------------------------------------------------
# Step 2: Download PDFs one by one
# ---------------------------------------------------------------------------

async def _download_pdfs(
    session: aiohttp.ClientSession,
    symbol: str,
    pdfs: list[dict[str, str]],
    rate_limit: float = 0.5,
    timeout: int = 60,
) -> dict[str, Any]:
    """Download PDF files for a single symbol.

    Returns result dict with counts and file details.
    """
    out_dir = BASE_DIR / symbol
    out_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, Any] = {
        "symbol": symbol,
        "found": len(pdfs),
        "downloaded": 0,
        "skipped": 0,
        "errors": 0,
        "files": [],
    }

    for pdf in pdfs:
        date_clean = pdf["date"].replace(",", "").replace(" ", "-")
        doc_id = pdf["url"].split("/")[-1]
        filename = f"{date_clean}_{doc_id}"
        if not filename.endswith(".pdf"):
            filename += ".pdf"
        dest = out_dir / filename

        if dest.exists() and dest.stat().st_size > 1000:
            result["skipped"] += 1
            result["files"].append({"file": filename, "status": "exists"})
            continue

        await asyncio.sleep(rate_limit)
        try:
            async with session.get(
                pdf["url"],
                timeout=aiohttp.ClientTimeout(total=timeout),
                ssl=False,
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    dest.write_bytes(data)
                    result["downloaded"] += 1
                    result["files"].append({"file": filename, "status": "ok", "size_kb": len(data) // 1024})
                else:
                    result["errors"] += 1
                    result["files"].append({"file": filename, "status": f"HTTP {resp.status}"})
        except Exception as e:
            result["errors"] += 1
            result["files"].append({"file": filename, "status": f"error: {str(e)[:80]}"})

    return result


# ---------------------------------------------------------------------------
# Step 3: Process one symbol end-to-end
# ---------------------------------------------------------------------------

async def _process_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
) -> dict[str, Any]:
    """Collect and download financial result PDFs for one symbol."""
    pdfs = await _collect_financial_pdfs(session, symbol)
    if not pdfs:
        return {"symbol": symbol, "found": 0, "downloaded": 0, "skipped": 0, "errors": 0, "files": []}
    return await _download_pdfs(session, symbol, pdfs)


# ---------------------------------------------------------------------------
# Batch runner
# ---------------------------------------------------------------------------

async def _run_batch(
    symbols: list[str],
    max_concurrent: int = 3,
    progress_cb: Callable[[int, int, str, int], None] | None = None,
) -> list[dict[str, Any]]:
    """Process multiple symbols sequentially (DPS rate limiting).

    Args:
        symbols: List of PSX symbol codes.
        max_concurrent: Max concurrent symbols (kept low for DPS).
        progress_cb: Called with (done, total, symbol, found_count).
    """
    connector = aiohttp.TCPConnector(limit=max_concurrent * 2, ssl=False)
    results: list[dict[str, Any]] = []

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:
        semaphore = asyncio.Semaphore(max_concurrent)
        done = 0
        total = len(symbols)

        async def _wrapped(sym: str) -> dict[str, Any]:
            nonlocal done
            async with semaphore:
                r = await _process_symbol(session, sym)
                done += 1
                if progress_cb:
                    try:
                        progress_cb(done, total, sym, r["found"])
                    except Exception:
                        pass
                return r

        tasks = [_wrapped(sym) for sym in symbols]
        results = await asyncio.gather(*tasks, return_exceptions=False)

    return list(results)


# ---------------------------------------------------------------------------
# Public entry point (synchronous)
# ---------------------------------------------------------------------------

def download_dps_financials(
    symbols: list[str],
    progress_cb: Callable[[int, int, str, int], None] | None = None,
) -> dict[str, Any]:
    """Download financial results PDFs from DPS announcements.

    Args:
        symbols: List of PSX symbol codes to process.
        progress_cb: Progress callback (done, total, symbol, found_count).

    Returns:
        Summary dict with per-symbol results.
    """
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("DPS announcements download: %d symbols", len(symbols))
    results = asyncio.run(_run_batch(symbols, progress_cb=progress_cb))

    total_found = sum(r["found"] for r in results)
    total_downloaded = sum(r["downloaded"] for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    total_errors = sum(r["errors"] for r in results)
    symbols_with_results = sum(1 for r in results if r["found"] > 0)

    summary = {
        "total_symbols": len(results),
        "symbols_with_results": symbols_with_results,
        "pdfs_found": total_found,
        "downloaded": total_downloaded,
        "skipped_existing": total_skipped,
        "errors": total_errors,
        "base_dir": str(BASE_DIR),
        "details": results,
    }
    logger.info(
        "DPS download complete: %d symbols, %d with results, %d found, %d downloaded, %d skipped, %d errors",
        len(results), symbols_with_results, total_found, total_downloaded, total_skipped, total_errors,
    )
    return summary


def _upsert_sync_status(
    con, symbol: str, pdfs_found: int, status: str = "completed", error: str | None = None,
) -> None:
    """Record per-symbol sync status in announcements_sync_status."""
    from datetime import datetime as _dt
    now = _dt.now().strftime("%Y-%m-%d %H:%M:%S")
    today = _dt.now().strftime("%Y-%m-%d")
    con.execute(
        """INSERT INTO announcements_sync_status
           (sync_type, symbol, last_sync_date, total_records, status, error_message, started_at, completed_at)
           VALUES ('financial_results', ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(sync_type, symbol) DO UPDATE SET
             last_sync_date = excluded.last_sync_date,
             total_records = excluded.total_records,
             status = excluded.status,
             error_message = excluded.error_message,
             completed_at = excluded.completed_at,
             scraped_at = datetime('now')""",
        (symbol, today, pdfs_found, status, error, now, now),
    )
    con.commit()


def get_stale_symbols(
    con, stale_days: int = 7,
) -> tuple[list[str], dict]:
    """Get symbols that need DPS announcements checking.

    Returns symbols not checked in `stale_days` days, ordered by
    never-checked first, then oldest-checked.

    Args:
        con: SQLite connection.
        stale_days: Re-check after this many days.

    Returns:
        (symbols_list, metrics_dict) where metrics has:
        total, never_checked, stale, up_to_date counts.
    """
    from ..db.repositories.symbols import get_scrapable_symbols

    all_symbols = get_scrapable_symbols(con)
    all_set = set(all_symbols)

    # Get last sync dates
    rows = con.execute(
        "SELECT symbol, last_sync_date FROM announcements_sync_status "
        "WHERE sync_type = 'financial_results'"
    ).fetchall()
    sync_dates: dict[str, str] = {r[0]: r[1] for r in rows}

    from datetime import datetime as _dt, timedelta
    cutoff = (_dt.now() - timedelta(days=stale_days)).strftime("%Y-%m-%d")

    never: list[str] = []
    stale: list[str] = []
    up_to_date = 0

    for sym in all_symbols:
        last = sync_dates.get(sym)
        if last is None:
            never.append(sym)
        elif last < cutoff:
            stale.append(sym)
        else:
            up_to_date += 1

    # Never-checked first, then oldest stale
    stale.sort(key=lambda s: sync_dates.get(s, ""))
    result = never + stale

    metrics = {
        "total": len(all_symbols),
        "never_checked": len(never),
        "stale": len(stale),
        "up_to_date": up_to_date,
    }
    return result, metrics


def download_dps_financials_batch(
    con,
    symbols: list[str] | None = None,
    stale_days: int = 7,
    progress_cb: Callable[[int, int, str, int], None] | None = None,
) -> dict[str, Any]:
    """Download financial results PDFs from DPS announcements.

    Smart mode (symbols=None): only checks symbols not synced in `stale_days` days.
    Tracks per-symbol sync status in `announcements_sync_status` table.

    Args:
        con: SQLite connection.
        symbols: Explicit list of symbols. If None, uses smart stale detection.
        stale_days: Days before re-checking a symbol (used when symbols=None).
        progress_cb: Progress callback (done, total, symbol, found_count).

    Returns:
        Summary dict.
    """
    if symbols is None:
        symbols, _ = get_stale_symbols(con, stale_days)

    if not symbols:
        return {"total_symbols": 0, "message": "No symbols to process."}

    # Normalize to base symbols (strip XD/XB/XR/NC/WU suffixes)
    from ..db.repositories.symbols import normalize_symbol
    seen: set[str] = set()
    clean: list[str] = []
    for s in symbols:
        base, _ = normalize_symbol(s.upper())
        if base not in seen:
            seen.add(base)
            clean.append(base)

    result = download_dps_financials(clean, progress_cb=progress_cb)

    # Record sync status per symbol
    for detail in result.get("details", []):
        sym = detail.get("symbol", "")
        if not sym:
            continue
        found = detail.get("found", 0)
        errors = detail.get("errors", 0)
        if errors > 0 and found == 0:
            _upsert_sync_status(con, sym, found, "failed", f"{errors} errors")
        else:
            _upsert_sync_status(con, sym, found, "completed")

    return result
