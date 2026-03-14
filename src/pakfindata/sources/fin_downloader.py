"""Financial statement PDF downloader.

Visits company investor-relations pages, finds PDF links for the last
N years of financial reports, and downloads them into
``/mnt/e/psxsymbolfin/{SYMBOL}/`` organised by date.

Usage:
    from pakfindata.sources.fin_downloader import download_financials
    result = download_financials(con, symbols=["OGDC", "HBL"], years=2)
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urljoin, urlparse

import aiohttp
from lxml import html

logger = logging.getLogger("pakfindata")

BASE_DIR = Path("/mnt/e/psxsymbolfin")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

# Keywords that indicate a financial report PDF (in link text or URL)
REPORT_KEYWORDS = re.compile(
    r"annual.?report|quarterly.?report|half.?year|financial.?statement|"
    r"financial.?report|first.?quarter|second.?quarter|third.?quarter|"
    r"nine.?month|9m.?report|hy.?report|"
    r"condensed.?interim|unconsolidated|consolidated.?financial",
    re.I,
)

# Exclude presentations, transcripts, forms, etc.
EXCLUDE_KEYWORDS = re.compile(
    r"presentation|transcript|conference.?call|request.?form|proxy|"
    r"notice.?of|code.?of|governance|compliance|csr|sustainability",
    re.I,
)

# Pattern to extract year from text/URL (2015-2029)
YEAR_PATTERN = re.compile(r"20(1[5-9]|2[0-9])")


def _extract_years(text: str) -> list[int]:
    """Extract all 4-digit years (2020-2029) from text."""
    return [int(f"20{m}") for m in YEAR_PATTERN.findall(text)]


def _is_financial_pdf(href: str, text: str) -> bool:
    """Check if a link points to a financial report PDF."""
    href_lower = href.lower()
    if not href_lower.endswith(".pdf"):
        return False
    combined = f"{text} {unquote(href)}"
    if EXCLUDE_KEYWORDS.search(combined):
        return False
    return bool(REPORT_KEYWORDS.search(combined))


def _year_matches(href: str, text: str, target_years: set[int]) -> bool:
    """Check if the PDF is from one of the target years."""
    combined = f"{text} {unquote(href)}"
    years = _extract_years(combined)
    if not years:
        return False  # skip PDFs with no identifiable year
    return any(y in target_years for y in years)


def _safe_filename(url: str, symbol: str) -> str:
    """Generate a clean filename from a PDF URL."""
    parsed = urlparse(url)
    name = unquote(os.path.basename(parsed.path))
    # Clean up weird characters
    name = re.sub(r"[^\w\-. ()]+", "_", name)
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    return name


async def _find_pdf_links(
    session: aiohttp.ClientSession,
    page_url: str,
    target_years: set[int],
    timeout: int = 20,
) -> list[dict[str, str]]:
    """Fetch a financial page and extract PDF links.

    Returns list of dicts: {url, text, year}
    """
    pdfs: list[dict[str, str]] = []
    try:
        async with session.get(
            page_url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
            ssl=False,
        ) as resp:
            if resp.status != 200:
                logger.warning("PDF scan %s got HTTP %d", page_url, resp.status)
                return pdfs
            body = await resp.text(errors="replace")
    except Exception as e:
        logger.warning("PDF scan %s failed: %s", page_url, e)
        return pdfs

    try:
        tree = html.fromstring(body)
        tree.make_links_absolute(page_url)
    except Exception:
        return pdfs

    seen: set[str] = set()
    for a in tree.xpath("//a[@href]"):
        href = a.get("href", "").strip()
        text = a.text_content().strip()
        if not href or href in seen:
            continue
        seen.add(href)

        if _is_financial_pdf(href, text) and _year_matches(href, text, target_years):
            years = _extract_years(f"{text} {unquote(href)}")
            pdfs.append({
                "url": href,
                "text": text[:120],
                "year": str(max(years)) if years else "unknown",
            })

    return pdfs


async def _download_pdf(
    session: aiohttp.ClientSession,
    url: str,
    dest: Path,
    timeout: int = 120,
) -> bool:
    """Download a single PDF file."""
    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout),
            allow_redirects=True,
            ssl=False,
        ) as resp:
            if resp.status != 200:
                logger.warning("Download %s -> HTTP %d", url, resp.status)
                return False
            data = await resp.read()
            dest.write_bytes(data)
            logger.info("Downloaded %s (%d KB)", dest.name, len(data) // 1024)
            return True
    except Exception as e:
        logger.warning("Download %s failed: %s", url, e)
        return False


async def _process_symbol(
    session: aiohttp.ClientSession,
    symbol: str,
    financial_urls: list[str],
    target_years: set[int],
    rate_limit: float = 0.5,
) -> dict[str, Any]:
    """Process one symbol: find PDFs, download them.

    Returns result dict with counts.
    """
    symbol_dir = BASE_DIR / symbol
    symbol_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, Any] = {
        "symbol": symbol,
        "pdfs_found": 0,
        "downloaded": 0,
        "skipped": 0,
        "errors": 0,
        "files": [],
    }

    # Phase 1: Find PDF links from all financial pages
    all_pdfs: list[dict[str, str]] = []
    seen_urls: set[str] = set()

    for page_url in financial_urls:
        await asyncio.sleep(rate_limit)
        pdfs = await _find_pdf_links(session, page_url, target_years)
        for p in pdfs:
            if p["url"] not in seen_urls:
                seen_urls.add(p["url"])
                all_pdfs.append(p)

    result["pdfs_found"] = len(all_pdfs)

    # Phase 2: Download PDFs
    for pdf in all_pdfs:
        filename = _safe_filename(pdf["url"], symbol)
        dest = symbol_dir / filename

        if dest.exists() and dest.stat().st_size > 1000:
            result["skipped"] += 1
            result["files"].append({"file": filename, "status": "exists"})
            continue

        await asyncio.sleep(rate_limit)
        ok = await _download_pdf(session, pdf["url"], dest)
        if ok:
            result["downloaded"] += 1
            result["files"].append({"file": filename, "status": "ok"})
        else:
            result["errors"] += 1
            result["files"].append({"file": filename, "status": "error"})

    return result


async def _run_batch(
    symbols_urls: list[tuple[str, list[str]]],
    target_years: set[int],
    max_concurrent: int = 5,
    progress_cb: Callable[[int, int, str], None] | None = None,
) -> list[dict[str, Any]]:
    """Download financial PDFs for multiple symbols."""
    semaphore = asyncio.Semaphore(max_concurrent)
    results: list[dict[str, Any]] = []
    total = len(symbols_urls)
    done = 0

    connector = aiohttp.TCPConnector(limit=max_concurrent * 2, ssl=False)

    async with aiohttp.ClientSession(connector=connector, headers=HEADERS) as session:

        async def _wrapped(sym: str, urls: list[str]) -> dict:
            nonlocal done
            async with semaphore:
                r = await _process_symbol(session, sym, urls, target_years)
                done += 1
                if progress_cb:
                    try:
                        progress_cb(done, total, sym)
                    except Exception:
                        pass
                return r

        tasks = [_wrapped(sym, urls) for sym, urls in symbols_urls]
        results = await asyncio.gather(*tasks, return_exceptions=False)

    return list(results)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def download_financials(
    con,
    symbols: list[str] | None = None,
    years: int = 5,
    year_from: int | None = None,
    year_to: int | None = None,
    progress_cb: Callable[[int, int, str], None] | None = None,
) -> dict[str, Any]:
    """Download financial statement PDFs for scanned symbols.

    Args:
        con: SQLite connection.
        symbols: Specific symbols to download (default: all with financial URLs).
        years: Number of years back to download (default 5, used if year_from/year_to not set).
        year_from: Start year (inclusive). Overrides ``years`` if set.
        year_to: End year (inclusive). Defaults to current year.
        progress_cb: Progress callback (done, total, symbol).

    Returns:
        Summary dict with per-symbol results.
    """
    BASE_DIR.mkdir(parents=True, exist_ok=True)

    current_year = datetime.now().year
    if year_from is not None:
        yr_to = year_to if year_to is not None else current_year
        target_years = set(range(year_from, yr_to + 1))
    else:
        target_years = {current_year - i for i in range(years + 1)}

    # Get symbols with financial URLs from website scan
    query = """
        SELECT symbol, financial_urls
        FROM company_website_scan
        WHERE has_financial_page = 1
          AND financial_urls IS NOT NULL
          AND financial_urls != '[]'
    """
    params: list = []
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        query += f" AND symbol IN ({placeholders})"
        params = [s.upper() for s in symbols]

    rows = con.execute(query, params).fetchall()

    symbols_urls: list[tuple[str, list[str]]] = []
    for row in rows:
        sym = row[0]
        try:
            urls = json.loads(row[1]) if isinstance(row[1], str) else row[1]
        except (json.JSONDecodeError, TypeError):
            continue
        if urls:
            symbols_urls.append((sym, urls if isinstance(urls, list) else [urls]))

    if not symbols_urls:
        return {"total": 0, "message": "No symbols with financial URLs found."}

    logger.info("Downloading financials for %d symbols (years: %s)", len(symbols_urls), target_years)

    results = asyncio.run(_run_batch(symbols_urls, target_years, progress_cb=progress_cb))

    total_found = sum(r["pdfs_found"] for r in results)
    total_downloaded = sum(r["downloaded"] for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    total_errors = sum(r["errors"] for r in results)

    summary = {
        "total_symbols": len(results),
        "pdfs_found": total_found,
        "downloaded": total_downloaded,
        "skipped_existing": total_skipped,
        "errors": total_errors,
        "target_years": sorted(target_years),
        "base_dir": str(BASE_DIR),
        "details": results,
    }
    logger.info(
        "Download complete: %d symbols, %d found, %d downloaded, %d skipped, %d errors",
        len(results), total_found, total_downloaded, total_skipped, total_errors,
    )
    return summary


# ---------------------------------------------------------------------------
# Import downloaded PDFs into database
# ---------------------------------------------------------------------------

# Filename patterns to classify report type and period
_ANNUAL_RE = re.compile(
    r"annual.?report|AR[_\s-]?\d{4}|full.?year",
    re.I,
)
_QUARTERLY_RE = re.compile(
    r"quarter|Q[1-4]|half.?year|interim|nine.?month|six.?month|three.?month|HY|9M|6M|3M",
    re.I,
)
_SKIP_FILE_RE = re.compile(
    r"presentation|transcript|conference|request.?form|proxy|"
    r"governance|compliance|csr|sustainability|summary",
    re.I,
)


def _classify_pdf_filename(filename: str) -> dict[str, str | None]:
    """Classify a PDF filename to extract report type and fiscal year hint.

    Returns dict with 'report_type' ('annual'|'quarterly') and 'year_hint'.
    """
    name = Path(filename).stem

    # Skip non-financial files
    if _SKIP_FILE_RE.search(name):
        return {"report_type": None, "year_hint": None, "skip_reason": "non-financial"}

    # Detect type
    if _ANNUAL_RE.search(name):
        report_type = "annual"
    elif _QUARTERLY_RE.search(name):
        report_type = "quarterly"
    else:
        # Default: try to parse the PDF to figure it out
        report_type = "unknown"

    # Extract year hint
    year_match = re.search(r"20(1[5-9]|2[0-9])", name)
    year_hint = f"20{year_match.group(1)}" if year_match else None

    # FY pattern: FY26, FY2026, FY25-26
    fy_match = re.search(r"FY[_\s-]?(\d{2,4})", name, re.I)
    if fy_match and not year_hint:
        y = fy_match.group(1)
        year_hint = f"20{y}" if len(y) == 2 else y

    return {"report_type": report_type, "year_hint": year_hint}


def scan_symbol_pdfs(symbol_dir: Path) -> list[dict]:
    """Scan a symbol directory for financial PDF files.

    Returns list of dicts with: path, filename, report_type, year_hint, size_kb.
    """
    results = []
    if not symbol_dir.is_dir():
        return results

    for pdf_path in sorted(symbol_dir.glob("*.pdf")):
        info = _classify_pdf_filename(pdf_path.name)
        if info.get("skip_reason"):
            continue

        results.append({
            "path": str(pdf_path),
            "filename": pdf_path.name,
            "report_type": info["report_type"],
            "year_hint": info["year_hint"],
            "size_kb": round(pdf_path.stat().st_size / 1024, 1),
        })

    return results


def import_symbol_pdfs(
    con,
    symbol: str,
    base_dir: Path = BASE_DIR,
    dry_run: bool = False,
    progress_callback: Callable | None = None,
) -> dict[str, Any]:
    """Parse all downloaded PDFs for a symbol and upsert into company_financials.

    Args:
        con: SQLite connection
        symbol: Stock symbol
        base_dir: Root directory containing symbol folders
        dry_run: If True, parse but don't write to DB
        progress_callback: fn(filename, status, detail)

    Returns:
        Summary dict with counts and details.
    """
    from .financial_parser import flatten_parsed_to_financials, parse_ir_pdf
    from .report_parser import is_bank_symbol
    from ..db.repositories.company import upsert_company_financials

    symbol = symbol.upper()
    symbol_dir = base_dir / symbol

    if not symbol_dir.is_dir():
        return {"symbol": symbol, "error": f"No directory: {symbol_dir}", "parsed": 0, "upserted": 0}

    pdfs = scan_symbol_pdfs(symbol_dir)
    if not pdfs:
        return {"symbol": symbol, "error": "No financial PDFs found", "parsed": 0, "upserted": 0}

    is_bank = is_bank_symbol(con, symbol)

    summary: dict[str, Any] = {
        "symbol": symbol,
        "is_bank": is_bank,
        "total_pdfs": len(pdfs),
        "parsed": 0,
        "upserted": 0,
        "skipped": 0,
        "errors": [],
        "details": [],
    }

    for pdf_info in pdfs:
        fname = pdf_info["filename"]
        fpath = pdf_info["path"]

        try:
            with open(fpath, "rb") as f:
                pdf_bytes = f.read()

            parsed = parse_ir_pdf(pdf_bytes, symbol=symbol, is_bank=is_bank)

            confidence = parsed.get("confidence", 0.0)
            warnings = parsed.get("warnings", [])
            pl = parsed.get("income_statement", {})
            bs = parsed.get("balance_sheet", {})

            period_info = parsed.get("period_info", {})
            period_end = period_info.get("period_end_date") or pdf_info.get("year_hint")
            period_type = period_info.get("period_type") or pdf_info.get("report_type") or "annual"
            if period_type == "unknown":
                period_type = "annual"

            detail = {
                "file": fname,
                "confidence": confidence,
                "period_end": period_end,
                "period_type": period_type,
                "pl_fields": len(pl),
                "bs_fields": len(bs),
                "warnings": warnings,
            }

            if not period_end:
                detail["status"] = "skipped"
                detail["reason"] = "no period detected"
                summary["skipped"] += 1
                summary["details"].append(detail)
                if progress_callback:
                    progress_callback(fname, "skip", "no period detected")
                continue

            if not pl and not bs:
                detail["status"] = "skipped"
                detail["reason"] = "no P&L or BS data extracted"
                summary["skipped"] += 1
                summary["details"].append(detail)
                if progress_callback:
                    progress_callback(fname, "skip", "no data extracted")
                continue

            summary["parsed"] += 1

            if not dry_run:
                entries = flatten_parsed_to_financials(parsed, symbol, period_end, period_type)
                rows = upsert_company_financials(con, symbol, entries)
                summary["upserted"] += rows
                detail["rows_upserted"] = rows

            detail["status"] = "ok"
            summary["details"].append(detail)

            if progress_callback:
                progress_callback(fname, "ok", f"period={period_end} pl={len(pl)} bs={len(bs)}")

        except Exception as e:
            summary["errors"].append({"file": fname, "error": str(e)})
            summary["details"].append({"file": fname, "status": "error", "error": str(e)})
            if progress_callback:
                progress_callback(fname, "error", str(e))

    return summary


def import_all_pdfs(
    con,
    base_dir: Path = BASE_DIR,
    symbols: list[str] | None = None,
    dry_run: bool = False,
    progress_callback: Callable | None = None,
) -> dict[str, Any]:
    """Import financial PDFs for all symbols found in base_dir.

    Args:
        con: SQLite connection
        base_dir: Root directory (default /mnt/e/psxsymbolfin)
        symbols: Optional filter — only these symbols
        dry_run: Parse only, don't write to DB
        progress_callback: fn(symbol, filename, status, detail)

    Returns:
        Aggregate summary.
    """
    if not base_dir.is_dir():
        return {"error": f"Directory not found: {base_dir}", "symbols_processed": 0}

    # Find all symbol directories
    if symbols:
        dirs = [base_dir / s.upper() for s in symbols]
        dirs = [d for d in dirs if d.is_dir()]
    else:
        dirs = sorted([d for d in base_dir.iterdir() if d.is_dir()])

    total_summary: dict[str, Any] = {
        "base_dir": str(base_dir),
        "symbols_found": len(dirs),
        "symbols_processed": 0,
        "total_parsed": 0,
        "total_upserted": 0,
        "total_errors": 0,
        "per_symbol": [],
    }

    for sym_dir in dirs:
        symbol = sym_dir.name.upper()

        def _cb(fname, status, detail, _sym=symbol):
            if progress_callback:
                progress_callback(_sym, fname, status, detail)

        result = import_symbol_pdfs(con, symbol, base_dir=base_dir, dry_run=dry_run, progress_callback=_cb)
        total_summary["symbols_processed"] += 1
        total_summary["total_parsed"] += result.get("parsed", 0)
        total_summary["total_upserted"] += result.get("upserted", 0)
        total_summary["total_errors"] += len(result.get("errors", []))
        total_summary["per_symbol"].append(result)

    return total_summary
