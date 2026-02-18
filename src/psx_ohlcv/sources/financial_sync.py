"""Financial Statement Sync Orchestrator.

Orchestrates downloading and parsing of financial report PDFs from two sources:
1. PSX portal (financials.psx.com.pk) — standardized PDFs, 10-30 pages
2. Company IR websites (local PDFs in /mnt/e/psxsymbolfin/) — varied format, 100+ pages

Annual/final reports take priority — quarterly data is only used when no
annual report exists for that fiscal year.

Usage:
    from psx_ohlcv.sources.financial_sync import sync_psx_financials, sync_ir_financials

    con = connect(); init_schema(con)
    result = sync_psx_financials(con, symbols=["HBL", "FFC"], years=5)
    result = sync_ir_financials(con, symbols=["LUCK"], years=5)
"""

import logging
import os
import time
from pathlib import Path
from typing import Any, Callable

from ..db.connection import connect, init_schema
from ..db.repositories.company import (
    get_company_financials,
    upsert_company_financials,
)
from ..db.repositories.financials import (
    compute_ratios_from_financials,
    get_parse_summary,
    is_pdf_parsed,
    is_psx_pdf_parsed,
    pdf_hash,
    upsert_pdf_parse_log,
)
from .financial_parser import (
    flatten_parsed_to_financials,
    parse_pdf,
)
from .report_parser import (
    _download_pdf,
    _resolve_period_end,
    get_report_links,
    is_bank_symbol,
)

logger = logging.getLogger("psx_ohlcv.financial_sync")

# ---------------------------------------------------------------------------
# PSX Portal Sync
# ---------------------------------------------------------------------------


def sync_psx_financials(
    con=None,
    symbols: list[str] | None = None,
    years: int = 5,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> dict[str, Any]:
    """Download and parse financial PDFs from PSX portal.

    Processes all available reports for each symbol, prioritizing annual
    reports over quarterly. Skips already-parsed PDFs (by pdf_id).

    Args:
        con: SQLite connection. If None, creates a new one.
        symbols: List of symbols to process. If None, processes all.
        years: Number of years of history to parse.
        progress_cb: Callback(symbol, current_idx, total) for progress updates.

    Returns:
        Dict with: symbols_processed, pdfs_parsed, pdfs_skipped, pdfs_failed,
                    total_items_extracted, errors
    """
    if con is None:
        con = connect()
        init_schema(con)

    result = {
        "symbols_processed": 0,
        "pdfs_parsed": 0,
        "pdfs_skipped": 0,
        "pdfs_failed": 0,
        "total_items_extracted": 0,
        "errors": [],
    }

    # Get symbol list
    if symbols is None:
        rows = con.execute(
            "SELECT DISTINCT symbol FROM symbols ORDER BY symbol"
        ).fetchall()
        symbols = [r[0] for r in rows]
    else:
        symbols = [s.upper() for s in symbols]

    total = len(symbols)

    for idx, symbol in enumerate(symbols):
        if progress_cb:
            progress_cb(symbol, idx + 1, total)

        try:
            _sync_psx_symbol(con, symbol, years, result)
            result["symbols_processed"] += 1
        except Exception as e:
            logger.warning("Error syncing PSX financials for %s: %s", symbol, e)
            result["errors"].append(f"{symbol}: {e}")

        # Rate limit: 1s between symbols to avoid hammering PSX
        if idx < total - 1:
            time.sleep(1.0)

    return result


def _sync_psx_symbol(
    con,
    symbol: str,
    years: int,
    result: dict,
) -> None:
    """Sync all reports for one symbol from PSX portal."""
    is_bank = is_bank_symbol(con, symbol)

    try:
        links = get_report_links(symbol)
    except Exception as e:
        logger.debug("No report links for %s: %s", symbol, e)
        return

    if not links:
        return

    # Filter by year range
    import re

    current_year = int(time.strftime("%Y"))
    min_year = current_year - years

    filtered = []
    for link in links:
        year_match = re.search(r"(\d{4})", link.get("period_ended", ""))
        if year_match and int(year_match.group(1)) >= min_year:
            filtered.append(link)

    # Sort: annual first, then by period (newest first) — annual priority
    def sort_key(link):
        is_annual = 0 if link.get("report_type", "") == "annual" else 1
        pe = link.get("period_ended", "")
        return (is_annual, pe)

    filtered.sort(key=sort_key, reverse=True)

    # Track which fiscal years already have annual data
    annual_years: set[str] = set()

    for link in filtered:
        pdf_id = link.get("pdf_id", "")

        # Skip already parsed
        if pdf_id and is_psx_pdf_parsed(con, symbol, pdf_id):
            result["pdfs_skipped"] += 1
            continue

        # Check if this quarterly report should be skipped (annual exists)
        resolved = _resolve_period_end(
            link.get("period_ended", ""), link.get("report_type", "")
        )
        if resolved:
            period_end, period_type = resolved
            year_match = re.search(r"(\d{4})", period_end)
            fiscal_year = year_match.group(1) if year_match else ""

            if period_type == "annual":
                annual_years.add(fiscal_year)
            elif period_type == "quarterly" and fiscal_year in annual_years:
                # Skip quarterly if annual exists for this year
                result["pdfs_skipped"] += 1
                continue

        # Download and parse
        t0 = time.monotonic()
        try:
            pdf_bytes = _download_pdf(link["url"])
        except Exception as e:
            _log_parse_attempt(
                con, symbol, "psx_pdf", pdf_id=pdf_id,
                status="failed", error=f"Download failed: {e}",
                is_bank=is_bank,
            )
            result["pdfs_failed"] += 1
            time.sleep(0.5)
            continue

        content_hash = pdf_hash(pdf_bytes)
        parsed = parse_pdf(pdf_bytes, is_bank=is_bank, source="psx_pdf")
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        # Count items
        items = len(parsed.get("income_statement", {})) + len(
            parsed.get("balance_sheet", {})
        )

        # Determine period from PDF or from DPS metadata
        pi = parsed.get("period_info", {})
        pdf_period_end = pi.get("period_end_date")
        pdf_period_type = pi.get("period_type")

        if resolved:
            period_end, period_type = resolved
        elif pdf_period_end:
            period_end = pdf_period_end
            period_type = pdf_period_type or "annual"
        else:
            period_end = link.get("period_ended", "")
            period_type = link.get("report_type", "annual")

        if items == 0:
            _log_parse_attempt(
                con, symbol, "psx_pdf", pdf_id=pdf_id, pdf_hash=content_hash,
                status="failed", error="No items extracted",
                is_bank=is_bank, confidence=0.0, elapsed_ms=elapsed_ms,
                period_end=period_end, period_type=period_type,
            )
            result["pdfs_failed"] += 1
            continue

        # Store financial data
        entries = flatten_parsed_to_financials(
            parsed, symbol, period_end, period_type
        )
        for entry in entries:
            upsert_company_financials(con, symbol, [entry])

        # Compute ratios
        compute_ratios_from_financials(con, symbol)

        # Log success
        status = "success" if parsed.get("confidence", 0) >= 0.5 else "partial"
        _log_parse_attempt(
            con, symbol, "psx_pdf", pdf_id=pdf_id, pdf_hash=content_hash,
            status=status, items=items, is_bank=is_bank,
            confidence=parsed.get("confidence", 0.0),
            elapsed_ms=elapsed_ms,
            period_end=period_end, period_type=period_type,
        )

        result["pdfs_parsed"] += 1
        result["total_items_extracted"] += items

        # Small delay between PDFs
        time.sleep(0.5)


# ---------------------------------------------------------------------------
# IR PDF Sync (local files)
# ---------------------------------------------------------------------------


def sync_ir_financials(
    con=None,
    symbols: list[str] | None = None,
    base_dir: str = "/mnt/e/psxsymbolfin",
    years: int = 5,
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> dict[str, Any]:
    """Parse already-downloaded IR PDFs from local storage.

    For each symbol directory:
    1. List PDF files
    2. SHA-256 hash check → skip if already parsed
    3. Parse, store, compute ratios

    Args:
        con: SQLite connection. If None, creates a new one.
        symbols: List of symbols to process. If None, scans all directories.
        base_dir: Root directory containing per-symbol PDF directories.
        years: Number of years of history (filters by filename year).
        progress_cb: Callback(symbol, current_idx, total) for progress updates.

    Returns:
        Dict with sync stats.
    """
    if con is None:
        con = connect()
        init_schema(con)

    result = {
        "symbols_processed": 0,
        "pdfs_parsed": 0,
        "pdfs_skipped": 0,
        "pdfs_failed": 0,
        "total_items_extracted": 0,
        "errors": [],
    }

    base = Path(base_dir)
    if not base.exists():
        result["errors"].append(f"Base directory not found: {base_dir}")
        return result

    # Get symbol directories
    if symbols is None:
        dirs = sorted(
            [d for d in base.iterdir() if d.is_dir()],
            key=lambda d: d.name,
        )
    else:
        dirs = [base / s.upper() for s in symbols if (base / s.upper()).is_dir()]

    total = len(dirs)

    for idx, sym_dir in enumerate(dirs):
        symbol = sym_dir.name.upper()
        if progress_cb:
            progress_cb(symbol, idx + 1, total)

        try:
            _sync_ir_symbol(con, symbol, sym_dir, years, result)
            result["symbols_processed"] += 1
        except Exception as e:
            logger.warning("Error syncing IR PDFs for %s: %s", symbol, e)
            result["errors"].append(f"{symbol}: {e}")

    return result


def _sync_ir_symbol(
    con,
    symbol: str,
    sym_dir: Path,
    years: int,
    result: dict,
) -> None:
    """Sync all IR PDFs for one symbol from local storage."""
    import re

    is_bank = is_bank_symbol(con, symbol)

    # List PDF files, filter by year
    current_year = int(time.strftime("%Y"))
    min_year = current_year - years

    pdfs = sorted(sym_dir.glob("*.pdf"), key=lambda p: p.name)

    # Sort: annual-looking files first, then by year (newest first)
    def sort_key(p: Path):
        name = p.stem.lower()
        is_annual = 0 if "annual" in name or "year" in name else 1
        year_match = re.search(r"(\d{4})", name)
        year = int(year_match.group(1)) if year_match else 0
        return (is_annual, -year, name)

    pdfs.sort(key=sort_key)

    for pdf_path in pdfs:
        # Filter by year if possible
        year_match = re.search(r"(\d{4})", pdf_path.stem)
        if year_match and int(year_match.group(1)) < min_year:
            continue

        # Read file and compute hash
        try:
            pdf_bytes = pdf_path.read_bytes()
        except Exception as e:
            result["pdfs_failed"] += 1
            result["errors"].append(f"{symbol}/{pdf_path.name}: read error: {e}")
            continue

        content_hash = pdf_hash(pdf_bytes)

        # Skip already parsed
        if is_pdf_parsed(con, symbol, content_hash):
            result["pdfs_skipped"] += 1
            continue

        # Parse
        t0 = time.monotonic()
        parsed = parse_pdf(pdf_bytes, is_bank=is_bank, source="ir_pdf")
        elapsed_ms = int((time.monotonic() - t0) * 1000)

        items = len(parsed.get("income_statement", {})) + len(
            parsed.get("balance_sheet", {})
        )

        pi = parsed.get("period_info", {})
        period_end = pi.get("period_end_date") or ""
        period_type = pi.get("period_type") or "annual"

        # Fallback: try to extract year from filename
        if not period_end and year_match:
            period_end = year_match.group(1)
            period_type = "annual"

        if items == 0:
            _log_parse_attempt(
                con, symbol, "ir_pdf", pdf_path=str(pdf_path),
                pdf_hash=content_hash, status="failed",
                error="No items extracted", is_bank=is_bank,
                elapsed_ms=elapsed_ms, file_size=len(pdf_bytes),
            )
            result["pdfs_failed"] += 1
            continue

        # Store financial data
        entries = flatten_parsed_to_financials(
            parsed, symbol, period_end, period_type
        )
        for entry in entries:
            upsert_company_financials(con, symbol, [entry])

        # Compute ratios
        compute_ratios_from_financials(con, symbol)

        # Log success
        status = "success" if parsed.get("confidence", 0) >= 0.5 else "partial"
        _log_parse_attempt(
            con, symbol, "ir_pdf", pdf_path=str(pdf_path),
            pdf_hash=content_hash, status=status, items=items,
            is_bank=is_bank, confidence=parsed.get("confidence", 0.0),
            elapsed_ms=elapsed_ms, file_size=len(pdf_bytes),
            period_end=period_end, period_type=period_type,
        )

        result["pdfs_parsed"] += 1
        result["total_items_extracted"] += items


# ---------------------------------------------------------------------------
# Backfill Service
# ---------------------------------------------------------------------------


def backfill_financials(
    con=None,
    symbols: list[str] | None = None,
    years: int = 10,
    source: str = "both",
    progress_cb: Callable[[str, int, int], None] | None = None,
) -> dict[str, Any]:
    """Deep historical backfill (>5 years).

    Runs both PSX portal and IR PDF sync with extended year range.
    """
    if con is None:
        con = connect()
        init_schema(con)

    combined = {
        "symbols_processed": 0,
        "pdfs_parsed": 0,
        "pdfs_skipped": 0,
        "pdfs_failed": 0,
        "total_items_extracted": 0,
        "errors": [],
    }

    if source in ("both", "psx_pdf"):
        psx_result = sync_psx_financials(
            con, symbols=symbols, years=years, progress_cb=progress_cb
        )
        _merge_results(combined, psx_result)

    if source in ("both", "ir_pdf"):
        ir_result = sync_ir_financials(
            con, symbols=symbols, years=years, progress_cb=progress_cb
        )
        _merge_results(combined, ir_result)

    return combined


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _log_parse_attempt(
    con,
    symbol: str,
    source: str,
    *,
    pdf_id: str | None = None,
    pdf_path: str | None = None,
    pdf_hash: str | None = None,
    status: str = "failed",
    error: str | None = None,
    items: int = 0,
    is_bank: bool = False,
    confidence: float = 0.0,
    elapsed_ms: int = 0,
    file_size: int | None = None,
    period_end: str | None = None,
    period_type: str | None = None,
) -> None:
    """Log a parse attempt to pdf_parse_log."""
    entry = {
        "symbol": symbol.upper(),
        "pdf_source": source,
        "pdf_id": pdf_id,
        "pdf_path": pdf_path,
        "pdf_hash": pdf_hash or "",
        "parse_status": status,
        "items_extracted": items,
        "is_bank": is_bank,
        "confidence": confidence,
        "error_message": error,
        "parse_duration_ms": elapsed_ms,
        "file_size": file_size,
        "period_end": period_end,
        "period_type": period_type,
    }
    upsert_pdf_parse_log(con, entry)


def _merge_results(combined: dict, part: dict) -> None:
    """Merge partial sync result into combined."""
    for key in ("symbols_processed", "pdfs_parsed", "pdfs_skipped",
                "pdfs_failed", "total_items_extracted"):
        combined[key] += part.get(key, 0)
    combined["errors"].extend(part.get("errors", []))
