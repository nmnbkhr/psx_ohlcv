"""Financial Report PDF parser for PSX companies.

Downloads and parses financial reports from financials.psx.com.pk to extract
detailed P&L data not available on the DPS company page — specifically:
- Banks: Mark-up/Interest earned & expensed (for gross margin)
- Non-banks: Cost of sales, Gross profit (if not on DPS page)

Usage:
    from psx_ohlcv.sources.report_parser import (
        get_report_links,
        parse_bank_pl,
        parse_nonbank_pl,
        fetch_and_parse_report,
    )
"""

import io
import logging
import re
import sqlite3
from typing import Any

import requests
from lxml import html

logger = logging.getLogger("psx_ohlcv.report_parser")

# PSX reports page
DPS_REPORTS_URL = "https://dps.psx.com.pk/company/reports/{symbol}"
PDF_DOWNLOAD_URL = "https://financials.psx.com.pk/lib/DownloadPDF.php?id={pdf_id}"

# Bank sector codes from PSX (numeric codes)
BANK_SECTOR_CODES = {"0807", "0813"}  # COMMERCIAL BANKS, INV. BANKS
BANK_SECTOR_KEYWORDS = {"BANK", "MODARABA"}


def get_report_links(symbol: str) -> list[dict]:
    """Get available financial report PDF links from PSX.

    Args:
        symbol: Stock symbol (e.g., 'HBL')

    Returns:
        List of dicts with keys: report_type, period_ended, posting_date, url, pdf_id
    """
    url = DPS_REPORTS_URL.format(symbol=symbol.upper())
    resp = requests.get(url, timeout=15)
    resp.raise_for_status()

    tree = html.fromstring(resp.content)
    rows = tree.xpath("//table//tr")
    reports = []

    for row in rows:
        cells = row.xpath(".//td//text() | .//th//text()")
        cells = [c.strip() for c in cells if c.strip()]
        links = row.xpath(".//a/@href")

        if len(cells) >= 3 and links:
            pdf_url = links[0]
            # Extract pdf_id from URL
            pdf_id_match = re.search(r"id=(.+)", pdf_url)
            pdf_id = pdf_id_match.group(1) if pdf_id_match else ""

            reports.append({
                "report_type": cells[0].lower(),  # 'annual' or 'quarterly'
                "period_ended": cells[1],
                "posting_date": cells[2],
                "url": pdf_url,
                "pdf_id": pdf_id,
            })

    return reports


def _download_pdf(url: str, timeout: int = 60) -> bytes:
    """Download PDF from URL."""
    resp = requests.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()
    return resp.content


def _fix_split_numbers(line: str) -> str:
    """Fix numbers split by column boundaries in PDF text extraction.

    Some bank PDFs have fixed-width columns that cause pdfplumber to split
    leading digits from the rest of the number, e.g.:
        "7 6,529,292" should be "76,529,292"
        "1 05,264,607" should be "105,264,607"

    The regex uses a lookbehind to only match a digit preceded by whitespace
    (column boundary), followed by whitespace, then a comma-formatted number.
    This avoids merging correctly-formatted numbers like "26 76,529,292".
    """
    return re.sub(r"(?<=\s)(\d)\s+(\d{1,2}(?:,\d{3})+)", r"\1\2", line)


def _extract_numbers(text: str) -> list[float | None]:
    """Extract comma-separated numbers from a line of text.

    Handles: 503,403,470  (0.15)  38.70  -  (negative in brackets)
    """
    # Find all number patterns: digits with optional commas, decimals, brackets for negative
    pattern = r"\(?([\d,]+(?:\.\d+)?)\)?"
    matches = re.finditer(pattern, text)

    numbers = []
    for m in matches:
        raw = m.group(0)
        val_str = m.group(1).replace(",", "")
        try:
            val = float(val_str)
            # Bracketed numbers are negative
            if raw.startswith("(") and raw.endswith(")"):
                val = -val
            numbers.append(val)
        except ValueError:
            continue

    return numbers


def _find_pl_pages(pdf_pages: list) -> list[int]:
    """Find pages containing the P&L statement.

    Checks only the FIRST 2 lines (the page title) for P&L keywords.
    This avoids false positives from body text like "reclassified to
    profit and loss account" on Comprehensive Income pages.

    Returns list of page indices (0-based).
    """
    pl_pages = []
    for i, page in enumerate(pdf_pages):
        text = page.extract_text()
        if not text:
            continue

        # Only check the TITLE area (first 2 lines) — not body text
        title_lines = "\n".join(text.split("\n")[:2]).upper()

        is_pl = (
            "PROFIT AND LOSS" in title_lines
            or "PROFIT OR LOSS" in title_lines
            or "INCOME STATEMENT" in title_lines
        )
        # Exclude comprehensive income, cash flow, and notes pages
        is_excluded = (
            "COMPREHENSIVE" in title_lines
            or "CASH FLOW" in title_lines
            or "NOTES TO" in title_lines
        )

        if is_pl and not is_excluded:
            pl_pages.append(i)

    return pl_pages


def _extract_period_info(lines: list[str]) -> dict[str, str | None]:
    """Extract period end date and currency/scale from P&L page header.

    Parses lines like:
        "For the year ended December 31, 2024"
        "For the nine months ended September 30, 2025"
        "(Rupees in '000)"

    Returns dict with: period_header, period_end_date, period_type, currency_scale
    """
    info: dict[str, str | None] = {
        "period_header": None,
        "period_end_date": None,
        "period_type": None,
        "currency_scale": None,
    }

    for line in lines[:10]:
        line_stripped = line.strip()

        # Period: "For the year/nine months/quarter ended <Month> <Day>, <Year>"
        period_match = re.search(
            r"for the\s+(year|.*?months?(?:\s+period)?|quarter|half[\s-]?year)\s+ended\s+"
            r"(January|February|March|April|May|June|July|August|September|"
            r"October|November|December)\s+(\d{1,2}),?\s+(\d{4})",
            line_stripped,
            re.IGNORECASE,
        )
        if period_match:
            info["period_header"] = line_stripped
            duration = period_match.group(1).lower()
            month = period_match.group(2)
            day = period_match.group(3)
            year = period_match.group(4)
            info["period_end_date"] = f"{year}-{month}-{day}"

            if "year" in duration and "half" not in duration:
                info["period_type"] = "annual"
            else:
                info["period_type"] = "quarterly"

        # Currency scale: "(Rupees in '000)" or "Amounts in Pakistan Rupees"
        if "'000" in line_stripped or "thousand" in line_stripped.lower():
            info["currency_scale"] = "thousands"
        elif "million" in line_stripped.lower():
            info["currency_scale"] = "millions"

    return info


def _filter_note_refs(numbers: list[float]) -> list[float]:
    """Remove note reference numbers (small integers 1-99) from extracted numbers.

    Bank PDFs have note references like "24" between value groups, e.g.:
        "Mark-up earned  1,389,424  1,460,380  23  245,252,121  257,776,834"
    The "23" is a note reference, not a financial value.

    Also handles note refs at the start: "24  503,403,470  410,299,000"
    """
    if len(numbers) <= 2:
        # 2 or fewer numbers: only strip leading note ref
        if len(numbers) == 2 and abs(numbers[0]) < 100 and abs(numbers[1]) > 1000:
            return [numbers[1]]
        return numbers

    # For 3+ numbers, filter out any value < 100 that sits between larger values
    filtered = []
    for i, n in enumerate(numbers):
        if abs(n) < 100:
            # Keep small numbers only if ALL numbers are small (e.g., ratios)
            has_big = any(abs(x) > 1000 for x in numbers)
            if has_big:
                continue  # skip note reference
        filtered.append(n)
    return filtered if filtered else numbers


def parse_bank_pl(pdf_content: bytes) -> dict[str, Any]:
    """Parse bank P&L from PDF to extract markup earned/expensed.

    Handles varied bank terminology:
    - "Mark-up earned" / "Mark-up / return earned" / "Mark-up / return / interest earned"
    - "Mark-up expensed" / "Mark-up / return expensed" / "Mark-up / return / interest expensed"

    When a page has both consolidated and unconsolidated columns (4 values per line),
    only the first 2 values are kept (current period + prior year). The GPM ratio
    is the same regardless of entity type since ME and MEX come from the same columns.

    Args:
        pdf_content: Raw PDF bytes

    Returns:
        Dict with extracted data and metadata.
    """
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not installed — cannot parse PDF reports")
        return {}

    result: dict[str, Any] = {
        "periods": [],
        "markup_earned": [],
        "markup_expensed": [],
        "net_interest_income": [],
        "profit_after_tax": [],
    }

    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            pl_pages = _find_pl_pages(pdf.pages)
            if not pl_pages:
                logger.debug("No P&L pages found in PDF")
                return result

            # Use the LAST P&L page (unconsolidated) — PSX DPS shows unconsolidated data
            page = pdf.pages[pl_pages[-1]]
            text = page.extract_text()
            if not text:
                return result

            lines = text.split("\n")

            # Extract period and currency from header
            period_info = _extract_period_info(lines)
            result["period_info"] = period_info

            for line in lines:
                # Fix numbers split across columns (e.g., "7 6,529,292" → "76,529,292")
                line = _fix_split_numbers(line)
                line_upper = line.upper()
                numbers = _extract_numbers(line)

                if not numbers:
                    continue

                # Filter out note reference numbers (small integers like 23, 24)
                numbers = _filter_note_refs(numbers)

                # When PDF has consolidated + unconsolidated side by side (4 values),
                # keep only first 2 (current period + prior year from same entity)
                if len(numbers) > 2:
                    numbers = numbers[:2]

                if not numbers:
                    continue

                # Mark-up / return / interest earned (NOT expensed)
                if "EARNED" in line_upper and ("MARK" in line_upper or "INTEREST" in line_upper or "RETURN" in line_upper):
                    if "EXPENS" not in line_upper and not result["markup_earned"]:
                        result["markup_earned"] = numbers
                        continue

                # Mark-up / return / interest expensed (NOT "non mark-up" expenses)
                if "EXPENSED" in line_upper and ("MARK" in line_upper or "INTEREST" in line_upper or "RETURN" in line_upper):
                    if "NON" not in line_upper and "TOTAL NON" not in line_upper and not result["markup_expensed"]:
                        result["markup_expensed"] = numbers
                        continue

                # Net mark-up / interest income (first occurrence only)
                if "NET" in line_upper and ("MARK" in line_upper or "INTEREST" in line_upper) and "INCOME" in line_upper:
                    if "NON" not in line_upper and not result["net_interest_income"]:
                        result["net_interest_income"] = numbers
                        continue

                # Profit after taxation (first occurrence only)
                if "PROFIT AFTER" in line_upper and "TAX" in line_upper:
                    if not result["profit_after_tax"]:
                        result["profit_after_tax"] = numbers
                        continue

    except Exception as e:
        logger.warning("Failed to parse bank P&L from PDF: %s", e)

    return result


def parse_nonbank_pl(pdf_content: bytes) -> dict[str, Any]:
    """Parse non-bank P&L from PDF to extract sales, cost of sales, gross profit.

    Args:
        pdf_content: Raw PDF bytes

    Returns:
        Dict with extracted data per period.
    """
    try:
        import pdfplumber
    except ImportError:
        return {}

    result: dict[str, Any] = {
        "sales": [],
        "cost_of_sales": [],
        "gross_profit": [],
        "profit_after_tax": [],
    }

    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            pl_pages = _find_pl_pages(pdf.pages)
            if not pl_pages:
                return result

            page = pdf.pages[pl_pages[0]]
            text = page.extract_text()
            if not text:
                return result

            lines = text.split("\n")

            for line in lines:
                line_upper = line.upper()
                numbers = _extract_numbers(line)

                if not numbers:
                    continue

                # Net sales / Revenue
                if ("NET SALES" in line_upper or "REVENUE" in line_upper or "TURNOVER" in line_upper) and "COST" not in line_upper:
                    result["sales"] = numbers
                    continue

                # Cost of sales
                if "COST OF" in line_upper and ("SALES" in line_upper or "GOODS" in line_upper or "REVENUE" in line_upper):
                    result["cost_of_sales"] = numbers
                    continue

                # Gross profit
                if "GROSS PROFIT" in line_upper:
                    result["gross_profit"] = numbers
                    continue

                # Profit after taxation
                if "PROFIT AFTER" in line_upper and "TAX" in line_upper:
                    result["profit_after_tax"] = numbers
                    continue

    except Exception as e:
        logger.warning("Failed to parse non-bank P&L from PDF: %s", e)

    return result


def is_bank_symbol(con: sqlite3.Connection, symbol: str) -> bool:
    """Check if a symbol is a bank based on sector or existing markup_earned data."""
    # Check if markup_earned exists in financials
    row = con.execute(
        "SELECT markup_earned FROM company_financials WHERE symbol = ? AND markup_earned IS NOT NULL LIMIT 1",
        (symbol.upper(),),
    ).fetchone()
    if row:
        return True

    # Check sector code and sector_name
    row = con.execute(
        "SELECT sector, sector_name FROM symbols WHERE symbol = ?",
        (symbol.upper(),),
    ).fetchone()
    if row:
        sector_code = row[0] or ""
        sector_name = (row[1] or "").upper()
        if sector_code in BANK_SECTOR_CODES:
            return True
        if any(kw in sector_name for kw in BANK_SECTOR_KEYWORDS):
            return True

    return False


def fetch_and_parse_report(
    symbol: str,
    report_type: str = "quarterly",
    con: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    """Download latest report PDF and parse P&L for a symbol.

    Args:
        symbol: Stock symbol
        report_type: 'annual' or 'quarterly'
        con: Optional DB connection (to check if bank)

    Returns:
        Dict with parsed financial data and metadata.
    """
    symbol = symbol.upper()
    result = {"symbol": symbol, "success": False, "data": {}}

    # Get report links
    try:
        reports = get_report_links(symbol)
    except Exception as e:
        result["error"] = f"Failed to get reports: {e}"
        return result

    # Filter by type and get the most recent
    typed_reports = [r for r in reports if r["report_type"] == report_type]
    if not typed_reports:
        # Fallback to any available
        typed_reports = reports

    if not typed_reports:
        result["error"] = "No reports available"
        return result

    # Most recent report is last in the list
    latest = typed_reports[-1]
    result["report_url"] = latest["url"]
    result["period_ended"] = latest["period_ended"]
    result["report_type"] = latest["report_type"]

    # Download PDF
    try:
        pdf_content = _download_pdf(latest["url"])
        result["pdf_size"] = len(pdf_content)
    except Exception as e:
        result["error"] = f"Failed to download PDF: {e}"
        return result

    # Determine if bank
    is_bank = False
    if con:
        is_bank = is_bank_symbol(con, symbol)

    # Try bank parser first (if bank or unknown)
    if is_bank:
        data = parse_bank_pl(pdf_content)
    else:
        # Try bank parser first as heuristic
        data = parse_bank_pl(pdf_content)
        if data.get("markup_earned"):
            is_bank = True
        else:
            data = parse_nonbank_pl(pdf_content)

    result["is_bank"] = is_bank
    result["data"] = data
    result["success"] = bool(data.get("markup_earned") or data.get("sales") or data.get("gross_profit"))

    return result


def _resolve_period_end(report_period_ended: str, report_type: str) -> tuple[str, str] | None:
    """Convert DPS report period_ended (e.g. '2024', '2025-09-30') to DB period format.

    Returns (period_end, period_type) matching the company_financials schema.
    """
    if not report_period_ended:
        return None

    # Annual: just the year like "2024"
    if re.match(r"^\d{4}$", report_period_ended):
        return report_period_ended, "annual"

    # Quarterly: "2025-09-30" or "September 30, 2025" etc
    date_match = re.search(r"(\d{4})-(\d{2})-(\d{2})", report_period_ended)
    if date_match:
        year = date_match.group(1)
        month = int(date_match.group(2))
        # Map month to quarter
        quarter_map = {3: 1, 6: 2, 9: 3, 12: 4}
        quarter = quarter_map.get(month)
        if quarter:
            if quarter == 4:
                # Full year — this is annual
                return year, "annual"
            return f"Q{quarter} {year}", "quarterly"
        # Fallback for non-standard months
        return f"{year}", "annual"

    return None


def _sync_one_report(
    con: sqlite3.Connection,
    symbol: str,
    report: dict,
) -> dict[str, Any]:
    """Download one bank report PDF, parse it, and upsert markup data.

    Returns result dict with periods_matched count.
    """
    from ..db import upsert_company_financials, upsert_company_ratios

    result: dict[str, Any] = {"success": False, "periods_matched": 0}

    # Download PDF
    try:
        pdf_content = _download_pdf(report["url"])
    except Exception as e:
        result["error"] = f"Failed to download PDF: {e}"
        return result

    data = parse_bank_pl(pdf_content)
    markup_earned = data.get("markup_earned", [])
    markup_expensed = data.get("markup_expensed", [])

    if not markup_earned or not markup_expensed:
        return result

    # Resolve period from DPS metadata
    resolved = _resolve_period_end(report["period_ended"], report["report_type"])
    if not resolved:
        return result

    period_end, period_type = resolved

    # PDF columns: col0 = current period, col1 = prior-year same period
    periods_to_update = [{"period_end": period_end, "period_type": period_type}]

    year_match = re.search(r"(\d{4})", period_end)
    if year_match and len(markup_earned) >= 2:
        prior_year = str(int(year_match.group(1)) - 1)
        prior_period = period_end.replace(year_match.group(1), prior_year)
        periods_to_update.append({"period_end": prior_period, "period_type": period_type})

    updates = []
    ratios_updates = []

    for i, period_info in enumerate(periods_to_update):
        if i >= len(markup_earned) or i >= len(markup_expensed):
            break

        me_val = markup_earned[i]
        mex_val = markup_expensed[i]
        pe = period_info["period_end"]
        pt = period_info["period_type"]

        # Sanity check: ME must be a meaningful positive number and > MEX
        if me_val < 1000 or mex_val < 0 or mex_val >= me_val:
            logger.debug("Skipping %s %s: ME=%.0f MEX=%.0f (sanity failed)", pe, pt, me_val, mex_val)
            continue

        net_interest = me_val - mex_val
        gross_margin = (net_interest / me_val * 100) if me_val > 0 else None

        updates.append({
            "period_end": pe,
            "period_type": pt,
            "markup_earned": me_val,
            "markup_expensed": mex_val,
            "gross_profit": net_interest,
        })

        if gross_margin is not None:
            ratios_updates.append({
                "period_end": pe,
                "period_type": pt,
                "gross_profit_margin": round(gross_margin, 2),
            })

    if updates:
        upsert_company_financials(con, symbol, updates)
    if ratios_updates:
        upsert_company_ratios(con, symbol, ratios_updates)

    result["success"] = bool(updates)
    result["periods_matched"] = len(updates)
    return result


def sync_bank_financials(
    con: sqlite3.Connection,
    symbol: str,
    report_type: str = "quarterly",
    process_all: bool = False,
    max_years: int = 4,
) -> dict[str, Any]:
    """Fetch bank report(s), parse, and update company_financials with markup data.

    Args:
        con: Database connection
        symbol: Bank symbol (e.g., 'HBL')
        report_type: 'annual' or 'quarterly'
        process_all: If True, process reports for the last max_years years (not just latest).
                     Each annual report yields 2 years of data.
        max_years: How many years of reports to process (default 4).

    Returns:
        Result dict with total periods_matched.
    """
    from datetime import datetime

    symbol = symbol.upper()
    result: dict[str, Any] = {"symbol": symbol, "success": False, "periods_matched": 0}

    try:
        reports = get_report_links(symbol)
    except Exception as e:
        result["error"] = f"Failed to get reports: {e}"
        return result

    typed_reports = [r for r in reports if r["report_type"] == report_type]
    if not typed_reports:
        result["error"] = f"No {report_type} reports available"
        return result

    if process_all:
        cutoff_year = datetime.now().year - max_years
        to_process = []
        for rpt in typed_reports:
            # Skip Q4 quarterly reports (Dec-31) — they resolve to annual periods
            # and would overwrite data from annual report PDFs.
            if report_type == "quarterly":
                resolved = _resolve_period_end(rpt["period_ended"], rpt["report_type"])
                if resolved and resolved[1] == "annual":
                    continue
            # Only process recent reports (within max_years)
            year_match = re.search(r"(\d{4})", rpt["period_ended"])
            if year_match and int(year_match.group(1)) < cutoff_year:
                continue
            to_process.append(rpt)
    else:
        to_process = [typed_reports[-1]]  # latest only

    total_matched = 0
    for rpt in to_process:
        sub = _sync_one_report(con, symbol, rpt)
        total_matched += sub.get("periods_matched", 0)

    result["success"] = total_matched > 0
    result["periods_matched"] = total_matched
    logger.info(
        "%s: updated %d periods with markup data from %d %s PDFs",
        symbol, total_matched, len(to_process), report_type,
    )

    return result
