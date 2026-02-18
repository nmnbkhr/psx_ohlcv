"""Comprehensive Financial Statement PDF Parser.

Extracts Income Statement (P&L) and Balance Sheet data from PSX financial
report PDFs. Handles both bank and non-bank formats, both PSX portal
(standardized) and company IR (varied) PDF sources.

All amounts are normalized to absolute PKR (no thousands/millions scale).
Currency scale is detected from PDF headers and applied automatically.

Usage:
    from psx_ohlcv.sources.financial_parser import parse_pdf, classify_page

    result = parse_pdf(pdf_bytes, is_bank=False)
    # result["income_statement"]["sales"] -> 12345678.00  (full PKR)
    # result["balance_sheet"]["total_assets"] -> 98765432.00
"""

import io
import logging
import re
from typing import Any

logger = logging.getLogger("psx_ohlcv.financial_parser")

# ---------------------------------------------------------------------------
# Reuse proven helpers from report_parser
# ---------------------------------------------------------------------------
from .report_parser import (
    _extract_numbers,
    _extract_period_info,
    _filter_note_refs,
    _fix_split_numbers,
)

# ---------------------------------------------------------------------------
# Page Classification
# ---------------------------------------------------------------------------

# Statement types
PL = "pl"
BS = "bs"
CF = "cf"
CI = "ci"
NOTES = "notes"
UNKNOWN = "unknown"


def classify_page(page_text: str) -> str:
    """Classify a PDF page by its financial statement type.

    Only checks the first 3 lines (title area) to avoid false positives
    from body text like "reclassified to profit and loss".

    Returns: 'pl', 'bs', 'cf', 'ci', 'notes', or 'unknown'
    """
    if not page_text:
        return UNKNOWN

    title = "\n".join(page_text.split("\n")[:3]).upper()

    # Exclusions first
    is_notes = "NOTES TO" in title and "FINANCIAL" in title
    if is_notes:
        return NOTES

    # Comprehensive income (must check before P&L)
    if "COMPREHENSIVE" in title and ("INCOME" in title or "LOSS" in title):
        return CI

    # Cash flow
    if "CASH FLOW" in title:
        return CF

    # P&L / Income Statement
    is_pl = (
        "PROFIT AND LOSS" in title
        or "PROFIT OR LOSS" in title
        or "INCOME STATEMENT" in title
        or ("STATEMENT OF PROFIT" in title and "COMPREHENSIVE" not in title)
    )
    if is_pl:
        return PL

    # Balance Sheet / Statement of Financial Position
    is_bs = (
        "BALANCE SHEET" in title
        or "STATEMENT OF FINANCIAL POSITION" in title
        or "FINANCIAL POSITION" in title
        or ("ASSETS AND LIABILITIES" in title)
    )
    if is_bs:
        return BS

    return UNKNOWN


# ---------------------------------------------------------------------------
# Label Maps — regex patterns → canonical field names
# ---------------------------------------------------------------------------

# P&L line items (order matters — first match wins for ambiguous lines)
PL_LABELS: list[tuple[re.Pattern, str]] = [
    # Bank-specific (check before generic)
    (re.compile(r"MARK.?UP.*(?:EARNED|INCOME)|INTEREST\s+(?:/\s*RETURN\s+)?EARNED|RETURN\s+EARNED", re.I), "markup_earned"),
    (re.compile(r"MARK.?UP.*EXPENS|INTEREST\s+(?:/\s*RETURN\s+)?EXPENS|RETURN\s+EXPENS", re.I), "markup_expensed"),
    (re.compile(r"NET\s+(?:MARK.?UP|INTEREST)[\s/\w]*?(?:INCOME|MARGIN)", re.I), "net_interest_income"),
    (re.compile(r"NON.?MARK.?UP\s+INCOME|NON.?INTEREST\s+INCOME|FEE.*COMMISSION\s+INCOME", re.I), "non_markup_income"),
    (re.compile(r"TOTAL\s+INCOME", re.I), "total_income"),
    (re.compile(r"PROVISION\s+(?:AGAINST|FOR)\s+(?:ADVANCES|NON.?PERFORMING|LOANS|DIMINUTION)", re.I), "provisions"),

    # Non-bank P&L
    (re.compile(r"(?:NET\s+)?(?:SALES|REVENUE)(?:\s+FROM\s+(?:OPERATIONS|CONTRACT))?|NET\s+REVENUE|TURNOVER", re.I), "sales"),
    (re.compile(r"COST\s+OF\s+(?:SALES|REVENUE|GOODS\s+(?:SOLD|MANUFACTURED))", re.I), "cost_of_sales"),
    (re.compile(r"GROSS\s+PROFIT", re.I), "gross_profit"),
    (re.compile(r"(?:ADMIN|GENERAL)\s*(?:&|AND)?\s*(?:ADMIN|GENERAL)?\s*EXPENS|SELLING\s*(?:&|AND)?\s*DISTRIBUT|DISTRIBUTION\s+COST|OPERATING\s+EXPENS", re.I), "operating_expenses"),
    (re.compile(r"OPERATING\s+PROFIT|PROFIT\s+FROM\s+OPERATIONS|EBIT(?:DA)?", re.I), "operating_profit"),
    (re.compile(r"FINANCE\s+COST|INTEREST\s+EXPENSE|FINANCIAL\s+CHARGES|BORROWING\s+COST", re.I), "finance_cost"),
    (re.compile(r"OTHER\s+(?:OPERATING\s+)?INCOME|OTHER\s+CHARGES", re.I), "other_income"),
    (re.compile(r"PROFIT\s+BEFORE\s*\.?\s*TAX|PBT\b", re.I), "profit_before_tax"),
    (re.compile(r"(?:INCOME\s+)?TAX(?:ATION)(?:\s+(?:CHARGE|EXPENSE))?|^TAXATION\b", re.I), "taxation"),
    (re.compile(r"PROFIT\s+(?:AFTER\s+TAX|FOR\s+THE\s+(?:YEAR|PERIOD|QUARTER|HALF))|NET\s+(?:INCOME|PROFIT)\b|(?<!\w)PAT(?!\w)", re.I), "profit_after_tax"),
    (re.compile(r"(?:BASIC\s+)?EARNINGS?\s+PER\s+SHARE|(?:BASIC\s+)?EPS\b", re.I), "eps"),
]

# Balance Sheet line items
BS_LABELS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"TOTAL\s+ASSETS", re.I), "total_assets"),
    (re.compile(r"TOTAL\s+LIABILIT", re.I), "total_liabilities"),
    (re.compile(r"(?:TOTAL\s+)?(?:SHAREHOLDERS?|STOCK\s*HOLDERS?).?\s*EQUITY|TOTAL\s+EQUITY(?!\s+AND)|NET\s+ASSETS", re.I), "total_equity"),
    (re.compile(r"(?:TOTAL\s+)?CURRENT\s+ASSETS", re.I), "current_assets"),
    (re.compile(r"(?:TOTAL\s+)?NON\s*[-–—.]?\s*CURRENT\s+ASSETS|PROPERTY.*PLANT|FIXED\s+ASSETS", re.I), "non_current_assets"),
    (re.compile(r"(?:TOTAL\s+)?CURRENT\s+LIABILIT", re.I), "current_liabilities"),
    (re.compile(r"(?:TOTAL\s+)?NON\s*[-–—.]?\s*CURRENT\s+LIABILIT|LONG.?TERM\s+(?:DEBT|BORROWING|LIABILIT)", re.I), "non_current_liabilities"),
    (re.compile(r"CASH\s+AND\s+(?:CASH\s+)?EQUIV|CASH\s+AND\s+BANK|CASH\s+AT\s+BANK", re.I), "cash_and_equivalents"),
    (re.compile(r"(?:SHARE|PAID.?UP|ISSUED)\s+CAPITAL", re.I), "share_capital"),
]

# Lines to skip (sub-totals, notes, headers)
SKIP_PATTERNS = re.compile(
    r"(?:RUPEES?\s+IN|PKR|RS\.?\s*IN|AMOUNTS?\s+IN|FOR\s+THE\s+(?:YEAR|PERIOD|QUARTER))"
    r"|(?:HALF\s+YEAR|SIX\s+MONTHS|NINE\s+MONTHS|THREE\s+MONTHS)"
    r"|(?:AUDITED|UN.?AUDITED|CONDENSED|CONSOLIDATED|UNCONSOLIDATED)"
    r"|(?:RESTATED|RECLASSIFIED|NOTE|SEE\s+NOTE)",
    re.I,
)


# ---------------------------------------------------------------------------
# Currency Scale Detection
# ---------------------------------------------------------------------------

def _extract_period_info_extended(lines: list[str]) -> dict[str, str | None]:
    """Extended period extraction that handles more date formats.

    Handles:
        "For the year ended December 31, 2024"  (Month DD, YYYY)
        "For the Quarter ended 30 September 2025"  (DD Month YYYY)
        "For the nine months ended September 30, 2025"
        "As at September 30, 2025" (BS date)
    """
    info = _extract_period_info(lines)

    # If the original parser found a period, return it
    if info.get("period_end_date"):
        return info

    # Try DD Month YYYY format
    months = (
        "January|February|March|April|May|June|July|August|"
        "September|October|November|December"
    )
    for line in lines[:15]:
        line_stripped = line.strip()

        # "for the year/quarter/months [period] ended DD Month YYYY"
        match = re.search(
            rf"for\s+the\s+(year|.*?months?(?:\s+period)?|quarter|half[\s-]?year)\s+ended\s+"
            rf"(\d{{1,2}})\s+({months}),?\s+(\d{{4}})",
            line_stripped,
            re.IGNORECASE,
        )
        if match:
            duration = match.group(1).lower()
            day = match.group(2)
            month = match.group(3)
            year = match.group(4)
            info["period_header"] = line_stripped
            info["period_end_date"] = f"{year}-{month}-{day}"
            if "year" in duration and "half" not in duration:
                info["period_type"] = "annual"
            else:
                info["period_type"] = "quarterly"
            return info

        # "As at DD Month YYYY"
        match = re.search(
            rf"as\s+at\s+(\d{{1,2}})\s+({months})\s+(\d{{4}})",
            line_stripped,
            re.IGNORECASE,
        )
        if match:
            day = match.group(1)
            month = match.group(2)
            year = match.group(3)
            info["period_end_date"] = f"{year}-{month}-{day}"
            # Can't determine period_type from "As at" alone
            return info

        # "As at Month DD, YYYY"
        match = re.search(
            rf"as\s+at\s+({months})\s+(\d{{1,2}}),?\s+(\d{{4}})",
            line_stripped,
            re.IGNORECASE,
        )
        if match:
            month = match.group(1)
            day = match.group(2)
            year = match.group(3)
            info["period_end_date"] = f"{year}-{month}-{day}"
            return info

    return info


def _detect_scale(lines: list[str]) -> float:
    """Detect currency scale from PDF header lines.

    Handles both ASCII apostrophe (') and unicode quotes (\u2018, \u2019).
    Returns multiplier: 1000 for 'thousands', 1_000_000 for 'millions', 1 for units.
    """
    for line in lines[:15]:
        line_lower = line.lower()
        # Check for '000 with various quote characters
        if "'000" in line or "\u2018000" in line or "\u2019000" in line or "thousand" in line_lower:
            return 1000.0
        if "million" in line_lower:
            return 1_000_000.0
    return 1.0


def _scale_label(multiplier: float) -> str:
    """Human-readable label for scale."""
    if multiplier >= 1_000_000:
        return "millions"
    if multiplier >= 1000:
        return "thousands"
    return "units"


# ---------------------------------------------------------------------------
# Core Extraction
# ---------------------------------------------------------------------------

def _extract_line_items(
    lines: list[str],
    label_map: list[tuple[re.Pattern, str]],
    max_columns: int = 2,
) -> dict[str, list[float]]:
    """Extract financial line items from text lines using label map.

    For each line:
    1. Match against label patterns
    2. Extract numbers
    3. Filter note references
    4. Truncate to max_columns

    Returns dict of {canonical_field: [col0_value, col1_value, ...]}.
    """
    found: dict[str, list[float]] = {}

    for line in lines:
        line_fixed = _fix_split_numbers(line)
        line_upper = line_fixed.upper().strip()

        if not line_upper or len(line_upper) < 3:
            continue

        # Skip header/metadata lines
        if SKIP_PATTERNS.search(line_upper) and not any(
            pat.search(line_upper) for pat, _ in label_map
        ):
            continue

        # Try to match a label
        matched_field = None
        for pattern, field_name in label_map:
            if pattern.search(line_upper):
                # Extra exclusions for ambiguous matches
                if field_name == "markup_earned" and "EXPENS" in line_upper:
                    continue
                if field_name == "markup_expensed" and "NON" in line_upper:
                    continue
                if field_name == "sales" and "COST" in line_upper:
                    continue
                if field_name == "taxation" and (
                    "BEFORE" in line_upper or "AFTER" in line_upper
                    or "NET OF" in line_upper or "ASSOCIATE" in line_upper
                    or "DEFERRED" in line_upper
                ):
                    continue
                if field_name == "profit_after_tax" and "BEFORE" in line_upper:
                    continue
                matched_field = field_name
                break

        if not matched_field:
            continue

        # Don't overwrite first match (use first occurrence)
        if matched_field in found:
            continue

        numbers = _extract_numbers(line_fixed)
        if not numbers:
            continue

        numbers = _filter_note_refs(numbers)

        # Truncate to max columns (handles consolidated + unconsolidated)
        if len(numbers) > max_columns:
            numbers = numbers[:max_columns]

        if not numbers:
            continue

        # Consistency check: if _fix_split_numbers merged a note ref with the
        # actual value (e.g., "4 14,231,086" → "414,231,086"), the values
        # will be wildly inconsistent. Re-extract without split fix.
        if len(numbers) >= 2 and numbers[0] != 0 and numbers[1] != 0:
            ratio = abs(numbers[0]) / abs(numbers[1]) if numbers[1] != 0 else 0
            if ratio > 10 or (ratio > 0 and ratio < 0.1):
                # Try without split fix
                raw_numbers = _extract_numbers(line)
                raw_numbers = _filter_note_refs(raw_numbers)
                if len(raw_numbers) > max_columns:
                    raw_numbers = raw_numbers[:max_columns]
                if len(raw_numbers) >= 2:
                    raw_ratio = abs(raw_numbers[0]) / abs(raw_numbers[1]) if raw_numbers[1] != 0 else 0
                    # Use raw if more consistent
                    if 0.1 <= raw_ratio <= 10:
                        numbers = raw_numbers

        if numbers:
            found[matched_field] = numbers

    return found


def _extract_bs_totals(
    lines: list[str],
    found: dict[str, list[float]],
    max_columns: int = 2,
) -> None:
    """Extract total_assets, total_liabilities, and total_equity from BS pages
    where totals appear as standalone number lines without text labels.

    Bank BS format (Assets/Liabilities sections):
        ASSETS
        <individual items>
        6,827,420,374  5,659,803,830   ← total_assets (number-only line)
        LIABILITIES
        <individual items>
        6,404,286,353  5,282,018,534   ← total_liabilities (number-only line)
        NET ASSETS  423,134,021  377,785,296

    Non-bank BS format (Equity first, then Liabilities):
        EQUITY AND LIABILITIES
          EQUITY
            Share capital ...
            255,647,595  244,592,909   ← equity subtotal
          NON - CURRENT LIABILITIES
            61,341,915   66,535,969    ← non-current subtotal
          CURRENT LIABILITIES
            253,246,216  262,256,852   ← current subtotal
        TOTAL EQUITY AND LIABILITIES 570,602,468
        ASSETS
          ...
        TOTAL ASSETS 570,602,468
    """
    section = None  # 'assets', 'liabilities_nc', 'liabilities_c', 'equity'
    last_number_line: dict[str, list[float]] = {}
    grand_total_candidate: list[float] | None = None
    _nc_re = re.compile(r"NON\s*[-–—]?\s*CURRENT", re.I)

    for line in lines:
        line_upper = line.strip().upper()
        if not line_upper:
            continue

        # Detect section headings
        if line_upper == "ASSETS" or line_upper.startswith("ASSETS "):
            section = "assets"
            continue

        # Equity section: "SHARE CAPITAL AND RESERVES" or standalone "EQUITY"
        # or "EQUITY AND LIABILITIES" (overall BS heading → equity is first)
        if "SHARE CAPITAL" in line_upper and "RESERVES" in line_upper:
            section = "equity"
            continue

        if (
            line_upper == "EQUITY"
            or line_upper == "EQUITY AND LIABILITIES"
        ) and "TOTAL" not in line_upper:
            section = "equity"
            continue

        if line_upper == "LIABILITIES" or line_upper.startswith("LIABILITIES "):
            # Transition from ASSETS to LIABILITIES (bank format)
            if section == "assets" and "total_assets" not in found:
                if "assets" in last_number_line:
                    found["total_assets"] = last_number_line["assets"]
            section = "liabilities_nc"
            last_number_line = {}
            continue

        if _nc_re.search(line_upper) and "LIABILIT" in line_upper:
            # NON-CURRENT LIABILITIES (handles "NON - CURRENT", "NON-CURRENT", etc.)
            # Transition from equity section to liabilities
            if section == "equity" and "total_equity" not in found:
                if "equity" in last_number_line:
                    found["total_equity"] = last_number_line["equity"]
            section = "liabilities_nc"
            last_number_line = {}
            continue

        if "CURRENT LIABILIT" in line_upper and not _nc_re.search(line_upper):
            # CURRENT LIABILITIES (not non-current)
            # Save non-current subtotal before switching
            if section == "liabilities_nc":
                if "liabilities_nc" in last_number_line:
                    # Store non-current subtotal
                    found.setdefault("_nc_liabilities", last_number_line["liabilities_nc"])
            # If we were in equity section (no non-current liabilities heading)
            if section == "equity" and "total_equity" not in found:
                if "equity" in last_number_line:
                    found["total_equity"] = last_number_line["equity"]
            section = "liabilities_c"
            last_number_line = {}
            continue

        if "NET ASSETS" in line_upper or "REPRESENTED BY" in line_upper:
            # Transition: last number line in liabilities = total_liabilities
            if section in ("liabilities_nc", "liabilities_c") and "total_liabilities" not in found:
                sec_key = section
                if sec_key in last_number_line:
                    found["total_liabilities"] = last_number_line[sec_key]
            section = None
            continue

        if "CONTINGENCIES" in line_upper or "ANNEXED NOTES" in line_upper:
            # End of BS data
            break

        if section is None:
            continue

        # Check if this line is a number-only line (standalone total)
        line_fixed = _fix_split_numbers(line)
        text_part = re.sub(r"[\d,.\-\(\)\s]", "", line_fixed)
        numbers = _extract_numbers(line_fixed)
        numbers = _filter_note_refs(numbers)

        if numbers and len(text_part) < 3:
            if len(numbers) > max_columns:
                numbers = numbers[:max_columns]
            if section:
                last_number_line[section] = numbers
            grand_total_candidate = numbers

    # After processing all lines, handle remaining sections.
    # First, sum non-current + current liabilities if both are tracked.
    nc = found.pop("_nc_liabilities", None)
    cc = last_number_line.get("liabilities_c")

    if "total_liabilities" not in found:
        if nc and cc:
            # Both non-current and current subtotals → sum for total
            found["total_liabilities"] = [
                nc[i] + cc[i] for i in range(min(len(nc), len(cc)))
            ]
        elif nc and not cc:
            # Only non-current captured (rare)
            nc_sub = last_number_line.get("liabilities_nc")
            if nc_sub:
                found["total_liabilities"] = nc_sub
        elif cc:
            # Only current captured (no non-current section)
            found["total_liabilities"] = cc
        elif section in ("liabilities_nc", "liabilities_c"):
            sec_key = section
            if sec_key in last_number_line:
                found["total_liabilities"] = last_number_line[sec_key]

    # If total_assets still missing but we have total_equity and total_liabilities,
    # look for the grand total (last standalone number line after TOTAL LIABILITIES)
    if "total_assets" not in found and grand_total_candidate:
        tl = found.get("total_liabilities", [0])[0] if "total_liabilities" in found else 0
        te = found.get("total_equity", [0])[0] if "total_equity" in found else 0
        gt = grand_total_candidate[0]
        # Verify: grand total should approximately equal equity + liabilities
        if te > 0 and tl > 0 and gt > 0:
            expected = te + tl
            if abs(gt - expected) / expected < 0.01:  # within 1%
                found["total_assets"] = grand_total_candidate

    # --- A = E + L identity fallback ---
    # If we have 2 of 3 (total_assets, total_liabilities, total_equity),
    # compute the missing one via A = E + L.
    ta = found.get("total_assets")
    tl = found.get("total_liabilities")
    te = found.get("total_equity")

    if ta and tl and not te:
        found["total_equity"] = [
            ta[i] - tl[i] for i in range(min(len(ta), len(tl)))
        ]
    elif ta and te and not tl:
        found["total_liabilities"] = [
            ta[i] - te[i] for i in range(min(len(ta), len(te)))
        ]
    elif te and tl and not ta:
        found["total_assets"] = [
            te[i] + tl[i] for i in range(min(len(te), len(tl)))
        ]


def _find_statement_pages(
    pdf_pages: list,
    statement_type: str,
) -> list[int]:
    """Find pages of a specific statement type in the PDF.

    Returns list of page indices (0-based).
    """
    pages = []
    for i, page in enumerate(pdf_pages):
        text = page.extract_text()
        if not text:
            continue
        page_type = classify_page(text)
        if page_type == statement_type:
            pages.append(i)
    return pages


def _get_last_statement_text(
    pdf_pages: list,
    statement_page_indices: list[int],
) -> tuple[str, list[int]]:
    """Get concatenated text from the last group of consecutive statement pages.

    Balance Sheets often span 2 pages (equity+liabilities on one page,
    assets on the next). This finds the last group of consecutive pages
    and concatenates their text.

    Returns (concatenated_text, page_indices_used).
    """
    if not statement_page_indices:
        return "", []

    # Find last group of consecutive pages
    groups: list[list[int]] = []
    current_group: list[int] = [statement_page_indices[-1]]

    for idx in reversed(statement_page_indices[:-1]):
        if idx >= current_group[0] - 2:  # Allow 1-page gap (header pages)
            current_group.insert(0, idx)
        else:
            break

    # Concatenate text from the last group
    texts = []
    for idx in current_group:
        text = pdf_pages[idx].extract_text()
        if text:
            texts.append(text)

    return "\n".join(texts), current_group


# ---------------------------------------------------------------------------
# Main Parse Function
# ---------------------------------------------------------------------------

def parse_pdf(
    pdf_content: bytes,
    is_bank: bool = False,
    source: str = "psx_pdf",
) -> dict[str, Any]:
    """Parse a complete financial PDF, extracting P&L and Balance Sheet data.

    All monetary values are normalized to full PKR (applying scale multiplier).

    Args:
        pdf_content: Raw PDF bytes
        is_bank: Whether this is a bank (affects which P&L labels to match)
        source: 'psx_pdf' or 'ir_pdf'

    Returns:
        Dict with:
            income_statement: {field: value_in_pkr, ...}
            balance_sheet: {field: value_in_pkr, ...}
            period_info: {period_end_date, period_type, currency_scale, ...}
            prior_period: {income_statement: {...}, balance_sheet: {...}}
            confidence: float 0.0-1.0
            pages_used: [int, ...]
            warnings: [str, ...]
    """
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber not installed — cannot parse PDF")
        return _empty_result()

    result = _empty_result()
    result["source"] = source

    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            if not pdf.pages:
                result["warnings"].append("PDF has no pages")
                return result

            # --- Find P&L pages ---
            pl_pages = _find_statement_pages(pdf.pages, PL)
            if pl_pages:
                # Use LAST P&L page (unconsolidated for PSX convention)
                pl_idx = pl_pages[-1]
                result["pages_used"].append(pl_idx)

                page = pdf.pages[pl_idx]
                text = page.extract_text()
                if text:
                    lines = text.split("\n")

                    # Period info from header
                    period_info = _extract_period_info_extended(lines)
                    result["period_info"].update(period_info)

                    # Detect scale
                    scale = _detect_scale(lines)
                    result["period_info"]["currency_scale"] = _scale_label(scale)

                    # Extract P&L items
                    pl_items = _extract_line_items(lines, PL_LABELS)
                    _populate_statement(
                        result["income_statement"],
                        result["prior_period"]["income_statement"],
                        pl_items,
                        scale,
                    )
            else:
                result["warnings"].append("No P&L pages found")

            # --- Find Balance Sheet pages ---
            bs_pages = _find_statement_pages(pdf.pages, BS)
            if bs_pages:
                # Get concatenated text from last group of consecutive BS pages
                bs_text, bs_indices = _get_last_statement_text(pdf.pages, bs_pages)
                result["pages_used"].extend(bs_indices)

                if bs_text:
                    lines = bs_text.split("\n")

                    # Detect scale from BS header; if not found, inherit P&L scale
                    bs_scale = _detect_scale(lines)
                    if result["period_info"]["currency_scale"] is None:
                        scale = bs_scale
                        result["period_info"]["currency_scale"] = _scale_label(scale)
                    elif bs_scale > 1.0:
                        scale = bs_scale  # BS has its own scale indicator
                    # else: bs_scale=1.0 (no indicator) → use P&L scale (already set)

                    # If no period info from P&L, try BS header
                    if not result["period_info"].get("period_end_date"):
                        bs_period = _extract_period_info_extended(lines)
                        result["period_info"].update(
                            {k: v for k, v in bs_period.items() if v}
                        )

                    # Extract BS items
                    bs_items = _extract_line_items(lines, BS_LABELS)

                    # Handle bank BS format: totals as standalone number lines
                    _extract_bs_totals(lines, bs_items)

                    _populate_statement(
                        result["balance_sheet"],
                        result["prior_period"]["balance_sheet"],
                        bs_items,
                        scale,
                    )
            else:
                result["warnings"].append("No Balance Sheet pages found")

            # --- Compute confidence ---
            result["confidence"] = _compute_confidence(result, is_bank)

    except Exception as e:
        logger.warning("Failed to parse PDF: %s", e)
        result["warnings"].append(f"Parse error: {e}")

    return result


def _empty_result() -> dict[str, Any]:
    """Create empty result template."""
    return {
        "income_statement": {},
        "balance_sheet": {},
        "period_info": {
            "period_header": None,
            "period_end_date": None,
            "period_type": None,
            "currency_scale": None,
        },
        "prior_period": {
            "income_statement": {},
            "balance_sheet": {},
        },
        "confidence": 0.0,
        "pages_used": [],
        "warnings": [],
        "source": "unknown",
    }


def _populate_statement(
    current: dict,
    prior: dict,
    items: dict[str, list[float]],
    scale: float,
) -> None:
    """Populate current and prior period dicts from extracted items.

    Applies currency scale to normalize to full PKR.
    EPS is NOT scaled (already per-share).
    """
    no_scale_fields = {"eps"}

    for field, values in items.items():
        multiplier = 1.0 if field in no_scale_fields else scale

        # Current period (column 0)
        if values:
            current[field] = round(values[0] * multiplier, 2)

        # Prior period (column 1)
        if len(values) >= 2:
            prior[field] = round(values[1] * multiplier, 2)


def _compute_confidence(result: dict, is_bank: bool) -> float:
    """Compute parser confidence score (0.0-1.0).

    Based on how many expected items were found.
    """
    pl = result["income_statement"]
    bs = result["balance_sheet"]

    if is_bank:
        expected_pl = {"markup_earned", "markup_expensed", "profit_after_tax"}
    else:
        expected_pl = {"sales", "gross_profit", "profit_after_tax"}

    expected_bs = {"total_assets", "total_liabilities", "total_equity"}

    found_pl = sum(1 for k in expected_pl if k in pl and pl[k] is not None)
    found_bs = sum(1 for k in expected_bs if k in bs and bs[k] is not None)

    total_expected = len(expected_pl) + len(expected_bs)
    total_found = found_pl + found_bs

    return round(total_found / total_expected, 2) if total_expected > 0 else 0.0


# ---------------------------------------------------------------------------
# Convenience Entry Points
# ---------------------------------------------------------------------------

def parse_psx_pdf(pdf_content: bytes, is_bank: bool = False) -> dict[str, Any]:
    """Parse a PSX portal PDF (standardized format, 10-30 pages)."""
    return parse_pdf(pdf_content, is_bank=is_bank, source="psx_pdf")


def parse_ir_pdf(
    pdf_content: bytes,
    symbol: str = "",
    is_bank: bool = False,
) -> dict[str, Any]:
    """Parse a company IR annual/quarterly report PDF (may be 100+ pages).

    Strategy: classify ALL pages to find P&L and BS, then extract from those.
    Same core logic as parse_psx_pdf but expects larger documents.
    """
    return parse_pdf(pdf_content, is_bank=is_bank, source="ir_pdf")


def flatten_parsed_to_financials(
    parsed: dict[str, Any],
    symbol: str,
    period_end: str,
    period_type: str,
) -> list[dict]:
    """Convert parsed result to list of dicts suitable for upsert_company_financials.

    Returns up to 2 entries: current period and prior period.
    """
    entries = []

    # Current period
    pl = parsed.get("income_statement", {})
    bs = parsed.get("balance_sheet", {})

    if pl or bs:
        entry = {
            "period_end": period_end,
            "period_type": period_type,
            "source": parsed.get("source", "psx_pdf"),
            "currency_scale": parsed.get("period_info", {}).get("currency_scale"),
        }
        entry.update(pl)
        entry.update(bs)
        entries.append(entry)

    # Prior period
    prior = parsed.get("prior_period", {})
    prior_pl = prior.get("income_statement", {})
    prior_bs = prior.get("balance_sheet", {})

    if prior_pl or prior_bs:
        # Compute prior period_end (year - 1)
        import re as _re

        year_match = _re.search(r"(\d{4})", period_end)
        if year_match:
            year = int(year_match.group(1))
            prior_pe = period_end.replace(str(year), str(year - 1))
            prior_entry = {
                "period_end": prior_pe,
                "period_type": period_type,
                "source": parsed.get("source", "psx_pdf"),
                "currency_scale": parsed.get("period_info", {}).get("currency_scale"),
            }
            prior_entry.update(prior_pl)
            prior_entry.update(prior_bs)
            entries.append(prior_entry)

    return entries
