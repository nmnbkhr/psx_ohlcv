"""Raw Financial Statement Extractor.

Extracts ALL line items from PSX financial PDFs with original names as-is.
No normalization — captures raw data for downstream catalog matching.

Handles:
- Page classification (P&L, BS, CF, multi-year summary)
- Multi-column detection (2-col annual, 4-col quarterly)
- Section tracking (Non-Current Assets, Current Liabilities, etc.)
- Split number fixing (PDF column boundary artifacts)
- Currency scale detection (thousands, millions, etc.)
- OCR fallback for scanned PDFs (if tesseract available)
- Multi-year performance summary extraction

Usage:
    from pakfindata.sources.raw_financial_extractor import extract_pdf

    result = extract_pdf(pdf_bytes, symbol="OGDC", sector_code="0820")
"""

from __future__ import annotations

import io
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any

logger = logging.getLogger("pakfindata.raw_extractor")


# ─────────────────────────────────────────────────────────────────────────────
# Page Classification (improved from financial_parser.py)
# ─────────────────────────────────────────────────────────────────────────────

def classify_page(page_text: str) -> str:
    """Classify a PDF page by financial statement type.

    Checks first 5 lines (title area). P&L checked before CI to handle
    "Statement of Profit or Loss and Other Comprehensive Income" correctly.

    Returns: 'pl', 'bs', 'cf', 'ci', 'multi_year', 'notes', or 'unknown'
    """
    if not page_text:
        return "unknown"

    title = "\n".join(page_text.split("\n")[:5]).upper()

    # Notes (exclude early)
    if "NOTES TO" in title and "FINANCIAL" in title:
        return "notes"

    # Multi-year summary (high value — check early)
    if any(k in title for k in [
        "YEAR PERFORMANCE", "YEAR SUMMARY", "FINANCIAL HIGHLIGHTS",
        "FINANCIAL DATA", "KEY FINANCIAL", "YEAR AT A GLANCE",
        "YEAR REVIEW", "YEAR COMPARISON",
    ]):
        return "multi_year"

    # P&L (BEFORE CI — "Profit or Loss and Other Comprehensive" is P&L)
    if any(k in title for k in [
        "PROFIT AND LOSS", "PROFIT OR LOSS",
        "INCOME STATEMENT", "STATEMENT OF PROFIT",
    ]):
        return "pl"

    # Comprehensive Income (standalone OCI only)
    if "COMPREHENSIVE" in title and ("INCOME" in title or "LOSS" in title):
        return "ci"

    # Cash Flow
    if "CASH FLOW" in title:
        return "cf"

    # Balance Sheet / Financial Position
    if any(k in title for k in [
        "BALANCE SHEET", "STATEMENT OF FINANCIAL POSITION",
        "FINANCIAL POSITION",
    ]):
        return "bs"

    # Changes in Equity
    if "CHANGES IN EQUITY" in title:
        return "equity_changes"

    return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Number Extraction
# ─────────────────────────────────────────────────────────────────────────────

def _fix_split_numbers(line: str) -> str:
    """Fix numbers split by PDF column boundaries."""
    # "5 ,163,137" → "5,163,137"
    line = re.sub(r"(\d)\s+,(\d{3})", r"\1,\2", line)
    # "4 5,163,137" or "1 3,825,569" → merge leading 1-2 digits
    line = re.sub(r"(?<=\s)(\d{1,2})\s+(\d{1,2}(?:,\d{3})+)", r"\1\2", line)
    return line


def _extract_numbers(text: str) -> list[float]:
    """Extract numbers from a line. Handles brackets for negatives."""
    pattern = r"\(?([\d,]+(?:\.\d+)?)\)?"
    matches = re.finditer(pattern, text)

    numbers = []
    for m in matches:
        raw = m.group(0)
        val_str = m.group(1).replace(",", "")
        try:
            val = float(val_str)
            if raw.startswith("(") and raw.endswith(")"):
                val = -val
            numbers.append(val)
        except ValueError:
            continue
    return numbers


def _filter_note_refs(numbers: list[float], line_text: str) -> list[float]:
    """Remove likely note reference numbers (small integers at start)."""
    if not numbers:
        return numbers

    # If first number is a small integer (1-99) and rest are large, it's a note ref
    if len(numbers) >= 2 and 1 <= numbers[0] <= 99:
        ratio = abs(numbers[1]) / max(abs(numbers[0]), 1)
        if ratio > 100:
            return numbers[1:]

    return numbers


# ─────────────────────────────────────────────────────────────────────────────
# Scale Detection
# ─────────────────────────────────────────────────────────────────────────────

_SCALE_PATTERNS = [
    (re.compile(r"(?:RUPEES?\s+IN\s+)?['\u2018\u2019\"']?0{3}['\u2018\u2019\"']?s?\b|IN\s+THOUSANDS?|THOUSAND", re.I), 1_000),
    # OCR variants: "Rupees in '000'", "Rupees in 000", "(Rupees in '000')", "Rs in '000"
    (re.compile(r"RS\.?\s+IN\s+['\u2018\u2019\"']?0{3}", re.I), 1_000),
    (re.compile(r"\(Rupees\s+in\s+['\u2018\u2019\"']?0{3}", re.I), 1_000),
    (re.compile(r"(?:RUPEES?\s+IN\s+)?MILLION|IN\s+MILLIONS", re.I), 1_000_000),
    (re.compile(r"(?:RUPEES?\s+IN\s+)?BILLION|IN\s+BILLIONS", re.I), 1_000_000_000),
]


def _detect_scale(lines: list[str]) -> tuple[int, str]:
    """Detect currency scale from ALL lines (not just header). Returns (multiplier, label)."""
    # Check first 15 lines (header area), then fall back to all lines
    for search_range in [lines[:15], lines]:
        header = " ".join(search_range).upper()
        for pattern, mult in _SCALE_PATTERNS:
            if pattern.search(header):
                labels = {1_000: "thousands", 1_000_000: "millions", 1_000_000_000: "billions"}
                return mult, labels[mult]
    return 1, "units"


# ─────────────────────────────────────────────────────────────────────────────
# Date / Period Detection
# ─────────────────────────────────────────────────────────────────────────────

_MONTH_MAP = {
    "JANUARY": "01", "FEBRUARY": "02", "MARCH": "03", "APRIL": "04",
    "MAY": "05", "JUNE": "06", "JULY": "07", "AUGUST": "08",
    "SEPTEMBER": "09", "OCTOBER": "10", "NOVEMBER": "11", "DECEMBER": "12",
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09",
    "OCT": "10", "NOV": "11", "DEC": "12",
}

_PERIOD_TYPE_PATTERNS = [
    (re.compile(r"QUARTER\s+ENDED|THREE\s+MONTHS?\s+ENDED", re.I), "quarterly"),
    (re.compile(r"HALF\s+YEAR|SIX\s+MONTHS?\s+ENDED", re.I), "half_year"),
    (re.compile(r"NINE\s+MONTHS?\s+ENDED", re.I), "nine_months"),
    (re.compile(r"YEAR\s+ENDED|ANNUAL", re.I), "annual"),
]


def _parse_dates_from_header(lines: list[str]) -> dict[str, Any]:
    """Extract period info from statement header lines."""
    header = " ".join(lines[:8])
    result: dict[str, Any] = {
        "period_end": None,
        "period_type": None,
        "column_dates": [],
        "is_audited": None,
    }

    # Period type
    for pattern, ptype in _PERIOD_TYPE_PATTERNS:
        if pattern.search(header):
            result["period_type"] = ptype
            break

    # Audit status
    upper = header.upper()
    if "UN-AUDITED" in upper or "UNAUDITED" in upper or "UN AUDITED" in upper:
        result["is_audited"] = False
    elif "AUDITED" in upper:
        result["is_audited"] = True

    # Date extraction: "Month DD, YYYY" or "DD Month YYYY" or "YYYY"
    # Pattern 1: June 30, 2025
    dates = re.findall(
        r"((?:January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+\d{1,2},?\s+\d{4})",
        header, re.I
    )
    # Pattern 2: 30 September 2025
    dates += re.findall(
        r"(\d{1,2}\s+(?:January|February|March|April|May|June|July|August|September|October|November|December)"
        r"\s+\d{4})",
        header, re.I
    )

    current_year = datetime.now().year
    parsed_dates = []
    for d in dates:
        try:
            for fmt in ["%B %d, %Y", "%B %d %Y", "%d %B %Y"]:
                try:
                    dt = datetime.strptime(d.replace(",", "").strip(), fmt)
                    # Reject obviously wrong years (OCR errors like 2047, 2097)
                    if dt.year > current_year + 1 or dt.year < 1990:
                        continue
                    parsed_dates.append(dt.strftime("%Y-%m-%d"))
                    break
                except ValueError:
                    continue
        except Exception:
            pass

    # Deduplicate preserving order
    seen = set()
    unique_dates = []
    for d in parsed_dates:
        if d not in seen:
            seen.add(d)
            unique_dates.append(d)

    if unique_dates:
        result["period_end"] = unique_dates[0]
        result["column_dates"] = unique_dates

    # Fallback: just years
    if not result["period_end"]:
        years = re.findall(r"\b(20\d{2})\b", header)
        if years:
            result["column_dates"] = list(dict.fromkeys(years))

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Line Item Extraction
# ─────────────────────────────────────────────────────────────────────────────

_SKIP_LINE_PATTERNS = re.compile(
    r"(?:THE\s+ANNEXED\s+NOTES|FORM\s+AN\s+INTEGRAL\s+PART)"
    r"|(?:CHIEF\s+EXECUTIVE|CHIEF\s+FINANCIAL|DIRECTOR)"
    r"|(?:___+|----+)"
    r"|(?:^\s*$)"
    r"|(?:PIONEERING|ENERGY|FRONTIERS|ANNUAL\s+REPORT)"
    r"|(?:Tel:\s*\+)",
    re.I,
)

# Lines that indicate a fund/subsidiary entity (not the parent company)
_FUND_ENTITY_PATTERNS = re.compile(
    r"REMUNERATION\s+TO\s+THE\s+(?:MANAGEMENT|TRUSTEE)"
    r"|ACCOUNTING\s+AND\s+OPERATIONAL\s+CHARGES"
    r"|REMUNERATION\s+TO\s+THE\s+TRUSTEE"
    r"|INCOME\s+ALREADY\s+PAID\s+ON\s+UNITS"
    r"|ACCOUNTING\s+INCOME\s+AVAILABLE\s+FOR"
    r"|SINDH\s+SALES\s+TAX\s+ON\s+(?:REMUNERATION|TRUSTEE)"
    r"|NET\s+INCOME\s+FOR\s+THE\s+(?:YEAR|PERIOD)"
    r"|ALLOCATION\s+OF\s+NET\s+INCOME",
    re.I,
)

_SECTION_PATTERNS = [
    (re.compile(r"^NON.?CURRENT\s+ASSETS", re.I), "non_current_assets"),
    (re.compile(r"^CURRENT\s+ASSETS", re.I), "current_assets"),
    (re.compile(r"^(?:EQUITY\s+AND\s+LIABILITIES|CAPITAL\s+AND\s+LIABILITIES)", re.I), "equity_and_liabilities"),
    (re.compile(r"^SHARE\s+CAPITAL\s+AND\s+RESERVES|^SHAREHOLDERS?\s+FUNDS?", re.I), "equity"),
    (re.compile(r"^NON.?CURRENT\s+LIABILITIES", re.I), "non_current_liabilities"),
    (re.compile(r"^CURRENT\s+LIABILITIES", re.I), "current_liabilities"),
    (re.compile(r"^ASSETS\b", re.I), "assets"),
    (re.compile(r"^CONTINGENCIES\s+AND\s+COMMITMENTS", re.I), "contingencies"),
]


def _extract_line_items(text: str) -> list[dict[str, Any]]:
    """Extract all line items with text + numbers from a statement page.

    Returns list of dicts: {line, values, section, raw_line}
    """
    lines = text.split("\n")
    items = []
    current_section = ""

    for line in lines:
        raw_line = line
        line = line.strip()
        if not line or len(line) < 3:
            continue

        # Skip boilerplate
        if _SKIP_LINE_PATTERNS.search(line):
            continue

        # Stop if we hit fund/subsidiary entity lines (wrong entity)
        if _FUND_ENTITY_PATTERNS.search(line):
            break

        # Check for section header
        for pattern, section_name in _SECTION_PATTERNS:
            if pattern.search(line):
                current_section = section_name
                break

        # Fix split numbers
        fixed = _fix_split_numbers(line)

        # Extract text portion and numbers
        numbers = _extract_numbers(fixed)
        if not numbers:
            continue

        # Get text portion (everything before first number)
        text_match = re.match(r"^(.*?)[\d(\s]*[\d,]+", fixed)
        line_text = text_match.group(1).strip() if text_match else ""

        # Clean up line text
        line_text = re.sub(r"\s+", " ", line_text).strip()
        line_text = line_text.rstrip("-–—. ")

        # Skip if no meaningful text (standalone number lines are sub-totals)
        if not line_text or len(line_text) < 2:
            # Could be a total — check if it's just numbers
            if len(numbers) >= 2:
                # Label subtotals with context from preceding items
                sub_label = f"[subtotal in {current_section}]" if current_section else "[subtotal]"

                # Smart labeling: first subtotal after income items = total_income
                # Last subtotal before "Operating profit" = operating expenses total
                if items:
                    last_kpi = items[-1].get("kpi_code") if not items[-1].get("is_subtotal") else None
                    if last_kpi in ("income_debt_securities", "investment_income",
                                    "capital_gains", "markup_earned", "total_income"):
                        sub_label = "[total_income]"
                    elif last_kpi in ("total_assets",):
                        sub_label = "[total_assets_subtotal]"

                items.append({
                    "line": sub_label,
                    "values": numbers,
                    "section": current_section,
                    "raw_line": raw_line.strip(),
                    "is_subtotal": True,
                })
            continue

        # Filter note references
        numbers = _filter_note_refs(numbers, fixed)
        if not numbers:
            continue

        items.append({
            "line": line_text,
            "values": numbers,
            "section": current_section,
            "raw_line": raw_line.strip(),
            "is_subtotal": False,
        })

    return items


def _date_from_filename(filename: str) -> str | None:
    """Extract announcement date from PSX filename like 'Sep-19-2017_102859.pdf'.

    Returns YYYY-MM-DD or None. This is the announcement date, not the period end,
    but useful as a fallback year reference.
    """
    m = re.match(
        r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-(\d{1,2})-(\d{4})",
        filename, re.I,
    )
    if m:
        month_str, day, year = m.group(1), m.group(2), m.group(3)
        month_num = _MONTH_MAP.get(month_str.upper()[:3])
        if month_num:
            return f"{year}-{month_num}-{int(day):02d}"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Multi-Year Summary Extraction
# ─────────────────────────────────────────────────────────────────────────────

def _extract_multi_year(text: str) -> dict[str, Any] | None:
    """Extract multi-year performance summary table.

    Returns dict with year_columns and rows, or None if not a summary page.
    """
    lines = text.split("\n")

    # Find year header row (e.g., "2019-20 2020-21 2021-22 2022-23 2023-24 2024-25")
    years = []
    year_line_idx = None
    for i, line in enumerate(lines[:10]):
        # Match fiscal years: 2019-20 or 2024-25 or plain 2024
        found = re.findall(r"\b(20\d{2}(?:-\d{2})?)\b", line)
        if len(found) >= 3:
            years = found
            year_line_idx = i
            break

    if not years or len(years) < 3:
        return None

    # Extract rows
    items = []
    current_section = ""

    for line in lines[year_line_idx + 1:]:
        line = line.strip()
        if not line or len(line) < 3:
            continue
        if _SKIP_LINE_PATTERNS.search(line):
            continue

        # Detect section headers (lines with no numbers)
        numbers = _extract_numbers(line)
        if not numbers:
            # Could be a section header
            if len(line) > 3 and not line.startswith("(") and not line[0].isdigit():
                current_section = line.strip()
            continue

        # Get text portion
        text_match = re.match(r"^(.*?)[\d(\s]*[\d,]+", line)
        line_text = text_match.group(1).strip() if text_match else ""
        line_text = re.sub(r"\s+", " ", line_text).strip().rstrip("-–—. ")

        if not line_text or len(line_text) < 2:
            continue

        # Get unit if present (e.g., "Rs in billion", "%", "Times", "Numbers")
        unit = ""
        unit_match = re.search(r"(Rs\s+in\s+\w+|%|Times|Numbers|Rupees|BOE)", line, re.I)
        if unit_match:
            unit = unit_match.group(1)
            # Remove unit from line_text
            line_text = line_text.replace(unit, "").strip().rstrip("-–—. ")

        items.append({
            "line": line_text,
            "values": numbers[:len(years)],  # Align to year count
            "section": current_section,
            "unit": unit,
        })

    if not items:
        return None

    return {
        "years": years,
        "items": items,
    }


# ─────────────────────────────────────────────────────────────────────────────
# OCR Fallback
# ─────────────────────────────────────────────────────────────────────────────

_TESSDATA_PREFIX = "/opt/miniconda/envs/psx/share/tessdata"


# ─────────────────────────────────────────────────────────────────────────────
# Image Preprocessing (OpenCV-based, quality-adaptive)
# ─────────────────────────────────────────────────────────────────────────────

def _classify_scan_quality(gray: "np.ndarray") -> str:
    """Classify scan quality to choose preprocessing. Returns: clean/noisy/faded/dark/mixed."""
    import cv2
    mean_i = gray.mean()
    std_i = gray.std()
    lap_var = cv2.Laplacian(gray, cv2.CV_64F).var()

    if mean_i > 220 and std_i < 30:
        return "faded"
    elif mean_i < 100:
        return "dark"
    elif lap_var < 50:
        return "noisy"
    elif lap_var > 500 and std_i > 50:
        return "clean"
    else:
        return "mixed"


def _preprocess_for_ocr(img: "np.ndarray", quality: str | None = None) -> "np.ndarray":
    """Quality-adaptive image preprocessing for OCR."""
    import cv2
    import numpy as np

    gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY) if len(img.shape) == 3 else img.copy()

    if quality is None:
        quality = _classify_scan_quality(gray)

    if quality == "clean":
        clahe = cv2.createCLAHE(clipLimit=1.5, tileGridSize=(8, 8))
        return clahe.apply(gray)

    elif quality == "faded":
        clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray)
        return cv2.adaptiveThreshold(
            enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 15, 8
        )

    elif quality == "dark":
        if gray.mean() < 127:
            gray = cv2.bitwise_not(gray)
        clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray)
        return cv2.adaptiveThreshold(
            enhanced, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
            cv2.THRESH_BINARY, 15, 8
        )

    elif quality == "noisy":
        denoised = cv2.fastNlMeansDenoising(gray, None, h=12, templateWindowSize=7, searchWindowSize=21)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(denoised)
        kernel = np.array([[-0.5, -0.5, -0.5], [-0.5, 5.0, -0.5], [-0.5, -0.5, -0.5]])
        result = cv2.filter2D(enhanced, -1, kernel)
        return np.clip(result, 0, 255).astype(np.uint8)

    else:  # mixed
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8, 8))
        enhanced = clahe.apply(gray)
        kernel = np.array([[0, -0.5, 0], [-0.5, 3.0, -0.5], [0, -0.5, 0]])
        result = cv2.filter2D(enhanced, -1, kernel)
        return np.clip(result, 0, 255).astype(np.uint8)


def _detect_rotation_fast(gray: "np.ndarray") -> int:
    """Detect page rotation using text line orientation (no OCR needed).

    Returns: 0, 90, or 270.
    """
    import cv2

    _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV + cv2.THRESH_OTSU)

    # Detect horizontal vs vertical text lines
    kernel_h = cv2.getStructuringElement(cv2.MORPH_RECT, (30, 1))
    kernel_v = cv2.getStructuringElement(cv2.MORPH_RECT, (1, 30))

    h_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_h)
    v_lines = cv2.morphologyEx(binary, cv2.MORPH_OPEN, kernel_v)

    h_score = h_lines.sum()
    v_score = v_lines.sum()

    if v_score > h_score * 2:
        # Page is rotated — determine 90 vs 270 by checking top content
        rot_90 = cv2.rotate(binary, cv2.ROTATE_90_CLOCKWISE)
        rot_270 = cv2.rotate(binary, cv2.ROTATE_90_COUNTERCLOCKWISE)
        top_90 = rot_90[:rot_90.shape[0] // 4, :].sum()
        top_270 = rot_270[:rot_270.shape[0] // 4, :].sum()
        return 90 if top_90 > top_270 else 270

    return 0


def _clean_ocr_text(text: str) -> str:
    """Clean common OCR artifacts in financial documents."""
    # Fix pipe → 1 in number contexts
    text = re.sub(r"\|(\d)", r"1\1", text)
    text = re.sub(r"(\d)\|", r"\g<1>1", text)
    # Fix lowercase L before comma → 1
    text = re.sub(r"l,(\d{3})", r"1,\1", text)
    # Fix letter O in numbers → 0
    text = re.sub(r"(\d)[oO](\d)", r"\g<1>0\2", text)
    # Fix Rupees in thousands OCR errors
    text = re.sub(r"Rupees\s+in\s+['\u2018\u2019]?0+", "Rupees in '000", text)
    # Remove stray vertical bars from table borders
    text = re.sub(r"\s*[|]\s*$", "", text, flags=re.MULTILINE)
    text = re.sub(r"^\s*[|]\s*", "", text, flags=re.MULTILINE)
    return text


# Module-level EasyOCR reader (initialized once, reused)
_easyocr_reader = None


def _get_easyocr_reader():
    """Lazy-init EasyOCR reader with GPU."""
    global _easyocr_reader
    if _easyocr_reader is None:
        try:
            import easyocr
            _easyocr_reader = easyocr.Reader(["en"], gpu=True, verbose=False)
        except Exception:
            _easyocr_reader = False  # Mark as unavailable
    return _easyocr_reader if _easyocr_reader is not False else None


def _easyocr_to_text(results: list) -> str:
    """Convert EasyOCR results (bounding boxes) to structured text lines.

    Groups text blocks by y-position into lines. Uses average character height
    to determine line grouping threshold.
    """
    if not results:
        return ""

    # Filter low confidence and sort by y
    blocks = []
    for bbox, text, conf in results:
        if conf < 0.15 or not text.strip():
            continue
        top_y = int(bbox[0][1])
        bot_y = int(bbox[2][1])
        left_x = int(bbox[0][0])
        height = bot_y - top_y
        blocks.append((top_y, left_x, height, text.strip()))

    if not blocks:
        return ""

    blocks.sort(key=lambda b: (b[0], b[1]))

    # Calculate median text height for line grouping threshold
    heights = [b[2] for b in blocks if b[2] > 5]
    median_h = sorted(heights)[len(heights) // 2] if heights else 20
    line_threshold = max(median_h * 0.6, 10)

    # Group into lines by y-proximity
    lines: list[list[tuple[int, str]]] = []
    current_line: list[tuple[int, str]] = []
    current_y = blocks[0][0]

    for top_y, left_x, height, text in blocks:
        if current_line and abs(top_y - current_y) > line_threshold:
            lines.append(current_line)
            current_line = []
        current_line.append((left_x, text))
        # Use average y of blocks in current line
        current_y = top_y

    if current_line:
        lines.append(current_line)

    # Build text: sort each line by x position, join with appropriate spacing
    text_lines = []
    for line in lines:
        line.sort(key=lambda t: t[0])
        text_lines.append(" ".join(t[1] for t in line))

    return "\n".join(text_lines)


def _ocr_page(page, resolution: int = 300) -> str | None:
    """OCR a pdfplumber page. Uses EasyOCR (GPU) primary, tesserocr fallback.

    Pipeline:
    1. Render page → numpy array
    2. Fast rotation detection (OpenCV morphology)
    3. Quality-adaptive preprocessing
    4. EasyOCR (GPU) or tesserocr fallback
    5. Clean artifacts
    """
    try:
        import numpy as np

        # Render page to image
        rendered = page.to_image(resolution=resolution)
        pil_img = rendered.original

        if pil_img.size[0] < 100 or pil_img.size[1] < 100:
            return None

        img_np = np.array(pil_img)
        gray = np.array(pil_img.convert("L"))

        # Step 1: Fast rotation detection (OpenCV)
        # Skip if pdfplumber already applied PDF-level rotation (page.rotation)
        page_rotation = getattr(page, 'rotation', 0) or 0
        if page_rotation == 0:
            try:
                rotation = _detect_rotation_fast(gray)
                if rotation:
                    import cv2
                    if rotation == 90:
                        img_np = cv2.rotate(img_np, cv2.ROTATE_90_CLOCKWISE)
                        gray = cv2.rotate(gray, cv2.ROTATE_90_CLOCKWISE)
                    elif rotation == 270:
                        img_np = cv2.rotate(img_np, cv2.ROTATE_90_COUNTERCLOCKWISE)
                        gray = cv2.rotate(gray, cv2.ROTATE_90_COUNTERCLOCKWISE)
            except Exception:
                pass

        # Step 2: Quality-adaptive preprocessing
        try:
            processed = _preprocess_for_ocr(gray)
        except Exception:
            processed = gray

        # Step 3: OCR — tesserocr for structure, EasyOCR for quality validation
        # tesserocr PSM 6 (table mode) reads line-by-line — best for financial tables
        # EasyOCR reads individual text blocks — better character accuracy but loses structure
        text = None

        # Primary: tesserocr (better table structure)
        try:
            import tesserocr
            import os
            import sys
            os.environ.setdefault("TESSDATA_PREFIX", _TESSDATA_PREFIX)

            from PIL import Image
            ocr_img = Image.fromarray(processed)
            old_stderr = sys.stderr
            sys.stderr = open(os.devnull, "w")
            try:
                text = tesserocr.image_to_text(ocr_img, lang="eng", psm=6)
            finally:
                sys.stderr.close()
                sys.stderr = old_stderr
        except ImportError:
            pass

        # If tesserocr failed or produced garbage, try EasyOCR
        if not text or len(text.strip()) < 50:
            reader = _get_easyocr_reader()
            if reader is not None:
                try:
                    results = reader.readtext(processed, detail=1, paragraph=False)
                    text = _easyocr_to_text(results)
                except Exception:
                    pass

        if not text or len(text.strip()) < 50:
            return None

        return _clean_ocr_text(text)

    except Exception as e:
        logger.debug("OCR failed for page: %s", e)
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Main Extract Function
# ─────────────────────────────────────────────────────────────────────────────

def extract_pdf(
    pdf_content: bytes,
    symbol: str = "",
    sector_code: str | None = None,
    filename: str = "",
) -> dict[str, Any]:
    """Extract all financial data from a PDF.

    Returns structured dict with raw line items, period info, and metadata.
    """
    try:
        import pdfplumber
    except ImportError:
        return _empty_result(symbol, "pdfplumber not installed")

    result = _empty_result(symbol)

    try:
        with pdfplumber.open(io.BytesIO(pdf_content)) as pdf:
            if not pdf.pages:
                result["warnings"].append("PDF has no pages")
                return result

            result["page_count"] = len(pdf.pages)

            # Classify pages — smart scanning for large PDFs
            page_map: dict[str, list[int]] = {}
            total_text_chars = 0
            n_pages = len(pdf.pages)

            # For large PDFs (annual reports), limit OCR to likely pages
            # Financial statements are typically in pages 5-50 of annual reports
            if n_pages > 30:
                # Large PDF: scan strategically
                # Pages 0-5: cover/contents, 5-50: financial statements, 50+: notes
                scan_ranges = list(range(min(5, n_pages))) + list(range(5, min(50, n_pages)))
                # Also check last few pages for multi-year summary
                scan_ranges += list(range(max(0, n_pages - 10), n_pages))
                scan_pages = sorted(set(scan_ranges))
            else:
                scan_pages = list(range(n_pages))

            found_pl = False
            found_bs = False

            for i in scan_pages:
                page = pdf.pages[i]
                text = page.extract_text() or ""

                # Check for CID-encoded fonts (common in PSX portal PDFs)
                has_cid = "(cid:" in text

                # OCR fallback if empty or CID-encoded
                if len(text.strip()) < 20 or has_cid:
                    # Skip OCR on pages we don't need (already found P&L and BS)
                    if found_pl and found_bs and i not in scan_pages[-10:]:
                        continue
                    ocr_text = _ocr_page(page)
                    if ocr_text:
                        text = ocr_text
                        result["is_ocr"] = True

                total_text_chars += len(text)

                if len(text.strip()) < 20:
                    continue

                page_type = classify_page(text)
                if page_type != "unknown":
                    page_map.setdefault(page_type, []).append(i)
                    result.setdefault("_page_texts", {})[i] = text
                    if page_type == "pl":
                        found_pl = True
                    elif page_type == "bs":
                        found_bs = True

            result["text_chars"] = total_text_chars

            if total_text_chars < 50:
                result["is_scanned"] = True
                result["warnings"].append("Scanned PDF — no extractable text")
                return result

            texts = result.pop("_page_texts", {})

            def _pick_company_page(pages: list[int], stmt_texts: dict, sym: str) -> int:
                """Pick the page belonging to the company (not subsidiary/fund).

                If symbol is in page header, prefer that. Otherwise use first page.
                Multi-entity PDFs (e.g., 786 Investments + 786 Smart Fund) need this.
                """
                if len(pages) == 1:
                    return pages[0]

                # Try to match symbol or company name in first 3 lines
                sym_upper = sym.upper()
                for idx in pages:
                    text = stmt_texts.get(idx, "")
                    header = "\n".join(text.split("\n")[:5]).upper()
                    # Check if symbol or "SYMBOL" + common suffixes is in header
                    if sym_upper in header or f"{sym_upper} " in header:
                        return idx
                    # Check for "LIMITED" but not "FUND" (subsidiary fund)
                    if "LIMITED" in header and "FUND" not in header and "SMART" not in header:
                        return idx

                # Fallback: first page (more likely the parent company)
                return pages[0]

            # ── Extract P&L ──
            pl_pages = page_map.get("pl", [])
            if pl_pages:
                # Pick the page belonging to the company (not subsidiary/fund)
                pl_idx = _pick_company_page(pl_pages, texts, symbol)
                # Include consecutive PL pages ONLY if same entity
                pl_text = texts.get(pl_idx, "")
                company_header = "\n".join(pl_text.split("\n")[:3]).upper()
                for next_idx in range(pl_idx + 1, min(pl_idx + 3, len(pdf.pages))):
                    if next_idx in texts:
                        next_text = texts[next_idx]
                        next_type = classify_page(next_text)
                        next_header = "\n".join(next_text.split("\n")[:3]).upper()
                        # Stop if different entity (e.g., subsidiary fund)
                        if next_type == "pl" and next_header != company_header:
                            break
                        if next_type in ("pl", "unknown"):
                            pl_text += "\n" + next_text
                        else:
                            break

                period = _parse_dates_from_header(pl_text.split("\n"))
                result["period_info"].update(period)

                scale_mult, scale_label = _detect_scale(pl_text.split("\n"))
                result["scale_multiplier"] = scale_mult
                result["scale_label"] = scale_label

                items = _extract_line_items(pl_text)
                result["statements"]["pl"] = items
                result["pages_used"].extend([pl_idx])
            else:
                result["warnings"].append("No P&L pages found")

            # ── Extract BS ──
            bs_pages = page_map.get("bs", [])
            if bs_pages:
                # Pick the page belonging to the company (not subsidiary/fund)
                bs_idx = _pick_company_page(bs_pages, texts, symbol)
                bs_text = texts.get(bs_idx, "")
                # Include consecutive BS pages ONLY if same entity
                bs_header = "\n".join(bs_text.split("\n")[:3]).upper()
                for next_idx in range(bs_idx + 1, min(bs_idx + 3, len(pdf.pages))):
                    if next_idx in texts:
                        next_text = texts[next_idx]
                        next_type = classify_page(next_text)
                        next_header = "\n".join(next_text.split("\n")[:3]).upper()
                        if next_type == "bs" and next_header != bs_header:
                            break
                        if next_type in ("bs", "unknown"):
                            bs_text += "\n" + next_text
                        else:
                            break

                if not result["period_info"].get("period_end"):
                    period = _parse_dates_from_header(bs_text.split("\n"))
                    result["period_info"].update(period)

                if not result.get("scale_multiplier"):
                    scale_mult, scale_label = _detect_scale(bs_text.split("\n"))
                    result["scale_multiplier"] = scale_mult
                    result["scale_label"] = scale_label

                items = _extract_line_items(bs_text)
                result["statements"]["bs"] = items
                result["pages_used"].extend([bs_idx])
            else:
                result["warnings"].append("No Balance Sheet pages found")

            # ── Extract Multi-Year Summary ──
            my_pages = page_map.get("multi_year", [])
            for mi in my_pages:
                my_text = texts.get(mi, "")
                # Include consecutive summary pages
                for next_idx in range(mi + 1, min(mi + 4, len(pdf.pages))):
                    if next_idx in texts:
                        my_text += "\n" + texts[next_idx]

                summary = _extract_multi_year(my_text)
                if summary:
                    result["multi_year"] = summary
                    result["pages_used"].append(mi)
                    break

            # ── Apply catalog matching ──
            from pakfindata.sources.kpi_catalog import match_kpi, get_format_family

            fmt = get_format_family(sector_code)
            result["format_family"] = fmt

            for stmt_type in ("pl", "bs"):
                for item in result["statements"].get(stmt_type, []):
                    kpi = match_kpi(item["line"], stmt_type)
                    item["kpi_code"] = kpi

            if result.get("multi_year"):
                for item in result["multi_year"].get("items", []):
                    kpi = match_kpi(item["line"], "ratio")
                    if not kpi:
                        kpi = match_kpi(item["line"], "pl")
                    if not kpi:
                        kpi = match_kpi(item["line"], "bs")
                    item["kpi_code"] = kpi

            # ── Validate period_end using filename date ──
            period_end = result["period_info"].get("period_end")
            if filename and period_end:
                file_date = _date_from_filename(filename)
                if file_date:
                    file_year = int(file_date[:4])
                    period_year = int(period_end[:4])
                    # Period end can't be after the file download date (+ 1 year tolerance for fiscal years)
                    if period_year > file_year + 1:
                        result["warnings"].append(
                            f"OCR date likely wrong: {period_end} (file from {file_date})"
                        )
                        result["period_info"]["period_end"] = None
            # If no period_end extracted, try to infer year from filename
            if filename and not result["period_info"].get("period_end"):
                file_date = _date_from_filename(filename)
                if file_date:
                    result["period_info"]["file_date"] = file_date

    except Exception as e:
        result["warnings"].append(f"Parse error: {e}")
        logger.warning("Failed to parse PDF for %s: %s", symbol, e)

    return result


def _empty_result(symbol: str = "", error: str | None = None) -> dict[str, Any]:
    """Create empty result template."""
    r = {
        "symbol": symbol,
        "statements": {"pl": [], "bs": []},
        "period_info": {
            "period_end": None,
            "period_type": None,
            "column_dates": [],
            "is_audited": None,
        },
        "multi_year": None,
        "scale_multiplier": 1,
        "scale_label": "units",
        "format_family": "STANDARD",
        "pages_used": [],
        "page_count": 0,
        "text_chars": 0,
        "is_scanned": False,
        "is_ocr": False,
        "warnings": [],
    }
    if error:
        r["warnings"].append(error)
    return r
