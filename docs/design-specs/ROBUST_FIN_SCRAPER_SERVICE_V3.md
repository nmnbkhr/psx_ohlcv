# Claude Code Prompt: Robust Financial Scraper — Parser Upgrade + Service Mode

## What This Prompt Does (and Does NOT Do)

**DOES:**
- Creates `engine/universal_fin_parser.py` (better parser with synonym maps, multi-strategy, confidence scoring)
- Creates `services/scraper_service.py` daemon (same pattern as tick_service)
- Adds 3 new pages to the EXISTING `fin_scraper_app.py` (Dashboard, Parser Test, Service)
- Writes to the EXISTING `company_financials` table using its ACTUAL column names (`sales` not `revenue`, `eps` not `eps_basic`)
- Uses the EXISTING `sources/dps_announcements.download_dps_financials_batch()` with smart stale tracking
- Uses the EXISTING `sources/fin_downloader.download_financials()` for IR website PDFs

**DOES NOT:**
- Does NOT replace `fin_scraper_app.py` — adds 3 radio options to existing selector
- Does NOT create a new `company_financials_v2` table — writes to existing table
- Does NOT remove existing pages (Deep Scrape, Website Scan, DPS Announcements, PDF Download, PDF Import, Browse Files)
- Does NOT remove the sidebar (symbol refresh, SCD2 status tracking, definitions editor)
- Does NOT rewrite `sources/dps_announcements.py` or `sources/fin_downloader.py` — imports and calls them

## Step 0: Audit Existing Code — CRITICAL

```bash
cd ~/pakfindata && git checkout phase3-ui-arch && conda activate psx

# 1. Read the EXISTING fin_scraper_app.py — understand ALL current tabs
wc -l src/pakfindata/fin_scraper_app.py
head -100 src/pakfindata/fin_scraper_app.py
grep -n "st\.tabs\|st\.tab\|def render\|def _render\|Tab\|tab" src/pakfindata/fin_scraper_app.py

# 2. Read the EXISTING financial_parser.py — this is what we REPLACE
wc -l src/pakfindata/sources/financial_parser.py
grep -n "^def \|^class " src/pakfindata/sources/financial_parser.py
# Note the function signatures — any callers must be updated

# 3. Find ALL callers of financial_parser.py
grep -rn "financial_parser\|parse_ir_pdf\|flatten_parsed_to_financials\|upsert_company_financials" \
    src/ --include="*.py" | grep -v __pycache__

# 4. Read the EXISTING company_financials table schema
python3 -c "
import sqlite3
con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
cols = con.execute('PRAGMA table_info(company_financials)').fetchall()
print('=== company_financials schema ===')
for c in cols:
    print(f'  {c[0]:3d} {c[1]:30s} {c[2]}')
count = con.execute('SELECT COUNT(*) FROM company_financials').fetchone()[0]
symbols = con.execute('SELECT COUNT(DISTINCT symbol) FROM company_financials').fetchone()[0]
print(f'\nRows: {count:,}  Symbols: {symbols}')

# Sample a few rows to see actual column names and data format
sample = con.execute('SELECT * FROM company_financials LIMIT 3').fetchall()
col_names = [c[1] for c in cols]
for row in sample:
    print(f'\n--- {dict(zip(col_names, row))}')
con.close()
"

# 5. Read dps_announcements.py — this is what scraper_service calls
wc -l src/pakfindata/sources/dps_announcements.py
grep -n "^def \|^class " src/pakfindata/sources/dps_announcements.py

# 6. Read fin_downloader.py — understand its interface
grep -n "^def \|^class " src/pakfindata/sources/fin_downloader.py 2>/dev/null || \
grep -n "^def \|^class " src/pakfindata/engine/fin_downloader.py 2>/dev/null

# 7. Check what PDFs exist and their naming patterns
ls /mnt/e/psxsymbolfin/ 2>/dev/null | head -20
find /mnt/e/psxsymbolfin/ -name "*.pdf" 2>/dev/null | wc -l
find /mnt/e/psxsymbolfin/ -name "*.pdf" 2>/dev/null | shuf | head -10

# 8. Check available PDF libraries
python3 -c "
for lib in ['pdfplumber', 'tabula', 'camelot', 'PyPDF2', 'pymupdf', 'fitz', 'pdfminer']:
    try:
        __import__(lib)
        print(f'  ✓ {lib}')
    except ImportError:
        print(f'  ✗ {lib}')
"
```

**READ ALL OUTPUT.** The company_financials schema determines the column mapping.
The fin_scraper_app.py tab structure determines where new tabs are inserted.
The financial_parser.py function signatures determine what callers need updating.

## Step 1: Install pdfplumber (if missing)

```bash
conda activate psx
pip install pdfplumber --break-system-packages
```

## Step 2: Create the Universal Financial Statement Parser

Create `src/pakfindata/engine/universal_fin_parser.py`:

This REPLACES `sources/financial_parser.py`. After testing, update all callers
to import from `engine.universal_fin_parser` instead of `sources.financial_parser`.

```python
"""
Universal Financial Statement Parser for PSX Company PDFs.

REPLACES: sources/financial_parser.py (851 lines)
WRITES TO: existing company_financials table (same column names)

The Problem:
  PSX companies file financial results as PDFs with wildly different formats:
  - Different column layouts (Notes col on left, right, or absent)
  - Different line item names ("Revenue" vs "Sales" vs "Turnover")
  - Different number formats (millions, thousands, parentheses for negatives)
  - Different periods (annual, half-year, quarterly)
  - Consolidated vs standalone in same PDF

The Solution — Multi-Strategy Extraction:
  Strategy 1: pdfplumber table extraction (best for clean tables)
  Strategy 2: Line-by-line text parsing with fuzzy label matching
  Strategy 3: Regex-based extraction on raw text (fallback)

  Each strategy produces a normalized FinancialExtraction. The best result
  (highest confidence) wins.

Line Item Normalization:
  A SYNONYM MAP maps every label variant to a canonical field name.
  "Revenue", "Sales", "Turnover", "Net Revenue", "Net Sales" → all map to "revenue".

Usage:
  from pakfindata.engine.universal_fin_parser import parse_financial_pdf, flatten_to_db_row
  result = parse_financial_pdf("/path/to/file.pdf")
  row = flatten_to_db_row(result)  # dict matching company_financials columns
"""

import re
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger("universal_fin_parser")


# ═══════════════════════════════════════════════════════
# SYNONYM MAP — maps every label variant to canonical name
# ═══════════════════════════════════════════════════════

# Income Statement synonyms
IS_SYNONYMS: dict[str, list[str]] = {
    "revenue": [
        "revenue", "net revenue", "sales", "net sales", "turnover",
        "total revenue", "gross revenue", "revenue from contracts",
        "revenue from operations", "operating revenue",
        "income from operations", "total sales",
    ],
    "cost_of_sales": [
        "cost of sales", "cost of goods sold", "cogs",
        "cost of revenue", "cost of products sold",
        "direct costs", "cost of goods manufactured and sold",
        "cost of services", "operating costs",
    ],
    "gross_profit": [
        "gross profit", "gross margin", "gross income",
        "gross profit / (loss)", "gross profit/(loss)",
    ],
    "admin_expenses": [
        "administrative expenses", "admin expenses",
        "general and administrative expenses",
        "administrative and general expenses",
    ],
    "distribution_cost": [
        "distribution cost", "distribution costs",
        "selling and distribution", "distribution expenses",
        "selling expenses", "marketing expenses",
        "distribution and marketing expenses",
        "selling, general and administrative",
    ],
    "other_income": [
        "other income", "other operating income",
        "other revenue", "non-operating income",
        "income from other sources",
    ],
    "other_charges": [
        "other charges", "other expenses", "other operating charges",
        "other operating expenses", "impairment",
    ],
    "operating_profit": [
        "operating profit", "profit from operations",
        "operating income", "operating profit / (loss)",
        "profit/(loss) from operations", "ebit",
        "results from operating activities",
    ],
    "finance_cost": [
        "finance cost", "finance costs", "financial charges",
        "interest expense", "markup expense",
        "markup / interest expense", "borrowing costs",
        "finance charges", "interest and bank charges",
    ],
    "finance_income": [
        "finance income", "interest income", "markup income",
        "return on bank deposits", "income from financial assets",
    ],
    "profit_before_tax": [
        "profit before tax", "profit before taxation",
        "profit / (loss) before tax", "income before tax",
        "profit before income tax", "pbt",
        "profit/(loss) before taxation",
    ],
    "taxation": [
        "taxation", "tax expense", "income tax expense",
        "provision for taxation", "tax charge",
        "income tax", "current tax", "tax",
    ],
    "profit_after_tax": [
        "profit after tax", "profit after taxation",
        "net profit", "net income", "net profit / (loss)",
        "profit for the period", "profit for the year",
        "profit/(loss) after taxation", "pat", "net income/(loss)",
        "profit / (loss) for the period",
    ],
    "eps_basic": [
        "earnings per share", "eps", "basic eps",
        "earnings per share - basic", "basic earnings per share",
        "(loss)/earnings per share", "loss per share",
        "earnings / (loss) per share",
    ],
    "eps_diluted": [
        "diluted eps", "diluted earnings per share",
        "earnings per share - diluted",
    ],
    "ebitda": [
        "ebitda", "earnings before interest, tax, depreciation",
    ],
    "depreciation": [
        "depreciation", "depreciation and amortization",
        "depreciation and amortisation",
        "depreciation & amortization",
    ],
}

# Balance Sheet synonyms
BS_SYNONYMS: dict[str, list[str]] = {
    "total_assets": [
        "total assets", "assets total",
    ],
    "non_current_assets": [
        "non-current assets", "non current assets",
        "fixed assets", "property, plant and equipment",
        "property plant and equipment", "ppe",
        "total non-current assets", "total non current assets",
    ],
    "current_assets": [
        "current assets", "total current assets",
    ],
    "total_equity": [
        "total equity", "shareholders' equity",
        "shareholders equity", "share capital and reserves",
        "equity and reserves", "total equity and reserves",
        "equity attributable to owners",
    ],
    "share_capital": [
        "share capital", "issued capital", "paid-up capital",
        "paid up capital", "ordinary share capital",
        "issued, subscribed and paid-up capital",
    ],
    "reserves": [
        "reserves", "revenue reserves", "capital reserves",
        "retained earnings", "unappropriated profit",
        "accumulated profit", "accumulated profit/(loss)",
        "surplus on revaluation",
    ],
    "total_liabilities": [
        "total liabilities", "liabilities total",
    ],
    "non_current_liabilities": [
        "non-current liabilities", "non current liabilities",
        "long-term liabilities", "long term liabilities",
        "total non-current liabilities",
    ],
    "current_liabilities": [
        "current liabilities", "total current liabilities",
    ],
    "total_equity_and_liabilities": [
        "total equity and liabilities",
        "total liabilities and equity",
    ],
    "long_term_debt": [
        "long term financing", "long-term financing",
        "long term borrowings", "long-term borrowings",
        "long term debt", "long-term debt",
        "long term loans", "long-term loans",
    ],
    "short_term_debt": [
        "short term borrowings", "short-term borrowings",
        "short term financing", "current portion of long term",
        "running finance", "short term debt",
    ],
    "trade_payables": [
        "trade and other payables", "trade payables",
        "creditors", "accounts payable",
    ],
    "trade_receivables": [
        "trade debts", "trade receivables",
        "trade and other receivables", "debtors",
        "accounts receivable",
    ],
    "cash_and_equivalents": [
        "cash and bank balances", "cash and cash equivalents",
        "bank balances", "cash at bank", "cash",
    ],
    "inventories": [
        "stock in trade", "inventories", "stores and spares",
        "stores, spares and loose tools", "stock-in-trade",
    ],
}

# Cash Flow synonyms
CF_SYNONYMS: dict[str, list[str]] = {
    "operations": [
        "cash generated from operations", "net cash from operating",
        "cash flows from operating activities",
        "net cash generated from operating",
        "cash from operations",
    ],
    "investing": [
        "net cash used in investing", "cash flows from investing",
        "net cash from investing activities",
        "cash used in investing",
    ],
    "financing": [
        "net cash from financing", "cash flows from financing",
        "net cash used in financing activities",
        "cash from financing",
    ],
}


def _normalize_label(text: str) -> str:
    """Normalize a financial label for matching."""
    t = text.lower().strip()
    t = re.sub(r'\(?\s*note[\s\-]*\d+\.?\d*\s*\)?', '', t)
    t = re.sub(r'\s+\d+\.?\d*\s*$', '', t)
    t = re.sub(r'[^\w\s/\-\(\)]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def _match_label(text: str, synonym_map: dict[str, list[str]]) -> Optional[str]:
    """Match a text label to a canonical field name using fuzzy matching."""
    normalized = _normalize_label(text)
    if not normalized or len(normalized) < 3:
        return None

    for canonical, synonyms in synonym_map.items():
        for syn in synonyms:
            if normalized == syn:
                return canonical

    for canonical, synonyms in synonym_map.items():
        for syn in synonyms:
            if len(syn) > 5 and syn in normalized:
                return canonical

    for canonical, synonyms in synonym_map.items():
        for syn in synonyms:
            if len(normalized) > 5 and normalized in syn:
                return canonical

    return None


def _parse_number(text: str) -> Optional[float]:
    """Parse a financial number. Handles commas, parentheses (negative), dashes."""
    if not text:
        return None
    t = text.strip()
    if not t or t in ('-', '–', '—', 'N/A', 'n/a', '-  -', '- -'):
        return None

    is_negative = False
    if t.startswith('(') and t.endswith(')'):
        is_negative = True
        t = t[1:-1].strip()
    elif t.startswith('-'):
        is_negative = True
        t = t[1:].strip()

    t = t.replace(',', '').replace(' ', '')

    try:
        val = float(t)
        return -val if is_negative else val
    except ValueError:
        return None


# ═══════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════

@dataclass
class FinancialExtraction:
    """Result of parsing a single financial PDF."""
    symbol: str = ""
    source_file: str = ""
    period_end: str = ""           # "2024-12-31"
    period_type: str = ""          # "annual", "half_year", "quarterly"
    is_consolidated: bool = True
    unit_scale: float = 1.0        # 1=absolute, 1000=thousands, 1_000_000=millions

    income_statement: dict = field(default_factory=dict)
    balance_sheet: dict = field(default_factory=dict)
    cash_flow: dict = field(default_factory=dict)

    confidence: float = 0.0
    strategy_used: str = ""
    warnings: list = field(default_factory=list)
    raw_tables: list = field(default_factory=list)

    def score(self) -> float:
        """Calculate confidence score based on completeness."""
        score = 0.0
        is_f = self.income_statement

        if is_f.get("revenue"): score += 10
        if is_f.get("gross_profit"): score += 5
        if is_f.get("operating_profit"): score += 5
        if is_f.get("profit_before_tax"): score += 5
        if is_f.get("profit_after_tax"): score += 10
        if is_f.get("eps_basic"): score += 10
        if is_f.get("cost_of_sales"): score += 5

        bs = self.balance_sheet
        if bs.get("total_assets"): score += 10
        if bs.get("total_equity"): score += 10
        if bs.get("total_liabilities"): score += 5
        if bs.get("current_assets"): score += 2.5
        if bs.get("non_current_assets"): score += 2.5

        cf = self.cash_flow
        if cf.get("operations"): score += 5
        if cf.get("investing"): score += 5
        if cf.get("financing"): score += 5

        if self.period_end: score += 3
        if self.period_type: score += 2

        self.confidence = round(score / 100, 2)
        return self.confidence


# ═══════════════════════════════════════════════════════
# STRATEGY 1: pdfplumber table extraction
# ═══════════════════════════════════════════════════════

def _strategy_pdfplumber(pdf_path: str, symbol: str) -> FinancialExtraction:
    """Extract financials using pdfplumber's table detection."""
    import pdfplumber

    result = FinancialExtraction(symbol=symbol, source_file=str(pdf_path),
                                 strategy_used="pdfplumber")

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_tables = []
            all_text_lines = []

            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                all_text_lines.extend(text.split('\n'))

                tables = page.extract_tables({
                    "vertical_strategy": "text",
                    "horizontal_strategy": "text",
                    "snap_tolerance": 5,
                    "join_tolerance": 5,
                    "min_words_vertical": 2,
                    "min_words_horizontal": 1,
                })
                for table in tables:
                    if table and len(table) > 2:
                        all_tables.append({"page": page_num + 1, "rows": table})

            result.raw_tables = all_tables
            result.unit_scale = _detect_unit_scale(all_text_lines)
            result.period_end, result.period_type = _detect_period(all_text_lines)
            result.is_consolidated = _detect_consolidated(all_text_lines)

            for tbl in all_tables:
                _classify_and_extract_table(tbl["rows"], result)

    except Exception as e:
        result.warnings.append(f"pdfplumber error: {e}")
        logger.warning("pdfplumber failed on %s: %s", pdf_path, e)

    result.score()
    return result


def _classify_and_extract_table(rows: list[list], result: FinancialExtraction):
    """Classify a table as IS/BS/CF and extract values."""
    if not rows or len(rows) < 3:
        return

    is_score, bs_score, cf_score = 0, 0, 0
    for row in rows:
        if not row:
            continue
        label = str(row[0] or "").strip()
        if _match_label(label, IS_SYNONYMS): is_score += 1
        if _match_label(label, BS_SYNONYMS): bs_score += 1
        if _match_label(label, CF_SYNONYMS): cf_score += 1

    if is_score >= bs_score and is_score >= cf_score and is_score > 0:
        target_map, target_dict = IS_SYNONYMS, result.income_statement
    elif bs_score >= cf_score and bs_score > 0:
        target_map, target_dict = BS_SYNONYMS, result.balance_sheet
    elif cf_score > 0:
        target_map, target_dict = CF_SYNONYMS, result.cash_flow
    else:
        return

    notes_col = _detect_notes_column(rows)

    for row in rows:
        if not row or len(row) < 2:
            continue
        label = str(row[0] or "").strip()
        canonical = _match_label(label, target_map)
        if not canonical:
            continue

        for i, cell in enumerate(row[1:], 1):
            if i == notes_col:
                continue
            val = _parse_number(str(cell or ""))
            if val is not None:
                if canonical not in ("eps_basic", "eps_diluted"):
                    val *= result.unit_scale
                if canonical not in target_dict:
                    target_dict[canonical] = val
                break


def _detect_notes_column(rows: list[list]) -> int:
    """Detect which column is the Notes column (small integers 1-99).
    Returns 1-based column index or -1 if none."""
    if not rows or len(rows) < 3:
        return -1

    col_scores = {}
    for row in rows[1:]:
        for i, cell in enumerate(row[1:], 1):
            txt = str(cell or "").strip()
            if not txt:
                continue
            if re.match(r'^\d{1,2}(\.\d)?$', txt):
                col_scores[i] = col_scores.get(i, 0) + 1

    if not col_scores:
        return -1

    best_col = max(col_scores, key=col_scores.get)
    if col_scores[best_col] >= len(rows) * 0.3:
        return best_col
    return -1


# ═══════════════════════════════════════════════════════
# STRATEGY 2: Line-by-line text parsing
# ═══════════════════════════════════════════════════════

def _strategy_text_lines(pdf_path: str, symbol: str) -> FinancialExtraction:
    """Extract financials by parsing text lines with fuzzy matching."""
    import pdfplumber

    result = FinancialExtraction(symbol=symbol, source_file=str(pdf_path),
                                 strategy_used="text_lines")

    try:
        with pdfplumber.open(pdf_path) as pdf:
            all_lines = []
            for page in pdf.pages:
                text = page.extract_text() or ""
                all_lines.extend(text.split('\n'))

        result.unit_scale = _detect_unit_scale(all_lines)
        result.period_end, result.period_type = _detect_period(all_lines)
        result.is_consolidated = _detect_consolidated(all_lines)

        current_section = None  # "IS", "BS", "CF"

        for line in all_lines:
            line = line.strip()
            if not line:
                continue

            lower = line.lower()
            if any(kw in lower for kw in [
                "profit and loss", "income statement", "statement of profit",
                "condensed interim profit", "profit or loss",
                "statement of comprehensive income",
            ]):
                current_section = "IS"
                continue
            elif any(kw in lower for kw in [
                "balance sheet", "statement of financial position",
                "condensed interim statement of financial position",
            ]):
                current_section = "BS"
                continue
            elif any(kw in lower for kw in [
                "cash flow", "statement of cash flows", "cash flows",
            ]):
                current_section = "CF"
                continue

            parts = re.split(r'\s{2,}|\t', line)
            if len(parts) < 2:
                continue

            label_text = parts[0].strip()
            value_candidates = parts[1:]

            if re.match(r'^(20\d{2}|note|rupee|rs|pkr)', label_text.lower()):
                continue

            # Determine target based on section context
            target_map = target_dict = None
            if current_section == "IS":
                target_map, target_dict = IS_SYNONYMS, result.income_statement
            elif current_section == "BS":
                target_map, target_dict = BS_SYNONYMS, result.balance_sheet
            elif current_section == "CF":
                target_map, target_dict = CF_SYNONYMS, result.cash_flow

            # If no section context, try all maps
            if target_map is None:
                for tmap, tdict in [
                    (IS_SYNONYMS, result.income_statement),
                    (BS_SYNONYMS, result.balance_sheet),
                    (CF_SYNONYMS, result.cash_flow),
                ]:
                    if _match_label(label_text, tmap):
                        target_map, target_dict = tmap, tdict
                        break

            if target_map is None:
                continue

            canonical = _match_label(label_text, target_map)
            if not canonical:
                continue

            for val_text in value_candidates:
                val_text = val_text.strip()
                if re.match(r'^\d{1,2}(\.\d)?$', val_text):
                    continue  # skip note references
                val = _parse_number(val_text)
                if val is not None:
                    if canonical not in ("eps_basic", "eps_diluted"):
                        val *= result.unit_scale
                    if canonical not in target_dict:
                        target_dict[canonical] = val
                    break

    except Exception as e:
        result.warnings.append(f"text_lines error: {e}")
        logger.warning("text_lines failed on %s: %s", pdf_path, e)

    result.score()
    return result


# ═══════════════════════════════════════════════════════
# STRATEGY 3: Regex fallback
# ═══════════════════════════════════════════════════════

def _strategy_regex(pdf_path: str, symbol: str) -> FinancialExtraction:
    """Last-resort extraction using broad regex patterns on full text."""
    import pdfplumber

    result = FinancialExtraction(symbol=symbol, source_file=str(pdf_path),
                                 strategy_used="regex")

    try:
        full_text = ""
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"

        result.unit_scale = _detect_unit_scale(full_text.split('\n'))
        result.period_end, result.period_type = _detect_period(full_text.split('\n'))

        patterns = {
            "revenue": r'(?:revenue|net\s+sales|turnover)[\s\S]{0,30}?([\d,]+(?:\.\d+)?)',
            "profit_after_tax": r'(?:profit\s+after\s+tax|net\s+(?:profit|income))[\s\S]{0,30}?([\d,]+(?:\.\d+)?)',
            "eps_basic": r'(?:earnings?\s+per\s+share|eps)[\s\S]{0,30}?([\d]+\.[\d]+)',
            "total_assets": r'(?:total\s+assets)[\s\S]{0,30}?([\d,]+(?:\.\d+)?)',
            "total_equity": r'(?:total\s+equity|shareholders[\'\s]+equity)[\s\S]{0,30}?([\d,]+(?:\.\d+)?)',
        }

        for canonical, pattern in patterns.items():
            m = re.search(pattern, full_text, re.IGNORECASE)
            if m:
                val = _parse_number(m.group(1))
                if val is not None:
                    if canonical not in ("eps_basic", "eps_diluted"):
                        val *= result.unit_scale
                    if canonical in IS_SYNONYMS:
                        result.income_statement[canonical] = val
                    elif canonical in BS_SYNONYMS:
                        result.balance_sheet[canonical] = val

    except Exception as e:
        result.warnings.append(f"regex error: {e}")

    result.score()
    return result


# ═══════════════════════════════════════════════════════
# METADATA DETECTION
# ═══════════════════════════════════════════════════════

def _detect_unit_scale(lines: list[str]) -> float:
    """Detect if amounts are in thousands, millions, or absolute."""
    for line in lines[:30]:
        lower = line.lower()
        if any(kw in lower for kw in ["in million", "in millions", "rs. in million"]):
            return 1_000_000
        if any(kw in lower for kw in [
            "in thousand", "in '000", "in 000", "in thousands",
            "(rupees in '000)", "rs in '000", "pkr '000",
        ]):
            return 1_000
    return 1.0


def _detect_period(lines: list[str]) -> tuple[str, str]:
    """Detect period end date and type."""
    period_end = ""
    period_type = ""

    for line in lines[:50]:
        lower = line.lower()

        if not period_type:
            if any(kw in lower for kw in ["for the year", "annual", "twelve months"]):
                period_type = "annual"
            elif any(kw in lower for kw in ["half year", "six months", "hyr"]):
                period_type = "half_year"
            elif any(kw in lower for kw in ["quarter ended", "three months", "iq", "iiq", "iiiq"]):
                period_type = "quarterly"

        if not period_end:
            m = re.search(
                r'(january|february|march|april|may|june|july|august|september|october|november|december)'
                r'\s+(\d{1,2}),?\s*(20\d{2})', lower,
            )
            if m:
                month_map = {"january":"01","february":"02","march":"03","april":"04",
                             "may":"05","june":"06","july":"07","august":"08",
                             "september":"09","october":"10","november":"11","december":"12"}
                period_end = f"{m.group(3)}-{month_map.get(m.group(1),'01')}-{m.group(2).zfill(2)}"

            if not period_end:
                m = re.search(r'(\d{2})/(\d{2})/(20\d{2})', line)
                if m:
                    period_end = f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

            if not period_end:
                m = re.search(r'(20\d{2})-(\d{2})-(\d{2})', line)
                if m:
                    period_end = m.group(0)

    if period_end and not period_type:
        month = int(period_end.split('-')[1])
        if month in (6, 12):
            period_type = "annual"
        elif month in (3, 9):
            period_type = "quarterly"

    return period_end, period_type


def _detect_consolidated(lines: list[str]) -> bool:
    for line in lines[:30]:
        lower = line.lower()
        if "unconsolidated" in lower or "standalone" in lower or "separate" in lower:
            return False
        if "consolidated" in lower:
            return True
    return True


# ═══════════════════════════════════════════════════════
# MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════

def parse_financial_pdf(
    pdf_path: str,
    symbol: str = "",
) -> FinancialExtraction:
    """Parse a financial statement PDF using multi-strategy approach.

    Tries three strategies, returns the one with highest confidence:
      1. pdfplumber table extraction
      2. Line-by-line text parsing with fuzzy label matching
      3. Regex-based extraction (fallback)
    """
    path = Path(pdf_path)
    if not path.exists():
        return FinancialExtraction(
            symbol=symbol, source_file=str(pdf_path),
            confidence=0, warnings=["File not found"],
        )

    if not symbol:
        symbol = path.parent.name if path.parent.name.isupper() else ""

    strategies = [
        ("pdfplumber", _strategy_pdfplumber),
        ("text_lines", _strategy_text_lines),
        ("regex", _strategy_regex),
    ]

    best: Optional[FinancialExtraction] = None

    for name, strategy_fn in strategies:
        try:
            result = strategy_fn(str(pdf_path), symbol)
            logger.info("Strategy %s → confidence=%.2f for %s", name, result.confidence, pdf_path)
            if best is None or result.confidence > best.confidence:
                best = result
            if result.confidence >= 0.60:
                break
        except Exception as e:
            logger.warning("Strategy %s crashed on %s: %s", name, pdf_path, e)

    if best is None:
        best = FinancialExtraction(
            symbol=symbol, source_file=str(pdf_path),
            confidence=0, warnings=["All strategies failed"],
        )

    if not best.income_statement.get("revenue"):
        best.warnings.append("Missing: revenue")
    if not best.income_statement.get("profit_after_tax"):
        best.warnings.append("Missing: profit_after_tax")
    if not best.income_statement.get("eps_basic"):
        best.warnings.append("Missing: eps_basic")
    if not best.balance_sheet.get("total_assets"):
        best.warnings.append("Missing: total_assets")
    if not best.balance_sheet.get("total_equity"):
        best.warnings.append("Missing: total_equity")
    if not best.cash_flow:
        best.warnings.append("Missing: cash_flow (entire section)")

    return best


def flatten_to_db_row(result: FinancialExtraction, existing_columns: list[str] = None) -> dict:
    """Flatten FinancialExtraction into a dict matching the EXISTING company_financials table.

    IMPORTANT: This function maps extracted data to the EXISTING table's column names.
    Run Step 0 audit to discover actual column names, then adapt the COLUMN_MAP below.

    The COLUMN_MAP is initialized with common PSX conventions. Claude Code MUST
    update it in Step 3 after reading the actual PRAGMA table_info output.
    """

    # ─── COLUMN MAP ───
    # Left = canonical name from parser, Right = ACTUAL DB column name
    # These are confirmed from PRAGMA table_info(company_financials) audit.
    # Claude Code: verify with Step 0 output and adjust if any mismatch.
    COLUMN_MAP = {
        # Metadata
        "symbol": "symbol",
        "period_end": "period_end",
        "period_type": "period_type",
        "is_consolidated": "is_consolidated",
        "confidence": "confidence",
        "strategy_used": "strategy_used",
        "source_file": "source_file",

        # Income Statement — mapped to ACTUAL column names
        "revenue": "sales",               # DB uses "sales" not "revenue"
        "cost_of_sales": "cost_of_sales",
        "gross_profit": "gross_profit",
        "admin_expenses": "admin_expenses",
        "distribution_cost": "distribution_cost",
        "other_income": "other_income",
        "other_charges": "other_charges",
        "operating_profit": "operating_profit",
        "finance_cost": "finance_cost",
        "finance_income": "finance_income",
        "profit_before_tax": "profit_before_tax",
        "taxation": "taxation",
        "profit_after_tax": "profit_after_tax",
        "eps_basic": "eps",                # DB uses "eps" not "eps_basic"
        "eps_diluted": "eps_diluted",
        "ebitda": "ebitda",
        "depreciation": "depreciation",

        # Balance Sheet — these match the DB as-is
        "total_assets": "total_assets",
        "non_current_assets": "non_current_assets",
        "current_assets": "current_assets",
        "total_equity": "total_equity",
        "share_capital": "share_capital",
        "reserves": "reserves",
        "total_liabilities": "total_liabilities",
        "non_current_liabilities": "non_current_liabilities",
        "current_liabilities": "current_liabilities",
        "long_term_debt": "long_term_debt",
        "short_term_debt": "short_term_debt",
        "trade_payables": "trade_payables",
        "trade_receivables": "trade_receivables",
        "cash_and_equivalents": "cash_and_equivalents",
        "inventories": "inventories",

        # Cash Flow — NO prefix on parser side, single cf_ prefix on DB side
        "operations": "cf_operations",
        "investing": "cf_investing",
        "financing": "cf_financing",
    }

    row = {}

    # Metadata
    row[COLUMN_MAP.get("symbol", "symbol")] = result.symbol
    row[COLUMN_MAP.get("period_end", "period_end")] = result.period_end
    row[COLUMN_MAP.get("period_type", "period_type")] = result.period_type
    row[COLUMN_MAP.get("is_consolidated", "is_consolidated")] = 1 if result.is_consolidated else 0
    row[COLUMN_MAP.get("confidence", "confidence")] = result.confidence
    row[COLUMN_MAP.get("strategy_used", "strategy_used")] = result.strategy_used
    row[COLUMN_MAP.get("source_file", "source_file")] = result.source_file

    # Income Statement
    for canonical, val in result.income_statement.items():
        col = COLUMN_MAP.get(canonical)
        if col:
            row[col] = val

    # Balance Sheet
    for canonical, val in result.balance_sheet.items():
        col = COLUMN_MAP.get(canonical)
        if col:
            row[col] = val

    # Cash Flow — keys are "operations", "investing", "financing" (no cf_ prefix)
    for canonical, val in result.cash_flow.items():
        col = COLUMN_MAP.get(canonical)
        if col:
            row[col] = val

    # Filter to only columns that exist in the table (if column list provided)
    if existing_columns:
        row = {k: v for k, v in row.items() if k in existing_columns}

    return row


# ─── BACKWARD COMPATIBILITY ───
# Alias for callers that import the old name from financial_parser.py
# After migration is complete, remove these.

def parse_ir_pdf(pdf_path: str, symbol: str = "") -> dict:
    """Backward-compatible wrapper for old financial_parser.parse_ir_pdf().

    Returns a dict in the OLD format. Use parse_financial_pdf() for new code.
    """
    result = parse_financial_pdf(pdf_path, symbol)
    return {
        "income_statement": result.income_statement,
        "balance_sheet": result.balance_sheet,
        "cash_flow": result.cash_flow,
        "period_end": result.period_end,
        "period_type": result.period_type,
        "is_consolidated": result.is_consolidated,
        "confidence": result.confidence,
        "warnings": result.warnings,
    }
```

## Step 3: Column Mapping Is Pre-configured

The COLUMN_MAP in `flatten_to_db_row()` is already set to match the existing
`company_financials` table based on the audit:
- Parser's `revenue` → DB's `sales`
- Parser's `eps_basic` → DB's `eps`
- All other columns match as-is

Claude Code: verify with Step 0 PRAGMA output. If any column name differs from
what's in COLUMN_MAP, fix it there. The `existing_columns` filter in
`flatten_to_db_row()` will silently skip any mapped column that doesn't exist
in the table, so mismatches won't crash — but data will be lost.

If columns like `confidence`, `strategy_used`, `source_file` don't exist yet,
they are added via ALTER TABLE in `scraper_service.py._run_parse()`.


## Step 4: Create the Scraper Service (Background Daemon)

Create `src/pakfindata/services/scraper_service.py`:

```python
"""
Financial Scraper Service — runs pipeline stages on schedule.

Same pattern as tick_service and fusion_service:
  - PID file for lifecycle management
  - JSON state file for Streamlit monitoring
  - Start/stop from Streamlit or CLI

Schedule (PKT):
  16:00 — DPS announcements scan + PDF download (smart stale tracking)
  16:30 — IR website PDF download (fin_downloader)
  17:00 — Parse unprocessed PDFs (universal parser)
  02:00 Sunday — Full: website_scan + deep_scrape(missing) + download + parse

Usage:
  python -m pakfindata.services.scraper_service                # foreground
  python -m pakfindata.services.scraper_service --daemon       # background
  python -m pakfindata.services.scraper_service --run-now parse # run a stage immediately
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger("scraper_service")
PKT = timezone(timedelta(hours=5))

try:
    from pakfindata.config import DATA_ROOT
except ImportError:
    DATA_ROOT = Path("/mnt/e/psxdata")

SCRAPER_STATE = DATA_ROOT / "scraper_state.json"
PID_FILE = DATA_ROOT / "services" / "scraper_service.pid"
LOG_FILE = DATA_ROOT / "services" / "scraper_service.log"
PDF_ROOT = Path("/mnt/e/psxsymbolfin")


class ScraperService:
    """Background service that runs scraper pipeline stages on schedule."""

    def __init__(self):
        self.running = False
        self.state = {
            "running": False,
            "last_scan": None,
            "last_download": None,
            "last_parse": None,
            "current_stage": None,
            "progress": {"done": 0, "total": 0, "symbol": ""},
            "stats": {
                "pdfs_total": 0,
                "pdfs_parsed": 0,
                "pdfs_failed": 0,
                "symbols_covered": 0,
                "last_error": "",
            },
            "log": [],
        }

    def _log(self, msg: str, level: str = "INFO"):
        now = datetime.now(PKT).strftime("%H:%M:%S")
        entry = {"time": now, "level": level, "msg": msg}
        self.state["log"].append(entry)
        if len(self.state["log"]) > 50:
            self.state["log"] = self.state["log"][-40:]
        logger.info(msg) if level == "INFO" else logger.warning(msg)
        self._write_state()

    def _write_state(self):
        self.state["running"] = self.running
        try:
            tmp = str(SCRAPER_STATE) + ".tmp"
            with open(tmp, "w") as f:
                json.dump(self.state, f, default=str)
            os.replace(tmp, str(SCRAPER_STATE))
        except OSError:
            pass

    def run_stage(self, stage: str):
        """Run a specific pipeline stage."""
        self.state["current_stage"] = stage
        self._log(f"Starting stage: {stage}")

        try:
            if stage == "scan":
                self._run_scan()
            elif stage == "download":
                self._run_download()
            elif stage == "parse":
                self._run_parse()
            elif stage == "full":
                self._run_full_weekly()
            else:
                self._log(f"Unknown stage: {stage}", "ERROR")
        except Exception as e:
            self._log(f"Stage {stage} failed: {e}", "ERROR")
            self.state["stats"]["last_error"] = str(e)
        finally:
            self.state["current_stage"] = None
            self._write_state()

    def _run_scan(self):
        """Scan DPS for new financial results PDFs.

        Uses the EXISTING sources/dps_announcements.py module with smart stale tracking.
        download_dps_financials_batch() already handles:
          - Checking announcements_sync_status for staleness
          - Filtering to FINANCIAL RESULTS announcements
          - Downloading attached PDFs to /mnt/e/psxsymbolfin/{SYMBOL}/
          - Skipping already-downloaded files
        """
        self._log("Scanning DPS for new financial results...")
        self.state["last_scan"] = datetime.now(PKT).isoformat()

        try:
            import sqlite3
            from pakfindata.sources.dps_announcements import download_dps_financials_batch

            con = sqlite3.connect(str(DATA_ROOT / "psx.sqlite"))
            result = download_dps_financials_batch(con, stale_days=1)
            con.close()

            # result may be a dict with stats, or just a count — adapt to actual return type
            if isinstance(result, dict):
                downloaded = result.get("downloaded", 0)
                skipped = result.get("skipped", 0)
                self._log(f"DPS scan: {downloaded} new PDFs downloaded, {skipped} skipped")
            else:
                self._log(f"DPS scan complete: {result}")

        except ImportError as e:
            self._log(f"dps_announcements module not available: {e}", "ERROR")
        except Exception as e:
            self._log(f"Scan error: {e}", "ERROR")

    def _run_download(self):
        """Download PDFs from IR websites (Step 3 of existing pipeline).

        Uses the EXISTING sources/fin_downloader.py module.
        DPS announcement PDFs are already downloaded by _run_scan via
        download_dps_financials_batch(). This stage handles IR website PDFs.
        """
        self._log("Downloading IR website PDFs...")
        self.state["last_download"] = datetime.now(PKT).isoformat()

        try:
            import sqlite3

            # Try to use existing fin_downloader
            try:
                from pakfindata.sources.fin_downloader import download_financials
                con = sqlite3.connect(str(DATA_ROOT / "psx.sqlite"))
                result = download_financials(con)
                con.close()
                self._log(f"IR download complete: {result}")
            except ImportError:
                self._log("fin_downloader not available — skipping IR downloads", "WARN")

        except Exception as e:
            self._log(f"Download error: {e}", "ERROR")

    def _run_parse(self):
        """Parse unprocessed PDFs into EXISTING company_financials table."""
        from pakfindata.engine.universal_fin_parser import parse_financial_pdf, flatten_to_db_row
        import sqlite3

        self._log("Parsing unprocessed PDFs...")
        self.state["last_parse"] = datetime.now(PKT).isoformat()

        all_pdfs = list(PDF_ROOT.rglob("*.pdf")) if PDF_ROOT.exists() else []
        self.state["stats"]["pdfs_total"] = len(all_pdfs)

        con = sqlite3.connect(str(DATA_ROOT / "psx.sqlite"))

        # Add tracking columns if they don't exist yet
        for col_name, col_type in [
            ("confidence", "REAL"),
            ("strategy_used", "TEXT"),
            ("source_file", "TEXT"),
        ]:
            try:
                con.execute(f"ALTER TABLE company_financials ADD COLUMN {col_name} {col_type}")
            except Exception:
                pass

        # Get existing table columns
        existing_columns = [
            row[1] for row in con.execute("PRAGMA table_info(company_financials)").fetchall()
        ]

        # Find already-parsed files
        parsed_files = set()
        if "source_file" in existing_columns:
            for row in con.execute(
                "SELECT source_file FROM company_financials WHERE source_file IS NOT NULL"
            ):
                parsed_files.add(row[0])

        unprocessed = [p for p in all_pdfs if str(p) not in parsed_files]
        self._log(f"Total: {len(all_pdfs)}, Parsed: {len(parsed_files)}, "
                  f"To process: {len(unprocessed)}")

        parsed, failed = 0, 0
        for i, pdf in enumerate(unprocessed):
            symbol = pdf.parent.name if pdf.parent.name.isupper() else ""
            self.state["progress"] = {
                "done": i, "total": len(unprocessed), "symbol": symbol,
            }
            self._write_state()

            try:
                result = parse_financial_pdf(str(pdf), symbol)

                if result.confidence >= 0.15:
                    row = flatten_to_db_row(result, existing_columns=existing_columns)
                    cols = list(row.keys())
                    placeholders = ", ".join(["?"] * len(cols))
                    col_names = ", ".join(cols)
                    con.execute(
                        f"INSERT OR REPLACE INTO company_financials ({col_names}) "
                        f"VALUES ({placeholders})",
                        [row.get(c) for c in cols],
                    )
                    con.commit()
                    parsed += 1
                else:
                    failed += 1
                    self._log(f"Low confidence ({result.confidence:.0%}): {pdf.name}", "WARN")

            except Exception as e:
                failed += 1
                self._log(f"Parse error {pdf.name}: {e}", "WARN")

        con.close()
        self.state["stats"]["pdfs_parsed"] = parsed
        self.state["stats"]["pdfs_failed"] = failed
        self.state["stats"]["symbols_covered"] = len(set(
            p.parent.name for p in all_pdfs if p.parent.name.isupper()
        ))
        self._log(f"Parse complete: {parsed} OK, {failed} failed")
        self.state["progress"] = {"done": 0, "total": 0, "symbol": ""}

    def run(self):
        """Main loop — check schedule every 60 seconds.

        Schedule (PKT):
          16:00 — DPS announcements scan + PDF download (smart stale tracking)
          16:30 — IR website PDF download (fin_downloader)
          17:00 — Parse unprocessed PDFs (universal parser)
          02:00 Sunday — Full: website_scan + deep_scrape(missing) + download + parse
        """
        self.running = True
        self._log("Scraper Service started")
        self._write_state()

        last_run = {}

        while self.running:
            try:
                now = datetime.now(PKT)
                hour = now.hour

                # 16:00 — DPS announcements (download_dps_financials_batch)
                if hour == 16 and now.minute < 30 and last_run.get("scan") != hour:
                    self.run_stage("scan")
                    last_run["scan"] = hour

                # 16:30 — IR website downloads (fin_downloader)
                if hour == 16 and now.minute >= 30 and last_run.get("download") != hour:
                    self.run_stage("download")
                    last_run["download"] = hour

                # 17:00 — Parse unprocessed PDFs
                if hour == 17 and last_run.get("parse") != hour:
                    self.run_stage("parse")
                    last_run["parse"] = hour

                # 02:00 Sunday — Full pipeline
                if hour == 2 and now.weekday() == 6 and last_run.get("full_scan") != now.date().isoformat():
                    self._run_full_weekly()
                    last_run["full_scan"] = now.date().isoformat()

            except Exception as e:
                self._log(f"Loop error: {e}", "ERROR")

            time.sleep(60)

        self._log("Scraper Service stopped")
        self.running = False
        self._write_state()

    def _run_full_weekly(self):
        """Full weekly pipeline: website scan + deep scrape (missing) + downloads + parse."""
        self._log("Starting weekly full pipeline...")

        # Step 1: Website scan (if available)
        try:
            import sqlite3
            from pakfindata.sources.website_scanner import run_website_scan
            con = sqlite3.connect(str(DATA_ROOT / "psx.sqlite"))
            run_website_scan(con)
            con.close()
            self._log("Website scan complete")
        except ImportError:
            self._log("website_scanner not available — skipping", "WARN")
        except Exception as e:
            self._log(f"Website scan error: {e}", "WARN")

        # Step 2: Deep scrape (missing symbols only, if available)
        try:
            import sqlite3
            from pakfindata.engine.psx_company_scraper import deep_scrape_batch
            con = sqlite3.connect(str(DATA_ROOT / "psx.sqlite"))
            deep_scrape_batch(con, missing_only=True)
            con.close()
            self._log("Deep scrape (missing) complete")
        except (ImportError, TypeError):
            self._log("deep_scrape_batch not available — skipping", "WARN")
        except Exception as e:
            self._log(f"Deep scrape error: {e}", "WARN")

        # Step 3-5: DPS scan + IR download + Parse
        self.run_stage("scan")
        self.run_stage("download")
        self.run_stage("parse")


# ═══════════════════════════════════════════════════════
# PID MANAGEMENT (same pattern as tick_service)
# ═══════════════════════════════════════════════════════

def _write_pid(pid: int):
    PID_FILE.parent.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(str(pid))

def _remove_pid():
    if PID_FILE.exists():
        PID_FILE.unlink()

def is_scraper_running() -> tuple[bool, int | None]:
    if PID_FILE.exists():
        try:
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, 0)
            return True, pid
        except (ValueError, ProcessLookupError, PermissionError):
            _remove_pid()
    return False, None

def stop_scraper_service() -> tuple[bool, str]:
    running, pid = is_scraper_running()
    if not running:
        return False, "Not running"
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(1)
        _remove_pid()
        return True, f"Stopped (PID {pid})"
    except Exception as e:
        return False, str(e)

def start_scraper_background() -> tuple[bool, str]:
    running, pid = is_scraper_running()
    if running:
        return False, f"Already running (PID {pid})"
    cmd = [sys.executable, "-m", "pakfindata.services.scraper_service", "--daemon"]
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                            start_new_session=True)
    time.sleep(1)
    if proc.poll() is None:
        _write_pid(proc.pid)
        return True, f"Started (PID {proc.pid})"
    return False, "Failed to start"


def main():
    parser = argparse.ArgumentParser(description="Financial Scraper Service")
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--run-now", choices=["scan", "download", "parse", "full"],
                        help="Run a stage immediately and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")

    if args.daemon:
        LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(filename=str(LOG_FILE), level=logging.INFO,
                            format="%(asctime)s %(message)s")

    _write_pid(os.getpid())

    svc = ScraperService()

    def _shutdown(signum, frame):
        svc.running = False
    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        if args.run_now:
            svc.run_stage(args.run_now)
        else:
            svc.run()
    finally:
        _remove_pid()


if __name__ == "__main__":
    main()
```

## Step 5: ADD New Pages to Existing fin_scraper_app.py

**DO NOT REPLACE the file.** The existing app uses RADIO-BASED tab persistence
(not `st.tabs()`), which survives Streamlit reruns. Add new options to the existing
radio selector.

### 5a. Add imports at the top

Add these imports near the existing imports:

```python
import json
import time as _time
from pathlib import Path

try:
    from streamlit_autorefresh import st_autorefresh
    HAS_AUTOREFRESH = True
except ImportError:
    HAS_AUTOREFRESH = False
```

### 5b. Find the radio-based page selector and ADD new options

The app uses `st.radio()` or `st.sidebar.radio()` for tab persistence.
Find the line that looks like:

```python
page = st.sidebar.radio("Page", ["Deep Scrape", "Website Scan", ...])
# or
page = st.radio("", ["Tab 0", "Tab 1", ...], horizontal=True)
```

ADD three new option names to the END of that list:

```python
# Add to the existing options list:
"📊 Dashboard", "🧪 Parser Test", "⚙️ Service"
```

### 5c. Add the new page routing AFTER existing if/elif blocks

Find the if/elif chain that routes to existing pages:

```python
if page == "Deep Scrape":
    ...
elif page == "Website Scan":
    ...
```

ADD at the END of that chain:

```python
elif page == "📊 Dashboard":
    _render_scraper_dashboard()
elif page == "🧪 Parser Test":
    _render_parser_test()
elif page == "⚙️ Service":
    _render_service_control()
```

### 5d. Add the new tab functions (add at bottom of file)

```python
# ═══════════════════════════════════════════════════════
# NEW TAB FUNCTIONS
# ═══════════════════════════════════════════════════════

try:
    from pakfindata.config import DATA_ROOT as _DATA_ROOT
except ImportError:
    _DATA_ROOT = Path("/mnt/e/psxdata")

_SCRAPER_STATE = _DATA_ROOT / "scraper_state.json"
_PDF_ROOT = Path("/mnt/e/psxsymbolfin")


def _load_scraper_state() -> dict:
    if _SCRAPER_STATE.exists():
        try:
            return json.loads(_SCRAPER_STATE.read_text())
        except (json.JSONDecodeError, IOError):
            pass
    return {}


def _render_scraper_dashboard():
    """Coverage stats, recent extractions, service log."""
    import sqlite3

    state = _load_scraper_state()

    # Auto-refresh if service running
    if state.get("running") and HAS_AUTOREFRESH:
        st_autorefresh(interval=5000, key="scraper_dash_refresh")

    # PDF coverage
    if _PDF_ROOT.exists():
        sym_dirs = [d for d in _PDF_ROOT.iterdir() if d.is_dir() and d.name.isupper()]
        total_pdfs = sum(len(list(d.glob("*.pdf"))) for d in sym_dirs)
    else:
        sym_dirs, total_pdfs = [], 0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Symbols with PDFs", len(sym_dirs))
    c2.metric("Total PDFs", total_pdfs)
    stats = state.get("stats", {})
    c3.metric("Parsed OK", stats.get("pdfs_parsed", 0))
    c4.metric("Parse Failed", stats.get("pdfs_failed", 0))

    # DB stats
    db_path = _DATA_ROOT / "psx.sqlite"
    if db_path.exists():
        con = sqlite3.connect(str(db_path))
        try:
            count = con.execute("SELECT COUNT(*) FROM company_financials").fetchone()[0]
            symbols = con.execute("SELECT COUNT(DISTINCT symbol) FROM company_financials").fetchone()[0]
            st.info(f"**Database:** {count:,} financial records across {symbols} symbols")

            # Recent extractions (if source_file column exists)
            cols = [r[1] for r in con.execute("PRAGMA table_info(company_financials)").fetchall()]
            if "confidence" in cols:
                import pandas as pd
                recent = pd.read_sql(
                    "SELECT symbol, period_end, period_type, confidence, strategy_used, "
                    "source_file FROM company_financials "
                    "WHERE confidence IS NOT NULL "
                    "ORDER BY ROWID DESC LIMIT 20",
                    con,
                )
                if not recent.empty:
                    st.markdown("**Recent Extractions (new parser)**")
                    st.dataframe(recent, use_container_width=True, hide_index=True)
        except Exception as e:
            st.warning(f"DB read error: {e}")
        finally:
            con.close()

    # Service log
    log = state.get("log", [])
    if log:
        st.markdown("**Service Log**")
        for entry in reversed(log[-15:]):
            icon = "🔴" if entry["level"] == "ERROR" else "⚠️" if entry["level"] == "WARN" else "✅"
            st.caption(f'{icon} {entry["time"]} — {entry["msg"]}')


def _render_parser_test():
    """Test the universal parser on a single PDF."""
    st.markdown("### Test Universal Parser")
    st.caption("Parse a single PDF and review extracted data before batch processing")

    # Symbol PDF selector
    if _PDF_ROOT.exists():
        sym_dirs = sorted([d.name for d in _PDF_ROOT.iterdir()
                          if d.is_dir() and d.name.isupper()])
    else:
        sym_dirs = []

    col1, col2 = st.columns([1, 3])
    with col1:
        sym = st.selectbox("Symbol", sym_dirs, key="parser_test_sym") if sym_dirs else None
    with col2:
        if sym:
            pdfs = sorted((_PDF_ROOT / sym).glob("*.pdf"))
            pdf_names = [p.name for p in pdfs]
            selected_pdf = st.selectbox("PDF File", pdf_names, key="parser_test_pdf") if pdf_names else None
        else:
            selected_pdf = None

    # Manual path input
    manual_path = st.text_input("Or enter PDF path directly",
                                placeholder="/mnt/e/psxsymbolfin/OGDC/2024-12-31.pdf",
                                key="parser_test_manual")

    test_path = None
    if manual_path:
        test_path = manual_path
    elif sym and selected_pdf:
        test_path = str(_PDF_ROOT / sym / selected_pdf)

    if test_path and st.button("🧪 Parse", type="primary", key="do_parse_test"):
        with st.spinner("Parsing..."):
            try:
                from pakfindata.engine.universal_fin_parser import parse_financial_pdf
                result = parse_financial_pdf(test_path)

                # Summary
                conf_color = "#22c55e" if result.confidence >= 0.5 else "#f59e0b" if result.confidence >= 0.3 else "#ef4444"
                st.markdown(
                    f'**Confidence:** <span style="color:{conf_color};font-weight:bold">'
                    f'{result.confidence:.0%}</span> &nbsp; '
                    f'**Strategy:** {result.strategy_used} &nbsp; '
                    f'**Period:** {result.period_end} ({result.period_type}) &nbsp; '
                    f'**{"Consolidated" if result.is_consolidated else "Standalone"}** &nbsp; '
                    f'**Scale:** {"×"+str(int(result.unit_scale)) if result.unit_scale > 1 else "absolute"}',
                    unsafe_allow_html=True,
                )

                if result.warnings:
                    st.warning("⚠️ " + " | ".join(result.warnings))

                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**Income Statement**")
                    for k, v in sorted(result.income_statement.items()):
                        fmt = f"{v:,.2f}" if isinstance(v, float) and abs(v) < 100 else f"{v:,.0f}"
                        st.caption(f"{k}: {fmt}")
                with c2:
                    st.markdown("**Balance Sheet**")
                    for k, v in sorted(result.balance_sheet.items()):
                        st.caption(f"{k}: {v:,.0f}" if isinstance(v, (int, float)) else f"{k}: {v}")
                with c3:
                    st.markdown("**Cash Flow**")
                    for k, v in sorted(result.cash_flow.items()):
                        st.caption(f"{k}: {v:,.0f}" if isinstance(v, (int, float)) else f"{k}: {v}")

                # Raw tables for debugging
                if result.raw_tables:
                    with st.expander(f"Raw Tables ({len(result.raw_tables)} tables found)"):
                        import pandas as pd
                        for i, tbl in enumerate(result.raw_tables):
                            st.caption(f"Table {i+1} (page {tbl.get('page', '?')})")
                            try:
                                df = pd.DataFrame(tbl["rows"])
                                st.dataframe(df, use_container_width=True, hide_index=True)
                            except Exception:
                                st.json(tbl)

            except Exception as e:
                st.error(f"Parse error: {e}")
                import traceback
                st.code(traceback.format_exc())

    # Batch parse button
    st.markdown("---")
    st.markdown("**Batch Operations**")
    bc1, bc2 = st.columns(2)
    with bc1:
        if st.button("📋 Parse All Unprocessed", key="batch_parse"):
            with st.spinner("Parsing unprocessed PDFs..."):
                try:
                    from pakfindata.services.scraper_service import ScraperService
                    svc = ScraperService()
                    svc._run_parse()
                    st.success("Batch parse complete!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Batch parse failed: {e}")
    with bc2:
        if st.button("🔍 Scan DPS + Download IR", key="scan_download"):
            with st.spinner("Scanning DPS and downloading IR PDFs..."):
                try:
                    from pakfindata.services.scraper_service import ScraperService
                    svc = ScraperService()
                    svc._run_scan()
                    svc._run_download()
                    st.success("Scan + download complete!")
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")


def _render_service_control():
    """Service start/stop and schedule display."""
    st.markdown("### Scraper Service Control")

    try:
        from pakfindata.services.scraper_service import (
            is_scraper_running, start_scraper_background, stop_scraper_service,
        )
    except ImportError:
        st.error("scraper_service module not available")
        return

    running, pid = is_scraper_running()

    if running:
        st.success(f"🟢 Service running (PID {pid})")
        if st.button("⏹ Stop Service", type="primary", key="svc_stop"):
            ok, msg = stop_scraper_service()
            st.success(msg) if ok else st.error(msg)
            st.rerun()
    else:
        st.warning("🔴 Service not running")
        if st.button("▶ Start Service", type="primary", key="svc_start"):
            ok, msg = start_scraper_background()
            st.success(msg) if ok else st.error(msg)
            st.rerun()

    # Manual triggers
    st.markdown("---")
    st.markdown("**Run Pipeline Stage Manually**")
    mc1, mc2, mc3, mc4 = st.columns(4)
    with mc1:
        if st.button("🔍 Scan", key="manual_scan"):
            with st.spinner("Scanning..."):
                from pakfindata.services.scraper_service import ScraperService
                ScraperService().run_stage("scan")
                st.rerun()
    with mc2:
        if st.button("📥 Download", key="manual_dl"):
            with st.spinner("Downloading..."):
                from pakfindata.services.scraper_service import ScraperService
                ScraperService().run_stage("download")
                st.rerun()
    with mc3:
        if st.button("📋 Parse", key="manual_parse"):
            with st.spinner("Parsing..."):
                from pakfindata.services.scraper_service import ScraperService
                ScraperService().run_stage("parse")
                st.rerun()
    with mc4:
        if st.button("🔄 Full Pipeline", key="manual_full"):
            with st.spinner("Running full pipeline..."):
                from pakfindata.services.scraper_service import ScraperService
                ScraperService().run_stage("full")
                st.rerun()

    # Review low-confidence
    st.markdown("---")
    st.markdown("**Low Confidence Extractions (need review)**")
    import sqlite3
    db_path = _DATA_ROOT / "psx.sqlite"
    if db_path.exists():
        con = sqlite3.connect(str(db_path))
        try:
            cols = [r[1] for r in con.execute("PRAGMA table_info(company_financials)").fetchall()]
            if "confidence" in cols:
                import pandas as pd
                low = pd.read_sql(
                    "SELECT symbol, period_end, confidence, strategy_used, source_file "
                    "FROM company_financials "
                    "WHERE confidence IS NOT NULL AND confidence < 0.4 "
                    "ORDER BY confidence ASC LIMIT 30",
                    con,
                )
                if not low.empty:
                    st.dataframe(low, use_container_width=True, hide_index=True)
                else:
                    st.caption("No low-confidence extractions")
            else:
                st.caption("Run parser to populate confidence scores")
        except Exception as e:
            st.warning(f"Error: {e}")
        finally:
            con.close()

    # Schedule
    st.markdown("---")
    st.markdown("**Automatic Schedule (PKT)**")
    st.caption("16:00 — DPS announcements scan + PDF download (smart stale tracking)")
    st.caption("16:30 — IR website PDF download (fin_downloader)")
    st.caption("17:00 — Parse unprocessed PDFs (universal parser)")
    st.caption("02:00 Sunday — Full: website scan + deep scrape (missing) + download + parse")
```

## Step 6: Update Callers of Old financial_parser.py

After Step 0, you found all files that import from `sources/financial_parser.py`.
For each caller, update the import:

```python
# BEFORE:
from pakfindata.sources.financial_parser import parse_ir_pdf

# AFTER (backward-compatible alias exists):
from pakfindata.engine.universal_fin_parser import parse_ir_pdf
```

**Do NOT delete `sources/financial_parser.py` yet.** It has two things the new parser lacks:
1. Bank-specific handling (different line items for bank financial statements)
2. `flatten_parsed_to_financials()` and `upsert_company_financials()` helpers

Keep it as `sources/financial_parser.py` (not renamed) until the new parser handles
banks AND has been tested on 50+ PDFs from diverse sectors. The existing PDF Import
tab (Tab 4) in `fin_scraper_app.py` may still call the old parser — that's fine,
both parsers write to the same `company_financials` table.

## Step 7: Add Makefile Targets

Add to Makefile:

```makefile
scraper:
	cd ~/pakfindata && conda run -n psx streamlit run src/pakfindata/fin_scraper_app.py --server.port 8502

scraper-service:
	cd ~/pakfindata && conda run -n psx python -m pakfindata.services.scraper_service

scraper-parse:
	cd ~/pakfindata && conda run -n psx python -m pakfindata.services.scraper_service --run-now parse

scraper-scan:
	cd ~/pakfindata && conda run -n psx python -m pakfindata.services.scraper_service --run-now scan
```

## Step 8: Test

```bash
cd ~/pakfindata && conda activate psx

# 1. Install pdfplumber
pip install pdfplumber --break-system-packages 2>/dev/null

# 2. Test universal parser on real PDFs
python3 -c "
import sys; sys.path.insert(0, 'src')
from pakfindata.engine.universal_fin_parser import parse_financial_pdf

from pathlib import Path
pdfs = list(Path('/mnt/e/psxsymbolfin').rglob('*.pdf'))[:10]
for pdf in pdfs:
    r = parse_financial_pdf(str(pdf))
    print(f'\n{pdf.parent.name}/{pdf.name}:')
    print(f'  Confidence: {r.confidence:.0%} ({r.strategy_used})')
    print(f'  Period: {r.period_end} ({r.period_type})')
    print(f'  Revenue: {r.income_statement.get(\"revenue\", \"MISSING\")}')
    print(f'  PAT: {r.income_statement.get(\"profit_after_tax\", \"MISSING\")}')
    print(f'  EPS: {r.income_statement.get(\"eps_basic\", \"MISSING\")}')
    print(f'  Total Assets: {r.balance_sheet.get(\"total_assets\", \"MISSING\")}')
    print(f'  CF Ops: {r.cash_flow.get(\"operations\", \"MISSING\")}')
    print(f'  Warnings: {r.warnings}')
"

# 3. Test flatten_to_db_row matches existing table + verify column mapping
python3 -c "
import sys, sqlite3; sys.path.insert(0, 'src')
from pakfindata.engine.universal_fin_parser import parse_financial_pdf, flatten_to_db_row
from pathlib import Path

con = sqlite3.connect('/mnt/e/psxdata/psx.sqlite')
existing_cols = [r[1] for r in con.execute('PRAGMA table_info(company_financials)').fetchall()]
print(f'Existing columns: {existing_cols}')
con.close()

pdf = next(Path('/mnt/e/psxsymbolfin').rglob('*.pdf'))
result = parse_financial_pdf(str(pdf))
row = flatten_to_db_row(result, existing_columns=existing_cols)
print(f'\nFlattened row keys: {list(row.keys())}')
print(f'All keys in table? {all(k in existing_cols for k in row.keys())}')
missing = [k for k in row.keys() if k not in existing_cols]
if missing:
    print(f'MISSING columns (need ALTER TABLE): {missing}')

# Verify critical mappings
print(f'\nColumn mapping check:')
print(f'  revenue → sales: {\"sales\" in row}')
print(f'  eps_basic → eps: {\"eps\" in row}')
print(f'  cf_operations (not cf_cf_operations): {\"cf_operations\" in row and \"cf_cf_operations\" not in row}')
"

# 4. Test scraper service parse stage
python -m pakfindata.services.scraper_service --run-now parse

# 5. Start Streamlit scraper app
streamlit run src/pakfindata/fin_scraper_app.py --server.port 8502
# Check all tabs: existing ones should be untouched, new ones should work

# 6. Start background service
python -m pakfindata.services.scraper_service --daemon
```

## CONFLICTS RESOLVED (v3 — updated from Claude Code audit)

| Conflict | v1 Problem | v3 Resolution |
|----------|-----------|---------------|
| App replacement drops existing tabs | Replaced fin_scraper_app.py entirely | **ADD** 3 new radio options to existing selector. All 6 existing tabs + sidebar preserved. |
| `company_financials_v2` data silo | New table with different column names | Write to EXISTING `company_financials` table. `revenue`→`sales`, `eps_basic`→`eps`. |
| `cf_cf_operations` double prefix | CF_SYNONYMS key `cf_operations` + `cf_` prefix = `cf_cf_operations` | CF_SYNONYMS keys are `operations`/`investing`/`financing`. COLUMN_MAP maps to `cf_operations`. Single prefix. |
| Duplicate parser, no migration | Both parsers exist, no path to switch | Explicit: universal_fin_parser replaces financial_parser. Backward-compat `parse_ir_pdf()` wrapper. Keep old file. |
| Wrong import in `_run_scan` | `psx_company_scraper.scrape_all_announcements` (doesn't exist) | Uses `dps_announcements.download_dps_financials_batch(con, stale_days=1)` with smart stale tracking |
| `_run_download` reimplements existing code | Custom download queue with requests.get() | Uses existing `fin_downloader.download_financials(con)` for IR website PDFs |
| st.tabs() resets on rerun | Prompt assumed st.tabs() | App uses radio-based persistence. New pages added as radio options. |
| Schedule uses wrong functions | scrape_all_announcements, custom queue | 16:00 `download_dps_financials_batch`, 16:30 `download_financials`, 17:00 parse, Sun 02:00 full |
| Missing: bank-aware parsing | Existing parser has bank/non-bank logic | Note added: existing bank handling in financial_parser.py should be consulted for bank-specific patterns |

## BANK-AWARE PARSING NOTE

The existing `sources/financial_parser.py` has special handling for bank financial statements
(different line items: interest income, provisions, deposits, etc.). The new universal parser
does NOT currently include bank-specific synonyms. When parsing banks (HBL, UBL, MCB, etc.),
the old parser may still produce better results.

**For now:** the backward-compatible `parse_ir_pdf()` wrapper lets the existing import pipeline
keep working. When a bank PDF gets low confidence from the universal parser, the service logs
it for manual review. A future enhancement can add bank-specific synonyms to IS_SYNONYMS
(e.g., `"net_interest_income": ["net interest income", "net markup income", ...]`).

## IMPORTANT NOTES

1. **Two files CREATED:** `engine/universal_fin_parser.py`, `services/scraper_service.py`
2. **One file MODIFIED:** `fin_scraper_app.py` — 3 new radio options added, nothing removed
3. **COLUMN_MAP is pre-configured:** `revenue`→`sales`, `eps_basic`→`eps`. Verify with Step 0.
4. **CF keys have NO double prefix** — synonym keys `operations`/`investing`/`financing`, mapped to `cf_operations` etc.
5. **`existing_columns` filter** in `flatten_to_db_row()` silently skips columns not in the table
6. **ALTER TABLE** adds `confidence`, `strategy_used`, `source_file` to track parser metadata
7. **Backward-compatible `parse_ir_pdf()`** wrapper lets old callers work without import changes
8. **Don't delete `sources/financial_parser.py` yet** — keep for bank PDFs and as fallback
9. **`_run_scan` uses `download_dps_financials_batch(con, stale_days=1)`** — existing smart stale tracking, NOT a rewritten scanner
10. **`_run_download` uses `download_financials(con)`** — existing fin_downloader for IR websites
11. **Radio-based navigation** — new pages are radio options, not st.tabs (matches existing app pattern)
12. **Port 8502** — separate from main app (8501)
13. **Bank PDFs** — may get low confidence. Review tab flags these. Future: add bank synonyms.
14. **SCD2 sidebar preserved** — the prompt doesn't touch the sidebar or definitions editor
