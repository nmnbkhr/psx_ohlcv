"""Standalone PDF Financial Parser Service.

Scans /mnt/e/psxsymbolfin/{SYMBOL}/*.pdf, extracts all financial line items
with original names, and writes per-symbol CSV files.

Outputs per symbol:
  {SYMBOL}_financials.csv  — catalog-matched KPIs with dates (clean, deduplicated)
  {SYMBOL}_raw_items.csv   — every extracted line item with original name
  {SYMBOL}_multi_year.csv  — multi-year performance summary (if found)
  {SYMBOL}_format_issues.csv — PDFs that couldn't be parsed

Usage:
    python -m pakfindata.services.pdf_parser_service
    python -m pakfindata.services.pdf_parser_service --symbol OGDC
    python -m pakfindata.services.pdf_parser_service --workers 4
    python -m pakfindata.services.pdf_parser_service --ocr-only
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

logger = logging.getLogger("pakfindata.pdf_parser_service")

PDF_ROOT = Path("/mnt/e/psxsymbolfin")

# KPI codes that should NOT be scaled (per-share, ratios, percentages)
_NO_SCALE_KPIS = {
    "eps_basic", "eps_diluted", "eps_basic_diluted",
    "dps", "book_value_per_share", "nav_per_unit", "face_value",
    "pe_ratio", "pb_ratio", "dividend_yield", "dividend_payout",
    "current_ratio", "quick_ratio", "debt_equity",
    "gross_profit_margin", "operating_profit_margin", "net_profit_margin",
    "ebitda_margin", "return_on_equity", "return_on_assets",
    "total_shareholder_return",
    "shares_issued", "shares_outstanding", "weighted_avg_shares",
}

# Ordered columns for financials CSV (readable layout)
_FINANCIALS_COLUMNS = [
    # Meta
    "period_end", "period_type", "is_audited", "file", "scale", "is_ocr", "format_family",
    # P&L — Revenue
    "revenue", "gross_turnover", "sales_tax", "cost_of_sales", "gross_profit",
    # P&L — Operating
    "admin_expenses", "selling_expenses", "distribution_cost", "other_expenses",
    "operating_profit", "other_income", "finance_cost",
    # P&L — Bottom line
    "levy", "pbt", "taxation", "pat", "total_comprehensive",
    # P&L — Per share
    "eps_basic", "eps_diluted",
    # P&L — Bank
    "markup_earned", "markup_expensed", "net_markup", "non_markup_income",
    "provisions", "total_income",
    # P&L — Insurance
    "premium_income", "net_insurance_premium", "claims_expense", "net_insurance_claims",
    "underwriting_result", "investment_income",
    # BS — Totals
    "total_assets", "total_liabilities", "total_equity",
    # BS — Assets
    "ppe", "intangibles", "non_current_assets", "current_assets",
    "inventory", "stores_spares", "trade_receivables", "cash",
    # BS — Liabilities
    "non_current_liabilities", "current_liabilities",
    "borrowings_lt", "borrowings_st", "trade_payables", "accrued_markup",
    # BS — Equity
    "share_capital", "reserves", "revaluation_surplus",
    # Other
    "deferred_tax", "lease_liabilities",
]

# Lines that are just header noise — skip from raw items
_JUNK_LINE_PATTERNS = re.compile(
    r"^(FOR\s+THE\s+|AS\s+AT\s+|Note\s+|---+|===+|Rupees\s+in|Rs\s+in|\(Rupees|\(Rs)"
    r"|^(January|February|March|April|May|June|July|August|September|October|November|December)\s*$"
    r"|^(Un.?[Aa]udited|Audited|Restated|Condensed|Consolidated)\s*$"
    r"|^\d{4}\s*$",
    re.I,
)

# Shared state for worker processes
_shared_sector_map: dict[str, str] = {}


def _init_worker(sector_map: dict[str, str], fy_map: dict[str, str]):
    global _shared_sector_map, _shared_fy_map
    _shared_sector_map = sector_map
    _shared_fy_map = fy_map


def _fmt_number(val) -> str:
    """Format number for CSV readability."""
    if val is None or val == "":
        return ""
    try:
        v = float(val)
        if v == 0:
            return "0"
        if abs(v) >= 1_000_000:
            return f"{v:,.0f}"
        if abs(v) >= 1:
            return f"{v:,.2f}"
        return f"{v:.4f}"
    except (ValueError, TypeError):
        return str(val)


def _is_junk_line(text: str) -> bool:
    """Check if a line item is just header/date noise, not a real KPI."""
    text = text.strip()
    if len(text) < 4:
        return True
    if _JUNK_LINE_PATTERNS.search(text):
        return True
    # Lines that are just dates or single words
    if re.match(r"^\d{1,2}[,.]?\s*\d{4}$", text):
        return True
    return False


def _parse_one_pdf(args: tuple) -> dict:
    """Worker: parse a single PDF. Returns dict with results."""
    pdf_path, symbol = args
    from pakfindata.sources.raw_financial_extractor import extract_pdf

    sector_code = _shared_sector_map.get(symbol)

    result = {
        "file": pdf_path.name,
        "symbol": symbol,
        "parse_ok": False,
        "error": None,
        "extracted": None,
        "format_issue": None,
    }

    try:
        pdf_bytes = pdf_path.read_bytes()
        extracted = extract_pdf(pdf_bytes, symbol=symbol, sector_code=sector_code, filename=pdf_path.name)

        pl_items = [i for i in extracted["statements"]["pl"]
                    if not i.get("is_subtotal") and not _is_junk_line(i.get("line", ""))]
        bs_items = [i for i in extracted["statements"]["bs"]
                    if not i.get("is_subtotal") and not _is_junk_line(i.get("line", ""))]
        has_data = len(pl_items) > 0 or len(bs_items) > 0

        if extracted["is_scanned"] and not has_data:
            result["format_issue"] = "SCANNED_NO_OCR"
        elif not has_data:
            result["format_issue"] = "NO_FINANCIAL_PAGES"
        elif not pl_items:
            result["format_issue"] = "NO_PL_FOUND"
        elif not bs_items:
            result["format_issue"] = "NO_BS_FOUND"

        result["extracted"] = extracted
        result["parse_ok"] = has_data

    except Exception as e:
        result["error"] = str(e)
        result["format_issue"] = "PARSE_ERROR"

    return result


def _load_sector_map() -> dict[str, str]:
    """Load symbol → sector_code mapping from DB."""
    try:
        from pakfindata.config import get_db_path
        con = sqlite3.connect(str(get_db_path()))
        rows = con.execute("SELECT symbol, sector FROM symbols WHERE sector IS NOT NULL").fetchall()
        con.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


_shared_fy_map: dict[str, str] = {}


def _load_fy_map() -> dict[str, str]:
    """Load symbol → fiscal_year_end month mapping from DB."""
    try:
        from pakfindata.config import get_db_path
        con = sqlite3.connect(str(get_db_path()))
        rows = con.execute(
            "SELECT symbol, fiscal_year_end FROM company_profile "
            "WHERE fiscal_year_end IS NOT NULL AND fiscal_year_end != ''"
        ).fetchall()
        con.close()
        return {r[0]: r[1] for r in rows}
    except Exception:
        return {}


def process_symbol(symbol: str, workers: int = 1, ocr_only: bool = False) -> dict:
    """Parse all PDFs for a symbol and write CSV outputs."""
    symbol_dir = PDF_ROOT / symbol
    if not symbol_dir.is_dir():
        return {"symbol": symbol, "error": f"Directory not found: {symbol_dir}"}

    pdf_files = sorted(symbol_dir.glob("*.pdf"))
    if not pdf_files:
        return {"symbol": symbol, "error": "No PDF files found"}

    # If ocr_only, filter to only previously-scanned PDFs
    if ocr_only:
        issues_csv = symbol_dir / f"{symbol}_format_issues.csv"
        if issues_csv.exists():
            scanned_files = set()
            with open(issues_csv) as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row.get("issue") in ("SCANNED_NO_OCR", "NO_FINANCIAL_PAGES"):
                        scanned_files.add(row["file"])
            pdf_files = [p for p in pdf_files if p.name in scanned_files]
        if not pdf_files:
            return {"symbol": symbol, "total_pdfs": 0, "parsed_ok": 0,
                    "format_issues": 0, "financials_rows": 0, "raw_items": 0, "multi_year": 0}

    # Parse all PDFs
    work_items = [(pdf, symbol) for pdf in pdf_files]
    results = []

    if workers > 1 and len(work_items) > 1:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_init_worker,
            initargs=(_shared_sector_map, _shared_fy_map),
        ) as executor:
            futures = {executor.submit(_parse_one_pdf, item): item[0] for item in work_items}
            for fut in as_completed(futures):
                results.append(fut.result())
    else:
        for item in work_items:
            results.append(_parse_one_pdf(item))

    # ── Build CSV outputs ──
    financials_rows = []
    raw_items_rows = []
    multi_year_rows = []
    format_issues = []

    for r in results:
        if r["error"]:
            format_issues.append({"file": r["file"], "issue": "PARSE_ERROR", "detail": r["error"]})
            continue

        if r["format_issue"]:
            format_issues.append({"file": r["file"], "issue": r["format_issue"], "detail": ""})

        ext = r.get("extracted")
        if not ext:
            continue

        period = ext["period_info"]
        scale = ext["scale_multiplier"]

        # Infer period_type from fiscal_year_end if missing
        fy_month = _shared_fy_map.get(symbol, "").upper()[:3]
        period_end = period.get("period_end", "")
        if period_end and fy_month:
            _MONTH_NUMS = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
                           "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
                           "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"}
            fy_mm = _MONTH_NUMS.get(fy_month, "")
            pe_mm = period_end[5:7] if len(period_end) >= 7 else ""
            if fy_mm and pe_mm:
                if pe_mm == fy_mm:
                    if not period.get("period_type"):
                        period["period_type"] = "annual"
                elif not period.get("period_type"):
                    # Calculate months from fiscal year start
                    fy_start = (int(fy_mm) % 12) + 1
                    pe_m = int(pe_mm)
                    months = (pe_m - fy_start) % 12 + 1
                    if months <= 4:
                        period["period_type"] = "quarterly"
                    elif months <= 7:
                        period["period_type"] = "half_year"
                    elif months <= 10:
                        period["period_type"] = "nine_months"
                    else:
                        period["period_type"] = "annual"

        # ── Financials CSV (catalog-matched KPIs) ──
        fin_row = {
            "period_end": period.get("period_end", ""),
            "period_type": period.get("period_type", ""),
            "is_audited": "Yes" if period.get("is_audited") is True else "No" if period.get("is_audited") is False else "",
            "file": r["file"],
            "scale": ext["scale_label"],
            "is_ocr": "Yes" if ext.get("is_ocr") else "",
            "format_family": ext.get("format_family", ""),
        }

        for stmt_type in ("pl", "bs"):
            for item in ext["statements"].get(stmt_type, []):
                kpi = item.get("kpi_code")
                # Capture labeled subtotals (e.g., [total_income] for investment companies)
                if item.get("is_subtotal") and item.get("line") == "[total_income]":
                    if "total_income" not in fin_row and item["values"]:
                        multiplier = 1 if "total_income" in _NO_SCALE_KPIS else scale
                        fin_row["total_income"] = item["values"][0] * multiplier
                    continue
                if kpi and not item.get("is_subtotal") and not _is_junk_line(item.get("line", "")):
                    multiplier = 1 if kpi in _NO_SCALE_KPIS else scale
                    val = item["values"][0] * multiplier if item["values"] else None
                    if kpi not in fin_row:
                        fin_row[kpi] = val

        if sum(1 for k in fin_row if k not in {"period_end", "period_type", "is_audited", "file", "scale", "is_ocr", "format_family"} and fin_row[k]) > 0:
            financials_rows.append(fin_row)

        # ── Raw Items CSV (every line item as-is) ──
        for stmt_type in ("pl", "bs"):
            for item in ext["statements"].get(stmt_type, []):
                if item.get("is_subtotal"):
                    continue
                line_text = item.get("line", "")
                if _is_junk_line(line_text):
                    continue
                kpi = item.get("kpi_code", "")
                multiplier = 1 if kpi in _NO_SCALE_KPIS else scale
                raw_items_rows.append({
                    "period_end": period.get("period_end", ""),
                    "statement": stmt_type.upper(),
                    "section": item.get("section", ""),
                    "line_item": line_text,
                    "kpi_match": kpi,
                    "current_period": _fmt_number(item["values"][0] * multiplier) if item["values"] else "",
                    "prior_period": _fmt_number(item["values"][1] * multiplier) if len(item["values"]) > 1 else "",
                    "col_3": _fmt_number(item["values"][2] * multiplier) if len(item["values"]) > 2 else "",
                    "col_4": _fmt_number(item["values"][3] * multiplier) if len(item["values"]) > 3 else "",
                    "file": r["file"],
                })

        # ── Multi-Year CSV ──
        my = ext.get("multi_year")
        if my and my.get("items"):
            for item in my["items"]:
                line_text = item.get("line", "")
                if _is_junk_line(line_text):
                    continue
                row = {
                    "kpi": item.get("line", ""),
                    "kpi_match": item.get("kpi_code", ""),
                    "section": item.get("section", ""),
                    "unit": item.get("unit", ""),
                }
                for j, year in enumerate(my["years"]):
                    val = item["values"][j] if j < len(item["values"]) else ""
                    row[year] = _fmt_number(val) if val != "" else ""
                row["file"] = r["file"]
                multi_year_rows.append(row)

    # ── Cross-validate scale: fix "units" outliers when majority is "thousands" ──
    if financials_rows:
        scale_counts: dict[str, int] = {}
        for row in financials_rows:
            s = row.get("scale", "units")
            scale_counts[s] = scale_counts.get(s, 0) + 1
        dominant_scale = max(scale_counts, key=scale_counts.get) if scale_counts else "units"

        if dominant_scale == "thousands" and scale_counts.get("units", 0) > 0:
            for row in financials_rows:
                if row.get("scale") == "units":
                    # Re-scale numeric KPI values × 1000
                    meta_keys_s = {"file", "period_end", "period_type", "scale", "is_audited", "is_ocr", "format_family"}
                    for k in list(row.keys()):
                        if k in meta_keys_s or k in _NO_SCALE_KPIS:
                            continue
                        try:
                            val = float(str(row[k]).replace(",", ""))
                            if val != 0 and abs(val) < 1_000_000_000:
                                row[k] = _fmt_number(val * 1000)
                        except (ValueError, TypeError):
                            pass
                    row["scale"] = "thousands (corrected)"

    # ── Deduplicate financials ──
    if financials_rows:
        meta_keys = {"file", "period_end", "period_type", "scale", "is_audited", "is_ocr", "format_family"}
        seen: dict[str, tuple[int, dict]] = {}
        for row in financials_rows:
            key = row.get("period_end", "")
            if not key:
                continue
            kpi_count = sum(1 for k, v in row.items() if k not in meta_keys and v)
            is_annual = 1 if row.get("period_type") == "annual" else 0
            is_audited = 1 if row.get("is_audited") == "Yes" else 0
            is_native = 1 if row.get("is_ocr") != "Yes" else 0
            score = is_annual * 1000 + is_audited * 500 + is_native * 100 + kpi_count
            existing = seen.get(key)
            if not existing or score > existing[0]:
                seen[key] = (score, row)
        financials_rows = sorted([v[1] for v in seen.values()], key=lambda x: x.get("period_end", "") or "")

        # Format numbers in financials
        for row in financials_rows:
            for k in list(row.keys()):
                if k not in meta_keys and isinstance(row[k], (int, float)):
                    row[k] = _fmt_number(row[k])

    # ── Write CSVs ──
    if financials_rows:
        _write_csv(symbol_dir / f"{symbol}_financials.csv", financials_rows, _FINANCIALS_COLUMNS)

    if raw_items_rows:
        _write_csv(symbol_dir / f"{symbol}_raw_items.csv", raw_items_rows)

    if multi_year_rows:
        _write_csv(symbol_dir / f"{symbol}_multi_year.csv", multi_year_rows)

    if format_issues:
        _write_csv(symbol_dir / f"{symbol}_format_issues.csv", format_issues)

    return {
        "symbol": symbol,
        "total_pdfs": len(pdf_files),
        "parsed_ok": sum(1 for r in results if r["parse_ok"]),
        "format_issues": len(format_issues),
        "financials_rows": len(financials_rows),
        "raw_items": len(raw_items_rows),
        "multi_year": len(multi_year_rows),
    }


def _write_csv(path: Path, rows: list[dict], ordered_cols: list[str] | None = None):
    """Write rows to CSV with clean formatting.

    Args:
        path: Output file path
        rows: List of row dicts
        ordered_cols: Optional preferred column order (extra cols appended)
    """
    if not rows:
        return

    if ordered_cols:
        # Start with ordered cols that exist in data, then append extras
        all_keys = set()
        for row in rows:
            all_keys.update(row.keys())
        cols = [c for c in ordered_cols if c in all_keys]
        for row in rows:
            for k in row:
                if k not in cols:
                    cols.append(k)
    else:
        cols = []
        for row in rows:
            for k in row:
                if k not in cols:
                    cols.append(k)

    for attempt in range(3):
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(rows)
            return
        except PermissionError:
            if attempt < 2:
                import time as _t
                _t.sleep(1)
            else:
                logger.warning("Permission denied writing %s — skipping", path)


def run(symbols: list[str] | None = None, workers: int = 4, ocr_only: bool = False):
    """Run parser across all symbols (or specific ones)."""
    global _shared_sector_map, _shared_fy_map
    _shared_sector_map = _load_sector_map()
    _shared_fy_map = _load_fy_map()

    if symbols:
        symbol_dirs = [PDF_ROOT / s for s in symbols]
    else:
        symbol_dirs = [d for d in PDF_ROOT.iterdir() if d.is_dir() and not d.name.startswith(".")]

    symbol_dirs = [d for d in symbol_dirs if d.is_dir()]
    symbol_dirs.sort(key=lambda d: d.name)

    if not symbol_dirs:
        print("No symbol directories found.")
        return

    t0 = time.time()
    total_pdfs = 0
    total_ok = 0
    total_issues = 0
    total_raw = 0
    total_my = 0

    mode = "OCR-only" if ocr_only else "Full"
    print(f"[{mode}] Processing {len(symbol_dirs)} symbols (workers={workers})...")
    print()

    for sym_dir in symbol_dirs:
        symbol = sym_dir.name
        summary = process_symbol(symbol, workers=workers, ocr_only=ocr_only)

        if "error" in summary and summary.get("total_pdfs") is None:
            print(f"  {symbol}: {summary['error']}")
            continue

        total_pdfs += summary["total_pdfs"]
        total_ok += summary["parsed_ok"]
        total_issues += summary["format_issues"]
        total_raw += summary["raw_items"]
        total_my += summary["multi_year"]

        ok = summary["parsed_ok"]
        total = summary["total_pdfs"]
        issues = summary["format_issues"]
        my_str = f" MY:{summary['multi_year']}" if summary["multi_year"] > 0 else ""
        status = "✓" if issues == 0 else "⚠" if ok > 0 else "✗"
        print(
            f"  {status} {symbol:8s} — {total:3d} PDFs, {ok:3d} parsed, "
            f"{summary['financials_rows']:3d} KPI rows, {summary['raw_items']:4d} raw{my_str}"
            + (f" ({issues} issues)" if issues > 0 else "")
        )

    elapsed = time.time() - t0
    print(f"\nDone in {elapsed:.1f}s")
    print(f"  Total PDFs:    {total_pdfs}")
    print(f"  Parsed OK:     {total_ok}")
    print(f"  Format issues: {total_issues}")
    print(f"  Raw items:     {total_raw}")
    print(f"  Multi-year:    {total_my}")


_FY_MONTH_MAP = {
    "JANUARY": "01", "FEBRUARY": "02", "MARCH": "03", "APRIL": "04",
    "MAY": "05", "JUNE": "06", "JULY": "07", "AUGUST": "08",
    "SEPTEMBER": "09", "OCTOBER": "10", "NOVEMBER": "11", "DECEMBER": "12",
}


def _classify_pdf_priority(filename: str, fy_month: str) -> str:
    """Classify a PDF as 'annual' or 'interim' based on filename date + fiscal year.

    Annual reports are filed 2-5 months after fiscal year-end.
    E.g., FY June company files annual in Aug-Nov → filename like Oct-xx-2024.
    """
    from pakfindata.sources.raw_financial_extractor import _date_from_filename
    file_date = _date_from_filename(filename)
    if not file_date or not fy_month:
        return "unknown"

    file_mm = int(file_date[5:7])
    fy_mm_str = _FY_MONTH_MAP.get(fy_month.upper()[:3] if len(fy_month) > 3 else fy_month.upper(), "")
    if not fy_mm_str:
        fy_mm_str = _FY_MONTH_MAP.get(fy_month.upper(), "")
    if not fy_mm_str:
        return "unknown"
    fy_mm = int(fy_mm_str)

    # Annual reports filed 2-5 months after FY end
    months_after = (file_mm - fy_mm) % 12
    if 1 <= months_after <= 5:
        return "annual"
    return "interim"


def run_pipeline(symbols: list[str] | None = None, workers: int = 8, ocr_only: bool = False):
    """Two-pipeline run: annuals first, then interims.

    Pipeline 1: Process annual report PDFs (most valuable, audited)
    Pipeline 2: Process interim PDFs after P1 completes each symbol
    """
    global _shared_sector_map, _shared_fy_map
    _shared_sector_map = _load_sector_map()
    _shared_fy_map = _load_fy_map()

    if symbols:
        symbol_dirs = [PDF_ROOT / s for s in symbols]
    else:
        symbol_dirs = [d for d in PDF_ROOT.iterdir() if d.is_dir() and not d.name.startswith(".")]

    symbol_dirs = [d for d in symbol_dirs if d.is_dir()]
    symbol_dirs.sort(key=lambda d: d.name)

    if not symbol_dirs:
        print("No symbol directories found.")
        return

    t0 = time.time()

    # ── Pipeline 1: Annual reports ──
    print(f"═══ Pipeline 1: ANNUAL reports ({len(symbol_dirs)} symbols, {workers} workers) ═══")
    print()
    p1_stats = {"pdfs": 0, "ok": 0, "issues": 0, "raw": 0, "my": 0}

    for sym_dir in symbol_dirs:
        symbol = sym_dir.name
        fy_month = _shared_fy_map.get(symbol, "")
        pdf_files = sorted(sym_dir.glob("*.pdf"))

        # Filter to annual PDFs only
        annual_pdfs = [p for p in pdf_files if _classify_pdf_priority(p.name, fy_month) == "annual"]
        if not annual_pdfs:
            # If can't classify, take most recent 3 PDFs (likely includes annual)
            annual_pdfs = pdf_files[-3:] if len(pdf_files) > 3 else pdf_files

        summary = _process_pdf_list(symbol, annual_pdfs, workers, ocr_only)
        _print_summary(symbol, summary, p1_stats)

    p1_elapsed = time.time() - t0
    print(f"\n── Pipeline 1 done in {p1_elapsed:.0f}s ──")
    _print_totals(p1_stats)

    # ── Pipeline 2: Interim reports ──
    print(f"\n═══ Pipeline 2: INTERIM reports ({len(symbol_dirs)} symbols, {workers} workers) ═══")
    print()
    p2_stats = {"pdfs": 0, "ok": 0, "issues": 0, "raw": 0, "my": 0}

    for sym_dir in symbol_dirs:
        symbol = sym_dir.name
        fy_month = _shared_fy_map.get(symbol, "")
        pdf_files = sorted(sym_dir.glob("*.pdf"))

        # Filter to non-annual PDFs
        annual_set = {p.name for p in pdf_files if _classify_pdf_priority(p.name, fy_month) == "annual"}
        interim_pdfs = [p for p in pdf_files if p.name not in annual_set]
        if not interim_pdfs:
            continue

        summary = _process_pdf_list(symbol, interim_pdfs, workers, ocr_only, append=True)
        _print_summary(symbol, summary, p2_stats)

    total_elapsed = time.time() - t0
    print(f"\n── Pipeline 2 done ──")
    _print_totals(p2_stats)
    print(f"\n═══ Total: {total_elapsed:.0f}s ═══")


def _process_pdf_list(
    symbol: str, pdf_files: list[Path], workers: int,
    ocr_only: bool = False, append: bool = False,
) -> dict:
    """Process a specific list of PDFs for a symbol."""
    if not pdf_files:
        return {"symbol": symbol, "total_pdfs": 0, "parsed_ok": 0,
                "format_issues": 0, "financials_rows": 0, "raw_items": 0, "multi_year": 0}

    if ocr_only:
        symbol_dir = PDF_ROOT / symbol
        issues_csv = symbol_dir / f"{symbol}_format_issues.csv"
        if issues_csv.exists():
            scanned = set()
            with open(issues_csv) as f:
                for row in csv.DictReader(f):
                    if row.get("issue") in ("SCANNED_NO_OCR", "NO_FINANCIAL_PAGES"):
                        scanned.add(row["file"])
            pdf_files = [p for p in pdf_files if p.name in scanned]

    if not pdf_files:
        return {"symbol": symbol, "total_pdfs": 0, "parsed_ok": 0,
                "format_issues": 0, "financials_rows": 0, "raw_items": 0, "multi_year": 0}

    work_items = [(pdf, symbol) for pdf in pdf_files]
    results = []

    if workers > 1 and len(work_items) > 1:
        with ProcessPoolExecutor(
            max_workers=workers,
            initializer=_init_worker,
            initargs=(_shared_sector_map, _shared_fy_map),
        ) as executor:
            futures = {executor.submit(_parse_one_pdf, item): item[0] for item in work_items}
            for fut in as_completed(futures):
                results.append(fut.result())
    else:
        for item in work_items:
            results.append(_parse_one_pdf(item))

    # Use process_symbol's post-processing but pass results directly
    return _build_csvs(symbol, results, append=append)


def _build_csvs(symbol: str, results: list[dict], append: bool = False) -> dict:
    """Build CSV outputs from parsed results."""
    symbol_dir = PDF_ROOT / symbol
    financials_rows = []
    raw_items_rows = []
    multi_year_rows = []
    format_issues = []

    for r in results:
        if r["error"]:
            format_issues.append({"file": r["file"], "issue": "PARSE_ERROR", "detail": r["error"]})
            continue
        if r["format_issue"]:
            format_issues.append({"file": r["file"], "issue": r["format_issue"], "detail": ""})

        ext = r.get("extracted")
        if not ext:
            continue

        period = ext["period_info"]
        scale = ext["scale_multiplier"]

        # Infer period_type from fiscal_year_end
        fy_month = _shared_fy_map.get(symbol, "").upper()[:3]
        period_end = period.get("period_end", "")
        if period_end and fy_month:
            _MN = {"JAN": "01", "FEB": "02", "MAR": "03", "APR": "04", "MAY": "05", "JUN": "06",
                    "JUL": "07", "AUG": "08", "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12"}
            fy_mm = _MN.get(fy_month, "")
            pe_mm = period_end[5:7] if len(period_end) >= 7 else ""
            if fy_mm and pe_mm:
                if pe_mm == fy_mm and not period.get("period_type"):
                    period["period_type"] = "annual"
                elif not period.get("period_type"):
                    fy_start = (int(fy_mm) % 12) + 1
                    months = (int(pe_mm) - fy_start) % 12 + 1
                    if months <= 4: period["period_type"] = "quarterly"
                    elif months <= 7: period["period_type"] = "half_year"
                    elif months <= 10: period["period_type"] = "nine_months"
                    else: period["period_type"] = "annual"

        fin_row = {
            "period_end": period.get("period_end", ""),
            "period_type": period.get("period_type", ""),
            "is_audited": "Yes" if period.get("is_audited") is True else "No" if period.get("is_audited") is False else "",
            "file": r["file"],
            "scale": ext["scale_label"],
            "is_ocr": "Yes" if ext.get("is_ocr") else "",
            "format_family": ext.get("format_family", ""),
        }

        for stmt_type in ("pl", "bs"):
            for item in ext["statements"].get(stmt_type, []):
                kpi = item.get("kpi_code")
                if kpi and not item.get("is_subtotal") and not _is_junk_line(item.get("line", "")):
                    multiplier = 1 if kpi in _NO_SCALE_KPIS else scale
                    val = item["values"][0] * multiplier if item["values"] else None
                    if kpi not in fin_row:
                        fin_row[kpi] = val

        if sum(1 for k in fin_row if k not in {"period_end", "period_type", "is_audited", "file", "scale", "is_ocr", "format_family"} and fin_row[k]) > 0:
            financials_rows.append(fin_row)

        for stmt_type in ("pl", "bs"):
            for item in ext["statements"].get(stmt_type, []):
                if item.get("is_subtotal") or _is_junk_line(item.get("line", "")):
                    continue
                kpi = item.get("kpi_code", "")
                multiplier = 1 if kpi in _NO_SCALE_KPIS else scale
                raw_items_rows.append({
                    "period_end": period.get("period_end", ""),
                    "statement": stmt_type.upper(),
                    "section": item.get("section", ""),
                    "line_item": item["line"],
                    "kpi_match": kpi,
                    "current_period": _fmt_number(item["values"][0] * multiplier) if item["values"] else "",
                    "prior_period": _fmt_number(item["values"][1] * multiplier) if len(item["values"]) > 1 else "",
                    "col_3": _fmt_number(item["values"][2] * multiplier) if len(item["values"]) > 2 else "",
                    "col_4": _fmt_number(item["values"][3] * multiplier) if len(item["values"]) > 3 else "",
                    "file": r["file"],
                })

        my = ext.get("multi_year")
        if my and my.get("items"):
            for item in my["items"]:
                if _is_junk_line(item.get("line", "")):
                    continue
                row = {"kpi": item.get("line", ""), "kpi_match": item.get("kpi_code", ""),
                       "section": item.get("section", ""), "unit": item.get("unit", "")}
                for j, year in enumerate(my["years"]):
                    val = item["values"][j] if j < len(item["values"]) else ""
                    row[year] = _fmt_number(val) if val != "" else ""
                row["file"] = r["file"]
                multi_year_rows.append(row)

    # Cross-validate scale
    if financials_rows:
        scale_counts: dict[str, int] = {}
        for row in financials_rows:
            s = row.get("scale", "units")
            scale_counts[s] = scale_counts.get(s, 0) + 1
        dominant = max(scale_counts, key=scale_counts.get) if scale_counts else "units"
        if dominant == "thousands" and scale_counts.get("units", 0) > 0:
            meta_s = {"file", "period_end", "period_type", "scale", "is_audited", "is_ocr", "format_family"}
            for row in financials_rows:
                if row.get("scale") == "units":
                    for k in list(row.keys()):
                        if k in meta_s or k in _NO_SCALE_KPIS: continue
                        try:
                            val = float(str(row[k]).replace(",", ""))
                            if val != 0 and abs(val) < 1_000_000_000:
                                row[k] = _fmt_number(val * 1000)
                        except (ValueError, TypeError): pass
                    row["scale"] = "thousands (corrected)"

    # Deduplicate
    if financials_rows:
        meta_keys = {"file", "period_end", "period_type", "scale", "is_audited", "is_ocr", "format_family"}
        seen: dict[str, tuple[int, dict]] = {}
        for row in financials_rows:
            key = row.get("period_end", "")
            if not key: continue
            kpi_count = sum(1 for k, v in row.items() if k not in meta_keys and v)
            is_annual = 1 if row.get("period_type") == "annual" else 0
            is_audited = 1 if row.get("is_audited") == "Yes" else 0
            is_native = 1 if row.get("is_ocr") != "Yes" else 0
            score = is_annual * 1000 + is_audited * 500 + is_native * 100 + kpi_count
            existing = seen.get(key)
            if not existing or score > existing[0]:
                seen[key] = (score, row)
        financials_rows = sorted([v[1] for v in seen.values()], key=lambda x: x.get("period_end", "") or "")
        for row in financials_rows:
            for k in list(row.keys()):
                if k not in meta_keys and isinstance(row[k], (int, float)):
                    row[k] = _fmt_number(row[k])

    # Write (append mode merges with existing CSVs)
    mode = "a" if append else "w"
    if financials_rows:
        out_path = symbol_dir / f"{symbol}_financials.csv"
        if append and out_path.exists():
            existing = []
            with open(out_path) as f:
                existing = list(csv.DictReader(f))
            financials_rows = existing + financials_rows
            # Re-deduplicate after merge
            meta_keys = {"file", "period_end", "period_type", "scale", "is_audited", "is_ocr", "format_family"}
            seen2: dict[str, tuple[int, dict]] = {}
            for row in financials_rows:
                key = row.get("period_end", "")
                if not key: continue
                kpi_count = sum(1 for k, v in row.items() if k not in meta_keys and v)
                is_annual = 1 if row.get("period_type") == "annual" else 0
                score = is_annual * 1000 + kpi_count
                existing2 = seen2.get(key)
                if not existing2 or score > existing2[0]:
                    seen2[key] = (score, row)
            financials_rows = sorted([v[1] for v in seen2.values()], key=lambda x: x.get("period_end", "") or "")
        _write_csv(out_path, financials_rows, _FINANCIALS_COLUMNS)

    if raw_items_rows:
        out_path = symbol_dir / f"{symbol}_raw_items.csv"
        if append and out_path.exists():
            existing = []
            with open(out_path) as f:
                existing = list(csv.DictReader(f))
            raw_items_rows = existing + raw_items_rows
        _write_csv(out_path, raw_items_rows)

    if multi_year_rows:
        _write_csv(symbol_dir / f"{symbol}_multi_year.csv", multi_year_rows)
    if format_issues:
        out_path = symbol_dir / f"{symbol}_format_issues.csv"
        if append and out_path.exists():
            existing = []
            with open(out_path) as f:
                existing = list(csv.DictReader(f))
            format_issues = existing + format_issues
        _write_csv(out_path, format_issues)

    return {
        "symbol": symbol,
        "total_pdfs": len(results),
        "parsed_ok": sum(1 for r in results if r["parse_ok"]),
        "format_issues": len(format_issues),
        "financials_rows": len(financials_rows),
        "raw_items": len(raw_items_rows),
        "multi_year": len(multi_year_rows),
    }


def _print_summary(symbol: str, summary: dict, stats: dict):
    """Print one-line summary and accumulate stats."""
    if "error" in summary and summary.get("total_pdfs") is None:
        print(f"  {symbol}: {summary['error']}")
        return
    stats["pdfs"] += summary["total_pdfs"]
    stats["ok"] += summary["parsed_ok"]
    stats["issues"] += summary["format_issues"]
    stats["raw"] += summary["raw_items"]
    stats["my"] += summary["multi_year"]
    ok = summary["parsed_ok"]
    total = summary["total_pdfs"]
    issues = summary["format_issues"]
    my_str = f" MY:{summary['multi_year']}" if summary["multi_year"] > 0 else ""
    status = "✓" if issues == 0 else "⚠" if ok > 0 else "✗"
    print(
        f"  {status} {symbol:8s} — {total:3d} PDFs, {ok:3d} parsed, "
        f"{summary['financials_rows']:3d} KPI rows, {summary['raw_items']:4d} raw{my_str}"
        + (f" ({issues} issues)" if issues > 0 else "")
    )


def _print_totals(stats: dict):
    """Print pipeline totals."""
    print(f"  PDFs: {stats['pdfs']}  Parsed: {stats['ok']}  Issues: {stats['issues']}  Raw: {stats['raw']}  MY: {stats['my']}")


def main():
    parser = argparse.ArgumentParser(description="Parse PSX financial PDFs to CSV")
    parser.add_argument("--symbol", "-s", nargs="+", help="Specific symbols to process")
    parser.add_argument("--workers", "-w", type=int, default=8, help="CPU workers (default: 8)")
    parser.add_argument("--ocr-only", action="store_true",
        help="Only re-process PDFs previously flagged as scanned")
    parser.add_argument("--pipeline", action="store_true",
        help="Two-pipeline mode: annuals first, then interims")
    args = parser.parse_args()

    if args.pipeline:
        run_pipeline(symbols=args.symbol, workers=args.workers, ocr_only=args.ocr_only)
    else:
        run(symbols=args.symbol, workers=args.workers, ocr_only=args.ocr_only)


if __name__ == "__main__":
    main()
