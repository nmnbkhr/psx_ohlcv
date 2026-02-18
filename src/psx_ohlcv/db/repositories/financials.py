"""Repository for comprehensive financial statement parsing and storage.

Handles:
- PDF parse tracking (pdf_parse_log table)
- Extended company_financials columns (P&L + Balance Sheet)
- Ratio computation from parsed financial data
- Schema migration for new columns
"""

import hashlib
import logging
import sqlite3

import pandas as pd

from ..repositories.company import (
    get_company_financials,
    upsert_company_financials,
    upsert_company_ratios,
)

logger = logging.getLogger("psx_ohlcv.financials")

__all__ = [
    "init_financials_schema",
    "upsert_pdf_parse_log",
    "get_parsed_pdfs",
    "get_unparsed_symbols",
    "get_parse_summary",
    "compute_ratios_from_financials",
    "pdf_hash",
]

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

_PDF_PARSE_LOG_SQL = """
CREATE TABLE IF NOT EXISTS pdf_parse_log (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol            TEXT NOT NULL,
    pdf_source        TEXT NOT NULL,
    pdf_path          TEXT,
    pdf_id            TEXT,
    pdf_hash          TEXT,
    file_size         INTEGER,
    parse_status      TEXT NOT NULL,
    items_extracted   INTEGER DEFAULT 0,
    period_end        TEXT,
    period_type       TEXT,
    is_bank           INTEGER DEFAULT 0,
    confidence        REAL,
    error_message     TEXT,
    parse_duration_ms INTEGER,
    parsed_at         TEXT NOT NULL DEFAULT (datetime('now')),

    UNIQUE(symbol, pdf_hash)
);

CREATE INDEX IF NOT EXISTS idx_pdf_parse_symbol
    ON pdf_parse_log(symbol);
CREATE INDEX IF NOT EXISTS idx_pdf_parse_status
    ON pdf_parse_log(parse_status);
CREATE INDEX IF NOT EXISTS idx_pdf_parse_source
    ON pdf_parse_log(pdf_source);
"""

# New columns for company_financials (P&L + Balance Sheet + metadata)
_FINANCIALS_NEW_COLS = [
    # P&L (non-bank)
    ("cost_of_sales", "REAL"),
    ("operating_expenses", "REAL"),
    ("finance_cost", "REAL"),
    ("other_income", "REAL"),
    ("taxation", "REAL"),
    # P&L (bank-specific)
    ("net_interest_income", "REAL"),
    ("non_markup_income", "REAL"),
    ("total_income", "REAL"),
    ("provisions", "REAL"),
    # Balance Sheet
    ("current_assets", "REAL"),
    ("non_current_assets", "REAL"),
    ("current_liabilities", "REAL"),
    ("non_current_liabilities", "REAL"),
    ("cash_and_equivalents", "REAL"),
    ("share_capital", "REAL"),
    # Metadata
    ("source", "TEXT"),
    ("currency_scale", "TEXT"),
    ("parsed_at", "TEXT"),
]

# New columns for company_ratios
_RATIOS_NEW_COLS = [
    ("debt_to_equity", "REAL"),
    ("current_ratio", "REAL"),
    ("interest_coverage", "REAL"),
    ("asset_turnover", "REAL"),
    ("equity_multiplier", "REAL"),
]


def init_financials_schema(con: sqlite3.Connection) -> None:
    """Create pdf_parse_log table and migrate company_financials/ratios columns."""
    con.executescript(_PDF_PARSE_LOG_SQL)
    con.commit()

    # Migrate company_financials
    cursor = con.execute("PRAGMA table_info(company_financials)")
    existing = {row[1] for row in cursor.fetchall()}
    for col_name, col_type in _FINANCIALS_NEW_COLS:
        if col_name not in existing:
            con.execute(
                f"ALTER TABLE company_financials ADD COLUMN {col_name} {col_type}"
            )
    con.commit()

    # Migrate company_ratios
    cursor = con.execute("PRAGMA table_info(company_ratios)")
    existing = {row[1] for row in cursor.fetchall()}
    for col_name, col_type in _RATIOS_NEW_COLS:
        if col_name not in existing:
            con.execute(
                f"ALTER TABLE company_ratios ADD COLUMN {col_name} {col_type}"
            )
    con.commit()


# ---------------------------------------------------------------------------
# PDF Parse Log
# ---------------------------------------------------------------------------


def pdf_hash(content: bytes) -> str:
    """SHA-256 hash of PDF content for dedup."""
    return hashlib.sha256(content).hexdigest()


def upsert_pdf_parse_log(con: sqlite3.Connection, entry: dict) -> bool:
    """Record a PDF parse attempt. Returns True if inserted/updated."""
    try:
        con.execute(
            """
            INSERT INTO pdf_parse_log (
                symbol, pdf_source, pdf_path, pdf_id, pdf_hash,
                file_size, parse_status, items_extracted,
                period_end, period_type, is_bank,
                confidence, error_message, parse_duration_ms, parsed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(symbol, pdf_hash) DO UPDATE SET
                parse_status = excluded.parse_status,
                items_extracted = excluded.items_extracted,
                period_end = excluded.period_end,
                period_type = excluded.period_type,
                confidence = excluded.confidence,
                error_message = excluded.error_message,
                parse_duration_ms = excluded.parse_duration_ms,
                parsed_at = datetime('now')
            """,
            (
                entry.get("symbol", ""),
                entry.get("pdf_source", "unknown"),
                entry.get("pdf_path"),
                entry.get("pdf_id"),
                entry.get("pdf_hash", ""),
                entry.get("file_size"),
                entry.get("parse_status", "failed"),
                entry.get("items_extracted", 0),
                entry.get("period_end"),
                entry.get("period_type"),
                1 if entry.get("is_bank") else 0,
                entry.get("confidence"),
                entry.get("error_message"),
                entry.get("parse_duration_ms"),
            ),
        )
        con.commit()
        return True
    except sqlite3.Error as e:
        logger.warning("Failed to log PDF parse: %s", e)
        return False


def get_parsed_pdfs(
    con: sqlite3.Connection,
    symbol: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """Query parse history."""
    query = "SELECT * FROM pdf_parse_log WHERE 1=1"
    params: list = []
    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    if source:
        query += " AND pdf_source = ?"
        params.append(source)
    query += " ORDER BY parsed_at DESC"
    return pd.read_sql_query(query, con, params=params)


def is_pdf_parsed(con: sqlite3.Connection, symbol: str, hash_val: str) -> bool:
    """Check if a PDF has already been successfully parsed."""
    row = con.execute(
        "SELECT 1 FROM pdf_parse_log WHERE symbol = ? AND pdf_hash = ? AND parse_status IN ('success', 'partial')",
        (symbol.upper(), hash_val),
    ).fetchone()
    return row is not None


def is_psx_pdf_parsed(con: sqlite3.Connection, symbol: str, pdf_id: str) -> bool:
    """Check if a PSX portal PDF has already been parsed (by pdf_id)."""
    row = con.execute(
        "SELECT 1 FROM pdf_parse_log WHERE symbol = ? AND pdf_id = ? AND parse_status IN ('success', 'partial')",
        (symbol.upper(), pdf_id),
    ).fetchone()
    return row is not None


def get_unparsed_symbols(
    con: sqlite3.Connection,
    source: str = "ir_pdf",
) -> list[str]:
    """Find symbols that have no successful parse log entries."""
    all_symbols = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT symbol FROM symbols ORDER BY symbol"
        ).fetchall()
    ]
    parsed = {
        row[0]
        for row in con.execute(
            "SELECT DISTINCT symbol FROM pdf_parse_log WHERE pdf_source = ? AND parse_status IN ('success', 'partial')",
            (source,),
        ).fetchall()
    }
    return [s for s in all_symbols if s not in parsed]


def get_parse_summary(con: sqlite3.Connection) -> dict:
    """Summary stats: total parsed, success rate, avg confidence, coverage."""
    row = con.execute(
        """
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN parse_status = 'success' THEN 1 ELSE 0 END) as success,
            SUM(CASE WHEN parse_status = 'partial' THEN 1 ELSE 0 END) as partial,
            SUM(CASE WHEN parse_status = 'failed' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN parse_status = 'skipped' THEN 1 ELSE 0 END) as skipped,
            AVG(CASE WHEN parse_status = 'success' THEN confidence END) as avg_confidence,
            COUNT(DISTINCT symbol) as symbols_parsed
        FROM pdf_parse_log
        """
    ).fetchone()

    return {
        "total": row[0] or 0,
        "success": row[1] or 0,
        "partial": row[2] or 0,
        "failed": row[3] or 0,
        "skipped": row[4] or 0,
        "avg_confidence": round(row[5], 2) if row[5] else 0.0,
        "symbols_parsed": row[6] or 0,
    }


# ---------------------------------------------------------------------------
# Ratio Computation
# ---------------------------------------------------------------------------


def compute_ratios_from_financials(
    con: sqlite3.Connection,
    symbol: str,
) -> int:
    """Compute all derived ratios from company_financials and upsert to company_ratios.

    All ratios rounded to 2 decimal places. Handles division by zero gracefully.

    Returns count of ratio rows upserted.
    """
    symbol = symbol.upper()
    financials = get_company_financials(con, symbol, limit=50)

    if financials.empty:
        return 0

    # Clear existing PDF-computed ratios so stale/bad values don't persist
    # via COALESCE. DPS-sourced ratios (pe_ratio, pb_ratio) are preserved
    # because we always pass them as None.
    con.execute(
        """UPDATE company_ratios SET
            gross_profit_margin = NULL,
            net_profit_margin = NULL,
            operating_margin = NULL,
            return_on_equity = NULL,
            return_on_assets = NULL,
            debt_to_equity = NULL,
            current_ratio = NULL,
            interest_coverage = NULL,
            asset_turnover = NULL,
            equity_multiplier = NULL,
            sales_growth = NULL,
            eps_growth = NULL,
            profit_growth = NULL
        WHERE symbol = ?""",
        (symbol,),
    )

    ratios_list = []

    for _, row in financials.iterrows():
        ratios: dict = {
            "period_end": row["period_end"],
            "period_type": row["period_type"],
        }

        # Determine top-line revenue (bank vs non-bank aware)
        sales = _safe_float(row.get("sales"))
        total_income = _safe_float(row.get("total_income"))
        me = _safe_float(row.get("markup_earned"))
        mex = _safe_float(row.get("markup_expensed"))
        nii = _safe_float(row.get("net_interest_income"))

        # Bank: use total_income or markup_earned as revenue
        # Non-bank: use sales
        is_bank_row = me is not None and me > 0
        if is_bank_row:
            revenue = total_income or me
        else:
            revenue = sales

        # Sanity check: if revenue and PAT differ by >1000x, likely scale mismatch
        pat = _safe_float(row.get("profit_after_tax"))
        if revenue and pat and revenue > 0 and pat > 0:
            ratio_check = pat / revenue
            if ratio_check > 100:
                # PAT >> revenue → revenue is probably unscaled DPS data
                revenue = None

        # --- Profitability ---

        # Bank gross margin: net_interest_income / markup_earned
        if me and me > 0:
            net_ii = nii or (me - (mex or 0))
            ratios["gross_profit_margin"] = round(net_ii / me * 100, 2)
        elif revenue and revenue > 0:
            gp = _safe_float(row.get("gross_profit"))
            if gp is not None:
                # Sanity: GPM should be between -200% and 200%
                gpm = gp / revenue * 100
                if -200 <= gpm <= 200:
                    ratios["gross_profit_margin"] = round(gpm, 2)

        if revenue and revenue > 0:
            if pat is not None:
                npm = pat / revenue * 100
                if -200 <= npm <= 200:
                    ratios["net_profit_margin"] = round(npm, 2)

            op = _safe_float(row.get("operating_profit"))
            if op is not None:
                opm = op / revenue * 100
                if -200 <= opm <= 200:
                    ratios["operating_margin"] = round(opm, 2)

        # --- Balance Sheet Ratios ---
        total_equity = _safe_float(row.get("total_equity"))
        total_assets = _safe_float(row.get("total_assets"))
        total_liabilities = _safe_float(row.get("total_liabilities"))
        current_assets = _safe_float(row.get("current_assets"))
        current_liabilities = _safe_float(row.get("current_liabilities"))
        finance_cost = _safe_float(row.get("finance_cost"))
        op = _safe_float(row.get("operating_profit"))

        if total_equity and total_equity > 0:
            if pat is not None:
                ratios["return_on_equity"] = round(pat / total_equity * 100, 2)
            if total_liabilities is not None:
                ratios["debt_to_equity"] = round(total_liabilities / total_equity, 2)

        if total_assets and total_assets > 0:
            if pat is not None:
                ratios["return_on_assets"] = round(pat / total_assets * 100, 2)
            if revenue:
                ratios["asset_turnover"] = round(revenue / total_assets, 2)
            if total_equity and total_equity > 0:
                ratios["equity_multiplier"] = round(total_assets / total_equity, 2)

        if current_assets and current_liabilities and current_liabilities > 0:
            ratios["current_ratio"] = round(current_assets / current_liabilities, 2)

        if op and finance_cost and finance_cost > 0:
            ratios["interest_coverage"] = round(op / finance_cost, 2)

        # Only upsert if we computed at least one ratio
        ratio_keys = {
            k
            for k in ratios
            if k not in ("period_end", "period_type") and ratios[k] is not None
        }
        if ratio_keys:
            ratios_list.append(ratios)

    # --- Growth metrics (YoY) ---
    _add_growth_metrics(financials, ratios_list)

    if ratios_list:
        return upsert_company_ratios(con, symbol, ratios_list)
    return 0


def _safe_float(val) -> float | None:
    """Convert value to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        import math

        f = float(val)
        return None if math.isnan(f) else f
    except (ValueError, TypeError):
        return None


def _add_growth_metrics(financials: pd.DataFrame, ratios_list: list[dict]) -> None:
    """Add YoY growth metrics (sales_growth, eps_growth, profit_growth)."""
    import re

    # Build lookup: (period_end, period_type) -> row
    lookup: dict[tuple[str, str], dict] = {}
    for _, row in financials.iterrows():
        key = (row["period_end"], row["period_type"])
        lookup[key] = row.to_dict()

    for ratios in ratios_list:
        pe = ratios["period_end"]
        pt = ratios["period_type"]

        # Find prior year key
        year_match = re.search(r"(\d{4})", pe)
        if not year_match:
            continue
        year = int(year_match.group(1))
        prior_pe = pe.replace(str(year), str(year - 1))
        prior = lookup.get((prior_pe, pt))
        if not prior:
            continue

        curr = lookup.get((pe, pt))
        if not curr:
            continue

        # Sales growth
        curr_sales = _safe_float(curr.get("sales"))
        prior_sales = _safe_float(prior.get("sales"))
        if curr_sales and prior_sales and prior_sales > 0:
            ratios["sales_growth"] = round(
                (curr_sales - prior_sales) / abs(prior_sales) * 100, 2
            )

        # EPS growth
        curr_eps = _safe_float(curr.get("eps"))
        prior_eps = _safe_float(prior.get("eps"))
        if curr_eps is not None and prior_eps and prior_eps != 0:
            ratios["eps_growth"] = round(
                (curr_eps - prior_eps) / abs(prior_eps) * 100, 2
            )

        # Profit growth
        curr_pat = _safe_float(curr.get("profit_after_tax"))
        prior_pat = _safe_float(prior.get("profit_after_tax"))
        if curr_pat is not None and prior_pat and prior_pat != 0:
            ratios["profit_growth"] = round(
                (curr_pat - prior_pat) / abs(prior_pat) * 100, 2
            )
