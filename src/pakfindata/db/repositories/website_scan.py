"""Company website financial statement scan repository."""

import json
import sqlite3

import pandas as pd

__all__ = [
    "init_website_scan_schema",
    "upsert_website_scan",
    "get_website_scans",
    "get_scan_summary",
]

WEBSITE_SCAN_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS company_website_scan (
    symbol              TEXT PRIMARY KEY,
    dps_website_url     TEXT,
    website_reachable   INTEGER DEFAULT 0,
    http_status         INTEGER,
    has_financial_page  INTEGER DEFAULT 0,
    financial_urls      TEXT,
    financial_keywords  TEXT,
    error_message       TEXT,
    scan_duration_ms    INTEGER,
    checked_at          TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_ws_reachable
    ON company_website_scan(website_reachable);
CREATE INDEX IF NOT EXISTS idx_ws_has_financial
    ON company_website_scan(has_financial_page);
"""


def init_website_scan_schema(con: sqlite3.Connection) -> None:
    """Create company_website_scan table if it doesn't exist."""
    con.executescript(WEBSITE_SCAN_SCHEMA_SQL)
    con.commit()


def upsert_website_scan(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a website scan result.

    Args:
        con: Database connection.
        data: Dict with keys: symbol (required), plus optional:
              dps_website_url, website_reachable, http_status,
              has_financial_page, financial_urls (list), financial_keywords (list),
              error_message, scan_duration_ms.

    Returns:
        True if the row was written.
    """
    fin_urls = data.get("financial_urls")
    if isinstance(fin_urls, list):
        fin_urls = json.dumps(fin_urls)

    fin_kw = data.get("financial_keywords")
    if isinstance(fin_kw, list):
        fin_kw = json.dumps(fin_kw)

    try:
        con.execute(
            """INSERT INTO company_website_scan (
                   symbol, dps_website_url, website_reachable, http_status,
                   has_financial_page, financial_urls, financial_keywords,
                   error_message, scan_duration_ms, checked_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(symbol) DO UPDATE SET
                 dps_website_url=excluded.dps_website_url,
                 website_reachable=excluded.website_reachable,
                 http_status=excluded.http_status,
                 has_financial_page=excluded.has_financial_page,
                 financial_urls=excluded.financial_urls,
                 financial_keywords=excluded.financial_keywords,
                 error_message=excluded.error_message,
                 scan_duration_ms=excluded.scan_duration_ms,
                 checked_at=datetime('now')
            """,
            (
                data["symbol"],
                data.get("dps_website_url"),
                int(data.get("website_reachable", 0)),
                data.get("http_status"),
                int(data.get("has_financial_page", 0)),
                fin_urls,
                fin_kw,
                data.get("error_message"),
                data.get("scan_duration_ms"),
            ),
        )
        con.commit()
        return True
    except sqlite3.Error:
        return False


def get_website_scans(
    con: sqlite3.Connection,
    has_financial: bool | None = None,
    reachable: bool | None = None,
) -> pd.DataFrame:
    """Get scan results with optional filters.

    Args:
        con: Database connection.
        has_financial: Filter by has_financial_page (True/False/None=all).
        reachable: Filter by website_reachable (True/False/None=all).

    Returns:
        DataFrame with scan results.
    """
    query = "SELECT * FROM company_website_scan WHERE 1=1"
    params: list = []

    if has_financial is not None:
        query += " AND has_financial_page = ?"
        params.append(int(has_financial))
    if reachable is not None:
        query += " AND website_reachable = ?"
        params.append(int(reachable))

    query += " ORDER BY symbol"
    return pd.read_sql_query(query, con, params=params)


def get_scan_summary(con: sqlite3.Connection) -> dict:
    """Get summary statistics for website scans.

    Returns:
        Dict with total, have_website, reachable, has_financial, errors counts.
    """
    row = con.execute(
        """SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN dps_website_url IS NOT NULL AND dps_website_url != '' THEN 1 ELSE 0 END) AS have_website,
            SUM(website_reachable) AS reachable,
            SUM(has_financial_page) AS has_financial,
            SUM(CASE WHEN error_message IS NOT NULL AND error_message != '' THEN 1 ELSE 0 END) AS errors
           FROM company_website_scan"""
    ).fetchone()
    if row is None:
        return {"total": 0, "have_website": 0, "reachable": 0, "has_financial": 0, "errors": 0}
    return {
        "total": row[0] or 0,
        "have_website": row[1] or 0,
        "reachable": row[2] or 0,
        "has_financial": row[3] or 0,
        "errors": row[4] or 0,
    }
