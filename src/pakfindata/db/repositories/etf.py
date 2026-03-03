"""ETF repository — master metadata, NAV history, and queries."""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_etf_schema",
    "upsert_etf_master",
    "upsert_etf_nav",
    "get_etf_list",
    "get_etf_nav_history",
    "get_etf_detail",
    "get_all_etf_latest_nav",
]

ETF_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS etf_master (
    symbol TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    amc TEXT,
    benchmark_index TEXT,
    inception_date TEXT,
    expense_ratio REAL,
    management_fee TEXT,
    shariah_compliant INTEGER DEFAULT 0,
    trustee TEXT,
    fiscal_year_end TEXT,
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS etf_nav (
    symbol TEXT NOT NULL,
    date TEXT NOT NULL,
    nav REAL,
    market_price REAL,
    premium_discount REAL,
    aum_millions REAL,
    outstanding_units INTEGER,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_etf_nav_date ON etf_nav(date);
CREATE INDEX IF NOT EXISTS idx_etf_nav_symbol ON etf_nav(symbol);
"""


def init_etf_schema(con: sqlite3.Connection) -> None:
    """Create ETF tables if they don't exist."""
    con.executescript(ETF_SCHEMA_SQL)
    con.commit()


def upsert_etf_master(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update an ETF master record."""
    try:
        con.execute(
            """INSERT INTO etf_master
               (symbol, name, amc, benchmark_index, inception_date,
                expense_ratio, management_fee, shariah_compliant,
                trustee, fiscal_year_end, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(symbol) DO UPDATE SET
                 name=excluded.name,
                 amc=excluded.amc,
                 benchmark_index=excluded.benchmark_index,
                 inception_date=excluded.inception_date,
                 expense_ratio=excluded.expense_ratio,
                 management_fee=excluded.management_fee,
                 shariah_compliant=excluded.shariah_compliant,
                 trustee=excluded.trustee,
                 fiscal_year_end=excluded.fiscal_year_end,
                 updated_at=datetime('now')
            """,
            (
                data["symbol"],
                data["name"],
                data.get("amc"),
                data.get("benchmark_index"),
                data.get("inception_date"),
                data.get("expense_ratio"),
                data.get("management_fee"),
                1 if data.get("shariah_compliant") else 0,
                data.get("trustee"),
                data.get("fiscal_year_end"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_etf_nav(
    con: sqlite3.Connection,
    symbol: str,
    date: str,
    nav: float | None = None,
    market_price: float | None = None,
    aum_millions: float | None = None,
    outstanding_units: int | None = None,
) -> bool:
    """Insert or update an ETF NAV record."""
    premium_discount = None
    if nav and market_price and nav > 0:
        premium_discount = round((market_price - nav) / nav * 100, 4)

    try:
        con.execute(
            """INSERT INTO etf_nav
               (symbol, date, nav, market_price, premium_discount,
                aum_millions, outstanding_units)
               VALUES (?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(symbol, date) DO UPDATE SET
                 nav=excluded.nav,
                 market_price=excluded.market_price,
                 premium_discount=excluded.premium_discount,
                 aum_millions=excluded.aum_millions,
                 outstanding_units=excluded.outstanding_units
            """,
            (symbol, date, nav, market_price, premium_discount,
             aum_millions, outstanding_units),
        )
        con.commit()
        return True
    except Exception:
        return False


def get_etf_list(con: sqlite3.Connection) -> list[dict]:
    """Get all ETFs from master table."""
    rows = con.execute(
        "SELECT * FROM etf_master ORDER BY symbol"
    ).fetchall()
    return [dict(r) for r in rows]


def get_etf_nav_history(
    con: sqlite3.Connection,
    symbol: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get NAV history for an ETF."""
    query = "SELECT * FROM etf_nav WHERE symbol = ?"
    params: list = [symbol]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date"
    return pd.read_sql_query(query, con, params=params)


def get_etf_detail(con: sqlite3.Connection, symbol: str) -> dict | None:
    """Get ETF master + latest NAV combined."""
    master = con.execute(
        "SELECT * FROM etf_master WHERE symbol = ?", (symbol,)
    ).fetchone()

    if not master:
        return None

    result = dict(master)

    latest_nav = con.execute(
        "SELECT * FROM etf_nav WHERE symbol = ? ORDER BY date DESC LIMIT 1",
        (symbol,),
    ).fetchone()

    if latest_nav:
        result["latest_nav"] = dict(latest_nav)
    else:
        result["latest_nav"] = None

    return result


def get_all_etf_latest_nav(con: sqlite3.Connection) -> pd.DataFrame:
    """Get latest NAV for all ETFs, joined with master data."""
    return pd.read_sql_query(
        """SELECT m.symbol, m.name, m.amc, m.benchmark_index,
                  m.shariah_compliant, m.inception_date,
                  n.date as nav_date, n.nav, n.market_price,
                  n.premium_discount, n.aum_millions
           FROM etf_master m
           LEFT JOIN (
               SELECT symbol, date, nav, market_price,
                      premium_discount, aum_millions
               FROM etf_nav
               WHERE (symbol, date) IN (
                   SELECT symbol, MAX(date) FROM etf_nav GROUP BY symbol
               )
           ) n ON m.symbol = n.symbol
           ORDER BY m.symbol
        """,
        con,
    )
