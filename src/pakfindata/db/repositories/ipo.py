"""IPO listings & listing status repository."""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_ipo_schema",
    "upsert_ipo_listing",
    "get_ipo_listings",
    "get_upcoming_ipos",
    "get_recent_listings",
    "get_ipo_by_symbol",
]

IPO_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS ipo_listings (
    symbol TEXT NOT NULL,
    company_name TEXT,
    board TEXT,
    status TEXT,
    offer_price REAL,
    shares_offered INTEGER,
    subscription_open TEXT,
    subscription_close TEXT,
    listing_date TEXT NOT NULL DEFAULT '',
    ipo_type TEXT,
    prospectus_url TEXT,
    updated_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (symbol, listing_date)
);

CREATE INDEX IF NOT EXISTS idx_ipo_status ON ipo_listings(status);
CREATE INDEX IF NOT EXISTS idx_ipo_listing_date ON ipo_listings(listing_date);
"""


def init_ipo_schema(con: sqlite3.Connection) -> None:
    """Create IPO tables if they don't exist."""
    con.executescript(IPO_SCHEMA_SQL)
    con.commit()


def upsert_ipo_listing(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update an IPO listing record."""
    try:
        con.execute(
            """INSERT INTO ipo_listings (
                   symbol, company_name, board, status,
                   offer_price, shares_offered,
                   subscription_open, subscription_close,
                   listing_date, ipo_type, prospectus_url, updated_at
               ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(symbol, listing_date) DO UPDATE SET
                 company_name=excluded.company_name,
                 board=excluded.board,
                 status=excluded.status,
                 offer_price=excluded.offer_price,
                 shares_offered=excluded.shares_offered,
                 subscription_open=excluded.subscription_open,
                 subscription_close=excluded.subscription_close,
                 ipo_type=excluded.ipo_type,
                 prospectus_url=excluded.prospectus_url,
                 updated_at=datetime('now')
            """,
            (
                data["symbol"],
                data.get("company_name"),
                data.get("board"),
                data.get("status"),
                data.get("offer_price"),
                data.get("shares_offered"),
                data.get("subscription_open"),
                data.get("subscription_close"),
                data.get("listing_date", ""),
                data.get("ipo_type"),
                data.get("prospectus_url"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def get_ipo_listings(
    con: sqlite3.Connection,
    status: str | None = None,
    board: str | None = None,
) -> pd.DataFrame:
    """Get IPO listings, optionally filtered by status or board."""
    query = "SELECT * FROM ipo_listings WHERE 1=1"
    params: list = []

    if status:
        query += " AND LOWER(status) = LOWER(?)"
        params.append(status)
    if board:
        query += " AND LOWER(board) = LOWER(?)"
        params.append(board)

    query += " ORDER BY listing_date DESC NULLS LAST"
    return pd.read_sql_query(query, con, params=params)


def get_upcoming_ipos(con: sqlite3.Connection) -> pd.DataFrame:
    """Get IPOs with status 'upcoming' or subscription still open."""
    today = datetime.now().strftime("%Y-%m-%d")
    return pd.read_sql_query(
        """SELECT * FROM ipo_listings
           WHERE LOWER(status) IN ('upcoming', 'open', 'subscription')
              OR subscription_close >= ?
           ORDER BY COALESCE(subscription_open, listing_date) ASC""",
        con,
        params=(today,),
    )


def get_recent_listings(
    con: sqlite3.Connection, n: int = 20
) -> pd.DataFrame:
    """Get most recently listed IPOs."""
    return pd.read_sql_query(
        """SELECT * FROM ipo_listings
           WHERE listing_date IS NOT NULL
           ORDER BY listing_date DESC
           LIMIT ?""",
        con,
        params=(n,),
    )


def get_ipo_by_symbol(
    con: sqlite3.Connection, symbol: str
) -> dict | None:
    """Get IPO details for a specific symbol."""
    row = con.execute(
        "SELECT * FROM ipo_listings WHERE symbol = ? ORDER BY listing_date DESC LIMIT 1",
        (symbol.upper(),),
    ).fetchone()
    return dict(row) if row else None
