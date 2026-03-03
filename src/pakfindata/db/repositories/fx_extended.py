"""Extended FX rate repository — SBP interbank, open market, kerb rates."""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_fx_extended_schema",
    "upsert_fx_interbank",
    "upsert_fx_open_market",
    "upsert_fx_kerb",
    "get_fx_rate",
    "get_fx_history",
    "get_all_fx_latest",
    "get_fx_spread",
]

FX_EXTENDED_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS sbp_fx_interbank (
    date TEXT NOT NULL,
    currency TEXT NOT NULL,
    buying REAL,
    selling REAL,
    mid REAL,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, currency)
);

CREATE INDEX IF NOT EXISTS idx_sbp_fx_ib_date ON sbp_fx_interbank(date);

CREATE TABLE IF NOT EXISTS sbp_fx_open_market (
    date TEXT NOT NULL,
    currency TEXT NOT NULL,
    buying REAL,
    selling REAL,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, currency)
);

CREATE INDEX IF NOT EXISTS idx_sbp_fx_om_date ON sbp_fx_open_market(date);

CREATE TABLE IF NOT EXISTS forex_kerb (
    date TEXT NOT NULL,
    currency TEXT NOT NULL,
    buying REAL,
    selling REAL,
    source TEXT DEFAULT 'forex.pk',
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, currency, source)
);

CREATE INDEX IF NOT EXISTS idx_forex_kerb_date ON forex_kerb(date);
"""


def init_fx_extended_schema(con: sqlite3.Connection) -> None:
    """Create extended FX tables."""
    con.executescript(FX_EXTENDED_SCHEMA_SQL)
    con.commit()


def upsert_fx_interbank(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update SBP interbank FX rate."""
    try:
        buying = data.get("buying")
        selling = data.get("selling")
        mid = (buying + selling) / 2.0 if buying and selling else None
        con.execute(
            """INSERT INTO sbp_fx_interbank (date, currency, buying, selling, mid, scraped_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, currency) DO UPDATE SET
                 buying=excluded.buying, selling=excluded.selling,
                 mid=excluded.mid, scraped_at=datetime('now')
            """,
            (data["date"], data["currency"], buying, selling, mid),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_fx_open_market(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update SBP open market FX rate."""
    try:
        con.execute(
            """INSERT INTO sbp_fx_open_market (date, currency, buying, selling, scraped_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, currency) DO UPDATE SET
                 buying=excluded.buying, selling=excluded.selling,
                 scraped_at=datetime('now')
            """,
            (data["date"], data["currency"], data.get("buying"), data.get("selling")),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_fx_kerb(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update kerb FX rate."""
    try:
        con.execute(
            """INSERT INTO forex_kerb (date, currency, buying, selling, source, scraped_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, currency, source) DO UPDATE SET
                 buying=excluded.buying, selling=excluded.selling,
                 scraped_at=datetime('now')
            """,
            (
                data["date"], data["currency"],
                data.get("buying"), data.get("selling"),
                data.get("source", "forex.pk"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def get_fx_rate(
    con: sqlite3.Connection,
    currency: str,
    source: str = "interbank",
    date: str | None = None,
) -> dict | None:
    """Get FX rate for a currency from a source."""
    table = _source_table(source)
    if date is None:
        row = con.execute(
            f"SELECT * FROM {table} WHERE currency=? ORDER BY date DESC LIMIT 1",
            (currency.upper(),),
        ).fetchone()
    else:
        row = con.execute(
            f"SELECT * FROM {table} WHERE currency=? AND date=?",
            (currency.upper(), date),
        ).fetchone()
    return dict(row) if row else None


def get_fx_history(
    con: sqlite3.Connection,
    currency: str,
    source: str = "interbank",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get FX rate history for a currency."""
    table = _source_table(source)
    query = f"SELECT * FROM {table} WHERE currency = ?"
    params: list = [currency.upper()]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"
    return pd.read_sql_query(query, con, params=params)


def get_all_fx_latest(
    con: sqlite3.Connection, source: str = "interbank"
) -> pd.DataFrame:
    """Get latest FX rates for all currencies from a source."""
    table = _source_table(source)
    return pd.read_sql_query(
        f"""SELECT f.* FROM {table} f
            INNER JOIN (
                SELECT currency, MAX(date) as max_date
                FROM {table}
                GROUP BY currency
            ) latest ON f.currency = latest.currency AND f.date = latest.max_date
            ORDER BY f.currency""",
        con,
    )


def get_fx_spread(
    con: sqlite3.Connection, currency: str, date: str | None = None
) -> dict:
    """Get FX spread across all sources for a currency."""
    result = {
        "currency": currency.upper(),
        "interbank": get_fx_rate(con, currency, "interbank", date),
        "open_market": get_fx_rate(con, currency, "open_market", date),
        "kerb": get_fx_rate(con, currency, "kerb", date),
    }
    return result


def _source_table(source: str) -> str:
    """Map source name to table name."""
    return {
        "interbank": "sbp_fx_interbank",
        "open_market": "sbp_fx_open_market",
        "kerb": "forex_kerb",
    }.get(source, "sbp_fx_interbank")
