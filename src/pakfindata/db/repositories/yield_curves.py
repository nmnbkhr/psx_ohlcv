"""Yield curve & overnight rate repository — PKRV, KONIA, KIBOR."""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_yield_curve_schema",
    "upsert_pkrv_point",
    "upsert_pkisrv_point",
    "upsert_pkfrv_point",
    "upsert_konia_rate",
    "upsert_kibor_rate",
    "get_pkrv_curve",
    "get_pkrv_history",
    "get_konia_history",
    "get_kibor_history",
    "compare_curves",
    "get_latest_konia",
]

YIELD_CURVE_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS pkrv_daily (
    date TEXT NOT NULL,
    tenor_months INTEGER NOT NULL,
    yield_pct REAL NOT NULL,
    change_bps REAL,
    source TEXT DEFAULT 'SBP',
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, tenor_months)
);

CREATE INDEX IF NOT EXISTS idx_pkrv_date ON pkrv_daily(date);

CREATE TABLE IF NOT EXISTS pkisrv_daily (
    date TEXT NOT NULL,
    tenor TEXT NOT NULL,
    yield_pct REAL NOT NULL,
    source TEXT DEFAULT 'MUFAP',
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, tenor)
);

CREATE INDEX IF NOT EXISTS idx_pkisrv_date ON pkisrv_daily(date);

CREATE TABLE IF NOT EXISTS pkfrv_daily (
    date TEXT NOT NULL,
    bond_code TEXT NOT NULL,
    issue_date TEXT,
    maturity_date TEXT,
    coupon_frequency TEXT,
    fma_price REAL,
    source TEXT DEFAULT 'MUFAP',
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, bond_code)
);

CREATE INDEX IF NOT EXISTS idx_pkfrv_date ON pkfrv_daily(date);

CREATE TABLE IF NOT EXISTS konia_daily (
    date TEXT PRIMARY KEY,
    rate_pct REAL NOT NULL,
    volume_billions REAL,
    high REAL,
    low REAL,
    scraped_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS kibor_daily (
    date TEXT NOT NULL,
    tenor TEXT NOT NULL,
    bid REAL,
    offer REAL,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, tenor)
);

CREATE INDEX IF NOT EXISTS idx_kibor_date ON kibor_daily(date);
"""


def init_yield_curve_schema(con: sqlite3.Connection) -> None:
    """Create yield curve tables if they don't exist."""
    con.executescript(YIELD_CURVE_SCHEMA_SQL)
    con.commit()


def upsert_pkrv_point(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a PKRV yield curve point."""
    try:
        con.execute(
            """INSERT INTO pkrv_daily (date, tenor_months, yield_pct, change_bps, source, scraped_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, tenor_months) DO UPDATE SET
                 yield_pct=excluded.yield_pct,
                 change_bps=excluded.change_bps,
                 source=excluded.source,
                 scraped_at=datetime('now')
            """,
            (
                data["date"],
                data["tenor_months"],
                data["yield_pct"],
                data.get("change_bps"),
                data.get("source", "SBP"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_pkisrv_point(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a PKISRV yield curve point."""
    try:
        con.execute(
            """INSERT INTO pkisrv_daily (date, tenor, yield_pct, source, scraped_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, tenor) DO UPDATE SET
                 yield_pct=excluded.yield_pct,
                 source=excluded.source,
                 scraped_at=datetime('now')
            """,
            (
                data["date"],
                data["tenor"],
                data["yield_pct"],
                data.get("source", "MUFAP"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_pkfrv_point(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a PKFRV (floating rate valuation) point."""
    try:
        con.execute(
            """INSERT INTO pkfrv_daily (date, bond_code, issue_date, maturity_date,
                   coupon_frequency, fma_price, source, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, bond_code) DO UPDATE SET
                 issue_date=excluded.issue_date,
                 maturity_date=excluded.maturity_date,
                 coupon_frequency=excluded.coupon_frequency,
                 fma_price=excluded.fma_price,
                 source=excluded.source,
                 scraped_at=datetime('now')
            """,
            (
                data["date"],
                data["bond_code"],
                data.get("issue_date"),
                data.get("maturity_date"),
                data.get("coupon_frequency"),
                data.get("fma_price"),
                data.get("source", "MUFAP"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_konia_rate(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a KONIA daily rate."""
    try:
        con.execute(
            """INSERT INTO konia_daily (date, rate_pct, volume_billions, high, low, scraped_at)
               VALUES (?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date) DO UPDATE SET
                 rate_pct=excluded.rate_pct,
                 volume_billions=excluded.volume_billions,
                 high=excluded.high,
                 low=excluded.low,
                 scraped_at=datetime('now')
            """,
            (
                data["date"],
                data["rate_pct"],
                data.get("volume_billions"),
                data.get("high"),
                data.get("low"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_kibor_rate(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a KIBOR daily rate."""
    try:
        con.execute(
            """INSERT INTO kibor_daily (date, tenor, bid, offer, scraped_at)
               VALUES (?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, tenor) DO UPDATE SET
                 bid=excluded.bid,
                 offer=excluded.offer,
                 scraped_at=datetime('now')
            """,
            (
                data["date"],
                data["tenor"],
                data.get("bid"),
                data.get("offer"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def get_pkrv_curve(
    con: sqlite3.Connection, date: str | None = None
) -> pd.DataFrame:
    """Get PKRV yield curve for a date (latest if None)."""
    if date is None:
        date_row = con.execute(
            "SELECT MAX(date) as max_date FROM pkrv_daily"
        ).fetchone()
        if not date_row or not date_row["max_date"]:
            return pd.DataFrame()
        date = date_row["max_date"]

    return pd.read_sql_query(
        "SELECT * FROM pkrv_daily WHERE date = ? ORDER BY tenor_months",
        con,
        params=(date,),
    )


def get_pkrv_history(
    con: sqlite3.Connection,
    tenor_months: int,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get PKRV history for a specific tenor."""
    query = "SELECT * FROM pkrv_daily WHERE tenor_months = ?"
    params: list = [tenor_months]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"
    return pd.read_sql_query(query, con, params=params)


def get_konia_history(
    con: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get KONIA rate history."""
    query = "SELECT * FROM konia_daily WHERE 1=1"
    params: list = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"
    return pd.read_sql_query(query, con, params=params)


def get_latest_konia(con: sqlite3.Connection) -> dict | None:
    """Get the latest KONIA rate."""
    row = con.execute(
        "SELECT * FROM konia_daily ORDER BY date DESC LIMIT 1"
    ).fetchone()
    return dict(row) if row else None


def get_kibor_history(
    con: sqlite3.Connection,
    tenor: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get KIBOR rate history."""
    query = "SELECT * FROM kibor_daily WHERE 1=1"
    params: list = []

    if tenor:
        query += " AND tenor = ?"
        params.append(tenor)
    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC, tenor"
    return pd.read_sql_query(query, con, params=params)


def compare_curves(
    con: sqlite3.Connection, date1: str, date2: str
) -> pd.DataFrame:
    """Compare two PKRV yield curves side-by-side."""
    c1 = get_pkrv_curve(con, date1)
    c2 = get_pkrv_curve(con, date2)

    if c1.empty and c2.empty:
        return pd.DataFrame()

    merged = pd.merge(
        c1[["tenor_months", "yield_pct"]].rename(columns={"yield_pct": f"yield_{date1}"}),
        c2[["tenor_months", "yield_pct"]].rename(columns={"yield_pct": f"yield_{date2}"}),
        on="tenor_months",
        how="outer",
    )
    merged["change_bps"] = (
        (merged[f"yield_{date2}"] - merged[f"yield_{date1}"]) * 100
    ).round(1)
    return merged.sort_values("tenor_months")
