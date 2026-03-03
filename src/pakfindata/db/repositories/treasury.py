"""Treasury repository — T-Bill & PIB auction data, GIS auctions."""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_treasury_schema",
    "upsert_tbill_auction",
    "upsert_pib_auction",
    "upsert_gis_auction",
    "get_tbill_auctions",
    "get_pib_auctions",
    "get_gis_auctions",
    "get_latest_tbill_yields",
    "get_latest_pib_yields",
    "get_yield_trend",
]

TREASURY_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS tbill_auctions (
    auction_date TEXT NOT NULL,
    tenor TEXT NOT NULL,
    target_amount_billions REAL,
    bids_received_billions REAL,
    amount_accepted_billions REAL,
    cutoff_yield REAL,
    cutoff_price REAL,
    weighted_avg_yield REAL,
    maturity_date TEXT,
    settlement_date TEXT,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (auction_date, tenor)
);

CREATE INDEX IF NOT EXISTS idx_tbill_date ON tbill_auctions(auction_date);
CREATE INDEX IF NOT EXISTS idx_tbill_tenor ON tbill_auctions(tenor);

CREATE TABLE IF NOT EXISTS pib_auctions (
    auction_date TEXT NOT NULL,
    tenor TEXT NOT NULL,
    pib_type TEXT NOT NULL DEFAULT 'Fixed',
    target_amount_billions REAL,
    bids_received_billions REAL,
    amount_accepted_billions REAL,
    cutoff_yield REAL,
    cutoff_price REAL,
    coupon_rate REAL,
    maturity_date TEXT,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (auction_date, tenor, pib_type)
);

CREATE INDEX IF NOT EXISTS idx_pib_date ON pib_auctions(auction_date);

CREATE TABLE IF NOT EXISTS gis_auctions (
    auction_date TEXT NOT NULL,
    gis_type TEXT NOT NULL,
    tenor TEXT,
    target_amount_billions REAL,
    amount_accepted_billions REAL,
    cutoff_rental_rate REAL,
    maturity_date TEXT,
    scraped_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (auction_date, gis_type)
);

CREATE INDEX IF NOT EXISTS idx_gis_date ON gis_auctions(auction_date);
"""


def init_treasury_schema(con: sqlite3.Connection) -> None:
    """Create treasury tables if they don't exist."""
    con.executescript(TREASURY_SCHEMA_SQL)
    con.commit()


def upsert_tbill_auction(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a T-Bill auction record."""
    try:
        con.execute(
            """INSERT INTO tbill_auctions
               (auction_date, tenor, target_amount_billions,
                bids_received_billions, amount_accepted_billions,
                cutoff_yield, cutoff_price, weighted_avg_yield,
                maturity_date, settlement_date, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(auction_date, tenor) DO UPDATE SET
                 target_amount_billions=excluded.target_amount_billions,
                 bids_received_billions=excluded.bids_received_billions,
                 amount_accepted_billions=excluded.amount_accepted_billions,
                 cutoff_yield=excluded.cutoff_yield,
                 cutoff_price=excluded.cutoff_price,
                 weighted_avg_yield=excluded.weighted_avg_yield,
                 maturity_date=excluded.maturity_date,
                 settlement_date=excluded.settlement_date,
                 scraped_at=datetime('now')
            """,
            (
                data["auction_date"],
                data["tenor"],
                data.get("target_amount_billions"),
                data.get("bids_received_billions"),
                data.get("amount_accepted_billions"),
                data.get("cutoff_yield"),
                data.get("cutoff_price"),
                data.get("weighted_avg_yield"),
                data.get("maturity_date"),
                data.get("settlement_date"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_pib_auction(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a PIB auction record."""
    try:
        con.execute(
            """INSERT INTO pib_auctions
               (auction_date, tenor, pib_type, target_amount_billions,
                bids_received_billions, amount_accepted_billions,
                cutoff_yield, cutoff_price, coupon_rate, maturity_date,
                scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(auction_date, tenor, pib_type) DO UPDATE SET
                 target_amount_billions=excluded.target_amount_billions,
                 bids_received_billions=excluded.bids_received_billions,
                 amount_accepted_billions=excluded.amount_accepted_billions,
                 cutoff_yield=excluded.cutoff_yield,
                 cutoff_price=excluded.cutoff_price,
                 coupon_rate=excluded.coupon_rate,
                 maturity_date=excluded.maturity_date,
                 scraped_at=datetime('now')
            """,
            (
                data["auction_date"],
                data["tenor"],
                data.get("pib_type", "Fixed"),
                data.get("target_amount_billions"),
                data.get("bids_received_billions"),
                data.get("amount_accepted_billions"),
                data.get("cutoff_yield"),
                data.get("cutoff_price"),
                data.get("coupon_rate"),
                data.get("maturity_date"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def upsert_gis_auction(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or update a GIS auction record."""
    try:
        con.execute(
            """INSERT INTO gis_auctions
               (auction_date, gis_type, tenor, target_amount_billions,
                amount_accepted_billions, cutoff_rental_rate,
                maturity_date, scraped_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(auction_date, gis_type) DO UPDATE SET
                 tenor=excluded.tenor,
                 target_amount_billions=excluded.target_amount_billions,
                 amount_accepted_billions=excluded.amount_accepted_billions,
                 cutoff_rental_rate=excluded.cutoff_rental_rate,
                 maturity_date=excluded.maturity_date,
                 scraped_at=datetime('now')
            """,
            (
                data["auction_date"],
                data["gis_type"],
                data.get("tenor"),
                data.get("target_amount_billions"),
                data.get("amount_accepted_billions"),
                data.get("cutoff_rental_rate"),
                data.get("maturity_date"),
            ),
        )
        con.commit()
        return True
    except Exception:
        return False


def get_tbill_auctions(
    con: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
    tenor: str | None = None,
) -> pd.DataFrame:
    """Get T-Bill auction history with optional filters."""
    query = "SELECT * FROM tbill_auctions WHERE 1=1"
    params: list = []

    if start_date:
        query += " AND auction_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND auction_date <= ?"
        params.append(end_date)
    if tenor:
        query += " AND tenor = ?"
        params.append(tenor)

    query += " ORDER BY auction_date DESC, tenor"
    return pd.read_sql_query(query, con, params=params)


def get_pib_auctions(
    con: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get PIB auction history."""
    query = "SELECT * FROM pib_auctions WHERE 1=1"
    params: list = []

    if start_date:
        query += " AND auction_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND auction_date <= ?"
        params.append(end_date)

    query += " ORDER BY auction_date DESC, tenor"
    return pd.read_sql_query(query, con, params=params)


def get_gis_auctions(
    con: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Get GIS auction history."""
    query = "SELECT * FROM gis_auctions WHERE 1=1"
    params: list = []

    if start_date:
        query += " AND auction_date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND auction_date <= ?"
        params.append(end_date)

    query += " ORDER BY auction_date DESC"
    return pd.read_sql_query(query, con, params=params)


def get_latest_tbill_yields(con: sqlite3.Connection) -> dict:
    """Get latest cutoff yields for all T-Bill tenors.

    Returns dict: {tenor: {cutoff_yield, auction_date, ...}}
    """
    rows = con.execute("""
        SELECT t.* FROM tbill_auctions t
        INNER JOIN (
            SELECT tenor, MAX(auction_date) as max_date
            FROM tbill_auctions
            GROUP BY tenor
        ) latest ON t.tenor = latest.tenor AND t.auction_date = latest.max_date
        ORDER BY t.tenor
    """).fetchall()
    return {row["tenor"]: dict(row) for row in rows}


def get_latest_pib_yields(con: sqlite3.Connection) -> dict:
    """Get latest cutoff yields for all PIB tenors."""
    rows = con.execute("""
        SELECT p.* FROM pib_auctions p
        INNER JOIN (
            SELECT tenor, pib_type, MAX(auction_date) as max_date
            FROM pib_auctions
            GROUP BY tenor, pib_type
        ) latest ON p.tenor = latest.tenor
                 AND p.pib_type = latest.pib_type
                 AND p.auction_date = latest.max_date
        ORDER BY p.tenor
    """).fetchall()
    return {f"{row['tenor']}_{row['pib_type']}": dict(row) for row in rows}


def get_yield_trend(
    con: sqlite3.Connection,
    tenor: str,
    n_auctions: int = 20,
) -> pd.DataFrame:
    """Get yield trend for a specific T-Bill tenor."""
    return pd.read_sql_query(
        """SELECT auction_date, cutoff_yield, weighted_avg_yield
           FROM tbill_auctions
           WHERE tenor = ?
           ORDER BY auction_date DESC
           LIMIT ?""",
        con,
        params=(tenor, n_auctions),
    )
