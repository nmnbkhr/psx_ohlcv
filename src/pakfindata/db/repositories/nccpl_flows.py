"""NCCPL Flow Intelligence repository — FIPI, LIPI, sector-wise, derived signals."""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_nccpl_schema",
    "upsert_fipi",
    "upsert_lipi",
    "upsert_fipi_sector",
    "upsert_derived",
    "get_fipi_latest",
    "get_lipi_latest",
    "get_sector_flows_latest",
    "get_derived_latest",
    "get_derived_series",
    "date_already_fetched",
]

NCCPL_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS nccpl_fipi (
    date                        TEXT PRIMARY KEY,
    fpi_buy                     REAL,
    fpi_sell                    REAL,
    fpi_net                     REAL,
    fpi_foreign_individual_net  REAL,
    fpi_foreign_corporate_net   REAL,
    fpi_overseas_pak_net        REAL,
    fetched_at                  TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_nccpl_fipi_date ON nccpl_fipi(date);

CREATE TABLE IF NOT EXISTS nccpl_lipi (
    date                TEXT PRIMARY KEY,
    mf_buy              REAL,
    mf_sell             REAL,
    mf_net              REAL,
    insurance_buy       REAL,
    insurance_sell      REAL,
    insurance_net       REAL,
    bank_buy            REAL,
    bank_sell           REAL,
    bank_net            REAL,
    retail_buy          REAL,
    retail_sell         REAL,
    retail_net          REAL,
    corporate_buy       REAL,
    corporate_sell      REAL,
    corporate_net       REAL,
    broker_buy          REAL,
    broker_sell         REAL,
    broker_net          REAL,
    nbfc_buy            REAL,
    nbfc_sell           REAL,
    nbfc_net            REAL,
    other_buy           REAL,
    other_sell          REAL,
    other_net           REAL,
    fetched_at          TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_nccpl_lipi_date ON nccpl_lipi(date);

CREATE TABLE IF NOT EXISTS nccpl_fipi_sector (
    date        TEXT,
    sector      TEXT,
    fpi_buy     REAL,
    fpi_sell    REAL,
    fpi_net     REAL,
    fetched_at  TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, sector)
);

CREATE INDEX IF NOT EXISTS idx_nccpl_fipi_sector_date ON nccpl_fipi_sector(date);

CREATE TABLE IF NOT EXISTS nccpl_flows_derived (
    date                        TEXT PRIMARY KEY,
    fpi_net_4w                  REAL,
    mf_net_4w                   REAL,
    retail_net_4w               REAL,
    bank_net_4w                 REAL,
    smart_money_net             REAL,
    dumb_money_net              REAL,
    smart_dumb_ratio            REAL,
    institutional_consensus     INTEGER,
    foreign_domestic_divergence INTEGER,
    flow_regime_signal          TEXT,
    computed_at                 TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_nccpl_derived_date ON nccpl_flows_derived(date);
"""


def init_nccpl_schema(con: sqlite3.Connection) -> None:
    """Create NCCPL flow tables if they don't exist."""
    for stmt in NCCPL_SCHEMA_SQL.split(";"):
        stmt = stmt.strip()
        if stmt:
            con.execute(stmt)
    con.commit()


# ── UPSERT FUNCTIONS ──────────────────────────────────────


def upsert_fipi(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or replace FIPI row. Returns True on success."""
    con.execute(
        """INSERT OR REPLACE INTO nccpl_fipi
           (date, fpi_buy, fpi_sell, fpi_net,
            fpi_foreign_individual_net, fpi_foreign_corporate_net,
            fpi_overseas_pak_net)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (
            data["date"],
            data.get("fpi_buy"),
            data.get("fpi_sell"),
            data.get("fpi_net"),
            data.get("fpi_foreign_individual_net"),
            data.get("fpi_foreign_corporate_net"),
            data.get("fpi_overseas_pak_net"),
        ),
    )
    con.commit()
    return True


def upsert_lipi(con: sqlite3.Connection, data: dict) -> bool:
    """Insert or replace LIPI row. Returns True on success."""
    con.execute(
        """INSERT OR REPLACE INTO nccpl_lipi
           (date, mf_buy, mf_sell, mf_net,
            insurance_buy, insurance_sell, insurance_net,
            bank_buy, bank_sell, bank_net,
            retail_buy, retail_sell, retail_net,
            corporate_buy, corporate_sell, corporate_net,
            broker_buy, broker_sell, broker_net,
            nbfc_buy, nbfc_sell, nbfc_net,
            other_buy, other_sell, other_net)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            data["date"],
            data.get("mf_buy"), data.get("mf_sell"), data.get("mf_net"),
            data.get("insurance_buy"), data.get("insurance_sell"), data.get("insurance_net"),
            data.get("bank_buy"), data.get("bank_sell"), data.get("bank_net"),
            data.get("retail_buy"), data.get("retail_sell"), data.get("retail_net"),
            data.get("corporate_buy"), data.get("corporate_sell"), data.get("corporate_net"),
            data.get("broker_buy"), data.get("broker_sell"), data.get("broker_net"),
            data.get("nbfc_buy"), data.get("nbfc_sell"), data.get("nbfc_net"),
            data.get("other_buy"), data.get("other_sell"), data.get("other_net"),
        ),
    )
    con.commit()
    return True


def upsert_fipi_sector(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Insert or replace sector FIPI rows. Returns count inserted."""
    count = 0
    for row in rows:
        con.execute(
            """INSERT OR REPLACE INTO nccpl_fipi_sector
               (date, sector, fpi_buy, fpi_sell, fpi_net)
               VALUES (?, ?, ?, ?, ?)""",
            (
                row["date"],
                row["sector"],
                row.get("fpi_buy"),
                row.get("fpi_sell"),
                row.get("fpi_net"),
            ),
        )
        count += 1
    con.commit()
    return count


def upsert_derived(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Write derived signals to nccpl_flows_derived. Returns rows written."""
    cols = [
        "date", "fpi_net_4w", "mf_net_4w", "retail_net_4w", "bank_net_4w",
        "smart_money_net", "dumb_money_net", "smart_dumb_ratio",
        "institutional_consensus", "foreign_domestic_divergence",
        "flow_regime_signal",
    ]
    available = [c for c in cols if c in df.columns]
    df[available].to_sql("nccpl_flows_derived", con, if_exists="replace", index=False)
    return len(df)


# ── QUERY FUNCTIONS ───────────────────────────────────────


def get_fipi_latest(con: sqlite3.Connection, limit: int = 30) -> pd.DataFrame:
    """Get most recent FIPI rows."""
    return pd.read_sql_query(
        "SELECT * FROM nccpl_fipi ORDER BY date DESC LIMIT ?",
        con, params=(limit,),
    )


def get_lipi_latest(con: sqlite3.Connection, limit: int = 30) -> pd.DataFrame:
    """Get most recent LIPI rows."""
    return pd.read_sql_query(
        "SELECT * FROM nccpl_lipi ORDER BY date DESC LIMIT ?",
        con, params=(limit,),
    )


def get_sector_flows_latest(con: sqlite3.Connection) -> pd.DataFrame:
    """Get sector flows for the most recent date."""
    return pd.read_sql_query(
        """SELECT * FROM nccpl_fipi_sector
           WHERE date = (SELECT MAX(date) FROM nccpl_fipi_sector)
           ORDER BY fpi_net DESC""",
        con,
    )


def get_derived_latest(con: sqlite3.Connection) -> dict | None:
    """Get the most recent derived signals row."""
    row = con.execute(
        "SELECT * FROM nccpl_flows_derived ORDER BY date DESC LIMIT 1"
    ).fetchone()
    if row:
        return dict(row)
    return None


def get_derived_series(con: sqlite3.Connection, limit: int = 90) -> pd.DataFrame:
    """Get derived signal time series."""
    return pd.read_sql_query(
        "SELECT * FROM nccpl_flows_derived ORDER BY date DESC LIMIT ?",
        con, params=(limit,),
    )


def date_already_fetched(con: sqlite3.Connection, date_str: str) -> bool:
    """Check if a date already has FIPI data."""
    row = con.execute(
        "SELECT 1 FROM nccpl_fipi WHERE date = ?", (date_str,)
    ).fetchone()
    return row is not None
