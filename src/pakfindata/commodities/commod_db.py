"""Commodity database module for PMEX OHLC and Margins data.

Separate SQLite database at /mnt/e/psxdata/commod/commod.db.
Tables: pmex_ohlc, pmex_margins.
"""

import json
import logging
import sqlite3
from datetime import date
from pathlib import Path

logger = logging.getLogger("pakfindata.commodities.commod_db")

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

COMMOD_DATA_ROOT = Path("/mnt/e/psxdata/commod")
COMMOD_DB_PATH = COMMOD_DATA_ROOT / "commod.db"
PMEX_OHLC_DIR = COMMOD_DATA_ROOT / "pmex_ohlc"
PMEX_MARGINS_DIR = COMMOD_DATA_ROOT / "pmex_margins"

# ─────────────────────────────────────────────────────────────────────────────
# Schema
# ─────────────────────────────────────────────────────────────────────────────

COMMOD_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS pmex_ohlc (
    trading_date     DATE NOT NULL,
    symbol           TEXT NOT NULL,
    open             REAL,
    high             REAL,
    low              REAL,
    close            REAL,
    traded_volume    INTEGER DEFAULT 0,
    settlement_price REAL,
    fx_rate          REAL,
    fetched_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (trading_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_pmex_ohlc_sym  ON pmex_ohlc(symbol);
CREATE INDEX IF NOT EXISTS idx_pmex_ohlc_date ON pmex_ohlc(trading_date);

CREATE TABLE IF NOT EXISTS pmex_margins (
    report_date          DATE NOT NULL,
    sheet_name           TEXT NOT NULL,
    product_group        TEXT,
    contract_code        TEXT NOT NULL,
    reference_price      REAL,
    initial_margin_pct   REAL,
    initial_margin_value REAL,
    wcm                  REAL,
    maintenance_margin   REAL,
    lower_limit          REAL,
    upper_limit          REAL,
    fx_rate              REAL,
    is_active            BOOLEAN DEFAULT 1,
    fetched_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (report_date, contract_code)
);
CREATE INDEX IF NOT EXISTS idx_pmex_margins_date ON pmex_margins(report_date);
CREATE INDEX IF NOT EXISTS idx_pmex_margins_code ON pmex_margins(contract_code);
"""

# ─────────────────────────────────────────────────────────────────────────────
# Connection & Schema Init
# ─────────────────────────────────────────────────────────────────────────────


def ensure_commod_dirs() -> None:
    """Create the commod directory tree if needed."""
    for d in [COMMOD_DATA_ROOT, PMEX_OHLC_DIR, PMEX_MARGINS_DIR]:
        d.mkdir(parents=True, exist_ok=True)


def get_commod_connection() -> sqlite3.Connection:
    """Connect to commod.db with WAL mode and Row factory."""
    ensure_commod_dirs()
    con = sqlite3.connect(str(COMMOD_DB_PATH), check_same_thread=False, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=30000")
    return con


def init_commod_schema(con: sqlite3.Connection) -> None:
    """Create tables and indexes if they don't exist."""
    con.executescript(COMMOD_SCHEMA_SQL)
    con.commit()


# ─────────────────────────────────────────────────────────────────────────────
# OHLC Upsert & Query
# ─────────────────────────────────────────────────────────────────────────────


def upsert_pmex_ohlc(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert OHLC rows into pmex_ohlc. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO pmex_ohlc
            (trading_date, symbol, open, high, low, close,
             traded_volume, settlement_price, fx_rate)
        VALUES
            (:trading_date, :symbol, :open, :high, :low, :close,
             :traded_volume, :settlement_price, :fx_rate)
        """,
        rows,
    )
    con.commit()
    return len(rows)


def get_pmex_ohlc_stats(con: sqlite3.Connection) -> dict:
    """Return summary stats for pmex_ohlc table."""
    row = con.execute(
        """
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT symbol) as symbols,
               MIN(trading_date) as min_date,
               MAX(trading_date) as max_date
        FROM pmex_ohlc
        """
    ).fetchone()
    return dict(row) if row else {"total_rows": 0, "symbols": 0, "min_date": None, "max_date": None}


def query_pmex_ohlc(
    con: sqlite3.Connection,
    symbol: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    active_only: bool = False,
    limit: int = 500,
) -> list[dict]:
    """Query pmex_ohlc with optional filters."""
    conditions = []
    params: list = []

    if symbol:
        conditions.append("symbol = ?")
        params.append(symbol)
    if start_date:
        conditions.append("trading_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("trading_date <= ?")
        params.append(end_date)
    if active_only:
        conditions.append("traded_volume > 0")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM pmex_ohlc {where} ORDER BY trading_date DESC, symbol LIMIT ?"
    params.append(limit)

    return [dict(r) for r in con.execute(sql, params).fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Margins Upsert & Query
# ─────────────────────────────────────────────────────────────────────────────


def upsert_pmex_margins(con: sqlite3.Connection, rows: list[dict]) -> int:
    """Bulk upsert margins rows into pmex_margins. Returns row count."""
    if not rows:
        return 0
    con.executemany(
        """
        INSERT OR REPLACE INTO pmex_margins
            (report_date, sheet_name, product_group, contract_code,
             reference_price, initial_margin_pct, initial_margin_value,
             wcm, maintenance_margin, lower_limit, upper_limit,
             fx_rate, is_active)
        VALUES
            (:report_date, :sheet_name, :product_group, :contract_code,
             :reference_price, :initial_margin_pct, :initial_margin_value,
             :wcm, :maintenance_margin, :lower_limit, :upper_limit,
             :fx_rate, :is_active)
        """,
        rows,
    )
    con.commit()
    return len(rows)


def get_pmex_margins_stats(con: sqlite3.Connection) -> dict:
    """Return summary stats for pmex_margins table."""
    row = con.execute(
        """
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT contract_code) as contracts,
               MIN(report_date) as min_date,
               MAX(report_date) as max_date
        FROM pmex_margins
        """
    ).fetchone()
    return dict(row) if row else {"total_rows": 0, "contracts": 0, "min_date": None, "max_date": None}


def query_pmex_margins(
    con: sqlite3.Connection,
    report_date: str | None = None,
    active_only: bool = False,
    limit: int = 500,
) -> list[dict]:
    """Query pmex_margins with optional filters."""
    conditions = []
    params: list = []

    if report_date:
        conditions.append("report_date = ?")
        params.append(report_date)
    else:
        # Latest date
        conditions.append("report_date = (SELECT MAX(report_date) FROM pmex_margins)")
    if active_only:
        conditions.append("is_active = 1")

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM pmex_margins {where} ORDER BY product_group, contract_code LIMIT ?"
    params.append(limit)

    return [dict(r) for r in con.execute(sql, params).fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# File Save Helpers
# ─────────────────────────────────────────────────────────────────────────────


def save_ohlc_json(
    data: list[dict],
    from_date: date,
    to_date: date,
    save_dir: Path | None = None,
) -> Path:
    """Save raw OHLC data as JSON. Returns the file path."""
    out_dir = save_dir or PMEX_OHLC_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"ohlc_{from_date.isoformat()}_{to_date.isoformat()}.json"
    fpath = out_dir / fname

    with open(fpath, "w") as f:
        json.dump(data, f, indent=2, default=str)

    logger.info("Saved OHLC JSON: %s (%d records)", fpath, len(data))
    return fpath


def save_margins_excel(
    raw_bytes: bytes,
    report_date: date,
    save_dir: Path | None = None,
) -> Path:
    """Save raw margins Excel bytes to disk. Returns the file path."""
    out_dir = save_dir or PMEX_MARGINS_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    fname = f"Margins-{report_date.strftime('%d-%m-%Y')}.xlsx"
    fpath = out_dir / fname

    with open(fpath, "wb") as f:
        f.write(raw_bytes)

    logger.info("Saved Margins Excel: %s (%d bytes)", fpath, len(raw_bytes))
    return fpath
