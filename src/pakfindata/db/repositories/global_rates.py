"""Global reference rates repository — SOFR, EFFR, SONIA, EUSTR, TONA.

Post-LIBOR alternative reference rates (ARRs) for FCY-denominated instruments.
Primary source: NY Fed (SOFR/EFFR). Stubs for BoE (SONIA), ECB (EUSTR), BoJ (TONA).
"""

import sqlite3
from datetime import datetime

import pandas as pd

__all__ = [
    "init_global_rates_schema",
    "ensure_tables",
    "upsert_global_rate",
    "upsert_term_rate",
    "get_latest_rate",
    "get_rate_history",
    "get_all_latest_rates",
    "get_sofr_kibor_spread",
    "get_rate_comparison",
]

GLOBAL_RATES_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS global_reference_rates (
    date TEXT NOT NULL,
    rate_name TEXT NOT NULL,
    currency TEXT NOT NULL,
    tenor TEXT NOT NULL DEFAULT 'ON',
    rate REAL NOT NULL,
    volume REAL,
    percentile_25 REAL,
    percentile_75 REAL,
    source TEXT NOT NULL DEFAULT 'nyfed',
    fetched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, rate_name, tenor)
);

CREATE INDEX IF NOT EXISTS idx_grr_date ON global_reference_rates(date);
CREATE INDEX IF NOT EXISTS idx_grr_rate_name ON global_reference_rates(rate_name);
CREATE INDEX IF NOT EXISTS idx_grr_currency ON global_reference_rates(currency);

CREATE TABLE IF NOT EXISTS term_reference_rates (
    date TEXT NOT NULL,
    rate_name TEXT NOT NULL,
    tenor TEXT NOT NULL,
    currency TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL DEFAULT 'cme',
    PRIMARY KEY (date, rate_name, tenor)
);

CREATE INDEX IF NOT EXISTS idx_trr_date ON term_reference_rates(date);
"""

# View must be created separately (executescript doesn't handle CREATE VIEW IF NOT EXISTS well
# inside a multi-statement script when the view already exists in some SQLite versions).
SOFR_KIBOR_SPREAD_VIEW_SQL = """\
CREATE VIEW IF NOT EXISTS v_sofr_kibor_spread AS
SELECT
    k.date,
    k.tenor,
    k.bid AS kibor_bid,
    k.offer AS kibor_offer,
    g.rate AS sofr_rate,
    ROUND(k.offer - g.rate, 4) AS spread_over_sofr,
    f.selling AS usdpkr
FROM kibor_daily k
LEFT JOIN global_reference_rates g
    ON g.date = k.date AND g.rate_name = 'SOFR' AND g.tenor = 'ON'
LEFT JOIN sbp_fx_interbank f
    ON f.date = k.date AND f.currency = 'USD'
WHERE k.tenor IN ('1W', '1M', '3M', '6M', '12M')
ORDER BY k.date DESC, k.tenor;
"""


def init_global_rates_schema(con: sqlite3.Connection) -> None:
    """Create global rates tables if they don't exist."""
    con.executescript(GLOBAL_RATES_SCHEMA_SQL)
    con.execute(SOFR_KIBOR_SPREAD_VIEW_SQL)
    _migrate_fi_fcy_columns(con)
    con.commit()


# Alias to match prompt convention
ensure_tables = init_global_rates_schema


def _migrate_fi_fcy_columns(con: sqlite3.Connection) -> None:
    """Add FCY denomination columns to existing FI tables if not present."""
    cursor = con.cursor()

    # fi_instruments
    for col, default in [
        ("denomination_currency", "'PKR'"),
        ("reference_rate", "NULL"),
        ("spread_bps", "NULL"),
        ("coupon_frequency", "NULL"),
    ]:
        try:
            cursor.execute(
                f"ALTER TABLE fi_instruments ADD COLUMN {col} TEXT DEFAULT {default}"
            )
        except Exception:
            pass  # column already exists

    # bonds_master
    for col, default in [
        ("denomination_currency", "'PKR'"),
        ("reference_rate", "NULL"),
        ("spread_bps", "NULL"),
    ]:
        try:
            cursor.execute(
                f"ALTER TABLE bonds_master ADD COLUMN {col} TEXT DEFAULT {default}"
            )
        except Exception:
            pass

    # sukuk_master
    for col, default in [
        ("denomination_currency", "'PKR'"),
        ("reference_rate", "NULL"),
        ("spread_bps", "NULL"),
    ]:
        try:
            cursor.execute(
                f"ALTER TABLE sukuk_master ADD COLUMN {col} TEXT DEFAULT {default}"
            )
        except Exception:
            pass


def upsert_global_rate(
    con: sqlite3.Connection,
    date: str,
    rate_name: str,
    currency: str,
    tenor: str = "ON",
    rate: float = 0.0,
    volume: float | None = None,
    percentile_25: float | None = None,
    percentile_75: float | None = None,
    source: str = "nyfed",
    **_kwargs,
) -> bool:
    """Insert or update a global reference rate."""
    try:
        con.execute(
            """INSERT INTO global_reference_rates
               (date, rate_name, currency, tenor, rate, volume,
                percentile_25, percentile_75, source, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
               ON CONFLICT(date, rate_name, tenor) DO UPDATE SET
                 currency=excluded.currency,
                 rate=excluded.rate,
                 volume=excluded.volume,
                 percentile_25=excluded.percentile_25,
                 percentile_75=excluded.percentile_75,
                 source=excluded.source,
                 fetched_at=datetime('now')
            """,
            (date, rate_name, currency, tenor, rate, volume,
             percentile_25, percentile_75, source),
        )
        return True
    except Exception:
        return False


def upsert_term_rate(
    con: sqlite3.Connection,
    date: str,
    rate_name: str,
    tenor: str,
    currency: str,
    rate: float,
    source: str = "cme",
) -> bool:
    """Insert or update a term reference rate."""
    try:
        con.execute(
            """INSERT INTO term_reference_rates
               (date, rate_name, tenor, currency, rate, source)
               VALUES (?, ?, ?, ?, ?, ?)
               ON CONFLICT(date, rate_name, tenor) DO UPDATE SET
                 currency=excluded.currency,
                 rate=excluded.rate,
                 source=excluded.source
            """,
            (date, rate_name, tenor, currency, rate, source),
        )
        return True
    except Exception:
        return False


def get_latest_rate(
    con: sqlite3.Connection, rate_name: str = "SOFR", tenor: str = "ON"
) -> dict | None:
    """Get the latest value of a specific rate."""
    row = con.execute(
        """SELECT * FROM global_reference_rates
           WHERE rate_name = ? AND tenor = ?
           ORDER BY date DESC LIMIT 1""",
        (rate_name, tenor),
    ).fetchone()
    return dict(row) if row else None


def get_rate_history(
    con: sqlite3.Connection,
    rate_name: str = "SOFR",
    tenor: str = "ON",
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 365,
) -> pd.DataFrame:
    """Get rate history for a specific rate/tenor combination."""
    query = "SELECT * FROM global_reference_rates WHERE rate_name = ? AND tenor = ?"
    params: list = [rate_name, tenor]

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    query += " ORDER BY date DESC"
    if limit:
        query += f" LIMIT {limit}"

    return pd.read_sql_query(query, con, params=params)


def get_all_latest_rates(con: sqlite3.Connection) -> pd.DataFrame:
    """Return latest value of every rate_name/tenor combination."""
    return pd.read_sql_query(
        """SELECT g.*
           FROM global_reference_rates g
           INNER JOIN (
               SELECT rate_name, tenor, MAX(date) AS max_date
               FROM global_reference_rates
               GROUP BY rate_name, tenor
           ) latest
           ON g.rate_name = latest.rate_name
              AND g.tenor = latest.tenor
              AND g.date = latest.max_date
           ORDER BY g.rate_name, g.tenor""",
        con,
    )


def get_sofr_kibor_spread(
    con: sqlite3.Connection,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Query the SOFR-KIBOR spread view."""
    query = "SELECT * FROM v_sofr_kibor_spread WHERE 1=1"
    params: list = []

    if start_date:
        query += " AND date >= ?"
        params.append(start_date)
    if end_date:
        query += " AND date <= ?"
        params.append(end_date)

    return pd.read_sql_query(query, con, params=params)


def get_rate_comparison(
    con: sqlite3.Connection, date: str | None = None
) -> dict:
    """Return a dict with SOFR, EFFR, KIBOR, KONIA, policy_rate for a given date.

    If date is None, uses the latest available date for each rate.
    """
    result = {}

    # SOFR
    if date:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='SOFR' AND tenor='ON' AND date=?",
            (date,),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='SOFR' AND tenor='ON' ORDER BY date DESC LIMIT 1"
        ).fetchone()
    result["SOFR"] = row["rate"] if row else None

    # EFFR
    if date:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='EFFR' AND tenor='ON' AND date=?",
            (date,),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='EFFR' AND tenor='ON' ORDER BY date DESC LIMIT 1"
        ).fetchone()
    result["EFFR"] = row["rate"] if row else None

    # KIBOR (6M offer as benchmark)
    if date:
        row = con.execute(
            "SELECT offer FROM kibor_daily WHERE tenor='6M' AND date=?",
            (date,),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT offer FROM kibor_daily WHERE tenor='6M' ORDER BY date DESC LIMIT 1"
        ).fetchone()
    result["KIBOR_6M"] = row["offer"] if row else None

    # KONIA
    if date:
        row = con.execute(
            "SELECT rate_pct FROM konia_daily WHERE date=?", (date,)
        ).fetchone()
    else:
        row = con.execute(
            "SELECT rate_pct FROM konia_daily ORDER BY date DESC LIMIT 1"
        ).fetchone()
    result["KONIA"] = row["rate_pct"] if row else None

    # SONIA (if available)
    if date:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='SONIA' AND tenor='ON' AND date=?",
            (date,),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='SONIA' AND tenor='ON' ORDER BY date DESC LIMIT 1"
        ).fetchone()
    result["SONIA"] = row["rate"] if row else None

    # EUSTR
    if date:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='EUSTR' AND tenor='ON' AND date=?",
            (date,),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='EUSTR' AND tenor='ON' ORDER BY date DESC LIMIT 1"
        ).fetchone()
    result["EUSTR"] = row["rate"] if row else None

    # TONA
    if date:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='TONA' AND tenor='ON' AND date=?",
            (date,),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT rate FROM global_reference_rates WHERE rate_name='TONA' AND tenor='ON' ORDER BY date DESC LIMIT 1"
        ).fetchone()
    result["TONA"] = row["rate"] if row else None

    return result
