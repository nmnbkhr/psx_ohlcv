"""Naya Pakistan Certificate (NPC) rates repository.

Stores SBP-published conventional NPC rates (USD, GBP, EUR, PKR × 5 tenors)
and provides cross-currency analytics views joining NPC with global RFRs,
KIBOR, and FX rates.
"""

import logging
import sqlite3
from datetime import datetime

import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

NPC_RATES_SCHEMA = """
CREATE TABLE IF NOT EXISTS npc_rates (
    date TEXT NOT NULL,
    effective_date TEXT,
    currency TEXT NOT NULL,
    tenor TEXT NOT NULL,
    rate REAL NOT NULL,
    certificate_type TEXT NOT NULL DEFAULT 'conventional',
    sro_reference TEXT,
    source TEXT NOT NULL DEFAULT 'sbp',
    fetched_at TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (date, currency, tenor, certificate_type)
);

CREATE INDEX IF NOT EXISTS idx_npc_date ON npc_rates(date);
CREATE INDEX IF NOT EXISTS idx_npc_currency ON npc_rates(currency);
CREATE INDEX IF NOT EXISTS idx_npc_tenor ON npc_rates(tenor);
CREATE INDEX IF NOT EXISTS idx_npc_effective ON npc_rates(effective_date);
"""

# Views use subqueries for RFR/KIBOR/FX lookups because NPC rates change
# infrequently (SRO-driven) so exact date joins would miss most rows.

NPC_VS_RFR_VIEW = """
CREATE VIEW IF NOT EXISTS v_npc_vs_rfr_spread AS
SELECT
    n.date,
    n.currency,
    n.tenor,
    n.rate AS npc_rate,
    n.certificate_type,
    CASE n.currency
        WHEN 'USD' THEN (SELECT rate FROM global_reference_rates
                         WHERE rate_name='SOFR' AND tenor='ON'
                         AND date <= n.date ORDER BY date DESC LIMIT 1)
        WHEN 'GBP' THEN (SELECT rate FROM global_reference_rates
                         WHERE rate_name='SONIA' AND tenor='ON'
                         AND date <= n.date ORDER BY date DESC LIMIT 1)
        WHEN 'EUR' THEN (SELECT rate FROM global_reference_rates
                         WHERE rate_name='EUSTR' AND tenor='ON'
                         AND date <= n.date ORDER BY date DESC LIMIT 1)
    END AS rfr_rate,
    CASE n.currency
        WHEN 'USD' THEN 'SOFR'
        WHEN 'GBP' THEN 'SONIA'
        WHEN 'EUR' THEN 'EUSTR'
    END AS rfr_name,
    ROUND(n.rate - COALESCE(
        CASE n.currency
            WHEN 'USD' THEN (SELECT rate FROM global_reference_rates
                             WHERE rate_name='SOFR' AND tenor='ON'
                             AND date <= n.date ORDER BY date DESC LIMIT 1)
            WHEN 'GBP' THEN (SELECT rate FROM global_reference_rates
                             WHERE rate_name='SONIA' AND tenor='ON'
                             AND date <= n.date ORDER BY date DESC LIMIT 1)
            WHEN 'EUR' THEN (SELECT rate FROM global_reference_rates
                             WHERE rate_name='EUSTR' AND tenor='ON'
                             AND date <= n.date ORDER BY date DESC LIMIT 1)
        END, 0), 4) AS npc_premium_over_rfr
FROM npc_rates n
WHERE n.currency IN ('USD', 'GBP', 'EUR')
ORDER BY n.date DESC, n.currency, n.tenor;
"""

NPC_CARRY_TRADE_VIEW = """
CREATE VIEW IF NOT EXISTS v_npc_carry_trade AS
SELECT
    n.date,
    n.currency AS npc_currency,
    n.tenor,
    n.rate AS npc_rate,
    (SELECT offer FROM kibor_daily
     WHERE date <= n.date
     AND tenor = CASE n.tenor
         WHEN '3M' THEN '3M' WHEN '6M' THEN '6M'
         WHEN '12M' THEN '12M' ELSE '12M'
     END ORDER BY date DESC LIMIT 1) AS kibor_offer,
    ROUND(
        (SELECT offer FROM kibor_daily
         WHERE date <= n.date
         AND tenor = CASE n.tenor
             WHEN '3M' THEN '3M' WHEN '6M' THEN '6M'
             WHEN '12M' THEN '12M' ELSE '12M'
         END ORDER BY date DESC LIMIT 1)
        - n.rate, 4) AS kibor_npc_spread,
    (SELECT selling FROM sbp_fx_interbank
     WHERE date <= n.date
     AND currency = n.currency
     ORDER BY date DESC LIMIT 1) AS fx_rate_pkr
FROM npc_rates n
WHERE n.currency IN ('USD', 'GBP', 'EUR')
  AND n.certificate_type = 'conventional'
ORDER BY n.date DESC, n.currency, n.tenor;
"""

NPC_YIELD_CURVE_VIEW = """
CREATE VIEW IF NOT EXISTS v_npc_yield_curve AS
SELECT
    n.date,
    n.currency,
    n.certificate_type,
    MAX(CASE WHEN n.tenor = '3M' THEN n.rate END) AS rate_3m,
    MAX(CASE WHEN n.tenor = '6M' THEN n.rate END) AS rate_6m,
    MAX(CASE WHEN n.tenor = '12M' THEN n.rate END) AS rate_12m,
    MAX(CASE WHEN n.tenor = '3Y' THEN n.rate END) AS rate_3y,
    MAX(CASE WHEN n.tenor = '5Y' THEN n.rate END) AS rate_5y
FROM npc_rates n
GROUP BY n.date, n.currency, n.certificate_type
ORDER BY n.date DESC, n.currency;
"""

NPC_MULTICURRENCY_VIEW = """
CREATE VIEW IF NOT EXISTS v_multicurrency_dashboard AS
SELECT
    n.date,
    n.currency,
    n.tenor,
    n.rate AS npc_rate,
    CASE n.currency
        WHEN 'USD' THEN (SELECT rate FROM global_reference_rates
                         WHERE rate_name='SOFR' AND tenor='ON'
                         AND date <= n.date ORDER BY date DESC LIMIT 1)
        WHEN 'GBP' THEN (SELECT rate FROM global_reference_rates
                         WHERE rate_name='SONIA' AND tenor='ON'
                         AND date <= n.date ORDER BY date DESC LIMIT 1)
        WHEN 'EUR' THEN (SELECT rate FROM global_reference_rates
                         WHERE rate_name='EUSTR' AND tenor='ON'
                         AND date <= n.date ORDER BY date DESC LIMIT 1)
    END AS global_rfr,
    (SELECT offer FROM kibor_daily
     WHERE date <= n.date
     AND tenor = CASE n.tenor
         WHEN '3M' THEN '3M' WHEN '6M' THEN '6M'
         WHEN '12M' THEN '12M' ELSE '12M'
     END ORDER BY date DESC LIMIT 1) AS kibor_offer,
    (SELECT selling FROM sbp_fx_interbank
     WHERE date <= n.date
     AND currency = n.currency
     ORDER BY date DESC LIMIT 1) AS fx_rate_pkr
FROM npc_rates n
WHERE n.certificate_type = 'conventional'
  AND n.currency IN ('USD', 'GBP', 'EUR')
ORDER BY n.date DESC, n.currency, n.tenor;
"""


# ---------------------------------------------------------------------------
# Table / view bootstrap
# ---------------------------------------------------------------------------

def ensure_tables(con: sqlite3.Connection):
    """Create npc_rates table and analytics views."""
    con.executescript(NPC_RATES_SCHEMA)

    # Views depend on other tables; create silently and skip on error
    for name, ddl in [
        ("v_npc_vs_rfr_spread", NPC_VS_RFR_VIEW),
        ("v_npc_carry_trade", NPC_CARRY_TRADE_VIEW),
        ("v_npc_yield_curve", NPC_YIELD_CURVE_VIEW),
        ("v_multicurrency_dashboard", NPC_MULTICURRENCY_VIEW),
    ]:
        try:
            con.executescript(ddl)
        except Exception as e:
            logger.warning("Could not create view %s: %s", name, e)

    con.commit()


# ---------------------------------------------------------------------------
# Upsert
# ---------------------------------------------------------------------------

def upsert_npc_rate(
    con: sqlite3.Connection,
    date: str,
    currency: str,
    tenor: str,
    rate: float,
    certificate_type: str = "conventional",
    effective_date: str | None = None,
    sro_reference: str | None = None,
    source: str = "sbp",
) -> bool:
    """Insert or update an NPC rate row. Returns True if row was written."""
    try:
        con.execute(
            """INSERT INTO npc_rates
                   (date, currency, tenor, rate, certificate_type,
                    effective_date, sro_reference, source)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT (date, currency, tenor, certificate_type)
               DO UPDATE SET rate=excluded.rate,
                             effective_date=excluded.effective_date,
                             sro_reference=excluded.sro_reference,
                             source=excluded.source,
                             fetched_at=datetime('now')
            """,
            (date, currency, tenor, rate, certificate_type,
             effective_date, sro_reference, source),
        )
        return True
    except Exception as e:
        logger.error("upsert_npc_rate failed: %s", e)
        return False


# ---------------------------------------------------------------------------
# Change detection
# ---------------------------------------------------------------------------

def rates_changed(con: sqlite3.Connection, new_rates: list[dict]) -> bool:
    """Check if any scraped rate differs from the latest stored rate."""
    for r in new_rates:
        row = con.execute(
            """SELECT rate FROM npc_rates
               WHERE currency = ? AND tenor = ? AND certificate_type = ?
               ORDER BY date DESC LIMIT 1""",
            (r["currency"], r["tenor"], r.get("certificate_type", "conventional")),
        ).fetchone()
        if row is None or abs(row[0] - r["rate"]) > 0.001:
            return True
    return False


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def get_latest_npc_rates(
    con: sqlite3.Connection,
    currency: str | None = None,
    certificate_type: str = "conventional",
) -> pd.DataFrame:
    """Return latest NPC rates, optionally filtered by currency."""
    sql = """
        SELECT n.* FROM npc_rates n
        INNER JOIN (
            SELECT currency, tenor, certificate_type, MAX(date) AS max_date
            FROM npc_rates
            WHERE certificate_type = ?
            GROUP BY currency, tenor, certificate_type
        ) latest ON n.date = latest.max_date
                 AND n.currency = latest.currency
                 AND n.tenor = latest.tenor
                 AND n.certificate_type = latest.certificate_type
    """
    params: list = [certificate_type]
    if currency:
        sql += " WHERE n.currency = ?"
        params.append(currency.upper())
    sql += " ORDER BY n.currency, n.tenor"
    return pd.read_sql_query(sql, con, params=params)


def get_npc_rate_history(
    con: sqlite3.Connection,
    currency: str = "USD",
    tenor: str = "12M",
    certificate_type: str = "conventional",
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """Historical NPC rate for a specific currency+tenor."""
    sql = """SELECT * FROM npc_rates
             WHERE currency = ? AND tenor = ? AND certificate_type = ?"""
    params: list = [currency.upper(), tenor, certificate_type]
    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    if end_date:
        sql += " AND date <= ?"
        params.append(end_date)
    sql += " ORDER BY date DESC"
    return pd.read_sql_query(sql, con, params=params)


def get_npc_yield_curve(
    con: sqlite3.Connection,
    currency: str = "USD",
    date: str | None = None,
) -> dict | None:
    """Return yield curve (all tenors) for a currency on a date."""
    if date:
        row = con.execute(
            "SELECT * FROM v_npc_yield_curve WHERE currency = ? AND date = ?",
            (currency.upper(), date),
        ).fetchone()
    else:
        row = con.execute(
            "SELECT * FROM v_npc_yield_curve WHERE currency = ? ORDER BY date DESC LIMIT 1",
            (currency.upper(),),
        ).fetchone()
    if row:
        return dict(row)
    return None


def get_npc_vs_rfr_spread(
    con: sqlite3.Connection,
    currency: str | None = None,
    start_date: str | None = None,
) -> pd.DataFrame:
    """Query v_npc_vs_rfr_spread view."""
    sql = "SELECT * FROM v_npc_vs_rfr_spread WHERE 1=1"
    params: list = []
    if currency:
        sql += " AND currency = ?"
        params.append(currency.upper())
    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    sql += " ORDER BY date DESC, currency, tenor"
    return pd.read_sql_query(sql, con, params=params)


def get_carry_trade_analysis(
    con: sqlite3.Connection,
    currency: str = "USD",
    start_date: str | None = None,
) -> pd.DataFrame:
    """Query v_npc_carry_trade view."""
    sql = "SELECT * FROM v_npc_carry_trade WHERE npc_currency = ?"
    params: list = [currency.upper()]
    if start_date:
        sql += " AND date >= ?"
        params.append(start_date)
    sql += " ORDER BY date DESC, tenor"
    return pd.read_sql_query(sql, con, params=params)


def get_multicurrency_dashboard(
    con: sqlite3.Connection,
    date: str | None = None,
) -> pd.DataFrame:
    """Query v_multicurrency_dashboard for latest or specific date."""
    if date:
        sql = "SELECT * FROM v_multicurrency_dashboard WHERE date = ? ORDER BY currency, tenor"
        return pd.read_sql_query(sql, con, params=[date])
    else:
        # Latest date per currency
        sql = """
            SELECT d.* FROM v_multicurrency_dashboard d
            INNER JOIN (
                SELECT currency, MAX(date) AS max_date
                FROM v_multicurrency_dashboard
                GROUP BY currency
            ) latest ON d.date = latest.max_date AND d.currency = latest.currency
            ORDER BY d.currency, d.tenor
        """
        return pd.read_sql_query(sql, con)
