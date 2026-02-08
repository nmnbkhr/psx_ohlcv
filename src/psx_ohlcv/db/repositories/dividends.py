"""Dividend analytics repository — queries on company_payouts + eod_ohlcv."""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd

__all__ = [
    "get_dividend_history",
    "get_dividend_yield",
    "get_ex_dividend_dates",
    "get_highest_dividend_stocks",
    "get_upcoming_dividends",
]


def get_dividend_history(
    con: sqlite3.Connection,
    symbol: str,
    years: int | None = None,
) -> pd.DataFrame:
    """Get cash dividend history for a symbol, newest first.

    Args:
        symbol: Stock symbol.
        years: Limit to last N years (None = all).
    """
    query = """
        SELECT symbol, ex_date, amount, fiscal_year,
               announcement_date, book_closure_from, book_closure_to
        FROM company_payouts
        WHERE symbol = ? AND payout_type = 'cash'
    """
    params: list = [symbol.upper()]

    if years:
        cutoff = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")
        query += " AND ex_date >= ?"
        params.append(cutoff)

    query += " ORDER BY ex_date DESC"
    return pd.read_sql_query(query, con, params=params)


def get_dividend_yield(
    con: sqlite3.Connection,
    symbol: str,
    years: int = 1,
) -> float | None:
    """Compute trailing dividend yield for a symbol.

    yield = (sum of cash dividends in period) / latest close price * 100

    Args:
        symbol: Stock symbol.
        years: Lookback period (default 1 year).

    Returns:
        Yield percentage, or None if insufficient data.
    """
    symbol = symbol.upper()
    cutoff = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")

    # Sum cash dividends in the period
    row = con.execute(
        """SELECT COALESCE(SUM(amount), 0) as total_dps
           FROM company_payouts
           WHERE symbol = ? AND payout_type = 'cash' AND ex_date >= ?""",
        (symbol, cutoff),
    ).fetchone()
    total_dps = row["total_dps"] if row else 0
    if total_dps <= 0:
        return None

    # Get latest close price
    price_row = con.execute(
        """SELECT close FROM eod_ohlcv
           WHERE symbol = ? AND close > 0
           ORDER BY date DESC LIMIT 1""",
        (symbol,),
    ).fetchone()
    if not price_row or not price_row["close"]:
        return None

    return round(total_dps / price_row["close"] * 100, 2)


def get_ex_dividend_dates(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 20,
) -> list[str]:
    """Get ex-dividend dates for a symbol, newest first."""
    rows = con.execute(
        """SELECT DISTINCT ex_date FROM company_payouts
           WHERE symbol = ? AND payout_type = 'cash'
           ORDER BY ex_date DESC LIMIT ?""",
        (symbol.upper(), limit),
    ).fetchall()
    return [r["ex_date"] for r in rows]


def get_highest_dividend_stocks(
    con: sqlite3.Connection,
    n: int = 20,
    years: int = 1,
) -> pd.DataFrame:
    """Rank stocks by trailing dividend yield.

    Computes yield = SUM(dividends in period) / latest_close * 100
    for all symbols that paid dividends, then ranks by yield DESC.
    """
    cutoff = (datetime.now() - timedelta(days=365 * years)).strftime("%Y-%m-%d")

    return pd.read_sql_query(
        """
        WITH div_totals AS (
            SELECT symbol, SUM(amount) as total_dps, COUNT(*) as num_payouts
            FROM company_payouts
            WHERE payout_type = 'cash' AND ex_date >= ?
            GROUP BY symbol
            HAVING total_dps > 0
        ),
        latest_prices AS (
            SELECT e.symbol, e.close
            FROM eod_ohlcv e
            INNER JOIN (
                SELECT symbol, MAX(date) as max_date
                FROM eod_ohlcv
                WHERE close > 0
                GROUP BY symbol
            ) lp ON e.symbol = lp.symbol AND e.date = lp.max_date
        )
        SELECT d.symbol, d.total_dps, d.num_payouts,
               p.close as latest_price,
               ROUND(d.total_dps / p.close * 100, 2) as yield_pct
        FROM div_totals d
        INNER JOIN latest_prices p ON d.symbol = p.symbol
        ORDER BY yield_pct DESC
        LIMIT ?
        """,
        con,
        params=(cutoff, n),
    )


def get_upcoming_dividends(
    con: sqlite3.Connection,
    days_ahead: int = 30,
) -> pd.DataFrame:
    """Get upcoming ex-dividend dates within N days."""
    today = datetime.now().strftime("%Y-%m-%d")
    future = (datetime.now() + timedelta(days=days_ahead)).strftime("%Y-%m-%d")

    return pd.read_sql_query(
        """SELECT symbol, ex_date, payout_type, amount, fiscal_year
           FROM company_payouts
           WHERE ex_date >= ? AND ex_date <= ?
           ORDER BY ex_date ASC""",
        con,
        params=(today, future),
    )
