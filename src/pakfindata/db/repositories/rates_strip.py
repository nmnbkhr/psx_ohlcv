"""Rates & FX strip queries for the Dashboard header.

Consolidates the two query packs previously inlined in
``ui/page_views/dashboard.py``:

- ``get_rates_strip`` — macro rates (SBP policy, KIBOR 3M, T-Bill 3M, PKRV 10Y)
- ``get_fx_strip``    — per-currency latest selling rate from the interbank
  table with a kerb-market fallback.
"""

from __future__ import annotations

import sqlite3
from collections.abc import Sequence


DEFAULT_FX_CCY = ("USD", "EUR", "GBP", "AED", "SAR")


def get_rates_strip(con: sqlite3.Connection) -> dict:
    """Return latest-point tuples for the macro rates strip.

    Keys and value shapes:
      - ``policy``   -> (policy_rate, rate_date)     from ``sbp_policy_rates``
      - ``kibor3m``  -> (bid, offer, date)           from ``kibor_daily`` (tenor='3M')
      - ``tbill3m``  -> (cutoff_yield, auction_date) from ``tbill_auctions`` (tenor='3M')
      - ``pkrv10y``  -> (yield_pct, date)            from ``pkrv_daily`` (tenor_months=120)

    Any missing point returns ``None`` for that key.
    """
    queries = {
        "policy":  "SELECT policy_rate, rate_date FROM sbp_policy_rates ORDER BY rate_date DESC LIMIT 1",
        "kibor3m": "SELECT bid, offer, date FROM kibor_daily WHERE tenor='3M' ORDER BY date DESC LIMIT 1",
        "tbill3m": "SELECT cutoff_yield, auction_date FROM tbill_auctions WHERE tenor='3M' ORDER BY auction_date DESC LIMIT 1",
        "pkrv10y": "SELECT yield_pct, date FROM pkrv_daily WHERE tenor_months=120 ORDER BY date DESC LIMIT 1",
    }
    out: dict[str, tuple | None] = {}
    for key, sql in queries.items():
        try:
            row = con.execute(sql).fetchone()
            out[key] = tuple(row) if row else None
        except Exception:
            out[key] = None
    return out


def get_fx_strip(
    con: sqlite3.Connection,
    currencies: Sequence[str] = DEFAULT_FX_CCY,
) -> list[tuple[str, float, str]]:
    """Return the latest selling rate per currency.

    Tries ``sbp_fx_interbank`` first, then ``forex_kerb`` as fallback. The
    result preserves the requested order; currencies with no data in either
    table are skipped.

    Returns a list of ``(currency, selling_rate, as_of_date)`` tuples.
    """
    out: list[tuple[str, float, str]] = []
    for curr in currencies:
        rate = _fx_latest(con, "sbp_fx_interbank", curr) or _fx_latest(con, "forex_kerb", curr)
        if rate is not None:
            date_str, selling = rate
            out.append((curr, selling, date_str))
    return out


def _fx_latest(
    con: sqlite3.Connection,
    table: str,
    currency: str,
) -> tuple[str, float] | None:
    """Single-row helper — latest ``(date, selling)`` for a currency in a table."""
    try:
        row = con.execute(
            f"SELECT date, selling FROM {table} WHERE UPPER(currency)=? ORDER BY date DESC LIMIT 1",
            (currency,),
        ).fetchone()
        if row and row[1] is not None:
            return str(row[0]), float(row[1])
    except Exception:
        return None
    return None
