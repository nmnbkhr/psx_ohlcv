"""Sync FX microservice data into local PSX OHLCV database tables.

Pulls rates from the FX Trading Module (localhost:8100) and upserts into:
  - sbp_fx_interbank  (date, currency, buying, selling, mid)
  - kibor_daily        (date, tenor, bid, offer)
  - sbp_policy_rates   (rate_date, policy_rate)

Usage:
    from psx_ohlcv.sources.fx_sync import sync_fx_rates, backfill_fx_history
    con = connect()
    sync_fx_rates(con)                                # daily snapshot
    backfill_fx_history(con, "2025-01-01")            # full backfill
    backfill_fx_history(con, "2026-02-01", "2026-02-15")  # date range
"""

import sqlite3
import logging
from datetime import date

from .fx_client import FXClient

logger = logging.getLogger("fx_sync")
_fx = FXClient()


def sync_fx_rates(con: sqlite3.Connection) -> dict:
    """Pull latest FX rates from microservice and store locally.

    Syncs today's snapshot (interbank rates + KIBOR) into local DB tables.
    Returns summary dict with counts.
    """
    if not _fx.is_healthy():
        return {"status": "skipped", "reason": "FX service unavailable"}

    today = date.today().isoformat()
    rates_stored = 0
    kibor_stored = 0

    # ── 1. Interbank FX rates from snapshot ─────────────────────────
    snapshot = _fx.get_snapshot()
    if snapshot and "rates" in snapshot:
        for pair, data in snapshot["rates"].items():
            if not pair.endswith("/PKR"):
                continue
            currency = pair.replace("/PKR", "")
            buying = data.get("buying")
            selling = data.get("selling")
            if buying is None or selling is None:
                continue
            mid = round((buying + selling) / 2, 4)
            con.execute(
                """INSERT INTO sbp_fx_interbank (date, currency, buying, selling, mid)
                   VALUES (?, ?, ?, ?, ?)
                   ON CONFLICT(date, currency) DO UPDATE SET
                     buying=excluded.buying, selling=excluded.selling,
                     mid=excluded.mid, scraped_at=datetime('now')
                """,
                (today, currency, buying, selling, mid),
            )
            rates_stored += 1

    # ── 2. KIBOR rates ──────────────────────────────────────────────
    kibor_data = _fx.get_kibor()
    if kibor_data:
        kibor_rates = kibor_data.get("rates", [])
        kibor_date = kibor_data.get("date", today)
        for rate in kibor_rates:
            if not isinstance(rate, dict):
                continue
            tenor = rate.get("tenor")
            bid = rate.get("bid")
            offer = rate.get("offer")
            if tenor and bid is not None and offer is not None:
                con.execute(
                    """INSERT INTO kibor_daily (date, tenor, bid, offer)
                       VALUES (?, ?, ?, ?)
                       ON CONFLICT(date, tenor) DO UPDATE SET
                         bid=excluded.bid, offer=excluded.offer,
                         scraped_at=datetime('now')
                    """,
                    (kibor_date, tenor, bid, offer),
                )
                kibor_stored += 1

    con.commit()
    logger.info("FX sync: %d rates, %d KIBOR tenors stored for %s",
                rates_stored, kibor_stored, today)
    return {
        "status": "ok",
        "rates_stored": rates_stored,
        "kibor_stored": kibor_stored,
        "date": today,
    }


def backfill_fx_history(
    con: sqlite3.Connection,
    from_date: str = "2024-01-01",
    to_date: str | None = None,
) -> dict:
    """Backfill historical FX rates from FX microservice into local DB.

    Calls GET /rates/history on the FX service and INSERT OR IGNORE into
    sbp_fx_interbank (only fills gaps, never overwrites).

    Args:
        con: SQLite connection to PSX OHLCV database
        from_date: Start date (YYYY-MM-DD)
        to_date: End date (YYYY-MM-DD), defaults to today
    """
    if not _fx.is_healthy():
        return {"status": "error", "reason": "FX service unavailable"}

    if to_date is None:
        to_date = date.today().isoformat()

    # Use _get to call the history endpoint directly
    history = _fx._get("/rates/history", params={"from": from_date, "to": to_date})
    if not history or "rates" not in history:
        return {"status": "error", "reason": "No history returned from FX service"}

    inserted = 0
    skipped = 0
    for record in history["rates"]:
        pair = record.get("pair", "")
        if not pair.endswith("/PKR"):
            continue
        currency = pair.replace("/PKR", "")
        dt = record.get("date")
        buying = record.get("buying")
        selling = record.get("selling")
        if not dt or buying is None or selling is None:
            continue
        mid = round((buying + selling) / 2, 4)

        # INSERT OR IGNORE — only fills gaps, doesn't overwrite existing
        result = con.execute(
            """INSERT OR IGNORE INTO sbp_fx_interbank (date, currency, buying, selling, mid)
               VALUES (?, ?, ?, ?, ?)
            """,
            (dt, currency, buying, selling, mid),
        )
        if result.rowcount > 0:
            inserted += 1
        else:
            skipped += 1

    con.commit()
    logger.info("FX backfill: %d inserted, %d skipped (%s to %s)",
                inserted, skipped, from_date, to_date)
    return {
        "status": "ok",
        "inserted": inserted,
        "skipped": skipped,
        "from": from_date,
        "to": to_date,
    }
