"""
Instrument EOD sync for Phase 1.

This module handles syncing OHLCV data for non-equity instruments (ETFs, REITs, Indexes)
using the existing DPS EOD endpoint.
"""

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .config import get_db_path
from .instruments import NON_EQUITY_TYPES
from .db import (
    connect,
    create_instruments_sync_run,
    get_instrument_latest_date,
    get_instruments,
    init_schema,
    update_instruments_sync_run,
    upsert_ohlcv_instrument,
)
from .http import create_session
from .sources.eod import fetch_eod_json, filter_incremental, parse_eod_payload


@dataclass
class InstrumentSyncSummary:
    """Summary of instrument sync operation."""

    total: int = 0
    ok: int = 0
    failed: int = 0
    no_data: int = 0
    rows_upserted: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "ok": self.ok,
            "failed": self.failed,
            "no_data": self.no_data,
            "rows": self.rows_upserted,
        }


def sync_instrument_eod(
    con,
    instrument: dict,
    session=None,
    incremental: bool = True,
) -> tuple[int, str | None]:
    """
    Sync EOD data for a single instrument.

    Args:
        con: Database connection
        instrument: Instrument dict with instrument_id, symbol
        session: Requests session (optional)
        incremental: If True, only fetch data newer than existing

    Returns:
        Tuple of (rows_upserted, error_message)
        error_message is None on success, 'no_data' if no data available
    """
    instrument_id = instrument["instrument_id"]
    symbol = instrument["symbol"]

    if session is None:
        session = create_session()

    try:
        # Fetch EOD JSON
        payload = fetch_eod_json(symbol, session)

        # Parse into DataFrame
        df = parse_eod_payload(symbol, payload)

        if df.empty:
            return 0, "no_data"

        # Apply incremental filter
        if incremental:
            max_date = get_instrument_latest_date(con, instrument_id)
            if max_date:
                df = filter_incremental(df, max_date)

        if df.empty:
            return 0, None  # Success, but no new data

        # Upsert to database
        rows = upsert_ohlcv_instrument(con, instrument_id, df)
        return rows, None

    except Exception as e:
        return 0, str(e)


def sync_instruments_eod(
    db_path: Path | str | None = None,
    instrument_types: list[str] | None = None,
    incremental: bool = True,
    limit: int | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> InstrumentSyncSummary:
    """
    Sync EOD data for multiple instruments.

    Args:
        db_path: Path to database
        instrument_types: List of types to sync (e.g., ['ETF', 'REIT', 'INDEX']),
                         or None for all non-equity types
        incremental: If True, only fetch data newer than existing
        limit: Max number of instruments to sync (for testing)
        progress_callback: Callback(current, total, symbol) for progress updates

    Returns:
        InstrumentSyncSummary with counts and errors
    """
    if instrument_types is None:
        instrument_types = NON_EQUITY_TYPES

    summary = InstrumentSyncSummary()

    # Connect to database
    con = connect(db_path or get_db_path())
    init_schema(con)

    # Create sync run
    run_id = str(uuid.uuid4())[:8]
    types_str = ",".join(instrument_types)
    create_instruments_sync_run(con, run_id, types_str)

    # Get instruments to sync
    instruments = []
    for inst_type in instrument_types:
        instruments.extend(get_instruments(con, instrument_type=inst_type, active_only=True))

    if limit:
        instruments = instruments[:limit]

    summary.total = len(instruments)

    # Create session for connection pooling
    session = create_session()

    # Sync each instrument
    for i, instrument in enumerate(instruments):
        symbol = instrument["symbol"]

        if progress_callback:
            progress_callback(i + 1, summary.total, symbol)

        rows, error = sync_instrument_eod(
            con, instrument, session, incremental=incremental
        )

        if error is None:
            summary.ok += 1
            summary.rows_upserted += rows
        elif error == "no_data":
            summary.no_data += 1
        else:
            summary.failed += 1
            summary.errors.append((symbol, error))

    # Update sync run
    update_instruments_sync_run(con, run_id, summary.to_dict())

    con.close()
    return summary


def sync_single_instrument(
    symbol: str,
    db_path: Path | str | None = None,
    incremental: bool = True,
) -> tuple[int, str | None]:
    """
    Sync EOD data for a single instrument by symbol.

    Args:
        symbol: Instrument symbol
        db_path: Path to database
        incremental: If True, only fetch data newer than existing

    Returns:
        Tuple of (rows_upserted, error_message)
    """
    con = connect(db_path or get_db_path())
    init_schema(con)

    # Find instrument
    cur = con.execute(
        "SELECT * FROM instruments WHERE symbol = ?",
        (symbol,)
    )
    row = cur.fetchone()

    if not row:
        return 0, f"Instrument not found: {symbol}"

    instrument = dict(row)
    rows, error = sync_instrument_eod(con, instrument, incremental=incremental)

    con.close()
    return rows, error


def get_sync_status(db_path: Path | str | None = None) -> list[dict]:
    """
    Get recent sync runs for instruments.

    Args:
        db_path: Path to database

    Returns:
        List of sync run dicts
    """
    con = connect(db_path or get_db_path())

    try:
        cur = con.execute("""
            SELECT *
            FROM instruments_sync_runs
            ORDER BY started_at DESC
            LIMIT 10
        """)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []
    finally:
        con.close()
