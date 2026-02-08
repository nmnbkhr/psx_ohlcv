"""
Instrument management functions for Phase 1.

This module provides high-level functions for managing the instruments universe,
including equities, ETFs, REITs, and indexes.
"""

import sqlite3
from typing import Literal

from .db import (
    get_instrument_by_id,
    get_instrument_by_symbol,
    get_instruments,
    resolve_instrument_id,
    upsert_instrument,
)

InstrumentType = Literal["EQUITY", "ETF", "REIT", "INDEX"]

# Centralized constants to avoid hardcoding across the codebase
NON_EQUITY_TYPES: list[str] = ["ETF", "REIT", "INDEX"]
ALL_INSTRUMENT_TYPES: list[str] = ["EQUITY", "ETF", "REIT", "INDEX"]
DEFAULT_BENCHMARK_ID = "IDX:KSE100"

# ETF to tracking index mapping
# Each ETF tracks a specific index for benchmarking
ETF_INDEX_MAPPING: dict[str, str] = {
    "ACIETF": "ACI",       # Alfalah Consumer Index
    "HBLTETF": "HBLTTI",   # HBL Total Treasury Index
    "JSGBETF": "JSGBKTI",  # JS Global Banking Index
    "JSMFETF": "JSMFI",    # JS Momentum Factor Index
    "MIIETF": "MII30",     # Meezan Islamic Index 30
    "MZNPETF": "MZNPI",    # Meezan Pakistan Index
    "NBPGETF": "NBPPGI",   # NBP Pakistan Growth Index
    "NITGETF": "NITPGI",   # NIT Pakistan Gateway Index
    "UBLPETF": "KSE100",   # No specific index, uses KSE-100 as benchmark
}


def get_etf_benchmark(etf_symbol: str) -> str:
    """
    Get the benchmark index for an ETF.

    Args:
        etf_symbol: ETF symbol (e.g., "ACIETF")

    Returns:
        Index symbol that the ETF tracks
    """
    return ETF_INDEX_MAPPING.get(etf_symbol, "KSE100")


def get_instruments_by_type(
    con: sqlite3.Connection,
    instrument_type: InstrumentType,
    active_only: bool = True,
) -> list[dict]:
    """
    Get all instruments of a specific type.

    Args:
        con: Database connection
        instrument_type: One of 'EQUITY', 'ETF', 'REIT', 'INDEX'
        active_only: If True, only return active instruments

    Returns:
        List of instrument dicts
    """
    return get_instruments(con, instrument_type=instrument_type, active_only=active_only)


def get_all_instruments(con: sqlite3.Connection, active_only: bool = True) -> list[dict]:
    """
    Get all instruments across all types.

    Args:
        con: Database connection
        active_only: If True, only return active instruments

    Returns:
        List of instrument dicts
    """
    return get_instruments(con, instrument_type=None, active_only=active_only)


def add_instrument(
    con: sqlite3.Connection,
    symbol: str,
    instrument_type: InstrumentType,
    name: str | None = None,
    exchange: str = "PSX",
    currency: str = "PKR",
    source: str = "MANUAL",
    is_active: bool = True,
) -> str | None:
    """
    Add a new instrument to the universe.

    Args:
        con: Database connection
        symbol: Instrument symbol
        instrument_type: Type of instrument
        name: Full name (optional)
        exchange: Exchange code (default: PSX)
        currency: Currency code (default: PKR)
        source: Data source (default: MANUAL)
        is_active: Whether instrument is active

    Returns:
        instrument_id if successful, None otherwise
    """
    # For indexes, use IDX prefix
    if instrument_type == "INDEX":
        instrument_id = resolve_instrument_id("IDX", symbol)
    else:
        instrument_id = resolve_instrument_id(exchange, symbol)

    instrument = {
        "instrument_id": instrument_id,
        "symbol": symbol,
        "name": name,
        "instrument_type": instrument_type,
        "exchange": exchange,
        "currency": currency,
        "source": source,
        "is_active": 1 if is_active else 0,
    }

    if upsert_instrument(con, instrument):
        return instrument_id
    return None


def deactivate_instrument(con: sqlite3.Connection, instrument_id: str) -> bool:
    """
    Mark an instrument as inactive.

    Args:
        con: Database connection
        instrument_id: Instrument ID to deactivate

    Returns:
        True if successful
    """
    try:
        con.execute(
            "UPDATE instruments SET is_active = 0, updated_at = datetime('now') WHERE instrument_id = ?",
            (instrument_id,),
        )
        con.commit()
        return True
    except Exception:
        return False


def activate_instrument(con: sqlite3.Connection, instrument_id: str) -> bool:
    """
    Mark an instrument as active.

    Args:
        con: Database connection
        instrument_id: Instrument ID to activate

    Returns:
        True if successful
    """
    try:
        con.execute(
            "UPDATE instruments SET is_active = 1, updated_at = datetime('now') WHERE instrument_id = ?",
            (instrument_id,),
        )
        con.commit()
        return True
    except Exception:
        return False


def get_instrument_symbols_string(
    con: sqlite3.Connection,
    instrument_type: InstrumentType | None = None,
    separator: str = ",",
) -> str:
    """
    Get comma-separated string of instrument symbols.

    Args:
        con: Database connection
        instrument_type: Filter by type, or None for all
        separator: Separator between symbols

    Returns:
        String of symbols
    """
    instruments = get_instruments(con, instrument_type=instrument_type, active_only=True)
    return separator.join(inst["symbol"] for inst in instruments)


def get_instrument_counts(con: sqlite3.Connection) -> dict:
    """
    Get counts of instruments by type.

    Args:
        con: Database connection

    Returns:
        Dict with type -> count mapping
    """
    try:
        cur = con.execute("""
            SELECT instrument_type, COUNT(*) as count
            FROM instruments
            WHERE is_active = 1
            GROUP BY instrument_type
            ORDER BY instrument_type
        """)
        return {row["instrument_type"]: row["count"] for row in cur.fetchall()}
    except Exception:
        return {}


def find_instrument(
    con: sqlite3.Connection,
    symbol: str,
    exchange: str = "PSX",
) -> dict | None:
    """
    Find an instrument by symbol and exchange.

    Also checks for INDEX prefix if not found in given exchange.

    Args:
        con: Database connection
        symbol: Instrument symbol
        exchange: Exchange code

    Returns:
        Instrument dict or None
    """
    # Try the specified exchange first
    inst = get_instrument_by_symbol(con, symbol, exchange)
    if inst:
        return inst

    # Try INDEX prefix
    inst = get_instrument_by_symbol(con, symbol, "IDX")
    if inst:
        return inst

    # Try direct ID lookup
    for prefix in [exchange, "IDX"]:
        inst_id = resolve_instrument_id(prefix, symbol)
        inst = get_instrument_by_id(con, inst_id)
        if inst:
            return inst

    return None
