"""
Instrument universe seeding for Phase 1.

This module handles seeding the instruments table with:
- Equities from existing symbols master
- ETFs, REITs, and Indexes from configuration file
"""

import json
import sqlite3
from pathlib import Path
from typing import Any

from ..config import DATA_ROOT
from ..db import (
    resolve_instrument_id,
    upsert_instruments_batch,
)

# Default config file location
UNIVERSE_CONFIG_PATH = DATA_ROOT / "universe_phase1.json"

# Default instruments if config file doesn't exist
DEFAULT_INSTRUMENTS = {
    "indexes": [
        {"symbol": "KSE100", "name": "KSE-100 Index", "source": "DPS"},
        {"symbol": "KSE30", "name": "KSE-30 Index", "source": "DPS"},
        {"symbol": "KMI30", "name": "KMI-30 Index", "source": "DPS"},
        {"symbol": "ALLSHR", "name": "All Share Index", "source": "DPS"},
    ],
    "etfs": [
        # Known PSX ETFs - add more as they are discovered
        {"symbol": "NIUETF", "name": "NIT Islamic Equity Fund", "source": "DPS"},
        {"symbol": "MIETF", "name": "Meezan Islamic ETF", "source": "DPS"},
    ],
    "reits": [
        # Known PSX REITs - add more as they are discovered
        {"symbol": "DCR", "name": "Dolmen City REIT", "source": "DPS"},
        {"symbol": "IGILREIT", "name": "IGIL REIT", "source": "DPS"},
    ],
}


def load_universe_config(config_path: Path | None = None) -> dict:
    """
    Load universe configuration from JSON file.

    Args:
        config_path: Path to config file, or None for default

    Returns:
        Config dict with 'indexes', 'etfs', 'reits' keys
    """
    path = config_path or UNIVERSE_CONFIG_PATH

    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    return DEFAULT_INSTRUMENTS


def save_universe_config(config: dict, config_path: Path | None = None) -> bool:
    """
    Save universe configuration to JSON file.

    Args:
        config: Config dict
        config_path: Path to save to, or None for default

    Returns:
        True if successful
    """
    path = config_path or UNIVERSE_CONFIG_PATH

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
        return True
    except IOError:
        return False


def create_default_config(config_path: Path | None = None) -> bool:
    """
    Create default config file if it doesn't exist.

    Args:
        config_path: Path to config file, or None for default

    Returns:
        True if created or already exists
    """
    path = config_path or UNIVERSE_CONFIG_PATH

    if path.exists():
        return True

    return save_universe_config(DEFAULT_INSTRUMENTS, path)


def seed_indexes(con: sqlite3.Connection, config: dict | None = None) -> dict:
    """
    Seed index instruments.

    Args:
        con: Database connection
        config: Config dict, or None to load from file

    Returns:
        Dict with 'inserted', 'updated', 'failed' counts
    """
    if config is None:
        config = load_universe_config()

    indexes = config.get("indexes", [])
    instruments = []

    for idx in indexes:
        instrument_id = resolve_instrument_id("IDX", idx["symbol"])
        instruments.append({
            "instrument_id": instrument_id,
            "symbol": idx["symbol"],
            "name": idx.get("name"),
            "instrument_type": "INDEX",
            "exchange": "PSX",
            "currency": "PKR",
            "source": idx.get("source", "MANUAL"),
            "is_active": 1,
        })

    return upsert_instruments_batch(con, instruments)


def seed_etfs(con: sqlite3.Connection, config: dict | None = None) -> dict:
    """
    Seed ETF instruments.

    Args:
        con: Database connection
        config: Config dict, or None to load from file

    Returns:
        Dict with 'inserted', 'updated', 'failed' counts
    """
    if config is None:
        config = load_universe_config()

    etfs = config.get("etfs", [])
    instruments = []

    for etf in etfs:
        instrument_id = resolve_instrument_id("PSX", etf["symbol"])
        instruments.append({
            "instrument_id": instrument_id,
            "symbol": etf["symbol"],
            "name": etf.get("name"),
            "instrument_type": "ETF",
            "exchange": "PSX",
            "currency": "PKR",
            "source": etf.get("source", "MANUAL"),
            "is_active": 1,
        })

    return upsert_instruments_batch(con, instruments)


def seed_reits(con: sqlite3.Connection, config: dict | None = None) -> dict:
    """
    Seed REIT instruments.

    Args:
        con: Database connection
        config: Config dict, or None to load from file

    Returns:
        Dict with 'inserted', 'updated', 'failed' counts
    """
    if config is None:
        config = load_universe_config()

    reits = config.get("reits", [])
    instruments = []

    for reit in reits:
        instrument_id = resolve_instrument_id("PSX", reit["symbol"])
        instruments.append({
            "instrument_id": instrument_id,
            "symbol": reit["symbol"],
            "name": reit.get("name"),
            "instrument_type": "REIT",
            "exchange": "PSX",
            "currency": "PKR",
            "source": reit.get("source", "MANUAL"),
            "is_active": 1,
        })

    return upsert_instruments_batch(con, instruments)


def seed_equities_from_symbols(con: sqlite3.Connection) -> dict:
    """
    Seed equity instruments from existing symbols table.

    This converts existing equity symbols to the new instruments table format,
    allowing unified querying across all instrument types.

    Args:
        con: Database connection

    Returns:
        Dict with 'inserted', 'updated', 'failed' counts
    """
    try:
        cur = con.execute("""
            SELECT symbol, name, sector_name
            FROM symbols
            WHERE is_active = 1
            ORDER BY symbol
        """)
        symbols = [dict(row) for row in cur.fetchall()]
    except Exception:
        return {"inserted": 0, "updated": 0, "failed": 0}

    instruments = []
    for sym in symbols:
        instrument_id = resolve_instrument_id("PSX", sym["symbol"])
        instruments.append({
            "instrument_id": instrument_id,
            "symbol": sym["symbol"],
            "name": sym.get("name"),
            "instrument_type": "EQUITY",
            "exchange": "PSX",
            "currency": "PKR",
            "source": "DPS",
            "is_active": 1,
        })

    return upsert_instruments_batch(con, instruments)


def seed_universe(
    con: sqlite3.Connection,
    include_equities: bool = True,
    config_path: Path | None = None,
) -> dict:
    """
    Seed the complete instrument universe.

    Args:
        con: Database connection
        include_equities: If True, also seed equities from symbols table
        config_path: Path to config file, or None for default

    Returns:
        Summary dict with counts by type
    """
    # Ensure config file exists
    create_default_config(config_path)

    config = load_universe_config(config_path)

    results = {
        "indexes": seed_indexes(con, config),
        "etfs": seed_etfs(con, config),
        "reits": seed_reits(con, config),
    }

    if include_equities:
        results["equities"] = seed_equities_from_symbols(con)

    # Calculate totals
    totals = {"inserted": 0, "updated": 0, "failed": 0}
    for counts in results.values():
        totals["inserted"] += counts.get("inserted", 0)
        totals["updated"] += counts.get("updated", 0)
        totals["failed"] += counts.get("failed", 0)

    results["totals"] = totals
    return results


def add_instrument_to_config(
    symbol: str,
    name: str,
    instrument_type: str,
    source: str = "DPS",
    config_path: Path | None = None,
) -> bool:
    """
    Add an instrument to the config file.

    Args:
        symbol: Instrument symbol
        name: Instrument name
        instrument_type: 'etf', 'reit', or 'index'
        source: Data source
        config_path: Path to config file

    Returns:
        True if successful
    """
    config = load_universe_config(config_path)

    type_key = f"{instrument_type.lower()}s"  # 'etf' -> 'etfs'
    if type_key not in config:
        if instrument_type.upper() == "INDEX":
            type_key = "indexes"
        else:
            config[type_key] = []

    # Check for duplicate
    existing = [i["symbol"] for i in config.get(type_key, [])]
    if symbol in existing:
        return False

    config[type_key].append({
        "symbol": symbol,
        "name": name,
        "source": source,
    })

    return save_universe_config(config, config_path)


def remove_instrument_from_config(
    symbol: str,
    instrument_type: str,
    config_path: Path | None = None,
) -> bool:
    """
    Remove an instrument from the config file.

    Args:
        symbol: Instrument symbol
        instrument_type: 'etf', 'reit', or 'index'
        config_path: Path to config file

    Returns:
        True if successful
    """
    config = load_universe_config(config_path)

    type_key = f"{instrument_type.lower()}s"
    if instrument_type.upper() == "INDEX":
        type_key = "indexes"

    if type_key not in config:
        return False

    original_len = len(config[type_key])
    config[type_key] = [i for i in config[type_key] if i["symbol"] != symbol]

    if len(config[type_key]) == original_len:
        return False

    return save_universe_config(config, config_path)
