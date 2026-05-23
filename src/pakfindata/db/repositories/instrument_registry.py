"""Unified Instrument Registry — one row per instrument across the platform.

Thin reference layer that points to operational tables. Supports:
- Universal search across all asset classes
- Auto-discovery from live sources (PSX, MUFAP, SBP)
- As-is lookup: "what instruments exist, what type, where's the data?"
"""

import sqlite3

import pandas as pd

from pakfindata.models import now_iso

# Sector → instrument_type mapping for PSX symbols
_SECTOR_TYPE_MAP = {
    "EXCHANGE TRADED FUNDS": ("EQUITY", "ETF"),
    "REAL ESTATE INVESTMENT TRUST": ("EQUITY", "REIT"),
    "MODARABAS": ("EQUITY", "MODARABA"),
    "LEASING COMPANIES": ("EQUITY", "LEASING"),
    "CLOSE - END MUTUAL FUNDS": ("FUND", "CLOSED_END_FUND"),
    "INVESTMENT BANKS / INVESTMENT COMPANIES / SECURITIES COMPANIES": ("EQUITY", "INVESTMENT_CO"),
}

# Bond type → instrument_type mapping
_BOND_TYPE_MAP = {
    "PIB": "PIB",
    "T-Bill": "TBILL",
    "Sukuk": "SUKUK",
    "TFC": "TFC",
    "Corporate": "CORP_BOND",
}

# Sukuk/FI category → instrument_type
_FI_CATEGORY_MAP = {
    "MTB": "TBILL",
    "PIB": "PIB",
    "GOP_SUKUK": "SUKUK",
    "CORP_BOND": "CORP_BOND",
    "CORP_SUKUK": "SUKUK",
}

# Fund type → instrument_type
_FUND_TYPE_MAP = {
    "OPEN_END": "MUTUAL_FUND",
    "VPS": "VPS",
    "EMPLOYER_PENSION": "PENSION",
    "DEDICATED": "MUTUAL_FUND",
    "ETF": "ETF",
}


def init_registry_schema(con: sqlite3.Connection) -> None:
    """Create instrument_registry table if it doesn't exist."""
    con.executescript("""
        CREATE TABLE IF NOT EXISTS instrument_registry (
            registry_id     TEXT PRIMARY KEY,
            symbol          TEXT NOT NULL,
            name            TEXT,
            asset_class     TEXT NOT NULL,
            instrument_type TEXT NOT NULL,
            source          TEXT NOT NULL,
            source_table    TEXT,
            source_id       TEXT,
            isin            TEXT,
            currency        TEXT DEFAULT 'PKR',
            sector          TEXT,
            is_active       INTEGER DEFAULT 1,
            discovered_at   TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_ir_asset_class ON instrument_registry(asset_class);
        CREATE INDEX IF NOT EXISTS idx_ir_type ON instrument_registry(instrument_type);
        CREATE INDEX IF NOT EXISTS idx_ir_source ON instrument_registry(source);
        CREATE INDEX IF NOT EXISTS idx_ir_symbol ON instrument_registry(symbol);
        CREATE INDEX IF NOT EXISTS idx_ir_active ON instrument_registry(is_active) WHERE is_active = 1;
    """)
    con.commit()


def _upsert_batch(con: sqlite3.Connection, entries: list[dict]) -> int:
    """Bulk upsert registry entries."""
    if not entries:
        return 0
    now = now_iso()
    count = 0
    for e in entries:
        con.execute(
            """INSERT INTO instrument_registry
               (registry_id, symbol, name, asset_class, instrument_type,
                source, source_table, source_id, isin, currency, sector,
                is_active, discovered_at, updated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(registry_id) DO UPDATE SET
                 name = COALESCE(excluded.name, instrument_registry.name),
                 asset_class = excluded.asset_class,
                 instrument_type = excluded.instrument_type,
                 source_table = excluded.source_table,
                 source_id = excluded.source_id,
                 isin = COALESCE(excluded.isin, instrument_registry.isin),
                 sector = COALESCE(excluded.sector, instrument_registry.sector),
                 is_active = excluded.is_active,
                 updated_at = excluded.updated_at""",
            (
                e["registry_id"], e["symbol"], e.get("name"),
                e["asset_class"], e["instrument_type"],
                e["source"], e.get("source_table"), e.get("source_id"),
                e.get("isin"), e.get("currency", "PKR"), e.get("sector"),
                e.get("is_active", 1), now, now,
            ),
        )
        count += 1
    con.commit()
    return count


# ---------------------------------------------------------------------------
# Per-source sync functions
# ---------------------------------------------------------------------------

def sync_registry_from_symbols(con: sqlite3.Connection) -> int:
    """Populate registry from symbols table (equities, ETFs, REITs, modarabas)."""
    rows = con.execute(
        "SELECT symbol, name, sector_name, is_active FROM symbols"
    ).fetchall()

    entries = []
    for r in rows:
        sym = r[0] if isinstance(r, tuple) else r["symbol"]
        name = r[1] if isinstance(r, tuple) else r["name"]
        sector = r[2] if isinstance(r, tuple) else r["sector_name"]
        active = r[3] if isinstance(r, tuple) else r["is_active"]

        asset_class, inst_type = _SECTOR_TYPE_MAP.get(sector or "", ("EQUITY", "STOCK"))

        entries.append({
            "registry_id": f"PSX:{sym}",
            "symbol": sym,
            "name": name,
            "asset_class": asset_class,
            "instrument_type": inst_type,
            "source": "PSX",
            "source_table": "symbols",
            "source_id": sym,
            "sector": sector,
            "is_active": active,
        })
    return _upsert_batch(con, entries)


def sync_registry_from_funds(con: sqlite3.Connection) -> int:
    """Populate registry from mutual_funds table."""
    rows = con.execute(
        "SELECT fund_id, fund_name, amc_name, fund_type, category, is_active "
        "FROM mutual_funds"
    ).fetchall()

    entries = []
    for r in rows:
        fund_id = r[0] if isinstance(r, tuple) else r["fund_id"]
        name = r[1] if isinstance(r, tuple) else r["fund_name"]
        amc = r[2] if isinstance(r, tuple) else r["amc_name"]
        fund_type = r[3] if isinstance(r, tuple) else r["fund_type"]
        category = r[4] if isinstance(r, tuple) else r["category"]
        active = r[5] if isinstance(r, tuple) else r["is_active"]

        inst_type = _FUND_TYPE_MAP.get(fund_type, "MUTUAL_FUND")
        # Extract display symbol from fund_id (MUFAP:ABL-ISF → ABL-ISF)
        symbol = fund_id.replace("MUFAP:", "") if fund_id.startswith("MUFAP:") else fund_id

        entries.append({
            "registry_id": fund_id if fund_id.startswith("MUFAP:") else f"MUFAP:{fund_id}",
            "symbol": symbol,
            "name": name,
            "asset_class": "FUND",
            "instrument_type": inst_type,
            "source": "MUFAP",
            "source_table": "mutual_funds",
            "source_id": fund_id,
            "sector": f"{amc} / {category}" if amc and category else (amc or category),
            "is_active": active,
        })
    return _upsert_batch(con, entries)


def sync_registry_from_bonds(con: sqlite3.Connection) -> int:
    """Populate registry from bonds_master table."""
    rows = con.execute(
        "SELECT bond_id, isin, symbol, issuer, bond_type, is_active FROM bonds_master"
    ).fetchall()

    entries = []
    for r in rows:
        bond_id = r[0] if isinstance(r, tuple) else r["bond_id"]
        isin = r[1] if isinstance(r, tuple) else r["isin"]
        symbol = r[2] if isinstance(r, tuple) else r["symbol"]
        issuer = r[3] if isinstance(r, tuple) else r["issuer"]
        bond_type = r[4] if isinstance(r, tuple) else r["bond_type"]
        active = r[5] if isinstance(r, tuple) else r["is_active"]

        inst_type = _BOND_TYPE_MAP.get(bond_type, "CORP_BOND")

        entries.append({
            "registry_id": f"BOND:{bond_id}",
            "symbol": symbol or bond_id,
            "name": f"{issuer} {bond_type} {symbol}" if issuer else symbol,
            "asset_class": "FIXED_INCOME",
            "instrument_type": inst_type,
            "source": "MANUAL",
            "source_table": "bonds_master",
            "source_id": bond_id,
            "isin": isin,
            "sector": issuer,
            "is_active": active,
        })
    return _upsert_batch(con, entries)


def sync_registry_from_sukuk(con: sqlite3.Connection) -> int:
    """Populate registry from sukuk_master table."""
    rows = con.execute(
        "SELECT instrument_id, issuer, name, category, is_active FROM sukuk_master"
    ).fetchall()

    entries = []
    for r in rows:
        inst_id = r[0] if isinstance(r, tuple) else r["instrument_id"]
        issuer = r[1] if isinstance(r, tuple) else r["issuer"]
        name = r[2] if isinstance(r, tuple) else r["name"]
        category = r[3] if isinstance(r, tuple) else r["category"]
        active = r[4] if isinstance(r, tuple) else r["is_active"]

        entries.append({
            "registry_id": f"SUKUK:{inst_id}",
            "symbol": inst_id,
            "name": name,
            "asset_class": "FIXED_INCOME",
            "instrument_type": "SUKUK",
            "source": "MANUAL",
            "source_table": "sukuk_master",
            "source_id": inst_id,
            "sector": f"{issuer} / {category}" if issuer else category,
            "is_active": active,
        })
    return _upsert_batch(con, entries)


def sync_registry_from_fi(con: sqlite3.Connection) -> int:
    """Populate registry from fi_instruments table."""
    rows = con.execute(
        "SELECT instrument_id, isin, name, category, is_active FROM fi_instruments"
    ).fetchall()

    entries = []
    for r in rows:
        inst_id = r[0] if isinstance(r, tuple) else r["instrument_id"]
        isin = r[1] if isinstance(r, tuple) else r["isin"]
        name = r[2] if isinstance(r, tuple) else r["name"]
        category = r[3] if isinstance(r, tuple) else r["category"]
        active = r[4] if isinstance(r, tuple) else r["is_active"]

        inst_type = _FI_CATEGORY_MAP.get(category, "CORP_BOND")

        entries.append({
            "registry_id": f"FI:{inst_id}",
            "symbol": inst_id,
            "name": name,
            "asset_class": "FIXED_INCOME",
            "instrument_type": inst_type,
            "source": "SBP",
            "source_table": "fi_instruments",
            "source_id": inst_id,
            "isin": isin,
            "sector": category,
            "is_active": active,
        })
    return _upsert_batch(con, entries)


def sync_registry_from_commodities(con: sqlite3.Connection) -> int:
    """Populate registry from commodity_symbols table."""
    rows = con.execute(
        "SELECT symbol, name, category, is_active FROM commodity_symbols"
    ).fetchall()

    entries = []
    for r in rows:
        sym = r[0] if isinstance(r, tuple) else r["symbol"]
        name = r[1] if isinstance(r, tuple) else r["name"]
        category = r[2] if isinstance(r, tuple) else r["category"]
        active = r[3] if isinstance(r, tuple) else r["is_active"]

        entries.append({
            "registry_id": f"COMMOD:{sym}",
            "symbol": sym,
            "name": name,
            "asset_class": "COMMODITY",
            "instrument_type": "COMMODITY",
            "source": "MANUAL",
            "source_table": "commodity_symbols",
            "source_id": sym,
            "sector": category,
            "is_active": active,
        })
    return _upsert_batch(con, entries)


def sync_registry_from_fx(con: sqlite3.Connection) -> int:
    """Populate registry from fx_pairs table."""
    rows = con.execute(
        "SELECT pair, description, source, is_active FROM fx_pairs"
    ).fetchall()

    entries = []
    for r in rows:
        pair = r[0] if isinstance(r, tuple) else r["pair"]
        desc = r[1] if isinstance(r, tuple) else r["description"]
        source = r[2] if isinstance(r, tuple) else r["source"]
        active = r[3] if isinstance(r, tuple) else r["is_active"]

        entries.append({
            "registry_id": f"FX:{pair}",
            "symbol": pair,
            "name": desc,
            "asset_class": "FX",
            "instrument_type": "FX_PAIR",
            "source": source or "SBP",
            "source_table": "fx_pairs",
            "source_id": pair,
            "is_active": active,
        })
    return _upsert_batch(con, entries)


def sync_registry_from_indices(con: sqlite3.Connection) -> int:
    """Populate registry from instruments table (INDEX type only)."""
    rows = con.execute(
        "SELECT instrument_id, symbol, name, is_active FROM instruments "
        "WHERE instrument_type = 'INDEX'"
    ).fetchall()

    entries = []
    for r in rows:
        inst_id = r[0] if isinstance(r, tuple) else r["instrument_id"]
        sym = r[1] if isinstance(r, tuple) else r["symbol"]
        name = r[2] if isinstance(r, tuple) else r["name"]
        active = r[3] if isinstance(r, tuple) else r["is_active"]

        entries.append({
            "registry_id": f"IDX:{sym}",
            "symbol": sym,
            "name": name,
            "asset_class": "INDEX",
            "instrument_type": "INDEX",
            "source": "PSX",
            "source_table": "instruments",
            "source_id": inst_id,
            "is_active": active,
        })
    return _upsert_batch(con, entries)


# ---------------------------------------------------------------------------
# Sync all
# ---------------------------------------------------------------------------

def sync_all(con: sqlite3.Connection) -> dict[str, int]:
    """Sync registry from all source tables.

    Returns dict mapping source name to rows upserted.
    """
    init_registry_schema(con)
    results = {}
    for name, fn in [
        ("symbols", sync_registry_from_symbols),
        ("funds", sync_registry_from_funds),
        ("bonds", sync_registry_from_bonds),
        ("sukuk", sync_registry_from_sukuk),
        ("fi", sync_registry_from_fi),
        ("commodities", sync_registry_from_commodities),
        ("fx", sync_registry_from_fx),
        ("indices", sync_registry_from_indices),
    ]:
        try:
            results[name] = fn(con)
        except Exception as e:
            results[name] = 0
            results[f"{name}_error"] = str(e)
    return results


# ---------------------------------------------------------------------------
# Query functions
# ---------------------------------------------------------------------------

def search_registry(
    con: sqlite3.Connection,
    query: str,
    asset_class: str | None = None,
    instrument_type: str | None = None,
    active_only: bool = True,
    limit: int = 50,
) -> pd.DataFrame:
    """Universal search across all instruments.

    Args:
        query: Search term (matches symbol, name, sector).
        asset_class: Filter by asset class (EQUITY, FUND, etc.).
        instrument_type: Filter by type (STOCK, ETF, MUTUAL_FUND, etc.).
        active_only: Only return active instruments.
        limit: Max results.
    """
    sql = """
        SELECT registry_id, symbol, name, asset_class, instrument_type,
               source, source_table, sector, is_active
        FROM instrument_registry
        WHERE (symbol LIKE ? OR name LIKE ? OR sector LIKE ?)
    """
    pattern = f"%{query}%"
    params: list = [pattern, pattern, pattern]

    if asset_class:
        sql += " AND asset_class = ?"
        params.append(asset_class)
    if instrument_type:
        sql += " AND instrument_type = ?"
        params.append(instrument_type)
    if active_only:
        sql += " AND is_active = 1"

    sql += f" ORDER BY symbol LIMIT {limit}"
    return pd.read_sql_query(sql, con, params=params)


def get_registry_stats(con: sqlite3.Connection) -> pd.DataFrame:
    """Get instrument counts by asset_class and instrument_type."""
    return pd.read_sql_query(
        """SELECT asset_class, instrument_type, COUNT(*) as count,
                  SUM(is_active) as active
           FROM instrument_registry
           GROUP BY asset_class, instrument_type
           ORDER BY asset_class, count DESC""",
        con,
    )


def get_instrument(con: sqlite3.Connection, registry_id: str) -> dict | None:
    """Lookup a single instrument by registry_id."""
    row = con.execute(
        "SELECT * FROM instrument_registry WHERE registry_id = ?",
        (registry_id,),
    ).fetchone()
    if not row:
        return None
    cols = [d[0] for d in con.execute(
        "PRAGMA table_info(instrument_registry)"
    ).description or con.execute(
        "SELECT * FROM instrument_registry LIMIT 0"
    ).description]
    if isinstance(row, tuple):
        cols = [c[1] for c in con.execute("PRAGMA table_info(instrument_registry)").fetchall()]
        return dict(zip(cols, row))
    return dict(row)
