"""Symbol/ticker repository — CRUD operations for stock symbols."""

import re
import sqlite3

import pandas as pd

from pakfindata.models import now_iso

# PSX status suffixes that get concatenated to symbols on market watch pages.
# XD=ex-dividend, XB=ex-bonus, XR=ex-rights, XA=ex-AGM, XI=ex-interim,
# XW=ex-warrant, NC=non-clearing/new counter, O=odd lot
_STATUS_SUFFIXES = ("XD", "XB", "XR", "XA", "XI", "XW", "NC")

# Symbols ending with these suffixes are separate winding-up counters that
# PSX does not serve company pages for (HTTP 500).  Skip them during scraping.
_WINDING_UP_SUFFIX = "WU"
# Longest first so "XD" is checked before just "D"
_SUFFIX_RE = re.compile(r"^(.+?)(" + "|".join(_STATUS_SUFFIXES) + r")$")


def upsert_symbols(con: sqlite3.Connection, symbols: list[dict]) -> int:
    """
    Upsert symbols into the symbols table.

    Args:
        con: Database connection
        symbols: List of dicts with keys: symbol, name (optional), sector (optional)

    Returns:
        Number of rows inserted or updated
    """
    if not symbols:
        return 0

    now = now_iso()
    count = 0

    for sym in symbols:
        symbol = sym.get("symbol")
        if not symbol:
            continue

        name = sym.get("name")
        sector = sym.get("sector")

        # Try insert, on conflict update
        cur = con.execute(
            """
            INSERT INTO symbols
                (symbol, name, sector, is_active, discovered_at, updated_at)
            VALUES (?, ?, ?, 1, ?, ?)
            ON CONFLICT(symbol) DO UPDATE SET
                name = COALESCE(excluded.name, symbols.name),
                sector = COALESCE(excluded.sector, symbols.sector),
                is_active = 1,
                updated_at = excluded.updated_at
            """,
            (symbol, name, sector, now, now),
        )
        count += cur.rowcount

    con.commit()
    return count


def get_symbols_list(con: sqlite3.Connection, limit: int | None = None) -> list[str]:
    """
    Get list of active symbols in sorted order.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols

    Returns:
        List of symbol strings, sorted alphabetically
    """
    query = "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
    if limit is not None:
        query += f" LIMIT {int(limit)}"

    cur = con.execute(query)
    return [row["symbol"] for row in cur.fetchall()]


def get_symbols_string(con: sqlite3.Connection, limit: int | None = None) -> str:
    """
    Get comma-separated string of active symbols.

    Args:
        con: Database connection
        limit: Optional limit on number of symbols

    Returns:
        Comma-separated string of symbols, sorted alphabetically
    """
    symbols = get_symbols_list(con, limit)
    return ",".join(symbols)


def normalize_symbol(
    symbol: str,
    master_symbols: set[str] | None = None,
) -> tuple[str, str | None]:
    """Strip PSX status suffixes from a symbol.

    Handles concatenated suffixes like AMTEXNC → AMTEX, HBLXD → HBL.
    Only strips if the resulting base symbol is in the master set (when
    provided), preventing false positives like TEXNC being wrongly
    stripped to TEX.

    Args:
        symbol: Raw symbol string (e.g. "NBPXD", "AMTEXNC").
        master_symbols: Set of known canonical symbols for validation.
            If None, strips the suffix unconditionally.

    Returns:
        (base_symbol, suffix) — suffix is None if no suffix found.
    """
    symbol = symbol.strip().upper()

    m = _SUFFIX_RE.match(symbol)
    if not m:
        return symbol, None

    base, suffix = m.group(1), m.group(2)

    if master_symbols is None:
        # No master list → strip unconditionally
        return base, suffix

    if base in master_symbols:
        return base, suffix

    # Base not in master — keep the original symbol as-is
    return symbol, None


def get_scrapable_symbols(con: sqlite3.Connection) -> list[str]:
    """Get deduplicated list of base symbols suitable for scraping.

    Reads all active symbols, normalises suffixed variants (XD, XB, NC …)
    back to their base symbol using the master list as a reference, and
    returns a sorted, deduplicated list.

    Returns:
        Sorted list of unique base symbols.
    """
    all_active = get_symbols_list(con)
    # Build master set from the listed companies source (canonical names)
    master_rows = con.execute(
        "SELECT symbol FROM symbols WHERE source = 'LISTED_CMP' AND is_active = 1"
    ).fetchall()
    master_set = {r["symbol"] for r in master_rows}

    # Fall back to all active if no LISTED_CMP entries yet
    if not master_set:
        master_set = set(all_active)

    seen: set[str] = set()
    result: list[str] = []

    for sym in all_active:
        base, _ = normalize_symbol(sym, master_set)
        # Skip winding-up counters — PSX returns HTTP 500 for these
        if base.endswith(_WINDING_UP_SUFFIX):
            continue
        if base not in seen:
            seen.add(base)
            result.append(base)

    result.sort()
    return result


# ---------------------------------------------------------------------------
# SCD2 — Symbol Status History
#
# Tracks every status transition for each base symbol. A record is only
# written when status CHANGES — not on every refresh. This enables both
# as-is (current status) and as-was (status at any past date) queries.
#
# Statuses: NORMAL, XD, XB, XR, XA, XI, XW, NC, WU
# ---------------------------------------------------------------------------

# Default status definitions — seeded into DB on init, but DB is the source of truth.
_DEFAULT_STATUSES = {
    "NORMAL": ("Normal Trading", "Stock is trading normally with no corporate action pending"),
    "XD": ("Ex-Dividend", "Trading without entitlement to declared cash dividend"),
    "XB": ("Ex-Bonus", "Trading without entitlement to declared bonus shares"),
    "XR": ("Ex-Rights", "Trading without entitlement to declared rights issue"),
    "XA": ("Ex-AGM", "Post-AGM, before corporate action benefits applied"),
    "XI": ("Ex-Interim", "Trading without entitlement to declared interim dividend"),
    "XW": ("Ex-Warrant", "Trading without entitlement to declared warrants"),
    "NC": ("Non-Clearing", "New counter during corporate action settlement period"),
    "WU": ("Winding-Up", "Company is being wound up / delisted — permanent status"),
}

# Computed from defaults; refreshed from DB by get_status_definitions()
_ALL_STATUS_SUFFIXES = tuple(k for k in _DEFAULT_STATUSES if k != "NORMAL")


def init_status_history_schema(con: sqlite3.Connection) -> None:
    """Create symbol status tables and seed default status definitions.

    Caller commits via pakfindata.db.safe_writer.

    Uses individual con.execute() calls rather than con.executescript() —
    executescript() implicitly commits any pending transaction first, which
    would end a safe_writer BEGIN IMMEDIATE transaction prematurely.
    """
    con.execute("""
        CREATE TABLE IF NOT EXISTS symbol_status_definitions (
            status          TEXT PRIMARY KEY,
            label           TEXT NOT NULL,
            description     TEXT,
            is_suffix       INTEGER DEFAULT 1,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS symbol_status_history (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL,
            status          TEXT NOT NULL,
            start_date      TEXT NOT NULL,
            end_date        TEXT,
            is_current      INTEGER DEFAULT 1,
            source_symbol   TEXT NOT NULL,
            created_at      TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at      TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(symbol, status, start_date)
        )
    """)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_ssh_symbol "
        "ON symbol_status_history(symbol)"
    )
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_ssh_current "
        "ON symbol_status_history(is_current) WHERE is_current = 1"
    )

    # Seed defaults (INSERT OR IGNORE so user edits are preserved)
    for status, (label, desc) in _DEFAULT_STATUSES.items():
        con.execute(
            """INSERT OR IGNORE INTO symbol_status_definitions
               (status, label, description, is_suffix)
               VALUES (?, ?, ?, ?)""",
            (status, label, desc, 0 if status == "NORMAL" else 1),
        )


def get_status_definitions(con: sqlite3.Connection) -> dict[str, dict]:
    """Get all status definitions from DB.

    Returns:
        Dict mapping status code to {label, description, is_suffix}.
    """
    rows = con.execute(
        "SELECT status, label, description, is_suffix FROM symbol_status_definitions ORDER BY status"
    ).fetchall()
    return {
        r[0]: {"label": r[1], "description": r[2], "is_suffix": bool(r[3])}
        for r in rows
    }


def upsert_status_definition(
    con: sqlite3.Connection,
    status: str,
    label: str,
    description: str = "",
    is_suffix: bool = True,
) -> None:
    """Add or update a status definition.

    Args:
        status: Status code (e.g. "XD", "PP").
        label: Short label (e.g. "Ex-Dividend").
        description: Longer explanation.
        is_suffix: True if this appears as a symbol suffix on market watch.
    """
    now = now_iso()
    con.execute(
        """INSERT INTO symbol_status_definitions (status, label, description, is_suffix, created_at, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(status) DO UPDATE SET
             label = excluded.label,
             description = excluded.description,
             is_suffix = excluded.is_suffix,
             updated_at = excluded.updated_at""",
        (status.upper(), label, description, int(is_suffix), now, now),
    )
    con.commit()


def _get_status_label(con: sqlite3.Connection, status: str) -> str:
    """Get label for a status code, falling back to the code itself."""
    row = con.execute(
        "SELECT label FROM symbol_status_definitions WHERE status = ?", (status,)
    ).fetchone()
    if row:
        return row[0]
    # Fallback to hardcoded defaults
    default = _DEFAULT_STATUSES.get(status)
    return default[0] if default else status


def _get_known_suffixes(con: sqlite3.Connection) -> set[str]:
    """Get all known suffix codes from DB definitions."""
    rows = con.execute(
        "SELECT status FROM symbol_status_definitions WHERE is_suffix = 1"
    ).fetchall()
    result = {r[0] for r in rows}
    # Always include hardcoded ones as fallback
    result.update(_ALL_STATUS_SUFFIXES)
    return result


def refresh_symbol_status(
    con: sqlite3.Connection,
    today: str | None = None,
) -> dict:
    """Detect status CHANGES and update SCD2 history.

    Caller commits via pakfindata.db.safe_writer.

    Only writes when a symbol's status actually changes:
      - NBP was NORMAL, now NBPXD appears → close NORMAL row, open XD row
      - NBPXD disappears, NBP still active → close XD row, open NORMAL row
      - NBP still NORMAL, no change → no write

    Every base symbol always has exactly one is_current=1 row.

    Args:
        con: Database connection.
        today: Date string (YYYY-MM-DD). Defaults to now.

    Returns:
        Dict with opened, closed, unchanged counts.
    """
    from datetime import datetime as _dt

    if today is None:
        today = _dt.now().strftime("%Y-%m-%d")

    init_status_history_schema(con)
    now = now_iso()

    # Build master set for normalization
    master_rows = con.execute(
        "SELECT symbol FROM symbols WHERE source = 'LISTED_CMP' AND is_active = 1"
    ).fetchall()
    master_set = {r[0] if isinstance(r, tuple) else r["symbol"] for r in master_rows}
    if not master_set:
        all_rows = con.execute(
            "SELECT symbol FROM symbols WHERE is_active = 1"
        ).fetchall()
        master_set = {r[0] if isinstance(r, tuple) else r["symbol"] for r in all_rows}

    # Get known suffix codes from DB definitions
    known_suffixes = _get_known_suffixes(con)

    # Determine current status for each base symbol
    all_active = con.execute(
        "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
    ).fetchall()

    # base_symbol → (status, source_symbol)
    observed: dict[str, tuple[str, str]] = {}
    for row in all_active:
        sym = row[0] if isinstance(row, tuple) else row["symbol"]
        base, suffix = normalize_symbol(sym, master_set)
        if suffix and suffix in known_suffixes:
            # Suffixed symbol — record the status (overrides NORMAL)
            observed[base] = (suffix, sym)
        elif base not in observed:
            # Base symbol with no suffix seen yet → NORMAL
            observed[base] = ("NORMAL", base)

    # Get all currently open SCD2 rows: base → (id, status)
    open_rows = con.execute(
        "SELECT id, symbol, status FROM symbol_status_history WHERE is_current = 1"
    ).fetchall()
    current_open: dict[str, tuple[int, str]] = {}
    for row in open_rows:
        rid = row[0] if isinstance(row, tuple) else row["id"]
        sym = row[1] if isinstance(row, tuple) else row["symbol"]
        st = row[2] if isinstance(row, tuple) else row["status"]
        current_open[sym] = (rid, st)

    result = {"opened": 0, "closed": 0, "unchanged": 0}

    for base, (new_status, source_sym) in observed.items():
        if base in current_open:
            old_id, old_status = current_open.pop(base)
            if old_status == new_status:
                # No change — do nothing
                result["unchanged"] += 1
                continue
            # Status CHANGED — close old, open new
            con.execute(
                "UPDATE symbol_status_history "
                "SET end_date = ?, is_current = 0, updated_at = ? "
                "WHERE id = ?",
                (today, now, old_id),
            )
            result["closed"] += 1

        # Open new row (either first time or after change)
        con.execute(
            """INSERT OR IGNORE INTO symbol_status_history
               (symbol, status, start_date, end_date, is_current,
                source_symbol, created_at, updated_at)
               VALUES (?, ?, ?, NULL, 1, ?, ?, ?)""",
            (base, new_status, today, source_sym, now, now),
        )
        result["opened"] += 1

    # Close rows for symbols no longer active at all
    for base, (old_id, _) in current_open.items():
        con.execute(
            "UPDATE symbol_status_history "
            "SET end_date = ?, is_current = 0, updated_at = ? "
            "WHERE id = ?",
            (today, now, old_id),
        )
        result["closed"] += 1

    return result


def get_symbol_status_at(
    con: sqlite3.Connection,
    symbol: str,
    as_of: str | None = None,
) -> dict | None:
    """As-was query: what was the symbol's status on a given date?

    Args:
        symbol: Base symbol (e.g. "HBL").
        as_of: Date (YYYY-MM-DD). None = current (as-is).

    Returns:
        Dict with status, label, start_date, end_date or None.
    """
    symbol = symbol.upper()
    if as_of is None:
        # As-is: current row
        row = con.execute(
            """SELECT status, start_date, end_date, source_symbol
               FROM symbol_status_history
               WHERE symbol = ? AND is_current = 1
               LIMIT 1""",
            (symbol,),
        ).fetchone()
    else:
        # As-was: find row where start_date <= as_of AND (end_date >= as_of OR end_date IS NULL)
        row = con.execute(
            """SELECT status, start_date, end_date, source_symbol
               FROM symbol_status_history
               WHERE symbol = ?
                 AND start_date <= ?
                 AND (end_date >= ? OR end_date IS NULL)
               ORDER BY start_date DESC
               LIMIT 1""",
            (symbol, as_of, as_of),
        ).fetchone()

    if not row:
        return None

    status = row[0] if isinstance(row, tuple) else row["status"]
    return {
        "status": status,
        "label": _get_status_label(con, status),
        "start_date": row[1] if isinstance(row, tuple) else row["start_date"],
        "end_date": row[2] if isinstance(row, tuple) else row["end_date"],
        "source_symbol": row[3] if isinstance(row, tuple) else row["source_symbol"],
    }


def get_symbol_current_status(
    con: sqlite3.Connection,
    symbol: str,
) -> list[dict]:
    """Get all currently active statuses for a symbol (as-is).

    Returns list of dicts with: status, label, description, start_date, end_date, source_symbol.
    """
    defs = get_status_definitions(con)
    rows = con.execute(
        """SELECT status, start_date, end_date, source_symbol
           FROM symbol_status_history
           WHERE symbol = ? AND is_current = 1
           ORDER BY start_date DESC""",
        (symbol.upper(),),
    ).fetchall()
    result = []
    for r in rows:
        st = r[0] if isinstance(r, tuple) else r["status"]
        d = defs.get(st, {})
        result.append({
            "status": st,
            "label": d.get("label", st),
            "description": d.get("description", ""),
            "start_date": r[1] if isinstance(r, tuple) else r["start_date"],
            "end_date": r[2] if isinstance(r, tuple) else r["end_date"],
            "source_symbol": r[3] if isinstance(r, tuple) else r["source_symbol"],
        })
    return result


def get_symbol_status_history(
    con: sqlite3.Connection,
    symbol: str | None = None,
    status: str | None = None,
    current_only: bool = False,
) -> pd.DataFrame:
    """Get symbol status history with optional filters.

    Args:
        symbol: Filter by base symbol (e.g. "HBL").
        status: Filter by status (e.g. "XD").
        current_only: Only return currently active statuses.

    Returns:
        DataFrame with full SCD2 history.
    """
    query = "SELECT * FROM symbol_status_history WHERE 1=1"
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())
    if status:
        query += " AND status = ?"
        params.append(status.upper())
    if current_only:
        query += " AND is_current = 1"

    query += " ORDER BY symbol, start_date DESC"
    return pd.read_sql_query(query, con, params=params)


def get_unified_symbols_list(con: sqlite3.Connection) -> list[str]:
    """Get list of symbols available in Deep Data tables."""
    cur = con.execute(
        "SELECT DISTINCT symbol FROM company_snapshots ORDER BY symbol"
    )
    return [row[0] for row in cur.fetchall()]


def get_unified_symbol_count(con: sqlite3.Connection) -> int:
    """Get count of symbols in Deep Data tables."""
    cur = con.execute("SELECT COUNT(DISTINCT symbol) FROM company_snapshots")
    return cur.fetchone()[0]


def get_sector_map(con: sqlite3.Connection) -> dict[str, str]:
    """
    Get a mapping of sector_code -> sector_name.

    Args:
        con: Database connection

    Returns:
        Dict mapping sector codes to sector names
    """
    cur = con.execute("SELECT sector_code, sector_name FROM sectors")
    return {row["sector_code"]: row["sector_name"] for row in cur.fetchall()}


def get_sector_name(con: sqlite3.Connection, sector_code: str) -> str | None:
    """
    Get sector name for a given sector code.

    Args:
        con: Database connection
        sector_code: Sector code (e.g., '0101')

    Returns:
        Sector name or None if not found
    """
    cur = con.execute(
        "SELECT sector_name FROM sectors WHERE sector_code = ?",
        (sector_code,),
    )
    row = cur.fetchone()
    return row["sector_name"] if row else None


def get_sectors(con: sqlite3.Connection) -> pd.DataFrame:
    """
    Get all sectors from the database.

    Args:
        con: Database connection

    Returns:
        DataFrame with columns: sector_code, sector_name, updated_at, source
    """
    query = """
        SELECT sector_code, sector_name, updated_at, source
        FROM sectors
        ORDER BY sector_code
    """
    return pd.read_sql_query(query, con)


def upsert_sectors(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """
    Upsert sectors data from DataFrame.

    Args:
        con: Database connection
        df: DataFrame with columns: sector_code, sector_name

    Returns:
        Number of rows inserted or updated
    """
    if df.empty:
        return 0

    now = now_iso()
    count = 0

    required_cols = {"sector_code", "sector_name"}
    if not required_cols.issubset(df.columns):
        missing = required_cols - set(df.columns)
        raise ValueError(f"DataFrame missing columns: {missing}")

    for _, row in df.iterrows():
        cur = con.execute(
            """
            INSERT INTO sectors (sector_code, sector_name, updated_at, source)
            VALUES (?, ?, ?, 'DPS_SECTOR_SUMMARY')
            ON CONFLICT(sector_code) DO UPDATE SET
                sector_name = excluded.sector_name,
                updated_at = excluded.updated_at
            """,
            (row["sector_code"], row["sector_name"], now),
        )
        count += cur.rowcount

    con.commit()
    return count


def sync_sector_names_from_company_profile(con: sqlite3.Connection) -> int:
    """
    Sync sector_name from company_profile to symbols table.

    Updates the symbols.sector_name column with values from company_profile
    for any symbols where company_profile has a sector_name.

    Args:
        con: Database connection

    Returns:
        Number of rows updated
    """
    now = now_iso()
    cur = con.execute(
        """
        UPDATE symbols
        SET sector_name = (
            SELECT company_profile.sector_name
            FROM company_profile
            WHERE company_profile.symbol = symbols.symbol
              AND company_profile.sector_name IS NOT NULL
              AND company_profile.sector_name != ''
        ),
        updated_at = ?
        WHERE EXISTS (
            SELECT 1 FROM company_profile
            WHERE company_profile.symbol = symbols.symbol
              AND company_profile.sector_name IS NOT NULL
              AND company_profile.sector_name != ''
        )
        AND (
            symbols.sector_name IS NULL
            OR symbols.sector_name = ''
            OR symbols.sector_name != (
                SELECT company_profile.sector_name
                FROM company_profile
                WHERE company_profile.symbol = symbols.symbol
            )
        )
        """,
        (now,),
    )
    con.commit()
    return cur.rowcount


def get_symbol_activity(
    con: sqlite3.Connection,
    symbol: str,
    days: int = 30,
) -> dict:
    """
    Get activity statistics for a specific symbol.

    Args:
        con: Database connection
        symbol: Stock symbol
        days: Number of days to look back

    Returns:
        Dict with symbol-specific stats
    """
    from datetime import datetime, timedelta

    cutoff = (datetime.now() - timedelta(days=days)).isoformat()
    symbol = symbol.upper()

    stats: dict = {
        "symbol": symbol,
        "total_views": 0,
        "unique_sessions": 0,
        "action_breakdown": {},
        "recent_activity": [],
    }

    # Total views and unique sessions
    cur = con.execute(
        """
        SELECT COUNT(*) as total, COUNT(DISTINCT session_id) as sessions
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        """,
        (cutoff, symbol),
    )
    row = cur.fetchone()
    if row:
        stats["total_views"] = row[0]
        stats["unique_sessions"] = row[1]

    # Action breakdown
    cur = con.execute(
        """
        SELECT action_type, COUNT(*) as count
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        GROUP BY action_type
        """,
        (cutoff, symbol),
    )
    stats["action_breakdown"] = {row[0]: row[1] for row in cur.fetchall()}

    # Recent activity
    cur = con.execute(
        """
        SELECT timestamp, action_type, action_detail
        FROM user_interactions
        WHERE timestamp >= ? AND symbol = ?
        ORDER BY timestamp DESC
        LIMIT 10
        """,
        (cutoff, symbol),
    )
    stats["recent_activity"] = [
        {"timestamp": row[0], "action": row[1], "detail": row[2]}
        for row in cur.fetchall()
    ]

    return stats
