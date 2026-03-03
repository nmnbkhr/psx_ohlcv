"""Regular Market data fetcher and parser.

Fetches the REGULAR MARKET table from https://dps.psx.com.pk/market-watch
and provides functions to parse, store, and track changes.
"""

import hashlib
import sqlite3
from datetime import datetime
from typing import Any

import pandas as pd
import requests
from lxml import html

# Constants
MARKET_WATCH_URL = "https://dps.psx.com.pk/market-watch"
KARACHI_TZ = "Asia/Karachi"

# Column mapping from HTML table headers to our schema
COLUMN_MAP = {
    "SYMBOL": "symbol",
    "SECTOR": "sector_code",
    "LISTED IN": "listed_in",
    "LDCP": "ldcp",
    "OPEN": "open",
    "HIGH": "high",
    "LOW": "low",
    "CURRENT": "current",
    "CHANGE": "change",
    "CHANGE (%)": "change_pct",
    "VOLUME": "volume",
}

NUMERIC_COLS = [
    "ldcp", "open", "high", "low", "current", "change", "change_pct", "volume"
]


def fetch_market_watch_html(
    timeout: int = 30,
    max_retries: int = 3,
    backoff_factor: float = 0.5,
) -> str:
    """Fetch HTML from PSX market-watch page.

    Args:
        timeout: Request timeout in seconds.
        max_retries: Number of retry attempts.
        backoff_factor: Backoff multiplier between retries.

    Returns:
        Raw HTML content as string.

    Raises:
        requests.RequestException: If all retries fail.
    """
    import time

    last_error = None
    for attempt in range(max_retries):
        try:
            response = requests.get(
                MARKET_WATCH_URL,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (compatible; PSX-OHLCV/1.0)",
                    "Accept": "text/html,application/xhtml+xml",
                },
            )
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            last_error = e
            if attempt < max_retries - 1:
                time.sleep(backoff_factor * (2 ** attempt))

    raise last_error  # type: ignore


def _extract_symbol_and_status(cell_text: str) -> tuple[str, str | None]:
    """Extract symbol and status from a cell that may contain status markers.

    Status markers like NC, XD, XR, XB appear after the symbol.

    Args:
        cell_text: Raw cell text, e.g., "OGDC" or "HBL NC" or "MCB XD"

    Returns:
        Tuple of (symbol, status) where status may be None.
    """
    cell_text = cell_text.strip()
    if not cell_text:
        return ("", None)

    # Common status markers
    status_markers = ["NC", "XD", "XR", "XB", "XA", "XI", "XW"]

    parts = cell_text.split()
    if len(parts) == 1:
        return (parts[0].upper(), None)

    # Check if last part is a status marker
    if parts[-1].upper() in status_markers:
        symbol = " ".join(parts[:-1]).upper()
        status = parts[-1].upper()
        return (symbol, status)

    # No recognized status marker
    return (cell_text.upper(), None)


def compute_row_hash(row: dict[str, Any]) -> str:
    """Compute SHA256 hash of row data for change detection.

    Args:
        row: Dictionary with row data.

    Returns:
        Hex digest of SHA256 hash.
    """
    # Use stable field order for hashing
    fields = [
        "symbol", "status", "sector_code", "listed_in",
        "ldcp", "open", "high", "low", "current",
        "change", "change_pct", "volume"
    ]
    values = []
    for f in fields:
        v = row.get(f)
        if v is None or (isinstance(v, float) and pd.isna(v)):
            values.append("")
        else:
            values.append(str(v))

    joined = "|".join(values)
    return hashlib.sha256(joined.encode("utf-8")).hexdigest()


# Alias for backwards compatibility
_compute_row_hash = compute_row_hash


def parse_regular_market_html(html_content: str) -> pd.DataFrame:
    """Parse the REGULAR MARKET table from HTML content.

    Args:
        html_content: Raw HTML from market-watch page.

    Returns:
        DataFrame with columns: ts, symbol, status, sector_code, listed_in,
        ldcp, open, high, low, current, change, change_pct, volume, row_hash
    """
    tree = html.fromstring(html_content)

    # Get current timestamp in Asia/Karachi
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo(KARACHI_TZ)
    except ImportError:
        # Fallback for older Python
        import pytz  # type: ignore
        tz = pytz.timezone(KARACHI_TZ)

    now = datetime.now(tz)
    ts = now.isoformat()

    # Find the REGULAR MARKET table
    # The page has multiple tabs; we need to find the table for "Regular Market"
    # Strategy: Look for table with headers matching our expected columns

    tables = tree.xpath("//table")
    target_table = None

    for table in tables:
        # Check if this table has the expected headers
        headers = table.xpath(".//thead//th/text() | .//tr[1]//th/text()")
        headers = [h.strip().upper() for h in headers if h.strip()]

        # Check for key columns that identify the REGULAR MARKET table
        if "SYMBOL" in headers and "LDCP" in headers and "CURRENT" in headers:
            target_table = table
            break

    if target_table is None:
        # Try alternative: look for table within a div with specific class/id
        # containing "regular" or "market"
        regular_divs = tree.xpath(
            "//div[contains(@class, 'regular') or contains(@id, 'regular')]//table"
        )
        if regular_divs:
            target_table = regular_divs[0]

    if target_table is None:
        # Last resort: get the first table with many rows
        for table in tables:
            rows = table.xpath(".//tbody//tr")
            if len(rows) > 10:  # Likely the main data table
                target_table = table
                break

    if target_table is None:
        return _empty_regular_market_df()

    # Extract headers
    header_cells = target_table.xpath(".//thead//th | .//tr[1]//th")
    headers = []
    for cell in header_cells:
        # Get all text content including nested elements
        text = "".join(cell.itertext()).strip().upper()
        headers.append(text)

    if not headers:
        # Try first row as headers
        first_row = target_table.xpath(".//tr[1]//td | .//tr[1]//th")
        headers = ["".join(cell.itertext()).strip().upper() for cell in first_row]

    # Map headers to our column names
    # Sort COLUMN_MAP by key length descending so longer keys match first
    # This ensures "CHANGE (%)" matches before "CHANGE"
    col_indices = {}
    sorted_map = sorted(COLUMN_MAP.items(), key=lambda x: len(x[0]), reverse=True)
    for i, h in enumerate(headers):
        for orig, mapped in sorted_map:
            if mapped in col_indices:
                continue  # Already mapped this column
            if orig in h or h in orig:
                col_indices[mapped] = i
                break

    # Extract data rows
    rows_data = []
    body_rows = target_table.xpath(".//tbody//tr")
    if not body_rows:
        # No tbody, skip header row
        body_rows = target_table.xpath(".//tr")[1:]

    for row in body_rows:
        cells = row.xpath(".//td")
        if len(cells) < 5:  # Skip rows with too few cells
            continue

        cell_texts = ["".join(cell.itertext()).strip() for cell in cells]

        row_dict: dict[str, Any] = {"ts": ts}

        # Extract symbol and status
        symbol_idx = col_indices.get("symbol", 0)
        if symbol_idx < len(cell_texts):
            symbol, status = _extract_symbol_and_status(cell_texts[symbol_idx])
            row_dict["symbol"] = symbol
            row_dict["status"] = status
        else:
            continue  # Skip row without symbol

        if not row_dict["symbol"]:
            continue

        # Extract other columns
        for col_name, idx in col_indices.items():
            if col_name == "symbol":
                continue
            if idx < len(cell_texts):
                row_dict[col_name] = cell_texts[idx]
            else:
                row_dict[col_name] = None

        rows_data.append(row_dict)

    if not rows_data:
        return _empty_regular_market_df()

    df = pd.DataFrame(rows_data)

    # Ensure all expected columns exist
    expected_cols = [
        "ts", "symbol", "status", "sector_code", "listed_in"
    ] + NUMERIC_COLS
    for col in expected_cols:
        if col not in df.columns:
            df[col] = None

    # Convert numeric columns
    for col in NUMERIC_COLS:
        if col in df.columns:
            # Remove commas and other non-numeric characters except minus and dot
            df[col] = df[col].astype(str).str.replace(",", "", regex=False)
            df[col] = df[col].str.replace(r"[^\d.\-]", "", regex=True)
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Clean up symbol
    df["symbol"] = df["symbol"].str.strip().str.upper()

    # Compute row hash for each row
    df["row_hash"] = df.apply(lambda r: _compute_row_hash(r.to_dict()), axis=1)

    # Reorder columns
    final_cols = [
        "ts", "symbol", "status", "sector_code", "listed_in",
        "ldcp", "open", "high", "low", "current",
        "change", "change_pct", "volume", "row_hash"
    ]
    df = df[[c for c in final_cols if c in df.columns]]

    # Remove duplicate symbols (keep last)
    df = df.drop_duplicates(subset=["symbol"], keep="last")

    return df


def _empty_regular_market_df() -> pd.DataFrame:
    """Return an empty DataFrame with the expected schema."""
    return pd.DataFrame(columns=[
        "ts", "symbol", "status", "sector_code", "listed_in",
        "ldcp", "open", "high", "low", "current",
        "change", "change_pct", "volume", "row_hash"
    ])


def fetch_regular_market() -> pd.DataFrame:
    """Fetch and parse the regular market data.

    Returns:
        DataFrame with parsed market data.

    Raises:
        requests.RequestException: If HTTP request fails.
    """
    html_content = fetch_market_watch_html()
    return parse_regular_market_html(html_content)


# -----------------------------------------------------------------------------
# Database functions
# -----------------------------------------------------------------------------

def init_regular_market_schema(con: sqlite3.Connection) -> None:
    """Create regular market tables if they don't exist.

    Args:
        con: SQLite connection.
    """
    con.executescript("""
        CREATE TABLE IF NOT EXISTS regular_market_current (
            symbol TEXT PRIMARY KEY,
            ts TEXT NOT NULL,
            status TEXT,
            sector_code TEXT,
            listed_in TEXT,
            ldcp REAL,
            open REAL,
            high REAL,
            low REAL,
            current REAL,
            change REAL,
            change_pct REAL,
            volume REAL,
            row_hash TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS regular_market_snapshots (
            ts TEXT NOT NULL,
            symbol TEXT NOT NULL,
            status TEXT,
            sector_code TEXT,
            listed_in TEXT,
            ldcp REAL,
            open REAL,
            high REAL,
            low REAL,
            current REAL,
            change REAL,
            change_pct REAL,
            volume REAL,
            row_hash TEXT NOT NULL,
            ingested_at TEXT NOT NULL DEFAULT (datetime('now')),
            PRIMARY KEY (ts, symbol)
        );

        CREATE INDEX IF NOT EXISTS idx_rm_snapshots_symbol
            ON regular_market_snapshots(symbol);
        CREATE INDEX IF NOT EXISTS idx_rm_snapshots_ts
            ON regular_market_snapshots(ts);
    """)
    con.commit()


def get_current_hash(con: sqlite3.Connection, symbol: str) -> str | None:
    """Get the current row_hash for a symbol.

    Args:
        con: SQLite connection.
        symbol: Stock symbol.

    Returns:
        Current row_hash or None if not found.
    """
    cur = con.execute(
        "SELECT row_hash FROM regular_market_current WHERE symbol = ?",
        (symbol.upper(),)
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_all_current_hashes(con: sqlite3.Connection) -> dict[str, str]:
    """Get all current row_hashes as a dict.

    Args:
        con: SQLite connection.

    Returns:
        Dict mapping symbol -> row_hash.
    """
    cur = con.execute("SELECT symbol, row_hash FROM regular_market_current")
    return {row[0]: row[1] for row in cur.fetchall()}


def upsert_current(con: sqlite3.Connection, df: pd.DataFrame) -> int:
    """Upsert data into regular_market_current table.

    Args:
        con: SQLite connection.
        df: DataFrame with market data.

    Returns:
        Number of rows upserted.
    """
    if df.empty:
        return 0

    count = 0
    for _, row in df.iterrows():
        con.execute("""
            INSERT INTO regular_market_current (
                symbol, ts, status, sector_code, listed_in,
                ldcp, open, high, low, current,
                change, change_pct, volume, row_hash, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            ON CONFLICT(symbol) DO UPDATE SET
                ts = excluded.ts,
                status = excluded.status,
                sector_code = excluded.sector_code,
                listed_in = excluded.listed_in,
                ldcp = excluded.ldcp,
                open = excluded.open,
                high = excluded.high,
                low = excluded.low,
                current = excluded.current,
                change = excluded.change,
                change_pct = excluded.change_pct,
                volume = excluded.volume,
                row_hash = excluded.row_hash,
                updated_at = datetime('now')
        """, (
            row["symbol"],
            row["ts"],
            row.get("status"),
            row.get("sector_code"),
            row.get("listed_in"),
            row.get("ldcp"),
            row.get("open"),
            row.get("high"),
            row.get("low"),
            row.get("current"),
            row.get("change"),
            row.get("change_pct"),
            row.get("volume"),
            row["row_hash"],
        ))
        count += 1

    con.commit()
    return count


def insert_snapshots(
    con: sqlite3.Connection,
    df: pd.DataFrame,
    save_unchanged: bool = False,
    prev_hashes: dict[str, str] | None = None,
) -> int:
    """Insert snapshot rows into regular_market_snapshots.

    IMPORTANT: For correct change detection, pass prev_hashes loaded BEFORE
    upserting to regular_market_current. If prev_hashes is None, the function
    will read from regular_market_current (which may already be updated).

    Args:
        con: SQLite connection.
        df: DataFrame with market data.
        save_unchanged: If True, save all rows regardless of hash changes.
        prev_hashes: Dict of symbol -> previous row_hash (loaded before upsert).
                     If None, reads from regular_market_current (legacy behavior).

    Returns:
        Number of rows inserted.
    """
    if df.empty:
        return 0

    # If prev_hashes not provided, load from DB (legacy behavior - may be stale)
    if prev_hashes is None and not save_unchanged:
        prev_hashes = get_all_current_hashes(con)

    count = 0
    for _, row in df.iterrows():
        symbol = row["symbol"]
        new_hash = row["row_hash"]

        # Check if we should skip unchanged rows
        if not save_unchanged and prev_hashes is not None:
            prev_hash = prev_hashes.get(symbol)
            # Skip if symbol existed AND hash is unchanged
            if prev_hash is not None and prev_hash == new_hash:
                continue

        try:
            con.execute("""
                INSERT INTO regular_market_snapshots (
                    ts, symbol, status, sector_code, listed_in,
                    ldcp, open, high, low, current,
                    change, change_pct, volume, row_hash, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
            """, (
                row["ts"],
                symbol,
                row.get("status"),
                row.get("sector_code"),
                row.get("listed_in"),
                row.get("ldcp"),
                row.get("open"),
                row.get("high"),
                row.get("low"),
                row.get("current"),
                row.get("change"),
                row.get("change_pct"),
                row.get("volume"),
                new_hash,
            ))
            count += 1
        except sqlite3.IntegrityError:
            # Duplicate (ts, symbol) - skip
            pass

    con.commit()
    return count


def get_current_market(con: sqlite3.Connection) -> pd.DataFrame:
    """Get all current market data.

    Args:
        con: SQLite connection.

    Returns:
        DataFrame with current market data.
    """
    return pd.read_sql_query(
        "SELECT * FROM regular_market_current ORDER BY symbol",
        con
    )


def get_snapshots(
    con: sqlite3.Connection,
    symbol: str | None = None,
    start_ts: str | None = None,
    end_ts: str | None = None,
    limit: int = 1000
) -> pd.DataFrame:
    """Get snapshot history.

    Args:
        con: SQLite connection.
        symbol: Optional symbol filter.
        start_ts: Optional start timestamp.
        end_ts: Optional end timestamp.
        limit: Maximum rows to return.

    Returns:
        DataFrame with snapshot data.
    """
    query = "SELECT * FROM regular_market_snapshots WHERE 1=1"
    params: list = []

    if symbol:
        query += " AND symbol = ?"
        params.append(symbol.upper())

    if start_ts:
        query += " AND ts >= ?"
        params.append(start_ts)

    if end_ts:
        query += " AND ts <= ?"
        params.append(end_ts)

    query += " ORDER BY ts DESC, symbol LIMIT ?"
    params.append(limit)

    return pd.read_sql_query(query, con, params=params)
