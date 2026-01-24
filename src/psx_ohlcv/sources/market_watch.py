"""Market watch symbol discovery."""

import re
from dataclasses import dataclass
from io import StringIO
from pathlib import Path

import pandas as pd

from ..db import connect, init_schema, upsert_symbols
from ..http import create_session, fetch_url

MARKET_WATCH_URL = "https://dps.psx.com.pk/market-watch"

# Regex for typical PSX ticker: 2-10 uppercase letters, may contain numbers at end
TICKER_PATTERN = re.compile(r"^[A-Z]{2,10}[0-9]?$")


@dataclass
class RefreshResult:
    """Result of symbol refresh operation."""

    symbols_found: int
    symbols_upserted: int


def fetch_market_watch_html(session=None) -> str:
    """
    Fetch market watch HTML from PSX.

    Args:
        session: Optional requests Session. If None, creates a new one.

    Returns:
        HTML content as string

    Raises:
        requests.RequestException: On fetch failure
    """
    if session is None:
        session = create_session()

    response = fetch_url(session, MARKET_WATCH_URL, polite=False)
    return response.text


def parse_symbols_from_market_watch(html: str) -> list[dict]:
    """
    Parse symbols from market watch HTML.

    Uses pandas.read_html to extract tables, looking for SYMBOL column.
    Falls back to regex extraction if table parsing fails.

    Args:
        html: HTML content from market watch page

    Returns:
        List of symbol dicts with keys: symbol, name, sector, is_active
        Sorted alphabetically, unique symbols only.
    """
    symbols_set: set[str] = set()

    # Try pandas.read_html first
    try:
        tables = pd.read_html(StringIO(html))
        for df in tables:
            # Normalize column names
            df.columns = [str(c).strip().upper() for c in df.columns]

            # Look for SYMBOL column (various possible names)
            symbol_col = None
            for col in ["SYMBOL", "SYMBOLS", "TICKER", "CODE", "SCRIP"]:
                if col in df.columns:
                    symbol_col = col
                    break

            if symbol_col:
                for val in df[symbol_col].dropna():
                    sym = str(val).strip().upper()
                    if _is_valid_symbol(sym):
                        symbols_set.add(sym)

    except (ValueError, ImportError):
        # No tables found or lxml not available, fall through to regex
        pass

    # Fallback: regex extraction from raw HTML
    if not symbols_set:
        symbols_set = _extract_symbols_by_regex(html)

    # Convert to list of dicts
    symbols_list = [
        {"symbol": sym, "name": None, "sector": None, "is_active": 1}
        for sym in sorted(symbols_set)
    ]

    return symbols_list


def _is_valid_symbol(s: str) -> bool:
    """Check if string looks like a valid PSX symbol."""
    if not s or len(s) < 2 or len(s) > 10:
        return False
    # Must match ticker pattern and not be common words
    if not TICKER_PATTERN.match(s):
        return False
    # Filter out common non-symbol words that might appear
    blacklist = {
        "SYMBOL",
        "NAME",
        "SECTOR",
        "VOLUME",
        "CHANGE",
        "HIGH",
        "LOW",
        "OPEN",
        "CLOSE",
        "VALUE",
        "PRICE",
        "LAST",
        "CURRENT",
        "MARKET",
        "TOTAL",
        "DATE",
        "TIME",
    }
    return s not in blacklist


def _extract_symbols_by_regex(html: str) -> set[str]:
    """
    Extract symbols from HTML using regex.

    Looks for patterns in table cells or specific data attributes.
    """
    symbols = set()

    # Look for symbols in table cells: <td>SYMBOL</td> or similar
    td_pattern = re.compile(r"<td[^>]*>([A-Z]{2,10}[0-9]?)</td>", re.IGNORECASE)
    for match in td_pattern.finditer(html):
        candidate = match.group(1).upper()
        if _is_valid_symbol(candidate):
            symbols.add(candidate)

    # Also look for data-symbol attributes
    data_pattern = re.compile(
        r'data-symbol=["\']([A-Z]{2,10}[0-9]?)["\']', re.IGNORECASE
    )
    for match in data_pattern.finditer(html):
        candidate = match.group(1).upper()
        if _is_valid_symbol(candidate):
            symbols.add(candidate)

    return symbols


def refresh_symbols(db_path: Path | str | None = None) -> RefreshResult:
    """
    Refresh symbols from market watch into database.

    Fetches market watch HTML, parses symbols, and upserts into database.

    Args:
        db_path: Path to SQLite database. Uses default if None.

    Returns:
        RefreshResult with counts
    """
    # Fetch and parse
    html = fetch_market_watch_html()
    symbols = parse_symbols_from_market_watch(html)

    # Upsert to database
    con = connect(db_path)
    init_schema(con)
    upserted = upsert_symbols(con, symbols)
    con.close()

    return RefreshResult(symbols_found=len(symbols), symbols_upserted=upserted)
