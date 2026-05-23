"""EOD (End of Day) OHLCV data fetching and parsing."""

import json
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

from ..http import create_session, fetch_url

EOD_URL_TEMPLATE = "https://dps.psx.com.pk/timeseries/eod/{symbol}"

log = logging.getLogger(__name__)


def fetch_eod_json(symbol: str, session: requests.Session | None = None) -> dict | list:
    """
    Fetch EOD JSON data for a symbol.

    Args:
        symbol: Stock symbol (e.g., "HBL")
        session: Optional requests Session. If None, creates a new one.

    Returns:
        Raw JSON payload (dict or list)

    Raises:
        requests.RequestException: On fetch failure
    """
    if session is None:
        session = create_session()

    url = EOD_URL_TEMPLATE.format(symbol=symbol)
    response = fetch_url(session, url, polite=True)
    return response.json()


def parse_eod_payload(symbol: str, payload: dict | list) -> pd.DataFrame:
    """
    Parse EOD JSON payload into normalized DataFrame.

    Supports multiple payload shapes:
    - list[dict]: Direct list of records
    - list[list]: Array format [timestamp, close, volume, open] from PSX API
    - {"data": [...]}
    - {"timeseries": [...]}
    - dict containing first list value

    Args:
        symbol: Stock symbol to add to each row
        payload: Raw JSON payload from API

    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, volume
        Sorted by date, duplicates removed.
        Returns empty DataFrame if no valid rows.
    """
    # Extract the data list from various payload shapes
    data_list = _extract_data_list(payload)

    if not data_list:
        return _empty_eod_df()

    # Check if data is in array format (PSX API style)
    if _is_array_format(data_list):
        data_list = _convert_array_to_dicts(data_list)
        if not data_list:
            return _empty_eod_df()

    # Convert to DataFrame
    df = pd.DataFrame(data_list)

    # Normalize column names (lowercase)
    df.columns = [str(c).lower().strip() for c in df.columns]

    # Map common column name variants
    column_mapping = {
        "dt": "date",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "vol": "volume",
        "price": "close",
    }
    df = df.rename(columns=column_mapping)

    # Ensure required columns exist
    required = {"date", "open", "high", "low", "close", "volume"}
    available = set(df.columns)

    if not required.issubset(available):
        # Try to find date column with different names
        if "date" not in df.columns:
            for col in df.columns:
                if "date" in col.lower() or "time" in col.lower():
                    df = df.rename(columns={col: "date"})
                    break

    # Check again after remapping
    available = set(df.columns)
    missing = required - available
    if missing:
        # Return empty if we can't find required columns
        return _empty_eod_df()

    # Select and order columns
    df = df[["date", "open", "high", "low", "close", "volume"]].copy()

    # Add symbol column
    df.insert(0, "symbol", symbol)

    # Normalize date format to YYYY-MM-DD
    # Use format='mixed' to handle various date formats in the same column
    df["date"] = pd.to_datetime(
        df["date"], format="mixed", errors="coerce"
    ).dt.strftime("%Y-%m-%d")

    # Convert numeric columns
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")

    # Drop rows with invalid dates
    df = df.dropna(subset=["date"])

    # Drop duplicates on (symbol, date)
    df = df.drop_duplicates(subset=["symbol", "date"], keep="last")

    # Sort by date
    df = df.sort_values("date").reset_index(drop=True)

    return df


def _extract_data_list(payload: dict | list) -> list:
    """Extract the data list from various payload shapes."""
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        # Try known keys
        for key in ["data", "timeseries", "records", "rows"]:
            if key in payload and isinstance(payload[key], list):
                return payload[key]

        # Try first list value in dict
        for value in payload.values():
            if isinstance(value, list):
                return value

    return []


def _is_array_format(data_list: list) -> bool:
    """Check if data is in array format [timestamp, close, volume, open]."""
    if not data_list:
        return False
    first_item = data_list[0]
    # Array format: list of 4 numeric values where first is a Unix timestamp
    return (
        isinstance(first_item, list)
        and len(first_item) >= 4
        and isinstance(first_item[0], (int, float))
        and first_item[0] > 1000000000  # Unix timestamp check
    )


def _convert_array_to_dicts(data_list: list) -> list[dict]:
    """
    Convert array format data to list of dicts.

    PSX API returns: [timestamp, close, volume, open]
    We convert to dict with: date, open, high, low, close, volume

    Since API doesn't provide high/low, we use open and close to estimate:
    - high = max(open, close)
    - low = min(open, close)
    """
    from datetime import datetime

    result = []
    for item in data_list:
        if not isinstance(item, list) or len(item) < 4:
            continue

        timestamp, close, volume, open_price = item[0], item[1], item[2], item[3]

        # Convert Unix timestamp to date string
        try:
            dt = datetime.fromtimestamp(timestamp)
            date_str = dt.strftime("%Y-%m-%d")
        except (ValueError, OSError):
            continue

        # Derive high/low from open and close
        high = max(open_price, close) if open_price and close else close
        low = min(open_price, close) if open_price and close else close

        result.append({
            "date": date_str,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        })

    return result


def _empty_eod_df() -> pd.DataFrame:
    """Return empty DataFrame with correct schema."""
    return pd.DataFrame(
        columns=["symbol", "date", "open", "high", "low", "close", "volume"]
    )


# ═══════════════════════════════════════════════════════
# Batch Fetch + Store (API → JSON → DB) with parallel shards
# ═══════════════════════════════════════════════════════

_RATE_LIMIT = 0.05  # seconds between requests per thread
_WORKERS_PER_SHARD = 10


def _get_active_symbols(con: sqlite3.Connection) -> list[str]:
    """Get all active symbols from the symbols table."""
    rows = con.execute(
        "SELECT symbol FROM symbols WHERE is_active = 1 ORDER BY symbol"
    ).fetchall()
    return [r[0] for r in rows]


def _get_skip_symbols(con: sqlite3.Connection) -> set[str]:
    """
    Get symbols to skip from batch download.

    Skips symbols SUSPENDED or DELISTED more than 30 days ago.
    Only recently suspended symbols (within last 30 days) are kept.
    All other symbols (including WU/XD/XB/XR/NC suffixed) are kept.
    """
    skip: set[str] = set()

    # SUSPENDED / DELISTED more than 30 days ago (keep recent ones)
    try:
        rows = con.execute(
            """
            SELECT DISTINCT symbol FROM company_listing_status
            WHERE status IN ('SUSPENDED', 'DELISTED')
              AND is_current = 1
              AND first_seen <= date('now', '-30 days')
            """
        ).fetchall()
        skip.update(r[0] for r in rows)
    except sqlite3.OperationalError:
        pass

    return skip


def _shard_symbols(symbols: list[str], n: int = 3) -> list[list[str]]:
    """Split symbol list into n roughly equal shards."""
    shards: list[list[str]] = [[] for _ in range(n)]
    for i, sym in enumerate(symbols):
        shards[i % n].append(sym)
    return shards


def _fetch_and_save_json(
    symbol: str,
    json_dir: Path,
    session: requests.Session,
) -> dict | None:
    """Fetch EOD JSON for one symbol, save to disk, return parsed data."""
    time.sleep(_RATE_LIMIT)
    url = EOD_URL_TEMPLATE.format(symbol=symbol)
    try:
        resp = session.get(url, timeout=30)
        if resp.status_code != 200:
            return None
        payload = resp.json()
        out = json_dir / f"{symbol}.json"
        out.write_text(json.dumps(payload), encoding="utf-8")
        return payload
    except Exception as exc:
        log.debug("fetch %s failed: %s", symbol, exc)
        return None


def _run_shard(
    shard: list[str],
    json_dir: Path,
) -> tuple[list[pd.DataFrame], int, int]:
    """Download + parse one shard of symbols using a thread pool.

    Returns:
        (frames, ok_count, fail_count)
    """
    session = create_session()
    frames: list[pd.DataFrame] = []
    ok = 0
    fail = 0

    def _work(sym: str):
        payload = _fetch_and_save_json(sym, json_dir, session)
        if payload is None:
            return sym, None
        df = parse_eod_payload(sym, payload)
        return sym, df

    with ThreadPoolExecutor(max_workers=_WORKERS_PER_SHARD) as pool:
        futs = {pool.submit(_work, s): s for s in shard}
        for fut in as_completed(futs):
            sym, df = fut.result()
            if df is not None and not df.empty:
                frames.append(df)
                ok += 1
            else:
                fail += 1

    return frames, ok, fail


def prepare_batch_symbols(con: sqlite3.Connection, n_shards: int = 3) -> dict:
    """Build the filtered symbol list and shards (runs on main thread).

    Returns dict with: symbols, skipped, shards, json_dir
    """
    from pakfindata.config import DATA_ROOT

    all_symbols = _get_active_symbols(con)
    skipped = _get_skip_symbols(con)
    symbols = [s for s in all_symbols if s not in skipped]

    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    json_dir = DATA_ROOT / "eod_json" / date_str
    json_dir.mkdir(parents=True, exist_ok=True)

    shards = _shard_symbols(symbols, n_shards)

    return {
        "all_count": len(all_symbols),
        "symbols": symbols,
        "skipped": skipped,
        "shards": shards,
        "json_dir": json_dir,
    }


def run_shard_batch(
    shard: list[str],
    json_dir: Path,
    shard_idx: int = 0,
) -> tuple[pd.DataFrame | None, int, int]:
    """Run one shard: fetch JSONs in parallel, save CSVs, return DataFrame.

    Saves per-symbol CSVs to json_dir/../csv/{SYMBOL}.csv
    and a combined shard CSV to json_dir/../csv/shard_{idx}.csv.

    Returns:
        (combined_df_or_None, ok_count, fail_count)
    """
    frames, ok, fail = _run_shard(shard, json_dir)
    if frames:
        combined = pd.concat(frames, ignore_index=True)
        # Save CSVs
        csv_dir = json_dir.parent.parent / "eod_csv" / json_dir.name
        csv_dir.mkdir(parents=True, exist_ok=True)
        # Per-symbol CSVs
        for sym, grp in combined.groupby("symbol"):
            grp.to_csv(csv_dir / f"{sym}.csv", index=False)
        # Combined shard CSV
        combined.to_csv(csv_dir / f"shard_{shard_idx}.csv", index=False)
        return combined, ok, fail
    return None, ok, fail


def filter_incremental(df: pd.DataFrame, max_date: str | None) -> pd.DataFrame:
    """
    Filter DataFrame to only include rows newer than max_date.

    Args:
        df: DataFrame with 'date' column (YYYY-MM-DD format)
        max_date: Maximum date already in DB (YYYY-MM-DD), or None for no filtering

    Returns:
        Filtered DataFrame with only rows where date > max_date
    """
    if df.empty or max_date is None:
        return df

    # Filter to rows strictly after max_date
    return df[df["date"] > max_date].reset_index(drop=True)
