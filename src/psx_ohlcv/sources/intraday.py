"""Intraday time series data fetching and parsing.

Fetches intraday price data from PSX DPS endpoint.
Note: This endpoint is undocumented by PSX. Use with caution.
The response format may change without notice.

Source: https://dps.psx.com.pk/timeseries/int/{SYMBOL}
"""

from datetime import datetime

import pandas as pd
import requests

from ..http import create_session, fetch_url

INTRADAY_URL_TEMPLATE = "https://dps.psx.com.pk/timeseries/int/{symbol}"


def fetch_intraday_json(
    symbol: str, session: requests.Session | None = None
) -> dict | list:
    """
    Fetch intraday JSON data for a symbol.

    Args:
        symbol: Stock symbol (e.g., "HBL")
        session: Optional requests Session. If None, creates a new one.

    Returns:
        Raw JSON payload (dict or list)

    Raises:
        requests.RequestException: On fetch failure

    Note:
        This endpoint is undocumented. Response format may vary.
    """
    if session is None:
        session = create_session()

    url = INTRADAY_URL_TEMPLATE.format(symbol=symbol.upper())
    response = fetch_url(session, url, polite=True)
    return response.json()


def parse_intraday_payload(symbol: str, payload: dict | list) -> pd.DataFrame:
    """
    Parse intraday JSON payload into DataFrame.

    Supports multiple payload shapes:
    - list: Direct list of records/arrays
    - {"data": [...]}: Wrapped in data key
    - {"timeseries": [...]}: Wrapped in timeseries key
    - {"records": [...]}: Wrapped in records key
    - dict containing first list value

    Array format expected: [timestamp, close, volume, open] (same as EOD)
    or [timestamp, price, volume] format.

    Args:
        symbol: Stock symbol to add to each row
        payload: Raw JSON payload from API

    Returns:
        DataFrame with columns: symbol, ts, open, high, low, close, volume
        Returns empty DataFrame if no valid rows or unknown format.
    """
    if not payload:
        return _empty_intraday_df()

    # Extract data list from various payload shapes
    data_list = _extract_data_list(payload)

    if not data_list:
        return _empty_intraday_df()

    rows = []
    for item in data_list:
        row = _parse_single_item(symbol, item)
        if row:
            rows.append(row)

    if not rows:
        return _empty_intraday_df()

    df = pd.DataFrame(rows)

    # Convert numeric columns
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Ensure symbol is uppercase
    df["symbol"] = df["symbol"].str.upper()

    # Ensure ts is string and trimmed
    df["ts"] = df["ts"].astype(str).str.strip()

    # Compute ts_epoch for each row
    if "ts_epoch" not in df.columns:
        df["ts_epoch"] = df["ts"].apply(_ts_to_epoch)

    # Drop duplicates (symbol, ts)
    df = df.drop_duplicates(subset=["symbol", "ts"], keep="last")

    # Sort by ts_epoch for proper ordering
    df = df.sort_values("ts_epoch").reset_index(drop=True)

    return df


def _extract_data_list(payload: dict | list) -> list:
    """Extract data list from various payload shapes."""
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        # Try known keys first
        for key in ["data", "timeseries", "records", "rows"]:
            if key in payload and isinstance(payload[key], list):
                return payload[key]

        # Try first list value in dict
        for value in payload.values():
            if isinstance(value, list):
                return value

    return []


def _parse_single_item(symbol: str, item) -> dict | None:
    """
    Parse a single data item into a row dict.

    Handles both array format and dict format.
    """
    if isinstance(item, dict):
        return _parse_dict_item(symbol, item)
    elif isinstance(item, list):
        return _parse_array_item(symbol, item)
    return None


def _parse_dict_item(symbol: str, item: dict) -> dict | None:
    """Parse dict format item."""
    # Try to find timestamp field
    ts = None
    for key in ["ts", "timestamp", "datetime", "time", "date"]:
        if key in item:
            ts = item[key]
            break

    if ts is None:
        return None

    # Convert timestamp if numeric
    if isinstance(ts, (int, float)) and ts > 1000000000:
        try:
            dt = datetime.fromtimestamp(ts)
            ts = dt.strftime("%Y-%m-%d %H:%M:%S")
        except (ValueError, OSError):
            return None

    return {
        "symbol": symbol.upper(),
        "ts": str(ts).strip(),
        "open": item.get("open"),
        "high": item.get("high"),
        "low": item.get("low"),
        "close": item.get("close", item.get("price")),
        "volume": item.get("volume"),
    }


def _parse_array_item(symbol: str, item: list) -> dict | None:
    """
    Parse array format item.

    Expected formats:
    - [timestamp, close, volume, open] (PSX EOD-like format)
    - [timestamp, price, volume]
    - [timestamp, price]
    """
    if len(item) < 2:
        return None

    timestamp = item[0]
    if not isinstance(timestamp, (int, float)) or timestamp < 1000000000:
        return None

    try:
        dt = datetime.fromtimestamp(timestamp)
        ts = dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return None

    # Handle different array lengths
    if len(item) >= 4:
        # PSX format: [timestamp, close, volume, open]
        close = item[1]
        volume = item[2]
        open_price = item[3]
        # Derive high/low from open and close
        high = max(open_price, close) if open_price and close else close
        low = min(open_price, close) if open_price and close else close
    elif len(item) >= 3:
        # Format: [timestamp, price, volume]
        close = item[1]
        volume = item[2]
        open_price = close
        high = close
        low = close
    else:
        # Format: [timestamp, price]
        close = item[1]
        volume = None
        open_price = close
        high = close
        low = close

    return {
        "symbol": symbol.upper(),
        "ts": ts,
        "open": open_price,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


def _empty_intraday_df() -> pd.DataFrame:
    """Return empty DataFrame with correct schema."""
    return pd.DataFrame(
        columns=["symbol", "ts", "ts_epoch", "open", "high", "low", "close", "volume"]
    )


def aggregate_intraday_to_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate intraday data to daily OHLCV with actual high/low.

    This can provide true high/low values if intraday data is available.

    Args:
        df: Intraday DataFrame with columns: symbol, ts, open, high, low, close, volume

    Returns:
        DataFrame with columns: symbol, date, open, high, low, close, volume
    """
    if df.empty:
        return pd.DataFrame(
            columns=["symbol", "date", "open", "high", "low", "close", "volume"]
        )

    # Extract date from ts
    df = df.copy()
    df["date"] = pd.to_datetime(df["ts"]).dt.strftime("%Y-%m-%d")

    # Group by symbol and date
    def agg_ohlcv(group):
        return pd.Series({
            "open": group["open"].iloc[0],
            "high": group["high"].max(),
            "low": group["low"].min(),
            "close": group["close"].iloc[-1],
            "volume": group["volume"].sum(),
        })

    result = df.groupby(["symbol", "date"]).apply(
        agg_ohlcv, include_groups=False
    ).reset_index()

    return result


def filter_incremental(
    df: pd.DataFrame, last_ts_epoch: int | None
) -> pd.DataFrame:
    """
    Filter DataFrame to only include rows newer than last_ts_epoch.

    Args:
        df: DataFrame with 'ts_epoch' column
        last_ts_epoch: Last synced timestamp as Unix epoch (inclusive cutoff)

    Returns:
        Filtered DataFrame with only rows where ts_epoch > last_ts_epoch
    """
    if df.empty or last_ts_epoch is None:
        return df

    # Filter to rows strictly after last_ts_epoch using integer comparison
    if "ts_epoch" not in df.columns:
        # If ts_epoch not present, compute it
        df = df.copy()
        df["ts_epoch"] = df["ts"].apply(_ts_to_epoch)

    return df[df["ts_epoch"] > last_ts_epoch].reset_index(drop=True)


def _ts_to_epoch(ts: str) -> int:
    """Convert timestamp string to Unix epoch."""
    from datetime import datetime

    ts = str(ts).strip()

    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M",
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(ts[:19], fmt)
            return int(dt.timestamp())
        except ValueError:
            continue

    # Fallback: try pandas
    try:
        import pandas as pd
        dt = pd.to_datetime(ts)
        return int(dt.timestamp())
    except Exception:
        return 0
