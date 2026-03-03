"""EOD (End of Day) OHLCV data fetching and parsing."""

import pandas as pd
import requests

from ..http import create_session, fetch_url

EOD_URL_TEMPLATE = "https://dps.psx.com.pk/timeseries/eod/{symbol}"


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
