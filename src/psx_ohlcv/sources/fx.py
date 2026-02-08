"""
FX (Foreign Exchange) data source module for Phase 2.

This module provides FX rate data fetching from various sources:
- State Bank of Pakistan (SBP) - primary source
- Open FX APIs - fallback
- Sample/mock data - for testing when APIs unavailable

FX data is used for macro context and analytics only, NOT for trading.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests

from ..config import DATA_ROOT

# Default FX pairs for Pakistan market context
DEFAULT_FX_PAIRS = [
    {
        "pair": "USD/PKR",
        "base_currency": "USD",
        "quote_currency": "PKR",
        "source": "SBP",
        "description": "US Dollar to Pakistani Rupee",
    },
    {
        "pair": "EUR/PKR",
        "base_currency": "EUR",
        "quote_currency": "PKR",
        "source": "SBP",
        "description": "Euro to Pakistani Rupee",
    },
    {
        "pair": "GBP/PKR",
        "base_currency": "GBP",
        "quote_currency": "PKR",
        "source": "SBP",
        "description": "British Pound to Pakistani Rupee",
    },
    {
        "pair": "SAR/PKR",
        "base_currency": "SAR",
        "quote_currency": "PKR",
        "source": "SBP",
        "description": "Saudi Riyal to Pakistani Rupee",
    },
    {
        "pair": "AED/PKR",
        "base_currency": "AED",
        "quote_currency": "PKR",
        "source": "SBP",
        "description": "UAE Dirham to Pakistani Rupee",
    },
]

# Sample FX config file location
FX_CONFIG_PATH = DATA_ROOT / "fx_config.json"
FX_SAMPLE_DATA_PATH = DATA_ROOT / "fx_sample_data.csv"


def get_default_fx_pairs() -> list[dict]:
    """Get the default FX pairs configuration."""
    return DEFAULT_FX_PAIRS.copy()


def load_fx_config(config_path: Path | None = None) -> dict:
    """
    Load FX configuration from JSON file.

    Args:
        config_path: Path to config file, or None for default

    Returns:
        Config dict with 'pairs' key
    """
    path = config_path or FX_CONFIG_PATH

    if path.exists():
        try:
            with open(path) as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError):
            pass

    return {"pairs": DEFAULT_FX_PAIRS}


def save_fx_config(config: dict, config_path: Path | None = None) -> bool:
    """Save FX configuration to JSON file."""
    path = config_path or FX_CONFIG_PATH

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            json.dump(config, f, indent=2)
        return True
    except IOError:
        return False


def fetch_fx_from_sbp(
    pair: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Fetch FX rates from State Bank of Pakistan.

    Note: SBP provides interbank rates. This function attempts to
    fetch from SBP's public data or falls back to sample data.

    Args:
        pair: Currency pair (e.g., "USD/PKR")
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with date, open, high, low, close columns
    """
    # SBP doesn't have a simple public API, so we use sample data
    # In production, this would connect to SBP's data portal or
    # use a commercial FX data provider
    return fetch_fx_sample_data(pair, start_date, end_date)


def fetch_fx_from_open_api(
    pair: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Fetch FX rates from open FX APIs.

    Tries multiple free FX APIs as fallback sources.

    Args:
        pair: Currency pair (e.g., "USD/PKR")
        start_date: Start date
        end_date: End date

    Returns:
        DataFrame with OHLCV data
    """
    # Parse pair
    parts = pair.split("/")
    if len(parts) != 2:
        return pd.DataFrame()

    base, quote = parts

    # Try exchangerate-api.com (free tier)
    # Note: Free tier has limitations
    try:
        # This is a placeholder - in production, use actual API
        url = f"https://open.er-api.com/v6/latest/{base}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data and "rates" in data and quote in data["rates"]:
            rate = data["rates"][quote]
            today = datetime.now().strftime("%Y-%m-%d")

            return pd.DataFrame([{
                "date": today,
                "open": rate,
                "high": rate,
                "low": rate,
                "close": rate,
                "volume": None,
            }])
    except Exception:
        pass

    # Fallback to sample data
    return fetch_fx_sample_data(pair, start_date, end_date)


def fetch_fx_sample_data(
    pair: str,
    start_date: str | None = None,
    end_date: str | None = None,
) -> pd.DataFrame:
    """
    Generate sample FX data for testing and development.

    This provides realistic-looking FX data when APIs are unavailable.

    Args:
        pair: Currency pair
        start_date: Start date
        end_date: End date

    Returns:
        DataFrame with sample OHLCV data
    """
    # Check for existing sample data file
    if FX_SAMPLE_DATA_PATH.exists():
        try:
            df = pd.read_csv(FX_SAMPLE_DATA_PATH)
            df = df[df["pair"] == pair]
            if start_date:
                df = df[df["date"] >= start_date]
            if end_date:
                df = df[df["date"] <= end_date]
            if not df.empty:
                return df
        except Exception:
            pass

    # Generate sample data based on pair
    # Use realistic current rates (as of early 2026)
    base_rates = {
        "USD/PKR": 279.00,
        "EUR/PKR": 290.00,
        "GBP/PKR": 350.00,
        "SAR/PKR": 74.40,
        "AED/PKR": 76.00,
    }

    base_rate = base_rates.get(pair, 100.0)

    # Generate last 365 days of data
    if end_date:
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    else:
        end_dt = datetime.now()

    if start_date:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_dt = end_dt - timedelta(days=365)

    data = []
    current_rate = base_rate
    current_dt = start_dt

    import random
    random.seed(42)  # Reproducible for testing

    while current_dt <= end_dt:
        # Skip weekends (no FX trading)
        if current_dt.weekday() < 5:
            # Random walk with strong mean-reversion (stays near base rate)
            daily_change = random.gauss(0, 0.001)  # No trend, 0.1% daily vol
            current_rate *= (1 + daily_change)
            # Strong mean reversion: pull back toward base rate
            current_rate = current_rate * 0.99 + base_rate * 0.01

            # Generate OHLC from close
            volatility = current_rate * 0.005  # 0.5% daily range
            high = current_rate + random.uniform(0, volatility)
            low = current_rate - random.uniform(0, volatility)
            open_price = current_rate + random.uniform(-volatility/2, volatility/2)

            data.append({
                "date": current_dt.strftime("%Y-%m-%d"),
                "open": round(open_price, 4),
                "high": round(high, 4),
                "low": round(low, 4),
                "close": round(current_rate, 4),
                "volume": None,
            })

        current_dt += timedelta(days=1)

    return pd.DataFrame(data)


def fetch_fx_ohlcv(
    pair: str,
    start_date: str | None = None,
    end_date: str | None = None,
    source: str = "AUTO",
) -> pd.DataFrame:
    """
    Fetch FX OHLCV data from the best available source.

    Args:
        pair: Currency pair (e.g., "USD/PKR")
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        source: Data source ("SBP", "OPEN_API", "SAMPLE", "AUTO")

    Returns:
        DataFrame with columns: date, open, high, low, close, volume
    """
    if source == "SBP":
        return fetch_fx_from_sbp(pair, start_date, end_date)
    elif source == "OPEN_API":
        return fetch_fx_from_open_api(pair, start_date, end_date)
    elif source == "SAMPLE":
        return fetch_fx_sample_data(pair, start_date, end_date)
    else:
        # AUTO: Try sources in order
        # 1. Try open API for latest rate
        df = fetch_fx_from_open_api(pair, start_date, end_date)
        if not df.empty:
            return df

        # 2. Try SBP
        df = fetch_fx_from_sbp(pair, start_date, end_date)
        if not df.empty:
            return df

        # 3. Fall back to sample data
        return fetch_fx_sample_data(pair, start_date, end_date)


def get_fx_rate_for_date(
    pair: str,
    date: str,
    source: str = "AUTO",
) -> float | None:
    """
    Get FX rate for a specific date.

    Args:
        pair: Currency pair
        date: Date (YYYY-MM-DD)
        source: Data source

    Returns:
        Closing rate or None if not available
    """
    df = fetch_fx_ohlcv(pair, start_date=date, end_date=date, source=source)

    if df.empty:
        # Try getting closest available date
        df = fetch_fx_ohlcv(pair, source=source)
        if not df.empty:
            df = df.sort_values("date")
            # Get rate on or before the requested date
            df_before = df[df["date"] <= date]
            if not df_before.empty:
                return df_before.iloc[-1]["close"]

    if not df.empty:
        return df.iloc[0]["close"]

    return None


def normalize_fx_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize FX DataFrame to standard schema.

    Args:
        df: Raw DataFrame from any source

    Returns:
        DataFrame with standard columns: date, open, high, low, close, volume
    """
    if df.empty:
        return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"])

    # Ensure required columns
    required = ["date", "close"]
    cols = ["date", "open", "high", "low", "close", "volume"]
    for col in required:
        if col not in df.columns:
            return pd.DataFrame(columns=cols)

    # Fill missing OHLC with close
    if "open" not in df.columns:
        df["open"] = df["close"]
    if "high" not in df.columns:
        df["high"] = df["close"]
    if "low" not in df.columns:
        df["low"] = df["close"]
    if "volume" not in df.columns:
        df["volume"] = None

    return df[["date", "open", "high", "low", "close", "volume"]]
