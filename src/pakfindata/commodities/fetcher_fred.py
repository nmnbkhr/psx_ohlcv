"""Tier 2: FRED API fetcher for monthly commodity benchmark prices.

FRED (Federal Reserve Economic Data) provides free monthly commodity prices.
Requires a free API key from https://fred.stlouisfed.org/docs/api/api_key.html
No credit card needed.
"""

import logging
import os

import pandas as pd

logger = logging.getLogger("pakfindata.commodities.fred")


def _get_fred_client(api_key: str | None = None):
    """Create a FRED API client. Reads key from env if not provided."""
    try:
        from fredapi import Fred
    except ImportError:
        raise ImportError(
            "fredapi is required for FRED data. Install with: pip install fredapi"
        )

    key = api_key or os.environ.get("FRED_API_KEY")
    if not key:
        raise ValueError(
            "FRED API key required. Set FRED_API_KEY env var or pass api_key parameter. "
            "Register free at https://fred.stlouisfed.org/docs/api/api_key.html"
        )

    return Fred(api_key=key)


def fetch_fred_series(
    series_id: str,
    symbol: str,
    api_key: str | None = None,
    start: str | None = None,
) -> list[dict]:
    """Fetch a single FRED series and return as list of dicts.

    Args:
        series_id: FRED series ID (e.g., "PCOALAUUSDM").
        symbol: Internal commodity symbol to tag rows with.
        api_key: FRED API key.
        start: Optional start date (YYYY-MM-DD).

    Returns:
        List of dicts with keys: symbol, date, price, source, series_id.
    """
    fred = _get_fred_client(api_key)

    try:
        series = fred.get_series(series_id, observation_start=start)
    except Exception as e:
        logger.warning("FRED fetch failed for %s (%s): %s", symbol, series_id, e)
        return []

    if series is None or series.empty:
        logger.info("No FRED data for %s (%s)", symbol, series_id)
        return []

    rows = []
    for dt, value in series.items():
        if pd.notna(value):
            date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
            rows.append({
                "symbol": symbol,
                "date": date_str,
                "price": float(value),
                "source": "fred",
                "series_id": series_id,
            })

    logger.info("FRED: fetched %d monthly observations for %s (%s)", len(rows), symbol, series_id)
    return rows


def fetch_all_fred_series(
    api_key: str | None = None,
    start: str | None = None,
) -> dict[str, list[dict]]:
    """Fetch all configured FRED series.

    Returns dict mapping symbol -> list of monthly price dicts.
    """
    from .config import get_fred_series

    fred_map = get_fred_series()
    results = {}

    for symbol, series_id in fred_map.items():
        rows = fetch_fred_series(series_id, symbol, api_key=api_key, start=start)
        if rows:
            results[symbol] = rows

    return results


def fetch_pakistan_cpi(api_key: str | None = None) -> list[dict]:
    """Fetch Pakistan CPI All Items from FRED (PAKCEPIALLMINMEI).

    Useful for real-return analysis of commodity prices.
    """
    return fetch_fred_series("PAKCEPIALLMINMEI", "PK_CPI", api_key=api_key)
