"""Tier 1: yfinance fetcher for daily commodity OHLCV data.

yfinance provides free daily OHLCV for futures contracts and FX pairs.
No API key required. Same library already used for FX data in pakfindata.
"""

import logging
from datetime import datetime, timedelta

import pandas as pd

from .config import COMMODITY_UNIVERSE, CommodityDef, get_yfinance_tickers

logger = logging.getLogger("pakfindata.commodities.yfinance")


def _import_yfinance():
    """Lazy import yfinance to avoid hard dependency at module level."""
    try:
        import yfinance as yf
        return yf
    except ImportError:
        raise ImportError(
            "yfinance is required for commodity data. Install with: pip install yfinance"
        )


def fetch_single_commodity(
    commodity: CommodityDef,
    start: str | None = None,
    end: str | None = None,
    period: str = "1y",
) -> list[dict]:
    """Fetch OHLCV data for a single commodity from yfinance.

    Args:
        commodity: CommodityDef with yf_ticker set.
        start: Start date (YYYY-MM-DD). If None, uses period.
        end: End date (YYYY-MM-DD). If None, uses today.
        period: yfinance period string (e.g. "1y", "5y", "max"). Used if start is None.

    Returns:
        List of dicts with keys: symbol, date, open, high, low, close, volume, adj_close, source.
    """
    if not commodity.yf_ticker:
        return []

    yf = _import_yfinance()
    ticker = yf.Ticker(commodity.yf_ticker)

    try:
        if start:
            df = ticker.history(start=start, end=end or None, auto_adjust=False)
        else:
            df = ticker.history(period=period, auto_adjust=False)
    except Exception as e:
        logger.warning("Failed to fetch %s (%s): %s", commodity.symbol, commodity.yf_ticker, e)
        return []

    if df is None or df.empty:
        logger.info("No data for %s (%s)", commodity.symbol, commodity.yf_ticker)
        return []

    rows = []
    for dt, row in df.iterrows():
        date_str = dt.strftime("%Y-%m-%d") if hasattr(dt, "strftime") else str(dt)[:10]
        rows.append({
            "symbol": commodity.symbol,
            "date": date_str,
            "open": float(row.get("Open", 0)) if pd.notna(row.get("Open")) else None,
            "high": float(row.get("High", 0)) if pd.notna(row.get("High")) else None,
            "low": float(row.get("Low", 0)) if pd.notna(row.get("Low")) else None,
            "close": float(row.get("Close", 0)) if pd.notna(row.get("Close")) else None,
            "volume": int(row.get("Volume", 0)) if pd.notna(row.get("Volume")) else None,
            "adj_close": float(row.get("Adj Close", 0)) if pd.notna(row.get("Adj Close")) else None,
            "source": "yfinance",
        })

    logger.info("Fetched %d rows for %s (%s)", len(rows), commodity.symbol, commodity.yf_ticker)
    return rows


def fetch_batch_commodities(
    symbols: list[str] | None = None,
    start: str | None = None,
    end: str | None = None,
    period: str = "1y",
    categories: list[str] | None = None,
    pk_high_only: bool = False,
) -> dict[str, list[dict]]:
    """Fetch OHLCV data for multiple commodities from yfinance.

    Args:
        symbols: List of commodity symbols to fetch. If None, fetches all with yf_ticker.
        start: Start date for all.
        end: End date for all.
        period: yfinance period if start is None.
        categories: Filter by categories (e.g., ["metals", "energy"]).
        pk_high_only: If True, only fetch HIGH pk_relevance commodities.

    Returns:
        Dict mapping symbol -> list of OHLCV dicts.
    """
    # Build target list
    if symbols:
        targets = [COMMODITY_UNIVERSE[s] for s in symbols if s in COMMODITY_UNIVERSE and COMMODITY_UNIVERSE[s].yf_ticker]
    else:
        targets = [c for c in COMMODITY_UNIVERSE.values() if c.yf_ticker]

    if categories:
        cats = {c.lower() for c in categories}
        targets = [c for c in targets if c.category in cats]

    if pk_high_only:
        targets = [c for c in targets if c.pk_relevance == "HIGH"]

    results = {}
    for commodity in targets:
        rows = fetch_single_commodity(commodity, start=start, end=end, period=period)
        if rows:
            results[commodity.symbol] = rows

    return results


def fetch_fx_rates(
    start: str | None = None,
    end: str | None = None,
    period: str = "1y",
) -> dict[str, list[dict]]:
    """Fetch FX rate OHLCV for all currency pairs in the universe.

    Returns dict mapping pair symbol (e.g. USD_PKR) -> list of OHLCV dicts.
    """
    fx_commodities = [c for c in COMMODITY_UNIVERSE.values() if c.category == "fx" and c.yf_ticker]
    results = {}
    for commodity in fx_commodities:
        rows = fetch_single_commodity(commodity, start=start, end=end, period=period)
        if rows:
            results[commodity.symbol] = rows
    return results


def get_latest_usd_pkr(period: str = "5d") -> float | None:
    """Fetch the most recent USD/PKR rate from yfinance.

    Returns the latest close price, or None if unavailable.
    """
    yf = _import_yfinance()
    try:
        df = yf.Ticker("PKR=X").history(period=period)
        if df is not None and not df.empty:
            return float(df["Close"].iloc[-1])
    except Exception as e:
        logger.warning("Failed to fetch USD/PKR: %s", e)
    return None
