"""
FX Analytics module for Phase 2.

This module provides FX-related analytics:
- FX trend analysis (returns, volatility, trend indicators)
- FX-adjusted equity performance (read-only analytics)

All analytics are READ-ONLY and for informational purposes only.
No trading signals or execution logic.
"""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from .db import (
    get_fx_adjusted_metrics,
    get_fx_latest_rate,
    get_fx_ohlcv,
    upsert_fx_adjusted_metric,
)


def compute_fx_returns(df: pd.DataFrame, periods: list[int] | None = None) -> dict:
    """
    Compute FX returns for various periods.

    Args:
        df: DataFrame with 'date' and 'close' columns, sorted by date DESC
        periods: List of periods in trading days (default: [5, 21, 63])
            5 = ~1 week, 21 = ~1 month, 63 = ~3 months

    Returns:
        Dict with period keys and return values (as decimals)
    """
    if df.empty or "close" not in df.columns:
        return {}

    if periods is None:
        periods = [5, 21, 63]  # 1W, 1M, 3M

    # Ensure sorted by date descending
    df = df.sort_values("date", ascending=False).reset_index(drop=True)

    results = {}
    period_labels = {5: "1W", 21: "1M", 63: "3M", 126: "6M", 252: "1Y"}

    current_close = df["close"].iloc[0] if len(df) > 0 else None

    if current_close is None or current_close == 0:
        return {}

    for period in periods:
        label = period_labels.get(period, f"{period}D")
        key = f"return_{label}"

        if len(df) > period:
            past_close = df["close"].iloc[period]
            if past_close and past_close != 0:
                ret = (current_close - past_close) / past_close
                results[key] = round(ret, 6)

    return results


def compute_fx_volatility(df: pd.DataFrame, windows: list[int] | None = None) -> dict:
    """
    Compute FX volatility (annualized standard deviation of returns).

    Args:
        df: DataFrame with 'date' and 'close' columns
        windows: List of rolling windows in days (default: [21, 63])

    Returns:
        Dict with window keys and volatility values (as decimals)
    """
    if df.empty or "close" not in df.columns or len(df) < 5:
        return {}

    if windows is None:
        windows = [21, 63]  # 1M, 3M

    # Sort by date ascending for proper return calculation
    df = df.sort_values("date", ascending=True).copy()

    # Calculate daily returns
    df["return"] = df["close"].pct_change()

    results = {}
    window_labels = {21: "1M", 63: "3M"}

    for window in windows:
        label = window_labels.get(window, f"{window}D")
        key = f"vol_{label}"

        if len(df) >= window:
            recent_returns = df["return"].tail(window).dropna()
            if len(recent_returns) >= window // 2:
                std = recent_returns.std()
                # Annualize: multiply by sqrt(252)
                annualized_vol = std * (252 ** 0.5)
                results[key] = round(annualized_vol, 6)

    return results


def compute_fx_trend(df: pd.DataFrame, ma_period: int = 50) -> dict:
    """
    Compute FX trend indicators.

    Args:
        df: DataFrame with 'date' and 'close' columns
        ma_period: Moving average period for trend (default: 50)

    Returns:
        Dict with trend indicators
    """
    if df.empty or "close" not in df.columns or len(df) < ma_period:
        return {}

    # Sort by date ascending
    df = df.sort_values("date", ascending=True).copy()

    # Calculate moving average
    df["ma"] = df["close"].rolling(window=ma_period).mean()

    latest = df.iloc[-1]
    current_close = latest["close"]
    current_ma = latest["ma"]

    if pd.isna(current_ma) or current_ma == 0:
        return {}

    # Trend direction
    above_ma = current_close > current_ma
    pct_from_ma = (current_close - current_ma) / current_ma

    # Simple trend strength based on distance from MA
    if abs(pct_from_ma) < 0.01:
        trend_strength = "neutral"
    elif abs(pct_from_ma) < 0.03:
        trend_strength = "weak"
    elif abs(pct_from_ma) < 0.05:
        trend_strength = "moderate"
    else:
        trend_strength = "strong"

    return {
        "ma_period": ma_period,
        "current_close": round(current_close, 4),
        "moving_average": round(current_ma, 4),
        "above_ma": above_ma,
        "pct_from_ma": round(pct_from_ma, 4),
        "trend_direction": "up" if above_ma else "down",
        "trend_strength": trend_strength,
    }


def get_fx_analytics(
    con: sqlite3.Connection,
    pair: str,
) -> dict:
    """
    Get comprehensive FX analytics for a pair.

    Args:
        con: Database connection
        pair: FX pair (e.g., "USD/PKR")

    Returns:
        Dict with returns, volatility, and trend indicators
    """
    df = get_fx_ohlcv(con, pair, limit=300)

    if df.empty:
        return {"pair": pair, "error": "no_data"}

    analytics = {"pair": pair}

    # Latest rate
    latest = get_fx_latest_rate(con, pair)
    if latest:
        analytics["latest_date"] = latest.get("date")
        analytics["latest_close"] = latest.get("close")

    # Returns
    returns = compute_fx_returns(df)
    analytics.update(returns)

    # Volatility
    volatility = compute_fx_volatility(df)
    analytics.update(volatility)

    # Trend
    trend = compute_fx_trend(df)
    analytics["trend"] = trend

    return analytics


def compute_fx_adjusted_return(
    equity_return: float,
    fx_return: float,
) -> float:
    """
    Compute FX-adjusted return.

    For a PKR-denominated equity, the USD-adjusted return is:
    fx_adjusted_return = equity_return - fx_return

    If PKR depreciates (positive fx_return), the USD-adjusted
    return is lower than the PKR return.

    Args:
        equity_return: Equity return in local currency (decimal)
        fx_return: FX return (decimal, positive = depreciation)

    Returns:
        FX-adjusted return (decimal)
    """
    if equity_return is None or fx_return is None:
        return None

    return equity_return - fx_return


def compute_equity_fx_adjusted_metrics(
    con: sqlite3.Connection,
    symbol: str,
    fx_pair: str = "USD/PKR",
    periods: list[str] | None = None,
) -> list[dict]:
    """
    Compute FX-adjusted metrics for an equity symbol.

    Args:
        con: Database connection
        symbol: Equity symbol
        fx_pair: FX pair for adjustment (default: USD/PKR)
        periods: List of periods to compute (default: ['1W', '1M', '3M'])

    Returns:
        List of metric dicts for each period
    """
    if periods is None:
        periods = ["1W", "1M", "3M"]

    period_days = {"1W": 5, "1M": 21, "3M": 63, "6M": 126, "1Y": 252}

    # Get equity OHLCV
    try:
        equity_df = pd.read_sql_query(
            """
            SELECT date, close FROM eod_ohlcv
            WHERE symbol = ?
            ORDER BY date DESC
            LIMIT 300
            """,
            con,
            params=[symbol],
        )
    except Exception:
        return []

    if equity_df.empty:
        return []

    # Get FX OHLCV
    fx_df = get_fx_ohlcv(con, fx_pair, limit=300)

    if fx_df.empty:
        return []

    # Compute returns for each period
    period_list = [period_days[p] for p in periods if p in period_days]
    equity_returns = compute_fx_returns(equity_df, period_list)
    fx_returns = compute_fx_returns(fx_df, period_list)

    results = []
    as_of_date = datetime.now().strftime("%Y-%m-%d")

    for period in periods:
        key = f"return_{period}"
        eq_ret = equity_returns.get(key)
        fx_ret = fx_returns.get(key)

        if eq_ret is not None and fx_ret is not None:
            adj_ret = compute_fx_adjusted_return(eq_ret, fx_ret)

            metric = {
                "as_of_date": as_of_date,
                "symbol": symbol,
                "fx_pair": fx_pair,
                "equity_return": eq_ret,
                "fx_return": fx_ret,
                "fx_adjusted_return": adj_ret,
                "period": period,
            }
            results.append(metric)

    return results


def compute_and_store_fx_adjusted_metrics(
    con: sqlite3.Connection,
    symbols: list[str] | None = None,
    fx_pair: str = "USD/PKR",
    as_of_date: str | None = None,
) -> dict:
    """
    Compute and store FX-adjusted metrics for symbols.

    Args:
        con: Database connection
        symbols: List of equity symbols, or None for all with data
        fx_pair: FX pair for adjustment
        as_of_date: Date for metrics (default: today)

    Returns:
        Summary dict with counts
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    # Get symbols if not provided
    if symbols is None:
        try:
            cur = con.execute("""
                SELECT DISTINCT symbol FROM eod_ohlcv
                WHERE date >= date('now', '-30 days')
                ORDER BY symbol
            """)
            symbols = [row[0] for row in cur.fetchall()]
        except Exception:
            symbols = []

    if not symbols:
        return {"success": False, "error": "no_symbols"}

    stored = 0
    failed = 0

    for symbol in symbols:
        metrics = compute_equity_fx_adjusted_metrics(con, symbol, fx_pair)

        for metric in metrics:
            metric["as_of_date"] = as_of_date
            if upsert_fx_adjusted_metric(con, metric):
                stored += 1
            else:
                failed += 1

    return {
        "success": True,
        "symbols_processed": len(symbols),
        "metrics_stored": stored,
        "metrics_failed": failed,
    }


def get_fx_impact_summary(
    con: sqlite3.Connection,
    fx_pair: str = "USD/PKR",
    period: str = "1M",
    top_n: int = 20,
) -> list[dict]:
    """
    Get summary of FX impact on equities.

    Args:
        con: Database connection
        fx_pair: FX pair
        period: Period to analyze
        top_n: Number of top/bottom stocks to return

    Returns:
        List of metrics sorted by FX impact
    """
    metrics = get_fx_adjusted_metrics(
        con,
        fx_pair=fx_pair,
        period=period,
        limit=top_n * 2,
    )

    # Sort by difference (equity_return - fx_adjusted_return = fx_return)
    # Higher fx_return means more negative FX impact
    return sorted(
        metrics,
        key=lambda x: x.get("fx_adjusted_return") or 0,
        reverse=True,
    )[:top_n]


def get_normalized_fx_performance(
    con: sqlite3.Connection,
    pairs: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    base: float = 100.0,
) -> pd.DataFrame:
    """
    Get normalized FX performance for comparison.

    Args:
        con: Database connection
        pairs: List of FX pairs to compare
        start_date: Start date
        end_date: End date
        base: Base value for normalization (default: 100)

    Returns:
        DataFrame with date index and columns for each pair
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    dfs = []
    for pair in pairs:
        df = get_fx_ohlcv(con, pair, start_date=start_date, end_date=end_date)
        if not df.empty:
            df = df.sort_values("date").set_index("date")
            first_close = df["close"].iloc[0]
            if first_close and first_close != 0:
                df[pair] = (df["close"] / first_close) * base
                dfs.append(df[[pair]])

    if not dfs:
        return pd.DataFrame()

    result = dfs[0]
    for df in dfs[1:]:
        result = result.join(df, how="outer")

    return result.sort_index()
