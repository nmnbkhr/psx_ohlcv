"""
Phase 1 Analytics: Performance metrics and rankings for instruments.

This module provides analytics functions for computing returns, volatility,
and relative strength for ETFs, REITs, and Indexes.
"""

import sqlite3
from datetime import datetime, timedelta

import pandas as pd

from .db import (
    get_instruments,
    get_ohlcv_instrument,
    get_eod_ohlcv,
    upsert_instrument_ranking,
)
from .instruments import NON_EQUITY_TYPES, DEFAULT_BENCHMARK_ID


def compute_returns(df: pd.DataFrame, periods: list[int] | None = None) -> dict:
    """
    Compute returns for various periods.

    Args:
        df: DataFrame with 'date' and 'close' columns, sorted by date DESC
        periods: List of periods in trading days (default: [1, 5, 21, 63, 252])

    Returns:
        Dict with period keys and return values (as percentages)
        e.g., {'return_1d': 2.5, 'return_1w': -1.2, ...}
    """
    if df.empty or "close" not in df.columns:
        return {}

    if periods is None:
        periods = [1, 5, 21, 63, 252]  # 1d, 1w, 1m, 3m, 1y

    # Ensure sorted by date descending
    df = df.sort_values("date", ascending=False).reset_index(drop=True)

    results = {}
    period_labels = {1: "1d", 5: "1w", 21: "1m", 63: "3m", 252: "1y"}

    current_close = df["close"].iloc[0] if len(df) > 0 else None

    if current_close is None or current_close == 0:
        return {}

    for period in periods:
        label = period_labels.get(period, f"{period}d")
        key = f"return_{label}"

        if len(df) > period:
            past_close = df["close"].iloc[period]
            if past_close and past_close != 0:
                ret = ((current_close - past_close) / past_close) * 100
                results[key] = round(ret, 4)

    return results


def compute_volatility(df: pd.DataFrame, windows: list[int] | None = None) -> dict:
    """
    Compute rolling volatility (annualized standard deviation of returns).

    Args:
        df: DataFrame with 'date' and 'close' columns
        windows: List of rolling windows in days (default: [21, 63])

    Returns:
        Dict with window keys and volatility values (as percentages)
        e.g., {'vol_1m': 25.5, 'vol_3m': 22.1}
    """
    if df.empty or "close" not in df.columns or len(df) < 5:
        return {}

    if windows is None:
        windows = [21, 63]  # 1m, 3m

    # Sort by date ascending for proper return calculation
    df = df.sort_values("date", ascending=True).copy()

    # Calculate daily returns
    df["return"] = df["close"].pct_change()

    results = {}
    window_labels = {21: "1m", 63: "3m"}

    for window in windows:
        label = window_labels.get(window, f"{window}d")
        key = f"vol_{label}"

        if len(df) >= window:
            # Calculate rolling std and annualize (252 trading days)
            recent_returns = df["return"].tail(window).dropna()
            if len(recent_returns) >= window // 2:  # Require at least half the window
                std = recent_returns.std()
                annualized_vol = std * (252 ** 0.5) * 100
                results[key] = round(annualized_vol, 4)

    return results


def compute_relative_strength(
    asset_df: pd.DataFrame,
    benchmark_df: pd.DataFrame,
    periods: list[int] | None = None,
) -> dict:
    """
    Compute relative strength vs benchmark.

    Relative strength = (asset_return / benchmark_return) - 1

    Args:
        asset_df: Asset OHLCV DataFrame
        benchmark_df: Benchmark OHLCV DataFrame
        periods: List of periods in days

    Returns:
        Dict with period keys and relative strength values
        e.g., {'rs_1m': 0.05} means 5% outperformance
    """
    if asset_df.empty or benchmark_df.empty:
        return {}

    if periods is None:
        periods = [21, 63]  # 1m, 3m

    asset_returns = compute_returns(asset_df, periods)
    benchmark_returns = compute_returns(benchmark_df, periods)

    results = {}
    period_labels = {21: "1m", 63: "3m"}

    for period in periods:
        label = period_labels.get(period, f"{period}d")
        asset_key = f"return_{label}"
        rs_key = f"rs_{label}"

        if asset_key in asset_returns and asset_key in benchmark_returns:
            asset_ret = asset_returns[asset_key]
            bench_ret = benchmark_returns[asset_key]

            if bench_ret != 0:
                # Relative strength as difference
                rs = asset_ret - bench_ret
                results[rs_key] = round(rs, 4)

    return results


def _get_instrument_ohlcv(
    con: sqlite3.Connection,
    instrument_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 300,
) -> pd.DataFrame:
    """
    Get OHLCV data for an instrument, trying multiple sources.

    First tries eod_ohlcv by symbol (for equities, ETFs, REITs synced via DPS).
    Falls back to ohlcv_instruments table (for indices, legacy data).

    Args:
        con: Database connection
        instrument_id: Instrument ID
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        limit: Max rows to return

    Returns:
        DataFrame with OHLCV data
    """
    # Look up instrument to get symbol
    try:
        cur = con.execute(
            "SELECT symbol FROM instruments WHERE instrument_id = ?",
            (instrument_id,)
        )
        row = cur.fetchone()
        symbol = row[0] if row else None
    except Exception:
        symbol = None

    # Try eod_ohlcv first (equities, ETFs, REITs synced via psxsync eod)
    if symbol:
        df = get_eod_ohlcv(con, symbol=symbol, start_date=start_date, end_date=end_date, limit=limit)
        if not df.empty:
            return df

    # Fall back to ohlcv_instruments table (indices, legacy sync)
    return get_ohlcv_instrument(con, instrument_id, start_date=start_date, end_date=end_date, limit=limit)


def compute_all_metrics(
    con: sqlite3.Connection,
    instrument_id: str,
    benchmark_id: str | None = DEFAULT_BENCHMARK_ID,
) -> dict:
    """
    Compute all metrics for an instrument.

    Args:
        con: Database connection
        instrument_id: Instrument ID
        benchmark_id: Benchmark instrument ID (default: KSE-100)

    Returns:
        Dict with all computed metrics
    """
    # Get instrument OHLCV (handles ETF/REIT/INDEX vs other instruments)
    df = _get_instrument_ohlcv(con, instrument_id, limit=300)

    if df.empty:
        return {"instrument_id": instrument_id, "error": "no_data"}

    metrics = {"instrument_id": instrument_id}

    # Returns
    returns = compute_returns(df)
    metrics.update(returns)

    # Volatility
    volatility = compute_volatility(df)
    metrics.update(volatility)

    # Relative strength vs benchmark
    if benchmark_id:
        benchmark_df = _get_instrument_ohlcv(con, benchmark_id, limit=300)
        if not benchmark_df.empty:
            rs = compute_relative_strength(df, benchmark_df)
            metrics.update(rs)

    return metrics


def compute_rankings(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
    instrument_types: list[str] | None = None,
    top_n: int = 10,
    store: bool = True,
) -> dict:
    """
    Compute performance rankings for instruments.

    Args:
        con: Database connection
        as_of_date: Date for rankings (default: today)
        instrument_types: List of types like ['ETF', 'REIT', 'INDEX']
        top_n: Number of top performers to include
        store: Whether to store rankings in database

    Returns:
        Dict with success status and instruments_ranked count
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    if instrument_types is None:
        instrument_types = NON_EQUITY_TYPES

    # Get all instruments of requested types
    all_instruments = []
    for inst_type in instrument_types:
        instruments = get_instruments(con, instrument_type=inst_type, active_only=True)
        for inst in instruments:
            inst["instrument_type"] = inst_type
            all_instruments.append(inst)

    # Compute metrics for each instrument
    all_metrics = []
    for inst in all_instruments:
        metrics = compute_all_metrics(con, inst["instrument_id"])
        if "error" not in metrics:
            metrics["symbol"] = inst["symbol"]
            metrics["name"] = inst.get("name")
            metrics["instrument_type"] = inst.get("instrument_type")
            all_metrics.append(metrics)

    if not all_metrics:
        return {"success": False, "error": "no_data", "instruments_ranked": 0}

    df = pd.DataFrame(all_metrics)

    # Store rankings if requested
    stored_count = 0
    if store:
        for _, row in df.iterrows():
            ranking_record = {
                "as_of_date": as_of_date,
                "instrument_id": row["instrument_id"],
                "instrument_type": row.get("instrument_type"),
                "return_1m": row.get("return_1m"),
                "return_3m": row.get("return_3m"),
                "return_6m": row.get("return_6m"),
                "return_1y": row.get("return_1y"),
                "volatility_30d": row.get("vol_1m"),
                "relative_strength": row.get("rs_1m"),
            }
            if upsert_instrument_ranking(con, ranking_record):
                stored_count += 1

    return {
        "success": True,
        "instruments_ranked": len(all_metrics),
        "stored": stored_count,
    }


def compute_rankings_by_type(
    con: sqlite3.Connection,
    instrument_type: str,
    as_of_date: str | None = None,
    top_n: int = 10,
) -> dict[str, list[dict]]:
    """
    Compute performance rankings for a single instrument type.

    Args:
        con: Database connection
        instrument_type: 'ETF', 'REIT', or 'INDEX'
        as_of_date: Date for rankings (default: today)
        top_n: Number of top performers to include

    Returns:
        Dict with rank_type keys and lists of ranked instruments
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    # Get all instruments of this type
    instruments = get_instruments(con, instrument_type=instrument_type, active_only=True)

    # Compute metrics for each instrument
    all_metrics = []
    for inst in instruments:
        metrics = compute_all_metrics(con, inst["instrument_id"])
        if "error" not in metrics:
            metrics["symbol"] = inst["symbol"]
            metrics["name"] = inst.get("name")
            all_metrics.append(metrics)

    if not all_metrics:
        return {}

    df = pd.DataFrame(all_metrics)

    rankings = {}

    # Rank by each metric type
    metric_cols = [col for col in df.columns if col.startswith(("return_", "vol_"))]

    for col in metric_cols:
        if col in df.columns:
            # For returns, higher is better
            # For volatility, lower is better (but we still show top absolute)
            ascending = col.startswith("vol_")

            ranked = df[df[col].notna()].sort_values(
                col, ascending=ascending
            ).head(top_n)

            rankings[col] = [
                {
                    "rank": i + 1,
                    "instrument_id": row["instrument_id"],
                    "symbol": row["symbol"],
                    "name": row.get("name"),
                    "value": row[col],
                }
                for i, (_, row) in enumerate(ranked.iterrows())
            ]

    return rankings


def get_rankings(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
    instrument_types: list[str] | None = None,
    top_n: int = 10,
) -> list[dict]:
    """
    Get stored rankings from database.

    Args:
        con: Database connection
        as_of_date: Date for rankings (default: most recent)
        instrument_types: List of types to filter
        top_n: Number of top performers to return

    Returns:
        List of ranking records sorted by return_1m descending
    """
    if instrument_types is None:
        instrument_types = NON_EQUITY_TYPES

    # Build query
    type_placeholders = ",".join("?" * len(instrument_types))

    if as_of_date:
        query = f"""
            SELECT r.*, i.symbol, i.name
            FROM instrument_rankings r
            JOIN instruments i ON r.instrument_id = i.instrument_id
            WHERE r.as_of_date = ?
            AND r.instrument_type IN ({type_placeholders})
            ORDER BY r.return_1m DESC NULLS LAST
            LIMIT ?
        """
        params = [as_of_date] + instrument_types + [top_n]
    else:
        # Get most recent date
        query = f"""
            SELECT r.*, i.symbol, i.name
            FROM instrument_rankings r
            JOIN instruments i ON r.instrument_id = i.instrument_id
            WHERE r.as_of_date = (
                SELECT MAX(as_of_date) FROM instrument_rankings
            )
            AND r.instrument_type IN ({type_placeholders})
            ORDER BY r.return_1m DESC NULLS LAST
            LIMIT ?
        """
        params = instrument_types + [top_n]

    try:
        cur = con.execute(query, params)
        return [dict(row) for row in cur.fetchall()]
    except Exception:
        return []


def store_rankings(
    con: sqlite3.Connection,
    instrument_type: str,
    as_of_date: str | None = None,
    top_n: int = 10,
) -> int:
    """
    Compute and store rankings in the database.

    Args:
        con: Database connection
        instrument_type: 'ETF', 'REIT', or 'INDEX'
        as_of_date: Date for rankings
        top_n: Number of top performers to store

    Returns:
        Number of ranking records stored
    """
    if as_of_date is None:
        as_of_date = datetime.now().strftime("%Y-%m-%d")

    rankings = compute_rankings_by_type(con, instrument_type, as_of_date, top_n)

    count = 0
    for rank_type, ranked_items in rankings.items():
        for item in ranked_items:
            ranking_record = {
                "as_of_date": as_of_date,
                "instrument_type": instrument_type,
                "rank_type": rank_type,
                "rank": item["rank"],
                "instrument_id": item["instrument_id"],
                "value": item["value"],
            }
            if upsert_instrument_ranking(con, ranking_record):
                count += 1

    return count


def compute_and_store_all_rankings(
    con: sqlite3.Connection,
    as_of_date: str | None = None,
    top_n: int = 10,
    instrument_types: list[str] | None = None,
) -> dict:
    """
    Compute and store rankings for all instrument types.

    Args:
        con: Database connection
        as_of_date: Date for rankings
        top_n: Number of top performers
        instrument_types: Types to compute (default: ['ETF', 'REIT', 'INDEX'])

    Returns:
        Summary dict with counts by type
    """
    if instrument_types is None:
        instrument_types = NON_EQUITY_TYPES

    results = {}
    for inst_type in instrument_types:
        count = store_rankings(con, inst_type, as_of_date, top_n)
        results[inst_type] = count

    results["total"] = sum(results.values())
    return results


def get_normalized_performance(
    con: sqlite3.Connection,
    instrument_ids: list[str],
    start_date: str | None = None,
    end_date: str | None = None,
    base: float = 100.0,
) -> pd.DataFrame:
    """
    Get normalized performance (rebased to 100) for comparison.

    Args:
        con: Database connection
        instrument_ids: List of instrument IDs to compare
        start_date: Start date (default: 90 days ago)
        end_date: End date (default: today)
        base: Base value for normalization (default: 100)

    Returns:
        DataFrame with date index and columns for each instrument
    """
    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    if start_date is None:
        start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")

    dfs = []
    for inst_id in instrument_ids:
        df = _get_instrument_ohlcv(con, inst_id, start_date=start_date, end_date=end_date)
        if not df.empty:
            df = df.sort_values("date").set_index("date")
            # Normalize to base
            first_close = df["close"].iloc[0]
            if first_close and first_close != 0:
                df[inst_id] = (df["close"] / first_close) * base
                dfs.append(df[[inst_id]])

    if not dfs:
        return pd.DataFrame()

    # Merge all on date
    result = dfs[0]
    for df in dfs[1:]:
        result = result.join(df, how="outer")

    return result.sort_index()
