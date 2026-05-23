"""PMEX Analytics — derived metrics from existing PMEX data.

All functions are read-only: they query existing tables and return DataFrames/dicts.

Data sources:
  - pmex_market_watch (psx.sqlite) — bid/ask, OHLC, volume, change per contract per day
  - pmex_ohlc (commod.db) — daily OHLCV + settlement price + FX rate
  - pmex_margins (commod.db) — margin requirements, limit bands
"""

from __future__ import annotations

import logging
import sqlite3
from datetime import date, timedelta

import numpy as np
import pandas as pd

from .pmex_contract_calendar import (
    get_all_base_products,
    get_contract_chain,
    parse_contract,
)

logger = logging.getLogger("pakfindata.commodities.pmex_analytics")


# ─────────────────────────────────────────────────────────────────────────────
# Intraday Spread Analysis (from pmex_intraday_snapshots in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def compute_intraday_spreads(
    con: sqlite3.Connection,
    contract: str | None = None,
    snapshot_date: str | None = None,
) -> pd.DataFrame:
    """Compute bid-ask spreads from intraday snapshots (commod.db).

    Uses the pmex_intraday_snapshots table which has pre-computed
    spread and spread_pct columns from the poller.

    Args:
        con: Connection to commod.db.
        contract: Optional single contract filter.
        snapshot_date: Date to analyze (default: today).

    Returns:
        DataFrame with columns: contract, snapshot_ts, category, bid, ask,
        spread, spread_pct, mid_price, last_price, total_vol.
    """
    dt = snapshot_date or date.today().isoformat()
    params: list = [dt]

    where = "WHERE snapshot_date = ?"
    if contract:
        where += " AND contract = ?"
        params.append(contract)

    sql = f"""
        SELECT contract, snapshot_ts, category, bid, ask,
               spread, spread_pct, mid_price, last_price, total_vol
        FROM pmex_intraday_snapshots
        {where}
        ORDER BY snapshot_ts, contract
    """
    rows = con.execute(sql, params).fetchall()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def intraday_volume_timeseries(
    con: sqlite3.Connection,
    contract: str,
    snapshot_date: str | None = None,
) -> pd.DataFrame:
    """Get intraday volume progression for a contract.

    total_vol in PMEX is cumulative, so we compute incremental volume
    between polls.

    Args:
        con: Connection to commod.db.
        contract: Contract name.
        snapshot_date: Date (default: today).

    Returns:
        DataFrame with: snapshot_ts, total_vol, incremental_vol, last_price.
    """
    dt = snapshot_date or date.today().isoformat()
    rows = con.execute(
        """
        SELECT snapshot_ts, total_vol, last_price
        FROM pmex_intraday_snapshots
        WHERE contract = ? AND snapshot_date = ?
        ORDER BY snapshot_ts
        """,
        (contract, dt),
    ).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df["incremental_vol"] = df["total_vol"].diff().clip(lower=0).fillna(0).astype(int)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Bid-Ask Spread Analysis (from pmex_market_watch in psx.sqlite)
# ─────────────────────────────────────────────────────────────────────────────


def compute_spreads(
    con: sqlite3.Connection,
    contract: str | None = None,
    days: int = 30,
) -> pd.DataFrame:
    """Compute bid-ask spreads from pmex_market_watch.

    Args:
        con: Connection to psx.sqlite.
        contract: Optional single contract filter.
        days: Lookback window in days.

    Returns:
        DataFrame with columns: contract, snapshot_date, category, bid, ask,
        spread_abs, spread_pct, mid_price.
    """
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    params: list = [cutoff]

    where = "WHERE snapshot_date >= ? AND bid > 0 AND ask > 0"
    if contract:
        where += " AND contract = ?"
        params.append(contract)

    sql = f"""
        SELECT contract, snapshot_date, category, bid, ask
        FROM pmex_market_watch
        {where}
        ORDER BY snapshot_date DESC, contract
    """
    rows = con.execute(sql, params).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df["spread_abs"] = df["ask"] - df["bid"]
    df["mid_price"] = (df["bid"] + df["ask"]) / 2
    df["spread_pct"] = (df["spread_abs"] / df["mid_price"] * 100).round(4)
    return df


def spread_summary(con: sqlite3.Connection) -> pd.DataFrame:
    """Latest bid-ask spread for all active contracts, grouped by category.

    Flags contracts where current spread > 2x their 30-day average.

    Args:
        con: Connection to psx.sqlite.

    Returns:
        DataFrame with columns: contract, category, bid, ask, spread_abs,
        spread_pct, avg_spread_pct_30d, spread_anomaly.
    """
    # Latest snapshot per contract
    latest_sql = """
        SELECT p.contract, p.category, p.bid, p.ask
        FROM pmex_market_watch p
        INNER JOIN (
            SELECT contract, MAX(snapshot_date) as max_date
            FROM pmex_market_watch
            GROUP BY contract
        ) latest ON p.contract = latest.contract AND p.snapshot_date = latest.max_date
        WHERE p.bid > 0 AND p.ask > 0
        ORDER BY p.category, p.contract
    """
    rows = con.execute(latest_sql).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df["spread_abs"] = df["ask"] - df["bid"]
    df["mid_price"] = (df["bid"] + df["ask"]) / 2
    df["spread_pct"] = (df["spread_abs"] / df["mid_price"] * 100).round(4)

    # 30-day average spread per contract
    cutoff = (date.today() - timedelta(days=30)).isoformat()
    avg_sql = """
        SELECT contract,
               AVG(CASE WHEN bid > 0 AND ask > 0
                   THEN (ask - bid) / ((bid + ask) / 2.0) * 100
                   ELSE NULL END) as avg_spread_pct_30d
        FROM pmex_market_watch
        WHERE snapshot_date >= ?
        GROUP BY contract
    """
    avg_rows = con.execute(avg_sql, (cutoff,)).fetchall()
    avg_df = pd.DataFrame([dict(r) for r in avg_rows])

    if not avg_df.empty:
        df = df.merge(avg_df, on="contract", how="left")
        df["spread_anomaly"] = df["spread_pct"] > (df["avg_spread_pct_30d"] * 2)
    else:
        df["avg_spread_pct_30d"] = None
        df["spread_anomaly"] = False

    return df


# ─────────────────────────────────────────────────────────────────────────────
# Volume Profile (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def volume_profile(
    con: sqlite3.Connection,
    symbol: str,
    lookback_days: int = 60,
) -> dict:
    """Volume analytics for a single PMEX symbol.

    Args:
        con: Connection to commod.db.
        symbol: PMEX OHLC symbol, e.g. "GO1OZ-JU26".
        lookback_days: Window for analysis.

    Returns:
        Dict with keys: symbol, total_volume, avg_daily_volume, max_volume,
        max_volume_date, zero_volume_days, volume_trend, data_days.
    """
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    rows = con.execute(
        """
        SELECT trading_date, traded_volume
        FROM pmex_ohlc
        WHERE symbol = ? AND trading_date >= ?
        ORDER BY trading_date
        """,
        (symbol, cutoff),
    ).fetchall()

    if not rows:
        return {
            "symbol": symbol, "total_volume": 0, "avg_daily_volume": 0,
            "max_volume": 0, "max_volume_date": None, "zero_volume_days": 0,
            "volume_trend": "no_data", "data_days": 0,
        }

    df = pd.DataFrame([dict(r) for r in rows])
    vols = df["traded_volume"].fillna(0)
    total = int(vols.sum())
    avg_vol = round(vols.mean(), 1)
    max_idx = vols.idxmax()
    zero_days = int((vols == 0).sum())

    # Trend: compare first half avg vs second half avg
    mid = len(vols) // 2
    if mid > 0:
        first_half = vols.iloc[:mid].mean()
        second_half = vols.iloc[mid:].mean()
        if second_half > first_half * 1.2:
            trend = "rising"
        elif second_half < first_half * 0.8:
            trend = "falling"
        else:
            trend = "stable"
    else:
        trend = "insufficient_data"

    return {
        "symbol": symbol,
        "total_volume": total,
        "avg_daily_volume": avg_vol,
        "max_volume": int(vols.max()),
        "max_volume_date": df.loc[max_idx, "trading_date"],
        "zero_volume_days": zero_days,
        "volume_trend": trend,
        "data_days": len(df),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Category Aggregates (from pmex_market_watch in psx.sqlite)
# ─────────────────────────────────────────────────────────────────────────────


def category_aggregates(
    con: sqlite3.Connection,
    snapshot_date: str | None = None,
) -> pd.DataFrame:
    """Aggregate metrics per PMEX category for a given date.

    Args:
        con: Connection to psx.sqlite.
        snapshot_date: Specific date (YYYY-MM-DD) or None for latest.

    Returns:
        DataFrame with columns: category, num_contracts, total_volume,
        avg_change_pct, gainers, losers, unchanged.
    """
    if snapshot_date:
        date_clause = "snapshot_date = ?"
        params: list = [snapshot_date]
    else:
        date_clause = "snapshot_date = (SELECT MAX(snapshot_date) FROM pmex_market_watch)"
        params = []

    sql = f"""
        SELECT category,
               COUNT(*) as num_contracts,
               SUM(COALESCE(total_vol, 0)) as total_volume,
               AVG(change_pct) as avg_change_pct,
               SUM(CASE WHEN change_pct > 0 THEN 1 ELSE 0 END) as gainers,
               SUM(CASE WHEN change_pct < 0 THEN 1 ELSE 0 END) as losers,
               SUM(CASE WHEN change_pct = 0 OR change_pct IS NULL THEN 1 ELSE 0 END) as unchanged
        FROM pmex_market_watch
        WHERE {date_clause}
        GROUP BY category
        ORDER BY total_volume DESC
    """
    rows = con.execute(sql, params).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df["avg_change_pct"] = df["avg_change_pct"].round(3)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Contract Rollover Detection (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def detect_rollovers(
    con: sqlite3.Connection,
    lookback_days: int = 10,
) -> list[dict]:
    """Detect volume crossovers between near-month and far-month contracts.

    A rollover signal fires when the far-month volume exceeds near-month
    volume for 2+ consecutive days within the lookback window.

    Args:
        con: Connection to commod.db (pmex_ohlc table).
        lookback_days: Days to look back.

    Returns:
        List of dicts with keys: base, commodity, near_contract, far_contract,
        near_avg_vol, far_avg_vol, crossover_days, signal.
    """
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()

    # Get all recent symbols
    symbols_rows = con.execute(
        "SELECT DISTINCT symbol FROM pmex_ohlc WHERE trading_date >= ?",
        (cutoff,),
    ).fetchall()
    all_symbols = [r["symbol"] for r in symbols_rows]
    if not all_symbols:
        return []

    bases = get_all_base_products(all_symbols)
    signals = []

    for base in bases:
        chain = get_contract_chain(base, all_symbols)
        if len(chain) < 2:
            continue

        near, far = chain[0], chain[1]

        # Fetch volume for both contracts
        near_vols = _get_volumes(con, near.raw, cutoff)
        far_vols = _get_volumes(con, far.raw, cutoff)

        if near_vols.empty or far_vols.empty:
            continue

        # Align on common dates
        merged = pd.merge(
            near_vols.rename(columns={"traded_volume": "near_vol"}),
            far_vols.rename(columns={"traded_volume": "far_vol"}),
            on="trading_date",
            how="inner",
        )
        if merged.empty:
            continue

        merged["far_leads"] = merged["far_vol"] > merged["near_vol"]
        crossover_days = int(merged["far_leads"].sum())

        # Check for 2+ consecutive far-leading days
        consecutive = _max_consecutive_true(merged["far_leads"].values)

        if consecutive >= 2:
            signals.append({
                "base": base,
                "commodity": near.commodity,
                "near_contract": near.raw,
                "far_contract": far.raw,
                "near_avg_vol": round(merged["near_vol"].mean(), 1),
                "far_avg_vol": round(merged["far_vol"].mean(), 1),
                "crossover_days": crossover_days,
                "consecutive_days": consecutive,
                "signal": "ROLLOVER",
            })

    return signals


def _get_volumes(con: sqlite3.Connection, symbol: str, cutoff: str) -> pd.DataFrame:
    """Helper: fetch traded_volume for a symbol from cutoff date."""
    rows = con.execute(
        """
        SELECT trading_date, traded_volume
        FROM pmex_ohlc
        WHERE symbol = ? AND trading_date >= ?
        ORDER BY trading_date
        """,
        (symbol, cutoff),
    ).fetchall()
    return pd.DataFrame([dict(r) for r in rows]) if rows else pd.DataFrame()


def _max_consecutive_true(arr) -> int:
    """Return the max run of consecutive True values in a boolean array."""
    max_run = 0
    current = 0
    for v in arr:
        if v:
            current += 1
            max_run = max(max_run, current)
        else:
            current = 0
    return max_run


# ─────────────────────────────────────────────────────────────────────────────
# Settlement vs Close (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def settlement_vs_close(
    con: sqlite3.Connection,
    symbol: str,
    limit: int = 90,
) -> pd.DataFrame:
    """Compare settlement_price to close for a PMEX symbol.

    Args:
        con: Connection to commod.db.
        symbol: PMEX OHLC symbol.
        limit: Max rows.

    Returns:
        DataFrame with columns: trading_date, close, settlement_price,
        diff_abs, diff_pct.
    """
    rows = con.execute(
        """
        SELECT trading_date, close, settlement_price
        FROM pmex_ohlc
        WHERE symbol = ? AND close > 0 AND settlement_price > 0
        ORDER BY trading_date DESC
        LIMIT ?
        """,
        (symbol, limit),
    ).fetchall()
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows])
    df["diff_abs"] = df["settlement_price"] - df["close"]
    df["diff_pct"] = (df["diff_abs"] / df["close"] * 100).round(4)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Returns & Drawdown (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def compute_returns(
    con: sqlite3.Connection,
    symbol: str,
    periods: list[int] | None = None,
) -> dict:
    """Compute period returns and max drawdown for a PMEX symbol.

    Args:
        con: Connection to commod.db.
        symbol: PMEX OHLC symbol.
        periods: List of lookback days for return calculation (default [1, 5, 20, 60]).

    Returns:
        Dict with keys: symbol, returns (dict of period→pct), max_drawdown_pct,
        max_drawdown_peak_date, latest_close, latest_date.
    """
    if periods is None:
        periods = [1, 5, 20, 60]

    max_lookback = max(periods) + 5
    rows = con.execute(
        """
        SELECT trading_date, close
        FROM pmex_ohlc
        WHERE symbol = ? AND close > 0
        ORDER BY trading_date DESC
        LIMIT ?
        """,
        (symbol, max_lookback),
    ).fetchall()
    if not rows:
        return {"symbol": symbol, "returns": {}, "max_drawdown_pct": None,
                "latest_close": None, "latest_date": None}

    df = pd.DataFrame([dict(r) for r in rows]).sort_values("trading_date")
    closes = df["close"].values
    dates = df["trading_date"].values

    latest_close = float(closes[-1])
    latest_date = dates[-1]

    # Period returns
    returns = {}
    for p in periods:
        if len(closes) > p:
            old = closes[-(p + 1)]
            ret = (latest_close - old) / old * 100
            returns[f"{p}d"] = round(ret, 3)
        else:
            returns[f"{p}d"] = None

    # Max drawdown
    running_max = np.maximum.accumulate(closes)
    drawdowns = (closes - running_max) / running_max * 100
    max_dd = float(np.min(drawdowns))
    peak_idx = int(np.argmax(running_max[:np.argmin(drawdowns) + 1]))

    return {
        "symbol": symbol,
        "returns": returns,
        "max_drawdown_pct": round(max_dd, 3),
        "max_drawdown_peak_date": dates[peak_idx] if peak_idx < len(dates) else None,
        "latest_close": latest_close,
        "latest_date": latest_date,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Gap Detection (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def detect_gaps(
    con: sqlite3.Connection,
    symbol: str,
    threshold_pct: float = 2.0,
    limit: int = 200,
) -> pd.DataFrame:
    """Find days where open gaps significantly from previous close.

    Args:
        con: Connection to commod.db.
        symbol: PMEX OHLC symbol.
        threshold_pct: Minimum absolute gap percentage to report.
        limit: Max rows to scan.

    Returns:
        DataFrame with columns: trading_date, prev_close, open, gap_abs, gap_pct, direction.
    """
    rows = con.execute(
        """
        SELECT trading_date, open, close
        FROM pmex_ohlc
        WHERE symbol = ? AND open > 0 AND close > 0
        ORDER BY trading_date DESC
        LIMIT ?
        """,
        (symbol, limit),
    ).fetchall()
    if len(rows) < 2:
        return pd.DataFrame()

    df = pd.DataFrame([dict(r) for r in rows]).sort_values("trading_date").reset_index(drop=True)
    df["prev_close"] = df["close"].shift(1)
    df = df.dropna(subset=["prev_close"])

    df["gap_abs"] = df["open"] - df["prev_close"]
    df["gap_pct"] = (df["gap_abs"] / df["prev_close"] * 100).round(4)
    df["direction"] = df["gap_pct"].apply(lambda x: "gap_up" if x > 0 else "gap_down")

    gaps = df[df["gap_pct"].abs() >= threshold_pct]
    return gaps[["trading_date", "prev_close", "open", "gap_abs", "gap_pct", "direction"]].copy()
