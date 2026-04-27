"""PMEX Daily File Export — save daily snapshots as fast, portable files.

Three file types, all stored under /mnt/e/psxdata/commod/pmex_daily/:
  1. EOD snapshot    — one JSON per day with all contracts (latest close)
  2. OHLC Parquet    — daily OHLCV from pmex_ohlc (fast columnar reads)
  3. Margins Parquet — daily margins snapshot (fast columnar reads)

These files serve as:
  - Fast backup (no DB needed to read)
  - Portable export (share via USB/cloud)
  - Faster analytics (Parquet is 5-10x faster than SQLite for scans)

Usage:
  from pakfindata.commodities.pmex_daily_files import export_daily, export_all_history

  # Export today's data to files
  export_daily()

  # Export all historical data to Parquet
  export_all_history()
"""

from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

logger = logging.getLogger("pakfindata.commodities.pmex_daily_files")

# ─────────────────────────────────────────────────────────────────────────────
# Paths
# ─────────────────────────────────────────────────────────────────────────────

DAILY_ROOT = Path("/mnt/e/psxdata/commod/pmex_daily")
EOD_DIR = DAILY_ROOT / "eod_json"
OHLC_DIR = DAILY_ROOT / "ohlc_parquet"
MARGINS_DIR = DAILY_ROOT / "margins_parquet"
INTRADAY_ROLLUP_DIR = DAILY_ROOT / "intraday_rollup"


def _ensure_dirs():
    for d in [DAILY_ROOT, EOD_DIR, OHLC_DIR, MARGINS_DIR, INTRADAY_ROLLUP_DIR]:
        d.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────────────────────────────────────
# EOD JSON snapshot (from pmex_market_watch in psx.sqlite)
# ─────────────────────────────────────────────────────────────────────────────


def export_eod_json(target_date: str | None = None) -> Path | None:
    """Export latest PMEX market watch snapshot as JSON file.

    Args:
        target_date: "YYYY-MM-DD" or None for latest.

    Returns:
        Path to saved file, or None if no data.
    """
    from ..db.connection import connect
    from .models import get_pmex_latest

    _ensure_dirs()
    con = connect()

    if target_date:
        rows = con.execute(
            "SELECT * FROM pmex_market_watch WHERE snapshot_date = ? ORDER BY category, contract",
            (target_date,),
        ).fetchall()
        data = [dict(r) for r in rows]
        dt = target_date
    else:
        data = get_pmex_latest(con)
        dt = data[0]["snapshot_date"] if data else date.today().isoformat()

    if not data:
        logger.info("No EOD data to export for %s", dt)
        return None

    fpath = EOD_DIR / f"pmex_eod_{dt}.json"
    with open(fpath, "w") as f:
        json.dump(data, f, indent=2, default=str)

    logger.info("Exported EOD JSON: %s (%d contracts)", fpath, len(data))
    return fpath


# ─────────────────────────────────────────────────────────────────────────────
# OHLC Parquet (from pmex_ohlc in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def export_ohlc_parquet(
    start_date: str | None = None,
    end_date: str | None = None,
) -> Path | None:
    """Export PMEX OHLC data as Parquet file.

    Args:
        start_date: Start date filter (default: all data).
        end_date: End date filter (default: today).

    Returns:
        Path to saved Parquet file, or None if no data.
    """
    from .commod_db import get_commod_connection

    _ensure_dirs()
    con = get_commod_connection()

    conditions = []
    params: list = []
    if start_date:
        conditions.append("trading_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("trading_date <= ?")
        params.append(end_date)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM pmex_ohlc {where} ORDER BY trading_date, symbol"

    df = pd.read_sql_query(sql, con, params=params)
    if df.empty:
        logger.info("No OHLC data to export")
        return None

    # Filename reflects date range
    min_d = df["trading_date"].min()
    max_d = df["trading_date"].max()
    fpath = OHLC_DIR / f"pmex_ohlc_{min_d}_to_{max_d}.parquet"
    df.to_parquet(fpath, index=False, engine="pyarrow")

    logger.info("Exported OHLC Parquet: %s (%d rows, %d symbols)", fpath, len(df), df["symbol"].nunique())
    return fpath


def export_ohlc_daily_parquet(target_date: str | None = None) -> Path | None:
    """Export a single day of OHLC as a small Parquet file.

    Args:
        target_date: "YYYY-MM-DD" or None for today.

    Returns:
        Path to saved file.
    """
    from .commod_db import get_commod_connection

    _ensure_dirs()
    dt = target_date or date.today().isoformat()
    con = get_commod_connection()

    df = pd.read_sql_query(
        "SELECT * FROM pmex_ohlc WHERE trading_date = ? ORDER BY symbol",
        con, params=[dt],
    )
    if df.empty:
        return None

    fpath = OHLC_DIR / f"pmex_ohlc_{dt}.parquet"
    df.to_parquet(fpath, index=False, engine="pyarrow")

    logger.info("Exported daily OHLC: %s (%d rows)", fpath, len(df))
    return fpath


# ─────────────────────────────────────────────────────────────────────────────
# Margins Parquet (from pmex_margins in commod.db)
# ─────────────────────────────────────────────────────────────────────────────


def export_margins_parquet(
    start_date: str | None = None,
    end_date: str | None = None,
) -> Path | None:
    """Export PMEX margins data as Parquet file.

    Returns:
        Path to saved Parquet file.
    """
    from .commod_db import get_commod_connection

    _ensure_dirs()
    con = get_commod_connection()

    conditions = []
    params: list = []
    if start_date:
        conditions.append("report_date >= ?")
        params.append(start_date)
    if end_date:
        conditions.append("report_date <= ?")
        params.append(end_date)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    sql = f"SELECT * FROM pmex_margins {where} ORDER BY report_date, contract_code"

    df = pd.read_sql_query(sql, con, params=params)
    if df.empty:
        return None

    min_d = df["report_date"].min()
    max_d = df["report_date"].max()
    fpath = MARGINS_DIR / f"pmex_margins_{min_d}_to_{max_d}.parquet"
    df.to_parquet(fpath, index=False, engine="pyarrow")

    logger.info("Exported Margins Parquet: %s (%d rows)", fpath, len(df))
    return fpath


def export_margins_daily_parquet(target_date: str | None = None) -> Path | None:
    """Export a single day of margins as Parquet."""
    from .commod_db import get_commod_connection

    _ensure_dirs()
    dt = target_date or date.today().isoformat()
    con = get_commod_connection()

    df = pd.read_sql_query(
        "SELECT * FROM pmex_margins WHERE report_date = ? ORDER BY contract_code",
        con, params=[dt],
    )
    if df.empty:
        return None

    fpath = MARGINS_DIR / f"pmex_margins_{dt}.parquet"
    df.to_parquet(fpath, index=False, engine="pyarrow")

    logger.info("Exported daily Margins: %s (%d rows)", fpath, len(df))
    return fpath


# ─────────────────────────────────────────────────────────────────────────────
# Intraday Rollup — aggregate intraday snapshots into daily OHLCV bars
# ─────────────────────────────────────────────────────────────────────────────


def rollup_intraday_to_ohlcv(target_date: str | None = None) -> Path | None:
    """Build OHLCV bars from intraday snapshots for a given date.

    Uses pmex_intraday_snapshots table. Computes per-contract:
      open = first last_price, high = max, low = min, close = last last_price,
      volume = max total_vol (cumulative), trades = count of polls with volume change.

    Returns:
        Path to Parquet file with rolled-up OHLCV.
    """
    from .commod_db import get_commod_connection

    _ensure_dirs()
    dt = target_date or date.today().isoformat()
    con = get_commod_connection()

    df = pd.read_sql_query(
        """
        SELECT contract, category, snapshot_ts, last_price, bid, ask,
               total_vol, mid_price, spread_pct
        FROM pmex_intraday_snapshots
        WHERE snapshot_date = ? AND last_price > 0
        ORDER BY contract, snapshot_ts
        """,
        con, params=[dt],
    )
    if df.empty:
        return None

    # Aggregate per contract
    rollups = []
    for contract, group in df.groupby("contract"):
        group = group.sort_values("snapshot_ts")
        rollups.append({
            "date": dt,
            "contract": contract,
            "category": group["category"].iloc[0],
            "open": group["last_price"].iloc[0],
            "high": group["last_price"].max(),
            "low": group["last_price"].min(),
            "close": group["last_price"].iloc[-1],
            "volume": int(group["total_vol"].max()),
            "polls": len(group),
            "avg_spread_pct": round(group["spread_pct"].mean(), 6) if group["spread_pct"].notna().any() else None,
            "avg_mid_price": round(group["mid_price"].mean(), 4) if group["mid_price"].notna().any() else None,
            "first_poll": group["snapshot_ts"].iloc[0],
            "last_poll": group["snapshot_ts"].iloc[-1],
        })

    rollup_df = pd.DataFrame(rollups)
    fpath = INTRADAY_ROLLUP_DIR / f"pmex_intraday_rollup_{dt}.parquet"
    rollup_df.to_parquet(fpath, index=False, engine="pyarrow")

    logger.info("Intraday rollup %s: %d contracts, %d total polls", dt, len(rollups), len(df))
    return fpath


# ─────────────────────────────────────────────────────────────────────────────
# High-level daily export (all three types)
# ─────────────────────────────────────────────────────────────────────────────


def export_daily(target_date: str | None = None) -> dict:
    """Export all daily files: EOD JSON + OHLC Parquet + Margins Parquet + Intraday rollup.

    Args:
        target_date: "YYYY-MM-DD" or None for today.

    Returns:
        Dict with keys: eod_json, ohlc_parquet, margins_parquet, intraday_rollup
        (paths or None if no data).
    """
    dt = target_date or date.today().isoformat()
    result = {}

    try:
        result["eod_json"] = str(export_eod_json(dt)) if export_eod_json(dt) else None
    except Exception as e:
        result["eod_json"] = None
        logger.warning("EOD JSON export failed: %s", e)

    try:
        p = export_ohlc_daily_parquet(dt)
        result["ohlc_parquet"] = str(p) if p else None
    except Exception as e:
        result["ohlc_parquet"] = None
        logger.warning("OHLC Parquet export failed: %s", e)

    try:
        p = export_margins_daily_parquet(dt)
        result["margins_parquet"] = str(p) if p else None
    except Exception as e:
        result["margins_parquet"] = None
        logger.warning("Margins Parquet export failed: %s", e)

    try:
        p = rollup_intraday_to_ohlcv(dt)
        result["intraday_rollup"] = str(p) if p else None
    except Exception as e:
        result["intraday_rollup"] = None
        logger.warning("Intraday rollup failed: %s", e)

    logger.info("Daily export for %s: %s", dt, result)
    return result


def export_all_history() -> dict:
    """Export complete OHLC + Margins history as single Parquet files.

    Returns:
        Dict with keys: ohlc_parquet, margins_parquet (paths).
    """
    result = {}
    try:
        p = export_ohlc_parquet()
        result["ohlc_parquet"] = str(p) if p else None
    except Exception as e:
        result["ohlc_parquet"] = None
        logger.warning("Full OHLC export failed: %s", e)

    try:
        p = export_margins_parquet()
        result["margins_parquet"] = str(p) if p else None
    except Exception as e:
        result["margins_parquet"] = None
        logger.warning("Full Margins export failed: %s", e)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# File inventory
# ─────────────────────────────────────────────────────────────────────────────


def list_exported_files() -> dict:
    """List all exported files grouped by type.

    Returns:
        Dict with keys: eod_json, ohlc_parquet, margins_parquet, intraday_rollup.
        Each is a list of dicts with: date, path, size_kb.
    """
    _ensure_dirs()
    result = {}

    for label, directory, ext in [
        ("eod_json", EOD_DIR, "*.json"),
        ("ohlc_parquet", OHLC_DIR, "*.parquet"),
        ("margins_parquet", MARGINS_DIR, "*.parquet"),
        ("intraday_rollup", INTRADAY_ROLLUP_DIR, "*.parquet"),
    ]:
        files = []
        for fpath in sorted(directory.glob(ext)):
            files.append({
                "name": fpath.name,
                "path": str(fpath),
                "size_kb": round(fpath.stat().st_size / 1024, 1),
            })
        result[label] = files

    return result
