"""
FX sync module for Phase 2.

This module handles syncing FX rate data from various sources
into the local database for analytics purposes.

FX data is READ-ONLY and used for macro context, not trading.
"""

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .db import (
    connect,
    get_fx_latest_date,
    get_fx_pairs,
    get_fx_sync_runs,
    init_schema,
    record_fx_sync_run,
    update_fx_sync_run,
    upsert_fx_ohlcv,
    upsert_fx_pair,
)
from .sources.fx import (
    fetch_fx_ohlcv,
    get_default_fx_pairs,
    normalize_fx_dataframe,
    save_fx_config,
)


@dataclass
class FXSyncSummary:
    """Summary of FX sync operation."""
    total: int = 0
    ok: int = 0
    failed: int = 0
    no_data: int = 0
    rows_upserted: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)


def seed_fx_pairs(
    db_path: Path | str | None = None,
    pairs: list[dict] | None = None,
) -> dict:
    """
    Seed default FX pairs into the database.

    Args:
        db_path: Database path
        pairs: List of pair dicts, or None for defaults

    Returns:
        Summary dict with counts
    """
    if pairs is None:
        pairs = get_default_fx_pairs()

    con = connect(db_path)
    init_schema(con)

    inserted = 0
    failed = 0

    for pair_data in pairs:
        if upsert_fx_pair(con, pair_data):
            inserted += 1
        else:
            failed += 1

    con.close()

    # Also save config file
    save_fx_config({"pairs": pairs})

    return {
        "success": True,
        "inserted": inserted,
        "failed": failed,
        "total": len(pairs),
    }


def sync_fx_pair(
    pair: str,
    db_path: Path | str | None = None,
    incremental: bool = True,
    source: str = "AUTO",
) -> tuple[int, str | None]:
    """
    Sync OHLCV data for a single FX pair.

    Args:
        pair: FX pair (e.g., "USD/PKR")
        db_path: Database path
        incremental: If True, only fetch new data
        source: Data source

    Returns:
        Tuple of (rows_upserted, error_message)
    """
    con = connect(db_path)
    init_schema(con)

    try:
        # Determine start date for incremental sync
        start_date = None
        if incremental:
            latest = get_fx_latest_date(con, pair)
            if latest:
                # Start from day after latest
                from datetime import datetime, timedelta
                latest_dt = datetime.strptime(latest, "%Y-%m-%d")
                start_date = (latest_dt + timedelta(days=1)).strftime("%Y-%m-%d")

        # Fetch data
        df = fetch_fx_ohlcv(pair, start_date=start_date, source=source)

        if df.empty:
            con.close()
            return 0, None

        # Normalize and upsert
        df = normalize_fx_dataframe(df)
        rows = upsert_fx_ohlcv(con, pair, df)

        con.close()
        return rows, None

    except Exception as e:
        con.close()
        return 0, str(e)


def sync_fx_pairs(
    pairs: list[str] | None = None,
    db_path: Path | str | None = None,
    incremental: bool = True,
    source: str = "AUTO",
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> FXSyncSummary:
    """
    Sync OHLCV data for multiple FX pairs.

    Args:
        pairs: List of pairs to sync, or None for all active pairs
        db_path: Database path
        incremental: If True, only fetch new data
        source: Data source
        progress_callback: Optional callback(current, total, pair)

    Returns:
        FXSyncSummary with results
    """
    con = connect(db_path)
    init_schema(con)

    # Get pairs to sync
    if pairs is None:
        pair_records = get_fx_pairs(con, active_only=True)
        pairs = [p["pair"] for p in pair_records]

    if not pairs:
        con.close()
        return FXSyncSummary()

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_fx_sync_run(con, run_id, pairs)

    summary = FXSyncSummary(total=len(pairs))

    for i, pair in enumerate(pairs):
        if progress_callback:
            progress_callback(i + 1, len(pairs), pair)

        rows, error = sync_fx_pair(
            pair,
            db_path=db_path,
            incremental=incremental,
            source=source,
        )

        if error:
            summary.failed += 1
            summary.errors.append((pair, error))
        elif rows == 0:
            summary.no_data += 1
            summary.ok += 1  # No data is OK (might be up to date)
        else:
            summary.ok += 1
            summary.rows_upserted += rows

    # Update sync run
    status = "completed" if summary.failed == 0 else "partial"
    error_msg = None
    if summary.errors:
        error_msg = "; ".join([f"{p}: {e}" for p, e in summary.errors[:5]])
    update_fx_sync_run(con, run_id, status, summary.rows_upserted, error_msg)

    con.close()
    return summary


def get_sync_status(db_path: Path | str | None = None) -> list[dict]:
    """Get recent FX sync runs."""
    con = connect(db_path)
    init_schema(con)
    runs = get_fx_sync_runs(con, limit=10)
    con.close()
    return runs


def get_fx_data_summary(db_path: Path | str | None = None) -> dict:
    """
    Get summary of FX data in database.

    Returns:
        Dict with pair counts, date ranges, etc.
    """
    con = connect(db_path)
    init_schema(con)

    pairs = get_fx_pairs(con, active_only=False)

    summary = {
        "total_pairs": len(pairs),
        "active_pairs": len([p for p in pairs if p.get("is_active")]),
        "pairs": [],
    }

    for pair in pairs:
        pair_name = pair["pair"]
        latest = get_fx_latest_date(con, pair_name)

        # Get row count
        try:
            cur = con.execute(
                "SELECT COUNT(*) FROM fx_ohlcv WHERE pair = ?",
                (pair_name,)
            )
            row_count = cur.fetchone()[0]
        except Exception:
            row_count = 0

        summary["pairs"].append({
            "pair": pair_name,
            "source": pair.get("source"),
            "is_active": pair.get("is_active"),
            "latest_date": latest,
            "row_count": row_count,
        })

    con.close()
    return summary
