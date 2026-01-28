"""
MUFAP sync module for Phase 2.5.

This module handles syncing mutual fund data from MUFAP
into the local database for analytics purposes.

Mutual fund data is READ-ONLY and used for analytics, not investment recommendations.
"""

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

from .db import (
    connect,
    get_mf_data_summary,
    get_mf_latest_date,
    get_mf_sync_runs,
    get_mutual_fund,
    get_mutual_fund_by_symbol,
    get_mutual_funds,
    init_schema,
    record_mf_sync_run,
    update_mf_sync_run,
    upsert_mf_nav,
    upsert_mutual_fund,
)
from .sources.mufap import (
    fetch_mutual_fund_data,
    get_default_funds,
    normalize_nav_dataframe,
    save_mufap_config,
)


@dataclass
class MufapSyncSummary:
    """Summary of MUFAP sync operation."""
    total: int = 0
    ok: int = 0
    failed: int = 0
    no_data: int = 0
    rows_upserted: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)


def seed_mutual_funds(
    db_path: Path | str | None = None,
    funds: list[dict] | None = None,
    category: str | None = None,
    include_vps: bool = True,
) -> dict:
    """
    Seed mutual fund master data into the database.

    Args:
        db_path: Database path
        funds: List of fund dicts, or None to use defaults
        category: Filter by category (None = all)
        include_vps: Include VPS funds

    Returns:
        Summary dict with counts
    """
    if funds is None:
        funds = get_default_funds()

    # Apply filters
    if category:
        funds = [f for f in funds if f.get("category") == category]
    if not include_vps:
        funds = [f for f in funds if f.get("fund_type") != "VPS"]

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_mf_sync_run(con, run_id, "SEED", len(funds))

    inserted = 0
    failed = 0

    for fund_data in funds:
        if upsert_mutual_fund(con, fund_data):
            inserted += 1
        else:
            failed += 1

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    update_mf_sync_run(con, run_id, status, inserted, 0, None)

    con.close()

    # Also save config file
    save_mufap_config({"funds": funds})

    return {
        "success": True,
        "inserted": inserted,
        "failed": failed,
        "total": len(funds),
    }


def sync_fund_nav(
    fund_id: str,
    db_path: Path | str | None = None,
    incremental: bool = True,
    source: str = "AUTO",
) -> tuple[int, str | None]:
    """
    Sync NAV data for a single mutual fund.

    Args:
        fund_id: Mutual fund ID or symbol
        db_path: Database path
        incremental: If True, only fetch new data
        source: Data source

    Returns:
        Tuple of (rows_upserted, error_message)
    """
    con = connect(db_path)
    init_schema(con)

    try:
        # Try to find fund by ID or symbol
        fund = get_mutual_fund(con, fund_id)
        if not fund:
            fund = get_mutual_fund_by_symbol(con, fund_id)
        if not fund:
            con.close()
            return 0, f"Fund not found: {fund_id}"

        actual_fund_id = fund["fund_id"]

        # Determine start date for incremental sync
        start_date = None
        if incremental:
            latest = get_mf_latest_date(con, actual_fund_id)
            if latest:
                # Start from day after latest
                latest_dt = datetime.strptime(latest, "%Y-%m-%d")
                start_date = (latest_dt + timedelta(days=1)).strftime("%Y-%m-%d")

        # Fetch data
        df = fetch_mutual_fund_data(
            actual_fund_id, start_date=start_date, source=source
        )

        if df.empty:
            con.close()
            return 0, None

        # Normalize and upsert
        df = normalize_nav_dataframe(df)
        rows = upsert_mf_nav(con, actual_fund_id, df)

        con.close()
        return rows, None

    except Exception as e:
        con.close()
        return 0, str(e)


def sync_mutual_funds(
    fund_ids: list[str] | None = None,
    db_path: Path | str | None = None,
    incremental: bool = True,
    source: str = "AUTO",
    category: str | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> MufapSyncSummary:
    """
    Sync NAV data for multiple mutual funds.

    Args:
        fund_ids: List of fund IDs to sync, or None for all active funds
        db_path: Database path
        incremental: If True, only fetch new data
        source: Data source
        category: Filter by category code
        progress_callback: Optional callback(current, total, fund_id)

    Returns:
        MufapSyncSummary with results
    """
    con = connect(db_path)
    init_schema(con)

    # Get funds to sync
    if fund_ids is None:
        fund_records = get_mutual_funds(
            con,
            active_only=True,
            category=category
        )
        fund_ids = [f["fund_id"] for f in fund_records]

    if not fund_ids:
        con.close()
        return MufapSyncSummary()

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_mf_sync_run(con, run_id, "NAV_SYNC", len(fund_ids))

    summary = MufapSyncSummary(total=len(fund_ids))

    for i, fund_id in enumerate(fund_ids):
        if progress_callback:
            progress_callback(i + 1, len(fund_ids), fund_id)

        rows, error = sync_fund_nav(
            fund_id,
            db_path=db_path,
            incremental=incremental,
            source=source,
        )

        if error:
            summary.failed += 1
            summary.errors.append((fund_id, error))
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
        error_msg = "; ".join([f"{f}: {e}" for f, e in summary.errors[:5]])
    update_mf_sync_run(
        con, run_id, status, summary.ok, summary.rows_upserted, error_msg
    )

    con.close()
    return summary


def get_sync_status(db_path: Path | str | None = None) -> list[dict]:
    """Get recent MUFAP sync runs."""
    con = connect(db_path)
    init_schema(con)
    runs = get_mf_sync_runs(con, limit=10)
    con.close()
    return runs


def get_data_summary(db_path: Path | str | None = None) -> dict:
    """
    Get summary of mutual fund data in database.

    Returns:
        Dict with fund counts, date ranges, category breakdown, etc.
    """
    con = connect(db_path)
    init_schema(con)
    summary = get_mf_data_summary(con)
    con.close()
    return summary
