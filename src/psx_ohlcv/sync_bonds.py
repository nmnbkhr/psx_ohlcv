"""
Bond sync module for Phase 3.

This module handles syncing bond data from CSV files
into the local database for analytics purposes.

Bond data is READ-ONLY and used for analytics, not investment recommendations.
"""

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .db import (
    connect,
    get_bond_data_summary,
    get_bond_sync_runs,
    get_bonds,
    init_schema,
    record_bond_sync_run,
    update_bond_sync_run,
    upsert_bond,
    upsert_bond_quote,
)
from .sources.bonds_manual import (
    generate_sample_quotes,
    get_default_bonds,
    load_bonds_from_csv,
    load_quotes_from_csv,
    save_bonds_config,
)


@dataclass
class BondSyncSummary:
    """Summary of bond sync operation."""

    total: int = 0
    ok: int = 0
    failed: int = 0
    no_data: int = 0
    rows_upserted: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)


def init_bond_tables(db_path: Path | str | None = None) -> dict:
    """
    Initialize bond tables in database.

    Args:
        db_path: Database path

    Returns:
        Summary dict
    """
    con = connect(db_path)
    init_schema(con)
    con.close()

    return {"success": True, "message": "Bond tables initialized"}


def seed_bonds(
    db_path: Path | str | None = None,
    bonds: list[dict] | None = None,
    bond_type: str | None = None,
    include_islamic: bool = True,
) -> dict:
    """
    Seed bond master data into the database.

    Args:
        db_path: Database path
        bonds: List of bond dicts, or None to use defaults
        bond_type: Filter by bond type (None = all)
        include_islamic: Include Islamic sukuk

    Returns:
        Summary dict with counts
    """
    if bonds is None:
        bonds = get_default_bonds()

    # Apply filters
    if bond_type:
        bonds = [b for b in bonds if b.get("bond_type") == bond_type]
    if not include_islamic:
        bonds = [b for b in bonds if not b.get("is_islamic")]

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_bond_sync_run(con, run_id, "INIT", len(bonds))

    inserted = 0
    failed = 0

    for bond_data in bonds:
        if upsert_bond(con, bond_data):
            inserted += 1
        else:
            failed += 1

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    update_bond_sync_run(con, run_id, status, inserted, 0, None)

    con.close()

    # Save config file
    save_bonds_config({"bonds": bonds})

    return {
        "success": True,
        "inserted": inserted,
        "failed": failed,
        "total": len(bonds),
    }


def load_bonds_csv(
    csv_path: Path | str,
    db_path: Path | str | None = None,
) -> dict:
    """
    Load bond master data from CSV file.

    Args:
        csv_path: Path to CSV file
        db_path: Database path

    Returns:
        Summary dict
    """
    try:
        bonds = load_bonds_from_csv(csv_path)
    except FileNotFoundError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        return {"success": False, "error": f"Error loading CSV: {e}"}

    if not bonds:
        return {"success": False, "error": "No valid bonds found in CSV"}

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_bond_sync_run(con, run_id, "LOAD_MASTER", len(bonds))

    inserted = 0
    failed = 0
    errors = []

    for bond_data in bonds:
        if upsert_bond(con, bond_data):
            inserted += 1
        else:
            failed += 1
            errors.append(bond_data.get("bond_id", "unknown"))

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    error_msg = f"Failed: {', '.join(errors[:5])}" if errors else None
    update_bond_sync_run(con, run_id, status, inserted, 0, error_msg)

    con.close()

    return {
        "success": True,
        "inserted": inserted,
        "failed": failed,
        "total": len(bonds),
        "csv_path": str(csv_path),
    }


def load_quotes_csv(
    csv_path: Path | str,
    db_path: Path | str | None = None,
) -> dict:
    """
    Load bond quotes from CSV file.

    Args:
        csv_path: Path to CSV file
        db_path: Database path

    Returns:
        Summary dict
    """
    try:
        quotes = load_quotes_from_csv(csv_path)
    except FileNotFoundError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        return {"success": False, "error": f"Error loading CSV: {e}"}

    if not quotes:
        return {"success": False, "error": "No valid quotes found in CSV"}

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_bond_sync_run(con, run_id, "LOAD_QUOTES", len(quotes))

    inserted = 0
    failed = 0

    for quote in quotes:
        if upsert_bond_quote(con, quote):
            inserted += 1
        else:
            failed += 1

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    update_bond_sync_run(con, run_id, status, inserted, inserted, None)

    con.close()

    return {
        "success": True,
        "rows_upserted": inserted,
        "failed": failed,
        "total": len(quotes),
        "csv_path": str(csv_path),
    }


def sync_sample_quotes(
    db_path: Path | str | None = None,
    days: int = 90,
    bond_ids: list[str] | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> BondSyncSummary:
    """
    Generate and sync sample quotes for bonds.

    Args:
        db_path: Database path
        days: Number of days of sample data
        bond_ids: List of bond IDs to generate for (None = all)
        progress_callback: Optional callback(current, total, bond_id)

    Returns:
        BondSyncSummary
    """
    con = connect(db_path)
    init_schema(con)

    # Get bonds to generate quotes for
    bonds = get_bonds(con, active_only=True)
    if bond_ids:
        bonds = [b for b in bonds if b["bond_id"] in bond_ids]

    if not bonds:
        con.close()
        return BondSyncSummary()

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_bond_sync_run(con, run_id, "LOAD_QUOTES", len(bonds))

    # Generate sample quotes
    quotes = generate_sample_quotes(bonds, days=days)

    summary = BondSyncSummary(total=len(bonds))

    # Upsert quotes
    for i, quote in enumerate(quotes):
        if progress_callback and i % 100 == 0:
            progress_callback(i, len(quotes), quote.get("bond_id", ""))

        if upsert_bond_quote(con, quote):
            summary.rows_upserted += 1
        else:
            summary.failed += 1

    summary.ok = len(set(q["bond_id"] for q in quotes))

    # Update sync run
    status = "completed" if summary.failed == 0 else "partial"
    update_bond_sync_run(
        con, run_id, status, summary.ok, summary.rows_upserted, None
    )

    con.close()
    return summary


def sync_bond_quotes(
    bond_ids: list[str] | None = None,
    db_path: Path | str | None = None,
    source: str = "SAMPLE",
    days: int = 90,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> BondSyncSummary:
    """
    Sync bond quotes from specified source.

    Args:
        bond_ids: List of bond IDs to sync (None = all)
        db_path: Database path
        source: Data source ('SAMPLE' for sample data)
        days: Days of data for sample generation
        progress_callback: Optional callback

    Returns:
        BondSyncSummary
    """
    if source == "SAMPLE":
        return sync_sample_quotes(
            db_path=db_path,
            days=days,
            bond_ids=bond_ids,
            progress_callback=progress_callback,
        )

    # For other sources, return empty summary (not implemented)
    return BondSyncSummary()


def get_sync_status(db_path: Path | str | None = None) -> list[dict]:
    """Get recent bond sync runs."""
    con = connect(db_path)
    init_schema(con)
    runs = get_bond_sync_runs(con, limit=10)
    con.close()
    return runs


def get_data_summary(db_path: Path | str | None = None) -> dict:
    """
    Get summary of bond data in database.

    Returns:
        Dict with bond counts, date ranges, type breakdown, etc.
    """
    con = connect(db_path)
    init_schema(con)
    summary = get_bond_data_summary(con)
    con.close()
    return summary
