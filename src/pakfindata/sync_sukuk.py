"""
Sukuk sync module for Phase 3.

This module handles syncing sukuk/debt market data from CSV files
and SBP documents into the local database for analytics purposes.

Sukuk data is READ-ONLY and used for analytics, not investment recommendations.
"""

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .db import (
    connect,
    get_sukuk_data_summary,
    get_sukuk_list,
    get_sukuk_sync_runs,
    init_schema,
    record_sukuk_sync_run,
    update_sukuk_sync_run,
    upsert_sbp_document,
    upsert_sukuk,
    upsert_sukuk_quote,
    upsert_sukuk_yield_curve_point,
)
from .sources.sbp_primary_market import (
    index_documents,
    scan_document_directory,
)
from .sources.sukuk_manual import (
    generate_sample_quotes,
    generate_sample_yield_curve,
    get_default_sukuk,
    load_sukuk_curve_csv,
    load_sukuk_master_csv,
    load_sukuk_quotes_csv,
    save_sukuk_config,
)


@dataclass
class SukukSyncSummary:
    """Summary of sukuk sync operation."""

    total: int = 0
    ok: int = 0
    failed: int = 0
    no_data: int = 0
    rows_upserted: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)


def init_sukuk_tables(db_path: Path | str | None = None) -> dict[str, Any]:
    """
    Initialize sukuk tables in database.

    Args:
        db_path: Database path

    Returns:
        Summary dict
    """
    con = connect(db_path)
    init_schema(con)
    con.close()

    return {"success": True, "message": "Sukuk tables initialized"}


def seed_sukuk(
    db_path: Path | str | None = None,
    sukuk_list: list[dict] | None = None,
    category: str | None = None,
    shariah_only: bool = False,
) -> dict[str, Any]:
    """
    Seed sukuk master data into the database.

    Args:
        db_path: Database path
        sukuk_list: List of sukuk dicts, or None to use defaults
        category: Filter by category (None = all)
        shariah_only: Only include shariah-compliant instruments

    Returns:
        Summary dict with counts
    """
    if sukuk_list is None:
        sukuk_list = get_default_sukuk()

    # Apply filters
    if category:
        sukuk_list = [s for s in sukuk_list if s.get("category") == category]
    if shariah_only:
        sukuk_list = [s for s in sukuk_list if s.get("shariah_compliant")]

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_sukuk_sync_run(con, run_id, "SEED", len(sukuk_list))

    inserted = 0
    failed = 0

    for sukuk_data in sukuk_list:
        if upsert_sukuk(con, sukuk_data):
            inserted += 1
        else:
            failed += 1

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    update_sukuk_sync_run(con, run_id, status, inserted, 0, None)

    con.close()

    # Save config file
    save_sukuk_config({"sukuk": sukuk_list})

    return {
        "success": True,
        "inserted": inserted,
        "failed": failed,
        "total": len(sukuk_list),
    }


def load_sukuk_csv(
    csv_path: Path | str,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    """
    Load sukuk master data from CSV file.

    Args:
        csv_path: Path to CSV file
        db_path: Database path

    Returns:
        Summary dict
    """
    try:
        sukuk_list = load_sukuk_master_csv(csv_path)
    except FileNotFoundError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        return {"success": False, "error": f"Error loading CSV: {e}"}

    if not sukuk_list:
        return {"success": False, "error": "No valid sukuk found in CSV"}

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_sukuk_sync_run(con, run_id, "LOAD_MASTER", len(sukuk_list))

    inserted = 0
    failed = 0
    errors = []

    for sukuk_data in sukuk_list:
        if upsert_sukuk(con, sukuk_data):
            inserted += 1
        else:
            failed += 1
            errors.append(sukuk_data.get("instrument_id", "unknown"))

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    error_msg = f"Failed: {', '.join(errors[:5])}" if errors else None
    update_sukuk_sync_run(con, run_id, status, inserted, 0, error_msg)

    con.close()

    return {
        "success": True,
        "inserted": inserted,
        "failed": failed,
        "total": len(sukuk_list),
        "csv_path": str(csv_path),
    }


def load_quotes_csv(
    csv_path: Path | str,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    """
    Load sukuk quotes from CSV file.

    Args:
        csv_path: Path to CSV file
        db_path: Database path

    Returns:
        Summary dict
    """
    try:
        quotes = load_sukuk_quotes_csv(csv_path)
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
    record_sukuk_sync_run(con, run_id, "LOAD_QUOTES", len(quotes))

    inserted = 0
    failed = 0

    for quote in quotes:
        if upsert_sukuk_quote(con, quote):
            inserted += 1
        else:
            failed += 1

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    update_sukuk_sync_run(con, run_id, status, inserted, inserted, None)

    con.close()

    return {
        "success": True,
        "rows_upserted": inserted,
        "failed": failed,
        "total": len(quotes),
        "csv_path": str(csv_path),
    }


def load_yield_curve_csv(
    csv_path: Path | str,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    """
    Load sukuk yield curve data from CSV file.

    Args:
        csv_path: Path to CSV file
        db_path: Database path

    Returns:
        Summary dict
    """
    try:
        points = load_sukuk_curve_csv(csv_path)
    except FileNotFoundError as e:
        return {"success": False, "error": str(e)}
    except Exception as e:
        return {"success": False, "error": f"Error loading CSV: {e}"}

    if not points:
        return {"success": False, "error": "No valid yield curve points found in CSV"}

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_sukuk_sync_run(con, run_id, "LOAD_CURVE", len(points))

    inserted = 0
    failed = 0

    for point in points:
        if upsert_sukuk_yield_curve_point(con, point):
            inserted += 1
        else:
            failed += 1

    # Update sync run
    status = "completed" if failed == 0 else "partial"
    update_sukuk_sync_run(con, run_id, status, inserted, inserted, None)

    con.close()

    return {
        "success": True,
        "rows_upserted": inserted,
        "failed": failed,
        "total": len(points),
        "csv_path": str(csv_path),
    }


def sync_sample_quotes(
    db_path: Path | str | None = None,
    days: int = 90,
    instrument_ids: list[str] | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> SukukSyncSummary:
    """
    Generate and sync sample quotes for sukuk.

    Args:
        db_path: Database path
        days: Number of days of sample data
        instrument_ids: List of instrument IDs to generate for (None = all)
        progress_callback: Optional callback(current, total, instrument_id)

    Returns:
        SukukSyncSummary
    """
    con = connect(db_path)
    init_schema(con)

    # Get sukuk to generate quotes for
    sukuk_list = get_sukuk_list(con, active_only=True)
    if instrument_ids:
        sukuk_list = [s for s in sukuk_list if s["instrument_id"] in instrument_ids]

    if not sukuk_list:
        con.close()
        return SukukSyncSummary()

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_sukuk_sync_run(con, run_id, "SAMPLE_QUOTES", len(sukuk_list))

    # Generate sample quotes
    quotes = generate_sample_quotes(sukuk_list, days=days)

    summary = SukukSyncSummary(total=len(sukuk_list))

    # Upsert quotes
    for i, quote in enumerate(quotes):
        if progress_callback and i % 100 == 0:
            progress_callback(i, len(quotes), quote.get("instrument_id", ""))

        if upsert_sukuk_quote(con, quote):
            summary.rows_upserted += 1
        else:
            summary.failed += 1

    summary.ok = len(set(q["instrument_id"] for q in quotes))

    # Update sync run
    status = "completed" if summary.failed == 0 else "partial"
    update_sukuk_sync_run(
        con, run_id, status, summary.ok, summary.rows_upserted, None
    )

    con.close()
    return summary


def sync_sample_yield_curves(
    db_path: Path | str | None = None,
    days: int = 30,
    curve_names: list[str] | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> SukukSyncSummary:
    """
    Generate and sync sample yield curve data.

    Args:
        db_path: Database path
        days: Number of days of historical curves
        curve_names: List of curve names (default: GOP_SUKUK, PIB, TBILL)
        progress_callback: Optional callback

    Returns:
        SukukSyncSummary
    """
    from datetime import timedelta

    if curve_names is None:
        curve_names = ["GOP_SUKUK", "PIB", "TBILL"]

    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_sukuk_sync_run(con, run_id, "SAMPLE_CURVES", len(curve_names) * days)

    summary = SukukSyncSummary(total=len(curve_names))

    from datetime import datetime
    today = datetime.now().date()

    for curve_name in curve_names:
        curve_points = 0

        for i in range(days):
            curve_date = (today - timedelta(days=days - i - 1)).isoformat()
            points = generate_sample_yield_curve(curve_date, curve_name)

            for point in points:
                if progress_callback:
                    progress_callback(
                        curve_points, days * 9, f"{curve_name}:{curve_date}"
                    )

                if upsert_sukuk_yield_curve_point(con, point):
                    summary.rows_upserted += 1
                    curve_points += 1
                else:
                    summary.failed += 1

        summary.ok += 1

    # Update sync run
    status = "completed" if summary.failed == 0 else "partial"
    update_sukuk_sync_run(
        con, run_id, status, summary.ok, summary.rows_upserted, None
    )

    con.close()
    return summary


def sync_sukuk_quotes(
    instrument_ids: list[str] | None = None,
    db_path: Path | str | None = None,
    source: str = "SAMPLE",
    days: int = 90,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> SukukSyncSummary:
    """
    Sync sukuk quotes from specified source.

    Args:
        instrument_ids: List of instrument IDs to sync (None = all)
        db_path: Database path
        source: Data source ('SAMPLE' for sample data)
        days: Days of data for sample generation
        progress_callback: Optional callback

    Returns:
        SukukSyncSummary
    """
    if source == "SAMPLE":
        return sync_sample_quotes(
            db_path=db_path,
            days=days,
            instrument_ids=instrument_ids,
            progress_callback=progress_callback,
        )

    # For other sources, return empty summary (not implemented)
    return SukukSyncSummary()


def index_sbp_documents(
    docs_dir: Path | str | None = None,
    db_path: Path | str | None = None,
) -> dict[str, Any]:
    """
    Index SBP documents from directory into database.

    Args:
        docs_dir: Directory containing SBP documents
        db_path: Database path

    Returns:
        Summary dict
    """
    # First, index to JSON file
    index_result = index_documents(docs_dir)

    # Then, insert into database
    documents = scan_document_directory(docs_dir)

    if not documents:
        return {
            "success": True,
            "total_documents": 0,
            "inserted": 0,
            "message": "No documents found in directory",
        }

    con = connect(db_path)
    init_schema(con)

    inserted = 0
    failed = 0

    for doc in documents:
        if upsert_sbp_document(con, doc.to_dict()):
            inserted += 1
        else:
            failed += 1

    con.close()

    return {
        "success": True,
        "total_documents": len(documents),
        "inserted": inserted,
        "failed": failed,
        "index_path": index_result.get("index_path"),
    }


def get_sync_status(db_path: Path | str | None = None) -> list[dict]:
    """Get recent sukuk sync runs."""
    con = connect(db_path)
    init_schema(con)
    runs = get_sukuk_sync_runs(con, limit=10)
    con.close()
    return runs


def get_data_summary(db_path: Path | str | None = None) -> dict[str, Any]:
    """
    Get summary of sukuk data in database.

    Returns:
        Dict with sukuk counts, date ranges, category breakdown, etc.
    """
    con = connect(db_path)
    init_schema(con)
    summary = get_sukuk_data_summary(con)
    con.close()
    return summary
