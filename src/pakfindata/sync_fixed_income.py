"""
Fixed Income sync module for Phase 3.

This module handles syncing fixed income data from various sources:
- CSV file ingestion (instruments, quotes, curves)
- SBP PMA document archive
- Analytics computation

All data is READ-ONLY and for informational purposes only.
"""

import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable

from .db import (
    connect,
    get_fi_curve_dates,
    get_fi_data_summary,
    get_fi_instruments,
    get_fi_latest_quote,
    get_fi_sync_runs,
    init_schema,
    record_fi_sync_run,
    update_fi_sync_run,
    upsert_fi_curve_point,
    upsert_fi_instrument,
    upsert_fi_quote,
    upsert_sbp_pma_doc,
)
from .sources.fixed_income_manual import (
    create_csv_templates,
    get_default_curves,
    get_default_instruments,
    get_default_quotes,
    load_fi_curves_csv,
    load_fi_instruments_csv,
    load_fi_quotes_csv,
)
from .sources.sbp_pma import (
    convert_doc_to_db_record,
    download_documents,
    fetch_and_parse_pma,
    get_sample_pma_documents,
)


@dataclass
class FISyncSummary:
    """Summary of fixed income sync operation."""

    total: int = 0
    ok: int = 0
    failed: int = 0
    rows_upserted: int = 0
    errors: list[tuple[str, str]] = field(default_factory=list)


def seed_fi_instruments(
    db_path: Path | str | None = None,
    source: str = "SAMPLE",
    csv_path: Path | str | None = None,
) -> dict:
    """
    Seed fixed income instruments into the database.

    Args:
        db_path: Database path
        source: Data source ("SAMPLE" or "CSV")
        csv_path: Path to CSV file if source is "CSV"

    Returns:
        Summary dict with counts
    """
    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_fi_sync_run(con, run_id, "SEED_INSTRUMENTS", [])

    inserted = 0
    failed = 0
    errors = []

    try:
        if source == "CSV" and csv_path:
            instruments, load_errors = load_fi_instruments_csv(Path(csv_path))
            errors.extend(load_errors)
        else:
            instruments = get_default_instruments()

        for inst in instruments:
            if upsert_fi_instrument(con, inst):
                inserted += 1
            else:
                failed += 1
                errors.append(f"Failed to upsert: {inst.get('isin', 'unknown')}")

        status = "completed" if failed == 0 else "partial"
        error_msg = "; ".join(errors[:5]) if errors else None
        update_fi_sync_run(con, run_id, status, inserted, error_msg)

    except Exception as e:
        update_fi_sync_run(con, run_id, "failed", inserted, str(e))
        errors.append(str(e))

    con.close()

    return {
        "success": failed == 0,
        "inserted": inserted,
        "failed": failed,
        "total": inserted + failed,
        "errors": errors,
    }


def sync_fi_quotes(
    db_path: Path | str | None = None,
    source: str = "SAMPLE",
    csv_path: Path | str | None = None,
    quote_date: str | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> FISyncSummary:
    """
    Sync fixed income quotes from source.

    Args:
        db_path: Database path
        source: Data source ("SAMPLE" or "CSV")
        csv_path: Path to CSV file if source is "CSV"
        quote_date: Specific date to sync, or None for all
        progress_callback: Optional callback(current, total, isin)

    Returns:
        FISyncSummary with results
    """
    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_fi_sync_run(con, run_id, "SYNC_QUOTES", [])

    summary = FISyncSummary()

    try:
        if source == "CSV" and csv_path:
            quotes, errors = load_fi_quotes_csv(Path(csv_path))
            for err in errors:
                summary.errors.append(("load", err))
        else:
            quotes = get_default_quotes()

        # Filter by date if specified
        if quote_date:
            quotes = [q for q in quotes if q.get("date") == quote_date]

        summary.total = len(quotes)

        for i, quote in enumerate(quotes):
            if progress_callback:
                progress_callback(i + 1, len(quotes), quote.get("isin", ""))

            if upsert_fi_quote(con, quote):
                summary.ok += 1
                summary.rows_upserted += 1
            else:
                summary.failed += 1
                summary.errors.append((quote.get("isin", ""), "upsert failed"))

        status = "completed" if summary.failed == 0 else "partial"
        error_msg = None
        if summary.errors:
            error_msg = "; ".join([f"{k}: {v}" for k, v in summary.errors[:5]])
        update_fi_sync_run(con, run_id, status, summary.rows_upserted, error_msg)

    except Exception as e:
        update_fi_sync_run(con, run_id, "failed", summary.rows_upserted, str(e))
        summary.errors.append(("exception", str(e)))

    con.close()
    return summary


def sync_fi_curves(
    db_path: Path | str | None = None,
    source: str = "SAMPLE",
    csv_path: Path | str | None = None,
    curve_name: str | None = None,
    curve_date: str | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> FISyncSummary:
    """
    Sync yield curve data from source.

    Args:
        db_path: Database path
        source: Data source ("SAMPLE" or "CSV")
        csv_path: Path to CSV file if source is "CSV"
        curve_name: Specific curve to sync, or None for all
        curve_date: Specific date to sync, or None for all
        progress_callback: Optional callback(current, total, curve_name)

    Returns:
        FISyncSummary with results
    """
    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_fi_sync_run(con, run_id, "SYNC_CURVES", [])

    summary = FISyncSummary()

    try:
        if source == "CSV" and csv_path:
            curves, errors = load_fi_curves_csv(Path(csv_path))
            for err in errors:
                summary.errors.append(("load", err))
        else:
            curves = get_default_curves()

        # Filter by curve name if specified
        if curve_name:
            curves = [c for c in curves if c.get("curve_name") == curve_name]

        # Filter by date if specified
        if curve_date:
            curves = [c for c in curves if c.get("curve_date") == curve_date]

        summary.total = len(curves)

        for i, curve_point in enumerate(curves):
            if progress_callback:
                progress_callback(i + 1, len(curves), curve_point.get("curve_name", ""))

            if upsert_fi_curve_point(con, curve_point):
                summary.ok += 1
                summary.rows_upserted += 1
            else:
                summary.failed += 1
                curve_nm = curve_point.get("curve_name", "")
                summary.errors.append((curve_nm, "upsert failed"))

        status = "completed" if summary.failed == 0 else "partial"
        error_msg = None
        if summary.errors:
            error_msg = "; ".join([f"{k}: {v}" for k, v in summary.errors[:5]])
        update_fi_sync_run(con, run_id, status, summary.rows_upserted, error_msg)

    except Exception as e:
        update_fi_sync_run(con, run_id, "failed", summary.rows_upserted, str(e))
        summary.errors.append(("exception", str(e)))

    con.close()
    return summary


def sync_sbp_pma_docs(
    db_path: Path | str | None = None,
    source: str = "SBP",
    download: bool = False,
    category: str | None = None,
    progress_callback: Callable[[int, int, str], None] | None = None,
) -> FISyncSummary:
    """
    Sync SBP PMA document metadata.

    Args:
        db_path: Database path
        source: Data source ("SBP" or "SAMPLE")
        download: If True, also download PDF files
        category: Filter by category (MTB, PIB, GOP_SUKUK)
        progress_callback: Optional callback(current, total, title)

    Returns:
        FISyncSummary with results
    """
    con = connect(db_path)
    init_schema(con)

    # Record sync run
    run_id = str(uuid.uuid4())[:8]
    record_fi_sync_run(con, run_id, "SYNC_SBP_PMA", [])

    summary = FISyncSummary()

    try:
        # Fetch documents
        if source == "SBP":
            docs = fetch_and_parse_pma()
            if not docs:
                # Fallback to sample if SBP unavailable
                docs = get_sample_pma_documents()
        else:
            docs = get_sample_pma_documents()

        # Filter by category
        if category:
            docs = [d for d in docs if d.category == category]

        summary.total = len(docs)

        # Download if requested
        if download and docs:

            def make_dl_callback(cb):
                def dl_callback(c, t, d):
                    return cb(c, t, d.title)
                return dl_callback

            dl_cb = make_dl_callback(progress_callback) if progress_callback else None
            download_summary = download_documents(docs, progress_callback=dl_cb)
            summary.rows_upserted = download_summary.get("downloaded", 0)

        # Store metadata in database
        for i, doc in enumerate(docs):
            if progress_callback and not download:
                progress_callback(i + 1, len(docs), doc.title)

            record = convert_doc_to_db_record(doc)
            if upsert_sbp_pma_doc(con, record):
                summary.ok += 1
                if not download:
                    summary.rows_upserted += 1
            else:
                summary.failed += 1
                summary.errors.append((doc.title, "upsert failed"))

        status = "completed" if summary.failed == 0 else "partial"
        error_msg = None
        if summary.errors:
            error_msg = "; ".join([f"{k}: {v}" for k, v in summary.errors[:5]])
        update_fi_sync_run(con, run_id, status, summary.rows_upserted, error_msg)

    except Exception as e:
        update_fi_sync_run(con, run_id, "failed", summary.rows_upserted, str(e))
        summary.errors.append(("exception", str(e)))

    con.close()
    return summary


def sync_all_fixed_income(
    db_path: Path | str | None = None,
    source: str = "SAMPLE",
    instruments_csv: Path | str | None = None,
    quotes_csv: Path | str | None = None,
    curves_csv: Path | str | None = None,
    sync_sbp: bool = True,
    progress_callback: Callable[[str, int, int], None] | None = None,
) -> dict:
    """
    Sync all fixed income data.

    Args:
        db_path: Database path
        source: Data source ("SAMPLE" or "CSV")
        instruments_csv: Path to instruments CSV
        quotes_csv: Path to quotes CSV
        curves_csv: Path to curves CSV
        sync_sbp: Whether to sync SBP PMA documents
        progress_callback: Optional callback(stage, current, total)

    Returns:
        Combined summary dict
    """
    results = {}

    # 1. Seed instruments
    if progress_callback:
        progress_callback("instruments", 0, 1)

    results["instruments"] = seed_fi_instruments(
        db_path=db_path,
        source="CSV" if instruments_csv else source,
        csv_path=instruments_csv,
    )

    # 2. Sync quotes
    if progress_callback:
        progress_callback("quotes", 0, 1)

    quote_summary = sync_fi_quotes(
        db_path=db_path,
        source="CSV" if quotes_csv else source,
        csv_path=quotes_csv,
    )
    results["quotes"] = {
        "total": quote_summary.total,
        "ok": quote_summary.ok,
        "failed": quote_summary.failed,
        "rows_upserted": quote_summary.rows_upserted,
    }

    # 3. Sync curves
    if progress_callback:
        progress_callback("curves", 0, 1)

    curve_summary = sync_fi_curves(
        db_path=db_path,
        source="CSV" if curves_csv else source,
        csv_path=curves_csv,
    )
    results["curves"] = {
        "total": curve_summary.total,
        "ok": curve_summary.ok,
        "failed": curve_summary.failed,
        "rows_upserted": curve_summary.rows_upserted,
    }

    # 4. Sync SBP PMA docs
    if sync_sbp:
        if progress_callback:
            progress_callback("sbp_pma", 0, 1)

        pma_summary = sync_sbp_pma_docs(
            db_path=db_path,
            source="SAMPLE" if source == "SAMPLE" else "SBP",
        )
        results["sbp_pma"] = {
            "total": pma_summary.total,
            "ok": pma_summary.ok,
            "failed": pma_summary.failed,
            "rows_upserted": pma_summary.rows_upserted,
        }

    return results


def get_fi_sync_status(
    db_path: Path | str | None = None, limit: int = 10
) -> list[dict]:
    """Get recent fixed income sync runs."""
    con = connect(db_path)
    init_schema(con)
    runs = get_fi_sync_runs(con, limit=limit)
    con.close()
    return runs


def get_fi_status_summary(db_path: Path | str | None = None) -> dict:
    """
    Get comprehensive fixed income data status.

    Returns:
        Dict with instrument counts, latest dates, curve info, etc.
    """
    con = connect(db_path)
    init_schema(con)

    summary = get_fi_data_summary(con)

    # Add latest quote dates per category
    instruments = get_fi_instruments(con)
    category_latest = {}

    for inst in instruments:
        isin = inst.get("isin")
        category = inst.get("category")

        latest = get_fi_latest_quote(con, isin)
        if latest:
            date = latest.get("date")
            if category not in category_latest or date > category_latest[category]:
                category_latest[category] = date

    summary["latest_by_category"] = category_latest

    # Add curve dates
    curve_dates = get_fi_curve_dates(con)
    summary["curve_dates"] = curve_dates

    con.close()
    return summary


def setup_csv_templates(output_dir: Path | str | None = None) -> dict:
    """
    Create CSV template files for manual data entry.

    Args:
        output_dir: Directory for templates, or None for default

    Returns:
        Dict with created file paths
    """
    return create_csv_templates(output_dir)
