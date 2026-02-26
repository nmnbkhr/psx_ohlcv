"""Scrape jobs, sync runs, and notification repository."""

import json
import math
import sqlite3
import uuid

from pakfindata.models import now_iso


# =============================================================================
# Sync Run Functions
# =============================================================================


def record_sync_run_start(
    con: sqlite3.Connection, mode: str, symbols_total: int
) -> str:
    """
    Record the start of a sync run.

    Args:
        con: Database connection
        mode: Sync mode (e.g., 'full', 'incremental', 'symbols_only')
        symbols_total: Total number of symbols to sync

    Returns:
        run_id (UUID string)
    """
    run_id = str(uuid.uuid4())
    now = now_iso()

    con.execute(
        """
        INSERT INTO sync_runs (run_id, started_at, mode, symbols_total)
        VALUES (?, ?, ?, ?)
        """,
        (run_id, now, mode, symbols_total),
    )
    con.commit()

    return run_id


def record_sync_run_end(
    con: sqlite3.Connection,
    run_id: str,
    symbols_ok: int,
    symbols_failed: int,
    rows_upserted: int,
) -> None:
    """
    Record the end of a sync run.

    Args:
        con: Database connection
        run_id: The run ID returned by record_sync_run_start
        symbols_ok: Number of symbols successfully synced
        symbols_failed: Number of symbols that failed
        rows_upserted: Total number of EOD rows upserted
    """
    now = now_iso()

    con.execute(
        """
        UPDATE sync_runs
        SET ended_at = ?,
            symbols_ok = ?,
            symbols_failed = ?,
            rows_upserted = ?
        WHERE run_id = ?
        """,
        (now, symbols_ok, symbols_failed, rows_upserted, run_id),
    )
    con.commit()


def record_failure(
    con: sqlite3.Connection,
    run_id: str,
    symbol: str,
    error_type: str,
    error_message: str | None,
) -> None:
    """
    Record a sync failure for a specific symbol.

    Args:
        con: Database connection
        run_id: The run ID
        symbol: The symbol that failed
        error_type: Type of error (e.g., 'HTTP_ERROR', 'PARSE_ERROR')
        error_message: Detailed error message
    """
    now = now_iso()

    con.execute(
        """
        INSERT INTO sync_failures
            (run_id, symbol, error_type, error_message, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (run_id, symbol, error_type, error_message, now),
    )
    con.commit()


# =============================================================================
# Scrape Job Functions
# =============================================================================


def create_scrape_job(
    con: sqlite3.Connection,
    job_type: str,
    config: dict | None = None,
) -> str:
    """
    Create a new scrape job for tracking.

    Args:
        con: Database connection
        job_type: Type of job
        config: Optional job configuration

    Returns:
        Job ID
    """
    job_id = str(uuid.uuid4())[:8]
    now = now_iso()

    con.execute(
        """
        INSERT INTO scrape_jobs (job_id, job_type, started_at, status, config)
        VALUES (?, ?, ?, 'running', ?)
        """,
        (job_id, job_type, now, json.dumps(config) if config else None),
    )
    con.commit()
    return job_id


def update_scrape_job(
    con: sqlite3.Connection,
    job_id: str,
    status: str | None = None,
    symbols_requested: int | None = None,
    symbols_completed: int | None = None,
    symbols_failed: int | None = None,
    records_inserted: int | None = None,
    records_updated: int | None = None,
    errors: list | None = None,
) -> None:
    """
    Update a scrape job with progress.

    Args:
        con: Database connection
        job_id: Job ID
        status: New status ('completed', 'failed')
        symbols_requested: Total symbols to process
        symbols_completed: Number completed
        symbols_failed: Number failed
        records_inserted: Records inserted
        records_updated: Records updated
        errors: List of error dicts
    """
    updates = []
    params = []

    if status:
        updates.append("status = ?")
        params.append(status)
        if status in ("completed", "failed"):
            updates.append("ended_at = ?")
            params.append(now_iso())

    if symbols_requested is not None:
        updates.append("symbols_requested = ?")
        params.append(symbols_requested)
    if symbols_completed is not None:
        updates.append("symbols_completed = ?")
        params.append(symbols_completed)
    if symbols_failed is not None:
        updates.append("symbols_failed = ?")
        params.append(symbols_failed)
    if records_inserted is not None:
        updates.append("records_inserted = ?")
        params.append(records_inserted)
    if records_updated is not None:
        updates.append("records_updated = ?")
        params.append(records_updated)
    if errors is not None:
        updates.append("errors = ?")
        params.append(json.dumps(errors))

    if updates:
        params.append(job_id)
        query = f"UPDATE scrape_jobs SET {', '.join(updates)} WHERE job_id = ?"
        con.execute(query, params)
        con.commit()


def get_scrape_job(con: sqlite3.Connection, job_id: str) -> dict | None:
    """Get scrape job by ID."""
    cur = con.execute("SELECT * FROM scrape_jobs WHERE job_id = ?", (job_id,))
    row = cur.fetchone()
    if not row:
        return None

    result = dict(row)
    for field in ("errors", "config"):
        if result.get(field):
            try:
                result[field] = json.loads(result[field])
            except json.JSONDecodeError:
                pass
    return result


# =============================================================================
# Background Job Management Functions
# =============================================================================


def create_background_job(
    con: sqlite3.Connection,
    job_type: str,
    symbols: list[str],
    batch_size: int = 50,
    batch_pause_sec: int = 30,
    config: dict | None = None,
) -> str:
    """Create a new background scrape job.

    Args:
        con: Database connection
        job_type: Type of job ('bulk_deep_scrape', etc.)
        symbols: List of symbols to process
        batch_size: Symbols per batch
        batch_pause_sec: Pause between batches
        config: Optional configuration dict

    Returns:
        Job ID
    """
    job_id = str(uuid.uuid4())[:8]
    total_batches = math.ceil(len(symbols) / batch_size)

    config_data = config or {}
    config_data["symbols"] = symbols

    con.execute(
        """
        INSERT INTO scrape_jobs (
            job_id, job_type, started_at, status,
            symbols_requested, batch_size, batch_pause_sec,
            total_batches, config
        ) VALUES (?, ?, datetime('now'), 'pending', ?, ?, ?, ?, ?)
        """,
        (
            job_id,
            job_type,
            len(symbols),
            batch_size,
            batch_pause_sec,
            total_batches,
            json.dumps(config_data),
        ),
    )
    con.commit()
    return job_id


def update_job_progress(
    con: sqlite3.Connection,
    job_id: str,
    current_symbol: str | None = None,
    current_batch: int | None = None,
    symbols_completed: int | None = None,
    symbols_failed: int | None = None,
    records_inserted: int | None = None,
    status: str | None = None,
    pid: int | None = None,
) -> None:
    """Update job progress (called by worker)."""
    updates = ["last_heartbeat = datetime('now')"]
    params = []

    if current_symbol is not None:
        updates.append("current_symbol = ?")
        params.append(current_symbol)
    if current_batch is not None:
        updates.append("current_batch = ?")
        params.append(current_batch)
    if symbols_completed is not None:
        updates.append("symbols_completed = ?")
        params.append(symbols_completed)
    if symbols_failed is not None:
        updates.append("symbols_failed = ?")
        params.append(symbols_failed)
    if records_inserted is not None:
        updates.append("records_inserted = ?")
        params.append(records_inserted)
    if status is not None:
        updates.append("status = ?")
        params.append(status)
        if status in ("completed", "failed", "stopped"):
            updates.append("ended_at = datetime('now')")
    if pid is not None:
        updates.append("pid = ?")
        params.append(pid)

    params.append(job_id)
    con.execute(
        f"UPDATE scrape_jobs SET {', '.join(updates)} WHERE job_id = ?",
        params,
    )
    con.commit()


def request_job_stop(con: sqlite3.Connection, job_id: str) -> bool:
    """Request a job to stop (called by UI)."""
    con.execute(
        "UPDATE scrape_jobs SET stop_requested = 1 WHERE job_id = ?",
        (job_id,),
    )
    con.commit()
    return True


def is_job_stop_requested(con: sqlite3.Connection, job_id: str) -> bool:
    """Check if stop was requested for a job (called by worker)."""
    cur = con.execute(
        "SELECT stop_requested FROM scrape_jobs WHERE job_id = ?",
        (job_id,),
    )
    row = cur.fetchone()
    return bool(row and row[0])


def get_running_jobs(con: sqlite3.Connection) -> list[dict]:
    """Get all running/pending jobs."""
    cur = con.execute(
        """
        SELECT * FROM scrape_jobs
        WHERE status IN ('pending', 'running')
        ORDER BY started_at DESC
        """
    )
    jobs = []
    for row in cur.fetchall():
        job = dict(row)
        for field in ("errors", "config"):
            if job.get(field):
                try:
                    job[field] = json.loads(job[field])
                except json.JSONDecodeError:
                    pass
        jobs.append(job)
    return jobs


def get_recent_jobs(con: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent jobs (all statuses)."""
    cur = con.execute(
        """
        SELECT * FROM scrape_jobs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (limit,),
    )
    jobs = []
    for row in cur.fetchall():
        job = dict(row)
        for field in ("errors", "config"):
            if job.get(field):
                try:
                    job[field] = json.loads(job[field])
                except json.JSONDecodeError:
                    pass
        jobs.append(job)
    return jobs


# =============================================================================
# Job Notification Functions
# =============================================================================


def add_job_notification(
    con: sqlite3.Connection,
    job_id: str,
    notification_type: str,
    title: str,
    message: str | None = None,
) -> None:
    """Add a notification for a job."""
    con.execute(
        """
        INSERT INTO job_notifications (job_id, notification_type, title, message)
        VALUES (?, ?, ?, ?)
        """,
        (job_id, notification_type, title, message),
    )
    con.execute(
        "UPDATE scrape_jobs SET notification_sent = 1 WHERE job_id = ?",
        (job_id,),
    )
    con.commit()


def get_unread_notifications(con: sqlite3.Connection) -> list[dict]:
    """Get all unread notifications."""
    cur = con.execute(
        """
        SELECT n.*, j.job_type, j.symbols_completed, j.symbols_failed
        FROM job_notifications n
        JOIN scrape_jobs j ON n.job_id = j.job_id
        WHERE n.read_at IS NULL
        ORDER BY n.created_at DESC
        """
    )
    return [dict(row) for row in cur.fetchall()]


def mark_notification_read(con: sqlite3.Connection, notification_id: int) -> None:
    """Mark a notification as read."""
    con.execute(
        "UPDATE job_notifications SET read_at = datetime('now') WHERE id = ?",
        (notification_id,),
    )
    con.commit()


def mark_all_notifications_read(con: sqlite3.Connection) -> None:
    """Mark all notifications as read."""
    con.execute("UPDATE job_notifications SET read_at = datetime('now') WHERE read_at IS NULL")
    con.commit()
