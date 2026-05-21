"""Jobs queue helpers — table-backed worker dispatch.

Phase 1.4 introduces a generic ``jobs`` queue: ETL work is submitted
via ``POST /v1/jobs/{job_type}`` (or any caller calling
:func:`enqueue_job` directly), the worker process polls and dispatches,
and the result lands back on the row.

Lifecycle:
    pending  →  running  →  ok | failed | cancelled

Public API:
    init_jobs_schema(con)          — idempotent DDL bootstrap
    enqueue_job(...)               — INSERT 'pending', returns id
    claim_next_job(worker_pid)     — atomically mark next 'pending' → 'running'
    finish_job(id, status, ...)    — terminal update (ok/failed/cancelled)
    cancel_pending(id)             — move 'pending' → 'cancelled'
    get_job(id)                    — single-row read
    list_jobs(status, type, limit) — filtered listing

Design invariants (Phase 0 contract):
    - All WRITES go through ``pakfindata.db.safe_writer`` (one writer at a
      time; WAL checkpoints disciplined).
    - All READS use a per-call read-only SQLite connection (mode=ro URI).
    - ``params`` and ``result`` columns are JSON-encoded strings; helpers
      do the (de)serialization for callers.
    - Caller-provided params must be JSON-serializable; ``json.dumps``
      raises ``TypeError`` on non-serializable inputs.
"""

from __future__ import annotations

import json
import sqlite3
from typing import Any, Optional

from pakfindata.config import get_db_path
from pakfindata.db.safe_writer import safe_writer

# Module-level guard — avoids re-running the CREATE TABLE / CREATE INDEX
# block inside every safe_writer call. CREATE TABLE IF NOT EXISTS is
# cheap but not free; this saves the round-trip on hot paths.
_schema_initialized = False


# Each DDL statement is executed individually because ``executescript``
# would issue an implicit COMMIT — that breaks the BEGIN IMMEDIATE
# transaction held by ``safe_writer``. SafeWriter invariant from Phase
# 0.1: never use ``executescript`` inside a safe_writer block.
_DDL_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS jobs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        job_type      TEXT NOT NULL,
        params        TEXT,
        status        TEXT NOT NULL,
        enqueued_at   TEXT NOT NULL DEFAULT (datetime('now')),
        started_at    TEXT,
        finished_at   TEXT,
        duration_ms   INTEGER,
        result        TEXT,
        error         TEXT,
        error_detail  TEXT,
        worker_pid    INTEGER,
        priority      INTEGER NOT NULL DEFAULT 100,
        parent_job_id INTEGER,
        source        TEXT NOT NULL DEFAULT 'api',
        notes         TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_type_status ON jobs(job_type, status)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_enqueued ON jobs(enqueued_at)",
    "CREATE INDEX IF NOT EXISTS idx_jobs_pending_priority "
    "ON jobs(status, priority, enqueued_at) WHERE status = 'pending'",
)


def init_jobs_schema(con: sqlite3.Connection) -> None:
    """Bootstrap the ``jobs`` table + indexes. Idempotent.

    Safe to call from inside a ``safe_writer`` block — uses individual
    ``execute`` calls, NOT ``executescript`` (which would auto-commit
    and break the surrounding transaction).
    """
    for stmt in _DDL_STATEMENTS:
        con.execute(stmt)


def _ensure_schema(con: sqlite3.Connection) -> None:
    """Run init_jobs_schema once per process. Cheap, idempotent."""
    global _schema_initialized
    if _schema_initialized:
        return
    init_jobs_schema(con)
    _schema_initialized = True


def _decode_row(row: sqlite3.Row | None) -> dict | None:
    """Convert a Row to dict; decode ``params`` / ``result`` JSON columns."""
    if row is None:
        return None
    d = dict(row)
    if d.get("params"):
        try:
            d["params"] = json.loads(d["params"])
        except (TypeError, ValueError):
            pass
    if d.get("result"):
        try:
            d["result"] = json.loads(d["result"])
        except (TypeError, ValueError):
            pass
    return d


def enqueue_job(
    job_type: str,
    params: Optional[dict] = None,
    priority: int = 100,
    source: str = "api",
    notes: Optional[str] = None,
) -> int:
    """Insert a ``pending`` job row; return the new ``id``."""
    params_json = json.dumps(params or {})
    with safe_writer() as con:
        _ensure_schema(con)
        cur = con.execute(
            """INSERT INTO jobs (job_type, params, status, priority, source, notes)
               VALUES (?, ?, 'pending', ?, ?, ?)""",
            (job_type, params_json, priority, source, notes),
        )
        return cur.lastrowid


def claim_next_job(worker_pid: int) -> Optional[dict]:
    """Atomically claim the next pending job; return None if queue empty.

    The whole SELECT + UPDATE runs inside one ``safe_writer`` block, which
    holds the single SQLite writer lock — so the claim is race-free even
    if (hypothetically) a second worker process exists.
    """
    with safe_writer() as con:
        _ensure_schema(con)
        row = con.execute(
            """SELECT id, job_type, params, priority, source
                 FROM jobs
                 WHERE status = 'pending'
                 ORDER BY priority ASC, enqueued_at ASC
                 LIMIT 1"""
        ).fetchone()
        if row is None:
            return None
        job_id = row[0]
        con.execute(
            """UPDATE jobs
                  SET status = 'running',
                      started_at = datetime('now'),
                      worker_pid = ?
                WHERE id = ? AND status = 'pending'""",
            (worker_pid, job_id),
        )
        return {
            "id": job_id,
            "job_type": row[1],
            "params": json.loads(row[2] or "{}"),
            "priority": row[3],
            "source": row[4],
        }


def finish_job(
    job_id: int,
    *,
    status: str,
    result: Optional[dict] = None,
    error: Optional[str] = None,
    error_detail: Optional[str] = None,
) -> None:
    """Terminal update — sets finished_at + duration_ms + result/error."""
    if status not in ("ok", "failed", "cancelled"):
        raise ValueError(f"invalid terminal status: {status!r}")
    result_json = json.dumps(result) if result is not None else None
    with safe_writer() as con:
        _ensure_schema(con)
        con.execute(
            """UPDATE jobs
                  SET status = ?,
                      finished_at = datetime('now'),
                      duration_ms = CAST(
                          (julianday('now') - julianday(started_at)) * 86400000
                          AS INTEGER
                      ),
                      result = ?,
                      error = ?,
                      error_detail = ?
                WHERE id = ?""",
            (status, result_json, error, error_detail, job_id),
        )


def cancel_pending(job_id: int) -> bool:
    """Move a ``pending`` job to ``cancelled``. Returns False if the job
    isn't pending (already running, finished, or unknown)."""
    with safe_writer() as con:
        _ensure_schema(con)
        cur = con.execute(
            """UPDATE jobs
                  SET status = 'cancelled',
                      finished_at = datetime('now')
                WHERE id = ? AND status = 'pending'""",
            (job_id,),
        )
        return cur.rowcount > 0


def _open_ro_con() -> sqlite3.Connection:
    """Open a per-call read-only SQLite connection."""
    con = sqlite3.connect(
        f"file:{get_db_path()}?mode=ro",
        uri=True,
        check_same_thread=False,
    )
    con.row_factory = sqlite3.Row
    return con


def get_job(job_id: int) -> Optional[dict]:
    """Return the full job row as a dict, decoded; None if unknown."""
    con = _open_ro_con()
    try:
        row = con.execute(
            "SELECT * FROM jobs WHERE id = ?", (job_id,)
        ).fetchone()
        return _decode_row(row)
    except sqlite3.OperationalError:
        # Table doesn't exist yet — first ever job hasn't been enqueued.
        return None
    finally:
        con.close()


def list_jobs(
    status: Optional[str] = None,
    job_type: Optional[str] = None,
    limit: int = 50,
) -> list[dict]:
    """List recent jobs with optional filters. Newest first."""
    con = _open_ro_con()
    try:
        sql = "SELECT * FROM jobs WHERE 1=1"
        params: list[Any] = []
        if status:
            sql += " AND status = ?"
            params.append(status)
        if job_type:
            sql += " AND job_type = ?"
            params.append(job_type)
        sql += " ORDER BY enqueued_at DESC, id DESC LIMIT ?"
        params.append(limit)
        rows = con.execute(sql, params).fetchall()
        return [_decode_row(r) for r in rows if r is not None]
    except sqlite3.OperationalError:
        return []
    finally:
        con.close()
