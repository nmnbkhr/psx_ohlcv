"""data_catalog — single source of truth for dataset freshness.

Every safe_writer-migrated sync path writes a row here in the SAME
transaction as its data writes. Every UI freshness query reads from here.
Eliminates the multi-source drift seen in Phase 0 verification, where
dashboard / intraday dropdown / index monitor disagreed about "latest date".

Schema lives in `data_freshness` (table name predates this milestone — see
db/schema.py + the v1 migration in db/connection.py). This module exposes
a `dataset_id` API on top of the existing `domain` PK column.

Public API
----------
    update_catalog(con, dataset_id, *, latest_date=None, row_count=None,
                   status='ok', source, error=None, notes=None,
                   source_table=None, display_name=None, date_column='date')
        Upsert a row INSIDE the caller's transaction. The caller owns the
        commit (typically via safe_writer's __exit__).

    get_catalog(dataset_id=None)
        Read one row (or all rows if dataset_id is None). Opens its own
        short-lived read connection — does not require caller's transaction.
        Returned dicts use `dataset_id` instead of the SQL `domain` column.

    get_freshness(dataset_id)
        Convenience wrapper. Returns (days_old, latest_date_str, status)
        or (None, None, 'missing'). Read-only.

Design rules
------------
- Writes happen via the passed `con`; caller owns the transaction.
- Reads open their own short-lived read connection (`?mode=ro`).
- update_catalog is idempotent — upsert by `domain`.
- Failed syncs should still update the catalog with status='failed' so
  downstream UI can show a warning badge instead of silently displaying
  stale data.
- COALESCE on `last_row_date`, `row_count`, `notes` means a failed sync
  does NOT clobber the previously-known good values.
"""

import sqlite3
from datetime import datetime
from typing import Any

from pakfindata.config import get_db_path

__all__ = [
    "update_catalog",
    "update_catalog_from_table",
    "record_catalog_failure",
    "get_catalog",
    "get_freshness",
]


_VALID_STATUSES = {"ok", "partial", "failed", "unknown"}


def update_catalog(
    con: sqlite3.Connection,
    dataset_id: str,
    *,
    latest_date: str | None = None,
    row_count: int | None = None,
    status: str = "ok",
    source: str,
    error: str | None = None,
    notes: str | None = None,
    source_table: str | None = None,
    display_name: str | None = None,
    date_column: str = "date",
) -> None:
    """Upsert a freshness row for `dataset_id` inside the caller's transaction.

    The caller owns the commit. Use this from inside a `safe_writer()` block.

    For new rows (not pre-seeded in migration v1), display_name and
    source_table default to sensible derivations from dataset_id.
    """
    if not dataset_id or not dataset_id.replace("_", "").isalnum():
        raise ValueError(
            f"dataset_id must be non-empty snake_case identifier; got {dataset_id!r}"
        )
    if not source:
        raise ValueError("source must be non-empty")
    if status not in _VALID_STATUSES:
        raise ValueError(
            f"status must be one of {_VALID_STATUSES}; got {status!r}"
        )

    display_name = display_name or dataset_id.replace("_", " ").title()
    source_table = source_table or dataset_id

    con.execute(
        """
        INSERT INTO data_freshness (
            domain, display_name, source_table, date_column,
            last_sync_at, last_row_date, row_count, status,
            last_sync_error, source, schema_version, notes, updated_at
        )
        VALUES (
            ?, ?, ?, ?,
            datetime('now'), ?, ?, ?,
            ?, ?, 1, ?, datetime('now')
        )
        ON CONFLICT(domain) DO UPDATE SET
            last_sync_at    = datetime('now'),
            last_row_date   = COALESCE(excluded.last_row_date, last_row_date),
            row_count       = COALESCE(excluded.row_count, row_count),
            status          = excluded.status,
            last_sync_error = excluded.last_sync_error,
            source          = excluded.source,
            notes           = COALESCE(excluded.notes, notes),
            updated_at      = datetime('now')
        """,
        (
            dataset_id,
            display_name,
            source_table,
            date_column,
            latest_date,
            row_count,
            status,
            error,
            source,
            notes,
        ),
    )


def update_catalog_from_table(
    con: sqlite3.Connection,
    dataset_id: str,
    *,
    source: str,
    status: str = "ok",
    error: str | None = None,
    notes: str | None = None,
) -> None:
    """Update the catalog by re-reading MAX(date_column) + COUNT(*) from
    the source_table registered for this dataset_id.

    Convenience wrapper for the common case in safe_writer-migrated sync
    buttons: the SQL stays inside the caller's transaction (so it sees
    the rows just written), and per-button code stays one line per
    affected dataset.

    Requires the dataset to be pre-registered (run
    `scripts/backfill_data_catalog.py` once on a new DB). For special
    date columns (TEXT timestamps, unix epoch integers) call
    `update_catalog()` directly with an explicit `latest_date` instead.
    """
    row = con.execute(
        "SELECT source_table, date_column FROM data_freshness WHERE domain = ?",
        (dataset_id,),
    ).fetchone()
    if row is None:
        raise ValueError(
            f"Dataset {dataset_id!r} not registered. Run "
            f"scripts/backfill_data_catalog.py first, or call "
            f"update_catalog() directly with explicit source_table / "
            f"date_column."
        )
    source_table, date_column = row[0], row[1]

    try:
        result = con.execute(
            f"SELECT MAX({date_column}), COUNT(*) FROM {source_table}"
        ).fetchone()
        latest, count = result[0], int(result[1] or 0)
    except sqlite3.OperationalError as exc:
        # Schema drift or similar — record as failed (preserves last-good
        # values via COALESCE in update_catalog).
        update_catalog(
            con,
            dataset_id,
            status="failed",
            source=source,
            error=f"freshness query failed: {exc}",
        )
        return

    update_catalog(
        con,
        dataset_id,
        latest_date=latest,
        row_count=count,
        status=status,
        source=source,
        error=error,
        notes=notes,
    )


def record_catalog_failure(
    dataset_id: str,
    *,
    source: str,
    error: str | Exception,
) -> None:
    """Best-effort: record `status='failed'` for a sync that aborted.

    Opens a NEW tiny safe_writer block because the original sync's
    transaction has already been rolled back. Swallows secondary errors
    so a catalog write failure never masks the original sync error
    surfaced to the user.

    Usage:
        try:
            with safe_writer() as wcon:
                ...
                update_catalog_from_table(wcon, 'psx_indices', source='psx_dps')
        except SafeWriterBusyError:
            ...  # do NOT record (the writer is unavailable)
        except Exception as e:
            st.error(f"Sync failed: {e}")
            record_catalog_failure('psx_indices', source='psx_dps', error=e)
    """
    from .safe_writer import safe_writer  # local import to avoid cycle

    try:
        with safe_writer() as con:
            update_catalog(
                con,
                dataset_id,
                status="failed",
                source=source,
                error=str(error)[:500],
            )
    except Exception:
        pass  # never mask the original error


def _row_to_api(row: sqlite3.Row | None) -> dict[str, Any] | None:
    """Map a data_freshness row to the public API shape (domain -> dataset_id)."""
    if row is None:
        return None
    d = dict(row)
    # Expose `domain` as `dataset_id` in the API surface
    d["dataset_id"] = d.pop("domain")
    # Alias `last_row_date` -> `latest_date` for consistency with the API params
    if "last_row_date" in d:
        d["latest_date"] = d["last_row_date"]
    return d


def get_catalog(
    dataset_id: str | None = None,
) -> dict[str, Any] | list[dict[str, Any]] | None:
    """Read one row (or all rows) from the catalog.

    Opens its own read-only connection — does not require a caller transaction.

    Returns:
        - dict if dataset_id is provided and the row exists
        - None if dataset_id is provided and the row does not exist
        - list[dict] if dataset_id is None (returns all rows, ordered by domain)
    """
    db_path = get_db_path()
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True, timeout=10)
    con.row_factory = sqlite3.Row
    try:
        if dataset_id is None:
            rows = con.execute(
                "SELECT * FROM data_freshness ORDER BY domain"
            ).fetchall()
            return [_row_to_api(r) for r in rows]  # type: ignore[misc]
        row = con.execute(
            "SELECT * FROM data_freshness WHERE domain = ?", (dataset_id,)
        ).fetchone()
        return _row_to_api(row)
    finally:
        con.close()


def get_freshness(dataset_id: str) -> tuple[int | None, str | None, str]:
    """Return (days_old, latest_date_str, status).

    Returns (None, None, 'missing') when the row does not exist.
    Returns (None, last_row_date_or_None, status) when latest_date is
    NULL or unparseable but the row exists — the caller can still see
    whether the dataset is known and what its status is.
    """
    row = get_catalog(dataset_id)
    if row is None:
        return None, None, "missing"

    status = row.get("status") or "unknown"
    latest = row.get("latest_date")

    if not latest:
        return None, latest, status

    try:
        latest_dt = datetime.strptime(str(latest)[:10], "%Y-%m-%d")
        days_old = (datetime.now() - latest_dt).days
    except (ValueError, TypeError):
        return None, latest, status

    return days_old, str(latest)[:10], status
