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

__all__ = ["update_catalog", "get_catalog", "get_freshness"]


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
