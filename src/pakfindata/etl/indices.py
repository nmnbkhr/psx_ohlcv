"""ETL: PSX indices (psx_indices table).

Single entrypoint :func:`sync` used by three call sites:

- CLI handler in ``cli.py::handle_indices_sync`` (cron + manual)
- Worker handler in ``worker/handlers/sync_indices.py`` (UI button)
- Programmatic callers (tests, REPL)

The function:

1. Fetches index snapshots from PSX DPS (HTTP, no DB lock).
2. Writes each row + updates ``data_freshness['indices']`` inside one
   ``safe_writer`` block.
3. On exception: records ``data_freshness.status = 'failed'`` via
   :func:`pakfindata.db.catalog.record_catalog_failure` and re-raises.
4. Returns a JSON-serializable summary dict on success.

The catalog dataset key is ``"indices"`` — the PK in ``data_freshness``.
``"psx_indices"`` is the source table name (different layer).
"""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone

from pakfindata.config import get_db_path
from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.safe_writer import safe_writer
from pakfindata.sources.indices import fetch_indices_data, save_index_data

DATASET_ID = "indices"
SOURCE = "psx_dps"


def sync() -> dict:
    """Run the PSX-indices sync end-to-end.

    Returns:
        Dict with keys::

            {
                "indices_count": int,      # rows saved this run
                "latest_date":   str|None, # MAX(index_date) post-write
                "duration_ms":   int,      # wall time
                "as_of":         str,      # ISO timestamp when sync started
            }

    Raises:
        Whatever ``fetch_indices_data`` / ``save_index_data`` /
        ``safe_writer`` raise. A best-effort
        ``record_catalog_failure`` is written before the exception
        propagates.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()
    try:
        # HTTP fetch outside the writer lock — same pattern as the
        # CLI handler + the legacy inline Dashboard button.
        data = fetch_indices_data()
        with safe_writer() as con:
            count = sum(1 for d in data if save_index_data(con, d))
            update_catalog_from_table(con, DATASET_ID, source=SOURCE)
    except Exception as exc:
        # Best-effort failure record; never masks the real exception.
        record_catalog_failure(DATASET_ID, source=SOURCE, error=exc)
        raise

    # Read back MAX(index_date) for the result payload — a tiny ro
    # connection so we don't reuse the writer.
    latest: str | None
    rcon = sqlite3.connect(
        f"file:{get_db_path()}?mode=ro", uri=True, check_same_thread=False
    )
    try:
        row = rcon.execute("SELECT MAX(index_date) FROM psx_indices").fetchone()
        latest = row[0] if row and row[0] else None
    finally:
        rcon.close()

    return {
        "indices_count": count,
        "latest_date": latest,
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
