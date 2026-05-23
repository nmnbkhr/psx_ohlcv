"""ETL: PSX indices (psx_indices table).

Single entrypoint :func:`sync` used by three call sites:

- CLI handler in ``cli.py::handle_indices_sync`` (cron + manual)
- Worker handler in ``worker/handlers/sync_indices.py`` (UI button)
- Programmatic callers (tests, REPL)

The function:

1. Fetches index snapshots from PSX DPS (HTTP, no DB lock).
2. Writes each row + updates ``data_freshness['indices']`` +
   runs ``data_quality_rules`` for ``domain='indices'`` inside one
   ``safe_writer`` block.
3. On error-severity validator failure: raises ``DataQualityError``,
   transaction rolls back, the ``except`` clause records catalog
   failure and re-raises. **Pollution never persists.**
4. On any other exception: records ``data_freshness.status='failed'``
   via :func:`pakfindata.db.catalog.record_catalog_failure` and
   re-raises.
5. Returns a JSON-serializable summary dict on success — now includes
   a ``validation`` sub-dict with pass/fail counts so callers can
   surface data quality alongside data freshness.

The catalog dataset key is ``"indices"`` — the PK in ``data_freshness``
and the join key on ``data_quality_rules.domain``. ``"psx_indices"``
is the source table name (different layer; lives in rule params).
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone

from pakfindata.config import get_db_path
from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.safe_writer import safe_writer
from pakfindata.quality import DataQualityError, run_checks_for_domain
from pakfindata.sources.indices import fetch_indices_data, save_index_data

DATASET_ID = "indices"
DOMAIN = "indices"
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
                "validation": {
                    "total":  int,         # rules evaluated
                    "passed": int,
                    "failed": int,         # warn-severity only on happy path;
                                           # error-severity would have raised
                },
            }

    Raises:
        DataQualityError: at least one error-severity rule failed; the
            safe_writer transaction has rolled back. Catalog failure
            recorded; pollution NOT persisted.
        Other: whatever ``fetch_indices_data`` / ``save_index_data`` /
            ``safe_writer`` raise. Catalog failure recorded before
            the exception propagates.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()
    results: list = []
    try:
        # HTTP fetch outside the writer lock — same pattern as the
        # CLI handler + the legacy inline Dashboard button.
        data = fetch_indices_data()
        with safe_writer() as con:
            count = sum(1 for d in data if save_index_data(con, d))
            update_catalog_from_table(con, DATASET_ID, source=SOURCE)

            # Phase 2.A.1: run validators inside the same transaction
            # as the data write. Any error-severity failure raises and
            # safe_writer rolls back — pollution never persists.
            results = run_checks_for_domain(con, DOMAIN)
            for r in results:
                con.execute(
                    """
                    INSERT INTO data_quality_results
                    (rule_id, domain, check_type, severity, passed,
                     measured, expected, error_message, duration_ms)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        r.rule_id,
                        r.domain,
                        r.check_type,
                        r.severity,
                        1 if r.passed else 0,
                        json.dumps(r.measured),
                        json.dumps(r.expected),
                        r.error_message,
                        r.duration_ms,
                    ),
                )
            errors = [
                r for r in results
                if r.severity == "error" and not r.passed
            ]
            if errors:
                raise DataQualityError(
                    f"{len(errors)} error-severity validation(s) failed "
                    f"for domain={DOMAIN!r}: "
                    + "; ".join(e.error_message or e.rule_id for e in errors[:3])
                )
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

    passed_count = sum(1 for r in results if r.passed)
    return {
        "indices_count": count,
        "latest_date": latest,
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
        "validation": {
            "total": len(results),
            "passed": passed_count,
            "failed": len(results) - passed_count,
        },
    }
