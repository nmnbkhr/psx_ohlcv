"""ETL: index → equity membership table.

Single entry point :func:`sync_membership` used by:

- Worker handler in ``worker/handlers/sync_index_membership.py``
- Indices page inline fallback ("Sync Index Membership" button)

Wraps :func:`pakfindata.db.repositories.instruments.sync_index_membership`
which parses ``regular_market_current.listed_in`` and re-builds
``instrument_membership`` for today.

Catalog dataset touched::

    instrument_membership   source='psx_listed_in'
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.repositories.instruments import sync_index_membership
from pakfindata.db.safe_writer import safe_writer

DATASET_ID = "instrument_membership"
SOURCE = "psx_listed_in"


def sync_membership() -> dict:
    """Re-build ``instrument_membership`` from regular-market listed_in.

    Returns:
        ``{indices, memberships, skipped, duration_ms, as_of}``

    Raises:
        Whatever ``sync_index_membership`` / ``safe_writer`` raise.
        Before re-raising, ``record_catalog_failure`` is written for
        ``instrument_membership``.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        with safe_writer() as con:
            result = sync_index_membership(con)
            update_catalog_from_table(con, DATASET_ID, source=SOURCE)
    except Exception as exc:
        record_catalog_failure(DATASET_ID, source=SOURCE, error=exc)
        raise

    return {
        "indices": int(result.get("indices", 0)),
        "memberships": int(result.get("memberships", 0)),
        "skipped": int(result.get("skipped", 0)),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
