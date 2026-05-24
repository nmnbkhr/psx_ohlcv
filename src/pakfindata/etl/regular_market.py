"""ETL: PSX regular market snapshot.

Single entry point :func:`sync_snapshot` used by:

- CLI handler in ``cli.py::handle_regular_market_snapshot``
  (cron daily step + manual `pfsync regular-market snapshot`)
- Worker handler in ``worker/handlers/sync_regular_market.py``
  (Regular Market page button + Dashboard Refresh All)
- Dashboard inline fallback via ``_sync_market_data()``

The snapshot writes to two base tables (``regular_market_current``
and ``regular_market_snapshots``) and the derived ``market_analytics``
tables. All writes run inside one ``safe_writer`` transaction.

Catalog datasets touched on success::

    regular_market_current    source='psx_api'
    regular_market_snapshots  source='psx_api'
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.analytics import compute_all_analytics
from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.safe_writer import safe_writer
from pakfindata.sources.regular_market import (
    fetch_regular_market,
    get_all_current_hashes,
    init_regular_market_schema,
    insert_snapshots,
    upsert_current,
)

# (dataset_id, source, value_type) — value_type added in 2.B.6 so the helper
# truncates the .ts column's 'YYYY-MM-DDTHH:MM:SS+05:00' value before
# writing last_row_date. Without it, last_row_date held the full timestamp
# and downstream days_old computation broke.
DATASETS: tuple[tuple[str, str, str], ...] = (
    ("regular_market_current", "psx_api", "iso_timestamp"),
    ("regular_market_snapshots", "psx_api", "iso_timestamp"),
)


def sync_snapshot(save_unchanged: bool = False) -> dict:
    """Fetch and persist the latest PSX regular-market snapshot.

    Args:
        save_unchanged: If True, every row in the fetched DataFrame is
            written to ``regular_market_snapshots`` (even unchanged
            rows). Default False — only rows whose content hash
            differs from the previous snapshot are kept.

    Returns:
        Dict shaped::

            {
                "symbols":          int,        # rows in source DataFrame
                "rows_upserted":    int,        # current-table upsert count
                "snapshots_saved":  int,        # snapshot-table insert count
                "snapshot_ts":      str | None, # source timestamp
                "gainers":          int,        # from market_analytics
                "losers":           int,
                "unchanged":        int,
                "duration_ms":      int,
                "as_of":            str,        # ISO timestamp at start
            }

    Raises:
        Whatever ``fetch_regular_market`` / ``safe_writer`` raise.
        Before re-raising, ``record_catalog_failure`` is written for
        both datasets in :data:`DATASETS`.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    # HTTP fetch outside the writer lock — same shape as 1.5/1.6 ETL.
    df = fetch_regular_market()
    if df.empty:
        return {
            "symbols": 0,
            "rows_upserted": 0,
            "snapshots_saved": 0,
            "snapshot_ts": None,
            "gainers": 0,
            "losers": 0,
            "unchanged": 0,
            "duration_ms": int((time.monotonic() - t0) * 1000),
            "as_of": as_of,
        }

    snapshot_ts = df["ts"].iloc[0] if "ts" in df.columns else None

    try:
        with safe_writer() as con:
            init_regular_market_schema(con)

            # Load previous content hashes BEFORE upsert so change
            # detection compares against the row state at the start.
            prev_hashes = get_all_current_hashes(con)

            snapshots_saved = insert_snapshots(
                con, df,
                save_unchanged=save_unchanged,
                prev_hashes=prev_hashes,
            )
            rows_upserted = upsert_current(con, df)

            analytics = compute_all_analytics(con, snapshot_ts) if snapshot_ts else None

            for dataset, source, value_type in DATASETS:
                update_catalog_from_table(
                    con, dataset, source=source, value_type=value_type,
                )
    except Exception as exc:
        for dataset, source, _value_type in DATASETS:
            record_catalog_failure(dataset, source=source, error=exc)
        raise

    market_analytics = (analytics or {}).get("market_analytics") or {}

    return {
        "symbols": len(df),
        "rows_upserted": int(rows_upserted),
        "snapshots_saved": int(snapshots_saved),
        "snapshot_ts": snapshot_ts,
        "gainers": int(market_analytics.get("gainers_count") or 0),
        "losers": int(market_analytics.get("losers_count") or 0),
        "unchanged": int(market_analytics.get("unchanged_count") or 0),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
