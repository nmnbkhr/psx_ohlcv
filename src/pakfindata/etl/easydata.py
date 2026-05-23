"""ETL: SBP EasyData CSV-on-disk → DB sync.

Single entry point :func:`sync_csvs_to_db` used by:

- Worker handler in ``worker/handlers/sync_easydata_csv.py``
- Treasury Dashboard inline fallback ("Sync CSVs to DB")

Wraps :func:`pakfindata.sources.sbp_easydata.sync_all_to_db` which
runs four component syncs in sequence (KIBOR, FX monthly/daily, policy
rate) — all reading from local EasyData CSVs and writing to their
respective base tables.

This sub-wave migrates the *disk → DB* step. The earlier *API → disk*
step is still a background fetch (``start_fetch_background``) and is
left untouched (daemon-trigger pattern, per the 1.6 skip list).

Catalog datasets touched on success::

    kibor                source='sbp_easydata'
    sbp_fx_monthly_avg   source='sbp_easydata'
    sbp_fx_daily_avg     source='sbp_easydata'
    sbp_policy_rates     source='sbp_easydata'

Note: the FX *monthly/daily averages* tables here are different from
the FX interbank / kerb tables that the FX domain owns. They share a
common ``sbp_easydata`` source label but live in their own catalog
rows. The FX *UI pages* are deferred from 1.6 per Hard Rule 7 (FX
domain coherence); this Treasury-Dashboard button has always been a
"refresh whatever the API has on disk" convenience, not an FX-page
sync, and migrates here.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.safe_writer import safe_writer
from pakfindata.sources.sbp_easydata import sync_all_to_db

DATASETS: tuple[tuple[str, str], ...] = (
    ("kibor", "sbp_easydata"),
    ("sbp_fx_monthly_avg", "sbp_easydata"),
    ("sbp_fx_daily_avg", "sbp_easydata"),
    ("sbp_policy_rates", "sbp_easydata"),
)


def sync_csvs_to_db() -> dict:
    """Read SBP EasyData CSVs on disk and upsert into the base tables.

    Returns:
        Dict shaped::

            {
                # Component counts (whatever sync_all_to_db returns):
                "kibor_rows":     int,
                "fx_rows":        int,
                "daily_fx_rows":  int,
                "policy_rate_rows": int,
                # Plus standard fields:
                "duration_ms":    int,
                "as_of":          str,
            }

    Raises:
        Whatever ``sync_all_to_db`` / ``safe_writer`` raise. Before
        re-raising, ``record_catalog_failure`` is written for all four
        datasets in :data:`DATASETS`.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        with safe_writer() as con:
            counts = sync_all_to_db(con)
            for dataset, source in DATASETS:
                update_catalog_from_table(con, dataset, source=source)
    except Exception as exc:
        for dataset, source in DATASETS:
            record_catalog_failure(dataset, source=source, error=exc)
        raise

    return {
        **{k: int(v) for k, v in counts.items()},
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
