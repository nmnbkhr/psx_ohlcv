"""ETL: SBP / EasyData rates bundle.

This module is the single entry point for the Dashboard "Sync Rates"
button's composite operation:

- KIBOR daily rates via SBP EasyData (``kibor_daily`` table)
- T-Bill + PIB auctions via SBP PMA scraper (``tbill_auctions`` + ``pib_auctions``)
- SBP policy rate via SBP EasyData (``sbp_policy_rates``)

The three component sync paths each have their own CLI / repo writer.
This file bundles them into one ``safe_writer`` transaction so the
Dashboard convenience button runs as a single all-or-nothing job.

Phase 1.6.1 — first sub-wave of the bulk migration. Same pattern as
:mod:`pakfindata.etl.indices` (Phase 1.5): one ``sync_bundle()``
function shared by the worker handler and the Dashboard inline
fallback.

The catalog dataset keys touched on success::

    kibor              source='sbp_easydata'
    treasury           source='sbp'
    pib                source='sbp'
    sbp_policy_rates   source='sbp_easydata'

On exception inside ``safe_writer`` the transaction rolls back (so
none of the four base tables actually changed); ``record_catalog_failure``
is then written for all four datasets to make the failure visible in
the catalog. This mirrors the legacy inline behavior.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.safe_writer import safe_writer
from pakfindata.sources.sbp_easydata import (
    sync_kibor_to_db,
    sync_policy_rate_to_db,
)
from pakfindata.sources.sbp_treasury import SBPTreasuryScraper

DATASETS: tuple[tuple[str, str], ...] = (
    ("kibor", "sbp_easydata"),
    ("treasury", "sbp"),
    ("pib", "sbp"),
    ("sbp_policy_rates", "sbp_easydata"),
)

KIBOR_EASYDATA_DATASET = "kibor"
KIBOR_EASYDATA_SOURCE = "sbp_easydata"


def sync_bundle() -> dict:
    """Run the rates bundle end-to-end (KIBOR + Treasury + Policy).

    Returns:
        Dict shaped::

            {
                "kibor_rows":   int,             # EasyData KIBOR upsert count
                "tbills_ok":    int,             # T-Bill rows from PMA scrape
                "pibs_ok":      int,             # PIB rows from PMA scrape
                "auction_date": str | None,      # latest PMA auction date
                "duration_ms":  int,
                "as_of":        str,             # ISO timestamp at start
            }

    Raises:
        Whatever the upstream fetches / safe_writer raise. Before
        re-raising, ``record_catalog_failure`` is written for all four
        datasets in :data:`DATASETS`.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        # HTTP init outside the writer lock — SBPTreasuryScraper does
        # an inline GET in its constructor (cached session setup).
        scraper = SBPTreasuryScraper()

        with safe_writer() as con:
            kibor_result = sync_kibor_to_db(con)
            treasury_result = scraper.sync_treasury(con)
            sync_policy_rate_to_db(con)

            for dataset, source in DATASETS:
                update_catalog_from_table(con, dataset, source=source)
    except Exception as exc:
        for dataset, source in DATASETS:
            record_catalog_failure(dataset, source=source, error=exc)
        raise

    return {
        "kibor_rows": int(kibor_result.get("kibor_rows", 0)),
        "tbills_ok": int(treasury_result.get("tbills_ok", 0)),
        "pibs_ok": int(treasury_result.get("pibs_ok", 0)),
        "auction_date": treasury_result.get("auction_date"),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }


def sync_kibor_easydata() -> dict:
    """Sync KIBOR daily rates from SBP EasyData only.

    Standalone subset of :func:`sync_bundle` for the Rates Overview
    "Sync KIBOR (EasyData)" button. Faster than the bundle when the
    caller only needs KIBOR updates.

    Returns:
        ``{kibor_rows, duration_ms, as_of}``

    Raises:
        Whatever ``sync_kibor_to_db`` / ``safe_writer`` raise. Before
        re-raising, ``record_catalog_failure`` is written for ``kibor``.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        with safe_writer() as con:
            kibor_result = sync_kibor_to_db(con)
            update_catalog_from_table(
                con, KIBOR_EASYDATA_DATASET, source=KIBOR_EASYDATA_SOURCE
            )
    except Exception as exc:
        record_catalog_failure(
            KIBOR_EASYDATA_DATASET, source=KIBOR_EASYDATA_SOURCE, error=exc
        )
        raise

    return {
        "kibor_rows": int(kibor_result.get("kibor_rows", 0)),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
