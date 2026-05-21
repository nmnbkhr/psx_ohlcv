"""ETL: SBP benchmark snapshot.

Single entry point :func:`sync` used by:

- Worker handler in ``worker/handlers/sync_benchmark.py``
- Three UI inline fallbacks (Rates Overview, Bond Market, Benchmark
  Monitor) — same scraper, same output

Wraps :class:`pakfindata.sources.sbp_bond_market.SBPBondMarketScraper`
which scrapes the SBP MSM page and writes a single ``benchmark_snapshot``
row (one row per scrape, multiple metrics).

Catalog dataset touched::

    benchmark_snapshot   source='sbp_bond_market'
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.safe_writer import safe_writer
from pakfindata.sources.sbp_bond_market import SBPBondMarketScraper

DATASET_ID = "benchmark_snapshot"
SOURCE = "sbp_bond_market"


def sync() -> dict:
    """Scrape the latest SBP benchmark snapshot.

    Returns:
        Dict shaped::

            {
                "status":          str,            # 'ok' or 'failed'
                "metrics_stored":  int,
                "date":            str | None,     # benchmark snapshot date
                "duration_ms":     int,
                "as_of":           str,            # ISO timestamp at start
            }

    Raises:
        Whatever the scraper / safe_writer raises. Before re-raising,
        ``record_catalog_failure`` is written for ``benchmark_snapshot``.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        # HTTP init outside the writer lock.
        scraper = SBPBondMarketScraper()

        with safe_writer() as con:
            result = scraper.sync_benchmark(con)
            update_catalog_from_table(con, DATASET_ID, source=SOURCE)
    except Exception as exc:
        record_catalog_failure(DATASET_ID, source=SOURCE, error=exc)
        raise

    return {
        "status": result.get("status", "unknown"),
        "metrics_stored": int(result.get("metrics_stored", 0)),
        "date": result.get("date"),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
