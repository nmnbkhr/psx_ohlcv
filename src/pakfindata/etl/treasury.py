"""ETL: SBP Treasury auctions (T-Bill + PIB).

Single entry point :func:`sync_auctions` used by three call sites:

- CLI handler in ``cli.py::handle_treasury`` for ``treasury_command == "sync"``
  (cron weekday + manual `pfsync treasury sync`)
- Worker handler in ``worker/handlers/sync_treasury_auctions.py`` (UI button)
- Programmatic callers (tests, REPL)

Wraps :class:`pakfindata.sources.sbp_treasury.SBPTreasuryScraper` —
scrapes the SBP PMA page and upserts into ``tbill_auctions`` /
``pib_auctions``.

Catalog datasets touched on success::

    treasury   source='sbp'
    pib        source='sbp'

On exception inside ``safe_writer`` the transaction rolls back (so
neither table actually changed); ``record_catalog_failure`` is then
written for both datasets.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone

from pakfindata.db.catalog import (
    record_catalog_failure,
    update_catalog_from_table,
)
from pakfindata.db.safe_writer import safe_writer
from pakfindata.sources.sbp_treasury import SBPTreasuryScraper

DATASETS: tuple[tuple[str, str], ...] = (
    ("treasury", "sbp"),
    ("pib", "sbp"),
)


def sync_auctions() -> dict:
    """Scrape SBP PMA page and upsert T-Bill + PIB auctions.

    Returns:
        Dict shaped::

            {
                "tbills_ok":    int,        # T-Bill rows upserted
                "pibs_ok":      int,        # PIB rows upserted
                "auction_date": str | None, # PMA-derived auction date
                "failed":       int,        # row-level upsert failures
                "duration_ms":  int,
                "as_of":        str,
            }

    Raises:
        Whatever the scraper or safe_writer raises. Before re-raising,
        ``record_catalog_failure`` is written for both datasets in
        :data:`DATASETS`.
    """
    t0 = time.monotonic()
    as_of = datetime.now(timezone.utc).isoformat()

    try:
        # HTTP init outside the writer lock — same pattern as 1.6.1.
        scraper = SBPTreasuryScraper()

        with safe_writer() as con:
            result = scraper.sync_treasury(con)
            for dataset, source in DATASETS:
                update_catalog_from_table(con, dataset, source=source)
    except Exception as exc:
        for dataset, source in DATASETS:
            record_catalog_failure(dataset, source=source, error=exc)
        raise

    return {
        "tbills_ok": int(result.get("tbills_ok", 0)),
        "pibs_ok": int(result.get("pibs_ok", 0)),
        "auction_date": result.get("auction_date"),
        "failed": int(result.get("failed", 0)),
        "duration_ms": int((time.monotonic() - t0) * 1000),
        "as_of": as_of,
    }
