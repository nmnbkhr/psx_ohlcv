"""Job-type → handler dispatch registry.

The worker's ``main`` loop looks up the handler here via
``REGISTRY.get(job_type)``. Adding a new job type:

1. Create ``src/pakfindata/worker/handlers/your_job.py`` with a handler
   function ``handle_your_job(**params) -> dict | None``.
2. Register it here: ``REGISTRY['your_job'] = handle_your_job``.
3. Submit it via ``POST /v1/jobs/your_job`` (the route validates the
   type against this registry — unknown types return 400).

Phase 1.5+ adds the real ETL handlers (sync_indices, rebuild_eod_summary,
etc.). This milestone has just ``ping`` to prove the pipeline works.
"""

from __future__ import annotations

from typing import Callable

from pakfindata.worker.handlers.ping import handle_ping
from pakfindata.worker.handlers.rebuild_eod_summary_all import (
    handle_rebuild_eod_summary_all,
)
from pakfindata.worker.handlers.rebuild_eod_summary_missing import (
    handle_rebuild_eod_summary_missing,
)
from pakfindata.worker.handlers.rebuild_eod_summary_today import (
    handle_rebuild_eod_summary_today,
)
from pakfindata.worker.handlers.sync_benchmark import handle_sync_benchmark
from pakfindata.worker.handlers.sync_easydata_csv import (
    handle_sync_easydata_csv,
)
from pakfindata.worker.handlers.sync_indices import handle_sync_indices
from pakfindata.worker.handlers.sync_kibor_easydata import (
    handle_sync_kibor_easydata,
)
from pakfindata.worker.handlers.sync_rates_bundle import handle_sync_rates_bundle
from pakfindata.worker.handlers.sync_regular_market import (
    handle_sync_regular_market,
)
from pakfindata.worker.handlers.sync_sbp_curve import handle_sync_sbp_curve
from pakfindata.worker.handlers.sync_treasury_auctions import (
    handle_sync_treasury_auctions,
)


REGISTRY: dict[str, Callable[..., dict | None]] = {
    "ping": handle_ping,
    "rebuild_eod_summary_all": handle_rebuild_eod_summary_all,
    "rebuild_eod_summary_missing": handle_rebuild_eod_summary_missing,
    "rebuild_eod_summary_today": handle_rebuild_eod_summary_today,
    "sync_benchmark": handle_sync_benchmark,
    "sync_easydata_csv": handle_sync_easydata_csv,
    "sync_indices": handle_sync_indices,
    "sync_kibor_easydata": handle_sync_kibor_easydata,
    "sync_rates_bundle": handle_sync_rates_bundle,
    "sync_regular_market": handle_sync_regular_market,
    "sync_sbp_curve": handle_sync_sbp_curve,
    "sync_treasury_auctions": handle_sync_treasury_auctions,
}


def known_types() -> list[str]:
    """Sorted list of registered job_type keys — used by the API
    validation error message and the UI dropdown."""
    return sorted(REGISTRY.keys())
