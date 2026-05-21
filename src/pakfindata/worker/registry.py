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


REGISTRY: dict[str, Callable[..., dict | None]] = {
    "ping": handle_ping,
}


def known_types() -> list[str]:
    """Sorted list of registered job_type keys — used by the API
    validation error message and the UI dropdown."""
    return sorted(REGISTRY.keys())
