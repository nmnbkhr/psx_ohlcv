"""Worker handler for the ``sync_benchmark`` job type.

Thin wrapper around :func:`pakfindata.etl.benchmark.sync` — the same
function the Rates Overview, Bond Market, and Benchmark Monitor
inline fallbacks call.
"""

from __future__ import annotations

from pakfindata.etl.benchmark import sync


def handle_sync_benchmark() -> dict:
    """Run the benchmark-snapshot ETL and return its result."""
    return sync()
