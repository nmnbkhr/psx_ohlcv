"""Worker handler for the ``sync_rates_bundle`` job type.

Thin wrapper around :func:`pakfindata.etl.rates.sync_bundle` — the same
function the Dashboard's inline fallback calls. See that module for
the full semantics (atomic safe_writer block, 4 catalog datasets,
failure recording).

Params accepted: none today.

Returns:
    See :func:`pakfindata.etl.rates.sync_bundle` — at present
    ``{kibor_rows, tbills_ok, pibs_ok, auction_date, duration_ms, as_of}``.
"""

from __future__ import annotations

from pakfindata.etl.rates import sync_bundle


def handle_sync_rates_bundle() -> dict:
    """Run the consolidated rates-bundle ETL and return its result."""
    return sync_bundle()
