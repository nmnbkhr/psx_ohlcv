"""Worker handler for the ``sync_regular_market`` job type.

Thin wrapper around :func:`pakfindata.etl.regular_market.sync_snapshot`
— the same function the CLI handler, Regular Market page button, and
Dashboard Refresh-All flow call.

Params:
    save_unchanged (bool, default False): forwarded to
        :func:`sync_snapshot`. UI passes True when the "Save all rows"
        checkbox is ticked.
"""

from __future__ import annotations

from pakfindata.etl.regular_market import sync_snapshot


def handle_sync_regular_market(save_unchanged: bool = False) -> dict:
    """Run the regular-market-snapshot ETL and return its result."""
    return sync_snapshot(save_unchanged=save_unchanged)
