"""Worker handler for the ``sync_sbp_curve`` job type.

Thin wrapper around :func:`pakfindata.etl.rates.sync_sbp_curve` — the
same function the CLI handler and the Treasury Dashboard inline
fallback call.
"""

from __future__ import annotations

from pakfindata.etl.rates import sync_sbp_curve


def handle_sync_sbp_curve() -> dict:
    """Run the SBP-curve ETL and return its result."""
    return sync_sbp_curve()
