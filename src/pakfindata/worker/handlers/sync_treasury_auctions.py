"""Worker handler for the ``sync_treasury_auctions`` job type.

Thin wrapper around :func:`pakfindata.etl.treasury.sync_auctions` —
the same function the CLI handler and the Dashboard / Rates Overview
inline fallbacks call.

Returns:
    See :func:`pakfindata.etl.treasury.sync_auctions` — at present
    ``{tbills_ok, pibs_ok, auction_date, failed, duration_ms, as_of}``.
"""

from __future__ import annotations

from pakfindata.etl.treasury import sync_auctions


def handle_sync_treasury_auctions() -> dict:
    """Run the consolidated treasury-auctions ETL and return its result."""
    return sync_auctions()
