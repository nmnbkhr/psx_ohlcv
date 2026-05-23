"""Worker handler for the ``sync_kibor_easydata`` job type.

Thin wrapper around :func:`pakfindata.etl.rates.sync_kibor_easydata` —
the same function the Rates Overview inline fallback calls.

Returns:
    See :func:`pakfindata.etl.rates.sync_kibor_easydata` — at present
    ``{kibor_rows, duration_ms, as_of}``.
"""

from __future__ import annotations

from pakfindata.etl.rates import sync_kibor_easydata


def handle_sync_kibor_easydata() -> dict:
    """Run the KIBOR-EasyData ETL and return its result."""
    return sync_kibor_easydata()
