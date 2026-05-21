"""Worker handler for the ``sync_index_membership`` job type.

Thin wrapper around :func:`pakfindata.etl.instruments.sync_membership` —
the same function the Indices page inline fallback calls.
"""

from __future__ import annotations

from pakfindata.etl.instruments import sync_membership


def handle_sync_index_membership() -> dict:
    """Run the index-membership ETL and return its result."""
    return sync_membership()
