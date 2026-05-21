"""Worker handler for the ``sync_easydata_csv`` job type.

Thin wrapper around :func:`pakfindata.etl.easydata.sync_csvs_to_db` —
the same function the Treasury Dashboard inline fallback calls.
"""

from __future__ import annotations

from pakfindata.etl.easydata import sync_csvs_to_db


def handle_sync_easydata_csv() -> dict:
    """Run the EasyData CSV → DB sync and return its result."""
    return sync_csvs_to_db()
