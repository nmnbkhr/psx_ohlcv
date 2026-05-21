"""Worker handler for the ``sync_indices`` job type.

Thin wrapper around :func:`pakfindata.etl.indices.sync` — the same
function the CLI handler and the Dashboard inline path call. Whatever
that function returns becomes the ``jobs.result`` JSON column.

Params accepted (currently none — ``sync()`` takes no arguments):
    (reserved for future flags like ``force`` once a real use case
     appears)

Returns:
    See :func:`pakfindata.etl.indices.sync` — at present
    ``{indices_count, latest_date, duration_ms, as_of}``.

Exceptions:
    Bubble up to ``worker.main``, which records ``status='failed'``
    with the traceback. ``sync()`` has already written
    ``record_catalog_failure`` before re-raising — the catalog row is
    correctly marked failed by the time the worker logs the
    exception.
"""

from __future__ import annotations

from pakfindata.etl.indices import sync


def handle_sync_indices() -> dict:
    """Run the consolidated indices ETL and return its result dict."""
    return sync()
