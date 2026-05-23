"""Worker handler for the ``rebuild_eod_summary_missing`` job type.

Thin wrapper around :func:`pakfindata.etl.eod_summary.rebuild_missing`.

Params:
    batch_size (int, default 50): batch size for the bulk refresh.
"""

from __future__ import annotations

from pakfindata.etl.eod_summary import rebuild_missing


def handle_rebuild_eod_summary_missing(batch_size: int = 50) -> dict:
    return rebuild_missing(batch_size=batch_size)
