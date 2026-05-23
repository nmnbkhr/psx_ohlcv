"""Worker handler for the ``rebuild_eod_summary_today`` job type.

Thin wrapper around :func:`pakfindata.etl.eod_summary.rebuild_today`.

Params:
    date (str | None, default None): YYYY-MM-DD override. If None,
        auto-picks the latest trading day in ``eod_ohlcv``.
"""

from __future__ import annotations

from pakfindata.etl.eod_summary import rebuild_today


def handle_rebuild_eod_summary_today(date: str | None = None) -> dict:
    return rebuild_today(date=date)
