"""Worker handler for the ``build_intraday_summary`` job type.

Thin wrapper around :func:`pakfindata.etl.intraday_summary.build_for_date` —
the same function the Intraday Sync tab inline fallback calls.

Params:
    date (str): YYYY-MM-DD — required; identifies the JSONL source
        in ``tick_logs_cloud/ticks_{date}.jsonl``.
"""

from __future__ import annotations

from pakfindata.etl.intraday_summary import build_for_date


def handle_build_intraday_summary(date: str) -> dict:
    return build_for_date(date=date)
