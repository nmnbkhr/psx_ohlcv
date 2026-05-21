"""Worker handler for the ``build_intraday_summary_missing`` job type.

Thin wrapper around :func:`pakfindata.etl.intraday_summary.build_missing` —
the same function the Intraday Sync tab inline fallback calls.

No params. Loops over every JSONL date that isn't already in
``intraday_daily_summary`` and runs the per-date aggregator in
sequence. Each date is its own ``safe_writer`` block so a single bad
date doesn't block the rest.
"""

from __future__ import annotations

from pakfindata.etl.intraday_summary import build_missing


def handle_build_intraday_summary_missing() -> dict:
    return build_missing()
