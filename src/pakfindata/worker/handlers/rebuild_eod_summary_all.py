"""Worker handler for the ``rebuild_eod_summary_all`` job type.

Thin wrapper around :func:`pakfindata.etl.eod_summary.rebuild_all`.

Long-running (30-60 minutes). The UI submits it as fire-and-forget
via ``api_client.submit_job`` rather than ``run_job_with_progress``
so the dashboard is not blocked.

Params:
    batch_size (int, default 50): batch size for the bulk refresh.
"""

from __future__ import annotations

from pakfindata.etl.eod_summary import rebuild_all


def handle_rebuild_eod_summary_all(batch_size: int = 50) -> dict:
    return rebuild_all(batch_size=batch_size)
