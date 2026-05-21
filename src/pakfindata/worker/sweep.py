"""Stale-job sweep.

If the worker was SIGKILL'd (or the host hard-crashed), any jobs that
were ``running`` will stay ``running`` forever — no one's going to
finish them. This module marks those as ``failed`` with a clear
message, so the queue's state matches reality.

Run at worker startup (see :func:`pakfindata.worker.main.main`).
Idempotent. Safe to run repeatedly.
"""

from __future__ import annotations

import os

from pakfindata.db.jobs import finish_job, list_jobs


def _pid_alive(pid: int | None) -> bool:
    """Return True if ``pid`` is a live process this user can signal."""
    if not pid:
        return False
    try:
        os.kill(pid, 0)  # signal 0: probe; raises if the process is gone
        return True
    except (ProcessLookupError, PermissionError):
        return False


def sweep_stale_jobs() -> int:
    """Fail any 'running' job whose worker_pid is dead. Returns sweep count."""
    running = list_jobs(status="running", limit=500)
    swept = 0
    for j in running:
        pid = j.get("worker_pid")
        if not _pid_alive(pid):
            finish_job(
                j["id"],
                status="failed",
                error="worker died before completion",
                error_detail=f"stale job swept; original worker_pid={pid}",
            )
            swept += 1
    return swept


if __name__ == "__main__":
    n = sweep_stale_jobs()
    print(f"swept {n} stale jobs")
