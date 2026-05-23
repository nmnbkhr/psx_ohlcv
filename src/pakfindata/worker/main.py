"""pakfindata worker entrypoint.

Polls the ``jobs`` table every :data:`POLL_INTERVAL_SEC` seconds,
claims the next pending job, dispatches it to the matching handler
in :data:`pakfindata.worker.registry.REGISTRY`, and records the
outcome via :func:`pakfindata.db.jobs.finish_job`.

Run:
    python -m pakfindata.worker.main
or via systemd (Phase 1.4.4):
    systemctl --user start pakfindata-worker.service

Design (Hard Rules from PHASE1_04_WORKER_SCAFFOLD.md):
- Single process, single thread, single job at a time. SQLite's write
  lock is the bottleneck; parallel workers would just contend.
- All writes go through ``safe_writer`` (Phase 0 invariant).
- SIGTERM finishes the current job, then exits cleanly.
- SIGKILL leaves an orphaned 'running' row; :mod:`worker.sweep` (run
  at startup) marks those as failed.

The worker does NOT call the HTTP API. It reads + writes SQLite
directly via :mod:`pakfindata.db.jobs`. No ``PAKFINDATA_API_TOKEN``
required.
"""

from __future__ import annotations

import logging
import os
import signal
import time
import traceback

from pakfindata.api.logging import configure_logging
from pakfindata.db.jobs import claim_next_job, finish_job, init_jobs_schema
from pakfindata.db.safe_writer import safe_writer
from pakfindata.worker.registry import REGISTRY, known_types

POLL_INTERVAL_SEC = 2.0
BUSY_BACKOFF_SEC = 5.0  # extra sleep when safe_writer says "another writer holds the lock"

logger = logging.getLogger("pakfindata.worker")
_shutdown = False


def _handle_sigterm(signum, frame) -> None:
    global _shutdown
    logger.info("received signal %s; will exit after current job", signum)
    _shutdown = True


def _bootstrap_schema() -> None:
    """Make sure the jobs table exists before the first claim attempt."""
    with safe_writer() as con:
        init_jobs_schema(con)


def _run_handler(job: dict) -> dict:
    """Look up + call the handler. Caller catches exceptions."""
    handler = REGISTRY.get(job["job_type"])
    if handler is None:
        raise LookupError(
            f"unknown job_type: {job['job_type']!r} "
            f"(registered: {known_types()})"
        )
    result = handler(**job["params"])
    return result or {}


def main() -> int:
    configure_logging()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    pid = os.getpid()
    logger.info(
        "worker starting pid=%d poll=%.1fs handlers=%s",
        pid, POLL_INTERVAL_SEC, known_types(),
    )

    # Sub-wave 1.4.4 hooks the stale-job sweep here, before the first
    # claim attempt. Defensive import — sweep module lands in 1.4.4.
    try:
        from pakfindata.worker.sweep import sweep_stale_jobs

        swept = sweep_stale_jobs()
        if swept:
            logger.warning("swept %d stale 'running' jobs at startup", swept)
    except ImportError:
        # 1.4.2-only state — sweep doesn't exist yet.
        pass

    try:
        _bootstrap_schema()
    except Exception as exc:  # noqa: BLE001 — log + bail; systemd will restart
        logger.error("schema bootstrap failed: %s", exc)
        return 1

    while not _shutdown:
        try:
            job = claim_next_job(worker_pid=pid)
        except Exception as exc:  # noqa: BLE001
            # Most likely SafeWriterBusyError (daily_sync holds the
            # writer lock at 03:45 PKT for ~19 min, plus any UI
            # sync button currently running). Wait and retry.
            logger.warning("claim failed (%s); backing off", exc)
            time.sleep(BUSY_BACKOFF_SEC)
            continue

        if job is None:
            time.sleep(POLL_INTERVAL_SEC)
            continue

        logger.info(
            "claimed job_id=%d type=%s source=%s params=%s",
            job["id"], job["job_type"], job["source"], job["params"],
        )

        try:
            result = _run_handler(job)
            finish_job(job["id"], status="ok", result=result)
            logger.info(
                "job_id=%d status=ok result=%s", job["id"], result
            )
        except Exception as exc:  # noqa: BLE001 — capture every failure
            err = str(exc)[:500]
            detail = traceback.format_exc()
            try:
                finish_job(
                    job["id"], status="failed", error=err, error_detail=detail
                )
            except Exception as fin_exc:  # noqa: BLE001
                # finish_job itself failed (DB busy / IO error). Log and
                # keep looping — the orphan stays 'running' and gets
                # picked up by the next startup sweep.
                logger.error(
                    "finish_job(%d, failed) failed: %s", job["id"], fin_exc
                )
            else:
                logger.error(
                    "job_id=%d status=failed: %s", job["id"], err
                )

    logger.info("worker pid=%d exiting cleanly", pid)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
