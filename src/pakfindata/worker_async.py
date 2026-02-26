"""Async background worker for PSX data tasks.

Pure asyncio — no Redis, no Docker, no external dependencies beyond aiohttp.
Uses the existing scrape_jobs SQLite table for job tracking.

Usage:
    # As module:
    python -m pakfindata.worker_async --task sync_eod
    python -m pakfindata.worker_async --task sync_eod --symbols OGDC,HBL,MCB

    # Programmatic:
    worker = AsyncTaskWorker()
    job_id = await worker.submit('sync_eod', symbols=['OGDC', 'HBL'])
    result = await worker.run_once()
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import uuid
from datetime import datetime
from typing import Any

from pakfindata.db import (
    connect,
    create_background_job,
    get_scrape_job,
    get_symbols_list,
    init_schema,
    is_job_stop_requested,
    update_job_progress,
    add_job_notification,
    upsert_eod,
)
from pakfindata.sources.async_fetcher import AsyncPSXFetcher
from pakfindata.sources.eod import parse_eod_payload

logger = logging.getLogger("pakfindata")


class AsyncTaskWorker:
    """In-process async task runner with queue and job tracking."""

    def __init__(self):
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._running_tasks: dict[str, asyncio.Task] = {}
        self._results: dict[str, dict] = {}

    async def submit(self, task_name: str, **kwargs: Any) -> str:
        """Submit a task for execution.

        Args:
            task_name: One of 'sync_eod', 'deep_scrape', 'sync_intraday'.
            **kwargs: Task-specific arguments (symbols, batch_size, etc.).

        Returns:
            job_id (8-char string).
        """
        con = connect()
        init_schema(con)

        symbols = kwargs.get("symbols")
        if symbols is None:
            symbols = get_symbols_list(con)

        batch_size = kwargs.get("batch_size", 50)
        config = {"task_name": task_name, **kwargs}

        job_id = create_background_job(
            con,
            job_type=task_name,
            symbols=symbols,
            batch_size=batch_size,
            config=config,
        )

        await self._queue.put(job_id)
        logger.info("Job %s submitted: %s (%d symbols)", job_id, task_name, len(symbols))
        return job_id

    async def get_status(self, job_id: str) -> dict | None:
        """Get task status and results."""
        con = connect()
        job = get_scrape_job(con, job_id)
        if job is None:
            return None

        result = dict(job)
        if job_id in self._results:
            result["worker_result"] = self._results[job_id]
        return result

    async def cancel(self, job_id: str) -> bool:
        """Cancel a running task via stop_requested flag."""
        from pakfindata.db import request_job_stop
        con = connect()
        return request_job_stop(con, job_id)

    async def run_once(self) -> dict:
        """Process a single task from the queue.

        Returns:
            Result dict with job_id, status, and stats.
        """
        job_id = await self._queue.get()
        result = await self._execute_job(job_id)
        self._results[job_id] = result
        self._queue.task_done()
        return result

    async def run_worker(self, max_tasks: int | None = None) -> None:
        """Main worker loop — process tasks from queue until stopped.

        Args:
            max_tasks: Stop after processing this many tasks. None = run forever.
        """
        processed = 0
        while True:
            try:
                job_id = await asyncio.wait_for(self._queue.get(), timeout=5.0)
            except asyncio.TimeoutError:
                continue

            result = await self._execute_job(job_id)
            self._results[job_id] = result
            self._queue.task_done()

            processed += 1
            if max_tasks and processed >= max_tasks:
                break

    async def _execute_job(self, job_id: str) -> dict:
        """Execute a job by routing to the appropriate task function."""
        con = connect()
        job = get_scrape_job(con, job_id)

        if job is None:
            return {"job_id": job_id, "status": "failed", "error": "Job not found"}

        task_name = job.get("job_type", "")
        config = job.get("config", {}) or {}
        symbols = config.get("symbols", [])

        task_map = {
            "sync_eod": sync_eod_task,
            "deep_scrape": deep_scrape_task,
            "sync_intraday": sync_intraday_task,
        }

        task_func = task_map.get(task_name)
        if task_func is None:
            update_job_progress(con, job_id, status="failed")
            return {"job_id": job_id, "status": "failed", "error": f"Unknown task: {task_name}"}

        # Mark as running
        update_job_progress(
            con, job_id,
            status="running",
            pid=os.getpid(),
        )

        try:
            result = await task_func(job_id, symbols, config)
            return result
        except Exception as e:
            logger.exception("Job %s failed: %s", job_id, e)
            update_job_progress(con, job_id, status="failed")
            add_job_notification(
                con, job_id, "failed",
                f"Job {job_id} failed",
                str(e)[:200],
            )
            return {"job_id": job_id, "status": "failed", "error": str(e)}


# =============================================================================
# TASK FUNCTIONS
# =============================================================================

async def sync_eod_task(
    job_id: str,
    symbols: list[str],
    config: dict,
) -> dict:
    """Async EOD sync using AsyncPSXFetcher.

    Fetches EOD data for all symbols concurrently, parses, and upserts to DB.
    """
    con = connect()
    init_schema(con)
    batch_size = config.get("batch_size", 50)
    total = len(symbols)
    ok_count = 0
    fail_count = 0
    rows_inserted = 0

    # Process in batches
    batches = [symbols[i:i + batch_size] for i in range(0, total, batch_size)]
    total_batches = len(batches)

    async with AsyncPSXFetcher() as fetcher:
        for batch_num, batch in enumerate(batches):
            # Check for stop request
            if is_job_stop_requested(con, job_id):
                update_job_progress(con, job_id, status="stopped")
                add_job_notification(
                    con, job_id, "stopped",
                    f"Job {job_id} stopped by user",
                    f"Processed {ok_count}/{total} symbols before stop.",
                )
                return {
                    "job_id": job_id, "status": "stopped",
                    "ok": ok_count, "failed": fail_count,
                    "rows_inserted": rows_inserted,
                }

            result = await fetcher.fetch_eod_batch(batch)

            # Parse and upsert results
            for symbol, data in result["results"].items():
                try:
                    df = parse_eod_payload(symbol, data)
                    if not df.empty:
                        count = upsert_eod(con, df)
                        rows_inserted += count
                    ok_count += 1
                except Exception as e:
                    logger.warning("Parse/upsert failed for %s: %s", symbol, e)
                    fail_count += 1

            fail_count += result["failed"]

            update_job_progress(
                con, job_id,
                current_batch=batch_num + 1,
                symbols_completed=ok_count,
                symbols_failed=fail_count,
                records_inserted=rows_inserted,
            )

    # Final status
    final_status = "completed" if fail_count == 0 else "completed"
    update_job_progress(con, job_id, status=final_status)
    add_job_notification(
        con, job_id, "completed",
        f"EOD sync complete: {ok_count}/{total}",
        f"{ok_count} OK, {fail_count} failed, {rows_inserted} rows inserted.",
    )

    return {
        "job_id": job_id, "status": final_status,
        "ok": ok_count, "failed": fail_count,
        "rows_inserted": rows_inserted,
    }


async def deep_scrape_task(
    job_id: str,
    symbols: list[str],
    config: dict,
) -> dict:
    """Async deep scrape — fetches company pages for detailed data."""
    con = connect()
    batch_size = config.get("batch_size", 50)
    total = len(symbols)
    ok_count = 0
    fail_count = 0

    batches = [symbols[i:i + batch_size] for i in range(0, total, batch_size)]

    async with AsyncPSXFetcher() as fetcher:
        for batch_num, batch in enumerate(batches):
            if is_job_stop_requested(con, job_id):
                update_job_progress(con, job_id, status="stopped")
                add_job_notification(
                    con, job_id, "stopped",
                    f"Deep scrape {job_id} stopped",
                    f"Processed {ok_count}/{total} before stop.",
                )
                return {"job_id": job_id, "status": "stopped", "ok": ok_count, "failed": fail_count}

            for symbol in batch:
                sym, html, error = await fetcher.fetch_company_data(symbol)
                if error:
                    fail_count += 1
                    logger.debug("Deep scrape failed for %s: %s", sym, error)
                else:
                    ok_count += 1

                update_job_progress(
                    con, job_id,
                    current_symbol=sym,
                    current_batch=batch_num + 1,
                    symbols_completed=ok_count,
                    symbols_failed=fail_count,
                )

    update_job_progress(con, job_id, status="completed")
    add_job_notification(
        con, job_id, "completed",
        f"Deep scrape complete: {ok_count}/{total}",
    )
    return {"job_id": job_id, "status": "completed", "ok": ok_count, "failed": fail_count}


async def sync_intraday_task(
    job_id: str,
    symbols: list[str],
    config: dict,
) -> dict:
    """Async intraday sync using AsyncPSXFetcher."""
    con = connect()
    init_schema(con)
    total = len(symbols)
    ok_count = 0
    fail_count = 0

    async with AsyncPSXFetcher() as fetcher:
        result = await fetcher.fetch_intraday_batch(symbols)
        ok_count = result["ok"]
        fail_count = result["failed"]

        update_job_progress(
            con, job_id,
            symbols_completed=ok_count,
            symbols_failed=fail_count,
        )

    update_job_progress(con, job_id, status="completed")
    add_job_notification(
        con, job_id, "completed",
        f"Intraday sync complete: {ok_count}/{total}",
    )
    return {"job_id": job_id, "status": "completed", "ok": ok_count, "failed": fail_count}


# =============================================================================
# CLI
# =============================================================================

def main():
    """CLI entry point for async worker tasks."""
    parser = argparse.ArgumentParser(description="PSX OHLCV Async Worker")
    parser.add_argument("--task", required=True, choices=["sync_eod", "deep_scrape", "sync_intraday"],
                        help="Task to run")
    parser.add_argument("--symbols", type=str, default=None,
                        help="Comma-separated symbols (default: all)")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="Batch size for processing")
    args = parser.parse_args()

    symbols = None
    if args.symbols:
        symbols = [s.strip().upper() for s in args.symbols.split(",")]

    async def run():
        worker = AsyncTaskWorker()
        kwargs = {"batch_size": args.batch_size}
        if symbols:
            kwargs["symbols"] = symbols
        job_id = await worker.submit(args.task, **kwargs)
        print(f"Job submitted: {job_id}")
        result = await worker.run_once()
        print(f"Result: {result['status']} — {result.get('ok', 0)} OK, {result.get('failed', 0)} failed")
        if "rows_inserted" in result:
            print(f"Rows inserted: {result['rows_inserted']}")

    asyncio.run(run())


if __name__ == "__main__":
    main()
