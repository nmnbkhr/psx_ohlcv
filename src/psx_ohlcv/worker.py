"""Background worker for bulk deep scraping jobs.

This script runs as a separate process to scrape company data without
blocking the Streamlit UI. It reads job configuration from the database,
processes symbols in batches, and updates progress in real-time.

Usage:
    python -m psx_ohlcv.worker <job_id>
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def run_job(job_id: str) -> None:
    """Run a background scrape job.

    Args:
        job_id: The job ID to process
    """
    from psx_ohlcv.db import (
        connect,
        get_scrape_job,
        update_job_progress,
        is_job_stop_requested,
        add_job_notification,
    )
    from psx_ohlcv.sources.deep_scraper import deep_scrape_symbol

    logger.info(f"Starting worker for job {job_id}")

    # Connect to database
    con = connect()

    # Get job details
    job = get_scrape_job(con, job_id)
    if not job:
        logger.error(f"Job {job_id} not found")
        return

    if job["status"] not in ("pending", "running"):
        logger.error(f"Job {job_id} is not pending/running (status: {job['status']})")
        return

    # Get configuration
    config = job.get("config", {})
    symbols = config.get("symbols", [])
    batch_size = job.get("batch_size", 50)
    batch_pause_sec = job.get("batch_pause_sec", 30)
    request_delay = config.get("request_delay", 1.5)
    save_raw_html = config.get("save_raw_html", False)

    if not symbols:
        logger.error(f"No symbols in job {job_id}")
        update_job_progress(con, job_id, status="failed")
        return

    total_symbols = len(symbols)

    # Calculate total_batches if not set or 0
    import math
    total_batches = job.get("total_batches") or 0
    if total_batches == 0:
        total_batches = math.ceil(total_symbols / batch_size)

    logger.info(f"Job {job_id}: {total_symbols} symbols, {total_batches} batches, batch_size={batch_size}")

    # Update job as running with PID and correct counts
    con.execute(
        """
        UPDATE scrape_jobs SET
            status = 'running',
            pid = ?,
            symbols_requested = ?,
            total_batches = ?,
            batch_size = ?,
            last_heartbeat = datetime('now')
        WHERE job_id = ?
        """,
        (os.getpid(), total_symbols, total_batches, batch_size, job_id),
    )
    con.commit()

    # Process in batches
    completed = 0
    failed = 0
    records_inserted = 0
    current_batch = 0

    try:
        for batch_num in range(total_batches):
            current_batch = batch_num + 1
            batch_start = batch_num * batch_size
            batch_end = min(batch_start + batch_size, total_symbols)
            batch_symbols = symbols[batch_start:batch_end]

            logger.info(f"Batch {current_batch}/{total_batches}: symbols {batch_start+1}-{batch_end}")

            # Update batch progress
            update_job_progress(
                con, job_id,
                current_batch=current_batch,
            )

            # Process symbols in batch
            for i, symbol in enumerate(batch_symbols):
                # Check for stop request
                if is_job_stop_requested(con, job_id):
                    logger.info(f"Stop requested for job {job_id}")
                    update_job_progress(
                        con, job_id,
                        status="stopped",
                        symbols_completed=completed,
                        symbols_failed=failed,
                        records_inserted=records_inserted,
                    )
                    add_job_notification(
                        con, job_id, "stopped",
                        f"Job Stopped: {completed}/{total_symbols} completed",
                        f"Stopped at batch {current_batch}/{total_batches}. "
                        f"Completed: {completed}, Failed: {failed}",
                    )
                    return

                # Update current symbol
                update_job_progress(
                    con, job_id,
                    current_symbol=symbol,
                    symbols_completed=completed,
                    symbols_failed=failed,
                    records_inserted=records_inserted,
                )

                # Scrape symbol
                try:
                    result = deep_scrape_symbol(con, symbol, save_raw_html=save_raw_html)

                    if result.get("success"):
                        completed += 1
                        records_inserted += (
                            (1 if result.get("snapshot_saved") else 0) +
                            result.get("trading_sessions_saved", 0) +
                            result.get("announcements_saved", 0) +
                            (1 if result.get("equity_saved") else 0)
                        )
                        logger.debug(f"  {symbol}: OK")
                    else:
                        failed += 1
                        logger.warning(f"  {symbol}: FAILED - {result.get('error', 'Unknown error')}")

                except Exception as e:
                    failed += 1
                    logger.error(f"  {symbol}: ERROR - {e}")

                # Delay between requests
                if i < len(batch_symbols) - 1:
                    time.sleep(request_delay)

            # Update progress after batch
            update_job_progress(
                con, job_id,
                symbols_completed=completed,
                symbols_failed=failed,
                records_inserted=records_inserted,
            )

            # Pause between batches (except after last batch)
            if current_batch < total_batches:
                logger.info(f"Batch {current_batch} complete. Pausing {batch_pause_sec}s...")
                time.sleep(batch_pause_sec)

        # Job completed successfully
        logger.info(f"Job {job_id} completed: {completed} OK, {failed} failed")

        update_job_progress(
            con, job_id,
            status="completed",
            symbols_completed=completed,
            symbols_failed=failed,
            records_inserted=records_inserted,
            current_symbol=None,
        )

        add_job_notification(
            con, job_id, "completed",
            f"Bulk Scrape Complete: {completed}/{total_symbols}",
            f"Successfully scraped {completed} symbols. "
            f"Failed: {failed}. Records inserted: {records_inserted}",
        )

    except Exception as e:
        logger.exception(f"Job {job_id} failed with error: {e}")

        update_job_progress(
            con, job_id,
            status="failed",
            symbols_completed=completed,
            symbols_failed=failed,
            records_inserted=records_inserted,
        )

        add_job_notification(
            con, job_id, "failed",
            f"Job Failed: {e}",
            f"Completed {completed}/{total_symbols} before failure.",
        )


def main():
    """Main entry point for worker."""
    parser = argparse.ArgumentParser(description="Background scrape worker")
    parser.add_argument("job_id", help="Job ID to process")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    run_job(args.job_id)


if __name__ == "__main__":
    main()
