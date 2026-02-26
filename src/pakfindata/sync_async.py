"""Async sync orchestration for EOD data.

Uses AsyncPSXFetcher for concurrent HTTP fetches, then upserts
results to SQLite synchronously (batched after all fetches complete).
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path

from .config import DEFAULT_SYNC_CONFIG, SyncConfig
from .db import (
    connect,
    get_symbols_list,
    init_schema,
    record_failure,
    record_sync_run_end,
    record_sync_run_start,
    upsert_eod,
    upsert_symbols,
)
from .sources.async_fetcher import AsyncPSXFetcher
from .sources.eod import filter_incremental, parse_eod_payload
from .sources.market_watch import (
    fetch_market_watch_html,
    parse_symbols_from_market_watch,
)

logger = logging.getLogger("pakfindata")


@dataclass
class AsyncSyncSummary:
    """Summary of async sync operation."""

    run_id: str
    symbols_total: int
    symbols_ok: int
    symbols_failed: int
    rows_upserted: int
    elapsed: float = 0.0
    failures: list[dict] = field(default_factory=list)


async def sync_all_async(
    db_path: Path | str | None = None,
    refresh_symbols: bool = False,
    limit_symbols: int | None = None,
    symbols_list: list[str] | None = None,
    config: SyncConfig | None = None,
    progress_callback=None,
) -> AsyncSyncSummary:
    """Async version of sync_all. Uses concurrent HTTP fetches.

    Fetches all symbols concurrently via AsyncPSXFetcher, then
    upserts results to SQLite synchronously (SQLite is single-writer).

    Args:
        db_path: Path to SQLite database. Uses default if None.
        refresh_symbols: If True, refresh symbols from market-watch first.
        limit_symbols: Limit number of symbols to sync.
        symbols_list: Explicit list of symbols to sync.
        config: SyncConfig with options.
        progress_callback: Optional callback(current, total, symbol, status).

    Returns:
        AsyncSyncSummary with counts and failure details.
    """
    if config is None:
        config = DEFAULT_SYNC_CONFIG

    start_time = time.time()

    con = connect(db_path)
    init_schema(con)

    # Step 1: Optionally refresh symbols (sync HTTP — small request)
    if refresh_symbols:
        from .http import create_session
        session = create_session(config=config)
        logger.info("Refreshing symbols from market-watch")
        html = fetch_market_watch_html(session)
        symbols = parse_symbols_from_market_watch(html)
        upsert_symbols(con, symbols)
        logger.info("Refreshed %d symbols", len(symbols))

    # Step 2: Get symbols to sync
    if symbols_list is not None:
        symbols_to_sync = sorted([s.upper().strip() for s in symbols_list])
    else:
        symbols_to_sync = get_symbols_list(con, limit=limit_symbols)

    if not symbols_to_sync:
        logger.warning("No symbols to sync")
        run_id = record_sync_run_start(con, mode="async-full", symbols_total=0)
        record_sync_run_end(con, run_id, symbols_ok=0, symbols_failed=0, rows_upserted=0)
        con.close()
        return AsyncSyncSummary(
            run_id=run_id,
            symbols_total=0,
            symbols_ok=0,
            symbols_failed=0,
            rows_upserted=0,
            elapsed=time.time() - start_time,
        )

    # Step 3: Record sync run start
    mode = "async-partial" if symbols_list else "async-full"
    run_id = record_sync_run_start(con, mode=mode, symbols_total=len(symbols_to_sync))
    logger.info(
        "Starting async sync run %s: mode=%s, symbols=%d",
        run_id, mode, len(symbols_to_sync),
    )

    # Step 4: Fetch all symbols concurrently
    logger.info("Fetching %d symbols asynchronously...", len(symbols_to_sync))
    async with AsyncPSXFetcher(
        max_concurrent=config.max_retries * 8,  # ~25 concurrent
        rate_limit=config.delay_min / 6,  # faster than sync
        timeout=config.timeout,
        max_retries=config.max_retries,
    ) as fetcher:
        batch_results = await fetcher.fetch_eod_batch(
            symbols_to_sync,
            progress_cb=progress_callback,
        )

    fetch_elapsed = batch_results["elapsed"]
    logger.info(
        "Fetch complete: %d OK, %d failed in %.1fs",
        batch_results["ok"], batch_results["failed"], fetch_elapsed,
    )

    # Step 5: Upsert results to SQLite (synchronous — batch after fetch)
    symbols_ok = 0
    symbols_failed = 0
    total_rows = 0
    failures = []

    for symbol, data in batch_results["results"].items():
        try:
            df = parse_eod_payload(symbol, data)
            if not df.empty:
                rows = upsert_eod(con, df, source="per_symbol_api")
                total_rows += rows
                logger.debug("%s: upserted %d rows", symbol, rows)
            symbols_ok += 1
        except Exception as e:
            symbols_failed += 1
            error_msg = f"Parse error: {e}"
            record_failure(con, run_id, symbol, "PARSE_ERROR", error_msg)
            failures.append({"symbol": symbol, "error_type": "PARSE_ERROR", "error_message": error_msg})
            logger.warning("%s: %s", symbol, error_msg)

    # Record fetch failures
    for symbol, error in batch_results["errors"].items():
        symbols_failed += 1
        record_failure(con, run_id, symbol, "HTTP_ERROR", str(error))
        failures.append({"symbol": symbol, "error_type": "HTTP_ERROR", "error_message": str(error)})

    # Step 6: Record sync run end
    record_sync_run_end(con, run_id, symbols_ok, symbols_failed, total_rows)
    con.close()

    elapsed = time.time() - start_time
    logger.info(
        "Async sync run %s completed: ok=%d, failed=%d, rows=%d, %.1fs",
        run_id, symbols_ok, symbols_failed, total_rows, elapsed,
    )

    return AsyncSyncSummary(
        run_id=run_id,
        symbols_total=len(symbols_to_sync),
        symbols_ok=symbols_ok,
        symbols_failed=symbols_failed,
        rows_upserted=total_rows,
        elapsed=elapsed,
        failures=failures,
    )
