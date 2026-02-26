"""Sync orchestration for EOD data."""

from dataclasses import dataclass, field
from pathlib import Path

import requests

from .config import DEFAULT_SYNC_CONFIG, SyncConfig, get_logger
from .db import (
    connect,
    get_intraday_sync_state,
    get_max_date_for_symbol,
    get_symbols_list,
    init_schema,
    record_failure,
    record_sync_run_end,
    record_sync_run_start,
    update_intraday_sync_state,
    upsert_eod,
    upsert_intraday,
    upsert_symbols,
)
from .http import create_session
from .sources.eod import fetch_eod_json, filter_incremental, parse_eod_payload
from .sources.intraday import (
    fetch_intraday_json,
    parse_intraday_payload,
)
from .sources.intraday import (
    filter_incremental as filter_intraday_incremental,
)
from .sources.market_watch import (
    fetch_market_watch_html,
    parse_symbols_from_market_watch,
)
from .sources.indices import fetch_indices_data, save_index_data


@dataclass
class SyncSummary:
    """Summary of sync operation."""

    run_id: str
    symbols_total: int
    symbols_ok: int
    symbols_failed: int
    rows_upserted: int
    symbols_skipped: int = 0
    indices_synced: int = 0  # KSE-100 and other indices
    failures: list[dict] = field(default_factory=list)


def sync_all(
    db_path: Path | str | None = None,
    refresh_symbols: bool = False,
    limit_symbols: int | None = None,
    symbols_list: list[str] | None = None,
    session: requests.Session | None = None,
    config: SyncConfig | None = None,
    progress_callback=None,
) -> SyncSummary:
    """
    Sync EOD data for all symbols.

    Args:
        db_path: Path to SQLite database. Uses default if None.
        refresh_symbols: If True, refresh symbols from market-watch first.
        limit_symbols: Limit number of symbols to sync.
        symbols_list: Explicit list of symbols to sync (overrides DB lookup).
        session: Optional requests Session for HTTP calls.
        config: SyncConfig with options (incremental mode, retries, etc.).
        progress_callback: Optional callback(current, total, symbol, result) for progress updates.

    Returns:
        SyncSummary with counts and failure details.
    """
    if config is None:
        config = DEFAULT_SYNC_CONFIG

    logger = get_logger()

    if session is None:
        session = create_session(config=config)

    con = connect(db_path)
    init_schema(con)

    # Step 1: Optionally refresh symbols
    if refresh_symbols:
        logger.info("Refreshing symbols from market-watch")
        html = fetch_market_watch_html(session)
        symbols = parse_symbols_from_market_watch(html)
        upsert_symbols(con, symbols)
        logger.info("Refreshed %d symbols", len(symbols))

    # Step 1.5: Sync KSE-100 and other indices FIRST (before stock data)
    logger.info("Syncing KSE-100 and other indices...")
    indices_synced = 0
    try:
        indices_data = fetch_indices_data(timeout=config.timeout)
        for idx_data in indices_data:
            if save_index_data(con, idx_data):
                indices_synced += 1
        logger.info("Synced %d indices", indices_synced)
    except Exception as e:
        logger.warning("Failed to sync indices: %s", e)

    # Step 2: Get symbols to sync
    if symbols_list is not None:
        # Use explicit list, filter to uppercase and sorted
        symbols_to_sync = sorted([s.upper().strip() for s in symbols_list])
    else:
        symbols_to_sync = get_symbols_list(con, limit=limit_symbols)

    if not symbols_to_sync:
        # No symbols to sync
        logger.warning("No symbols to sync")
        run_id = record_sync_run_start(con, mode="full", symbols_total=0)
        record_sync_run_end(
            con, run_id, symbols_ok=0, symbols_failed=0, rows_upserted=0
        )
        con.close()
        return SyncSummary(
            run_id=run_id,
            symbols_total=0,
            symbols_ok=0,
            symbols_failed=0,
            rows_upserted=0,
            indices_synced=indices_synced,
        )

    # Step 3: Record sync run start
    if config.incremental:
        mode = "incremental"
    elif symbols_list is None:
        mode = "full"
    else:
        mode = "partial"
    run_id = record_sync_run_start(con, mode=mode, symbols_total=len(symbols_to_sync))
    logger.info(
        "Starting sync run %s: mode=%s, symbols=%d, incremental=%s",
        run_id, mode, len(symbols_to_sync), config.incremental
    )

    # Step 4: Loop through symbols
    symbols_ok = 0
    symbols_failed = 0
    total_rows = 0
    failures = []
    total_symbols = len(symbols_to_sync)

    for idx, symbol in enumerate(symbols_to_sync):
        try:
            # Fetch EOD data
            payload = fetch_eod_json(symbol, session)

            # Parse to DataFrame
            df = parse_eod_payload(symbol, payload)

            # Apply incremental filter if enabled
            if config.incremental and not df.empty:
                max_date = get_max_date_for_symbol(con, symbol)
                original_count = len(df)
                df = filter_incremental(df, max_date)
                logger.debug(
                    "%s: incremental filter %d -> %d rows (max_date=%s)",
                    symbol, original_count, len(df), max_date
                )

            # Upsert to database
            if not df.empty:
                rows = upsert_eod(con, df, source="per_symbol_api")
                total_rows += rows
                logger.debug("%s: upserted %d rows", symbol, rows)

            symbols_ok += 1

            # Progress callback for successful sync
            if progress_callback:
                progress_callback(idx + 1, total_symbols, symbol, "ok")

        except requests.RequestException as e:
            symbols_failed += 1
            error_type = "HTTP_ERROR"
            error_message = str(e)
            record_failure(con, run_id, symbol, error_type, error_message)
            failures.append({
                "symbol": symbol,
                "error_type": error_type,
                "error_message": error_message,
            })
            logger.warning("%s: %s - %s", symbol, error_type, error_message)

            # Progress callback for HTTP error
            if progress_callback:
                progress_callback(idx + 1, total_symbols, symbol, "error")

        except Exception as e:
            symbols_failed += 1
            error_type = "PARSE_ERROR"
            error_message = str(e)
            record_failure(con, run_id, symbol, error_type, error_message)
            failures.append({
                "symbol": symbol,
                "error_type": error_type,
                "error_message": error_message,
            })
            logger.warning("%s: %s - %s", symbol, error_type, error_message)

            # Progress callback for parse error
            if progress_callback:
                progress_callback(idx + 1, total_symbols, symbol, "error")

    # Step 5: Record sync run end
    record_sync_run_end(con, run_id, symbols_ok, symbols_failed, total_rows)
    con.close()

    logger.info(
        "Sync run %s completed: ok=%d, failed=%d, rows=%d",
        run_id, symbols_ok, symbols_failed, total_rows
    )

    return SyncSummary(
        run_id=run_id,
        symbols_total=len(symbols_to_sync),
        symbols_ok=symbols_ok,
        symbols_failed=symbols_failed,
        rows_upserted=total_rows,
        indices_synced=indices_synced,
        failures=failures,
    )


# =============================================================================
# Intraday Sync
# =============================================================================


@dataclass
class IntradaySyncSummary:
    """Summary of intraday sync operation for a single symbol."""

    symbol: str
    rows_upserted: int
    newest_ts: str | None
    error: str | None = None


def sync_intraday(
    db_path: Path | str | None = None,
    symbol: str = "",
    incremental: bool = True,
    max_rows: int | None = None,
    session: requests.Session | None = None,
    config: SyncConfig | None = None,
) -> IntradaySyncSummary:
    """
    Sync intraday data for a single symbol.

    Args:
        db_path: Path to SQLite database. Uses default if None.
        symbol: Stock symbol to sync (e.g., "OGDC").
        incremental: If True, only fetch data newer than last sync.
        max_rows: Optional limit on number of rows to keep (most recent).
        session: Optional requests Session for HTTP calls.
        config: SyncConfig with options (retries, timeouts, etc.).

    Returns:
        IntradaySyncSummary with counts and newest timestamp.
    """
    if config is None:
        config = DEFAULT_SYNC_CONFIG

    logger = get_logger()
    symbol = symbol.upper().strip()

    if not symbol:
        return IntradaySyncSummary(
            symbol="",
            rows_upserted=0,
            newest_ts=None,
            error="No symbol provided",
        )

    if session is None:
        session = create_session(config=config)

    con = connect(db_path)
    init_schema(con)

    try:
        # Get last synced timestamp if incremental
        last_ts = None
        last_ts_epoch = None
        if incremental:
            last_ts, last_ts_epoch = get_intraday_sync_state(con, symbol)
            logger.debug(
                "%s: last_ts=%s, last_ts_epoch=%s", symbol, last_ts, last_ts_epoch
            )

        # Fetch intraday data
        logger.info("Fetching intraday data for %s", symbol)
        payload = fetch_intraday_json(symbol, session)

        # Parse to DataFrame (includes ts_epoch)
        df = parse_intraday_payload(symbol, payload)

        if df.empty:
            logger.info("%s: no intraday data returned", symbol)
            con.close()
            return IntradaySyncSummary(
                symbol=symbol,
                rows_upserted=0,
                newest_ts=last_ts,
                error=None,
            )

        # Apply incremental filter if enabled (using epoch for comparison)
        if incremental and last_ts_epoch:
            original_count = len(df)
            df = filter_intraday_incremental(df, last_ts_epoch)
            logger.debug(
                "%s: incremental filter %d -> %d rows (last_ts_epoch=%s)",
                symbol, original_count, len(df), last_ts_epoch
            )

        # Apply max_rows limit (keep most recent by ts_epoch)
        if max_rows is not None and len(df) > max_rows:
            df = df.sort_values("ts_epoch").tail(max_rows).reset_index(drop=True)
            logger.debug("%s: limited to %d rows", symbol, max_rows)

        if df.empty:
            logger.info("%s: no new intraday data after filtering", symbol)
            con.close()
            return IntradaySyncSummary(
                symbol=symbol,
                rows_upserted=0,
                newest_ts=last_ts,
                error=None,
            )

        # Get newest timestamp before upsert (use max ts_epoch to find the row)
        newest_idx = df["ts_epoch"].idxmax()
        newest_ts = df.loc[newest_idx, "ts"]
        newest_ts_epoch = int(df.loc[newest_idx, "ts_epoch"])

        # Upsert to database
        rows = upsert_intraday(con, df)
        logger.info("%s: upserted %d intraday rows", symbol, rows)

        # Update sync state with epoch
        update_intraday_sync_state(con, symbol, newest_ts, newest_ts_epoch)

        con.close()

        return IntradaySyncSummary(
            symbol=symbol,
            rows_upserted=rows,
            newest_ts=newest_ts,
            error=None,
        )

    except requests.RequestException as e:
        error_msg = f"HTTP error: {e}"
        logger.warning("%s: %s", symbol, error_msg)
        con.close()
        return IntradaySyncSummary(
            symbol=symbol,
            rows_upserted=0,
            newest_ts=None,
            error=error_msg,
        )

    except Exception as e:
        error_msg = f"Error: {e}"
        logger.warning("%s: %s", symbol, error_msg)
        con.close()
        return IntradaySyncSummary(
            symbol=symbol,
            rows_upserted=0,
            newest_ts=None,
            error=error_msg,
        )


@dataclass
class BulkIntradaySyncSummary:
    """Summary of bulk intraday sync operation for all symbols."""

    symbols_total: int
    symbols_ok: int
    symbols_failed: int
    rows_upserted: int
    results: list[IntradaySyncSummary] = field(default_factory=list)


def sync_intraday_bulk(
    db_path: Path | str | None = None,
    symbols_list: list[str] | None = None,
    incremental: bool = True,
    max_rows: int | None = None,
    limit_symbols: int | None = None,
    session: requests.Session | None = None,
    config: SyncConfig | None = None,
    progress_callback=None,
) -> BulkIntradaySyncSummary:
    """
    Sync intraday data for all symbols or a specific list.

    Args:
        db_path: Path to SQLite database. Uses default if None.
        symbols_list: Explicit list of symbols to sync. If None, syncs all from DB.
        incremental: If True, only fetch data newer than last sync.
        max_rows: Optional limit on number of rows to keep per symbol (most recent).
        limit_symbols: Limit number of symbols to sync (if symbols_list is None).
        session: Optional requests Session for HTTP calls.
        config: SyncConfig with options (retries, timeouts, etc.).
        progress_callback: Optional callback(current, total, symbol, result) for progress updates.

    Returns:
        BulkIntradaySyncSummary with counts and per-symbol results.
    """
    import time

    if config is None:
        config = DEFAULT_SYNC_CONFIG

    logger = get_logger()

    if session is None:
        session = create_session(config=config)

    con = connect(db_path)
    init_schema(con)

    # Get symbols to sync
    if symbols_list is not None:
        symbols_to_sync = sorted([s.upper().strip() for s in symbols_list])
    else:
        symbols_to_sync = get_symbols_list(con, limit=limit_symbols)

    con.close()

    if not symbols_to_sync:
        logger.warning("No symbols to sync intraday data")
        return BulkIntradaySyncSummary(
            symbols_total=0,
            symbols_ok=0,
            symbols_failed=0,
            rows_upserted=0,
            results=[],
        )

    summary = BulkIntradaySyncSummary(
        symbols_total=len(symbols_to_sync),
        symbols_ok=0,
        symbols_failed=0,
        rows_upserted=0,
        results=[],
    )

    logger.info("Starting bulk intraday sync for %d symbols", len(symbols_to_sync))

    for i, symbol in enumerate(symbols_to_sync):
        # Sync this symbol
        result = sync_intraday(
            db_path=db_path,
            symbol=symbol,
            incremental=incremental,
            max_rows=max_rows,
            session=session,
            config=config,
        )

        summary.results.append(result)
        summary.rows_upserted += result.rows_upserted

        if result.error:
            summary.symbols_failed += 1
        else:
            summary.symbols_ok += 1

        # Progress callback
        if progress_callback:
            progress_callback(i + 1, len(symbols_to_sync), symbol, result)

        # Small delay between requests to avoid rate limiting
        if i < len(symbols_to_sync) - 1:
            time.sleep(0.5)

    logger.info(
        "Bulk intraday sync complete: %d ok, %d failed, %d rows",
        summary.symbols_ok,
        summary.symbols_failed,
        summary.rows_upserted,
    )

    return summary
