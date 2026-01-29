"""Background service for EOD data synchronization.

This service runs EOD sync operations in the background without blocking the UI.
It uses:
- PID file to track running state
- Status JSON file for UI communication
- Log file for detailed logging
"""

import json
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from ..config import DATA_ROOT, get_db_path, get_logger, setup_logging
from ..sync import sync_all, SyncConfig, DEFAULT_SYNC_CONFIG


# Service paths
SERVICE_DIR = DATA_ROOT / "services"
PID_FILE = SERVICE_DIR / "eod_sync.pid"
STATUS_FILE = SERVICE_DIR / "eod_sync_status.json"
LOG_FILE = SERVICE_DIR / "eod_sync.log"


@dataclass
class EODSyncStatus:
    """Status of the EOD sync service."""

    running: bool = False
    pid: int | None = None
    started_at: str | None = None
    completed_at: str | None = None
    result: str | None = None  # "success", "partial", "error"
    symbols_ok: int = 0
    symbols_failed: int = 0
    symbols_skipped: int = 0
    rows_upserted: int = 0
    current_symbol: str | None = None
    progress: float = 0.0  # 0-100
    progress_message: str | None = None
    mode: str = "incremental"
    refresh_symbols: bool = False
    error_message: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "EODSyncStatus":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


def ensure_service_dir():
    """Ensure service directory exists."""
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)


def read_eod_status() -> EODSyncStatus:
    """Read current EOD sync status from file."""
    ensure_service_dir()
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                data = json.load(f)
            status = EODSyncStatus.from_dict(data)
            # Verify PID is still running
            if status.running and status.pid:
                if not is_process_running(status.pid):
                    status.running = False
                    status.pid = None
                    write_eod_status(status)
            return status
        except (json.JSONDecodeError, KeyError):
            pass
    return EODSyncStatus()


def write_eod_status(status: EODSyncStatus):
    """Write EOD sync status to file."""
    ensure_service_dir()
    with open(STATUS_FILE, "w") as f:
        json.dump(status.to_dict(), f, indent=2)


def is_process_running(pid: int) -> bool:
    """Check if a process with given PID is running."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def read_pid() -> int | None:
    """Read PID from file."""
    if PID_FILE.exists():
        try:
            return int(PID_FILE.read_text().strip())
        except (ValueError, IOError):
            pass
    return None


def write_pid(pid: int):
    """Write PID to file."""
    ensure_service_dir()
    PID_FILE.write_text(str(pid))


def remove_pid():
    """Remove PID file."""
    if PID_FILE.exists():
        PID_FILE.unlink()


def is_eod_sync_running() -> tuple[bool, int | None]:
    """Check if EOD sync is running.

    Returns:
        Tuple of (is_running, pid)
    """
    pid = read_pid()
    if pid and is_process_running(pid):
        return True, pid
    return False, None


def stop_eod_sync() -> tuple[bool, str]:
    """Stop the running EOD sync.

    Returns:
        Tuple of (success, message)
    """
    running, pid = is_eod_sync_running()
    if not running:
        # Clean up stale files
        remove_pid()
        status = read_eod_status()
        status.running = False
        status.pid = None
        write_eod_status(status)
        return True, "EOD sync was not running"

    try:
        os.kill(pid, signal.SIGTERM)
        # Wait for process to terminate
        for _ in range(10):  # Wait up to 5 seconds
            time.sleep(0.5)
            if not is_process_running(pid):
                break

        # Force kill if still running
        if is_process_running(pid):
            os.kill(pid, signal.SIGKILL)
            time.sleep(0.5)

        remove_pid()
        status = read_eod_status()
        status.running = False
        status.pid = None
        status.result = "cancelled"
        status.completed_at = datetime.now().isoformat()
        write_eod_status(status)
        return True, f"EOD sync stopped (PID: {pid})"

    except OSError as e:
        return False, f"Failed to stop EOD sync: {e}"


def run_eod_sync(
    incremental: bool = True,
    refresh_symbols: bool = False,
):
    """Run the EOD sync operation.

    Args:
        incremental: If True, only fetch data newer than existing records
        refresh_symbols: If True, refresh symbol list before syncing
    """
    # Check if already running
    running, existing_pid = is_eod_sync_running()
    if running:
        print(f"EOD sync already running (PID: {existing_pid})")
        return

    # Setup
    ensure_service_dir()
    setup_logging(log_file=LOG_FILE, console=True)
    logger = get_logger()

    pid = os.getpid()
    write_pid(pid)

    # Initialize status
    status = EODSyncStatus(
        running=True,
        pid=pid,
        started_at=datetime.now().isoformat(),
        mode="incremental" if incremental else "full",
        refresh_symbols=refresh_symbols,
        progress_message="Initializing...",
    )
    write_eod_status(status)

    # Signal handler for graceful shutdown
    shutdown_requested = False

    def handle_signal(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown signal received")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "EOD sync started (PID: %d, mode: %s, refresh_symbols: %s)",
        pid,
        "incremental" if incremental else "full",
        refresh_symbols,
    )

    try:
        # Update status
        if refresh_symbols:
            status.progress_message = "Refreshing symbol list from PSX..."
            status.progress = 5.0
            write_eod_status(status)

        status.progress_message = "Fetching EOD data for all symbols..."
        status.progress = 10.0
        write_eod_status(status)

        # Create sync config
        config = SyncConfig(
            incremental=incremental,
            max_retries=DEFAULT_SYNC_CONFIG.max_retries,
            delay_min=DEFAULT_SYNC_CONFIG.delay_min,
            delay_max=DEFAULT_SYNC_CONFIG.delay_max,
            timeout=DEFAULT_SYNC_CONFIG.timeout,
        )

        # Progress callback to update status
        def progress_callback(current: int, total: int, symbol: str, result: str):
            nonlocal status
            if shutdown_requested:
                raise KeyboardInterrupt("Shutdown requested")
            status.current_symbol = symbol
            # Progress from 10% to 95%
            status.progress = 10.0 + (current / max(total, 1)) * 85.0
            status.progress_message = f"Syncing {symbol} ({current}/{total})..."
            write_eod_status(status)

        # Run sync
        summary = sync_all(
            db_path=get_db_path(),
            refresh_symbols=refresh_symbols,
            config=config,
            progress_callback=progress_callback,
        )

        # Update final status
        status.completed_at = datetime.now().isoformat()
        status.symbols_ok = summary.symbols_ok
        status.symbols_failed = summary.symbols_failed
        status.symbols_skipped = summary.symbols_skipped
        status.rows_upserted = summary.rows_upserted
        status.progress = 100.0
        status.current_symbol = None

        if summary.symbols_failed == 0:
            status.result = "success"
            status.progress_message = f"Completed: {summary.symbols_ok} symbols, {summary.rows_upserted:,} rows"
            status.error_message = None
        else:
            status.result = "partial"
            status.progress_message = f"Completed with {summary.symbols_failed} failures"
            status.error_message = f"{summary.symbols_failed} symbols failed to sync"

        logger.info(
            "EOD sync complete: %d OK, %d failed, %d skipped, %d rows",
            summary.symbols_ok,
            summary.symbols_failed,
            summary.symbols_skipped,
            summary.rows_upserted,
        )

    except KeyboardInterrupt:
        logger.info("EOD sync cancelled by user")
        status.result = "cancelled"
        status.progress_message = "Cancelled by user"
        status.completed_at = datetime.now().isoformat()

    except Exception as e:
        logger.error("EOD sync failed: %s", e)
        status.result = "error"
        status.progress_message = "Sync failed"
        status.error_message = str(e)
        status.completed_at = datetime.now().isoformat()

    finally:
        # Cleanup
        logger.info("EOD sync finished")
        status.running = False
        status.pid = None
        write_eod_status(status)
        remove_pid()


def start_eod_sync_background(
    incremental: bool = True,
    refresh_symbols: bool = False,
) -> tuple[bool, str]:
    """Start the EOD sync as a background process.

    Returns:
        Tuple of (success, message)
    """
    running, existing_pid = is_eod_sync_running()
    if running:
        return False, f"EOD sync already running (PID: {existing_pid})"

    # Fork to run in background
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - wait briefly for child to start
            time.sleep(1)
            running, child_pid = is_eod_sync_running()
            if running:
                return True, f"EOD sync started (PID: {child_pid})"
            else:
                return False, "EOD sync failed to start"
    except OSError as e:
        return False, f"Fork failed: {e}"

    # Child process - become daemon
    try:
        os.setsid()  # Create new session

        # Second fork to prevent zombie
        pid = os.fork()
        if pid > 0:
            os._exit(0)

        # Redirect standard file descriptors
        sys.stdin.close()
        sys.stdout = open(LOG_FILE, "a")
        sys.stderr = sys.stdout

        # Run the sync
        run_eod_sync(
            incremental=incremental,
            refresh_symbols=refresh_symbols,
        )

    except Exception as e:
        print(f"Daemon error: {e}", file=sys.stderr)
    finally:
        os._exit(0)


# CLI entry point
def main():
    """CLI entry point for EOD sync."""
    import argparse

    parser = argparse.ArgumentParser(description="EOD sync background service")
    parser.add_argument(
        "action",
        choices=["start", "stop", "status", "run"],
        help="Action to perform",
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Sync mode (default: incremental)",
    )
    parser.add_argument(
        "--refresh-symbols",
        action="store_true",
        help="Refresh symbol list before syncing",
    )

    args = parser.parse_args()

    if args.action == "status":
        status = read_eod_status()
        print(json.dumps(status.to_dict(), indent=2))

    elif args.action == "stop":
        success, msg = stop_eod_sync()
        print(msg)
        sys.exit(0 if success else 1)

    elif args.action == "start":
        success, msg = start_eod_sync_background(
            incremental=(args.mode == "incremental"),
            refresh_symbols=args.refresh_symbols,
        )
        print(msg)
        sys.exit(0 if success else 1)

    elif args.action == "run":
        # Run in foreground (for debugging)
        run_eod_sync(
            incremental=(args.mode == "incremental"),
            refresh_symbols=args.refresh_symbols,
        )


if __name__ == "__main__":
    main()
