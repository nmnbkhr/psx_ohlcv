"""Background service for intraday data synchronization.

This service runs independently of Streamlit and can be:
- Started/stopped via CLI or UI
- Scheduled to run at intervals
- Monitored via status file

The service uses:
- PID file to track running state
- Status JSON file for UI communication
- Log file for detailed logging
"""

import json
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable

from ..config import DATA_ROOT, get_db_path, get_logger, setup_logging
from ..db import connect, init_schema
from ..sync import sync_intraday_bulk
from ..sources.indices import fetch_indices_data, save_index_data


# Service paths
SERVICE_DIR = DATA_ROOT / "services"
PID_FILE = SERVICE_DIR / "intraday_sync.pid"
STATUS_FILE = SERVICE_DIR / "intraday_sync_status.json"
LOG_FILE = SERVICE_DIR / "intraday_sync.log"


@dataclass
class ServiceStatus:
    """Status of the intraday sync service."""

    running: bool = False
    pid: int | None = None
    started_at: str | None = None
    last_run_at: str | None = None
    last_run_result: str | None = None  # "success", "partial", "error"
    symbols_synced: int = 0
    symbols_failed: int = 0
    rows_upserted: int = 0
    current_symbol: str | None = None
    progress: float = 0.0  # 0-100
    mode: str = "incremental"  # "incremental" or "full"
    interval_seconds: int = 300  # 5 minutes default
    next_run_at: str | None = None
    error_message: str | None = None
    total_runs: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ServiceStatus":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


def ensure_service_dir():
    """Ensure service directory exists."""
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)


def read_status() -> ServiceStatus:
    """Read current service status from file."""
    ensure_service_dir()
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                data = json.load(f)
            status = ServiceStatus.from_dict(data)
            # Verify PID is still running
            if status.running and status.pid:
                if not is_process_running(status.pid):
                    status.running = False
                    status.pid = None
                    write_status(status)
            return status
        except (json.JSONDecodeError, KeyError):
            pass
    return ServiceStatus()


def write_status(status: ServiceStatus):
    """Write service status to file."""
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


def is_service_running() -> tuple[bool, int | None]:
    """Check if service is running.

    Returns:
        Tuple of (is_running, pid)
    """
    pid = read_pid()
    if pid and is_process_running(pid):
        return True, pid
    return False, None


def stop_service() -> tuple[bool, str]:
    """Stop the running service.

    Returns:
        Tuple of (success, message)
    """
    running, pid = is_service_running()
    if not running:
        # Clean up stale files
        remove_pid()
        status = read_status()
        status.running = False
        status.pid = None
        write_status(status)
        return True, "Service was not running"

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
        status = read_status()
        status.running = False
        status.pid = None
        write_status(status)
        return True, f"Service stopped (PID: {pid})"

    except OSError as e:
        return False, f"Failed to stop service: {e}"


def run_service(
    mode: str = "incremental",
    interval_seconds: int = 300,
    limit_symbols: int | None = None,
    run_once: bool = False,
):
    """Run the intraday sync service.

    Args:
        mode: "incremental" or "full"
        interval_seconds: Seconds between sync runs
        limit_symbols: Limit number of symbols to sync
        run_once: If True, run once and exit (for testing)
    """
    # Check if already running
    running, existing_pid = is_service_running()
    if running:
        print(f"Service already running (PID: {existing_pid})")
        return

    # Setup
    ensure_service_dir()
    setup_logging(log_file=LOG_FILE, console=True)
    logger = get_logger()

    pid = os.getpid()
    write_pid(pid)

    # Initialize status
    status = ServiceStatus(
        running=True,
        pid=pid,
        started_at=datetime.now().isoformat(),
        mode=mode,
        interval_seconds=interval_seconds,
    )
    write_status(status)

    # Signal handler for graceful shutdown
    shutdown_requested = False

    def handle_signal(signum, frame):
        nonlocal shutdown_requested
        logger.info("Shutdown signal received")
        shutdown_requested = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "Intraday sync service started (PID: %d, mode: %s, interval: %ds)",
        pid, mode, interval_seconds
    )

    try:
        while not shutdown_requested:
            # Update status - starting run
            status.current_symbol = None
            status.progress = 0.0
            write_status(status)

            # Progress callback
            def progress_callback(current, total, symbol, result):
                status.current_symbol = symbol
                status.progress = (current / total) * 100
                write_status(status)

            # Run sync
            logger.info("Starting sync run (%s mode)", mode)
            try:
                # Step 1: Sync KSE-100 and other indices FIRST
                logger.info("Syncing KSE-100 index...")
                status.current_symbol = "KSE-100 INDEX"
                write_status(status)
                try:
                    con = connect(get_db_path())
                    init_schema(con)
                    indices_data = fetch_indices_data(timeout=30)
                    for idx_data in indices_data:
                        save_index_data(con, idx_data)
                    con.close()
                    logger.info("KSE-100 index synced")
                except Exception as idx_err:
                    logger.warning("Failed to sync indices: %s", idx_err)

                # Step 2: Sync intraday data for all symbols
                summary = sync_intraday_bulk(
                    db_path=get_db_path(),
                    incremental=(mode == "incremental"),
                    limit_symbols=limit_symbols,
                    progress_callback=progress_callback,
                )

                # Update status with results
                status.last_run_at = datetime.now().isoformat()
                status.symbols_synced = summary.symbols_ok
                status.symbols_failed = summary.symbols_failed
                status.rows_upserted = summary.rows_upserted
                status.total_runs += 1

                if summary.symbols_failed == 0:
                    status.last_run_result = "success"
                    status.error_message = None
                else:
                    status.last_run_result = "partial"
                    status.error_message = f"{summary.symbols_failed} symbols failed"

                logger.info(
                    "Sync complete: %d OK, %d failed, %d rows",
                    summary.symbols_ok, summary.symbols_failed, summary.rows_upserted
                )

            except Exception as e:
                logger.error("Sync run failed: %s", e)
                status.last_run_at = datetime.now().isoformat()
                status.last_run_result = "error"
                status.error_message = str(e)

            status.current_symbol = None
            status.progress = 100.0

            if run_once:
                break

            # Calculate next run time
            next_run = datetime.now().timestamp() + interval_seconds
            status.next_run_at = datetime.fromtimestamp(next_run).isoformat()
            write_status(status)

            # Sleep with periodic checks for shutdown
            logger.info("Next run at %s", status.next_run_at)
            sleep_until = time.time() + interval_seconds
            while time.time() < sleep_until and not shutdown_requested:
                time.sleep(1)

    except Exception as e:
        logger.error("Service error: %s", e)
        status.error_message = str(e)

    finally:
        # Cleanup
        logger.info("Service shutting down")
        status.running = False
        status.pid = None
        status.current_symbol = None
        write_status(status)
        remove_pid()


def start_service_background(
    mode: str = "incremental",
    interval_seconds: int = 300,
    limit_symbols: int | None = None,
) -> tuple[bool, str]:
    """Start the service as a background process.

    Returns:
        Tuple of (success, message)
    """
    running, existing_pid = is_service_running()
    if running:
        return False, f"Service already running (PID: {existing_pid})"

    # Fork to run in background
    try:
        pid = os.fork()
        if pid > 0:
            # Parent process - wait briefly for child to start
            time.sleep(1)
            running, child_pid = is_service_running()
            if running:
                return True, f"Service started (PID: {child_pid})"
            else:
                return False, "Service failed to start"
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

        # Run the service
        run_service(
            mode=mode,
            interval_seconds=interval_seconds,
            limit_symbols=limit_symbols,
        )

    except Exception as e:
        print(f"Daemon error: {e}", file=sys.stderr)
    finally:
        os._exit(0)


# CLI entry point
def main():
    """CLI entry point for the service."""
    import argparse

    parser = argparse.ArgumentParser(description="Intraday sync background service")
    parser.add_argument(
        "action",
        choices=["start", "stop", "status", "run"],
        help="Action to perform"
    )
    parser.add_argument(
        "--mode",
        choices=["incremental", "full"],
        default="incremental",
        help="Sync mode (default: incremental)"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=300,
        help="Interval between syncs in seconds (default: 300)"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit number of symbols to sync"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (for testing)"
    )

    args = parser.parse_args()

    if args.action == "start":
        success, msg = start_service_background(
            mode=args.mode,
            interval_seconds=args.interval,
            limit_symbols=args.limit,
        )
        print(msg)
        sys.exit(0 if success else 1)

    elif args.action == "stop":
        success, msg = stop_service()
        print(msg)
        sys.exit(0 if success else 1)

    elif args.action == "status":
        status = read_status()
        print(json.dumps(status.to_dict(), indent=2))
        sys.exit(0)

    elif args.action == "run":
        # Run in foreground
        run_service(
            mode=args.mode,
            interval_seconds=args.interval,
            limit_symbols=args.limit,
            run_once=args.once,
        )


if __name__ == "__main__":
    main()
