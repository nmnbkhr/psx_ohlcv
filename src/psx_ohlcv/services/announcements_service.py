"""Background service for announcements data synchronization.

This service runs independently of Streamlit and can be:
- Started/stopped via CLI or UI
- Scheduled to run at intervals
- Resumed from where it left off

The service syncs:
- Company announcements (all symbols)
- Corporate events (AGM/EOGM calendar)
- Dividend payouts (per symbol)
"""

import json
import os
import signal
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path

from ..config import DATA_ROOT, get_db_path, get_logger, setup_logging
from ..db import connect, init_schema
from ..sources.announcements import (
    AnnouncementRecord,
    CorporateEvent,
    DividendPayout,
    fetch_announcements,
    fetch_company_payouts,
    fetch_corporate_events,
    save_announcement,
    save_corporate_event,
    save_dividend_payout,
)


# Service paths
SERVICE_DIR = DATA_ROOT / "services"
PID_FILE = SERVICE_DIR / "announcements_sync.pid"
STATUS_FILE = SERVICE_DIR / "announcements_sync_status.json"
LOG_FILE = SERVICE_DIR / "announcements_sync.log"


@dataclass
class AnnouncementsServiceStatus:
    """Status of the announcements sync service."""

    running: bool = False
    pid: int | None = None
    started_at: str | None = None
    last_run_at: str | None = None
    last_run_result: str | None = None  # "success", "partial", "error"

    # Current task
    current_task: str | None = None  # 'announcements', 'events', 'dividends'
    current_symbol: str | None = None
    progress: float = 0.0  # 0-100

    # Stats
    announcements_synced: int = 0
    events_synced: int = 0
    dividends_synced: int = 0
    symbols_processed: int = 0
    symbols_failed: int = 0

    # Configuration
    interval_seconds: int = 3600  # 1 hour default
    next_run_at: str | None = None
    error_message: str | None = None
    total_runs: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "AnnouncementsServiceStatus":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


def ensure_service_dir():
    """Ensure service directory exists."""
    SERVICE_DIR.mkdir(parents=True, exist_ok=True)


def read_status() -> AnnouncementsServiceStatus:
    """Read current service status from file."""
    ensure_service_dir()
    if STATUS_FILE.exists():
        try:
            with open(STATUS_FILE) as f:
                data = json.load(f)
            status = AnnouncementsServiceStatus.from_dict(data)
            # Verify PID is still running
            if status.running and status.pid:
                if not is_process_running(status.pid):
                    status.running = False
                    status.pid = None
                    write_status(status)
            return status
        except (json.JSONDecodeError, KeyError):
            pass
    return AnnouncementsServiceStatus()


def write_status(status: AnnouncementsServiceStatus):
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


def get_all_symbols(con) -> list[str]:
    """Get all active symbols from database."""
    try:
        cur = con.execute("""
            SELECT symbol FROM symbols
            WHERE is_active = 1
            ORDER BY symbol
        """)
        return [row[0] for row in cur.fetchall()]
    except Exception:
        return []


def sync_announcements(con, status: AnnouncementsServiceStatus, logger) -> int:
    """Sync company announcements.

    Returns:
        Number of announcements synced
    """
    total_synced = 0
    offset = 0
    page_size = 20

    logger.info("Syncing company announcements...")
    status.current_task = "announcements"
    write_status(status)

    while True:
        records, total = fetch_announcements(
            announcement_type="C",
            offset=offset,
        )

        if not records:
            break

        for record in records:
            if save_announcement(con, record):
                total_synced += 1

        offset += len(records)
        status.progress = min(99, (offset / max(total, 1)) * 100)
        status.announcements_synced = total_synced
        write_status(status)

        logger.info(f"Announcements: {offset}/{total} processed, {total_synced} saved")

        # Break if we've fetched all
        if offset >= total or len(records) < page_size:
            break

        # Small delay to be nice to the server
        time.sleep(0.5)

    return total_synced


def sync_corporate_events(con, status: AnnouncementsServiceStatus, logger) -> int:
    """Sync corporate events calendar.

    Returns:
        Number of events synced
    """
    logger.info("Syncing corporate events calendar...")
    status.current_task = "events"
    write_status(status)

    # Fetch events for the next 12 months
    from_date = datetime.now().strftime("%Y-%m-%d")
    to_date = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d")

    events = fetch_corporate_events(from_date, to_date)

    total_synced = 0
    for i, event in enumerate(events):
        if save_corporate_event(con, event):
            total_synced += 1
        status.progress = ((i + 1) / max(len(events), 1)) * 100
        status.events_synced = total_synced
        write_status(status)

    logger.info(f"Corporate events: {total_synced} saved")
    return total_synced


def sync_dividend_payouts(con, status: AnnouncementsServiceStatus, logger) -> int:
    """Sync dividend payouts for all symbols.

    Returns:
        Number of payouts synced
    """
    logger.info("Syncing dividend payouts...")
    status.current_task = "dividends"
    write_status(status)

    symbols = get_all_symbols(con)
    total_synced = 0

    for i, symbol in enumerate(symbols):
        status.current_symbol = symbol
        status.progress = ((i + 1) / max(len(symbols), 1)) * 100
        write_status(status)

        try:
            payouts = fetch_company_payouts(symbol)
            for payout in payouts:
                if save_dividend_payout(con, payout):
                    total_synced += 1
            status.symbols_processed += 1
        except Exception as e:
            logger.warning(f"Failed to sync dividends for {symbol}: {e}")
            status.symbols_failed += 1

        status.dividends_synced = total_synced
        write_status(status)

        # Small delay between symbols
        time.sleep(0.3)

    logger.info(f"Dividend payouts: {total_synced} saved from {len(symbols)} symbols")
    return total_synced


def run_service(
    interval_seconds: int = 3600,
    sync_announcements_flag: bool = True,
    sync_events_flag: bool = True,
    sync_dividends_flag: bool = True,
    run_once: bool = False,
):
    """Run the announcements sync service.

    Args:
        interval_seconds: Seconds between sync runs
        sync_announcements_flag: Whether to sync announcements
        sync_events_flag: Whether to sync corporate events
        sync_dividends_flag: Whether to sync dividend payouts
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
    status = AnnouncementsServiceStatus(
        running=True,
        pid=pid,
        started_at=datetime.now().isoformat(),
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
        "Announcements sync service started (PID: %d, interval: %ds)",
        pid, interval_seconds
    )

    try:
        while not shutdown_requested:
            # Reset progress
            status.current_task = None
            status.current_symbol = None
            status.progress = 0.0
            status.announcements_synced = 0
            status.events_synced = 0
            status.dividends_synced = 0
            status.symbols_processed = 0
            status.symbols_failed = 0
            write_status(status)

            # Connect to database
            con = connect(get_db_path())
            init_schema(con)

            try:
                # Sync announcements
                if sync_announcements_flag and not shutdown_requested:
                    sync_announcements(con, status, logger)

                # Sync corporate events
                if sync_events_flag and not shutdown_requested:
                    sync_corporate_events(con, status, logger)

                # Sync dividend payouts
                if sync_dividends_flag and not shutdown_requested:
                    sync_dividend_payouts(con, status, logger)

                # Update status
                status.last_run_at = datetime.now().isoformat()
                status.total_runs += 1

                if status.symbols_failed == 0:
                    status.last_run_result = "success"
                    status.error_message = None
                else:
                    status.last_run_result = "partial"
                    status.error_message = f"{status.symbols_failed} symbols failed"

                logger.info(
                    "Sync complete: %d announcements, %d events, %d dividends",
                    status.announcements_synced,
                    status.events_synced,
                    status.dividends_synced,
                )

            except Exception as e:
                logger.error("Sync run failed: %s", e)
                status.last_run_at = datetime.now().isoformat()
                status.last_run_result = "error"
                status.error_message = str(e)

            finally:
                con.close()

            status.current_task = None
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
        status.current_task = None
        status.current_symbol = None
        write_status(status)
        remove_pid()


def start_service_background(
    interval_seconds: int = 3600,
    sync_announcements_flag: bool = True,
    sync_events_flag: bool = True,
    sync_dividends_flag: bool = True,
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
            interval_seconds=interval_seconds,
            sync_announcements_flag=sync_announcements_flag,
            sync_events_flag=sync_events_flag,
            sync_dividends_flag=sync_dividends_flag,
        )

    except Exception as e:
        print(f"Daemon error: {e}", file=sys.stderr)
    finally:
        os._exit(0)


# CLI entry point
def main():
    """CLI entry point for the service."""
    import argparse

    parser = argparse.ArgumentParser(description="Announcements sync background service")
    parser.add_argument(
        "action",
        choices=["start", "stop", "status", "run"],
        help="Action to perform"
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=3600,
        help="Interval between syncs in seconds (default: 3600)"
    )
    parser.add_argument(
        "--no-announcements",
        action="store_true",
        help="Skip syncing announcements"
    )
    parser.add_argument(
        "--no-events",
        action="store_true",
        help="Skip syncing corporate events"
    )
    parser.add_argument(
        "--no-dividends",
        action="store_true",
        help="Skip syncing dividend payouts"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (for testing)"
    )

    args = parser.parse_args()

    if args.action == "start":
        success, msg = start_service_background(
            interval_seconds=args.interval,
            sync_announcements_flag=not args.no_announcements,
            sync_events_flag=not args.no_events,
            sync_dividends_flag=not args.no_dividends,
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
            interval_seconds=args.interval,
            sync_announcements_flag=not args.no_announcements,
            sync_events_flag=not args.no_events,
            sync_dividends_flag=not args.no_dividends,
            run_once=args.once,
        )


if __name__ == "__main__":
    main()
