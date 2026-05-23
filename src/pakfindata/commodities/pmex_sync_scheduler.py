"""PMEX Unified Sync Scheduler — self-contained Python daemon using `schedule`.

No cron, no systemd needed. Runs as a single long-lived process that handles
all three data layers on their own schedules:

  LAYER 1: REALTIME — poll every 5 min during market hours (Mon-Fri, 10:00-23:30 PKT)
  LAYER 2: DAILY FILES — export EOD JSON + Parquet at 23:45 PKT
  LAYER 3: EOD SYNC — full DB sync at 00:30 PKT (next day)

Usage:
  # Start the daemon (runs forever, handles all schedules)
  python -m pakfindata.commodities.pmex_sync_scheduler daemon

  # One-shot commands
  python -m pakfindata.commodities.pmex_sync_scheduler realtime [--once]
  python -m pakfindata.commodities.pmex_sync_scheduler daily
  python -m pakfindata.commodities.pmex_sync_scheduler eod
  python -m pakfindata.commodities.pmex_sync_scheduler all
  python -m pakfindata.commodities.pmex_sync_scheduler status
"""

from __future__ import annotations

import json
import logging
import signal
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger("pakfindata.commodities.pmex_scheduler")

PKT = timezone(timedelta(hours=5))
PID_FILE = Path("/tmp/pmex_scheduler.pid")
LOG_FILE = Path("/tmp/pmex_scheduler.log")

_STOP = False


def _signal_handler(sig, frame):
    global _STOP
    _STOP = True
    logger.info("Stop signal received, shutting down scheduler...")


# ─────────────────────────────────────────────────────────────────────────────
# Layer 1: Realtime (delegates to poller)
# ─────────────────────────────────────────────────────────────────────────────


def run_realtime_poll(save_to_db: bool = True, save_to_file: bool = True) -> dict:
    """Execute a single intraday poll."""
    from .pmex_poller import single_poll
    return single_poll(save_to_db=save_to_db, save_to_file=save_to_file)


def run_realtime_loop(interval_minutes: int = 5) -> list[dict]:
    """Start continuous polling loop (standalone mode)."""
    from .pmex_poller import run_poller
    return run_poller(interval_minutes=interval_minutes)


def _scheduled_poll():
    """Scheduled poll — only executes during market hours."""
    from .pmex_poller import is_market_hours

    now = datetime.now(PKT)
    if not is_market_hours(now):
        return

    try:
        result = run_realtime_poll()
        logger.info(
            "Scheduled poll: %d contracts, %d stored",
            result["contracts"], result["rows_stored"],
        )
    except Exception as e:
        logger.error("Scheduled poll failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2: Daily file export
# ─────────────────────────────────────────────────────────────────────────────


def run_daily_export(target_date: str | None = None) -> dict:
    """Export all daily files: EOD JSON + OHLC/Margins Parquet + Intraday Rollup."""
    from .pmex_daily_files import export_daily

    dt = target_date or date.today().isoformat()
    logger.info("Starting daily file export for %s", dt)

    result = export_daily(dt)
    logger.info("Daily export complete: %s", result)
    return result


def _scheduled_daily_export():
    """Scheduled daily export — runs at 23:45 PKT."""
    try:
        result = run_daily_export()
        logger.info("Scheduled daily export: %s", result)
    except Exception as e:
        logger.error("Scheduled daily export failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3: EOD database sync
# ─────────────────────────────────────────────────────────────────────────────


def run_eod_sync(
    include_global: bool = True,
    include_pmex_portal: bool = True,
    include_pmex_ohlc: bool = True,
    include_pmex_margins: bool = True,
    include_khistocks: bool = True,
) -> dict:
    """Run full end-of-day commodity sync pipeline."""
    from .sync import sync_all_commodities

    sources = []
    if include_global:
        sources.extend(["yfinance", "fred", "worldbank"])
    if include_khistocks:
        sources.append("khistocks")
    if include_pmex_portal:
        sources.append("pmex_portal")
    if include_pmex_ohlc:
        sources.append("pmex_ohlc")
    if include_pmex_margins:
        sources.append("pmex_margins")

    logger.info("Starting EOD sync with sources: %s", sources)
    results = sync_all_commodities(sources=sources)

    summary = {}
    for source, s in results.items():
        summary[source] = {
            "symbols_total": s.symbols_total,
            "symbols_ok": s.symbols_ok,
            "rows_upserted": s.rows_upserted,
            "errors": len(s.errors),
        }

    logger.info("EOD sync complete: %s", summary)
    return summary


def _scheduled_eod_sync():
    """Scheduled EOD sync — runs at 00:30 PKT."""
    try:
        result = run_eod_sync()
        logger.info("Scheduled EOD sync: %s", result)
    except Exception as e:
        logger.error("Scheduled EOD sync failed: %s", e)


# ─────────────────────────────────────────────────────────────────────────────
# Daemon — all-in-one scheduler using `schedule` library
# ─────────────────────────────────────────────────────────────────────────────


def run_daemon(
    poll_interval: int = 5,
    daily_export_time: str = "23:45",
    eod_sync_time: str = "00:30",
):
    """Run the unified scheduler daemon.

    This is a single long-lived process that:
      - Polls PMEX every `poll_interval` minutes during market hours
      - Exports daily files at `daily_export_time` PKT
      - Runs full EOD sync at `eod_sync_time` PKT

    Runs forever until SIGINT/SIGTERM.

    Args:
        poll_interval: Minutes between intraday polls (default 5).
        daily_export_time: HH:MM PKT for daily file export (default "23:45").
        eod_sync_time: HH:MM PKT for EOD sync (default "00:30").
    """
    import schedule as sched

    global _STOP
    _STOP = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    # Write PID file
    PID_FILE.write_text(str(sys.modules["os"].getpid()) if "os" in sys.modules else "")
    import os
    PID_FILE.write_text(str(os.getpid()))

    logger.info("=" * 60)
    logger.info("PMEX Scheduler Daemon starting")
    logger.info("  Poll interval: every %d min (market hours only)", poll_interval)
    logger.info("  Daily export:  %s PKT", daily_export_time)
    logger.info("  EOD sync:      %s PKT", eod_sync_time)
    logger.info("  PID file:      %s", PID_FILE)
    logger.info("  Log file:      %s", LOG_FILE)
    logger.info("=" * 60)

    # Schedule Layer 1: intraday poll every N minutes
    sched.every(poll_interval).minutes.do(_scheduled_poll)

    # Schedule Layer 2: daily file export
    sched.every().day.at(daily_export_time).do(_scheduled_daily_export)

    # Schedule Layer 3: EOD sync
    sched.every().day.at(eod_sync_time).do(_scheduled_eod_sync)

    # Run an initial poll immediately
    logger.info("Running initial poll...")
    _scheduled_poll()

    # Main loop
    while not _STOP:
        sched.run_pending()
        time.sleep(30)  # Check every 30 seconds

    # Cleanup
    PID_FILE.unlink(missing_ok=True)
    logger.info("Scheduler daemon stopped")


# ─────────────────────────────────────────────────────────────────────────────
# Status check
# ─────────────────────────────────────────────────────────────────────────────


def get_sync_status() -> dict:
    """Get current status of all three sync layers."""
    from .commod_db import get_commod_connection, get_pmex_ohlc_stats, get_pmex_margins_stats, get_pmex_intraday_stats
    from .pmex_poller import is_market_hours, list_daily_files
    from .pmex_daily_files import list_exported_files

    con = get_commod_connection()
    now = datetime.now(PKT)

    # Check if daemon is running
    daemon_running = False
    daemon_pid = None
    if PID_FILE.exists():
        try:
            import os
            pid = int(PID_FILE.read_text().strip())
            os.kill(pid, 0)  # Check if process exists
            daemon_running = True
            daemon_pid = pid
        except (ValueError, ProcessLookupError, PermissionError):
            daemon_running = False

    status = {
        "timestamp": now.isoformat(),
        "market_open": is_market_hours(now),
        "daemon_running": daemon_running,
        "daemon_pid": daemon_pid,
        "layers": {},
    }

    # Layer 1: Intraday
    intra_stats = get_pmex_intraday_stats(con)
    intra_files = list_daily_files()
    status["layers"]["realtime"] = {
        "db_stats": intra_stats,
        "jsonl_files": len(intra_files),
        "latest_jsonl": intra_files[-1] if intra_files else None,
    }

    # Layer 2: Daily files
    exported = list_exported_files()
    status["layers"]["daily_files"] = {
        "eod_json_count": len(exported.get("eod_json", [])),
        "ohlc_parquet_count": len(exported.get("ohlc_parquet", [])),
        "margins_parquet_count": len(exported.get("margins_parquet", [])),
        "intraday_rollup_count": len(exported.get("intraday_rollup", [])),
    }

    # Layer 3: EOD sync
    ohlc_stats = get_pmex_ohlc_stats(con)
    margins_stats = get_pmex_margins_stats(con)
    status["layers"]["eod_sync"] = {
        "pmex_ohlc": ohlc_stats,
        "pmex_margins": margins_stats,
    }

    return status


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────


def main():
    """CLI: python -m pakfindata.commodities.pmex_sync_scheduler <command>"""
    import argparse

    parser = argparse.ArgumentParser(
        description="PMEX Unified Sync Scheduler",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Start the all-in-one daemon (recommended)
  python -m pakfindata.commodities.pmex_sync_scheduler daemon

  # Start daemon in background
  nohup python -m pakfindata.commodities.pmex_sync_scheduler daemon &

  # One-shot commands
  python -m pakfindata.commodities.pmex_sync_scheduler realtime --once
  python -m pakfindata.commodities.pmex_sync_scheduler daily
  python -m pakfindata.commodities.pmex_sync_scheduler eod
  python -m pakfindata.commodities.pmex_sync_scheduler status
        """,
    )
    parser.add_argument(
        "command",
        choices=["daemon", "realtime", "daily", "eod", "all", "status", "stop"],
        help="Command to run",
    )
    parser.add_argument("--interval", type=int, default=5, help="Poll interval in minutes (default: 5)")
    parser.add_argument("--date", type=str, default=None, help="Target date for daily export (YYYY-MM-DD)")
    parser.add_argument("--once", action="store_true", help="Single poll for realtime mode")
    parser.add_argument("--daily-at", type=str, default="23:45", help="Daily export time HH:MM (default: 23:45)")
    parser.add_argument("--eod-at", type=str, default="00:30", help="EOD sync time HH:MM (default: 00:30)")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    # Setup logging
    handlers = [logging.StreamHandler()]
    if args.command == "daemon":
        handlers.append(logging.FileHandler(str(LOG_FILE)))

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        handlers=handlers,
    )

    if args.command == "daemon":
        run_daemon(
            poll_interval=args.interval,
            daily_export_time=args.daily_at,
            eod_sync_time=args.eod_at,
        )

    elif args.command == "realtime":
        if args.once:
            result = run_realtime_poll()
            print(json.dumps(result, indent=2, default=str))
        else:
            run_realtime_loop(interval_minutes=args.interval)

    elif args.command == "daily":
        result = run_daily_export(target_date=args.date)
        print(json.dumps(result, indent=2, default=str))

    elif args.command == "eod":
        result = run_eod_sync()
        print(json.dumps(result, indent=2, default=str))

    elif args.command == "all":
        print("=== LAYER 1: Realtime (single poll) ===")
        r1 = run_realtime_poll()
        print(json.dumps(r1, indent=2, default=str))

        print("\n=== LAYER 2: Daily File Export ===")
        r2 = run_daily_export(target_date=args.date)
        print(json.dumps(r2, indent=2, default=str))

        print("\n=== LAYER 3: EOD Sync ===")
        r3 = run_eod_sync()
        print(json.dumps(r3, indent=2, default=str))

    elif args.command == "status":
        status = get_sync_status()
        print(json.dumps(status, indent=2, default=str))

    elif args.command == "stop":
        if PID_FILE.exists():
            import os
            try:
                pid = int(PID_FILE.read_text().strip())
                os.kill(pid, signal.SIGTERM)
                print(f"Sent SIGTERM to daemon (PID {pid})")
            except (ValueError, ProcessLookupError):
                print("Daemon not running (stale PID file)")
                PID_FILE.unlink(missing_ok=True)
        else:
            print("No daemon PID file found")


if __name__ == "__main__":
    main()
