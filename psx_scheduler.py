#!/usr/bin/env python3
"""PSX Market Scheduler — waits for 9:15 AM PKT and fires start_psx.sh.

Usage:
    python3 ~/pakfindata/psx_scheduler.py          # foreground (Ctrl+C to stop)
    nohup python3 ~/pakfindata/psx_scheduler.py &  # background daemon

How it works:
  1. Calculates next market open (9:15 AM PKT, Mon-Fri)
  2. Sleeps until that exact time
  3. Fires start_psx.sh as a fully detached process
  4. Goes back to step 1 (waits for NEXT trading day)

The scheduler NEVER fires immediately on startup. It always waits for
the next 9:15 AM. If you start it at 10:00 AM, it waits until tomorrow.

Ctrl+C stops the scheduler only — trading services keep running.
"""

import os
import sys
import subprocess
import time
from datetime import datetime, timedelta
from pathlib import Path

# ── Config ──────────────────────────────────────────────────────────────────
MARKET_OPEN_HOUR = 9
MARKET_OPEN_MIN = 15
MARKET_DAYS = {0, 1, 2, 3, 4}  # Mon=0 ... Fri=4
START_SCRIPT = Path.home() / "pakfindata" / "start_psx.sh"
SCHED_LOG = Path.home() / "psxdata" / "scheduler.log"
PSX_LOG = Path.home() / "psxdata" / "scheduler_psx_output.log"  # separate log for start_psx.sh


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        SCHED_LOG.parent.mkdir(parents=True, exist_ok=True)
        with open(SCHED_LOG, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def next_market_open() -> datetime:
    """Calculate the NEXT market open (always in the future, never now)."""
    now = datetime.now()
    target = now.replace(hour=MARKET_OPEN_HOUR, minute=MARKET_OPEN_MIN, second=0, microsecond=0)

    # Always target the future — if we're past today's open, go to tomorrow
    if now >= target:
        target += timedelta(days=1)

    # Skip weekends
    while target.weekday() not in MARKET_DAYS:
        target += timedelta(days=1)

    return target


def format_wait(seconds: float) -> str:
    """Human-readable wait time."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    minutes = seconds / 60
    if minutes < 60:
        return f"{minutes:.0f}m"
    hours = minutes / 60
    if hours < 24:
        h = int(hours)
        m = int(minutes % 60)
        return f"{h}h {m}m"
    days = int(hours / 24)
    h = int(hours % 24)
    return f"{days}d {h}h"


def fire_start_psx():
    """Launch start_psx.sh as a fully independent detached process."""
    log(">>> FIRING start_psx.sh")
    try:
        with open(PSX_LOG, "a") as logf:
            logf.write(f"\n{'='*60}\n")
            logf.write(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] start_psx.sh launched by scheduler\n")
            logf.write(f"{'='*60}\n")
            logf.flush()
            subprocess.Popen(
                ["/usr/bin/setsid", "/bin/bash", str(START_SCRIPT)],
                stdout=logf,
                stderr=logf,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
        log(">>> start_psx.sh is now running as independent process")
        log(f"    PSX output log: {PSX_LOG}")
    except Exception as e:
        log(f"!!! ERROR launching start_psx.sh: {e}")


def main():
    # Clear screen for fresh start
    log("")
    log("=" * 60)
    log("PSX MARKET SCHEDULER")
    log("=" * 60)

    if not START_SCRIPT.exists():
        log(f"FATAL: {START_SCRIPT} not found!")
        sys.exit(1)

    log(f"  Script:    {START_SCRIPT}")
    log(f"  Trigger:   09:15 AM PKT, Mon-Fri")
    log(f"  Sched log: {SCHED_LOG}")
    log(f"  PSX log:   {PSX_LOG}")
    log("")

    # Main loop — each iteration waits for one market open, fires, repeats
    while True:
        try:
            target = next_market_open()
            now = datetime.now()
            wait_secs = (target - now).total_seconds()

            log(f"WAITING for {target.strftime('%A %Y-%m-%d')} at {target.strftime('%H:%M')} PKT")
            log(f"  Time now:  {now.strftime('%H:%M:%S')}")
            log(f"  Fires in:  {format_wait(wait_secs)}")
            log(f"  Status:    SLEEPING (safe to close terminal if using nohup)")
            log("")

            # Sleep in chunks so we can show periodic heartbeats
            while True:
                now = datetime.now()
                remaining = (target - now).total_seconds()

                if remaining <= 0:
                    break

                # Heartbeat every 30 min while far out, every 5 min in last 30 min
                if remaining > 1800:
                    log(f"  ... {format_wait(remaining)} remaining until {target.strftime('%A %H:%M')}")
                    time.sleep(min(1800, remaining))
                elif remaining > 60:
                    # Last 30 min — check every 5 minutes
                    log(f"  ... {format_wait(remaining)} remaining")
                    time.sleep(min(300, remaining))
                else:
                    # Last minute — check every 5 seconds for precision
                    time.sleep(min(5, remaining))

            # ── FIRE ────────────────────────────────────────────────
            log("")
            log("*" * 60)
            log(f"MARKET OPEN — {datetime.now().strftime('%A %Y-%m-%d %H:%M:%S')}")
            log("*" * 60)
            fire_start_psx()
            log("")
            log("Services launched. Scheduler will now wait for next trading day.")
            log("")

            # Small delay before calculating next open (avoid same-second re-trigger)
            time.sleep(60)

        except KeyboardInterrupt:
            log("")
            log("SCHEDULER STOPPED (Ctrl+C)")
            log("Trading services (if already launched) are NOT affected.")
            break
        except Exception as e:
            log(f"!!! UNEXPECTED ERROR: {e}")
            log("    Retrying in 60 seconds...")
            time.sleep(60)


if __name__ == "__main__":
    main()
