"""PMEX Intraday Poller — polls portal API at regular intervals, stores timestamped snapshots.

Three operating modes:
  1. single_poll()     — One-shot: fetch + store + return (for cron or manual)
  2. run_poller()      — Loop: polls every N minutes until market close (for daemon)
  3. CLI:  python -m pakfindata.commodities.pmex_poller [--interval 5] [--once]

PMEX Market Hours (PKT):
  Mon-Fri 10:00 — 23:30 (metals/oil/indices follow international markets)
  Physical gold (TOLAGOLD-WED): Mon-Fri 10:00 — 17:00

Data flow:
  fetch_pmex_snapshot() → enrich with spread/mid → upsert_pmex_intraday() → commod.db
  Optionally also dumps each poll as a JSON line to daily file for fast replay.
"""

from __future__ import annotations

import json
import logging
import signal
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from .commod_db import (
    get_commod_connection,
    init_commod_schema,
    upsert_pmex_intraday,
)
from .fetcher_pmex import fetch_pmex_snapshot

logger = logging.getLogger("pakfindata.commodities.pmex_poller")

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

PKT = timezone(timedelta(hours=5))  # Pakistan Standard Time (UTC+5)

# PMEX trading hours in PKT (generous window — some contracts trade late)
MARKET_OPEN_HOUR = 10
MARKET_CLOSE_HOUR = 23
MARKET_CLOSE_MINUTE = 30

# Daily file directory
INTRADAY_FILES_DIR = Path("/mnt/e/psxdata/commod/pmex_intraday")


# ─────────────────────────────────────────────────────────────────────────────
# Market hours check
# ─────────────────────────────────────────────────────────────────────────────


def is_market_hours(now: datetime | None = None) -> bool:
    """Check if current time is within PMEX trading hours (Mon-Fri, 10:00-23:30 PKT)."""
    now = now or datetime.now(PKT)
    # Weekend check (Sat=5, Sun=6)
    if now.weekday() >= 5:
        return False
    # Hour check
    t = now.hour * 60 + now.minute
    open_t = MARKET_OPEN_HOUR * 60
    close_t = MARKET_CLOSE_HOUR * 60 + MARKET_CLOSE_MINUTE
    return open_t <= t <= close_t


def next_market_open(now: datetime | None = None) -> datetime:
    """Return the next market open datetime (PKT)."""
    now = now or datetime.now(PKT)
    target = now.replace(hour=MARKET_OPEN_HOUR, minute=0, second=0, microsecond=0)
    if now >= target or now.weekday() >= 5:
        # Move to next weekday
        days_ahead = 1
        while True:
            candidate = now + timedelta(days=days_ahead)
            if candidate.weekday() < 5:
                return candidate.replace(hour=MARKET_OPEN_HOUR, minute=0, second=0, microsecond=0)
            days_ahead += 1
    return target


# ─────────────────────────────────────────────────────────────────────────────
# Single poll
# ─────────────────────────────────────────────────────────────────────────────


def single_poll(
    save_to_db: bool = True,
    save_to_file: bool = True,
) -> dict:
    """Execute one poll: fetch PMEX snapshot, enrich, store.

    Args:
        save_to_db: Upsert into pmex_intraday_snapshots table.
        save_to_file: Append to daily JSONL file.

    Returns:
        Dict with keys: poll_ts, contracts, rows_stored, file_path, errors.
    """
    now = datetime.now(PKT)
    poll_ts = now.isoformat()
    snapshot_date = now.strftime("%Y-%m-%d")

    result = {
        "poll_ts": poll_ts,
        "contracts": 0,
        "rows_stored": 0,
        "file_path": None,
        "errors": [],
    }

    # Fetch
    try:
        raw = fetch_pmex_snapshot()
    except Exception as e:
        result["errors"].append(f"Fetch failed: {e}")
        logger.error("Poll fetch failed: %s", e)
        return result

    if not raw:
        result["errors"].append("Empty response from PMEX portal")
        return result

    result["contracts"] = len(raw)

    # Enrich with spread and mid-price
    rows = []
    for r in raw:
        bid = r.get("bid") or 0
        ask = r.get("ask") or 0
        mid = (bid + ask) / 2 if bid > 0 and ask > 0 else None
        spread = (ask - bid) if bid > 0 and ask > 0 else None
        spread_pct = (spread / mid * 100) if mid and mid > 0 and spread else None

        rows.append({
            "contract": r["contract"],
            "snapshot_ts": poll_ts,
            "snapshot_date": snapshot_date,
            "category": r.get("category", ""),
            "bid": bid or None,
            "ask": ask or None,
            "open": r.get("open"),
            "high": r.get("high"),
            "low": r.get("low"),
            "close": r.get("close"),
            "last_price": r.get("last_price"),
            "last_vol": int(r.get("last_vol") or 0),
            "total_vol": int(r.get("total_vol") or 0),
            "change": r.get("change"),
            "change_pct": r.get("change_pct"),
            "bid_diff": r.get("bid_diff"),
            "ask_diff": r.get("ask_diff"),
            "mid_price": round(mid, 4) if mid else None,
            "spread": round(spread, 4) if spread else None,
            "spread_pct": round(spread_pct, 6) if spread_pct else None,
            "source": "pmex_poller",
        })

    # Store to DB
    if save_to_db:
        try:
            con = get_commod_connection()
            init_commod_schema(con)
            n = upsert_pmex_intraday(con, rows)
            result["rows_stored"] = n
        except Exception as e:
            result["errors"].append(f"DB upsert failed: {e}")
            logger.error("DB upsert failed: %s", e)

    # Append to daily JSONL file
    if save_to_file:
        try:
            fpath = _append_daily_jsonl(rows, snapshot_date)
            result["file_path"] = str(fpath)
        except Exception as e:
            result["errors"].append(f"File write failed: {e}")
            logger.error("File write failed: %s", e)

    logger.info(
        "Poll %s: %d contracts, %d stored, errors=%d",
        poll_ts, result["contracts"], result["rows_stored"], len(result["errors"]),
    )
    return result


def _append_daily_jsonl(rows: list[dict], snapshot_date: str) -> Path:
    """Append poll data as one JSON line per contract to daily file.

    File: /mnt/e/psxdata/commod/pmex_intraday/YYYY-MM-DD.jsonl
    Each line is a complete contract snapshot — fast for replay and analysis.
    """
    INTRADAY_FILES_DIR.mkdir(parents=True, exist_ok=True)
    fpath = INTRADAY_FILES_DIR / f"{snapshot_date}.jsonl"

    with open(fpath, "a") as f:
        for row in rows:
            f.write(json.dumps(row, default=str) + "\n")

    return fpath


# ─────────────────────────────────────────────────────────────────────────────
# Continuous poller loop
# ─────────────────────────────────────────────────────────────────────────────

_STOP = False


def _signal_handler(sig, frame):
    global _STOP
    _STOP = True
    logger.info("Stop signal received, finishing current poll...")


def run_poller(
    interval_minutes: int = 5,
    save_to_db: bool = True,
    save_to_file: bool = True,
    stop_after_close: bool = True,
) -> list[dict]:
    """Run continuous polling loop during market hours.

    Args:
        interval_minutes: Minutes between polls (default 5).
        save_to_db: Store each poll in commod.db.
        save_to_file: Append each poll to daily JSONL.
        stop_after_close: Auto-stop after market close (default True).

    Returns:
        List of poll results from the session.
    """
    global _STOP
    _STOP = False

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    logger.info(
        "PMEX Poller starting — interval=%dm, db=%s, file=%s",
        interval_minutes, save_to_db, save_to_file,
    )

    results = []
    interval_sec = interval_minutes * 60

    while not _STOP:
        now = datetime.now(PKT)

        if not is_market_hours(now):
            if stop_after_close:
                logger.info("Market closed at %s, stopping poller", now.strftime("%H:%M PKT"))
                break
            else:
                next_open = next_market_open(now)
                sleep_sec = (next_open - now).total_seconds()
                logger.info(
                    "Market closed. Next open: %s (sleeping %.0fm)",
                    next_open.strftime("%Y-%m-%d %H:%M PKT"), sleep_sec / 60,
                )
                time.sleep(min(sleep_sec, 3600))  # Sleep max 1hr, re-check
                continue

        # Execute poll
        poll_result = single_poll(save_to_db=save_to_db, save_to_file=save_to_file)
        results.append(poll_result)

        # Sleep until next interval
        if not _STOP:
            logger.debug("Sleeping %d minutes until next poll...", interval_minutes)
            # Sleep in small chunks so we can respond to stop signals
            for _ in range(interval_sec):
                if _STOP:
                    break
                time.sleep(1)

    logger.info("Poller stopped after %d polls", len(results))
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Daily file reader (for replay/analysis)
# ─────────────────────────────────────────────────────────────────────────────


def read_daily_jsonl(snapshot_date: str) -> list[dict]:
    """Read all intraday snapshots from a daily JSONL file.

    Args:
        snapshot_date: "YYYY-MM-DD"

    Returns:
        List of dicts (one per contract per poll).
    """
    fpath = INTRADAY_FILES_DIR / f"{snapshot_date}.jsonl"
    if not fpath.exists():
        return []

    rows = []
    with open(fpath) as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def list_daily_files() -> list[dict]:
    """List all available daily intraday JSONL files.

    Returns:
        List of dicts with keys: date, file_path, size_kb, lines.
    """
    if not INTRADAY_FILES_DIR.exists():
        return []

    files = []
    for fpath in sorted(INTRADAY_FILES_DIR.glob("*.jsonl")):
        size_kb = fpath.stat().st_size / 1024
        line_count = sum(1 for _ in open(fpath))
        files.append({
            "date": fpath.stem,
            "file_path": str(fpath),
            "size_kb": round(size_kb, 1),
            "lines": line_count,
        })
    return files


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────


def main():
    """CLI entry: python -m pakfindata.commodities.pmex_poller"""
    import argparse

    parser = argparse.ArgumentParser(description="PMEX Intraday Poller")
    parser.add_argument("--interval", type=int, default=5, help="Poll interval in minutes (default: 5)")
    parser.add_argument("--once", action="store_true", help="Single poll then exit")
    parser.add_argument("--no-db", action="store_true", help="Skip database storage")
    parser.add_argument("--no-file", action="store_true", help="Skip JSONL file storage")
    parser.add_argument("--no-stop", action="store_true", help="Don't stop after market close")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    if args.once:
        result = single_poll(
            save_to_db=not args.no_db,
            save_to_file=not args.no_file,
        )
        print(json.dumps(result, indent=2, default=str))
    else:
        run_poller(
            interval_minutes=args.interval,
            save_to_db=not args.no_db,
            save_to_file=not args.no_file,
            stop_after_close=not args.no_stop,
        )


if __name__ == "__main__":
    main()
