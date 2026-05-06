"""
PSX Multi-Resolution Archive Pull — Python orchestrator with multi-market support.

Pulls historical klines for multiple timeframes in one efficient run.
Handles WebSocket disconnects by reconnecting per-timeframe — if one timeframe
errors out, the next gets a fresh connection.

Usage:
    # Pull all 8 timeframes for REG market (default)
    python -m pakfindata.sources.psxt_archive_pull

    # Pull all timeframes for ALL markets (REG + FUT + IDX)
    python -m pakfindata.sources.psxt_archive_pull --markets REG FUT IDX

    # Specific timeframes only
    python -m pakfindata.sources.psxt_archive_pull --timeframes 1m 5m 1h

    # Single symbol, all timeframes
    python -m pakfindata.sources.psxt_archive_pull --symbol HUBC

    # Filter by date
    python -m pakfindata.sources.psxt_archive_pull --date 2026-04-29
    python -m pakfindata.sources.psxt_archive_pull --from 2026-04-27 --to 2026-04-30

    # Custom output dir
    python -m pakfindata.sources.psxt_archive_pull --out /tmp/psx_archive

    # Reconnect on failure (retry up to 3 times if a timeframe fails mid-pull)
    python -m pakfindata.sources.psxt_archive_pull --max-retries 3
"""

import argparse
import asyncio
import csv
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from pathlib import Path

try:
    import msgpack
    import websockets
    import requests
except ImportError as e:
    print(f"Missing dependency: {e}")
    print("Install with: pip install msgpack websockets requests")
    sys.exit(1)


# ════════════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════════════

INIT_URL = "https://psxterminal.com/api/init"
WS_URL_TEMPLATE = "wss://psxterminal.com/rt?t={token}"
PKT = timezone(timedelta(hours=5))

ALL_TIMEFRAMES = ["1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"]
VALID_MARKETS = {"REG", "FUT", "IDX"}
DEFAULT_MARKETS = ["REG"]
DEFAULT_OUT_DIR = Path.home() / "psxdata_rescue" / "intraday" / "psxt_ws"

PER_REQUEST_TIMEOUT = 12
MAX_WS_MESSAGE_SIZE = 50 * 1024 * 1024
PAUSE_BETWEEN_TIMEFRAMES = 2.0
RETRY_BACKOFF_BASE = 5.0  # seconds; exponential backoff per retry


# ════════════════════════════════════════════════════════════════
# CONNECTION HELPERS
# ════════════════════════════════════════════════════════════════

def get_token() -> str:
    r = requests.get(INIT_URL, timeout=10)
    r.raise_for_status()
    return r.json()["token"]


async def connect():
    """Open authenticated WebSocket and consume welcome message."""
    token = get_token()
    url = WS_URL_TEMPLATE.format(token=token)
    ws = await websockets.connect(url, open_timeout=15, max_size=MAX_WS_MESSAGE_SIZE)
    welcome = msgpack.unpackb(
        await asyncio.wait_for(ws.recv(), timeout=10), raw=False
    )
    return ws, welcome.get("clientId", "?")


async def request(ws, payload: dict):
    await ws.send(json.dumps(payload))
    raw = await asyncio.wait_for(ws.recv(), timeout=PER_REQUEST_TIMEOUT)
    return msgpack.unpackb(raw, raw=False)


async def fetch_klines(ws, symbol: str, timeframe: str, request_id: int) -> list[dict]:
    """Send klines request, return decoded bar list."""
    decoded = await request(ws, {
        "type": "klines",
        "symbol": symbol,
        "timeframe": timeframe,
        "requestId": request_id,
    })
    return decoded.get("klines", [])


async def fetch_symbol_list(ws, markets: list[str]) -> list[dict]:
    """Get symbols, filtered by markets. Returns list of {symbol, market} dicts."""
    decoded = await request(ws, {"type": "getSymbols", "requestId": 1})
    market_set = set(markets)
    return [s for s in decoded.get("symbols", []) if s.get("market") in market_set]


# ════════════════════════════════════════════════════════════════
# DATE FILTERING
# ════════════════════════════════════════════════════════════════

def in_date_range(ts_ms: int, from_date, to_date) -> bool:
    if from_date is None and to_date is None:
        return True
    bar_date = datetime.fromtimestamp(ts_ms / 1000, tz=PKT).date()
    if from_date and bar_date < from_date:
        return False
    if to_date and bar_date > to_date:
        return False
    return True


# ════════════════════════════════════════════════════════════════
# PULL LOGIC
# ════════════════════════════════════════════════════════════════

async def pull_timeframe_attempt(
    symbols: list[str], timeframe: str,
    out_path: Path, from_date, to_date,
) -> tuple[int, int, int, list[str], bool]:
    """
    Single attempt at pulling one timeframe with a fresh connection.
    Returns (ok_count, total_bars, kept_bars, failed_symbols, completed).
    `completed` is False if the connection died mid-pull.
    """
    ws, client_id = await connect()
    print(f"      Connected ({client_id[:8]}...)")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    total_bars = 0
    kept_bars = 0
    ok_count = 0
    failed: list[str] = []
    completed = True

    try:
        with open(out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow([
                "symbol", "market", "timestamp_ms", "datetime_pkt",
                "open", "high", "low", "close",
                "volume", "quoteVolume", "trades", "interval",
            ])

            for i, sym in enumerate(symbols, 1):
                try:
                    bars = await fetch_klines(ws, sym, timeframe, request_id=i + 10000)
                    total_bars += len(bars)
                    for b in bars:
                        ts_ms = int(b["timestamp"])
                        if not in_date_range(ts_ms, from_date, to_date):
                            continue
                        dt = datetime.fromtimestamp(ts_ms / 1000, tz=PKT)
                        w.writerow([
                            b["symbol"], b.get("market", ""), ts_ms,
                            dt.strftime("%Y-%m-%d %H:%M:%S"),
                            b["open"], b["high"], b["low"], b["close"],
                            b["volume"], b.get("quoteVolume", 0), b.get("trades", 0),
                            b.get("interval", timeframe),
                        ])
                        kept_bars += 1
                    ok_count += 1
                except websockets.ConnectionClosed:
                    print(f"      ⚠️  Connection closed at symbol {i}/{len(symbols)} ({sym})")
                    completed = False
                    break
                except asyncio.TimeoutError:
                    failed.append(sym)
                    # Connection might still be alive — keep trying others
                except Exception as e:
                    failed.append(f"{sym} ({type(e).__name__})")

                if i % 50 == 0:
                    print(f"      [{i}/{len(symbols)}] {ok_count} ok, "
                          f"{kept_bars:,}/{total_bars:,} bars")
    finally:
        try:
            await ws.close()
        except Exception:
            pass

    return ok_count, total_bars, kept_bars, failed, completed


async def pull_timeframe_with_retry(
    symbols: list[str], timeframe: str,
    out_path: Path, from_date, to_date,
    max_retries: int,
) -> dict:
    """Pull a timeframe with up to max_retries reconnections."""
    last_error = None
    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                backoff = RETRY_BACKOFF_BASE * (2 ** (attempt - 1))
                print(f"      Retry {attempt}/{max_retries} after {backoff:.0f}s pause...")
                await asyncio.sleep(backoff)

            ok, total, kept, failed, completed = await pull_timeframe_attempt(
                symbols, timeframe, out_path, from_date, to_date,
            )

            if completed:
                return {
                    "status": "ok", "ok": ok, "total_symbols": len(symbols),
                    "total_bars": total, "kept_bars": kept,
                    "failed_symbols": failed,
                    "out_path": out_path, "attempts": attempt + 1,
                }
            else:
                # Connection died mid-pull — partial data on disk
                if attempt < max_retries:
                    print(f"      Partial pull ({ok}/{len(symbols)}). Retrying...")
                    last_error = "connection_closed_mid_pull"
                else:
                    return {
                        "status": "partial", "ok": ok, "total_symbols": len(symbols),
                        "total_bars": total, "kept_bars": kept,
                        "failed_symbols": failed,
                        "out_path": out_path, "attempts": attempt + 1,
                        "error": "connection closed before completion",
                    }
        except Exception as e:
            last_error = str(e)
            if attempt < max_retries:
                print(f"      ❌ Attempt {attempt + 1} failed: {type(e).__name__}: {str(e)[:120]}")
            else:
                return {
                    "status": "failed", "error": last_error,
                    "attempts": attempt + 1, "out_path": out_path,
                }
    return {"status": "failed", "error": last_error or "unknown", "attempts": max_retries + 1}


async def run(args):
    print("=" * 64)
    print("  PSX Multi-Resolution Archive Pull")
    print(f"  Started:    {datetime.now(PKT).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"  Timeframes: {', '.join(args.timeframes)}")
    print(f"  Markets:    {', '.join(args.markets)}")
    print(f"  Symbol:     {args.symbol or 'ALL'}")
    print(f"  Output:     {args.out}")
    print(f"  Max retries: {args.max_retries}")
    if args.from_date or args.to_date:
        print(f"  Date filter: {args.from_date or 'start'} → {args.to_date or 'now'}")
    print("=" * 64)
    print()

    overall_start = time.time()
    results: list[dict] = []

    # First connection: get the symbol universe
    print("Connecting (initial)...")
    ws, client_id = await connect()
    print(f"  ✅ Connected. clientId: {client_id}")

    if args.symbol:
        symbols = [args.symbol]
    else:
        print(f"\nFetching symbol list (markets: {', '.join(args.markets)})...")
        sym_records = await fetch_symbol_list(ws, args.markets)
        symbols = [s["symbol"] for s in sym_records]
        by_market = Counter(s.get("market", "?") for s in sym_records)
        print(f"  {len(symbols)} symbols across {len(by_market)} markets:")
        for m, c in by_market.most_common():
            print(f"    {m}: {c}")

    await ws.close()
    print()

    # Pull each timeframe with its own fresh connection (more resilient)
    for i, tf in enumerate(args.timeframes, 1):
        print(f"[{i}/{len(args.timeframes)}] Timeframe: {tf}")
        if args.symbol:
            tag = args.symbol
        else:
            tag = "all_" + "_".join(sorted(args.markets)).lower()
        date_tag = ""
        if args.from_date and args.to_date and args.from_date == args.to_date:
            date_tag = f"_{args.from_date}"
        elif args.from_date or args.to_date:
            date_tag = f"_{args.from_date or 'start'}_{args.to_date or 'now'}"
        out_path = args.out / f"{tag}_{tf}{date_tag}.csv"

        tf_start = time.time()
        result = await pull_timeframe_with_retry(
            symbols, tf, out_path, args.from_date, args.to_date,
            max_retries=args.max_retries,
        )
        duration = time.time() - tf_start
        result["timeframe"] = tf
        result["duration"] = duration

        if result["status"] == "ok":
            size_mb = out_path.stat().st_size / 1_048_576 if out_path.exists() else 0
            result["size_mb"] = size_mb
            print(f"  ✅ {tf}: {result['ok']}/{result['total_symbols']} symbols, "
                  f"{result['kept_bars']:,}/{result['total_bars']:,} bars, "
                  f"{size_mb:.1f} MB, {duration:.0f}s")
        elif result["status"] == "partial":
            size_mb = out_path.stat().st_size / 1_048_576 if out_path.exists() else 0
            result["size_mb"] = size_mb
            print(f"  ⚠️  {tf}: PARTIAL ({result['ok']}/{result['total_symbols']} before disconnect)")
        else:
            print(f"  ❌ {tf}: FAILED — {result.get('error', '?')[:120]}")

        results.append(result)
        await asyncio.sleep(PAUSE_BETWEEN_TIMEFRAMES)
        print()

    # Summary
    overall_duration = time.time() - overall_start
    print("=" * 64)
    print("  SUMMARY")
    print("=" * 64)
    print(f"  Total time: {overall_duration:.0f}s "
          f"({int(overall_duration // 60)}m {int(overall_duration % 60)}s)")

    succeeded = [r for r in results if r.get("status") == "ok"]
    partial = [r for r in results if r.get("status") == "partial"]
    failed = [r for r in results if r.get("status") == "failed"]
    print(f"  Succeeded:  {len(succeeded)}/{len(results)}")
    if partial:
        print(f"  Partial:    {len(partial)}/{len(results)}")
    if failed:
        print(f"  Failed:     {len(failed)}/{len(results)}")
    print()

    if succeeded:
        print("  ✅ Successful pulls:")
        for r in succeeded:
            print(f"     - {r['timeframe']:>3s}: {r['kept_bars']:>10,} bars, "
                  f"{r.get('size_mb', 0):>5.1f} MB, {r['duration']:>4.0f}s")
    if partial:
        print("\n  ⚠️  Partial pulls (some data saved):")
        for r in partial:
            print(f"     - {r['timeframe']:>3s}: {r['kept_bars']:>10,} bars, "
                  f"only {r['ok']}/{r['total_symbols']} symbols completed")
    if failed:
        print("\n  ❌ Failed pulls:")
        for r in failed:
            print(f"     - {r['timeframe']}: {r.get('error', '?')[:120]}")

    print()
    print(f"  Output dir: {args.out}")
    print("=" * 64)

    return 0 if not failed and not partial else 1


# ════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════

def parse_date(s: str):
    if s is None:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date '{s}'. Use YYYY-MM-DD format.")


def main():
    parser = argparse.ArgumentParser(
        description="Pull complete multi-resolution PSX historical archive",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Valid timeframes: {' '.join(ALL_TIMEFRAMES)}
Valid markets:    {' '.join(sorted(VALID_MARKETS))}

Examples:
  # All 8 timeframes for REG market
  python -m pakfindata.sources.psxt_archive_pull

  # All timeframes, all markets (REG + FUT + IDX)
  python -m pakfindata.sources.psxt_archive_pull --markets REG FUT IDX

  # Just indices, all timeframes
  python -m pakfindata.sources.psxt_archive_pull --markets IDX

  # Specific timeframes for futures
  python -m pakfindata.sources.psxt_archive_pull --timeframes 1m 5m --markets FUT

  # Date filter
  python -m pakfindata.sources.psxt_archive_pull --date 2026-04-29
  python -m pakfindata.sources.psxt_archive_pull --from 2026-04-27 --to 2026-04-30
        """,
    )
    parser.add_argument(
        "--timeframes", "-t", nargs="+", default=ALL_TIMEFRAMES,
        choices=ALL_TIMEFRAMES,
        help="Timeframes to pull (default: all)",
    )
    parser.add_argument(
        "--markets", nargs="+", choices=sorted(VALID_MARKETS),
        default=DEFAULT_MARKETS,
        help=f"Markets to include (default: {' '.join(DEFAULT_MARKETS)})",
    )
    parser.add_argument(
        "--symbol", help="Single symbol (default: full universe)",
    )
    parser.add_argument(
        "--out", type=Path, default=DEFAULT_OUT_DIR,
        help=f"Output directory (default: {DEFAULT_OUT_DIR})",
    )
    parser.add_argument(
        "--date", type=parse_date,
        help="Filter to specific date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--from", dest="from_date", type=parse_date,
        help="Filter from this date onward",
    )
    parser.add_argument(
        "--to", dest="to_date", type=parse_date,
        help="Filter up to this date inclusive",
    )
    parser.add_argument(
        "--max-retries", type=int, default=2,
        help="Max retry attempts per timeframe on connection failure (default: 2)",
    )
    args = parser.parse_args()

    if args.date:
        args.from_date = args.date
        args.to_date = args.date

    sys.exit(asyncio.run(run(args)))


if __name__ == "__main__":
    main()
