"""
PSX Terminal WebSocket Backfill Tool

Fetches historical klines and live market data from psxterminal.com's new
WebSocket protocol (post-April 2026 migration).

Protocol:
    1. GET https://psxterminal.com/api/init  →  {"token": "..."}
    2. wss://psxterminal.com/rt?t={token}
    3. Server sends welcome (binary msgpack), then accepts text JSON commands
    4. Returns binary msgpack responses

Markets supported (verified via getSymbols probe):
    REG  — Regular equities (481 symbols, e.g. HUBC, OGDC)
    FUT  — Futures contracts (~90 symbols, *-MAY *-JUN suffixed)
    IDX  — Indices (7 symbols, e.g. KSE100, KMI30, ALLSHR)

Valid timeframes (verified via probe):
    1m, 5m, 15m, 1h, 4h, 1d, 1w, 1M
    (30m, 2h NOT supported — server rejects with "Invalid timeframe")
    (sub-minute NOT supported — use `live` mode for tick-level data)

Usage:
    # Test connection
    python -m pakfindata.sources.psxt_ws ping

    # List symbols across markets
    python -m pakfindata.sources.psxt_ws symbols
    python -m pakfindata.sources.psxt_ws symbols --markets REG FUT IDX
    python -m pakfindata.sources.psxt_ws symbols --markets REG FUT IDX --out symbols.csv

    # Fetch historical klines for one symbol
    python -m pakfindata.sources.psxt_ws klines HUBC --timeframe 1m
    python -m pakfindata.sources.psxt_ws klines KSE100 --timeframe 1d
    python -m pakfindata.sources.psxt_ws klines AGHA-MAY --timeframe 5m

    # Pull all REG symbols (default)
    python -m pakfindata.sources.psxt_ws klines --all --timeframe 1m

    # Pull all markets (REG + FUT + IDX)
    python -m pakfindata.sources.psxt_ws klines --all --markets REG FUT IDX --timeframe 1m

    # Pull only futures or only indices
    python -m pakfindata.sources.psxt_ws klines --all --markets FUT --timeframe 5m
    python -m pakfindata.sources.psxt_ws klines --all --markets IDX --timeframe 1m

    # Date filtering (server returns up to 2000 bars; we trim to date range)
    python -m pakfindata.sources.psxt_ws klines --all --timeframe 1m --date 2026-04-29
    python -m pakfindata.sources.psxt_ws klines --all --from 2026-04-27 --to 2026-04-30

    # Subscribe to live market data (streams until Ctrl+C)
    python -m pakfindata.sources.psxt_ws live HUBC OGDC
    python -m pakfindata.sources.psxt_ws live --all
    python -m pakfindata.sources.psxt_ws live --all --markets REG FUT IDX

Output:
    By default, klines saved to ~/psxdata_rescue/intraday/psxt_ws/{tag}_{tf}.csv
    Override with --out PATH

Requirements:
    pip install msgpack websockets requests
"""

import argparse
import asyncio
import csv
import json
import sys
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

DEFAULT_OUT_DIR = Path.home() / "psxdata_rescue" / "intraday" / "psxt_ws"
DEFAULT_TIMEFRAME = "1m"

VALID_TIMEFRAMES = {"1m", "5m", "15m", "1h", "4h", "1d", "1w", "1M"}
VALID_MARKETS = {"REG", "FUT", "IDX"}
DEFAULT_MARKETS = ["REG"]  # Conservative default; opt into FUT/IDX explicitly

PER_SYMBOL_TIMEOUT = 10
MAX_WS_MESSAGE_SIZE = 50 * 1024 * 1024


# ════════════════════════════════════════════════════════════════
# CONNECTION HELPERS
# ════════════════════════════════════════════════════════════════

def get_token() -> str:
    """Fetch a fresh connection token from the init endpoint."""
    r = requests.get(INIT_URL, timeout=10)
    r.raise_for_status()
    return r.json()["token"]


async def open_connection():
    """Open an authenticated WebSocket. Returns (ws, client_id)."""
    token = get_token()
    url = WS_URL_TEMPLATE.format(token=token)
    ws = await websockets.connect(url, open_timeout=15, max_size=MAX_WS_MESSAGE_SIZE)
    welcome_raw = await asyncio.wait_for(ws.recv(), timeout=10)
    welcome = msgpack.unpackb(welcome_raw, raw=False)
    return ws, welcome.get("clientId", "?")


async def request(ws, payload: dict) -> dict:
    """Send a JSON command and decode the next msgpack response."""
    await ws.send(json.dumps(payload))
    raw = await asyncio.wait_for(ws.recv(), timeout=PER_SYMBOL_TIMEOUT)
    return msgpack.unpackb(raw, raw=False)


def filter_symbols_by_market(symbols: list[dict], markets: list[str]) -> list[dict]:
    """Filter a symbol list to only the requested markets."""
    market_set = set(markets)
    return [s for s in symbols if s.get("market") in market_set]


# ════════════════════════════════════════════════════════════════
# COMMANDS
# ════════════════════════════════════════════════════════════════

async def cmd_ping():
    """Get token, connect, print welcome, disconnect."""
    print("Fetching token...")
    token = get_token()
    print(f"  Token: {token}")
    print(f"\nConnecting to {WS_URL_TEMPLATE.format(token=token[:8] + '...')}")
    ws, client_id = await open_connection()
    print(f"  ✅ Connected. clientId: {client_id}")
    await ws.close()
    print("  Disconnected cleanly.")


async def cmd_symbols(markets: list[str], out_path: Path | None):
    """Fetch full symbol list, optionally filter by market."""
    ws, client_id = await open_connection()
    print(f"Connected as {client_id}. Requesting symbol list...")
    resp = await request(ws, {"type": "getSymbols", "requestId": 1})
    await ws.close()

    all_symbols = resp.get("symbols", [])
    print(f"\nServer returned {len(all_symbols)} total symbols")

    by_market = Counter(s.get("market", "?") for s in all_symbols)
    print(f"\nBy market (server-side):")
    for m, c in by_market.most_common():
        marker = " ✓" if m in markets else ""
        print(f"  {m}: {c}{marker}")

    filtered = filter_symbols_by_market(all_symbols, markets)
    print(f"\nFiltered to markets {markets}: {len(filtered)} symbols")

    if out_path:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["symbol", "market"])
            for s in filtered:
                w.writerow([s["symbol"], s.get("market", "")])
        print(f"\nSaved to: {out_path}")


def _bar_in_date_range(bar_ts_ms: int, from_date, to_date) -> bool:
    if from_date is None and to_date is None:
        return True
    bar_date = datetime.fromtimestamp(bar_ts_ms / 1000, tz=PKT).date()
    if from_date and bar_date < from_date:
        return False
    if to_date and bar_date > to_date:
        return False
    return True


async def cmd_klines(
    symbol: str | None,
    timeframe: str,
    pull_all: bool,
    markets: list[str],
    from_date,
    to_date,
    out_path: Path,
):
    """Fetch historical klines for one symbol or many."""
    ws, client_id = await open_connection()
    print(f"Connected as {client_id}")

    if pull_all:
        print(f"Fetching symbol list (markets: {', '.join(markets)})...")
        resp = await request(ws, {"type": "getSymbols", "requestId": 1})
        all_symbols = resp.get("symbols", [])
        filtered = filter_symbols_by_market(all_symbols, markets)
        symbols = [s["symbol"] for s in filtered]
        by_market = Counter(s.get("market", "?") for s in filtered)
        print(f"  {len(symbols)} symbols across {len(by_market)} markets:")
        for m, c in by_market.most_common():
            print(f"    {m}: {c}")
    else:
        symbols = [symbol]

    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"\nWriting to: {out_path}")

    total_bars = 0
    kept_bars = 0
    succeeded = 0
    failed_symbols: list[str] = []

    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "symbol", "market", "timestamp_ms", "datetime_pkt",
            "open", "high", "low", "close",
            "volume", "quoteVolume", "trades", "interval",
        ])

        for i, sym in enumerate(symbols, 1):
            try:
                resp = await request(ws, {
                    "type": "klines",
                    "symbol": sym,
                    "timeframe": timeframe,
                    "requestId": i + 1000,
                })
                bars = resp.get("klines", [])
                total_bars += len(bars)

                kept_for_symbol = 0
                for b in bars:
                    ts_ms = int(b["timestamp"])
                    if not _bar_in_date_range(ts_ms, from_date, to_date):
                        continue
                    dt = datetime.fromtimestamp(ts_ms / 1000, tz=PKT)
                    w.writerow([
                        b["symbol"], b.get("market", ""), ts_ms,
                        dt.strftime("%Y-%m-%d %H:%M:%S"),
                        b["open"], b["high"], b["low"], b["close"],
                        b["volume"], b.get("quoteVolume", 0), b.get("trades", 0),
                        b.get("interval", timeframe),
                    ])
                    kept_for_symbol += 1
                kept_bars += kept_for_symbol
                succeeded += 1

                if pull_all and i % 25 == 0:
                    print(f"  [{i}/{len(symbols)}] {succeeded} ok, "
                          f"{kept_bars:,}/{total_bars:,} bars kept")
            except Exception as e:
                failed_symbols.append(sym)
                if not pull_all:
                    print(f"  ❌ {sym}: {type(e).__name__}: {str(e)[:120]}")

    await ws.close()

    print(f"\n✅ Done")
    print(f"   Symbols processed: {succeeded}/{len(symbols)}")
    if failed_symbols:
        head = ", ".join(failed_symbols[:10])
        more = f" ...+{len(failed_symbols)-10} more" if len(failed_symbols) > 10 else ""
        print(f"   Failed ({len(failed_symbols)}): {head}{more}")
    print(f"   Total bars received: {total_bars:,}")
    if from_date or to_date:
        print(f"   Bars kept (date filter): {kept_bars:,}")
    print(f"   Output: {out_path}")


async def cmd_live(symbols: list[str] | None, pull_all: bool, markets: list[str]):
    """Subscribe to live market data and stream to stdout."""
    ws, client_id = await open_connection()
    print(f"Connected as {client_id}")

    if pull_all:
        resp = await request(ws, {"type": "getSymbols", "requestId": 1})
        all_symbols = resp.get("symbols", [])
        filtered = filter_symbols_by_market(all_symbols, markets)
        symbols = [s["symbol"] for s in filtered]
        print(f"Subscribing to {len(symbols)} symbols across {markets}...")
    else:
        print(f"Subscribing to: {', '.join(symbols)}")

    await ws.send(json.dumps({
        "type": "subscribe",
        "subscriptionType": "marketData",
        "params": {"symbols": symbols},
        "requestId": 999,
    }))

    sub_resp = msgpack.unpackb(await ws.recv(), raw=False)
    if sub_resp.get("status") != "success":
        print(f"Subscribe failed: {sub_resp}")
        await ws.close()
        return
    print(f"  ✅ Subscribed (key: {sub_resp.get('subscriptionKey')})")
    print(f"\nStreaming live data — Ctrl+C to stop\n")

    try:
        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=30)
            except asyncio.TimeoutError:
                continue
            d = msgpack.unpackb(raw, raw=False)
            mtype = d.get("type")
            if mtype == "ping":
                await ws.send(json.dumps({
                    "type": "pong",
                    "timestamp": int(datetime.now().timestamp() * 1000),
                }))
                continue
            if mtype in ("marketData", "tick", "kline"):
                sym = d.get("symbol", "?")
                price = d.get("price") or d.get("close") or "?"
                vol = d.get("volume", 0)
                ts = d.get("timestamp", 0)
                if ts:
                    dt = datetime.fromtimestamp(ts / 1000, tz=PKT).strftime("%H:%M:%S")
                else:
                    dt = "?"
                print(f"  {dt} {sym:>10s} {price:>10}  vol={vol}")
            else:
                print(f"  📩 [{mtype}] {str(d)[:200]}")
    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        await ws.close()


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
        description="PSX Terminal WebSocket backfill & streaming tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Valid timeframes: {' '.join(sorted(VALID_TIMEFRAMES))}
Valid markets:    {' '.join(sorted(VALID_MARKETS))}

Examples:
  python -m pakfindata.sources.psxt_ws ping
  python -m pakfindata.sources.psxt_ws symbols
  python -m pakfindata.sources.psxt_ws symbols --markets REG FUT IDX
  python -m pakfindata.sources.psxt_ws klines HUBC --timeframe 1m
  python -m pakfindata.sources.psxt_ws klines KSE100 --timeframe 1d
  python -m pakfindata.sources.psxt_ws klines --all --timeframe 1m
  python -m pakfindata.sources.psxt_ws klines --all --markets REG FUT IDX --timeframe 1m
  python -m pakfindata.sources.psxt_ws klines --all --markets IDX --timeframe 1m
  python -m pakfindata.sources.psxt_ws klines --all --date 2026-04-29
  python -m pakfindata.sources.psxt_ws live HUBC OGDC
  python -m pakfindata.sources.psxt_ws live --all --markets REG FUT IDX
        """,
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("ping", help="Test connection")

    p_sym = sub.add_parser("symbols", help="Fetch symbol list")
    p_sym.add_argument(
        "--markets", nargs="+", choices=sorted(VALID_MARKETS),
        default=DEFAULT_MARKETS,
        help=f"Markets to include (default: {' '.join(DEFAULT_MARKETS)})",
    )
    p_sym.add_argument("--out", type=Path, default=None,
                       help="Save symbol list to CSV")

    p_kl = sub.add_parser("klines", help="Fetch historical OHLCV klines")
    g = p_kl.add_mutually_exclusive_group(required=True)
    g.add_argument("symbol", nargs="?", help="Single symbol (e.g. HUBC)")
    g.add_argument("--all", action="store_true", help="Fetch all symbols in --markets")
    p_kl.add_argument("--timeframe", "-t", default=DEFAULT_TIMEFRAME,
                      choices=sorted(VALID_TIMEFRAMES),
                      help=f"Bar timeframe (default: {DEFAULT_TIMEFRAME})")
    p_kl.add_argument(
        "--markets", nargs="+", choices=sorted(VALID_MARKETS),
        default=DEFAULT_MARKETS,
        help=f"Markets to include with --all (default: {' '.join(DEFAULT_MARKETS)})",
    )
    p_kl.add_argument("--date", type=parse_date,
                      help="Filter bars to a specific date (YYYY-MM-DD)")
    p_kl.add_argument("--from", dest="from_date", type=parse_date,
                      help="Filter bars from this date onwards")
    p_kl.add_argument("--to", dest="to_date", type=parse_date,
                      help="Filter bars up to this date (inclusive)")
    p_kl.add_argument("--out", type=Path,
                      help="Output CSV path")

    p_live = sub.add_parser("live", help="Subscribe to live market data")
    g2 = p_live.add_mutually_exclusive_group(required=True)
    g2.add_argument("symbols", nargs="*", default=[], help="Symbols to subscribe")
    g2.add_argument("--all", action="store_true", help="Subscribe to all in --markets")
    p_live.add_argument(
        "--markets", nargs="+", choices=sorted(VALID_MARKETS),
        default=DEFAULT_MARKETS,
        help=f"Markets to include with --all (default: {' '.join(DEFAULT_MARKETS)})",
    )

    args = parser.parse_args()

    if args.cmd == "ping":
        asyncio.run(cmd_ping())

    elif args.cmd == "symbols":
        asyncio.run(cmd_symbols(args.markets, args.out))

    elif args.cmd == "klines":
        from_date = args.date or args.from_date
        to_date = args.date or args.to_date

        if args.out:
            out_path = args.out
        else:
            if args.all:
                tag = "all_" + "_".join(sorted(args.markets)).lower()
            else:
                tag = args.symbol
            date_tag = ""
            if from_date and to_date and from_date == to_date:
                date_tag = f"_{from_date}"
            elif from_date or to_date:
                date_tag = f"_{from_date or 'start'}_{to_date or 'now'}"
            out_path = DEFAULT_OUT_DIR / f"{tag}_{args.timeframe}{date_tag}.csv"

        asyncio.run(cmd_klines(
            symbol=args.symbol,
            timeframe=args.timeframe,
            pull_all=args.all,
            markets=args.markets,
            from_date=from_date,
            to_date=to_date,
            out_path=out_path,
        ))

    elif args.cmd == "live":
        if args.all:
            asyncio.run(cmd_live(symbols=None, pull_all=True, markets=args.markets))
        else:
            if not args.symbols:
                parser.error("Must specify symbols or --all")
            asyncio.run(cmd_live(symbols=args.symbols, pull_all=False, markets=args.markets))


if __name__ == "__main__":
    main()
