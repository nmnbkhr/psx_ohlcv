#!/usr/bin/env python3
"""
Build OHLCV candles (1m, 5m, 15m, 30m, 1h) from tick JSONL files.

Reads tick_logs_cloud JSONL files, aggregates into OHLCV bars per symbol,
and writes one CSV per date+timeframe.

Output: /mnt/e/psxdata/ohlcv/psx-{date}-{tf}-ohlcv.csv

Usage:
    python scripts/build_ohlcv_from_ticks.py              # all dates, all timeframes
    python scripts/build_ohlcv_from_ticks.py 2026-04-02   # one date
"""

import json, csv, sys
from pathlib import Path
from datetime import datetime, timezone, timedelta
from collections import defaultdict

TICK_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
OUT_DIR = Path("/mnt/e/psxdata/ohlcv")
PKT = timezone(timedelta(hours=5))

# Skip index symbols
INDEX_SYMBOLS = {
    "ALLSHR", "KSE100", "KSE100PR", "KSE30", "KMI30", "KMIALLSHR",
    "BKTI", "OGTI", "PSXDIV20", "UPP9", "NITPGI", "NBPPGI", "MZNPI",
    "JSMFI", "ACI", "JSGBKTI", "HBLTTI", "MII30"
}

TIMEFRAMES = {
    "1m": 1,
    "5m": 5,
    "15m": 15,
    "30m": 30,
    "1h": 60,
    "1d": 1440,
}


def floor_minute(dt, interval_min):
    """Floor a datetime to the nearest interval."""
    total_min = dt.hour * 60 + dt.minute
    floored = (total_min // interval_min) * interval_min
    return dt.replace(hour=floored // 60, minute=floored % 60, second=0, microsecond=0)


def load_ticks(filepath):
    """Load ticks from JSONL, skip indices, return sorted by (symbol, timestamp)."""
    ticks = []
    errors = 0
    with open(filepath) as f:
        for line in f:
            try:
                d = json.loads(line)
                if d["symbol"] in INDEX_SYMBOLS:
                    continue
                if d.get("market") == "IDX":
                    continue
                ticks.append(d)
            except Exception:
                errors += 1
    if errors:
        print(f"    {errors} parse errors skipped")
    ticks.sort(key=lambda t: (t["symbol"], t["timestamp"]))
    return ticks


def build_bars(ticks, interval_min):
    """
    Aggregate ticks into OHLCV bars.
    Volume is cumulative in ticks — compute per-bar volume from diff.
    """
    # Group ticks by symbol
    by_symbol = defaultdict(list)
    for t in ticks:
        by_symbol[t["symbol"]].append(t)

    bars = []
    for sym, sym_ticks in by_symbol.items():
        # Group by bar period
        bar_ticks = defaultdict(list)
        for t in sym_ticks:
            dt = datetime.fromtimestamp(t["timestamp"], PKT)
            bar_key = floor_minute(dt, interval_min)
            bar_ticks[bar_key].append(t)

        # Track previous cumulative volume for delta
        prev_cum_vol = None

        for bar_time in sorted(bar_ticks.keys()):
            bt = bar_ticks[bar_time]

            o = bt[0]["price"]
            c = bt[-1]["price"]
            h = max(t["price"] for t in bt)
            l = min(t["price"] for t in bt)

            # Volume: cumulative → per-bar delta
            cum_vol_start = bt[0]["volume"]
            cum_vol_end = bt[-1]["volume"]

            if prev_cum_vol is not None and cum_vol_end >= prev_cum_vol:
                bar_vol = cum_vol_end - prev_cum_vol
            else:
                # First bar or reset — use diff within bar
                bar_vol = max(0, cum_vol_end - cum_vol_start)

            prev_cum_vol = cum_vol_end

            # Trades delta (also cumulative)
            trades = bt[-1].get("trades", 0) - bt[0].get("trades", 0)
            trades = max(0, trades)

            bars.append({
                "symbol": sym,
                "datetime": bar_time.strftime("%Y-%m-%d %H:%M:%S"),
                "open": round(o, 4),
                "high": round(h, 4),
                "low": round(l, 4),
                "close": round(c, 4),
                "volume": bar_vol,
                "trades": trades,
            })

    bars.sort(key=lambda b: (b["symbol"], b["datetime"]))
    return bars


def write_csv(bars, filepath):
    with open(filepath, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "datetime", "open", "high", "low", "close", "volume", "trades"])
        w.writeheader()
        w.writerows(bars)


def process_date(tick_file):
    """Process one JSONL file into all timeframes."""
    date_str = tick_file.stem.replace("ticks_", "")
    print(f"\n  {date_str}: loading ticks...")

    ticks = load_ticks(tick_file)
    if not ticks:
        print(f"    No ticks found, skipping")
        return

    symbols = len(set(t["symbol"] for t in ticks))
    print(f"    {len(ticks):,} ticks, {symbols} symbols")

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    for tf_name, interval in TIMEFRAMES.items():
        bars = build_bars(ticks, interval)
        if bars:
            fname = f"psx-{date_str}-{tf_name}-ohlcv.csv"
            write_csv(bars, OUT_DIR / fname)
            print(f"    {tf_name}: {len(bars):,} bars → {fname}")


def main():
    # Find all tick files
    tick_files = sorted(TICK_DIR.glob("ticks_*.jsonl"))

    if not tick_files:
        print("No tick files found")
        return

    # Filter by date if specified
    if len(sys.argv) > 1:
        date_filter = sys.argv[1]
        tick_files = [f for f in tick_files if date_filter in f.name]
        if not tick_files:
            print(f"No tick file found for {date_filter}")
            return

    print(f"{'='*60}")
    print(f"  BUILD OHLCV FROM TICKS — {len(tick_files)} dates")
    print(f"  Timeframes: {', '.join(TIMEFRAMES.keys())}")
    print(f"  Output: {OUT_DIR}")
    print(f"{'='*60}")

    for tf in tick_files:
        process_date(tf)

    print(f"\n{'='*60}")
    print(f"  DONE — files in {OUT_DIR}")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
