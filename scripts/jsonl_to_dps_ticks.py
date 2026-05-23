#!/usr/bin/env python3
"""Convert tick JSONL → dps_ticks_{date}.csv format expected by Intraday Quant Lab.

The original `dps_ticks_*.csv` files were produced by `psx_market_data.py ticks` on
the cloud VM during the trading day (since the DPS API only returns "today"). For
historical dates the only source of truth is the JSONL stream. Schemas match in
columns — row counts differ because DPS includes idle 10s polling rows while JSONL
is event-driven.

Reads:  /mnt/e/psxdata/tick_logs_cloud/ticks_{date}.jsonl
Writes: ~/projects/psxdata/intraday/dps_ticks_{date}.csv
        with header: symbol,timestamp,datetime,price,volume

Skips IDX market and known index symbols (matching build_ohlcv_from_ticks.py).

Usage:
    python scripts/jsonl_to_dps_ticks.py 2026-04-24
    python scripts/jsonl_to_dps_ticks.py --all-missing
"""
import argparse
import csv
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

JSONL_DIR = Path("/mnt/e/psxdata/tick_logs_cloud")
OUT_DIR = Path.home() / "projects" / "psxdata" / "intraday"
PKT = timezone(timedelta(hours=5))

INDEX_SYMBOLS = {
    "ALLSHR", "KSE100", "KSE100PR", "KSE30", "KMI30", "KMIALLSHR",
    "BKTI", "OGTI", "PSXDIV20", "UPP9", "NITPGI", "NBPPGI", "MZNPI",
    "JSMFI", "ACI", "JSGBKTI", "HBLTTI", "MII30",
}


def convert(date_str: str) -> int:
    src = JSONL_DIR / f"ticks_{date_str}.jsonl"
    if not src.exists():
        print(f"  SKIP {date_str}: no JSONL at {src}")
        return 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out = OUT_DIR / f"dps_ticks_{date_str}.csv"
    written = 0
    skipped = 0

    with open(src) as f, open(out, "w", newline="") as g:
        w = csv.writer(g)
        w.writerow(["symbol", "timestamp", "datetime", "price", "volume"])
        for line in f:
            try:
                d = json.loads(line)
            except Exception:
                skipped += 1
                continue
            sym = d.get("symbol")
            if not sym or sym in INDEX_SYMBOLS or d.get("market") == "IDX":
                continue
            ts = d.get("timestamp")
            price = d.get("price")
            vol = d.get("volume", 0)
            if ts is None or price is None:
                skipped += 1
                continue
            ts_int = int(float(ts))
            dt_str = datetime.fromtimestamp(ts_int, PKT).strftime("%Y-%m-%d %H:%M:%S")
            w.writerow([sym, ts_int, dt_str, price, int(vol or 0)])
            written += 1

    print(f"  {date_str}: {written:,} rows → {out.name}  ({skipped} skipped)")
    return written


def find_missing() -> list[str]:
    jsonl_dates = {p.stem.replace("ticks_", "") for p in JSONL_DIR.glob("ticks_*.jsonl")}
    csv_dates = {p.stem.replace("dps_ticks_", "") for p in OUT_DIR.glob("dps_ticks_*.csv")}
    return sorted(jsonl_dates - csv_dates)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("date", nargs="?", help="YYYY-MM-DD; omit if --all-missing")
    ap.add_argument("--all-missing", action="store_true")
    args = ap.parse_args()

    if args.all_missing:
        dates = find_missing()
        if not dates:
            print("No missing dates.")
            return
        print(f"Converting {len(dates)} missing dates: {dates}")
        for d in dates:
            convert(d)
    elif args.date:
        convert(args.date)
    else:
        print("Need a date arg or --all-missing", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
