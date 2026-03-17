"""
Convert 1-minute klines CSV to JSONL tick-like format.
Input:  ~/pakfindata/redo_psxt_2026-03-16_1m.csv
Output: /mnt/e/psxdata/tick_logs/ticks_2026-03-16.jsonl
"""

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

PKT = timezone(timedelta(hours=5))

CSV_INPUT = Path.home() / "pakfindata" / "redo_psxt_2026-03-16_1m.csv"
JSONL_PREV = Path("/mnt/e/psxdata/tick_logs/ticks_2026-03-13.jsonl")
JSONL_OUTPUT = Path("/mnt/e/psxdata/tick_logs/ticks_2026-03-16.jsonl")


def load_previous_close() -> dict:
    """Extract last price per symbol from March 13 JSONL as previousClose."""
    prev_close = {}
    print(f"Reading {JSONL_PREV} for previousClose...")
    with open(JSONL_PREV, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                tick = json.loads(line)
                sym = tick.get("symbol", "")
                price = tick.get("price", 0)
                if sym and price:
                    prev_close[sym] = price
            except Exception:
                continue
    print(f"  Got previousClose for {len(prev_close)} symbols")
    return prev_close


def read_csv(filepath: Path) -> list[dict]:
    rows = []
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                "symbol": r["symbol"].strip(),
                "timestamp_ms": int(r["timestamp"].strip()),
                "open": float(r["open"].strip()),
                "high": float(r["high"].strip()),
                "low": float(r["low"].strip()),
                "close": float(r["close"].strip()),
                "volume": int(r["volume"].strip()),
            })
    return rows


def convert(rows: list[dict], prev_close: dict) -> list[dict]:
    by_symbol = defaultdict(list)
    for r in rows:
        by_symbol[r["symbol"]].append(r)
    for sym in by_symbol:
        by_symbol[sym].sort(key=lambda x: x["timestamp_ms"])

    output = []

    for sym, bars in sorted(by_symbol.items()):
        pc = prev_close.get(sym, bars[0]["open"])

        day_open = bars[0]["open"]
        day_high = bars[0]["high"]
        day_low = bars[0]["low"]
        cum_volume = 0
        cum_value = 0.0

        market = "REG"

        for bar in bars:
            day_high = max(day_high, bar["high"])
            day_low = min(day_low, bar["low"])
            cum_volume += bar["volume"]

            bar_vwap = (bar["open"] + bar["high"] + bar["low"] + bar["close"]) / 4
            cum_value += bar["volume"] * bar_vwap

            price = bar["close"]
            change = round(price - pc, 4)
            change_pct = round(change / pc, 6) if pc else 0

            ts_seconds = bar["timestamp_ms"] / 1000 + 59
            dt = datetime.fromtimestamp(ts_seconds, PKT)
            iso_ts = dt.strftime("%Y-%m-%dT%H:%M:%S.000+05:00")

            record = {
                "symbol": sym,
                "market": market,
                "price": price,
                "open": day_open,
                "change": round(change, 2),
                "changePercent": round(change_pct, 5),
                "volume": cum_volume,
                "value": round(cum_value, 2),
                "trades": 0,
                "high": day_high,
                "low": day_low,
                "bid": 0.0,
                "ask": 0.0,
                "bidVol": 0,
                "askVol": 0,
                "previousClose": pc,
                "timestamp": round(ts_seconds, 3),
                "_ts": iso_ts,
                "_source": "klines_1m",
                "_bar_open": bar["open"],
                "_bar_high": bar["high"],
                "_bar_low": bar["low"],
                "_bar_close": bar["close"],
                "_bar_volume": bar["volume"],
            }
            output.append(record)

    output.sort(key=lambda x: x["timestamp"])
    return output


if __name__ == "__main__":
    prev_close = load_previous_close()

    print(f"Reading CSV: {CSV_INPUT}")
    rows = read_csv(CSV_INPUT)
    print(f"  {len(rows):,} bars, {len(set(r['symbol'] for r in rows))} symbols")

    print("Converting...")
    output = convert(rows, prev_close)

    print(f"Writing: {JSONL_OUTPUT}")
    with open(JSONL_OUTPUT, "w") as f:
        for record in output:
            f.write(json.dumps(record) + "\n")

    total_syms = len(set(rec["symbol"] for rec in output))
    with_pc = sum(1 for s in set(rec["symbol"] for rec in output) if s in prev_close)
    print(f"\n✅ {len(output):,} records, {total_syms} symbols ({with_pc} with real prevClose)")
    print(f"   File: {JSONL_OUTPUT} ({JSONL_OUTPUT.stat().st_size / 1024 / 1024:.1f} MB)")
    print(json.dumps(output[0], indent=2))
