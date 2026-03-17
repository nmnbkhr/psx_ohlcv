"""Backfill today's intraday data (1d, 1h, 5m) from PSX Terminal API."""

import csv
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

BASE = "https://psxterminal.com/api"
PKT = timezone(timedelta(hours=5))
today = datetime.now(PKT)
date_str = today.strftime("%Y-%m-%d")

# Output folder
OUT = Path("/mnt/e/psxdata/intraday")
OUT.mkdir(parents=True, exist_ok=True)

# Today's market open
start_ts = int(
    today.replace(hour=9, minute=15, second=0, microsecond=0).timestamp() * 1000
)

# Get symbols
print("Fetching symbols...")
syms = requests.get(f"{BASE}/symbols", timeout=15).json()["data"]
skip = {
    "ALLSHR", "KSE100", "KSE100PR", "KSE30", "KMI30", "KMIALLSHR",
    "BKTI", "OGTI", "PSXDIV20", "UPP9", "NITPGI", "NBPPGI", "MZNPI",
    "JSMFI", "ACI", "JSGBKTI", "HBLTTI", "MII30",
}
syms = [s for s in syms if s not in skip]
print(f"{len(syms)} symbols")

# Collect all bars
all_1d = []
all_1h = []
all_5m = []

for i, sym in enumerate(syms, 1):
    try:
        # Daily
        r = requests.get(f"{BASE}/klines/{sym}/1d?limit=1", timeout=10)
        if r.status_code == 200:
            d = r.json()
            if d.get("success") and d.get("data"):
                all_1d.extend(d["data"])

        # Hourly — paginate from open
        ts = start_ts
        while True:
            r = requests.get(
                f"{BASE}/klines/{sym}/1h?limit=100&startTimestamp={ts}", timeout=10
            )
            if r.status_code != 200:
                break
            d = r.json()
            if not d.get("success") or not d.get("data"):
                break
            all_1h.extend(d["data"])
            if len(d["data"]) < 100:
                break
            ts = max(b["timestamp"] for b in d["data"]) + 1

        # 5-minute — paginate from open
        ts = start_ts
        while True:
            r = requests.get(
                f"{BASE}/klines/{sym}/5m?limit=100&startTimestamp={ts}", timeout=10
            )
            if r.status_code != 200:
                break
            d = r.json()
            if not d.get("success") or not d.get("data"):
                break
            all_5m.extend(d["data"])
            if len(d["data"]) < 100:
                break
            ts = max(b["timestamp"] for b in d["data"]) + 1

        if i % 25 == 0:
            print(f"  [{i}/{len(syms)}] 1d:{len(all_1d)} 1h:{len(all_1h)} 5m:{len(all_5m)}")
        time.sleep(0.3)
    except Exception:
        pass


# Write CSV files
def write_csv(filepath, bars):
    with open(filepath, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["symbol", "timestamp", "datetime", "open", "high", "low", "close", "volume"])
        for b in sorted(bars, key=lambda x: (x["symbol"], x["timestamp"])):
            dt = datetime.fromtimestamp(b["timestamp"] / 1000, PKT).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
            w.writerow([
                b["symbol"], b["timestamp"], dt,
                b["open"], b["high"], b["low"], b["close"], b["volume"],
            ])
    print(f"  >> {filepath.name}: {len(bars):,} rows")


write_csv(OUT / f"{date_str}_1d.csv", all_1d)
write_csv(OUT / f"{date_str}_1h.csv", all_1h)
write_csv(OUT / f"{date_str}_5m.csv", all_5m)

# Also save raw JSON for archival
with open(OUT / f"{date_str}_raw.json", "w") as f:
    json.dump({"date": date_str, "1d": all_1d, "1h": all_1h, "5m": all_5m}, f)
print(f"  >> {date_str}_raw.json: full archive")

print(f"\nDone: {len(all_1d) + len(all_1h) + len(all_5m):,} total bars")
print(f"Files saved to {OUT}/")
