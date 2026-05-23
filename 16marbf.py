import requests, csv, json, time
from datetime import datetime, timezone, timedelta

PKT = timezone(timedelta(hours=5))
BASE = "https://psxterminal.com/api"

# March 16, 2026 market hours
start = datetime(2026, 3, 16, 9, 15, 0, tzinfo=PKT)
end   = datetime(2026, 3, 16, 15, 30, 0, tzinfo=PKT)
start_ms = int(start.timestamp() * 1000)
end_ms   = int(end.timestamp() * 1000)

DATE = "2026-03-16"
print(f"Fetching 1m klines for {DATE}")
print(f"Start: {start_ms}  End: {end_ms}")

# Get symbols
syms = requests.get(f"{BASE}/symbols", timeout=10).json()["data"]
skip = {"ALLSHR","KSE100","KSE100PR","KSE30","KMI30","KMIALLSHR",
        "BKTI","OGTI","PSXDIV20","UPP9","NITPGI","NBPPGI","MZNPI",
        "JSMFI","ACI","JSGBKTI","HBLTTI","MII30"}
syms = [s for s in syms if s not in skip]
print(f"{len(syms)} symbols\n")

all_bars = []
for i, sym in enumerate(syms, 1):
    sym_bars = []
    ts = start_ms
    pages = 0

    while True:
        try:
            r = requests.get(
                f"{BASE}/klines/{sym}/1m?limit=100&startTimestamp={ts}&endTimestamp={end_ms}",
                timeout=15
            )
            if r.status_code != 200:
                break
            text = r.text.split("<")[0]
            d = json.loads(text)
            if not d.get("success") or not d.get("data"):
                break

            batch = d["data"]
            # Filter only March 16
            batch = [b for b in batch if start_ms <= b["timestamp"] <= end_ms]
            sym_bars.extend(batch)
            pages += 1

            if len(d["data"]) < 100:
                break

            # Paginate forward
            ts = max(b["timestamp"] for b in d["data"]) + 1
            if ts > end_ms:
                break
            time.sleep(0.2)
        except Exception as e:
            break

    # Deduplicate
    seen = set()
    for b in sym_bars:
        key = (b["symbol"], b["timestamp"])
        if key not in seen:
            seen.add(key)
            all_bars.append(b)

    if i % 25 == 0:
        print(f"  [{i}/{len(syms)}] {len(all_bars):,} bars (last: {sym} got {len(sym_bars)} in {pages} pages)")
    time.sleep(0.3)

# Write CSV
outfile = f"redo_psxt_{DATE}_1m.csv"
with open(outfile, "w", newline="") as f:
    w = csv.writer(f)
    w.writerow(["symbol","timestamp","datetime","open","high","low","close","volume","timeframe"])
    for b in sorted(all_bars, key=lambda x: (x["symbol"], x["timestamp"])):
        dt = datetime.fromtimestamp(b["timestamp"]/1000, PKT).strftime("%Y-%m-%d %H:%M:%S")
        w.writerow([b["symbol"], b["timestamp"], dt, b["open"], b["high"], b["low"], b["close"], b["volume"], "1m"])

print(f"\n✅ {outfile}: {len(all_bars):,} rows")
print(f"   Symbols with data: {len(set(b['symbol'] for b in all_bars))}")

# Quick stats
if all_bars:
    times = [datetime.fromtimestamp(b["timestamp"]/1000, PKT) for b in all_bars]
    print(f"   Time range: {min(times).strftime('%H:%M')} → {max(times).strftime('%H:%M')}")
