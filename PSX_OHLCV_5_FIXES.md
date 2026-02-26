# Claude Code Prompt: PSX OHLCV — 5 Fixes in One

## File: services/tick_service.py (or src/psx_ohlcv/services/tick_service.py)

Find the correct path first:
```bash
find ~/psx_ohlcv -name "tick_service.py" -type f
```

Apply ALL 5 fixes below to that file.

---

## FIX 1: Snapshot write — Remove atomic rename (CRITICAL)

The snapshot write fails on WSL2 Windows mount (/mnt/e/) because os.rename 
is unreliable on /mnt/ drives:

  Snapshot write failed: [Errno 2] No such file or directory: 
  '/mnt/e/psxdata/live_snapshot.tmp' -> '/mnt/e/psxdata/live_snapshot.json'

Find the write_snapshot() method. It currently writes to a .tmp file then 
renames. The current code at the end of write_snapshot() already shows 
direct write:
```python
with open(str(SNAPSHOT_PATH), "w") as f:
    json.dump(snapshot, f, default=str)
```

Search the ENTIRE file for ANY remaining references to:
- ".tmp"
- "os.rename" 
- "snapshot.tmp"
- "rename("

Remove or replace ALL of them. There may be old code paths or fallback 
code that still uses the tmp+rename pattern. Search thoroughly:
```bash
grep -n "\.tmp\|rename\|atomic" <filepath>
```

Every snapshot write must be direct write — no .tmp, no rename, anywhere.

---

## FIX 2: Crash-safe checkpoints (CRITICAL)

Currently ALL data is in RAM until EOD flush at 15:35 PKT. If process 
crashes, entire day's data is lost. Add periodic checkpoints.

Add this method to the collector class:

```python
def _checkpoint_flush(self):
    """Write current data to tick_bars.db without clearing memory.
    Called every 30 minutes during market hours as crash protection."""
    
    if not self.completed_bars and not self.raw_ticks:
        return
    
    db_path = DATA_ROOT / "tick_bars.db"
    con = sqlite3.connect(str(db_path))
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    
    # Create tables if needed (same CREATE TABLE IF NOT EXISTS as eod_flush)
    self._ensure_tables(con)
    
    # Insert with OR IGNORE to handle duplicates from previous checkpoints
    bar_count = 0
    tick_count = 0
    
    if self.completed_bars:
        # Use same insert logic as eod_flush but with INSERT OR IGNORE
        for bar in self.completed_bars:
            try:
                con.execute(
                    "INSERT OR IGNORE INTO ohlcv_5s VALUES (?,?,?,?,?,?,?,?,?)",
                    (bar["symbol"], bar.get("market","REG"), bar["timestamp"],
                     bar["open"], bar["high"], bar["low"], bar["close"],
                     bar.get("volume",0), bar.get("trades",0))
                )
                bar_count += 1
            except Exception:
                pass
    
    if self.raw_ticks:
        for t in self.raw_ticks:
            try:
                con.execute(
                    "INSERT OR IGNORE INTO raw_ticks VALUES (?,?,?,?,?,?,?,?,?)",
                    (t["symbol"], t.get("market","REG"), t.get("timestamp",0),
                     t.get("price",0), t.get("volume",0),
                     t.get("bid",0), t.get("ask",0),
                     t.get("bidVol",0), t.get("askVol",0))
                )
                tick_count += 1
            except Exception:
                pass
    
    # Index tables too
    idx_bars = [b for b in self.completed_bars if b.get("market") == "IDX"]
    for b in idx_bars:
        try:
            con.execute(
                "INSERT OR IGNORE INTO index_ohlcv_5s VALUES (?,?,?,?,?,?,?,?)",
                (b["symbol"], b["timestamp"],
                 b["open"], b["high"], b["low"], b["close"],
                 b.get("volume",0), b.get("turnover",0))
            )
        except Exception:
            pass
    
    for t in getattr(self, 'index_ticks', []):
        try:
            con.execute(
                "INSERT OR IGNORE INTO index_raw_ticks VALUES (?,?,?,?,?,?)",
                (t["symbol"], t.get("timestamp",0),
                 t.get("value", t.get("price",0)), t.get("change",0),
                 t.get("changePercent",0), t.get("volume",0))
            )
        except Exception:
            pass
    
    con.commit()
    con.close()
    
    print(f"💾 Checkpoint: {bar_count:,} bars, {tick_count:,} ticks saved (memory NOT cleared)")
```

Add UNIQUE constraints to _ensure_tables() for all 4 tables:
- ohlcv_5s: UNIQUE(symbol, market, ts)
- raw_ticks: UNIQUE(symbol, market, ts, price) 
- index_ohlcv_5s: UNIQUE(symbol, ts)
- index_raw_ticks: UNIQUE(symbol, ts, value)

Add checkpoint tracking to __init__:
```python
self._last_checkpoint = time.time()
```

In the main loop, add checkpoint check (near where write_snapshot is called):
```python
# Checkpoint every 30 minutes
if time.time() - self._last_checkpoint >= 1800:
    self._checkpoint_flush()
    self._last_checkpoint = time.time()
```

DO NOT clear memory after checkpoint — data stays in RAM for live display.
EOD flush still does final cleanup.

---

## FIX 3: Remove CSF from MARKETS

PSX Terminal WebSocket does not support CSF (Cash Settled Futures).
It returns: {"type":"error","message":"Invalid market type: CSF"}

Find the MARKETS list/tuple and remove "CSF":

```python
# Change FROM:
MARKETS = ["REG", "FUT", "ODL", "BNB", "CSF", "IDX"]
# Change TO:
MARKETS = ["REG", "FUT", "ODL", "BNB", "IDX"]  # CSF not yet supported by psxterminal.com
```

---

## FIX 4: Sleep until exact 9:15 AM PKT

Currently the overnight sleep loops in 600s chunks. Replace with 
precise sleep targeting next trading day 9:15 AM.

Find the sleep/reconnect logic after market close. Replace with:

```python
def _sleep_until_next_session(self):
    """Sleep until 9:15 AM PKT on the next trading day."""
    now = datetime.now(PKT)
    next_day = now + timedelta(days=1)
    # Skip weekends (Saturday=5, Sunday=6)
    while next_day.weekday() >= 5:
        next_day += timedelta(days=1)
    target = next_day.replace(hour=9, minute=15, second=0, microsecond=0)
    sleep_seconds = (target - now).total_seconds()
    if sleep_seconds > 0:
        print(f"💤 Sleeping until {target.strftime('%A %Y-%m-%d %H:%M')} PKT ({sleep_seconds/3600:.1f} hours)")
        time.sleep(sleep_seconds)
```

Call this method instead of the current sleep loop after EOD flush.

---

## FIX 5: Add emoji to console output

Add emoji prefixes to all major console output lines:

- Startup: `🚀 PSX Tick Collector (memory mode) — PID {pid}`
- Relay: `📡 WS relay on ws://0.0.0.0:8765`  
- Connected: `🔗 Connected to wss://psxterminal.com/`
- Subscribed: `✅ Subscribed: REG FUT ODL BNB IDX`
- Status line: `⚡ Ticks: {n} | Bars: {n} | Symbols: {n} | Indices: {n} | WS Clients: {n} | RAM: {n} MB`
- Checkpoint: `💾 Checkpoint: {n} bars, {n} ticks saved (memory NOT cleared)`
- Market close: `🔔 Market closed — preparing EOD flush`
- EOD flush: `📊 EOD flush: {n} bars, {n} ticks, {n} index ticks`
- EOD complete: `✅ EOD complete → {db_path}`
- Sleep: `💤 Sleeping until {date} PKT ({hours} hours)`
- Disconnect: `⚠️ Disconnected. Reconnecting in {n}s...`
- Shutdown: `🛑 Shutting down...`

Only add emoji where they don't already exist. Some lines may already have them.

---

## VERIFY

```bash
# Check all fixes applied:
echo "=== FIX 1: No .tmp or rename ==="
grep -n "\.tmp\|os\.rename\|rename(" /home/adnoman/psx_ohlcv/src/psx_ohlcv/services/tick_service.py || echo "CLEAN ✅"

echo "=== FIX 2: Checkpoint exists ==="
grep -n "checkpoint_flush\|_last_checkpoint\|1800" /home/adnoman/psx_ohlcv/src/psx_ohlcv/services/tick_service.py

echo "=== FIX 3: No CSF ==="
grep -n "CSF" /home/adnoman/psx_ohlcv/src/psx_ohlcv/services/tick_service.py || echo "CLEAN ✅"

echo "=== FIX 4: Sleep until 9:15 ==="
grep -n "sleep_until\|9.*15\|weekday" /home/adnoman/psx_ohlcv/src/psx_ohlcv/services/tick_service.py

echo "=== FIX 5: Emoji ==="
grep -n "🚀\|📡\|🔗\|⚡\|💾\|🔔\|📊\|💤\|🛑" /home/adnoman/psx_ohlcv/src/psx_ohlcv/services/tick_service.py

echo "=== Import check ==="
python -c "from psx_ohlcv.services.tick_service import TickCollectorService; print('OK')" 2>&1 || \
python -c "import importlib.util; spec = importlib.util.spec_from_file_location('ts', '$(find ~/psx_ohlcv -name tick_service.py -type f)'); print('File found OK')"
```
