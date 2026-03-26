# Claude Code Prompt: DB Cleanup + JSONL Fallback Migration

## Context

Two SQLite databases have grown too large:
- `tick_bars.db`: 889 MB at `/mnt/e/psxdata/tick_bars.db`
- `psx.sqlite`: 4,062 MB at `/mnt/e/psxdata/psx.sqlite`

We now collect tick data on an Oracle Cloud VM. The primary tick data source 
going forward is **cloud JSONL files** at `/mnt/e/psxdata/tick_logs_cloud/*.jsonl`.
The local `tick_service.py` is NO LONGER running.

## Step 1: Audit before changes

```bash
# Confirm tick_service is NOT running locally
ps aux | grep tick_service | grep -v grep

# Check tick_bars.db tables
sqlite3 /mnt/e/psxdata/tick_bars.db ".tables"
sqlite3 /mnt/e/psxdata/tick_bars.db ".schema raw_ticks"
sqlite3 /mnt/e/psxdata/tick_bars.db ".schema ohlcv_5s"
sqlite3 /mnt/e/psxdata/tick_bars.db ".schema index_raw_ticks"
sqlite3 /mnt/e/psxdata/tick_bars.db ".schema index_ohlcv_5s"

# Check psx.sqlite — list ALL tables with approximate sizes
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
"

# Quick size check per table (page count method — instant, no row scan)
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT 'total pages', page_count, 'page size', page_size, 
       'est MB', (page_count * page_size) / 1024 / 1024
FROM pragma_page_count(), pragma_page_size();
"

# Check JSONL cloud data availability
ls -lh /mnt/e/psxdata/tick_logs_cloud/ | head -10
wc -l /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | tail -5

# Check JSONL format (first line)
head -1 /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | head -1
```

**STOP — read ALL output before proceeding.**

## Step 2: Clean up tick_bars.db

### What to DROP (saves ~500MB):

| Table | Rows | Action | Why |
|-------|------|--------|-----|
| `raw_ticks` | 4.1M | **DROP** | Cloud JSONL replaces this, cleaner + deduplicated |

### What to KEEP:

| Table | Action | Why |
|-------|--------|-----|
| `ohlcv_5s` | **KEEP** | 5-second bars, used by tick_analytics.py, live.py API, tick_summary.py |
| `index_ohlcv_5s` | **KEEP** | Index 5-second bars, used by tick_analytics.py |
| `index_raw_ticks` | **KEEP** | Used by intraday.py for KSE100 overlay |
| `market_snapshots` | **KEEP** (if exists) | Market watch poller data |

### Execute cleanup:

```bash
# Backup first (optional — we have cloud data)
# cp /mnt/e/psxdata/tick_bars.db /mnt/e/psxdata/tick_bars_backup_$(date +%Y%m%d).db

# Drop raw_ticks
sqlite3 /mnt/e/psxdata/tick_bars.db "
DROP TABLE IF EXISTS raw_ticks;
"

# Verify it's gone
sqlite3 /mnt/e/psxdata/tick_bars.db ".tables"

# VACUUM to reclaim space
sqlite3 /mnt/e/psxdata/tick_bars.db "VACUUM;"

# Check new size
ls -lh /mnt/e/psxdata/tick_bars.db
```

## Step 3: Investigate psx.sqlite bloat

```bash
# List all tables
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;
"

# For each table, get row count estimate WITHOUT full scan
# Use sqlite_stat1 if available, otherwise quick LIMIT probe
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT 'sqlite_stat1 exists:', COUNT(*) FROM sqlite_master WHERE name='sqlite_stat1';
"

# If stat1 exists:
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT tbl, stat FROM sqlite_stat1 ORDER BY CAST(stat AS INTEGER) DESC LIMIT 20;
" 2>/dev/null

# If not, try quick probes (these are fast — just check if table is large)
for tbl in $(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT name FROM sqlite_master WHERE type='table'"); do
    count=$(sqlite3 /mnt/e/psxdata/psx.sqlite "SELECT COUNT(*) FROM (SELECT 1 FROM [$tbl] LIMIT 100001)" 2>/dev/null)
    if [ "$count" = "100001" ]; then
        echo "$tbl: >100K rows (LARGE)"
    else
        echo "$tbl: $count rows"
    fi
done

# Check for any raw_ticks or tick-related tables in psx.sqlite
sqlite3 /mnt/e/psxdata/psx.sqlite "
SELECT name FROM sqlite_master WHERE type='table' 
AND (name LIKE '%tick%' OR name LIKE '%raw%' OR name LIKE '%snapshot%');
"
```

**STOP — show me the psx.sqlite table sizes before making any changes to it.**
Report what you find. I will decide what to clean.

## Step 4: Fix tick_analytics.py — JSONL fallback for raw_ticks

The `_load_raw_ticks()` function in `tick_analytics.py` queries the now-dropped 
`raw_ticks` table. Add a JSONL fallback.

### File: `src/pakfindata/ui/page_views/tick_analytics.py`

Find the function `_load_raw_ticks` (around line 220). It currently does:

```python
def _load_raw_ticks(date_str: str, symbol: str) -> pd.DataFrame:
    # ... queries raw_ticks table from tick_bars.db ...
```

Replace with a version that:
1. Tries `tick_bars.db` `raw_ticks` table first (for any remaining historical data)
2. If table doesn't exist or returns empty → falls back to cloud JSONL
3. If cloud JSONL doesn't exist → falls back to local JSONL
4. Returns same DataFrame schema either way

```python
@st.cache_data(ttl=300)
def _load_raw_ticks(date_str: str, symbol: str) -> pd.DataFrame:
    """Load raw ticks — DB first, then JSONL fallback."""
    
    # Method 1: Try tick_bars.db raw_ticks table (legacy data)
    try:
        con = _tick_bars_con()
        # Check if table exists
        tables = [r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='raw_ticks'"
        ).fetchall()]
        
        if "raw_ticks" in tables:
            # Convert date to epoch range
            from datetime import datetime, timezone, timedelta
            PKT = timezone(timedelta(hours=5))
            day_start = datetime.strptime(date_str, "%Y-%m-%d").replace(
                hour=0, minute=0, second=0, tzinfo=PKT
            )
            day_end = day_start.replace(hour=23, minute=59, second=59)
            ts_start = day_start.timestamp()
            ts_end = day_end.timestamp()
            
            df = pd.read_sql_query(
                """SELECT symbol, ts, price, volume as vol, 
                          bid, ask, bid_vol, ask_vol, trades
                   FROM raw_ticks
                   WHERE symbol = ? AND ts BETWEEN ? AND ?
                   ORDER BY ts""",
                con, params=(symbol, ts_start, ts_end)
            )
            con.close()
            
            if not df.empty:
                return df
        else:
            con.close()
    except Exception:
        pass
    
    # Method 2: Cloud JSONL (primary going forward)
    cloud_path = Path(f"/mnt/e/psxdata/tick_logs_cloud/{date_str}.jsonl")
    local_path = Path(f"/mnt/e/psxdata/tick_logs/{date_str}.jsonl")
    
    jsonl_path = cloud_path if cloud_path.exists() else local_path
    
    if not jsonl_path.exists():
        return pd.DataFrame()
    
    records = []
    with open(jsonl_path) as f:
        for line in f:
            try:
                rec = json.loads(line.strip())
                if rec.get("symbol") != symbol:
                    continue
                records.append({
                    "symbol": rec.get("symbol"),
                    "ts": rec.get("timestamp"),
                    "price": rec.get("price", 0),
                    "vol": rec.get("volume", 0),
                    "bid": rec.get("bid", 0),
                    "ask": rec.get("ask", 0),
                    "bid_vol": rec.get("bidVol", 0),
                    "ask_vol": rec.get("askVol", 0),
                    "trades": rec.get("trades", 0),
                })
            except:
                continue
    
    if not records:
        return pd.DataFrame()
    
    df = pd.DataFrame(records)
    return df
```

### Key points:
- Column names must match what the rest of tick_analytics.py expects
- Check what columns the existing `_load_raw_ticks` returns and match exactly
- The JSONL fields use camelCase (`bidVol`) while DB uses snake_case (`bid_vol`) — map correctly
- Filter by symbol DURING read (line-by-line) not after — JSONL files are 200MB+

## Step 5: Fix live_ticker.py — remove raw_ticks metric

### File: `src/pakfindata/ui/page_views/live_ticker.py`

Line 129 shows `raw_ticks_in_memory` count. Since tick_service isn't running locally,
this will always be 0. Two options:

**Option A (recommended):** Change the metric label to something useful:
```python
# Before:
cols[4].metric("Raw ticks", f"{data.get('raw_ticks_in_memory', 0):,}")

# After:
cols[4].metric("Cloud ticks", "☁️ Active" if Path("/mnt/e/psxdata/tick_logs_cloud").exists() else "—")
```

**Option B:** Just hide it if zero:
```python
raw_count = data.get('raw_ticks_in_memory', 0)
if raw_count > 0:
    cols[4].metric("Raw ticks", f"{raw_count:,}")
else:
    cols[4].metric("Data source", "Cloud JSONL")
```

## Step 6: Fix live.py API — handle missing raw_ticks

### File: `src/pakfindata/api/routers/live.py`

Line 52 references `raw_ticks_in_memory`. This comes from tick_service status.
Since tick_service isn't running locally, this endpoint may not even be called.
But for safety:

```python
# Line 52 — make it safe
"raw_ticks_in_memory": data.get("raw_ticks_in_memory", 0),
# Already safe — .get() with default 0. No change needed.
```

Line 87 queries `ohlcv_5s` — this table is KEPT, no change needed.

## Step 7: Fix intraday.py — handle missing index_raw_ticks gracefully

### File: `src/pakfindata/ui/page_views/intraday.py`

Lines 907-909 query `index_raw_ticks`. This table is KEPT so no change needed.
But add a try/except for robustness:

```python
# Around line 907 — wrap in try/except
try:
    # existing index_raw_ticks query
    ...
except Exception:
    # Table might not exist or be empty
    pass
```

## Step 8: Fix tick_summary.py — handle missing raw_ticks

### File: `src/pakfindata/db/repositories/tick_summary.py`

Line 160 already says "skip raw_ticks for now (too slow)". Confirm it doesn't 
actually query raw_ticks anywhere:

```bash
grep -n "FROM raw_ticks\|INTO raw_ticks\|raw_ticks" \
    ~/pakfindata/src/pakfindata/db/repositories/tick_summary.py
```

If it only references raw_ticks in comments, no change needed.
If it queries the table, wrap in try/except.

## Step 9: Verify everything works

```bash
# Check tick_bars.db is smaller
ls -lh /mnt/e/psxdata/tick_bars.db

# Check remaining tables
sqlite3 /mnt/e/psxdata/tick_bars.db ".tables"

# Test tick_analytics page loads
cd ~/pakfindata
streamlit run src/pakfindata/ui/app.py
# Navigate to Tick Analytics → pick a date → pick a symbol
# Should load from JSONL if date is recent, or show empty gracefully

# Test intraday page
# Navigate to Intraday → check KSE100 overlay still works

# Test live_ticker page  
# Navigate to Live Ticker → check no errors

# Test signal scanner
# Navigate to Signal Scanner → run batch scan → check no errors
```

## Step 10: Report psx.sqlite findings

After Step 3, report:
- Which tables are largest
- Any tick-related tables that can be cleaned
- Total potential savings

**DO NOT drop anything from psx.sqlite without reporting first.**
psx.sqlite has 5-year EOD data, mutual fund NAVs, and other critical data 
that cannot be recreated.

## SUMMARY OF CHANGES

| File | Change | Risk |
|------|--------|------|
| tick_bars.db | DROP raw_ticks + VACUUM | None — cloud JSONL replaces |
| tick_analytics.py | `_load_raw_ticks()` → JSONL fallback | Low — same data, different source |
| live_ticker.py | Line 129 metric label change | None — cosmetic |
| live.py | No change needed | None |
| intraday.py | Add try/except around index_raw_ticks query | None — defensive |
| tick_summary.py | Verify no raw_ticks queries | None — already skipped |
| psx.sqlite | **REPORT ONLY — do not modify** | — |

**Total expected savings: ~500MB from tick_bars.db**
