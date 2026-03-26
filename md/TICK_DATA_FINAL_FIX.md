# Claude Code Prompt: Fix ALL Tick Data Issues — One Pass

## THE PROBLEM

Tick data exists in 5 places but pages can't find it. Every page that needs 
tick data should have a 4-level fallback chain and work on page load/refresh 
without manual intervention.

## THE RULE

Every tick data query in the ENTIRE app must use this fallback chain:

```
1. DuckDB tick_logs table (fastest, most data — 4.6M rows)
2. DuckDB read_json_auto() on cloud JSONL files  
3. DuckDB read_json_auto() on local JSONL files
4. Fall back to most recent date that HAS data
```

If today's data isn't available, AUTOMATICALLY use the most recent date.
NEVER show "requires tick data" if ANY historical tick data exists.

## Step 1: Create ONE universal tick data loader

Add to `src/pakfindata/db/connections.py`:

```python
import duckdb
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

PKT = timezone(timedelta(hours=5))

# All JSONL locations (checked in order)
JSONL_DIRS = [
    Path("/mnt/e/psxdata/tick_logs_cloud"),   # cloud synced
    Path.home() / "psxdata" / "tick_logs",     # local tick_service
    Path("/mnt/e/psxdata/tick_logs"),           # legacy local
]

DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")


def find_jsonl(date_str: str) -> Optional[Path]:
    """Find JSONL tick file for a date — checks all locations + naming patterns."""
    for d in JSONL_DIRS:
        if not d.exists():
            continue
        for pattern in [f"ticks_{date_str}.jsonl", f"{date_str}.jsonl"]:
            path = d / pattern
            if path.exists() and path.stat().st_size > 100:
                return path
    return None


def get_available_tick_dates() -> list[str]:
    """Get all dates that have tick data (DuckDB + JSONL)."""
    dates = set()
    
    # From DuckDB
    try:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        rows = con.execute("SELECT DISTINCT date FROM tick_logs ORDER BY date DESC").fetchall()
        dates.update(str(r[0]) for r in rows)
        con.close()
    except:
        pass
    
    # From JSONL files
    for d in JSONL_DIRS:
        if not d.exists():
            continue
        for f in d.glob("*.jsonl"):
            # Extract date from ticks_2026-03-24.jsonl or 2026-03-24.jsonl
            stem = f.stem.replace("ticks_", "").replace("raw_ws_", "")
            if len(stem) == 10 and stem[4] == "-":  # looks like YYYY-MM-DD
                dates.add(stem)
    
    return sorted(dates, reverse=True)


def load_ticks(symbol: str, date_str: str = None, 
               columns: str = "*", limit: int = None) -> "pd.DataFrame":
    """
    Universal tick data loader — tries all sources with automatic fallback.
    
    Fallback chain:
      1. DuckDB tick_logs (fastest)
      2. Cloud JSONL via read_json_auto
      3. Local JSONL via read_json_auto
      4. Most recent available date
    
    Always returns a DataFrame. Never raises. Empty DataFrame if truly no data.
    """
    import pandas as pd
    
    if date_str is None:
        date_str = datetime.now(PKT).strftime("%Y-%m-%d")
    
    limit_clause = f"LIMIT {limit}" if limit else ""
    
    # ── Attempt 1: DuckDB tick_logs table ──
    try:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        df = con.execute(f"""
            SELECT {columns} FROM tick_logs 
            WHERE symbol = ? AND date = ?
            ORDER BY timestamp
            {limit_clause}
        """, [symbol, date_str]).df()
        con.close()
        
        if not df.empty:
            df["_source"] = "duckdb"
            return df
    except:
        pass
    
    # ── Attempt 2: JSONL files via DuckDB read_json_auto ──
    jsonl_path = find_jsonl(date_str)
    if jsonl_path:
        try:
            con = duckdb.connect()
            df = con.execute(f"""
                SELECT {columns} FROM read_json_auto('{jsonl_path}',
                     format='newline_delimited',
                     maximum_object_size=10485760)
                WHERE symbol = '{symbol}'
                ORDER BY timestamp
                {limit_clause}
            """).df()
            con.close()
            
            if not df.empty:
                df["_source"] = f"jsonl:{jsonl_path.name}"
                return df
        except:
            pass
    
    # ── Attempt 3: Fall back to most recent date with data ──
    if date_str == datetime.now(PKT).strftime("%Y-%m-%d"):
        # Only fallback if we were looking for today (not yet synced)
        available = get_available_tick_dates()
        for fallback_date in available:
            if fallback_date == date_str:
                continue
            
            # Try DuckDB first
            try:
                con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
                df = con.execute(f"""
                    SELECT {columns} FROM tick_logs 
                    WHERE symbol = ? AND date = ?
                    ORDER BY timestamp
                    {limit_clause}
                """, [symbol, fallback_date]).df()
                con.close()
                
                if not df.empty:
                    df["_source"] = f"duckdb:fallback:{fallback_date}"
                    df["_fallback_date"] = fallback_date
                    return df
            except:
                pass
            
            # Try JSONL
            fb_path = find_jsonl(fallback_date)
            if fb_path:
                try:
                    con = duckdb.connect()
                    df = con.execute(f"""
                        SELECT {columns} FROM read_json_auto('{fb_path}',
                             format='newline_delimited',
                             maximum_object_size=10485760)
                        WHERE symbol = '{symbol}'
                        ORDER BY timestamp
                        {limit_clause}
                    """).df()
                    con.close()
                    
                    if not df.empty:
                        df["_source"] = f"jsonl:fallback:{fb_path.name}"
                        df["_fallback_date"] = fallback_date
                        return df
                except:
                    pass
            
            break  # Only try the most recent fallback date
    
    # ── Nothing found ──
    return pd.DataFrame()


def load_ticks_for_date(date_str: str, symbol: str = None,
                        columns: str = "*") -> "pd.DataFrame":
    """
    Load ALL ticks for a date (optionally filtered by symbol).
    Same fallback chain as load_ticks but without symbol filter if None.
    """
    import pandas as pd
    
    where_sym = f"AND symbol = '{symbol}'" if symbol else ""
    
    # DuckDB first
    try:
        con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
        df = con.execute(f"""
            SELECT {columns} FROM tick_logs 
            WHERE date = ? {where_sym.replace('AND', '')} 
            ORDER BY timestamp
        """, [date_str]).df()
        con.close()
        if not df.empty:
            return df
    except:
        pass
    
    # JSONL fallback
    jsonl_path = find_jsonl(date_str)
    if jsonl_path:
        try:
            where = f"WHERE symbol = '{symbol}'" if symbol else ""
            con = duckdb.connect()
            df = con.execute(f"""
                SELECT {columns} FROM read_json_auto('{jsonl_path}',
                     format='newline_delimited',
                     maximum_object_size=10485760)
                {where}
                ORDER BY timestamp
            """).df()
            con.close()
            if not df.empty:
                return df
        except:
            pass
    
    return pd.DataFrame()
```

## Step 2: Find and fix EVERY page that reads tick data

```bash
# Find every file that queries tick data
grep -rn "tick_logs\|tick_data\|_load_raw_ticks\|ohlcv_5s\|read_json_auto\|tick.*require\|requires tick\|No tick\|no tick" \
    ~/pakfindata/src/pakfindata/ui/page_views/ --include="*.py" | grep -v __pycache__ | sort -t: -k1,1 -u
```

For EACH file found, replace direct DB queries with the universal loader.

### signal_dashboard.py — Layer 3 fix

Find the Layer 3 / Execution DNA section:
```bash
grep -n "Layer 3\|EXECUTION DNA\|requires tick\|tick.*require\|execution_score\|tick_logs" \
    ~/pakfindata/src/pakfindata/ui/page_views/signal_dashboard.py | head -20
```

Read the surrounding code. Replace the tick data loading with:

```python
from pakfindata.db.connections import load_ticks

# Load tick data for symbol
ticks_df = load_ticks(symbol, date_str)

if ticks_df.empty:
    st.info("📊 No tick data available for this symbol yet. Layer 3 scores will update after market sync.")
    execution_score = 0
else:
    # Check if using fallback date
    if "_fallback_date" in ticks_df.columns:
        fb_date = ticks_df["_fallback_date"].iloc[0]
        st.caption(f"📊 Using tick data from {fb_date}")
    
    # ... existing Layer 3 analytics code using ticks_df ...
```

**KEY: Change the warning message from "requires tick data, sync or start collector" 
to something that doesn't confuse the user. The data may exist — it just needs 
the fallback chain to find it.**

### tick_analytics.py — _load_raw_ticks fix

Find the function:
```bash
grep -n "_load_raw_ticks\|def _load\|def load_ticks" \
    ~/pakfindata/src/pakfindata/ui/page_views/tick_analytics.py | head -10
```

Replace `_load_raw_ticks()` to use the universal loader:

```python
from pakfindata.db.connections import load_ticks, find_jsonl

def _load_raw_ticks(date_str: str, symbol: str) -> pd.DataFrame:
    """Load raw ticks — uses universal loader with full fallback chain."""
    return load_ticks(symbol, date_str)
```

### microstructure.py — JSONL loading fix

```bash
grep -n "jsonl\|tick_logs\|load.*tick\|read_json" \
    ~/pakfindata/src/pakfindata/ui/page_views/microstructure.py | head -10
```

Replace with universal loader:

```python
from pakfindata.db.connections import load_ticks
ticks = load_ticks(symbol, date_str)
```

### tick_replay.py — JSONL loading fix

```bash
grep -n "jsonl\|tick_logs\|load.*tick\|read_json" \
    ~/pakfindata/src/pakfindata/ui/page_views/tick_replay.py | head -10
```

Replace with:

```python
from pakfindata.db.connections import load_ticks, get_available_tick_dates
available_dates = get_available_tick_dates()
# Use these for the date picker instead of scanning directories
```

### intraday.py — Index tab fix

```bash
grep -n "index_ohlcv\|index_raw\|JSONL\|tick_logs.*index" \
    ~/pakfindata/src/pakfindata/ui/page_views/intraday.py | head -10
```

If reading JSONL directly for index data, switch to DuckDB.

## Step 3: Add sync status to sidebar (app.py)

Add a small data freshness indicator in the sidebar so the user always 
knows when data was last synced:

```python
# In app.py sidebar section, after logo/theme
import json
from pathlib import Path

st.sidebar.markdown("---")

# Quick data status
try:
    import duckdb
    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)
    latest = con.execute("SELECT MAX(date) FROM tick_logs").fetchone()[0]
    count = con.execute("SELECT COUNT(*) FROM tick_logs").fetchone()[0]
    con.close()
    
    today = datetime.now(PKT).strftime("%Y-%m-%d")
    if str(latest) == today:
        st.sidebar.success(f"📡 Ticks: {today} ({count:,})")
    else:
        st.sidebar.warning(f"📡 Ticks: {latest} ({count:,})")
except:
    st.sidebar.caption("📡 Tick status unavailable")
```

## Step 4: Create the cloud sync script (if not yet created)

```bash
cat > ~/sync_psx_cloud.sh << 'SCRIPT'
#!/bin/bash
echo "📥 Syncing from Oracle Cloud..."
rsync -avz --progress psx-cloud:~/psxdata/tick_logs/ /mnt/e/psxdata/tick_logs_cloud/
rsync -avz --progress psx-cloud:~/psxdata/tick_bars.db /mnt/e/psxdata/tick_bars.db

# Symlink for naming compatibility
for f in /mnt/e/psxdata/tick_logs_cloud/ticks_*.jsonl; do
    [ -f "$f" ] || continue
    base=$(basename "$f")
    link="/mnt/e/psxdata/tick_logs_cloud/${base#ticks_}"
    [ ! -e "$link" ] && ln -sf "$f" "$link" 2>/dev/null
done

echo "✅ Done"
ls -lh /mnt/e/psxdata/tick_logs_cloud/*.jsonl 2>/dev/null | tail -5
SCRIPT
chmod +x ~/sync_psx_cloud.sh
```

## Step 5: Create auto-sync cron (15 min during market hours)

```bash
mkdir -p ~/pakfindata/scripts

cat > ~/pakfindata/scripts/auto_sync.sh << 'SCRIPT'
#!/bin/bash
# Runs via cron: */15 9-17 * * 1-5
LOG=~/psxdata/auto_sync.log
TODAY=$(date +%Y-%m-%d)
TMP=/tmp/pfsync
mkdir -p $TMP

echo "[$(date +%H:%M:%S)] Starting sync..." >> $LOG

# 1. rsync today's JSONL + tick_bars.db
rsync -az psx-cloud:~/psxdata/tick_logs/ticks_${TODAY}.jsonl /mnt/e/psxdata/tick_logs_cloud/ 2>>$LOG
rsync -az psx-cloud:~/psxdata/tick_bars.db /mnt/e/psxdata/tick_bars.db 2>>$LOG

# 2. Symlink for naming
JSONL="/mnt/e/psxdata/tick_logs_cloud/ticks_${TODAY}.jsonl"
LINK="/mnt/e/psxdata/tick_logs_cloud/${TODAY}.jsonl"
[ -f "$JSONL" ] && [ ! -e "$LINK" ] && ln -sf "$JSONL" "$LINK"

# 3. Import JSONL → DuckDB via /tmp/
if [ -f "$JSONL" ]; then
    cp /mnt/e/psxdata/pakfindata.duckdb $TMP/target.duckdb 2>>$LOG
    python3 -c "
import duckdb
con = duckdb.connect('/tmp/pfsync/target.duckdb')
try:
    con.execute(\"\"\"
        INSERT OR IGNORE INTO tick_logs
        SELECT * FROM read_json_auto('/mnt/e/psxdata/tick_logs_cloud/ticks_${TODAY}.jsonl',
             format='newline_delimited', maximum_object_size=10485760)
    \"\"\")
    print(f'tick_logs: {con.execute(\"SELECT COUNT(*) FROM tick_logs\").fetchone()[0]:,}')
except Exception as e:
    print(f'JSONL import error: {e}')
con.close()
" >> $LOG 2>&1
    cp $TMP/target.duckdb /mnt/e/psxdata/pakfindata.duckdb 2>>$LOG
fi

# 4. Sync ohlcv_5s from tick_bars.db → DuckDB via /tmp/
cp /mnt/e/psxdata/tick_bars.db $TMP/source.db 2>>$LOG
cp /mnt/e/psxdata/pakfindata.duckdb $TMP/target.duckdb 2>>$LOG
python3 -c "
import duckdb
con = duckdb.connect('/tmp/pfsync/target.duckdb')
con.execute('INSTALL sqlite; LOAD sqlite;')
con.execute(\"ATTACH '/tmp/pfsync/source.db' AS src (TYPE SQLITE, READ_ONLY)\")
for t in ['ohlcv_5s', 'index_ohlcv_5s', 'index_raw_ticks']:
    try:
        before = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        con.execute(f'INSERT OR IGNORE INTO {t} SELECT * FROM src.{t}')
        after = con.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
        if after > before: print(f'{t}: +{after-before:,}')
    except Exception as e:
        print(f'{t}: {e}')
con.execute('DETACH src')
con.close()
" >> $LOG 2>&1
cp $TMP/target.duckdb /mnt/e/psxdata/pakfindata.duckdb 2>>$LOG

# Cleanup
rm -f $TMP/source.db $TMP/target.duckdb

# Write status file for sidebar
python3 -c "
import json, duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
latest = str(con.execute('SELECT MAX(date) FROM tick_logs').fetchone()[0])
count = con.execute('SELECT COUNT(*) FROM tick_logs').fetchone()[0]
con.close()
import time
status = {'last_sync': time.strftime('%Y-%m-%d %H:%M'), 'latest_tick_date': latest, 'tick_count': count}
with open('/mnt/e/psxdata/sync_status.json', 'w') as f:
    json.dump(status, f)
print(f'Status: {latest}, {count:,} ticks')
" >> $LOG 2>&1

echo "[$(date +%H:%M:%S)] Done" >> $LOG
SCRIPT
chmod +x ~/pakfindata/scripts/auto_sync.sh

# Install cron
(crontab -l 2>/dev/null | grep -v auto_sync; echo "*/15 9-17 * * 1-5 bash ~/pakfindata/scripts/auto_sync.sh") | crontab -
```

## Step 6: Verify EVERYTHING

```bash
echo "=== 1. Universal loader exists ==="
grep -c "def load_ticks\|def find_jsonl\|def get_available_tick_dates" \
    ~/pakfindata/src/pakfindata/db/connections.py

echo "=== 2. Signal dashboard uses universal loader ==="
grep -c "load_ticks\|from.*connections.*import" \
    ~/pakfindata/src/pakfindata/ui/page_views/signal_dashboard.py

echo "=== 3. No more 'requires tick data' hardcoded ==="
grep -n "requires tick data\|Sync tick logs\|start the tick collector" \
    ~/pakfindata/src/pakfindata/ui/page_views/signal_dashboard.py

echo "=== 4. tick_analytics uses universal loader ==="
grep -c "load_ticks\|from.*connections.*import" \
    ~/pakfindata/src/pakfindata/ui/page_views/tick_analytics.py

echo "=== 5. Cloud sync script exists ==="
ls -la ~/sync_psx_cloud.sh ~/pakfindata/scripts/auto_sync.sh

echo "=== 6. Cron installed ==="
crontab -l 2>/dev/null | grep auto_sync

echo "=== 7. Available tick dates ==="
python3 -c "
import sys; sys.path.insert(0, '$HOME/pakfindata/src')
from pakfindata.db.connections import get_available_tick_dates
dates = get_available_tick_dates()
print(f'Available dates: {len(dates)}')
for d in dates[:5]: print(f'  {d}')
"

echo "=== 8. Test load_ticks for HUBC ==="
python3 -c "
import sys; sys.path.insert(0, '$HOME/pakfindata/src')
from pakfindata.db.connections import load_ticks
df = load_ticks('HUBC')
print(f'Rows: {len(df)}')
if not df.empty:
    print(f'Source: {df[\"_source\"].iloc[0]}')
    if '_fallback_date' in df.columns:
        print(f'Fallback date: {df[\"_fallback_date\"].iloc[0]}')
"
```

## IMPORTANT

1. **ONE universal loader** — `load_ticks()` in connections.py. ALL pages use it.
2. **NEVER show "requires tick data"** if ANY historical data exists — fall back automatically.
3. **4-level fallback**: DuckDB → cloud JSONL → local JSONL → most recent date.
4. **Works on page refresh** — no manual sync needed for the page to load.
5. **Auto-sync cron** keeps DuckDB fresh every 15 minutes during market hours.
6. **Sidebar status** shows data freshness on every page.
7. **DO NOT break existing pages** — only ADD the universal loader and update import paths.
8. **All /tmp/ pattern** for SQLite→DuckDB sync operations.
