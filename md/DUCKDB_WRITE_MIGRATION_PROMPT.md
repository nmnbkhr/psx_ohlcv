# Claude Code Prompt: Migrate Write Paths to DuckDB

## Context

DuckDB migration is done. Reads are fast. But writes still go to SQLite — 
meaning DuckDB gets stale. Need to make the app write to DuckDB for all 
migrated tables.

**Architecture after this change:**

```
DuckDB (pakfindata.duckdb) — PRIMARY for migrated tables:
  ├── eod_ohlcv          ← scrapers write here
  ├── intraday_bars      ← scrapers write here
  ├── tick_logs          ← signal scorer writes here
  ├── ohlcv_5s           ← tick_service writes here (if run locally)
  ├── index_ohlcv_5s     ← tick_service writes here
  ├── index_raw_ticks    ← tick_service writes here
  ├── psx_eod            ← DPS EOD scraper writes here
  └── market_snapshots   ← market watch poller writes here

SQLite (psx.sqlite) — keeps non-migrated tables:
  ├── mutual_fund_nav    ← MUFAP sync writes here (unchanged)
  ├── mutual_funds       ← unchanged
  ├── fund_risk_metrics  ← unchanged
  ├── companies          ← unchanged
  ├── sectors            ← unchanged
  ├── signal_configs     ← unchanged
  ├── kibor_daily        ← unchanged
  └── all other small tables ← unchanged

SQLite (tick_bars.db) — LEGACY, read-only archive:
  ├── ohlcv_5s           ← no more writes (DuckDB is primary)
  └── index tables       ← no more writes
```

## Step 1: Find ALL write paths for migrated tables

```bash
# Find every INSERT/UPDATE/CREATE for migrated tables
echo "=== eod_ohlcv / daily_ohlcv ==="
grep -rn "INSERT.*eod_ohlcv\|INSERT.*daily_ohlcv\|INTO.*eod_ohlcv\|INTO.*daily_ohlcv" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

echo "=== intraday_bars ==="
grep -rn "INSERT.*intraday_bars\|INTO.*intraday_bars" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

echo "=== tick_logs ==="
grep -rn "INSERT.*tick_logs\|INTO.*tick_logs" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

echo "=== ohlcv_5s ==="
grep -rn "INSERT.*ohlcv_5s\|INTO.*ohlcv_5s" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

echo "=== psx_eod ==="
grep -rn "INSERT.*psx_eod\|INTO.*psx_eod" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

echo "=== market_snapshots ==="
grep -rn "INSERT.*market_snapshots\|INTO.*market_snapshots" \
    ~/pakfindata/src/ --include="*.py" | grep -v __pycache__

echo "=== Which files connect to psx.sqlite for writes? ==="
grep -rn "psx\.sqlite\|SQLITE_PATH\|sqlite3\.connect" \
    ~/pakfindata/src/pakfindata/sources/ --include="*.py" | grep -v __pycache__
grep -rn "psx\.sqlite\|SQLITE_PATH\|sqlite3\.connect" \
    ~/pakfindata/src/pakfindata/services/ --include="*.py" | grep -v __pycache__
grep -rn "psx\.sqlite\|SQLITE_PATH\|sqlite3\.connect" \
    ~/pakfindata/src/pakfindata/engine/ --include="*.py" | grep -v __pycache__

echo "=== Which files connect to tick_bars.db for writes? ==="
grep -rn "tick_bars\|TICK_DB\|EOD_DB" \
    ~/pakfindata/src/pakfindata/services/ --include="*.py" | grep -v __pycache__
```

**STOP — read ALL output. Map every writer before changing anything.**

## Step 2: Create DuckDB write helper

Add to `src/pakfindata/db/connections.py`:

```python
import duckdb
from pathlib import Path

DUCKDB_PATH = Path("/mnt/e/psxdata/pakfindata.duckdb")

def duck_write() -> duckdb.DuckDBPyConnection:
    """Get DuckDB connection for writes. One writer at a time."""
    con = duckdb.connect(str(DUCKDB_PATH))
    return con


def duck_insert(table: str, df: "pd.DataFrame"):
    """Insert a DataFrame into DuckDB table. Handles duplicates via INSERT OR IGNORE."""
    if df is None or df.empty:
        return 0
    
    con = duck_write()
    try:
        # Register DataFrame as a view
        con.register("_temp_df", df)
        
        # Insert, ignoring duplicates (primary key conflicts)
        con.execute(f"""
            INSERT OR IGNORE INTO {table}
            SELECT * FROM _temp_df
        """)
        
        count = len(df)
        con.unregister("_temp_df")
        con.close()
        return count
    except Exception as e:
        con.close()
        raise e


def duck_execute(sql: str, params: list = None):
    """Execute a write SQL statement on DuckDB."""
    con = duck_write()
    try:
        if params:
            con.execute(sql, params)
        else:
            con.execute(sql)
        con.close()
    except Exception as e:
        con.close()
        raise e
```

## Step 3: Migrate each writer

For each file found in Step 1, apply this pattern:

### Pattern: Replace sqlite3 INSERT with DuckDB INSERT

```python
# BEFORE (SQLite):
import sqlite3
con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")
con.execute("INSERT OR IGNORE INTO daily_ohlcv VALUES (?,?,?,?,?,?,?)", row)
con.commit()
con.close()

# AFTER (DuckDB + SQLite dual-write for safety):
from pakfindata.db.connections import duck_execute, sqlite_query

# Write to DuckDB (primary)
try:
    duck_execute("INSERT OR IGNORE INTO eod_ohlcv VALUES (?,?,?,?,?,?,?)", row)
except Exception:
    pass  # DuckDB failure is not fatal

# Also write to SQLite (backup — remove later when confident)
con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")
con.execute("INSERT OR IGNORE INTO daily_ohlcv VALUES (?,?,?,?,?,?,?)", row)
con.commit()
con.close()
```

### Pattern: Batch insert with DataFrame

```python
# BEFORE:
import sqlite3
con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")
df.to_sql("intraday_bars", con, if_exists="append", index=False)
con.close()

# AFTER:
from pakfindata.db.connections import duck_insert

# Write to DuckDB (primary)
try:
    duck_insert("intraday_bars", df)
except Exception:
    pass

# Also write to SQLite (backup)
import sqlite3
con = sqlite3.connect("/mnt/e/psxdata/psx.sqlite")
df.to_sql("intraday_bars", con, if_exists="append", index=False)
con.close()
```

## Step 4: Specific file changes

Based on Step 1 findings, update each file. Common writers will be:

### sources/market_summary.py (or similar EOD scraper)
- Writes to: `daily_ohlcv` / `eod_ohlcv`
- Change: Add `duck_insert()` call before/alongside SQLite insert

### sources/sync_timeseries.py (or DPS intraday scraper)
- Writes to: `intraday_bars`
- Change: Add `duck_insert()` call

### engine/signal_score.py
- Writes to: `tick_logs`
- Change: Add `duck_insert()` call

### services/tick_service.py (local — not running, but fix for future)
- Writes to: `ohlcv_5s`, `index_ohlcv_5s`, `index_raw_ticks` in tick_bars.db
- Change: Add `duck_insert()` call in EOD flush function
- Note: This only matters if local tick_service is ever run again

### sources/psx_market_data.py (if exists)
- Writes to: `psx_eod`
- Change: Add `duck_insert()` call

## Step 5: Handle DuckDB table name differences

SQLite and DuckDB may have different table names for the same data.
Create a mapping:

```python
# Table name mapping: SQLite → DuckDB
TABLE_MAP = {
    "daily_ohlcv": "eod_ohlcv",      # different name in DuckDB
    "intraday_bars": "intraday_bars",  # same name
    "tick_logs": "tick_logs",          # same name
    "ohlcv_5s": "ohlcv_5s",           # same name
    "index_ohlcv_5s": "index_ohlcv_5s",
    "index_raw_ticks": "index_raw_ticks",
    "psx_eod": "psx_eod",
    "market_snapshots": "market_snapshots",
}
```

Check the actual DuckDB schema vs SQLite schema — column names and types 
must match exactly. If they differ, the insert will fail.

```bash
# Compare schemas
echo "=== DuckDB schema ==="
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
for tbl in ['eod_ohlcv','intraday_bars','tick_logs','ohlcv_5s','index_ohlcv_5s','index_raw_ticks','psx_eod','market_snapshots']:
    try:
        cols = con.execute(f'DESCRIBE {tbl}').df()
        print(f'\n{tbl}:')
        for _, row in cols.iterrows():
            print(f'  {row[\"column_name\"]:20s} {row[\"column_type\"]}')
    except: pass
con.close()
"

echo "=== SQLite schema (psx.sqlite) ==="
for tbl in daily_ohlcv intraday_bars tick_logs psx_eod; do
    echo "--- $tbl ---"
    sqlite3 /mnt/e/psxdata/psx.sqlite ".schema $tbl" 2>/dev/null
done

echo "=== SQLite schema (tick_bars.db) ==="
for tbl in ohlcv_5s index_ohlcv_5s index_raw_ticks market_snapshots; do
    echo "--- $tbl ---"
    sqlite3 /mnt/e/psxdata/tick_bars.db ".schema $tbl" 2>/dev/null
done
```

**If schemas differ, create adapter functions that remap columns before insert.**

## Step 6: Dual-write strategy

For the first 2 weeks, write to BOTH DuckDB and SQLite:

```python
def write_to_both(table_sqlite: str, table_duck: str, df: pd.DataFrame,
                  sqlite_path: str = "/mnt/e/psxdata/psx.sqlite"):
    """Dual-write: DuckDB (primary) + SQLite (backup)."""
    
    # DuckDB (primary)
    try:
        duck_insert(table_duck, df)
    except Exception as e:
        print(f"⚠️ DuckDB write failed for {table_duck}: {e}")
    
    # SQLite (backup — remove after 2 weeks when confident)
    try:
        con = sqlite3.connect(sqlite_path)
        df.to_sql(table_sqlite, con, if_exists="append", index=False)
        con.commit()
        con.close()
    except Exception as e:
        print(f"⚠️ SQLite write failed for {table_sqlite}: {e}")
```

After 2 weeks of dual-write with no issues, remove SQLite writes 
for migrated tables.

## Step 7: Test writes

```bash
cd ~/pakfindata
source .venv/bin/activate
export PYTHONPATH=~/pakfindata/src

# Test 1: Run EOD scraper (if available)
# python -m pakfindata.sources.market_summary  # or whatever the command is

# Test 2: Check DuckDB has new data
python3 -c "
import duckdb
con = duckdb.connect('/mnt/e/psxdata/pakfindata.duckdb', read_only=True)
print('=== eod_ohlcv latest ===')
print(con.execute('SELECT date, COUNT(*) FROM eod_ohlcv GROUP BY date ORDER BY date DESC LIMIT 5').df())
print('=== intraday_bars latest ===')
print(con.execute('SELECT date, COUNT(*) FROM intraday_bars GROUP BY date ORDER BY date DESC LIMIT 5').df())
print('=== tick_logs latest ===')
print(con.execute(\"SELECT DATE_TRUNC('day', TIMESTAMP 'epoch' + timestamp * INTERVAL '1 second') as dt, COUNT(*) FROM tick_logs GROUP BY dt ORDER BY dt DESC LIMIT 5\").df())
con.close()
"

# Test 3: Streamlit app loads correctly
streamlit run src/pakfindata/ui/app.py
```

## Step 8: Cloud tick_service — write to DuckDB on sync

When you download JSONL from cloud and want to load into DuckDB:

```python
# This already works via read_json_auto() — no import needed!
# DuckDB queries JSONL files directly.

# But if you WANT to import for faster repeated queries:
from pakfindata.db.connections import duck_write

con = duck_write()
con.execute("""
    INSERT OR IGNORE INTO tick_logs
    SELECT 
        symbol,
        timestamp,
        price,
        open,
        high,
        low,
        price as close,
        volume,
        change,
        "changePercent" as change_pct,
        bid,
        ask,
        "bidVol" as bid_vol,
        "askVol" as ask_vol,
        trades,
        value,
        "previousClose" as previous_close,
        market,
        "_ts" as ts_iso,
        CAST(STRFTIME(TIMESTAMP 'epoch' + timestamp * INTERVAL '1 second', '%Y-%m-%d') AS DATE) as date
    FROM read_json_auto('/mnt/e/psxdata/tick_logs_cloud/2026-03-18.jsonl',
         format='newline_delimited', maximum_object_size=10485760)
""")
con.close()
```

## IMPORTANT NOTES

1. **Dual-write for safety** — write to BOTH DuckDB + SQLite for 2 weeks.
   Then drop SQLite writes for migrated tables.

2. **DuckDB INSERT OR IGNORE** — DuckDB supports this for primary key 
   conflicts. Same semantics as SQLite.

3. **Schema must match exactly** — column count, names, types. Step 5 
   compares schemas. Fix any mismatches before writing.

4. **One writer at a time** — DuckDB allows multiple readers but only 
   one writer. Close write connections promptly.

5. **Don't touch SQLite tables that weren't migrated** — mutual_fund_nav,
   companies, sectors, configs etc. stay in SQLite only.

6. **JSONL files don't need import** — DuckDB reads them directly.
   Only import to tick_logs table if you need faster repeated queries
   or joins with other tables.

7. **After 2 weeks:** Remove SQLite writes for migrated tables. 
   SQLite files become read-only archives. Eventually deletable when 
   all pages are migrated to DuckDB reads.
